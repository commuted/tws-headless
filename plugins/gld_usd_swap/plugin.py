"""
plugins/gld_usd_swap/plugin.py — Gold/USD Swap Strategy v3 (multi-factor)

Two decisions per trading day:

  AT OPEN  (09:30): overnight position matures.
                    Check the composite regime saved at the prior close:
                      GOLD → hold GLD through the intraday session
                      CASH → sell GLD at open (sit out intraday, re-buy at close)

  AT CLOSE (15:45): session nears end.
                    Place a MOC (Market on Close) order to buy GLD for the overnight
                    hold — fills at the official 16:00 closing auction price.
                    Save current composite regime for tomorrow's open decision.
                    (15:45 bar completes at 15:50, inside NYSE ARCA's MOC cutoff.)

This separates two independent return streams:

  Overnight drift   — unconditional (~+0.10 %/night, Sharpe ~2.0)
                      The bulk of GLD's long-run gain occurs during Asian /
                      European hours. Not driven by any regime.

  Intraday edge     — regime-conditional (composite Sharpe ~3.0+ when signal on).
                      Captured only on gold-composite-regime days.

─────────────────────────────────────────────────────────────────────────────
COMPOSITE INTRADAY REGIME (three-factor signal)

Primary factors (both must agree for baseline gold signal):
  UUP signal : UUP fast SMA < slow SMA → USD weakening → gold bullish
  TLT signal : TLT fast SMA > slow SMA → nominal rates falling → gold bullish

Inflation extension (meta-gated):
  RINF signal: RINF fast SMA > slow SMA → inflation expectations rising → gold bullish
  Meta-gate  : GLD 20-bar SMA > GLD 60-bar SMA → GLD structural uptrend active

Regime logic:
  If GLD is in a structural uptrend (meta=True):
      gold_regime = UUP_gold AND (TLT_gold OR RINF_gold)
      ↑ In trending gold markets, RINF extending into TLT-bear/RINF-bull (stagflation)
        adds confirmed alpha; Sharpe improves from 2.98 → 3.52 in 2023-2025.
  Else:
      gold_regime = UUP_gold AND TLT_gold
      ↑ In choppy/bear gold markets (e.g. 2016-2018), RINF is noise;
        UUP+TLT alone achieves Sharpe 1.01 vs 0.36 for UUP alone.

Backtested Sharpe by period (combined overnight+intraday strategy):
  2016-2018  UUP+TLT       Sh=1.01  MaxDD= 9%   (meta suppresses RINF correctly)
  2019-2022  UUP+TLT       Sh=0.98  MaxDD=16%
  2023-2025  UUP+(TLT|RINF) gated Sh=3.52  MaxDD= 7%

─────────────────────────────────────────────────────────────────────────────
SMOOTHING

Each ETF (UUP, TLT, RINF) is pre-smoothed with an adaptive StreamingTriangleTooth
(inlined from volomom/volmon.py) before entering its SMA window.
  mean(push(close)) = close on normal bars; pulled toward prev close on spike bars.
  Derivative = rolling p50 of |Δclose| for that ETF, updated live.

─────────────────────────────────────────────────────────────────────────────
Default parameters (tunable at runtime):
  fast_bars              =  5    (25 min at 5-min bars)
  slow_bars              = 20    (100 min)
  meta_fast_bars         = 20    GLD trend fast SMA
  meta_slow_bars         = 60    GLD trend slow SMA
  vol_window             = 20    rolling window for derivative estimation
  derivative_percentile  = 50    p50 of recent |Δclose| sets slope limit
  allocation_dollars     = 10 000 USD
"""

import logging
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional

from ibapi.order import Order as IbOrder

from ib.contract_builder import ContractBuilder
from plugins.base import PluginBase, TradeSignal

logger = logging.getLogger(__name__)

REGIME_GOLD    = "gold"
REGIME_CASH    = "cash"
REGIME_UNKNOWN = "unknown"

# Fallback derivatives before adaptive estimate warms up
_INIT_DERIV_UUP  = 0.074   # p50 UUP daily moves 2023-2025
_INIT_DERIV_TLT  = 0.500   # conservative TLT initial (~$100 ETF, 0.5% daily)
_INIT_DERIV_RINF = 0.200   # conservative RINF initial

_OPEN_HOUR,  _OPEN_MIN  = 9,  30
_CLOSE_HOUR, _CLOSE_MIN = 15, 45   # bar completes 15:50 — inside NYSE ARCA MOC cutoff


# ---------------------------------------------------------------------------
# StreamingTriangleTooth — inlined from volomom/volmon.py
# ---------------------------------------------------------------------------
class _StreamingTriangleTooth:
    """
    Slope-limited streaming interpolator.

    push(value) → [ramp_pt_1, …, ramp_pt_n, value]
    For moves within the slope limit the list contains only [value].
    mean(push(value)) attenuates spike moves toward the previous sample,
    damping the SMA's reaction to outlier bars without changing bar count.
    """

    def __init__(self, target_derivative: float, step: float = 1.0):
        if step <= 0:
            raise ValueError("step must be positive")
        if target_derivative == 0:
            raise ValueError("target_derivative cannot be zero")
        self.max_slope = abs(target_derivative)
        self.step      = step
        self._prev: float | None = None

    def push(self, value: float) -> List[float]:
        if self._prev is None:
            self._prev = value
            return [value]
        ramp       = self._ramp_between(self._prev, value)
        self._prev = value
        return ramp + [value]

    def _ramp_between(self, start: float, end: float) -> List[float]:
        distance    = end - start
        direction   = 1 if distance > 0 else -1
        dy_per_step = direction * self.max_slope * self.step
        if (direction > 0 and start + dy_per_step >= end) or \
           (direction < 0 and start + dy_per_step <= end):
            return []
        ramp, cur = [], start
        while (direction > 0 and cur + dy_per_step < end) or \
              (direction < 0 and cur + dy_per_step > end):
            cur += dy_per_step
            ramp.append(cur)
        return ramp

    def seed(self, value: float) -> None:
        """Prime the smoother with a known price (no output emitted)."""
        self._prev = value


# ---------------------------------------------------------------------------
# Per-instrument state container (avoids repetitive attribute naming)
# ---------------------------------------------------------------------------
class _InstrumentState:
    """Adaptive smoother + rolling SMA state for a single ETF."""

    def __init__(self, init_derivative: float, maxlen_closes: int = 80,
                 maxlen_moves: int = 200):
        self.smoother:    _StreamingTriangleTooth = _StreamingTriangleTooth(init_derivative)
        self.derivative:  float = init_derivative
        self.closes:      deque = deque(maxlen=maxlen_closes)
        self.abs_moves:   deque = deque(maxlen=maxlen_moves)
        self.prev_close:  float = 0.0
        self.price:       float = 0.0
        self.fast_sma:    float = 0.0
        self.slow_sma:    float = 0.0

    def push(self, close: float, vol_window: int, percentile: int,
             fast: int, slow: int, change_thresh: float = 0.05) -> None:
        """Feed one bar. Updates closes, SMAs, and adapts derivative."""
        if self.prev_close > 0:
            self.abs_moves.append(abs(close - self.prev_close))
            self._adapt(percentile, change_thresh)
        self.prev_close = close
        self.price      = close

        out = self.smoother.push(close)
        self.closes.append(sum(out) / len(out))

        if len(self.closes) >= slow:
            cl = list(self.closes)
            self.fast_sma = sum(cl[-fast:]) / fast
            self.slow_sma = sum(cl[-slow:]) / slow

    def warmed_up(self, slow: int) -> bool:
        return len(self.closes) >= slow

    def _adapt(self, percentile: int, change_thresh: float) -> None:
        moves = sorted(self.abs_moves)
        n     = len(moves)
        if n < 5:
            return
        idx       = max(0, int(n * percentile / 100) - 1)
        new_deriv = moves[idx]
        if new_deriv <= 0:
            return
        if abs(new_deriv - self.derivative) / self.derivative > change_thresh:
            self.derivative = new_deriv
            s = _StreamingTriangleTooth(new_deriv)
            if self.prev_close > 0:
                s.seed(self.prev_close)
            self.smoother = s

    def save(self) -> dict:
        return {"derivative": self.derivative, "price": self.price,
                "prev_close": self.prev_close}

    def restore(self, d: dict, init_derivative: float) -> None:
        self.derivative = d.get("derivative", init_derivative)
        self.price      = d.get("price",      0.0)
        self.prev_close = d.get("prev_close", 0.0)
        self.smoother   = _StreamingTriangleTooth(self.derivative)
        if self.prev_close > 0:
            self.smoother.seed(self.prev_close)


# ---------------------------------------------------------------------------
# Plugin
# ---------------------------------------------------------------------------
class GldUsdSwapPlugin(PluginBase):
    """Session-aware GLD/USD swap — see module docstring for full details."""

    VERSION = "3.0.0"
    IS_SYSTEM_PLUGIN = False

    def __init__(self, base_path=None, portfolio=None,
                 shared_holdings=None, message_bus=None):
        super().__init__("gld_usd_swap", base_path, portfolio,
                         shared_holdings, message_bus)

        # --- tunable parameters ---
        self.fast_bars:             int   = 5
        self.slow_bars:             int   = 20
        self.meta_fast_bars:        int   = 20   # GLD trend fast SMA
        self.meta_slow_bars:        int   = 60   # GLD trend slow SMA
        self.vol_window:            int   = 20
        self.derivative_percentile: int   = 50
        self.allocation_dollars:    float = 10_000.0

        # --- per-instrument signal state ---
        self._uup  = _InstrumentState(_INIT_DERIV_UUP)
        self._tlt  = _InstrumentState(_INIT_DERIV_TLT)
        self._rinf = _InstrumentState(_INIT_DERIV_RINF)

        # --- GLD meta-signal state (structural trend gate for RINF) ---
        self._gld_meta_closes: deque = deque(maxlen=80)   # raw closes, no smoother
        self._gld_meta_fast:   float = 0.0
        self._gld_meta_slow:   float = 0.0
        self._gld_in_uptrend:  bool  = False

        # --- composite regime ---
        self._regime:                str = REGIME_UNKNOWN
        self._regime_at_prior_close: str = REGIME_UNKNOWN

        # --- session / position state ---
        self._gld_price:    float = 0.0
        self._holding_gld:  bool  = False

        # --- diagnostics ---
        self._pending_signals:  List[TradeSignal] = []
        self._trade_count:      int               = 0
        self._overnight_holds:  int               = 0
        self._intraday_holds:   int               = 0
        self._last_trade_time:  Optional[str]     = None

        # --- last signal factors (for logging / status) ---
        self._last_factors: dict = {}

    @property
    def description(self) -> str:
        return (
            "Session-aware GLD/USD swap v3: overnight hold every night + intraday "
            "hold when composite signal is gold. Signal: UUP+TLT primary, "
            "RINF extension gated by GLD structural uptrend (20/60 SMA)."
        )

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    def start(self) -> bool:
        saved = self.load_state()
        if saved:
            self._holding_gld            = saved.get("holding_gld",            False)
            self._regime_at_prior_close  = saved.get("regime_at_prior_close",  REGIME_UNKNOWN)
            self._trade_count            = saved.get("trade_count",            0)
            self._overnight_holds        = saved.get("overnight_holds",        0)
            self._intraday_holds         = saved.get("intraday_holds",         0)
            self._last_trade_time        = saved.get("last_trade_time")
            self.fast_bars               = saved.get("fast_bars",               self.fast_bars)
            self.slow_bars               = saved.get("slow_bars",               self.slow_bars)
            self.meta_fast_bars          = saved.get("meta_fast_bars",          self.meta_fast_bars)
            self.meta_slow_bars          = saved.get("meta_slow_bars",          self.meta_slow_bars)
            self.vol_window              = saved.get("vol_window",              self.vol_window)
            self.derivative_percentile   = saved.get("derivative_percentile",   self.derivative_percentile)
            self.allocation_dollars      = saved.get("allocation_dollars",      self.allocation_dollars)
            self._uup.restore(saved.get("uup",  {}), _INIT_DERIV_UUP)
            self._tlt.restore(saved.get("tlt",  {}), _INIT_DERIV_TLT)
            self._rinf.restore(saved.get("rinf", {}), _INIT_DERIV_RINF)
            logger.info(
                f"Restored: holding={self._holding_gld}, "
                f"prior_regime={self._regime_at_prior_close}, "
                f"trades={self._trade_count}"
            )

        if self.portfolio:
            actual = self._current_gld_shares()
            if actual > 0 and not self._holding_gld:
                logger.info(f"Reconcile: found {actual} GLD shares → holding_gld=True")
                self._holding_gld = True
            elif actual == 0 and self._holding_gld:
                logger.info("Reconcile: no GLD in portfolio → holding_gld=False")
                self._holding_gld = False

        self._warm_up_from_history()

        # Subscribe to live 5-min bars via keepUpToDate=True.
        # IB delivers backfill bars (historicalData) then live updates
        # (historicalDataUpdate) through the same on_bar callback.
        self._live_bar_req_ids: Dict[str, Optional[int]] = {}
        for symbol, cb in [
            ("GLD",  lambda b, s="GLD":  self._on_live_bar(s, b)),
            ("UUP",  lambda b, s="UUP":  self._on_live_bar(s, b)),
            ("TLT",  lambda b, s="TLT":  self._on_live_bar(s, b)),
            ("RINF", lambda b, s="RINF": self._on_live_bar(s, b)),
        ]:
            req_id = self.subscribe_live_bars(
                contract=ContractBuilder.etf(symbol),
                on_bar=cb,
            )
            self._live_bar_req_ids[symbol] = req_id

        logger.info(
            f"Started GLD/USD swap v{self.VERSION}: "
            f"fast={self.fast_bars} slow={self.slow_bars}, "
            f"meta={self.meta_fast_bars}/{self.meta_slow_bars}, "
            f"alloc=${self.allocation_dollars:,.0f}, "
            f"holding={self._holding_gld}, prior_regime={self._regime_at_prior_close}"
        )
        return True

    def stop(self) -> bool:
        for req_id in getattr(self, "_live_bar_req_ids", {}).values():
            if req_id is not None:
                self.cancel_live_bars(req_id)
        self.unsubscribe_all()
        self._save_state()
        return True

    def freeze(self) -> bool:
        self._save_state()
        return True

    def resume(self) -> bool:
        return True

    def _save_state(self) -> None:
        self.save_state({
            "holding_gld":           self._holding_gld,
            "regime_at_prior_close": self._regime_at_prior_close,
            "trade_count":           self._trade_count,
            "overnight_holds":       self._overnight_holds,
            "intraday_holds":        self._intraday_holds,
            "last_trade_time":       self._last_trade_time,
            "fast_bars":             self.fast_bars,
            "slow_bars":             self.slow_bars,
            "meta_fast_bars":        self.meta_fast_bars,
            "meta_slow_bars":        self.meta_slow_bars,
            "vol_window":            self.vol_window,
            "derivative_percentile": self.derivative_percentile,
            "allocation_dollars":    self.allocation_dollars,
            "uup":                   self._uup.save(),
            "tlt":                   self._tlt.save(),
            "rinf":                  self._rinf.save(),
        })

    # =========================================================================
    # HISTORICAL WARM-UP
    # =========================================================================

    def _warm_up_from_history(self) -> None:
        """
        Fetch 2 days of 5-min bars for UUP, TLT, and RINF (plus GLD for meta)
        to seed all SMAs and derivative estimators on startup.

        Without warm-up the signal is dark for meta_slow_bars × 5 min = 300 min
        (5 hours at defaults) after every restart.
        """
        if not self.portfolio:
            logger.info("Warm-up skipped — no portfolio (test mode)")
            return

        last_close_regime = REGIME_UNKNOWN
        _UTC = timezone.utc
        _now = datetime.now(_UTC)
        _start = _now - timedelta(days=2)

        for symbol, state, init_d in [
            ("UUP",  self._uup,  _INIT_DERIV_UUP),
            ("TLT",  self._tlt,  _INIT_DERIV_TLT),
            ("RINF", self._rinf, _INIT_DERIV_RINF),
        ]:
            bars = self.get_bars_cached(
                contract=ContractBuilder.etf(symbol),
                start_dt=_start,
                end_dt=_now,
                bar_size_setting="5 mins",
                what_to_show="TRADES",
                use_rth=True,
            )
            if not bars:
                logger.warning(f"Warm-up: no historical data for {symbol}")
                continue

            for b in bars:
                state.push(
                    float(b.close),
                    self.vol_window,
                    self.derivative_percentile,
                    self.fast_bars,
                    self.slow_bars,
                )

            logger.info(
                f"Warm-up {symbol}: {len(bars)} bars, "
                f"price={state.price:.4f}, deriv={state.derivative:.5f}, "
                f"fast={state.fast_sma:.4f}, slow={state.slow_sma:.4f}"
            )

        # GLD meta warm-up
        gld_bars = self.get_bars_cached(
            contract=ContractBuilder.etf("GLD"),
            start_dt=_start,
            end_dt=_now,
            bar_size_setting="5 mins",
            what_to_show="TRADES",
            use_rth=True,
        )
        for b in gld_bars or []:
            self._push_gld_meta(float(b.close))
        if gld_bars:
            self._gld_price = float(gld_bars[-1].close)

        # Derive composite regime and detect last 15:45 bar regime
        self._recompute_regime()

        # Scan UUP bars for prior-close regime (if not already saved)
        if self._regime_at_prior_close == REGIME_UNKNOWN:
            uup_bars = self.get_bars_cached(
                contract=ContractBuilder.etf("UUP"),
                start_dt=_start,
                end_dt=_now,
                bar_size_setting="5 mins",
                what_to_show="TRADES",
                use_rth=True,
            )
            for b in uup_bars or []:
                try:
                    ts = datetime.strptime(b.date[:8] + " " + b.date[9:17], "%Y%m%d %H:%M:%S")
                except (ValueError, AttributeError):
                    continue
                if ts.hour == _CLOSE_HOUR and ts.minute == _CLOSE_MIN:
                    last_close_regime = self._regime  # regime at that 15:45 bar

            if last_close_regime != REGIME_UNKNOWN:
                self._regime_at_prior_close = last_close_regime
                logger.info(f"Warm-up derived prior-close regime: {last_close_regime}")

        logger.info(
            f"Warm-up complete: regime={self._regime}, "
            f"prior_close={self._regime_at_prior_close}, "
            f"gld_uptrend={self._gld_in_uptrend}, "
            f"factors={self._last_factors}"
        )

    # =========================================================================
    # MARKET DATA
    # =========================================================================

    def _on_live_bar(self, symbol: str, bar) -> None:
        """Process one 5-min bar for the given symbol.

        Called by subscribe_live_bars for both backfill and live updates.

        IB bar date format for 5-min bars: "20260318 09:30:00" (legacy) or
        "20260318-09:30:00" (new API, UTC endDateTime).  We parse chars 0-7
        as date and 9-16 as time, which works for both separators.
        """
        close = float(bar.close)

        try:
            ts = datetime.strptime(bar.date[:8] + " " + bar.date[9:17], "%Y%m%d %H:%M:%S")
        except (ValueError, AttributeError):
            ts = None

        if symbol == "GLD":
            self._gld_price = close
            self._push_gld_meta(close)
            self._recompute_regime()
            if ts:
                self._handle_session_event(ts)
        elif symbol == "UUP":
            self._uup.push(close, self.vol_window, self.derivative_percentile,
                           self.fast_bars, self.slow_bars)
            self._recompute_regime()
        elif symbol == "TLT":
            self._tlt.push(close, self.vol_window, self.derivative_percentile,
                           self.fast_bars, self.slow_bars)
            self._recompute_regime()
        elif symbol == "RINF":
            self._rinf.push(close, self.vol_window, self.derivative_percentile,
                            self.fast_bars, self.slow_bars)
            self._recompute_regime()

    def _push_gld_meta(self, close: float) -> None:
        """Feed GLD close into the meta-signal SMA (no smoother — structural trend only)."""
        self._gld_meta_closes.append(close)
        if len(self._gld_meta_closes) >= self.meta_slow_bars:
            cl = list(self._gld_meta_closes)
            self._gld_meta_fast = sum(cl[-self.meta_fast_bars:]) / self.meta_fast_bars
            self._gld_meta_slow = sum(cl[-self.meta_slow_bars:]) / self.meta_slow_bars
            self._gld_in_uptrend = self._gld_meta_fast > self._gld_meta_slow

    def _handle_session_event(self, ts: datetime) -> None:
        if ts.hour == _OPEN_HOUR  and ts.minute == _OPEN_MIN:
            self._on_market_open(ts)
        elif ts.hour == _CLOSE_HOUR and ts.minute == _CLOSE_MIN:
            self._on_market_close(ts)

    # =========================================================================
    # COMPOSITE REGIME
    # =========================================================================

    def _recompute_regime(self) -> None:
        """
        Compute composite gold/cash regime from UUP, TLT, RINF, and GLD meta.

        Requires UUP to be warmed up (primary signal). Falls back gracefully
        as TLT, RINF, and meta warm up over the first session.
        """
        if not self._uup.warmed_up(self.slow_bars):
            return   # primary signal not ready

        uup_gold = self._uup.fast_sma < self._uup.slow_sma   # USD weakening

        if self._tlt.warmed_up(self.slow_bars):
            tlt_gold = self._tlt.fast_sma > self._tlt.slow_sma   # nominal rates falling

            if (self._gld_in_uptrend and self._rinf.warmed_up(self.slow_bars)):
                # GLD structural uptrend: extend with RINF (stagflation scenario)
                rinf_gold = self._rinf.fast_sma > self._rinf.slow_sma
                gold = uup_gold and (tlt_gold or rinf_gold)
                mode = "UUP+TLT|RINF(meta)"
            else:
                # No GLD uptrend (or RINF not warmed): UUP AND TLT only
                gold = uup_gold and tlt_gold
                mode = "UUP+TLT"
        else:
            # TLT not warmed up yet — fall back to UUP only
            gold = uup_gold
            mode = "UUP(fallback)"

        self._regime = REGIME_GOLD if gold else REGIME_CASH
        self._last_factors = {
            "mode":         mode,
            "uup_fast":     round(self._uup.fast_sma,  4),
            "uup_slow":     round(self._uup.slow_sma,  4),
            "tlt_fast":     round(self._tlt.fast_sma,  4),
            "tlt_slow":     round(self._tlt.slow_sma,  4),
            "rinf_fast":    round(self._rinf.fast_sma, 4),
            "rinf_slow":    round(self._rinf.slow_sma, 4),
            "gld_uptrend":  self._gld_in_uptrend,
            "gld_meta_fast": round(self._gld_meta_fast, 2),
            "gld_meta_slow": round(self._gld_meta_slow, 2),
            "regime":       self._regime,
        }

    # =========================================================================
    # SESSION DECISIONS
    # =========================================================================

    def _on_market_open(self, ts: datetime) -> None:
        """
        09:30 — overnight position matures.
        Sell if prior-close regime was cash; hold through day if gold.
        """
        if not self._holding_gld:
            return

        if self._regime_at_prior_close == REGIME_CASH:
            qty = self._current_gld_shares() or int(self.allocation_dollars / self._gld_price)
            if qty > 0:
                self._emit_sell(
                    qty=qty,
                    reason=(
                        f"Open {ts.date()}: composite CASH — MKT sell; "
                        f"sit out intraday, re-buy via MOC at 15:50. "
                        f"factors={self._last_factors}"
                    ),
                )
                self._holding_gld = False
        else:
            self._intraday_holds += 1
            logger.info(
                f"Open {ts.date()}: holding GLD intraday "
                f"(regime={self._regime_at_prior_close}, factors={self._last_factors})"
            )

    def _on_market_close(self, ts: datetime) -> None:
        """
        15:45 bar (completes ~15:50) — inside NYSE ARCA MOC submission cutoff.
        Save current composite regime for tomorrow's open decision.
        Place MOC order to buy GLD overnight if not already long.
        """
        self._recompute_regime()
        self._regime_at_prior_close = self._regime

        if not self._holding_gld:
            shares = int(self.allocation_dollars / self._gld_price) if self._gld_price > 0 else 0
            if shares > 0:
                self._place_moc_buy(
                    shares,
                    reason=(
                        f"Close {ts.date()}: MOC overnight entry "
                        f"(tomorrow regime={self._regime}, factors={self._last_factors})"
                    ),
                )
                self._holding_gld  = True
                self._overnight_holds += 1
        else:
            self._overnight_holds += 1
            logger.info(
                f"Close {ts.date()}: rolling overnight (no order), "
                f"regime={self._regime}, factors={self._last_factors}"
            )

    # =========================================================================
    # ORDER EXECUTION
    # =========================================================================

    def _place_moc_buy(self, shares: int, reason: str) -> None:
        self._trade_count    += 1
        self._last_trade_time = datetime.now().isoformat()

        self.publish(
            "gld_usd_swap_signals",
            {
                "timestamp":  self._last_trade_time,
                "action":     "BUY",
                "order_type": "MOC",
                "quantity":   shares,
                "gld_price":  self._gld_price,
                "factors":    self._last_factors,
                "reason":     reason,
            },
            message_type="signal",
        )

        if not self.portfolio:
            logger.info(f"[no portfolio] MOC BUY {shares} GLD — {reason}")
            return

        contract               = ContractBuilder.etf("GLD")
        order                  = IbOrder()
        order.action           = "BUY"
        order.totalQuantity    = shares
        order.orderType        = "MOC"
        order.transmit         = True

        oid = self.portfolio.place_order_custom(contract, order)
        if oid is not None:
            self.register_order(oid)
            logger.info(f"MOC BUY {shares} GLD (order_id={oid}) — {reason}")
        else:
            logger.error(f"Failed to place MOC BUY {shares} GLD — {reason}")

    def _emit_sell(self, qty: int, reason: str) -> None:
        signal = TradeSignal(
            symbol="GLD",
            action="SELL",
            quantity=Decimal(str(qty)),
            reason=reason,
            urgency="Urgent",
        )
        self._pending_signals.append(signal)
        self._trade_count    += 1
        self._last_trade_time = datetime.now().isoformat()

        self.publish(
            "gld_usd_swap_signals",
            {
                "timestamp":  self._last_trade_time,
                "action":     "SELL",
                "order_type": "MKT",
                "quantity":   qty,
                "gld_price":  self._gld_price,
                "factors":    self._last_factors,
                "reason":     reason,
            },
            message_type="signal",
        )
        logger.info(f"MKT SELL {qty} GLD @ ~${self._gld_price:.2f} — {reason}")

    def _current_gld_shares(self) -> int:
        if self.portfolio:
            for pos in self.portfolio.positions:
                if pos.symbol == "GLD":
                    return max(0, int(pos.quantity))
        return 0

    # =========================================================================
    # SIGNALS
    # =========================================================================

    def calculate_signals(self) -> List[TradeSignal]:
        signals, self._pending_signals = self._pending_signals[:], []
        return signals

    # =========================================================================
    # REQUESTS / CLI
    # =========================================================================

    def handle_request(self, request_type: str, payload: Dict) -> Dict:
        if request_type == "get_status":
            return {
                "success": True,
                "data": {
                    "version":               self.VERSION,
                    "holding_gld":           self._holding_gld,
                    "regime":                self._regime,
                    "regime_at_prior_close": self._regime_at_prior_close,
                    "signal_factors":        self._last_factors,
                    "gld_price":             self._gld_price,
                    "uup_price":             self._uup.price,
                    "tlt_price":             self._tlt.price,
                    "rinf_price":            self._rinf.price,
                    "gld_in_uptrend":        self._gld_in_uptrend,
                    "gld_meta_fast":         round(self._gld_meta_fast, 2),
                    "gld_meta_slow":         round(self._gld_meta_slow, 2),
                    "uup_warmed_up":         self._uup.warmed_up(self.slow_bars),
                    "tlt_warmed_up":         self._tlt.warmed_up(self.slow_bars),
                    "rinf_warmed_up":        self._rinf.warmed_up(self.slow_bars),
                    "meta_warmed_up":        len(self._gld_meta_closes) >= self.meta_slow_bars,
                    "uup_bars":              len(self._uup.closes),
                    "tlt_bars":              len(self._tlt.closes),
                    "rinf_bars":             len(self._rinf.closes),
                    "uup_derivative":        round(self._uup.derivative,  5),
                    "tlt_derivative":        round(self._tlt.derivative,  5),
                    "rinf_derivative":       round(self._rinf.derivative, 5),
                    "trade_count":           self._trade_count,
                    "overnight_holds":       self._overnight_holds,
                    "intraday_holds":        self._intraday_holds,
                    "last_trade_time":       self._last_trade_time,
                },
            }

        if request_type == "get_parameters":
            return {
                "success": True,
                "data": {
                    "fast_bars":             self.fast_bars,
                    "slow_bars":             self.slow_bars,
                    "meta_fast_bars":        self.meta_fast_bars,
                    "meta_slow_bars":        self.meta_slow_bars,
                    "vol_window":            self.vol_window,
                    "derivative_percentile": self.derivative_percentile,
                    "allocation_dollars":    self.allocation_dollars,
                },
            }

        if request_type == "set_parameter":
            key, value = payload.get("key"), payload.get("value")
            if not key or value is None:
                return {"success": False, "message": "Requires 'key' and 'value'"}
            return self._set_parameter(key, value)

        if request_type == "force_regime":
            regime = payload.get("regime")
            if regime not in (REGIME_GOLD, REGIME_CASH, REGIME_UNKNOWN):
                return {"success": False, "message": f"Invalid regime '{regime}'"}
            self._regime_at_prior_close = regime
            logger.info(f"Prior-close regime forced to: {regime}")
            return {"success": True, "message": f"regime_at_prior_close={regime}"}

        return {"success": False, "message": f"Unknown request: {request_type}"}

    def _set_parameter(self, key: str, value) -> Dict:
        try:
            if key == "fast_bars":
                v = max(1, int(value))
                if v >= self.slow_bars:
                    return {"success": False, "message": "fast_bars must be < slow_bars"}
                self.fast_bars = v
            elif key == "slow_bars":
                self.slow_bars = max(self.fast_bars + 1, int(value))
                for state in (self._uup, self._tlt, self._rinf):
                    state.closes = deque(list(state.closes),
                                        maxlen=max(80, self.slow_bars + 10))
            elif key == "meta_fast_bars":
                v = max(1, int(value))
                if v >= self.meta_slow_bars:
                    return {"success": False, "message": "meta_fast_bars must be < meta_slow_bars"}
                self.meta_fast_bars = v
            elif key == "meta_slow_bars":
                self.meta_slow_bars = max(self.meta_fast_bars + 1, int(value))
                self._gld_meta_closes = deque(list(self._gld_meta_closes),
                                              maxlen=max(80, self.meta_slow_bars + 10))
            elif key == "vol_window":
                self.vol_window = max(5, int(value))
            elif key == "derivative_percentile":
                self.derivative_percentile = max(1, min(99, int(value)))
            elif key == "allocation_dollars":
                self.allocation_dollars = max(0.0, float(value))
            else:
                return {"success": False, "message": f"Unknown parameter: {key}"}
        except (TypeError, ValueError) as exc:
            return {"success": False, "message": f"Invalid value for {key}: {exc}"}
        logger.info(f"Parameter updated: {key}={value}")
        return {"success": True, "message": f"Set {key}={value}"}

    def cli_help(self) -> str:
        return (
            "gld_usd_swap v3 commands:\n"
            "  plugin request gld_usd_swap get_status {}\n"
            "  plugin request gld_usd_swap get_parameters {}\n"
            "  plugin request gld_usd_swap set_parameter '{\"key\": \"fast_bars\",             \"value\": 5}'\n"
            "  plugin request gld_usd_swap set_parameter '{\"key\": \"slow_bars\",             \"value\": 20}'\n"
            "  plugin request gld_usd_swap set_parameter '{\"key\": \"meta_fast_bars\",        \"value\": 20}'\n"
            "  plugin request gld_usd_swap set_parameter '{\"key\": \"meta_slow_bars\",        \"value\": 60}'\n"
            "  plugin request gld_usd_swap set_parameter '{\"key\": \"vol_window\",            \"value\": 20}'\n"
            "  plugin request gld_usd_swap set_parameter '{\"key\": \"derivative_percentile\", \"value\": 50}'\n"
            "  plugin request gld_usd_swap set_parameter '{\"key\": \"allocation_dollars\",    \"value\": 10000}'\n"
            "  plugin request gld_usd_swap force_regime '{\"regime\": \"gold\"}'\n"
            "  plugin request gld_usd_swap force_regime '{\"regime\": \"cash\"}'\n"
        )
