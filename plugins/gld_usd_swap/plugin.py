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

─────────────────────────────────────────────────────────────────────────────
RESET-CADENCE OVERLAY (opt-in, disabled by default)

Backtested in volomom/backtest_reset_cadence_short.py: a walk-forward
kill-switch on the strategy's OWN trailing performance. Each trading day at
the close, if the strategy's realized daily-NAV return over the trailing
`reset_lookback_days` compounds below `reset_threshold`, the strategy is
judged in a real drawdown and pauses for `reset_cooldown_days` trading days.
During the pause, the overnight leg is INVERTED — short GLD overnight
instead of long — while the intraday leg stays flat regardless of regime
(the "short overnight only" variant: best drawdown control of the four
short/cash variants tested, and the most literal reading of "invert the
hold", since the overnight leg is the only unconditional position the base
strategy ever takes).

This requires a fresh short-open at every paused close and a fresh cover at
every paused open (not a single continuous short across the cooldown),
since staying flat intraday is exactly the point. Real IBKR GLD borrow
fees are immaterial at that cadence (backtest_reset_cadence_borrow_fee.py:
~0.01 Sharpe impact), but per-order COMMISSION is not: cash-during-pause
needs only ~2 orders total per triggered window, while this variant needs
two orders every paused day (cover + re-short), and IBKR's GLD commission
($0.0035/share, $1.00/order minimum) hits the $1 floor on every single one
of them at this position size — 830 orders / $830 total over the 2012-2026
backtest (backtest_reset_cadence_commission.py). Net of that cost,
full-history Sharpe drops from 1.47 to 1.32 — barely ahead of just sitting
in cash (1.30), where before commissions were modeled it looked like a
clear win. Worse, the cost concentrates specifically in bear markets, not
uniformly across time: 25.2% of days are paused within 2012-2016 alone
vs. 11.6% over the full history, since the trigger is by construction
responding to the drawdown that defines that window — and against that
window's thin gross edge (+0.9%), the fixed per-order cost alone drags it
to -4.8%. Worth reconfirming actual paper/live fill commissions before
trusting this variant's edge over plain cash-during-pause.

Locked-in default combo from an 80-combo sweep (sweep_reset_cadence_short.py):
  reset_lookback_days = 42, reset_cooldown_days = 126, reset_threshold = -8%
  -> full-history Sharpe 0.80->1.44 gross, 1.32 net of commission
  -> 2012-2016 loss -33.5%->-0.3% gross, -5.9% net of commission

Position sign convention: HoldingPosition.quantity is a plain signed float
with no non-negative guard in add_position() (remove_position() is the only
method that rejects going negative, and it's simply not used for the short
leg) — so going short is just add_position("GLD", -shares, ...), and NAV
(cash + quantity*price) nets out correctly for a short with zero special
casing. A single sell order sized as (current long shares + target short
shares) correctly crosses through zero in one fill when the pause triggers
while still holding long.
"""

import bisect
import logging
import time
from collections import deque
from datetime import datetime, timedelta, timezone, time as dt_time
from zoneinfo import ZoneInfo
from itertools import groupby
from typing import Dict, List, Optional, Tuple

from ibapi.order import Order as IbOrder

from ib.contract_builder import ContractBuilder
from plugins.base import PluginBase, PluginState, TradeSignal

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

_NY_TZ = ZoneInfo("America/New_York")

# Declared to PluginExecutive.aggregate_trading_windows() via trading_hours()
# below: the wall-clock span this plugin actually needs its four feeds and
# connection alive for. A little before the open (so the 09:30 decision bar
# and its inputs are already warm) through a little after the MOC submission
# cutoff (so the fill confirmation isn't missed).
_TRADING_WINDOW_START = dt_time(9, 15)
_TRADING_WINDOW_END   = dt_time(16, 0)

# Wall-clock validity windows (ET). A session decision is only meaningful
# while it can still act on THIS session: the open sell shortly after the
# open, the MOC entry before the ~15:50 submission cutoff. A session bar
# arriving outside its window is stale — observed live: IB re-delivers the
# day's 15:45 bar through the live-update path on an after-hours
# (re)subscribe, which sailed past the backfill/live gate and placed an MOC
# at 19:53 ET that IB queued for the NEXT day's close. The bar-level gates
# (is_live + high-water-mark) dedupe and filter replay; this wall-clock
# check is the absolute bound that makes late firing impossible.
_OPEN_WINDOW_ET  = ((9, 30),  (9, 45))
_CLOSE_WINDOW_ET = ((15, 45), (15, 55))

# Alert when an order has had no fill/terminal status for this long. (The
# watchdog plugin runs an independent wall-clock check; this in-plugin check
# only advances on bar callbacks, so it mainly covers intraday sells.)
_PENDING_ORDER_ALERT_SECONDS = 1800

# Alert after this many consecutive unparseable bar timestamps — session
# decisions match on the parsed clock time, so a format/timezone drift would
# otherwise silently disable all trading (including exits) with zero errors.
_PARSE_FAILURE_ALERT_THRESHOLD = 10

# After (re)subscribing, IB replays a burst of already-completed backfill bars
# (via historicalData) before live updates start (via historicalDataUpdate).
# Session decisions must never fire from a replayed bar — a backfilled
# 09:30/15:45 bar on startup/resume would place a stale real order.  The
# subscription delivers backfill and live bars through separate callbacks
# (subscribe_live_bars on_bar vs on_live_bar), so gating is positive
# identification, not a timing heuristic: only bars arriving through the
# live-update callback, with a timestamp strictly newer than any bar seen,
# may trigger session decisions.


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
        self.smoother:      _StreamingTriangleTooth = _StreamingTriangleTooth(init_derivative)
        self.derivative:    float = init_derivative
        self.closes:        deque = deque(maxlen=maxlen_closes)
        self.abs_moves:     deque = deque(maxlen=maxlen_moves)
        self._sorted_moves: list  = []   # sorted mirror of abs_moves for O(log n) percentile
        self.prev_close:    float = 0.0
        self.price:         float = 0.0
        self.fast_sma:      float = 0.0
        self.slow_sma:      float = 0.0

    def push(self, close: float, vol_window: int, percentile: int,
             fast: int, slow: int, change_thresh: float = 0.05) -> None:
        """Feed one bar. Updates closes, SMAs, and adapts derivative."""
        if self.prev_close > 0:
            move = abs(close - self.prev_close)
            if len(self.abs_moves) == self.abs_moves.maxlen:
                # Evict oldest before the deque drops it
                evicted = self.abs_moves[0]
                del self._sorted_moves[bisect.bisect_left(self._sorted_moves, evicted)]
            self.abs_moves.append(move)
            bisect.insort(self._sorted_moves, move)
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
        n = len(self._sorted_moves)
        if n < 5:
            return
        idx       = max(0, int(n * percentile / 100) - 1)
        new_deriv = self._sorted_moves[idx]
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

        # --- reset-cadence overlay (opt-in; see module docstring) ---
        self.reset_cadence_enabled: bool  = False
        self.reset_lookback_days:   int   = 42
        self.reset_cooldown_days:   int   = 126
        self.reset_threshold:       float = -0.08

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

        # --- reset-cadence overlay state ---
        self._short_gld:                 bool          = False
        self._reset_cooldown_remaining:  int           = 0
        self._daily_returns:             deque         = deque(maxlen=self.reset_lookback_days)
        self._prior_nav:                 Optional[float] = None

        # --- live-bar gating (hazard: backfill replay must not fire orders) ---
        # Session events fire only for bars delivered through the live-update
        # callback AND strictly newer than any bar seen (the high-water-mark
        # also dedupes repeated in-progress updates of a forming bar).
        self._hwm_ts:              Optional[datetime] = None   # high-water-mark bar time
        self._open_fired_date:     Optional[object]   = None   # date the open decision ran
        self._close_fired_date:    Optional[object]   = None   # date the close decision ran

        # True when state restore found orders still in flight from a previous
        # session (e.g. crash between MOC placement and the 16:00 fill).  Makes
        # startup reconciliation conservative so the plugin can never place a
        # duplicate overnight buy for an unresolved order.
        self._restored_pending_buy: bool = False

        # --- pending order tracking (order_id → "BUY"/"SELL") ---
        self._pending_order_actions: Dict[int, str] = {}
        self._pending_order_placed_at: Dict[int, float] = {}  # order_id → time.time()
        self._pending_alerted: set = set()   # order_ids already alerted as stuck

        # --- bar-timestamp parse failure tracking (timezone/format drift) ---
        self._bar_parse_failures: int = 0

        # --- diagnostics ---
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
            self.reset_cadence_enabled   = saved.get("reset_cadence_enabled",   self.reset_cadence_enabled)
            self.reset_lookback_days     = saved.get("reset_lookback_days",     self.reset_lookback_days)
            self.reset_cooldown_days     = saved.get("reset_cooldown_days",     self.reset_cooldown_days)
            self.reset_threshold         = saved.get("reset_threshold",         self.reset_threshold)
            self._short_gld              = saved.get("short_gld",               False)
            self._reset_cooldown_remaining = saved.get("reset_cooldown_remaining", 0)
            self._daily_returns          = deque(saved.get("daily_returns", []),
                                                  maxlen=self.reset_lookback_days)
            self._prior_nav              = saved.get("prior_nav")
            self._uup.restore(saved.get("uup",  {}), _INIT_DERIV_UUP)
            self._tlt.restore(saved.get("tlt",  {}), _INIT_DERIV_TLT)
            self._rinf.restore(saved.get("rinf", {}), _INIT_DERIV_RINF)
            self._pending_order_actions = {
                int(oid): action
                for oid, action in saved.get("pending_orders", {}).items()
            }
            # Age restored orders from now — if they stay unresolved, the
            # stuck-order alert fires after the normal threshold.
            self._pending_order_placed_at = {
                oid: time.time() for oid in self._pending_order_actions
            }
            logger.info(
                f"Restored: holding={self._holding_gld}, "
                f"prior_regime={self._regime_at_prior_close}, "
                f"trades={self._trade_count}"
            )

        if self._pending_order_actions:
            # Orders were in flight when the previous session ended (e.g. crash
            # between the 15:45 MOC placement and the 16:00 fill).  We cannot
            # attribute their fills after a restart, so fail safe:
            #   - a pending BUY is treated as holding (prevents a duplicate
            #     overnight buy at the next close; worst case is a missed trade)
            #   - the operator must verify the fill in TWS and, if it filled,
            #     transfer the GLD shares into this plugin's holdings.
            self._restored_pending_buy = (
                "BUY" in self._pending_order_actions.values()
            )
            logger.error(
                f"Restored {len(self._pending_order_actions)} unresolved order(s) "
                f"from previous session: {self._pending_order_actions} — verify "
                f"fills in TWS; if a BUY filled while the engine was down, "
                f"transfer the GLD shares into this plugin's holdings "
                f"(ibctl transfer position _unassigned {self.name} GLD <qty>)"
            )
            if self._restored_pending_buy and not self._holding_gld:
                self._holding_gld = True
                logger.warning(
                    "Unresolved BUY in flight → conservatively assuming "
                    "holding_gld=True until manually reconciled"
                )

        if self.portfolio:
            actual = self._current_gld_shares()
            if actual > 0 and not self._holding_gld:
                logger.info(f"Reconcile: found {actual} GLD shares → holding_gld=True")
                self._holding_gld = True
            elif actual == 0 and self._holding_gld and not self._restored_pending_buy:
                # Skipped when a BUY is unresolved: its shares may have filled
                # into the account (and been swept to _unassigned) without ever
                # reaching this plugin's holdings — flipping to False here would
                # trigger a duplicate overnight buy at the next close.
                logger.info("Reconcile: no GLD in portfolio → holding_gld=False")
                self._holding_gld = False

            # Signed reconciliation for the reset-cadence short leg — the plain
            # _current_gld_shares() clamps negative to zero, which would hide
            # an actual short position instead of detecting it.
            actual_signed = self._current_gld_shares_signed()
            if actual_signed < 0 and not self._short_gld:
                logger.info(f"Reconcile: found {actual_signed:.0f} GLD shares (short) → short_gld=True")
                self._short_gld = True
            elif actual_signed == 0 and self._short_gld:
                logger.info("Reconcile: no short GLD in portfolio → short_gld=False")
                self._short_gld = False

        self._warm_up_from_history()
        self._start_subscriptions()

        logger.info(
            f"Started GLD/USD swap v{self.VERSION}: "
            f"fast={self.fast_bars} slow={self.slow_bars}, "
            f"meta={self.meta_fast_bars}/{self.meta_slow_bars}, "
            f"alloc=${self.allocation_dollars:,.0f}, "
            f"holding={self._holding_gld}, prior_regime={self._regime_at_prior_close}"
        )
        return True

    def stop(self) -> bool:
        self._cancel_subscriptions()
        self._save_state()
        return True

    def freeze(self) -> bool:
        # Cancel subscriptions so no bars — and therefore no session decisions
        # or orders — can occur while frozen.  resume() re-subscribes.
        self._cancel_subscriptions()
        self._save_state()
        return True

    def resume(self) -> bool:
        # Cancel stale req_ids (no-op if the connection dropped) then
        # re-subscribe so live bars are guaranteed after any freeze.
        self._cancel_subscriptions()
        self._start_subscriptions()
        return True

    def trading_hours(self) -> List[Tuple[dt_time, dt_time]]:
        return [(_TRADING_WINDOW_START, _TRADING_WINDOW_END)]

    def on_reconnect(self) -> None:
        # keepUpToDate subscriptions are not restored by the connection
        # manager's stream recovery. Without this the plugin runs blind after
        # any TWS reconnect — holding a position with no bars, no session
        # decisions, and no errors logged.
        logger.warning("Reconnected — re-creating live bar subscriptions")
        self._cancel_subscriptions()
        self._start_subscriptions()

    def _start_subscriptions(self) -> None:
        """Subscribe to live 5-min bars for all four symbols.

        Backfill bars (historicalData replay) arrive via on_bar and only feed
        the signal state; live updates (historicalDataUpdate) arrive via
        on_live_bar and may additionally trigger session decisions.  This
        positive backfill/live separation is what guarantees a replayed
        09:30/15:45 bar can never fire a real order on startup/resume.

        UUP/TLT/RINF are signal-only (weight 0.0, never held) and thin enough
        that a TRADES-basis feed — which only produces a bar when the exchange
        prints an actual trade — goes stale for long stretches (observed live:
        the watchdog's stale_feed alert firing on these three on nearly every
        session, most persistently on RINF). MIDPOINT instead derives from the
        live bid/ask, so it updates continuously regardless of print activity.
        GLD stays on TRADES (the default) since it is the instrument actually
        filled and its signal should track real transaction prices.
        """
        self._live_bar_req_ids: Dict[str, Optional[int]] = {}
        for symbol in ("GLD", "UUP", "TLT", "RINF"):
            kwargs = {} if symbol == "GLD" else {"what_to_show": "MIDPOINT"}
            req_id = self.subscribe_live_bars(
                contract=ContractBuilder.etf(symbol),
                on_bar=lambda b, s=symbol: self._on_bar(s, b, is_live=False),
                on_live_bar=lambda b, s=symbol: self._on_bar(s, b, is_live=True),
                **kwargs,
            )
            self._live_bar_req_ids[symbol] = req_id

    def _cancel_subscriptions(self) -> None:
        for req_id in getattr(self, "_live_bar_req_ids", {}).values():
            if req_id is not None:
                self.cancel_live_bars(req_id)
        self._live_bar_req_ids = {}

    def get_state_for_save(self) -> dict:
        """The plugin's full persistable state.

        Also consumed by the executive's periodic auto-save. Implementing
        this is what keeps the executive from ever writing its generic
        stub over state.json (which would destroy the regime, counters,
        and the pending-order records that back crash recovery).
        """
        return {
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
            "reset_cadence_enabled": self.reset_cadence_enabled,
            "reset_lookback_days":   self.reset_lookback_days,
            "reset_cooldown_days":   self.reset_cooldown_days,
            "reset_threshold":       self.reset_threshold,
            "short_gld":             self._short_gld,
            "reset_cooldown_remaining": self._reset_cooldown_remaining,
            "daily_returns":         list(self._daily_returns),
            "prior_nav":             self._prior_nav,
            "uup":                   self._uup.save(),
            "tlt":                   self._tlt.save(),
            "rinf":                  self._rinf.save(),
            # In-flight orders survive a restart so an unresolved MOC buy can
            # never be silently forgotten (and duplicated) after a crash.
            "pending_orders":        {str(oid): action for oid, action
                                      in self._pending_order_actions.items()},
        }

    def _save_state(self) -> None:
        self.save_state(self.get_state_for_save())

    # =========================================================================
    # HISTORICAL WARM-UP
    # =========================================================================

    def _warm_up_from_history(self) -> None:
        """
        Fetch 2 days of 5-min bars for UUP, TLT, RINF, and GLD to seed all
        SMAs and derivative estimators on startup.

        Bars are replayed in chronological order across all symbols so that
        _recompute_regime() sees a consistent multi-signal snapshot at each
        bar time.  The regime at the last 15:45 bar is captured as the
        prior-close regime.
        """
        if not self.portfolio:
            logger.info("Warm-up skipped — no portfolio (test mode)")
            return

        _UTC = timezone.utc
        _now = datetime.now(_UTC)
        _start = _now - timedelta(days=2)

        # --- fetch all symbols upfront ---
        signal_configs = [
            ("UUP",  self._uup),
            ("TLT",  self._tlt),
            ("RINF", self._rinf),
        ]
        bars_by_symbol: Dict[str, list] = {}
        for symbol, _ in signal_configs + [("GLD", None)]:
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
            bars_by_symbol[symbol] = bars or []

        # --- merge into a single chronological stream ---
        merged: List[tuple] = []
        for symbol, bars in bars_by_symbol.items():
            for b in bars:
                merged.append((b.date, symbol, float(b.close)))
        merged.sort(key=lambda x: x[0])

        # --- replay in time order, capturing regime at each 15:45 bar ---
        last_close_regime = REGIME_UNKNOWN
        for ts_str, group in groupby(merged, key=lambda x: x[0]):
            for _, symbol, close in group:
                if symbol == "UUP":
                    self._uup.push(close, self.vol_window, self.derivative_percentile,
                                   self.fast_bars, self.slow_bars)
                elif symbol == "TLT":
                    self._tlt.push(close, self.vol_window, self.derivative_percentile,
                                   self.fast_bars, self.slow_bars)
                elif symbol == "RINF":
                    self._rinf.push(close, self.vol_window, self.derivative_percentile,
                                    self.fast_bars, self.slow_bars)
                elif symbol == "GLD":
                    self._push_gld_meta(close)
                    self._gld_price = close

            self._recompute_regime()

            try:
                ts = datetime.strptime(ts_str[:8] + " " + ts_str[9:17], "%Y%m%d %H:%M:%S")
                if (ts.hour == _CLOSE_HOUR and ts.minute == _CLOSE_MIN
                        and self._regime != REGIME_UNKNOWN):
                    last_close_regime = self._regime
            except (ValueError, TypeError, AttributeError):
                pass

        # --- log per-symbol stats ---
        for symbol, state in signal_configs:
            bars = bars_by_symbol[symbol]
            if bars:
                logger.info(
                    f"Warm-up {symbol}: {len(bars)} bars, "
                    f"price={state.price:.4f}, deriv={state.derivative:.5f}, "
                    f"fast={state.fast_sma:.4f}, slow={state.slow_sma:.4f}"
                )

        if self._regime_at_prior_close == REGIME_UNKNOWN and last_close_regime != REGIME_UNKNOWN:
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

    def _on_bar(self, symbol: str, bar, is_live: bool = False) -> None:
        """Process one 5-min bar for the given symbol.

        Called by subscribe_live_bars: backfill bars arrive with is_live=False
        (historicalData replay), live updates with is_live=True
        (historicalDataUpdate).  All bars feed the signal state; only live
        bars may trigger session decisions.

        IB bar date format for 5-min bars: "20260318 09:30:00" (legacy) or
        "20260318-09:30:00" (new API, UTC endDateTime).  We parse chars 0-7
        as date and 9-16 as time, which works for both separators.
        """
        if self._state == PluginState.FROZEN:
            # Backstop: freeze() cancels subscriptions, but an in-flight
            # callback must never process data or trade while frozen.
            return

        close = float(bar.close)

        try:
            ts = datetime.strptime(bar.date[:8] + " " + bar.date[9:17], "%Y%m%d %H:%M:%S")
        except (ValueError, TypeError, AttributeError):
            ts = None

        # Session decisions match on the parsed clock time; a bar-date format
        # or timezone drift would silently disable all trading (incl. exits).
        if ts is None:
            self._bar_parse_failures += 1
            if self._bar_parse_failures == _PARSE_FAILURE_ALERT_THRESHOLD:
                self._alert(
                    "bar_parse_failure",
                    f"{self._bar_parse_failures} consecutive unparseable bar "
                    f"timestamps (last: {getattr(bar, 'date', None)!r}) — "
                    f"session decisions CANNOT fire; check TWS date format "
                    f"and timezone",
                )
        else:
            self._bar_parse_failures = 0

        if is_live:
            self._check_pending_order_age()

        if symbol == "GLD":
            self._gld_price = close
            self._push_gld_meta(close)
            self._recompute_regime()
            if ts and self._is_live_bar(ts, is_live):
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

    def _is_live_bar(self, ts: datetime, is_live: bool) -> bool:
        """Gate session decisions to genuinely-new bars from the live callback.

        Two hazards this blocks, both of which would otherwise place real orders:
          1. Startup/resume backfill — subscribe_live_bars replays already-completed
             bars (incl. today's 09:30/15:45). Those arrive with is_live=False
             (historicalData vs historicalDataUpdate) and never fire decisions.
          2. Repeated in-progress updates — historicalDataUpdate fires multiple times
             for the same forming bar (same timestamp). Only the first, strictly newer
             than any bar seen, is treated as new.

        The high-water-mark advances on every bar, including backfill, so the
        forming bar already replayed at subscribe time can't fire again when its
        live updates start arriving.
        """
        is_new = self._hwm_ts is None or ts > self._hwm_ts
        if is_new:
            self._hwm_ts = ts
        return is_live and is_new

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
            if self._session_decision_valid(ts, _OPEN_WINDOW_ET, "Open"):
                self._on_market_open(ts)
        elif ts.hour == _CLOSE_HOUR and ts.minute == _CLOSE_MIN:
            if self._session_decision_valid(ts, _CLOSE_WINDOW_ET, "Close"):
                self._on_market_close(ts)

    def _now_ny(self) -> datetime:
        """Current wall-clock time in New York (overridable in tests)."""
        return datetime.now(_NY_TZ)

    def _session_decision_valid(self, ts: datetime, window, label: str) -> bool:
        """A session decision must be for TODAY's bar and executed while it
        can still act on this session (see _OPEN/_CLOSE_WINDOW_ET). Refusing
        outside the window turns a stale re-delivered session bar (e.g. on an
        after-hours restart) into a logged no-op instead of a next-session
        order."""
        now = self._now_ny()
        (sh, sm), (eh, em) = window
        minutes = now.hour * 60 + now.minute
        ok = (ts.date() == now.date()
              and sh * 60 + sm <= minutes < eh * 60 + em)
        if not ok:
            logger.warning(
                f"{label} decision for bar {ts} refused — wall clock "
                f"{now:%Y-%m-%d %H:%M %Z} is outside the decision's validity "
                f"window ({sh:02d}:{sm:02d}–{eh:02d}:{em:02d} ET). Stale "
                f"session bar (e.g. re-delivered on an after-hours restart); "
                f"acting now would produce a next-session order."
            )
        return ok

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
        Normal: sell if prior-close regime was cash; hold through day if gold.
        Reset-cadence pause: cover the overnight short regardless of regime;
        stay flat intraday (see module docstring).
        """
        if self._open_fired_date == ts.date():
            return   # already decided this session (guard against repeated bars)

        if self.reset_cadence_enabled and self._reset_cooldown_remaining > 0:
            self._open_fired_date = ts.date()
            if self._short_gld:
                qty = -self._current_gld_shares_signed()   # positive shares to buy back
                if qty > 0:
                    self._place_cover_buy(
                        int(qty),
                        reason=(
                            f"Open {ts.date()}: reset-cadence pause "
                            f"({self._reset_cooldown_remaining}d remaining) — "
                            f"MKT cover short, sit out intraday"
                        ),
                    )
                else:
                    logger.warning(
                        f"Open {ts.date()}: reset-cadence pause active but plugin "
                        f"holds no short GLD to cover (short_gld flag was "
                        f"{self._short_gld})"
                    )
            self._reset_cooldown_remaining = max(0, self._reset_cooldown_remaining - 1)
            if self._reset_cooldown_remaining == 0:
                logger.info(
                    f"Open {ts.date()}: reset-cadence pause ended — "
                    f"resuming normal long/cash operation"
                )
            self._save_state()
            return

        if not self._holding_gld:
            return
        self._open_fired_date = ts.date()

        if self._regime_at_prior_close == REGIME_CASH:
            # Sell only this plugin's own GLD slice — never account-wide GLD that
            # may belong to other plugins or unrelated activity (hazard #2).
            qty = self._current_gld_shares()
            if qty > 0:
                self._emit_sell(
                    qty=qty,
                    reason=(
                        f"Open {ts.date()}: composite CASH — MKT sell; "
                        f"sit out intraday, re-buy via MOC at 15:50. "
                        f"factors={self._last_factors}"
                    ),
                )
            else:
                logger.warning(
                    f"Open {ts.date()}: composite CASH but plugin holds 0 GLD in its "
                    f"holdings — skipping sell (holding_gld flag was {self._holding_gld})"
                )
        else:
            self._intraday_holds += 1
            logger.info(
                f"Open {ts.date()}: holding GLD intraday "
                f"(regime={self._regime_at_prior_close}, factors={self._last_factors})"
            )
        # Persist immediately: a crash before the next scheduled save must not
        # lose today's decision (pending order, counters).
        self._save_state()

    def _on_market_close(self, ts: datetime) -> None:
        """
        15:45 bar (completes ~15:50) — inside NYSE ARCA MOC submission cutoff.
        Save current composite regime for tomorrow's open decision.
        Normal: place MOC order to buy GLD overnight if not already long.
        Reset-cadence: evaluate the trigger on today's realized NAV return,
        then either enter/continue the short-overnight pause or buy long as
        usual (see module docstring).
        """
        if self._close_fired_date == ts.date():
            return   # already decided this session (guard against repeated bars)
        self._close_fired_date = ts.date()

        self._regime_at_prior_close = self._regime

        if self.reset_cadence_enabled:
            self._update_reset_cadence()

        if self.reset_cadence_enabled and self._reset_cooldown_remaining > 0:
            # Paused: short overnight instead of buying long. Sized as any
            # existing long shares (first pause evening only — every other
            # evening this is already flat from that morning's cover) plus
            # the short target; one order correctly crosses through zero in
            # a single fill (see _apply_signed_fill_to_holdings).
            target_short = int(self.allocation_dollars / self._gld_price) if self._gld_price > 0 else 0
            current_signed = self._current_gld_shares_signed()
            sell_qty = max(0, int(current_signed)) + target_short
            if sell_qty > 0:
                self._place_moc_short(
                    sell_qty,
                    reason=(
                        f"Close {ts.date()}: reset-cadence pause "
                        f"({self._reset_cooldown_remaining}d remaining) — "
                        f"MOC short overnight entry"
                    ),
                )
            elif self.portfolio and self._gld_price > 0:
                logger.error(
                    f"Close {ts.date()}: reset-cadence short skipped — "
                    f"computed 0 shares at ${self._gld_price:.2f}"
                )
            self._save_state()
            return

        if not self._holding_gld:
            budget = self.allocation_dollars
            if self.portfolio:
                # Real trading: never spend beyond the cash actually funded to
                # this plugin (via ibctl transfer). An unfunded plugin places
                # no orders instead of drawing on account-wide capital.
                cash = self.holdings.current_cash if self.holdings else 0.0
                budget = min(budget, cash)
            shares = int(budget / self._gld_price) if self._gld_price > 0 else 0
            if shares > 0:
                self._place_moc_buy(
                    shares,
                    reason=(
                        f"Close {ts.date()}: MOC overnight entry "
                        f"(tomorrow regime={self._regime}, factors={self._last_factors})"
                    ),
                )
            elif self.portfolio and self._gld_price > 0:
                logger.error(
                    f"Close {ts.date()}: MOC buy skipped — plugin cash "
                    f"${budget:,.2f} buys 0 shares at ${self._gld_price:.2f}. "
                    f"Fund the plugin: ibctl transfer cash _unassigned "
                    f"{self.name} {self.allocation_dollars:.0f} --confirm"
                )
        else:
            self._overnight_holds += 1
            logger.info(
                f"Close {ts.date()}: rolling overnight (no order), "
                f"regime={self._regime}, factors={self._last_factors}"
            )
        # Persist immediately: the regime saved here drives tomorrow's open
        # decision and must survive an overnight crash/restart.
        self._save_state()

    # =========================================================================
    # ORDER EXECUTION
    # =========================================================================

    def _place_moc_buy(self, shares: int, reason: str) -> None:
        self._trade_count    += 1
        self._last_trade_time = datetime.now(timezone.utc).isoformat()

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
            self._pending_order_actions[oid] = "BUY"
            self._pending_order_placed_at[oid] = time.time()
            self.register_order(oid)
            logger.info(f"MOC BUY {shares} GLD (order_id={oid}) — {reason}")
        else:
            logger.error(f"Failed to place MOC BUY {shares} GLD — {reason}")

    def _emit_sell(self, qty: int, reason: str) -> None:
        self._trade_count    += 1
        self._last_trade_time = datetime.now(timezone.utc).isoformat()

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

        if not self.portfolio:
            logger.info(f"[no portfolio] MKT SELL {qty} GLD — {reason}")
            return

        contract            = ContractBuilder.etf("GLD")
        order               = IbOrder()
        order.action        = "SELL"
        order.totalQuantity = qty
        order.orderType     = "MKT"
        order.transmit      = True

        oid = self.portfolio.place_order_custom(contract, order)
        if oid is not None:
            self._pending_order_actions[oid] = "SELL"
            self._pending_order_placed_at[oid] = time.time()
            self.register_order(oid)
            logger.info(f"MKT SELL {qty} GLD (order_id={oid}) — {reason}")
        else:
            logger.error(f"Failed to place MKT SELL {qty} GLD — {reason}")

    def _place_moc_short(self, shares: int, reason: str) -> None:
        """Reset-cadence pause: sell to open (or extend into) a short
        position at the closing auction. shares may include both closing an
        existing long and continuing into the short target in one order —
        _apply_signed_fill_to_holdings handles that crossing-through-zero
        correctly on fill, so this is just a SELL like _emit_sell, tagged
        differently so on_order_fill knows to set _short_gld."""
        self._trade_count    += 1
        self._last_trade_time = datetime.now(timezone.utc).isoformat()

        self.publish(
            "gld_usd_swap_signals",
            {
                "timestamp":  self._last_trade_time,
                "action":     "SELL",
                "order_type": "MOC",
                "quantity":   shares,
                "gld_price":  self._gld_price,
                "factors":    self._last_factors,
                "reason":     reason,
            },
            message_type="signal",
        )

        if not self.portfolio:
            logger.info(f"[no portfolio] MOC SHORT SELL {shares} GLD — {reason}")
            return

        contract               = ContractBuilder.etf("GLD")
        order                  = IbOrder()
        order.action           = "SELL"
        order.totalQuantity    = shares
        order.orderType        = "MOC"
        order.transmit         = True

        oid = self.portfolio.place_order_custom(contract, order)
        if oid is not None:
            self._pending_order_actions[oid] = "SHORT_OPEN"
            self._pending_order_placed_at[oid] = time.time()
            self.register_order(oid)
            logger.info(f"MOC SHORT SELL {shares} GLD (order_id={oid}) — {reason}")
        else:
            logger.error(f"Failed to place MOC SHORT SELL {shares} GLD — {reason}")

    def _place_cover_buy(self, shares: int, reason: str) -> None:
        """Reset-cadence pause: buy to cover the overnight short at the
        market open, going flat for the intraday session."""
        self._trade_count    += 1
        self._last_trade_time = datetime.now(timezone.utc).isoformat()

        self.publish(
            "gld_usd_swap_signals",
            {
                "timestamp":  self._last_trade_time,
                "action":     "BUY",
                "order_type": "MKT",
                "quantity":   shares,
                "gld_price":  self._gld_price,
                "factors":    self._last_factors,
                "reason":     reason,
            },
            message_type="signal",
        )

        if not self.portfolio:
            logger.info(f"[no portfolio] MKT COVER BUY {shares} GLD — {reason}")
            return

        contract            = ContractBuilder.etf("GLD")
        order               = IbOrder()
        order.action        = "BUY"
        order.totalQuantity = shares
        order.orderType     = "MKT"
        order.transmit      = True

        oid = self.portfolio.place_order_custom(contract, order)
        if oid is not None:
            self._pending_order_actions[oid] = "SHORT_COVER"
            self._pending_order_placed_at[oid] = time.time()
            self.register_order(oid)
            logger.info(f"MKT COVER BUY {shares} GLD (order_id={oid}) — {reason}")
        else:
            logger.error(f"Failed to place MKT COVER BUY {shares} GLD — {reason}")

    def _alert(self, kind: str, message: str, **data) -> None:
        """Log at ERROR and publish to the 'alerts' channel.

        The watchdog plugin's sink turns alerts-channel messages into
        alerts.jsonl entries and optional webhook notifications — the path
        that reaches a human when nobody is watching the logs.
        """
        logger.error(f"[alert:{kind}] {message}")
        if self._message_bus is not None:
            self.publish(
                "alerts",
                {
                    "kind":      kind,
                    "message":   message,
                    "plugin":    self.name,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    **data,
                },
                message_type="alert",
            )

    def _check_pending_order_age(self) -> None:
        """Alert once per order that has no fill/terminal status past the
        threshold. Runs on live bar callbacks, so it covers intraday orders;
        the watchdog plugin covers the overnight MOC window on wall clock."""
        now = time.time()
        for oid, placed in self._pending_order_placed_at.items():
            if oid in self._pending_alerted:
                continue
            age = now - placed
            if age > _PENDING_ORDER_ALERT_SECONDS:
                self._pending_alerted.add(oid)
                action = self._pending_order_actions.get(oid, "?")
                self._alert(
                    "stuck_order",
                    f"{action} order {oid} unresolved for {age / 60:.0f} min — "
                    f"no fill or terminal status from IB; check TWS",
                    order_id=oid, action=action, age_seconds=int(age),
                )

    def _current_gld_shares(self) -> int:
        """GLD shares in THIS plugin's holdings (its allocated slice) — not the
        account-wide GLD position. Reading portfolio.positions here would let the
        plugin sell GLD held by other plugins or unrelated account activity
        (hazard #2); scope strictly to the plugin's own holdings instead.

        Clamped to >= 0: only ever used by the long-side flow, where a
        negative reading would mean something is already wrong. Use
        _current_gld_shares_signed() for anything that needs to see a short."""
        if self.holdings:
            pos = self.holdings.get_position("GLD")
            if pos:
                return max(0, int(pos.quantity))
        return 0

    def _current_gld_shares_signed(self) -> float:
        """Signed GLD share count in this plugin's holdings — negative means
        short. HoldingPosition.quantity has no non-negative guard (that guard
        lives only in remove_position(), which the short leg doesn't use), so
        this is a plain read, not a special short-aware accessor."""
        if self.holdings:
            pos = self.holdings.get_position("GLD")
            if pos:
                return pos.quantity
        return 0.0

    def _current_nav(self) -> float:
        """Mark-to-market NAV: cash + signed shares * current price.

        Deliberately not holdings.total_value — that property sums
        HoldingPosition.market_value, which add_position() never sets (it
        stays at its dataclass default of 0.0 after every fill), so it
        under-reports NAV by the entire position's worth. Computing it
        directly here is sign-agnostic: a short's mark-to-market P&L falls
        out correctly (price up -> position value more negative -> NAV
        down) with no special-casing.
        """
        if not self.holdings:
            return 0.0
        return self.holdings.current_cash + self._current_gld_shares_signed() * self._gld_price

    def _update_reset_cadence(self) -> None:
        """Track daily NAV return and evaluate the reset-cadence trigger.

        Only accumulates/evaluates while NOT already paused (mirrors
        apply_reset_cadence in volomom: paused days are `continue`d without
        touching the trailing window at all, so the window doesn't fill with
        paused-period returns and a fresh window builds once normal
        operation resumes). _prior_nav still updates unconditionally so the
        first post-pause day computes a real return instead of comparing
        against a stale pre-pause NAV.
        """
        nav = self._current_nav()
        if (self._prior_nav is not None and self._prior_nav > 0
                and self._reset_cooldown_remaining == 0):
            daily_ret = nav / self._prior_nav - 1.0
            self._daily_returns.append(daily_ret)
            if len(self._daily_returns) == self.reset_lookback_days:
                trailing = 1.0
                for r in self._daily_returns:
                    trailing *= (1 + r)
                trailing -= 1.0
                if trailing < self.reset_threshold:
                    self._reset_cooldown_remaining = self.reset_cooldown_days
                    self._daily_returns.clear()
                    self._alert(
                        "reset_cadence_triggered",
                        f"Trailing {self.reset_lookback_days}d return {trailing:+.1%} "
                        f"below threshold {self.reset_threshold:+.1%} — entering "
                        f"{self.reset_cooldown_days}-trading-day short-overnight pause",
                        trailing_return=trailing, threshold=self.reset_threshold,
                    )
        self._prior_nav = nav

    def _apply_signed_fill_to_holdings(self, signed_qty: float, price: float) -> None:
        """Apply a fill's signed share delta to holdings — positive for a
        buy, negative for a sell — regardless of whether the resulting
        position ends up long, short, or flat.

        add_position() has no non-negative guard (unlike remove_position(),
        which isn't used here), so a single call handles every transition:
        opening a short, covering one, or a combined sell that closes an
        existing long and continues into a new short in one fill.
        """
        if not self.holdings or signed_qty == 0:
            return
        px = price if price > 0 else self._gld_price
        self.holdings.add_position("GLD", signed_qty, cost_basis=px, current_price=px)
        self.holdings.add_cash(-signed_qty * price)
        self.save_holdings()

    # =========================================================================
    # SIGNALS
    # =========================================================================

    def calculate_signals(self) -> List[TradeSignal]:
        # Orders are placed directly; the signal queue is not used.
        return []

    # =========================================================================
    # ORDER FILL / STATUS
    # =========================================================================

    def on_order_fill(self, order_record) -> None:
        action = self._pending_order_actions.pop(order_record.order_id, None)
        self._pending_order_placed_at.pop(order_record.order_id, None)
        self._pending_alerted.discard(order_record.order_id)
        qty    = float(order_record.filled_quantity or 0)
        price  = float(order_record.avg_fill_price or 0.0)

        if action == "BUY":
            self._holding_gld = True
            self._overnight_holds += 1
            self._apply_fill_to_holdings("BUY", qty, price)
            logger.info(f"MOC BUY filled: {qty:.0f} GLD @ ${price:.2f}")
        elif action == "SELL":
            self._holding_gld = False
            self._apply_fill_to_holdings("SELL", qty, price)
            logger.info(f"MKT SELL filled: {qty:.0f} GLD @ ${price:.2f}")
        elif action in ("SHORT_OPEN", "SHORT_COVER"):
            # Signed delta, not a fixed direction: a SHORT_OPEN sell may
            # cross through zero in one fill if it was also closing an
            # existing long (see _place_moc_short), so the resulting flags
            # are read back from the position after the fill lands rather
            # than assumed from the action tag.
            signed_qty = -qty if action == "SHORT_OPEN" else qty
            self._apply_signed_fill_to_holdings(signed_qty, price)
            resulting = self._current_gld_shares_signed()
            self._holding_gld = resulting > 0
            self._short_gld   = resulting < 0
            verb = "SHORT SELL" if action == "SHORT_OPEN" else "COVER BUY"
            logger.info(
                f"{verb} filled: {qty:.0f} GLD @ ${price:.2f} "
                f"(resulting position: {resulting:+.0f})"
            )
        if action:
            self._save_state()

    def _apply_fill_to_holdings(self, action: str, qty: float, price: float) -> None:
        """Record a fill in this plugin's holdings ledger.

        Without this, bought shares never enter holdings.json: the open-time
        sell would always find 0 shares (strategy stuck long), and after a
        restart the startup account reconciliation would sweep the shares to
        _unassigned while this plugin — seeing empty holdings — buys another
        full allocation.  Holdings are the plugin's source of truth for what
        it may sell and how much cash it may spend, so every fill must land
        here.  (MOC can fill above the 15:45 close; a small negative cash
        balance after slippage is bookkeeping, corrected by reconciliation.)
        """
        if not self.holdings or qty <= 0:
            return
        if action == "BUY":
            self.holdings.add_position("GLD", qty, cost_basis=price,
                                       current_price=price)
            self.holdings.add_cash(-qty * price)
        else:
            self.holdings.remove_position("GLD", qty)
            self.holdings.add_cash(qty * price)
        self.save_holdings()

    def on_order_status(self, order_record) -> None:
        from ib.models import OrderStatus
        if order_record.status not in (
            OrderStatus.CANCELLED, OrderStatus.INACTIVE, OrderStatus.ERROR
        ):
            return
        action = self._pending_order_actions.pop(order_record.order_id, None)
        self._pending_order_placed_at.pop(order_record.order_id, None)
        self._pending_alerted.discard(order_record.order_id)
        if action:
            self._alert(
                "order_terminal",
                f"{action} order {order_record.order_id} "
                f"{order_record.status.value} — "
                f"holding_gld={self._holding_gld} unchanged; "
                f"manual reconciliation may be needed",
                order_id=order_record.order_id,
                action=action,
                status=order_record.status.value,
            )
            self._save_state()   # drop the terminal order from persisted pending

    def on_ib_error(self, req_id: int, error_code: int, error_string: str) -> None:
        is_order = req_id in self._pending_order_actions
        self._alert(
            "ib_error",
            f"IB error on {'order' if is_order else 'request'} {req_id}: "
            f"code={error_code} {error_string}",
            req_id=req_id, error_code=error_code, is_order=is_order,
        )

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
                    "reset_cadence_enabled": self.reset_cadence_enabled,
                    "short_gld":             self._short_gld,
                    "reset_cooldown_remaining": self._reset_cooldown_remaining,
                    "daily_returns_tracked": len(self._daily_returns),
                    "current_nav":           round(self._current_nav(), 2),
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
                    "reset_cadence_enabled": self.reset_cadence_enabled,
                    "reset_lookback_days":   self.reset_lookback_days,
                    "reset_cooldown_days":   self.reset_cooldown_days,
                    "reset_threshold":       self.reset_threshold,
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
            elif key == "reset_cadence_enabled":
                self.reset_cadence_enabled = bool(value)
            elif key == "reset_lookback_days":
                v = max(1, int(value))
                self.reset_lookback_days = v
                self._daily_returns = deque(list(self._daily_returns), maxlen=v)
            elif key == "reset_cooldown_days":
                self.reset_cooldown_days = max(1, int(value))
            elif key == "reset_threshold":
                v = float(value)
                if v >= 0:
                    return {"success": False, "message": "reset_threshold must be negative (a drawdown)"}
                self.reset_threshold = v
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
            "  plugin request gld_usd_swap set_parameter '{\"key\": \"reset_cadence_enabled\", \"value\": true}'\n"
            "  plugin request gld_usd_swap set_parameter '{\"key\": \"reset_lookback_days\",   \"value\": 42}'\n"
            "  plugin request gld_usd_swap set_parameter '{\"key\": \"reset_cooldown_days\",   \"value\": 126}'\n"
            "  plugin request gld_usd_swap set_parameter '{\"key\": \"reset_threshold\",       \"value\": -0.08}'\n"
            "  plugin request gld_usd_swap force_regime '{\"regime\": \"gold\"}'\n"
            "  plugin request gld_usd_swap force_regime '{\"regime\": \"cash\"}'\n"
        )
