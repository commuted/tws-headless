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

SHADOW BACKFILL ON ENABLE

On a fresh enable, waiting reset_lookback_days (42) real trading days for
the trigger to have an opinion means it's blind through the exact kind of
early-onset drawdown it exists to catch. _maybe_backfill_reset_cadence()
reconstructs that history from market data instead, via
_compute_reset_cadence_backfill() — synthetic NAV from historical bars is
predictive of a real bear regime; this plugin's own actual operational
history isn't reliably available for that (crashes, feed staleness, TWS
relogin cycles, or simply not having run continuously for 42 days).

Runs synchronously inside start(), immediately after _warm_up_from_history()
and before _start_subscriptions() — so it always finishes before any live
bar can reach a session decision, eliminating any window where a real
open/close could fire against a still-cold trigger.

Computed with throwaway shadow state (fresh _InstrumentState objects, local
variables) that never touches self._uup/_tlt/_rinf/_gld_meta_closes/_regime
— the live signal state used by real trading decisions is untouched by the
computation, so there's no live state to save and restore around it. Only
two lines of the live instance change, both after the shadow computation
has fully finished: _daily_returns (the reconstructed history) and
_prior_nav (seeded from the REAL current NAV via _current_nav(), not from
anything in the shadow computation, since today's first live daily return
must be measured from where the real portfolio actually stands).

Position sign convention: HoldingPosition.quantity is a plain signed float
with no non-negative guard in add_position() (remove_position() is the only
method that rejects going negative, and it's simply not used for the short
leg) — so going short is just add_position("GLD", -shares, ...), and NAV
(cash + quantity*price) nets out correctly for a short with zero special
casing. A single sell order sized as (current long shares + target short
shares) correctly crosses through zero in one fill when the pause triggers
while still holding long.

GLL FALLBACK — CASH ACCOUNTS AND IRAS CANNOT SELL SHORT

Confirmed via a real IB whatIf order (never a real trade — a pre-trade
margin/rejection check only): _maybe_check_short_selling_capability() calls
Portfolio.check_short_selling_permitted() once, cached in
self._short_selling_permitted (persisted; not re-checked every restart,
since account permissions don't change session to session). Runs
synchronously in start() alongside the backfill, and defensively again the
instant a pause first triggers (_update_reset_cadence) in case
reset_cadence_enabled was turned on mid-session after start() already ran
— the capability is always known before a real order is ever attempted,
never discovered via a rejected order at the worst possible moment.
AccountSummaryTags.AccountType (Individual, IRA, ...) describes legal
structure, not margin/shorting permissions, so it isn't a substitute for
actually asking IB.

When not permitted, GLD's overnight leg during the pause is inverted via
GLL (ProShares UltraShort Gold, -2x GLD daily) instead of a short: bought
long at half notional (_GLL_WEIGHT = 0.5) at the close, sold at the open,
flat intraday — same cadence as the direct short, an ordinary long
purchase requiring no shorting privileges at all. Validated in
volomom/backtest_reset_cadence_gll.py (apply_reset_cadence_gll_overnight_only,
NOT the continuous-hold variant in that same file, which was found to
replicate full-day exposure instead — a mismatch caught before this was
wired in): Sharpe 1.42 vs the direct short's 1.44, essentially the same
trade. GLL is a different symbol from GLD, so unlike the short leg's
single crossing-through-zero order, entering the hedge for the first time
while still holding a GLD long takes two separate orders (sell the GLD
long, buy the GLL hedge) rather than one combined order.
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

# GLL (ProShares UltraShort Gold, -2x GLD daily) fallback weight when this
# account can't sell short. Half notional of GLL long replicates roughly
# -1x GLD exposure — validated in volomom/backtest_reset_cadence_gll.py
# (apply_reset_cadence_gll_overnight_only): Sharpe 1.42 vs the direct
# short's 1.44, essentially the same trade with an ordinary long purchase.
_GLL_WEIGHT = 0.5

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

# IB error codes that mean an order was outright rejected and no orderStatus
# transition will follow. Treated as terminal in on_ib_error: the pending-order
# tracker is cleared so the stuck-order alerter (and the watchdog plugin) stop
# re-firing every 30 min. Codes not on this list stay alert-only — some IB
# order-attributed codes are advisories, not rejections. Extend as new
# rejection modes are seen in practice.
_TERMINAL_REJECT_CODES = frozenset({
    201,    # Order rejected — reason: …
    202,    # Order cancelled — reason: …
    203,    # The security is not available or allowed for this account
    321,    # Server error validating message (malformed order)
    388,    # Order size does not conform to market rule
    434,    # Order size does not conform to market rule
    435,    # You must specify an account (Gateway-side validation reject)
    10052,  # Invalid time in force
    10147,  # OrderId to cancel not found
    10148,  # OrderId to cancel is in a state that cannot be cancelled
    10289,  # Short sale not permitted for this security
})

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
        # Minimum fractional gap fast/slow must show for a per-ETF gold/cash
        # vote to count.  Prevents float64 summation associativity (fast=
        # sum(buf[-5:])/5 vs slow=sum(buf)/20) from choosing the tie-break
        # on a genuinely stationary tape.  Ties bias toward NOT-gold, which
        # in the composite AND-OR reduces to the cash-lean side — the
        # conservative default matching how the plugin already treats
        # warm-up.  1e-8 leaves ~6 orders of margin above ULP noise (~1e-14
        # for these price magnitudes) while sitting far below any real bar-
        # tick contribution to a 20-sample SMA (a $0.01 tick moves a 20-bar
        # average by 5e-4 — five orders above this threshold).
        self.regime_deadband:       float = 1e-8

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

        # --- GLL fallback (cash accounts/IRAs cannot sell short at all) ---
        # None = not yet checked; True/False = confirmed via a real whatIf
        # order test (see _maybe_check_short_selling_capability). Checked
        # once and persisted — not re-checked every restart, since account
        # permissions don't change session to session.
        self._short_selling_permitted: Optional[bool] = None
        self._gll_price: float = 0.0
        self._holding_gll: bool = False   # overnight-only GLL hedge, half notional

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
            self.regime_deadband         = saved.get("regime_deadband",         self.regime_deadband)
            self.reset_cadence_enabled   = saved.get("reset_cadence_enabled",   self.reset_cadence_enabled)
            self.reset_lookback_days     = saved.get("reset_lookback_days",     self.reset_lookback_days)
            self.reset_cooldown_days     = saved.get("reset_cooldown_days",     self.reset_cooldown_days)
            self.reset_threshold         = saved.get("reset_threshold",         self.reset_threshold)
            self._short_gld              = saved.get("short_gld",               False)
            self._reset_cooldown_remaining = saved.get("reset_cooldown_remaining", 0)
            self._daily_returns          = deque(saved.get("daily_returns", []),
                                                  maxlen=self.reset_lookback_days)
            self._prior_nav              = saved.get("prior_nav")
            self._short_selling_permitted = saved.get("short_selling_permitted")
            self._holding_gll            = saved.get("holding_gll", False)
            self._uup.restore(saved.get("uup",  {}), _INIT_DERIV_UUP)
            self._tlt.restore(saved.get("tlt",  {}), _INIT_DERIV_TLT)
            self._rinf.restore(saved.get("rinf", {}), _INIT_DERIV_RINF)
            self._pending_order_actions = {
                int(oid): action
                for oid, action in saved.get("pending_orders", {}).items()
            }
            # Restore placed_at from the previous save if available; fall
            # back to "now" for entries missing a timestamp (older state
            # files pre-persistence).
            saved_placed_at = saved.get("pending_orders_placed_at", {}) or {}
            _now = time.time()
            self._pending_order_placed_at = {
                oid: float(saved_placed_at.get(str(oid), _now))
                for oid in self._pending_order_actions
            }
            # All orders this plugin places are DAY-TIF (MKT and MOC). A DAY
            # order that hasn't hit a terminal status within one full session
            # cycle (~20h covers overnight + the next morning restart) is
            # certainly dead at IB — either filled long ago or expired at the
            # close. Sweep them so the stuck-order alerter and the
            # _restored_pending_buy hold-flag stop firing on ghost state.
            # For bit-exact confirmation of borderline cases, snapshot
            # reqAllOpenOrders() at start and drop any local entry IB doesn't
            # know about — not yet wired here; the age sweep is sufficient
            # for the observed failure mode (permId isn't tracked, so cross-
            # session correlation isn't possible without that plumbing).
            _MAX_PENDING_AGE_SECS = 20 * 3600
            _stale = [oid for oid, ts in self._pending_order_placed_at.items()
                      if _now - ts > _MAX_PENDING_AGE_SECS]
            for oid in _stale:
                action = self._pending_order_actions.pop(oid, None)
                self._pending_order_placed_at.pop(oid, None)
                age_h = (_now - float(saved_placed_at.get(str(oid), _now))) / 3600
                logger.warning(
                    f"Sweeping stale pending {action} order {oid} "
                    f"(age {age_h:.1f}h > 20h; DAY-TIF orders can't survive "
                    f"past the following market close)"
                )
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
        self._maybe_backfill_reset_cadence()
        self._maybe_check_short_selling_capability()
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
        """Subscribe to live 5-min bars for all four symbols (five when the
        reset-cadence overlay is enabled — see below).

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

        GLL (the fallback hedge when this account can't sell short) is
        subscribed only when reset_cadence_enabled — no reason to carry a
        fifth live feed for every plugin instance that never uses it. It's a
        potential trade, not signal-only, so it stays on TRADES too.
        """
        symbols = ["GLD", "UUP", "TLT", "RINF"]
        if self.reset_cadence_enabled:
            symbols.append("GLL")

        self._live_bar_req_ids: Dict[str, Optional[int]] = {}
        for symbol in symbols:
            kwargs = {} if symbol in ("GLD", "GLL") else {"what_to_show": "MIDPOINT"}
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
            "regime_deadband":       self.regime_deadband,
            "reset_cadence_enabled": self.reset_cadence_enabled,
            "reset_lookback_days":   self.reset_lookback_days,
            "reset_cooldown_days":   self.reset_cooldown_days,
            "reset_threshold":       self.reset_threshold,
            "short_gld":             self._short_gld,
            "reset_cooldown_remaining": self._reset_cooldown_remaining,
            "daily_returns":         list(self._daily_returns),
            "prior_nav":             self._prior_nav,
            "short_selling_permitted": self._short_selling_permitted,
            "holding_gll":           self._holding_gll,
            "uup":                   self._uup.save(),
            "tlt":                   self._tlt.save(),
            "rinf":                  self._rinf.save(),
            # In-flight orders survive a restart so an unresolved MOC buy can
            # never be silently forgotten (and duplicated) after a crash.
            # placed_at is persisted alongside so the load-time sweep can
            # judge age against real elapsed wall-clock, not "restarted now."
            "pending_orders":        {str(oid): action for oid, action
                                      in self._pending_order_actions.items()},
            "pending_orders_placed_at": {str(oid): ts for oid, ts
                                         in self._pending_order_placed_at.items()},
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
    # RESET-CADENCE SHADOW BACKFILL
    # =========================================================================

    def _maybe_backfill_reset_cadence(self) -> None:
        """
        On a fresh enable (self._daily_returns empty), reconstruct
        reset_lookback_days of the composite strategy's own daily returns
        from historical market data instead of waiting reset_lookback_days
        real trading days to accumulate them live.

        Runs synchronously in the same start() phase as _warm_up_from_history
        — before _start_subscriptions() — so no live bar can reach a session
        decision (open/close, real orders) until this returns. That ordering
        is what makes the backfill safe: there's no window where a real
        decision could fire on a still-cold trigger.

        Deliberately reconstructs from market data rather than any real
        operational history this plugin might have (persisted daily NAV from
        a previous run, if any): actual uptime has gaps — crashes, feed
        staleness, TWS relogin cycles, the plugin being frozen — that would
        make 42 CONSECUTIVE real trading days unreliable to assemble. Market
        data doesn't care whether this engine was running.

        Never touches self._uup/_tlt/_rinf/_gld_meta_closes/_regime — see
        _compute_reset_cadence_backfill, which builds entirely separate
        shadow state. Only two lines of the live instance are set here, both
        after the shadow computation has fully finished: _daily_returns and
        _prior_nav.
        """
        if not self.reset_cadence_enabled or self._daily_returns:
            return   # disabled, or already has real or previously-backfilled history

        returns, ok = self._compute_reset_cadence_backfill(self.reset_lookback_days)
        if not ok:
            logger.warning(
                "Reset-cadence backfill: could not reconstruct history — "
                "starting cold, trigger will warm up from live trading instead"
            )
            return

        self._daily_returns = deque(returns, maxlen=self.reset_lookback_days)
        # Seed from the REAL current NAV, not anything from the shadow
        # computation (which is a hypothetical parallel strategy, not this
        # plugin's actual position) — today's first live daily return must
        # be measured from where the real portfolio actually stands right now.
        self._prior_nav = self._current_nav()
        logger.info(
            f"Reset-cadence backfill: reconstructed {len(returns)}/"
            f"{self.reset_lookback_days} days from history, "
            f"seeded prior_nav=${self._prior_nav:,.2f}"
        )

    def _compute_reset_cadence_backfill(self, lookback_days: int) -> Tuple[List[float], bool]:
        """
        Reconstruct lookback_days of the composite strategy's own daily
        returns from historical 5-min bars, using throwaway shadow signal
        state — fresh _InstrumentState objects and local variables, never
        self._uup/_tlt/_rinf/_gld_meta_closes/_regime. Mirrors
        volomom's plugin_proxy_daily_returns exactly (overnight leg always
        held, intraday leg conditional on the regime decided at the PRIOR
        day's close) but computed from this plugin's own live signal logic
        (5-min bars, adaptive smoothing, the meta gate) rather than
        volomom's daily-bar approximation.

        Returns (daily_returns, ok). ok is False only when historical data
        could not be fetched at all; a shorter-than-requested but non-empty
        reconstruction (e.g. near a data-availability boundary) is still ok
        — the live tracker fills the remainder from real trading days.
        """
        if not self.portfolio:
            return [], False

        _UTC = timezone.utc
        _now = datetime.now(_UTC)
        # Comfortably more calendar days than lookback_days trading days:
        # the shadow signal needs under a day to warm up (meta_slow_bars=60
        # 5-min bars ≈ 5h), so this is mostly slack for weekends/holidays.
        _start = _now - timedelta(days=int(lookback_days * 1.6) + 10)

        fetch_started = time.time()
        logger.info(
            f"Reset-cadence backfill: requesting 5-min bars "
            f"{_start.date()} → {_now.date()} for GLD/UUP/TLT/RINF "
            f"(lookback_days={lookback_days})"
        )

        bars_by_symbol: Dict[str, list] = {}
        for symbol in ("GLD", "UUP", "TLT", "RINF"):
            what_to_show = "TRADES" if symbol == "GLD" else "MIDPOINT"
            symbol_started = time.time()
            bars = self.get_bars_cached(
                contract=ContractBuilder.etf(symbol),
                start_dt=_start,
                end_dt=_now,
                bar_size_setting="5 mins",
                what_to_show=what_to_show,
                use_rth=True,
            )
            if not bars:
                logger.warning(f"Reset-cadence backfill: no historical data for {symbol}")
                return [], False
            logger.info(
                f"Reset-cadence backfill: {symbol} {len(bars)} bars "
                f"({bars[0].date} → {bars[-1].date}) in "
                f"{time.time() - symbol_started:.1f}s"
            )
            bars_by_symbol[symbol] = bars

        merged: List[tuple] = []
        for symbol, bars in bars_by_symbol.items():
            for b in bars:
                merged.append((b.date, symbol, float(b.open), float(b.close)))
        merged.sort(key=lambda x: x[0])

        # --- shadow state: isolated from self.*, discarded after this call ---
        shadow_uup  = _InstrumentState(_INIT_DERIV_UUP)
        shadow_tlt  = _InstrumentState(_INIT_DERIV_TLT)
        shadow_rinf = _InstrumentState(_INIT_DERIV_RINF)
        shadow_meta_closes: deque = deque(maxlen=80)
        shadow = {
            "meta_fast": 0.0, "meta_slow": 0.0, "gld_uptrend": False,
            "regime": REGIME_UNKNOWN, "regime_at_prior_close": REGIME_UNKNOWN,
        }
        day_open: Dict[object, float] = {}
        day_close: Dict[object, float] = {}
        daily_returns: List[float] = []
        prev_close_price: Optional[float] = None

        def _shadow_recompute_regime() -> None:
            if not shadow_uup.warmed_up(self.slow_bars):
                return
            uup_gold = shadow_uup.fast_sma < shadow_uup.slow_sma
            if shadow_tlt.warmed_up(self.slow_bars):
                tlt_gold = shadow_tlt.fast_sma > shadow_tlt.slow_sma
                if shadow["gld_uptrend"] and shadow_rinf.warmed_up(self.slow_bars):
                    rinf_gold = shadow_rinf.fast_sma > shadow_rinf.slow_sma
                    gold = uup_gold and (tlt_gold or rinf_gold)
                else:
                    gold = uup_gold and tlt_gold
            else:
                gold = uup_gold
            shadow["regime"] = REGIME_GOLD if gold else REGIME_CASH

        for ts_str, group in groupby(merged, key=lambda x: x[0]):
            try:
                ts = datetime.strptime(ts_str[:8] + " " + ts_str[9:17], "%Y%m%d %H:%M:%S")
            except (ValueError, TypeError):
                continue
            date = ts.date()

            for _, symbol, o, c in group:
                if symbol == "GLD":
                    day_open.setdefault(date, o)
                    day_close[date] = c
                    shadow_meta_closes.append(c)
                    if len(shadow_meta_closes) >= self.meta_slow_bars:
                        cl = list(shadow_meta_closes)
                        shadow["meta_fast"] = sum(cl[-self.meta_fast_bars:]) / self.meta_fast_bars
                        shadow["meta_slow"] = sum(cl[-self.meta_slow_bars:]) / self.meta_slow_bars
                        shadow["gld_uptrend"] = shadow["meta_fast"] > shadow["meta_slow"]
                    _shadow_recompute_regime()
                elif symbol == "UUP":
                    shadow_uup.push(c, self.vol_window, self.derivative_percentile,
                                     self.fast_bars, self.slow_bars)
                    _shadow_recompute_regime()
                elif symbol == "TLT":
                    shadow_tlt.push(c, self.vol_window, self.derivative_percentile,
                                     self.fast_bars, self.slow_bars)
                    _shadow_recompute_regime()
                elif symbol == "RINF":
                    shadow_rinf.push(c, self.vol_window, self.derivative_percentile,
                                      self.fast_bars, self.slow_bars)
                    _shadow_recompute_regime()

            if ts.hour == _CLOSE_HOUR and ts.minute == _CLOSE_MIN:
                # Session close for `date`: settle the day's realized return
                # (overnight from the previous close into today's open, plus
                # intraday conditional on the PRIOR day's close regime —
                # never today's, which isn't known until this same instant),
                # then roll the regime forward for tomorrow's decision.
                o_today = day_open.get(date)
                c_today = day_close.get(date)
                if (prev_close_price and o_today and c_today
                        and shadow["regime_at_prior_close"] != REGIME_UNKNOWN):
                    cash = 1.0 + (o_today / prev_close_price - 1.0)
                    if shadow["regime_at_prior_close"] == REGIME_GOLD:
                        cash *= 1.0 + (c_today / o_today - 1.0)
                    daily_returns.append(cash - 1.0)
                if shadow["regime"] != REGIME_UNKNOWN:
                    shadow["regime_at_prior_close"] = shadow["regime"]
                prev_close_price = c_today

        elapsed = time.time() - fetch_started
        if not daily_returns:
            logger.warning(
                f"Reset-cadence backfill: fetched bars but reconstructed 0 "
                f"days (in {elapsed:.1f}s) — shadow regime never warmed up "
                f"or no valid close-to-close pairs in range"
            )
            return [], False
        if len(daily_returns) < lookback_days:
            logger.warning(
                f"Reset-cadence backfill: only reconstructed {len(daily_returns)} "
                f"of {lookback_days} requested days (in {elapsed:.1f}s)"
            )
            return daily_returns, True

        logger.info(
            f"Reset-cadence backfill: reconstructed {len(daily_returns)} days "
            f"(kept last {lookback_days}) in {elapsed:.1f}s total"
        )
        return daily_returns[-lookback_days:], True

    # =========================================================================
    # RESET-CADENCE GLL FALLBACK
    # =========================================================================

    def _maybe_check_short_selling_capability(self) -> None:
        """
        Confirm via a real IB whatIf order — not an assumption, not an
        account-type tag — whether this account can sell short at all.
        Cash accounts and IRAs cannot; AccountSummaryTags.AccountType
        describes legal structure (Individual, IRA, ...), not margin/
        shorting permissions, so it isn't a substitute for actually asking.

        Checked once and persisted (self._short_selling_permitted stays
        None only until the first successful check) — account permissions
        don't change session to session, so there's no reason to repeat a
        blocking IB round-trip on every restart.

        Runs synchronously in start(), alongside _maybe_backfill_reset_cadence
        and before _start_subscriptions() — the capability is known before
        any real trading decision can occur, not discovered reactively via a
        rejected order at the worst possible moment.
        """
        if not self.reset_cadence_enabled or self._short_selling_permitted is not None:
            return
        if not self.portfolio:
            return

        target_shares = (
            max(1, int(self.allocation_dollars / self._gld_price))
            if self._gld_price > 0 else 1
        )
        logger.info(
            f"Reset-cadence: submitting whatIf SELL {target_shares} GLD "
            f"to test short-selling capability (gld_price=${self._gld_price:.2f})"
        )
        checked_at = time.time()
        result = self.portfolio.check_short_selling_permitted(
            ContractBuilder.etf("GLD"), target_shares,
        )
        self._short_selling_permitted = result["permitted"]
        elapsed = time.time() - checked_at

        if result["permitted"]:
            raw = result.get("raw") or {}
            logger.info(
                f"Reset-cadence: short selling confirmed permitted on this "
                f"account (whatIf order test, {elapsed:.1f}s) — "
                f"status={raw.get('status')} "
                f"initMargin {raw.get('initMarginBefore')}->{raw.get('initMarginAfter')}"
            )
        else:
            logger.warning(
                f"Reset-cadence: whatIf check ({elapsed:.1f}s) found short "
                f"selling NOT permitted — reject_reason={result['reject_reason']!r} "
                f"raw={result.get('raw')}"
            )
            self._alert(
                "short_selling_not_permitted",
                f"Reset-cadence: short selling NOT permitted on this account "
                f"({result['reject_reason']}) — falling back to 1/2 GLL "
                f"(ProShares UltraShort Gold) during pauses instead",
                reject_reason=result["reject_reason"],
            )
        self._save_state()

    def _current_gll_shares_signed(self) -> float:
        """Signed GLL share count in this plugin's holdings. GLL is always
        held long (never short) when it's held at all, so this is really
        just a same-shaped accessor alongside _current_gld_shares_signed —
        but add_position()/get_position() are shared machinery regardless
        of symbol, so there's nothing GLL-specific about it."""
        if self.holdings:
            pos = self.holdings.get_position("GLL")
            if pos:
                return pos.quantity
        return 0.0

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
        elif symbol == "GLL":
            # Fallback hedge instrument — just needs a current price to size
            # orders against; no signal role, nothing feeds the regime.
            self._gll_price = close

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

        # Dead-band: require a positive gap of at least regime_deadband
        # (relative to slow_sma) for a per-ETF vote to lean gold. A tie
        # falls to False, matching the conservative cash-lean default.
        eps = self.regime_deadband
        uup_gold = (self._uup.slow_sma - self._uup.fast_sma) > abs(self._uup.slow_sma) * eps

        if self._tlt.warmed_up(self.slow_bars):
            tlt_gold = (self._tlt.fast_sma - self._tlt.slow_sma) > abs(self._tlt.slow_sma) * eps

            if (self._gld_in_uptrend and self._rinf.warmed_up(self.slow_bars)):
                # GLD structural uptrend: extend with RINF (stagflation scenario)
                rinf_gold = (self._rinf.fast_sma - self._rinf.slow_sma) > abs(self._rinf.slow_sma) * eps
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
            # Checked against actual position flags (set from the resulting
            # fill, not assumed) rather than re-deriving from
            # _short_selling_permitted — self-correcting the same way
            # on_order_fill already is.
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
            elif self._holding_gll:
                qty = self._current_gll_shares_signed()
                if qty > 0:
                    self._place_gll_sell(
                        int(qty),
                        reason=(
                            f"Open {ts.date()}: reset-cadence pause "
                            f"({self._reset_cooldown_remaining}d remaining) — "
                            f"MKT sell GLL hedge, sit out intraday"
                        ),
                    )
                else:
                    logger.warning(
                        f"Open {ts.date()}: reset-cadence pause active but plugin "
                        f"holds no GLL to sell (holding_gll flag was "
                        f"{self._holding_gll})"
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
            if self._short_selling_permitted:
                # Paused: short overnight instead of buying long. Sized as
                # any existing long shares (first pause evening only — every
                # other evening this is already flat from that morning's
                # cover) plus the short target; one order correctly crosses
                # through zero in a single fill (see
                # _apply_signed_fill_to_holdings).
                target_short = (
                    int(self.allocation_dollars / self._gld_price)
                    if self._gld_price > 0 else 0
                )
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
            else:
                # GLL fallback: this account can't sell short (or capability
                # is unconfirmed — treated the same as "can't", conservatively).
                # Two separate orders, since GLL is a different symbol from
                # GLD and the single-order crossing-through-zero trick only
                # works within one symbol: sell any existing GLD long first
                # (first pause evening only), then buy GLL at half notional
                # for the overnight hedge (every paused evening — flat again
                # each morning, same cadence as the direct short).
                gld_qty = self._current_gld_shares()
                if gld_qty > 0:
                    self._emit_sell(
                        qty=gld_qty,
                        reason=(
                            f"Close {ts.date()}: reset-cadence pause, GLL fallback "
                            f"— MKT sell existing GLD long before hedging with GLL"
                        ),
                    )
                gll_shares = (
                    int(_GLL_WEIGHT * self.allocation_dollars / self._gll_price)
                    if self._gll_price > 0 else 0
                )
                if gll_shares > 0:
                    self._place_gll_buy(
                        gll_shares,
                        reason=(
                            f"Close {ts.date()}: reset-cadence pause "
                            f"({self._reset_cooldown_remaining}d remaining) — "
                            f"MOC BUY GLL (1/2 notional) overnight hedge, "
                            f"short selling not permitted on this account"
                        ),
                    )
                elif self.portfolio and self._gll_price > 0:
                    logger.error(
                        f"Close {ts.date()}: reset-cadence GLL hedge skipped — "
                        f"computed 0 shares at ${self._gll_price:.2f}"
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

    def _record_trade(self, when: str) -> None:
        """Count a trade, once IB has accepted the order.

        These counters used to increment at the top of each order method,
        before placement — so anything that stopped an order short still
        counted it. On 2026-08-05 a dry_run engine suppressed a live MKT SELL
        of 26 GLD and trade_count still went 19 -> 20, recording a trade that
        never reached the market. A rejection did the same.

        The rule is that the plugin counts a trade unless it has been told the
        order failed. A portfolio returning None said no — suppressed by
        dry_run, or rejected. No portfolio at all (backtests, unit tests) is
        not a refusal: there is nothing there to refuse, the order method logs
        "[no portfolio]" and the trade is assumed, so it still counts.

        The published signal keeps its own timestamp, taken when the decision
        was made: the signal genuinely happened even when the order did not.
        """
        self._trade_count += 1
        self._last_trade_time = when

    def _place_moc_buy(self, shares: int, reason: str) -> None:
        now = datetime.now(timezone.utc).isoformat()

        self.publish(
            "gld_usd_swap_signals",
            {
                "timestamp":  now,
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
            self._record_trade(now)
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
            self._record_trade(now)
            self._pending_order_actions[oid] = "BUY"
            self._pending_order_placed_at[oid] = time.time()
            self.register_order(oid)
            logger.info(f"MOC BUY {shares} GLD (order_id={oid}) — {reason}")
        else:
            logger.error(f"Failed to place MOC BUY {shares} GLD — {reason}")

    def _emit_sell(self, qty: int, reason: str) -> None:
        now = datetime.now(timezone.utc).isoformat()

        self.publish(
            "gld_usd_swap_signals",
            {
                "timestamp":  now,
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
            self._record_trade(now)
            logger.info(f"[no portfolio] MKT SELL {qty} GLD — {reason}")
            return

        contract            = ContractBuilder.etf("GLD")
        order               = IbOrder()
        order.action        = "SELL"
        order.totalQuantity = qty
        order.orderType     = "MKT"
        order.tif           = "DAY"
        order.transmit      = True

        oid = self.portfolio.place_order_custom(contract, order)
        if oid is not None:
            self._record_trade(now)
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
        now = datetime.now(timezone.utc).isoformat()

        self.publish(
            "gld_usd_swap_signals",
            {
                "timestamp":  now,
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
            self._record_trade(now)
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
            self._record_trade(now)
            self._pending_order_actions[oid] = "SHORT_OPEN"
            self._pending_order_placed_at[oid] = time.time()
            self.register_order(oid)
            logger.info(f"MOC SHORT SELL {shares} GLD (order_id={oid}) — {reason}")
        else:
            logger.error(f"Failed to place MOC SHORT SELL {shares} GLD — {reason}")

    def _place_cover_buy(self, shares: int, reason: str) -> None:
        """Reset-cadence pause: buy to cover the overnight short at the
        market open, going flat for the intraday session."""
        now = datetime.now(timezone.utc).isoformat()

        self.publish(
            "gld_usd_swap_signals",
            {
                "timestamp":  now,
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
            self._record_trade(now)
            logger.info(f"[no portfolio] MKT COVER BUY {shares} GLD — {reason}")
            return

        contract            = ContractBuilder.etf("GLD")
        order               = IbOrder()
        order.action        = "BUY"
        order.totalQuantity = shares
        order.orderType     = "MKT"
        order.tif           = "DAY"
        order.transmit      = True

        oid = self.portfolio.place_order_custom(contract, order)
        if oid is not None:
            self._record_trade(now)
            self._pending_order_actions[oid] = "SHORT_COVER"
            self._pending_order_placed_at[oid] = time.time()
            self.register_order(oid)
            logger.info(f"MKT COVER BUY {shares} GLD (order_id={oid}) — {reason}")
        else:
            logger.error(f"Failed to place MKT COVER BUY {shares} GLD — {reason}")

    def _place_gll_buy(self, shares: int, reason: str) -> None:
        """Reset-cadence pause, GLL fallback: buy GLL at the closing auction
        for the overnight hedge — used instead of a GLD short when this
        account can't sell short. Mirrors _place_moc_short's cadence exactly
        (fresh position every paused evening, closed every paused morning),
        just on a different symbol via an ordinary long purchase."""
        now = datetime.now(timezone.utc).isoformat()

        self.publish(
            "gld_usd_swap_signals",
            {
                "timestamp":  now,
                "action":     "BUY",
                "order_type": "MOC",
                "quantity":   shares,
                "gll_price":  self._gll_price,
                "factors":    self._last_factors,
                "reason":     reason,
            },
            message_type="signal",
        )

        if not self.portfolio:
            self._record_trade(now)
            logger.info(f"[no portfolio] MOC BUY {shares} GLL — {reason}")
            return

        contract               = ContractBuilder.etf("GLL")
        order                  = IbOrder()
        order.action           = "BUY"
        order.totalQuantity    = shares
        order.orderType        = "MOC"
        order.transmit         = True

        oid = self.portfolio.place_order_custom(contract, order)
        if oid is not None:
            self._record_trade(now)
            self._pending_order_actions[oid] = "GLL_OPEN"
            self._pending_order_placed_at[oid] = time.time()
            self.register_order(oid)
            logger.info(f"MOC BUY {shares} GLL (order_id={oid}) — {reason}")
        else:
            logger.error(f"Failed to place MOC BUY {shares} GLL — {reason}")

    def _place_gll_sell(self, shares: int, reason: str) -> None:
        """Reset-cadence pause, GLL fallback: sell the overnight GLL hedge
        at the market open, going flat for the intraday session — mirrors
        _place_cover_buy's cadence, an ordinary sell instead of a cover."""
        now = datetime.now(timezone.utc).isoformat()

        self.publish(
            "gld_usd_swap_signals",
            {
                "timestamp":  now,
                "action":     "SELL",
                "order_type": "MKT",
                "quantity":   shares,
                "gll_price":  self._gll_price,
                "factors":    self._last_factors,
                "reason":     reason,
            },
            message_type="signal",
        )

        if not self.portfolio:
            self._record_trade(now)
            logger.info(f"[no portfolio] MKT SELL {shares} GLL — {reason}")
            return

        contract            = ContractBuilder.etf("GLL")
        order               = IbOrder()
        order.action        = "SELL"
        order.totalQuantity = shares
        order.orderType     = "MKT"
        order.tif           = "DAY"
        order.transmit      = True

        oid = self.portfolio.place_order_custom(contract, order)
        if oid is not None:
            self._record_trade(now)
            self._pending_order_actions[oid] = "GLL_CLOSE"
            self._pending_order_placed_at[oid] = time.time()
            self.register_order(oid)
            logger.info(f"MKT SELL {shares} GLL (order_id={oid}) — {reason}")
        else:
            logger.error(f"Failed to place MKT SELL {shares} GLL — {reason}")

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

        Equivalent to holdings.total_value now that HoldingPosition.market_value
        is a derived property (plugins/base.py) rather than a field add_position()
        could forget to set — computed explicitly here anyway to stay scoped to
        this plugin's own GLD position specifically, rather than summing every
        position in holdings. Sign-agnostic: a short's mark-to-market P&L falls
        out correctly (price up -> position value more negative -> NAV down)
        with no special-casing.
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
                    # Safety net: normally checked once at startup, but if
                    # reset_cadence_enabled was turned on mid-session (after
                    # start() already ran), this is the first point a pause
                    # is about to act — capability must be known before that,
                    # not discovered via a rejected order this same evening.
                    self._maybe_check_short_selling_capability()
                    self._alert(
                        "reset_cadence_triggered",
                        f"Trailing {self.reset_lookback_days}d return {trailing:+.1%} "
                        f"below threshold {self.reset_threshold:+.1%} — entering "
                        f"{self.reset_cooldown_days}-trading-day short-overnight pause "
                        f"({'short GLD' if self._short_selling_permitted else '1/2 GLL fallback'})",
                        trailing_return=trailing, threshold=self.reset_threshold,
                    )
        self._prior_nav = nav

    def _apply_signed_fill_to_holdings(self, signed_qty: float, price: float,
                                        symbol: str = "GLD") -> None:
        """Apply a fill's signed share delta to holdings — positive for a
        buy, negative for a sell — regardless of whether the resulting
        position ends up long, short, or flat.

        add_position() has no non-negative guard (unlike remove_position(),
        which isn't used here), so a single call handles every transition:
        opening a short, covering one, or a combined sell that closes an
        existing long and continues into a new short in one fill. Also used
        for the GLL fallback hedge (always a plain long, never negative),
        which is why symbol is a parameter rather than hardcoded.
        """
        if not self.holdings or signed_qty == 0:
            return
        default_price = self._gld_price if symbol == "GLD" else self._gll_price
        px = price if price > 0 else default_price
        self.holdings.add_position(symbol, signed_qty, cost_basis=px, current_price=px)
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
        elif action in ("GLL_OPEN", "GLL_CLOSE"):
            # GLL is only ever held long (it's the no-shorting-required
            # fallback), so the sign is fixed by the action, unlike the
            # GLD short leg above.
            signed_qty = qty if action == "GLL_OPEN" else -qty
            self._apply_signed_fill_to_holdings(signed_qty, price, symbol="GLL")
            self._holding_gll = self._current_gll_shares_signed() > 0
            verb = "BUY" if action == "GLL_OPEN" else "SELL"
            logger.info(
                f"GLL {verb} filled: {qty:.0f} GLL @ ${price:.2f} "
                f"(holding_gll={self._holding_gll})"
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

        # Outright rejection: clear pending-order state so the stuck-order
        # alerter and watchdog stop re-firing every 30 min. Idempotent with
        # on_order_status (whichever fires second finds nothing to pop and
        # no-ops via the `if action` guard there).
        if is_order and error_code in _TERMINAL_REJECT_CODES:
            action = self._pending_order_actions.pop(req_id, None)
            self._pending_order_placed_at.pop(req_id, None)
            self._pending_alerted.discard(req_id)
            self._alert(
                "order_rejected",
                f"{action} order {req_id} rejected by IB "
                f"[code={error_code}] {error_string} — "
                f"no fill occurred; holdings unchanged, manual reconciliation "
                f"may be needed",
                order_id=req_id, action=action,
                error_code=error_code, error_string=error_string,
            )
            self._save_state()
            return

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
            elif key == "regime_deadband":
                v = float(value)
                if v < 0:
                    return {"success": False, "message": "regime_deadband must be >= 0"}
                self.regime_deadband = v
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
            "  plugin request gld_usd_swap set_parameter '{\"key\": \"regime_deadband\",       \"value\": 1e-8}'\n"
            "  plugin request gld_usd_swap set_parameter '{\"key\": \"reset_cadence_enabled\", \"value\": true}'\n"
            "  plugin request gld_usd_swap set_parameter '{\"key\": \"reset_lookback_days\",   \"value\": 42}'\n"
            "  plugin request gld_usd_swap set_parameter '{\"key\": \"reset_cooldown_days\",   \"value\": 126}'\n"
            "  plugin request gld_usd_swap set_parameter '{\"key\": \"reset_threshold\",       \"value\": -0.08}'\n"
            "  plugin request gld_usd_swap force_regime '{\"regime\": \"gold\"}'\n"
            "  plugin request gld_usd_swap force_regime '{\"regime\": \"cash\"}'\n"
        )
