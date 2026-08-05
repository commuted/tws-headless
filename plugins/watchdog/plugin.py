"""
plugins/watchdog/plugin.py — Operational anomaly detection and alert egress.

The engine's built-in instrumentation is forensic: it records what happened
(logs, ExecutionDB, execution log) but nothing notices when an expected thing
FAILS to happen, and nothing carries a detected anomaly beyond the log file.
This plugin closes both gaps.

A monitor thread wakes every check_interval_seconds and runs three checks:

  1. Bar-feed staleness — every active keepUpToDate live-bar subscription
     (portfolio.keep_up_to_date_feeds()) must have delivered a bar within
     bar_staleness_seconds during regular trading hours. A silent feed —
     e.g. a subscription lost across a TWS reconnect — is the single most
     dangerous failure mode: a strategy holds a position with no exit
     signals and no errors. Detection closes into ACTION: when feeds are
     stale (and auto_remediate_stale_feeds is on), the watchdog asks the
     executive to have plugins re-create their subscriptions (the
     on_reconnect contract), rate-limited by remediation_cooldown_seconds.

  2. Stuck orders — any order in portfolio.pending_orders with no fill or
     terminal status after order_stuck_seconds (covers the overnight MOC
     window on wall clock, where in-plugin bar-driven checks cannot run).

  3. Periodic reconciliation — every reconcile_interval_seconds, run
     PluginExecutive.reconcile_with_account() and alert on discrepancies
     (position/cash drift between plugin ledgers and the real account),
     instead of only detecting drift at the next engine restart.

Alert egress: the plugin subscribes to the MessageBus "alerts" channel and
acts as the sink for ALL alerts (its own checks, the executive's ib_error /
reconnected alerts, and any plugin's published alerts):

  - every alert is appended as a JSON line to {plugin_dir}/alerts.jsonl
    (account-scoped, so paper and live alerts never comingle) — point an
    external tailer/cron at this file;
  - if webhook_url is set, each alert is POSTed as JSON (drained by the
    monitor thread, so delivery lags up to check_interval_seconds; failures
    are logged and dropped, never retried into a pile-up).

RTH awareness uses America/New_York wall clock, weekdays 09:30–16:00.
Market holidays are not modeled: a staleness alert on a holiday is a
tolerable false positive (set rth_only=false to check around the clock,
or ignore holiday alerts).
"""

import json
import logging
import queue
import subprocess
import threading
import time
import urllib.request
from datetime import datetime, timezone, time as dt_time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from plugins.base import PluginBase, TradeSignal

logger = logging.getLogger(__name__)

_NY = ZoneInfo("America/New_York")

# plugins/watchdog/plugin.py -> repo root, so the default relaunch script
# path doesn't depend on the engine's current working directory.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_RELAUNCH_SCRIPT = str(_REPO_ROOT / "relaunch_tws.sh")

_RTH_START = (9, 30)
_RTH_END   = (16, 0)

# Fallback used only when no loaded plugin answers PluginExecutive
# .aggregate_trading_windows() (i.e. nothing declares an opinion via
# PluginBase.trading_hours()) — plain regular-hours, so "in session" never
# silently defaults to "always off-hours" just because nothing asked for it.
_DEFAULT_SESSION_WINDOW: List[Tuple[dt_time, dt_time]] = [
    (dt_time(*_RTH_START), dt_time(*_RTH_END)),
]

# Known-noisy window, never eligible for an automatic TWS relaunch regardless
# of the in-session/off-hours timeout: observed live, recurring connectivity
# blips around 02:30 Pacific — a fixed 3-hour offset from America/New_York
# (both zones shift DST on the same date), i.e. ~05:30 ET. Widened to a half
# hour either side. This is deliberately separate from the pre-market
# boundary rather than folded into it — IB's technical pre-market session
# starts well before 05:30 ET, so a single "pre-market start" boundary could
# not exclude this window without also excluding legitimate early pre-market
# activity.
_DEFAULT_BLACKOUT_WINDOWS: List[Dict[str, str]] = [
    {"start": "05:15", "end": "05:45"},
]


class WatchdogPlugin(PluginBase):
    """Operational watchdog — see module docstring."""

    VERSION = "1.0.0"
    IS_SYSTEM_PLUGIN = False

    def __init__(self, base_path=None, portfolio=None,
                 shared_holdings=None, message_bus=None):
        super().__init__("watchdog", base_path, portfolio,
                         shared_holdings, message_bus)

        # --- tunable parameters ---
        self.check_interval_seconds:     float = 60.0
        self.bar_staleness_seconds:      float = 600.0    # 10 min without a bar
        self.order_stuck_seconds:        float = 1800.0   # 30 min unresolved
        self.reconcile_interval_seconds: float = 3600.0   # hourly drift check
        self.rth_only:                   bool  = True
        self.webhook_url:                str   = ""       # "" = file sink only
        # Stale-feed auto-remediation: when feeds stay stale, ask the
        # executive to have plugins re-create their live-bar subscriptions
        # (the on_reconnect contract). Observed live 2026-07-15: three of
        # four feeds silent a full session with a healthy socket — detected
        # at the first RTH check, but nothing acted until a human restart.
        self.auto_remediate_stale_feeds:  bool  = True
        self.remediation_cooldown_seconds: float = 900.0  # min gap between nudges

        # Escalation past resubscription, in two further tiers of
        # increasing cost. Both off by default: each bounces something live
        # unattended and should be turned on deliberately, not inherited
        # silently.
        #
        #   1. force_reconnect — a full API disconnect/reconnect cycle,
        #      TWS process untouched, no credentials involved. Cheap and
        #      safe; the natural first escalation past a resubscribe that
        #      didn't work.
        #   2. auto_relaunch_tws — kill and relaunch the TWS process itself
        #      (see relaunch_tws.sh). Last resort: TWS may come back up
        #      asking for credentials again (observed live 2026-07-20 —
        #      confirmed by the launcher's own log: "Daily auto-restart is
        #      not enabled" — so this does NOT achieve unattended recovery
        #      by itself; the alert this fires is what has to reach a human).
        #
        # Both timeout pairs are "timeout, not keep-out" (deliberately
        # looser off-hours, not disabled): while any plugin's declared
        # trading_hours() window is active, escalate reasonably fast;
        # outside all of them, still escalate eventually rather than leave
        # a real fault to fester unnoticed until the next session, just on
        # a much longer clock. reconnect's timeouts sit below relaunch's at
        # every tier so reconnect always gets tried first, on both clocks.
        self.auto_reconnect_on_stale:        bool  = False
        self.reconnect_in_session_timeout_seconds: float = 600.0    # 10 min
        self.reconnect_off_hours_timeout_seconds:  float = 3600.0   # 1 h
        self.reconnect_warmup_seconds:  float = 120.0   # 2 min
        self.reconnect_cooldown_seconds: float = 600.0  # 10 min

        self.auto_relaunch_tws:              bool  = False
        self.relaunch_script_path:           str   = _DEFAULT_RELAUNCH_SCRIPT
        self.relaunch_in_session_timeout_seconds: float = 1200.0    # 20 min
        self.relaunch_off_hours_timeout_seconds:  float = 14400.0   # 4 h
        # After issuing a reconnect or relaunch: don't even evaluate
        # escalation again until warmup elapses (reconnect + resubscribe +
        # one full staleness-check cycle legitimately takes several minutes
        # — the live TWS relaunch on 2026-07-20 took ~10 min before feeds
        # could be judged healthy again; a plain reconnect should be
        # faster). Cooldown is the separate, harder floor on actually
        # repeating the SAME action, checked independently so it still
        # holds even if warmup has elapsed but feeds are stale again.
        self.relaunch_warmup_seconds:  float = 900.0    # 15 min
        self.relaunch_cooldown_seconds: float = 1800.0  # 30 min

        # Known-noisy windows (e.g. the ~02:30 Pacific / ~05:30 ET blip) —
        # never escalate (reconnect OR relaunch) inside these, regardless
        # of the timeouts above. America/New_York wall-clock,
        # [{"start": "HH:MM", "end": "HH:MM"}].
        self.blackout_windows: List[Dict[str, str]] = list(_DEFAULT_BLACKOUT_WINDOWS)

        # --- monitor thread ---
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # --- alert sink ---
        self._webhook_queue: "queue.Queue" = queue.Queue()

        # --- check state ---
        self._stale_alerted: set = set()    # req_ids currently alerted stale
        self._stuck_alerted: set = set()    # order_ids already alerted
        self._last_reconcile: float = 0.0
        self._last_check_at: Optional[str] = None
        self._last_remediation: float = 0.0
        self._remediations: int = 0
        self._stale_since: Optional[float] = None   # epoch; None = currently healthy
        self._last_reconnect: float = 0.0
        self._reconnects: int = 0
        self._last_relaunch: float = 0.0
        self._relaunches: int = 0

        # --- counters (diagnostics) ---
        self._checks_run:     int = 0
        self._alerts_raised:  int = 0   # alerts this plugin raised
        self._alerts_sunk:    int = 0   # alerts received on the channel
        self._webhook_errors: int = 0

    @property
    def description(self) -> str:
        return (
            "Operational watchdog: detects dead live-bar feeds, stuck orders, "
            "and holdings/account drift; sinks the 'alerts' channel to "
            "alerts.jsonl and an optional webhook."
        )

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    def start(self) -> bool:
        saved = self.load_state()
        if saved:
            self.check_interval_seconds     = saved.get("check_interval_seconds",     self.check_interval_seconds)
            self.bar_staleness_seconds      = saved.get("bar_staleness_seconds",      self.bar_staleness_seconds)
            self.order_stuck_seconds        = saved.get("order_stuck_seconds",        self.order_stuck_seconds)
            self.reconcile_interval_seconds = saved.get("reconcile_interval_seconds", self.reconcile_interval_seconds)
            self.rth_only                   = saved.get("rth_only",                   self.rth_only)
            self.webhook_url                = saved.get("webhook_url",                self.webhook_url)
            self.auto_remediate_stale_feeds  = saved.get("auto_remediate_stale_feeds",  self.auto_remediate_stale_feeds)
            self.remediation_cooldown_seconds = saved.get("remediation_cooldown_seconds", self.remediation_cooldown_seconds)
            self.auto_reconnect_on_stale    = saved.get("auto_reconnect_on_stale",    self.auto_reconnect_on_stale)
            self.reconnect_in_session_timeout_seconds = saved.get(
                "reconnect_in_session_timeout_seconds", self.reconnect_in_session_timeout_seconds)
            self.reconnect_off_hours_timeout_seconds = saved.get(
                "reconnect_off_hours_timeout_seconds", self.reconnect_off_hours_timeout_seconds)
            self.reconnect_warmup_seconds   = saved.get("reconnect_warmup_seconds",   self.reconnect_warmup_seconds)
            self.reconnect_cooldown_seconds = saved.get("reconnect_cooldown_seconds", self.reconnect_cooldown_seconds)
            self.auto_relaunch_tws          = saved.get("auto_relaunch_tws",          self.auto_relaunch_tws)
            self.relaunch_script_path       = saved.get("relaunch_script_path",       self.relaunch_script_path)
            self.relaunch_in_session_timeout_seconds = saved.get(
                "relaunch_in_session_timeout_seconds", self.relaunch_in_session_timeout_seconds)
            self.relaunch_off_hours_timeout_seconds = saved.get(
                "relaunch_off_hours_timeout_seconds", self.relaunch_off_hours_timeout_seconds)
            self.blackout_windows           = saved.get("blackout_windows",           self.blackout_windows)
            self.relaunch_warmup_seconds    = saved.get("relaunch_warmup_seconds",    self.relaunch_warmup_seconds)
            self.relaunch_cooldown_seconds  = saved.get("relaunch_cooldown_seconds",  self.relaunch_cooldown_seconds)
            # Persisted so a plugin (or engine) restart doesn't forget a
            # reconnect/relaunch just happened and immediately re-escalate
            # before the real system has had time to recover.
            self._last_reconnect            = saved.get("_last_reconnect", self._last_reconnect)
            self._reconnects                = saved.get("_reconnects", self._reconnects)
            self._last_relaunch             = saved.get("_last_relaunch", self._last_relaunch)
            self._relaunches                = saved.get("_relaunches", self._relaunches)

        # Engine startup already reconciles; first periodic run comes later.
        self._last_reconcile = time.time()

        self.subscribe("alerts", self._on_alert)
        self._start_monitor()
        logger.info(
            f"Watchdog started: interval={self.check_interval_seconds:.0f}s, "
            f"bar_staleness={self.bar_staleness_seconds:.0f}s, "
            f"order_stuck={self.order_stuck_seconds:.0f}s, "
            f"reconcile={self.reconcile_interval_seconds:.0f}s, "
            f"webhook={'set' if self.webhook_url else 'off'}"
        )
        return True

    def stop(self) -> bool:
        self._stop_monitor()
        self.unsubscribe_all()
        self._save_state()
        return True

    def freeze(self) -> bool:
        self._stop_monitor()
        self._save_state()
        return True

    def resume(self) -> bool:
        self._start_monitor()
        return True

    def get_state_for_save(self) -> Dict:
        """Persistable state — also consumed by the executive's auto-save
        (implementing this keeps the executive from overwriting state.json
        with its generic stub)."""
        return {
            "check_interval_seconds":     self.check_interval_seconds,
            "bar_staleness_seconds":      self.bar_staleness_seconds,
            "order_stuck_seconds":        self.order_stuck_seconds,
            "reconcile_interval_seconds": self.reconcile_interval_seconds,
            "rth_only":                   self.rth_only,
            "webhook_url":                self.webhook_url,
            "auto_remediate_stale_feeds":  self.auto_remediate_stale_feeds,
            "remediation_cooldown_seconds": self.remediation_cooldown_seconds,
            "auto_reconnect_on_stale":     self.auto_reconnect_on_stale,
            "reconnect_in_session_timeout_seconds": self.reconnect_in_session_timeout_seconds,
            "reconnect_off_hours_timeout_seconds":  self.reconnect_off_hours_timeout_seconds,
            "reconnect_warmup_seconds":    self.reconnect_warmup_seconds,
            "reconnect_cooldown_seconds":  self.reconnect_cooldown_seconds,
            "auto_relaunch_tws":           self.auto_relaunch_tws,
            "relaunch_script_path":        self.relaunch_script_path,
            "relaunch_in_session_timeout_seconds": self.relaunch_in_session_timeout_seconds,
            "relaunch_off_hours_timeout_seconds":  self.relaunch_off_hours_timeout_seconds,
            "blackout_windows":            self.blackout_windows,
            "relaunch_warmup_seconds":     self.relaunch_warmup_seconds,
            "relaunch_cooldown_seconds":   self.relaunch_cooldown_seconds,
            "_last_reconnect":             self._last_reconnect,
            "_reconnects":                 self._reconnects,
            "_last_relaunch":              self._last_relaunch,
            "_relaunches":                 self._relaunches,
        }

    def _save_state(self) -> None:
        self.save_state(self.get_state_for_save())

    # =========================================================================
    # MONITOR THREAD
    # =========================================================================

    def _start_monitor(self) -> None:
        if self._monitor_thread and self._monitor_thread.is_alive():
            return
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, name="watchdog-monitor", daemon=True,
        )
        self._monitor_thread.start()

    def _stop_monitor(self) -> None:
        self._stop_event.set()
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5.0)
        self._monitor_thread = None

    def _monitor_loop(self) -> None:
        while not self._stop_event.wait(self.check_interval_seconds):
            try:
                self.run_checks()
            except Exception as e:
                logger.error(f"Watchdog check cycle failed: {e}")
            self._drain_webhook_queue()
        self._drain_webhook_queue()   # flush remaining alerts on shutdown

    # =========================================================================
    # CHECKS
    # =========================================================================

    def run_checks(self) -> Dict:
        """Run all checks once. Returns a summary (also used by check_now)."""
        self._checks_run += 1
        self._last_check_at = datetime.now(timezone.utc).isoformat()
        summary = {
            "stale_feeds":   self._check_feed_staleness(),
            "stuck_orders":  self._check_stuck_orders(),
            "reconciled":    self._maybe_reconcile(),
            "checked_at":    self._last_check_at,
        }
        # Independent of rth_only: escalation needs to keep evaluating
        # around the clock (with its own, separate off-hours timeout) even
        # when the ordinary alert/remediation path above is RTH-gated.
        self._check_stale_feed_escalation()
        return summary

    def _check_feed_staleness(self) -> List[Dict]:
        """Alert once per keepUpToDate feed that has gone silent during RTH."""
        if not self.portfolio or not hasattr(self.portfolio, "keep_up_to_date_feeds"):
            return []
        if self.rth_only and not self._in_rth():
            return []
        if not getattr(self.portfolio, "connected", True):
            return []   # disconnection is handled (and alerted) elsewhere

        stale = []
        fresh_req_ids = set()
        for feed in self.portfolio.keep_up_to_date_feeds():
            req_id = feed["req_id"]
            age = feed["seconds_since_last_bar"]
            if age > self.bar_staleness_seconds:
                stale.append(feed)
                if req_id not in self._stale_alerted:
                    self._stale_alerted.add(req_id)
                    self._raise_alert(
                        "stale_feed",
                        f"Live-bar feed for {feed['symbol']} (req_id={req_id}) "
                        f"has delivered no bars for {age / 60:.0f} min during "
                        f"market hours — strategies on this feed are blind; "
                        f"check the subscription and TWS connection",
                        symbol=feed["symbol"], req_id=req_id,
                        seconds_since_last_bar=int(age),
                    )
            else:
                fresh_req_ids.add(req_id)

        recovered = self._stale_alerted & fresh_req_ids
        for req_id in recovered:
            self._stale_alerted.discard(req_id)
            logger.info(f"Watchdog: feed req_id={req_id} delivering bars again")
        # Prune dedupe entries for req_ids that no longer exist (a
        # resubscription replaces them with fresh ids).
        current_ids = {f["req_id"] for f in self.portfolio.keep_up_to_date_feeds()}
        self._stale_alerted &= current_ids

        if stale:
            self._maybe_remediate_stale(stale)
        return stale

    def _maybe_remediate_stale(self, stale_feeds: List[Dict]) -> None:
        """Close the detection→action loop: nudge plugins to resubscribe.

        Rate-limited by remediation_cooldown_seconds so a genuine outage
        produces periodic (cheap) resubscription attempts plus alerts, not
        a flood. Detection alone left three dead feeds unattended for a
        full session (2026-07-15); the nudge is what a human restart was
        doing manually."""
        if not self.auto_remediate_stale_feeds or not self._executive:
            return
        now = time.time()
        if now - self._last_remediation < self.remediation_cooldown_seconds:
            return
        self._last_remediation = now
        self._remediations += 1
        symbols = sorted({f["symbol"] for f in stale_feeds})
        self._raise_alert(
            "stale_feed_remediation",
            f"{len(stale_feeds)} live-bar feed(s) stale ({', '.join(symbols)}) — "
            f"asking plugins to re-create their subscriptions "
            f"(remediation #{self._remediations}, cooldown "
            f"{self.remediation_cooldown_seconds:.0f}s)",
            symbols=symbols,
            remediation_count=self._remediations,
        )
        try:
            notified = self._executive.request_feed_resubscription(
                f"watchdog: feeds stale ({', '.join(symbols)})"
            )
            logger.info(f"Watchdog remediation: resubscription requested "
                        f"from {notified}")
        except Exception as e:
            logger.error(f"Watchdog remediation failed: {e}")

    # =========================================================================
    # ESCALATION — automatic TWS relaunch when resubscription alone isn't
    # clearing the fault (see relaunch_tws.sh for why this bounces the TWS
    # process itself rather than driving its GUI)
    # =========================================================================

    def _check_stale_feed_escalation(self) -> None:
        """Escalate past resubscription in two further tiers, if enabled:
        force_reconnect() (cheap, no credentials) then, if that still
        doesn't clear it, a full TWS relaunch (see module docstring / the
        __init__ comment block for why these are two separate tiers rather
        than jumping straight to the expensive one).

        Runs every check cycle regardless of rth_only (unlike the ordinary
        stale/remediation path above) because the whole point is a longer,
        separate timeout off-hours — "less aggressive, not disabled" — so
        off-hours has to still be evaluated, just against a longer clock.
        """
        if not (self.auto_reconnect_on_stale or self.auto_relaunch_tws):
            return
        if not self.portfolio or not hasattr(self.portfolio, "keep_up_to_date_feeds"):
            return
        if not getattr(self.portfolio, "connected", True):
            # The connection_manager's own reconnect loop already handles
            # this and has, live, recovered on its own (2026-07-17 -> 20
            # outage); acting while we cannot even reach TWS adds risk
            # without a clear benefit, so it's out of scope here.
            return

        now = time.time()

        # Warmup: either action just happened — don't judge staleness at
        # all yet, whichever recovery window is later/longer. Reconnect +
        # resubscribe + one full staleness window legitimately takes
        # several minutes (observed live: ~10 min from reconnect to the
        # first clean check on 2026-07-20, for a full TWS relaunch; a plain
        # reconnect should clear faster, hence its own shorter warmup).
        warming_until = max(self._last_reconnect + self.reconnect_warmup_seconds,
                            self._last_relaunch + self.relaunch_warmup_seconds)
        if now < warming_until:
            self._stale_since = None
            return

        feeds = self.portfolio.keep_up_to_date_feeds()
        if not feeds:
            self._stale_since = None
            return
        stale_now = [f for f in feeds
                     if f["seconds_since_last_bar"] > self.bar_staleness_seconds]

        if not stale_now:
            self._stale_since = None
            return
        if self._stale_since is None:
            self._stale_since = now
        duration = now - self._stale_since

        now_et = datetime.now(_NY)
        if self._in_blackout(now_et):
            # Keep accumulating duration through the blackout — don't reset
            # the clock just because we happen to be inside a known-noisy
            # window right now; only suppress ACTING while inside it.
            return

        in_session = self._in_declared_session(now_et)
        symbols = sorted({f["symbol"] for f in stale_now})

        # Relaunch's timeout is always the longer of the two (see __init__
        # comment), so check it first: if duration has already reached it,
        # jump straight there rather than also firing a reconnect this same
        # cycle — a reconnect will already have had its own chance on an
        # earlier cycle if auto_reconnect_on_stale is on.
        relaunch_timeout = (self.relaunch_in_session_timeout_seconds if in_session
                            else self.relaunch_off_hours_timeout_seconds)
        if self.auto_relaunch_tws and duration >= relaunch_timeout:
            if now - self._last_relaunch < self.relaunch_cooldown_seconds:
                logger.warning(
                    f"Watchdog: relaunch escalation conditions met "
                    f"({duration/60:.0f} min stale, in_session={in_session}) "
                    f"but suppressed by cooldown "
                    f"({self.relaunch_cooldown_seconds:.0f}s since last relaunch)"
                )
                return
            self._relaunch_tws(
                f"{len(stale_now)} feed(s) stale ({', '.join(symbols)}) for "
                f"{duration/60:.0f} min (in_session={in_session}, "
                f"timeout={relaunch_timeout/60:.0f} min) — resubscription "
                f"and reconnect did not clear it"
            )
            return

        reconnect_timeout = (self.reconnect_in_session_timeout_seconds if in_session
                             else self.reconnect_off_hours_timeout_seconds)
        if self.auto_reconnect_on_stale and duration >= reconnect_timeout:
            if now - self._last_reconnect < self.reconnect_cooldown_seconds:
                logger.warning(
                    f"Watchdog: reconnect escalation conditions met "
                    f"({duration/60:.0f} min stale, in_session={in_session}) "
                    f"but suppressed by cooldown "
                    f"({self.reconnect_cooldown_seconds:.0f}s since last reconnect)"
                )
                return
            self._force_reconnect(
                f"{len(stale_now)} feed(s) stale ({', '.join(symbols)}) for "
                f"{duration/60:.0f} min (in_session={in_session}, "
                f"timeout={reconnect_timeout/60:.0f} min) — resubscription "
                f"alone did not clear it"
            )

    def _in_declared_session(self, now: Optional[datetime] = None) -> bool:
        """True if `now` (America/New_York) falls inside the union of every
        loaded plugin's declared PluginBase.trading_hours(), or inside the
        plain regular-hours fallback if no plugin expresses an opinion."""
        now = now or datetime.now(_NY)
        windows = None
        if self._executive:
            try:
                windows = self._executive.aggregate_trading_windows()
            except Exception as e:
                logger.error(f"Watchdog: aggregate_trading_windows() failed: {e}")
        if not windows:
            windows = _DEFAULT_SESSION_WINDOW
        t = now.time()
        return any(start <= t < end for start, end in windows)

    def _in_blackout(self, now: Optional[datetime] = None) -> bool:
        """True if `now` (America/New_York) falls inside a configured
        blackout window — never eligible for reconnect or relaunch regardless of
        the in-session/off-hours timeout."""
        now = now or datetime.now(_NY)
        t = now.time()
        for w in self.blackout_windows:
            try:
                start = dt_time.fromisoformat(w["start"])
                end = dt_time.fromisoformat(w["end"])
            except (KeyError, ValueError, TypeError):
                continue
            if start <= t < end:
                return True
        return False

    def _force_reconnect(self, reason: str) -> None:
        """Schedule PluginExecutive.force_reconnect() — a full API
        disconnect/reconnect cycle, TWS process untouched, no credentials
        involved. The cheap tier: try this before paying the cost of a full
        TWS relaunch (which may end with the engine stuck waiting for a
        human to log back in — see the auto_relaunch_tws comment)."""
        self._last_reconnect = time.time()
        self._reconnects += 1
        self._stale_since = None
        self._save_state()
        self._raise_alert(
            "tws_reconnect_triggered",
            f"Forcing API reconnect (attempt #{self._reconnects}): {reason}",
            reason=reason, reconnect_count=self._reconnects,
        )
        if not self._executive:
            logger.error("Watchdog: cannot force_reconnect — no executive wired")
            return
        try:
            self._executive.force_reconnect(reason)
        except Exception as e:
            logger.error(f"Watchdog: force_reconnect failed: {e}")

    def _relaunch_tws(self, reason: str) -> None:
        """Fire relaunch_tws.sh (kill the TWS process, relaunch its own
        installer script) and record the attempt. Fire-and-forget: the
        script backgrounds the actual TWS process itself, so this only
        needs to survive long enough to issue the SIGTERM and start the
        relaunch, not to wait for TWS to finish coming up."""
        self._last_relaunch = time.time()
        self._relaunches += 1
        self._stale_since = None
        self._save_state()
        self._raise_alert(
            "tws_relaunch_triggered",
            f"Auto-relaunching TWS (attempt #{self._relaunches}): {reason}",
            reason=reason, relaunch_count=self._relaunches,
        )
        try:
            self.plugin_dir.mkdir(parents=True, exist_ok=True)
            log_path = self.plugin_dir / "relaunch.log"
            with open(log_path, "a") as logf:
                logf.write(
                    f"\n--- {datetime.now(timezone.utc).isoformat()} "
                    f"relaunch #{self._relaunches}: {reason} ---\n"
                )
                logf.flush()
                subprocess.Popen(
                    [self.relaunch_script_path],
                    stdout=logf, stderr=subprocess.STDOUT,
                    start_new_session=True,  # outlive our own process group
                )
        except Exception as e:
            logger.error(f"Watchdog: failed to launch relaunch script: {e}")

    def _check_stuck_orders(self) -> List[Dict]:
        """Alert once per order with no fill/terminal status past threshold."""
        if not self.portfolio or not hasattr(self.portfolio, "pending_orders"):
            return []

        now = datetime.now()
        stuck = []
        pending_ids = set()
        for rec in self.portfolio.pending_orders:
            pending_ids.add(rec.order_id)
            submitted = getattr(rec, "submitted_time", None)
            if not submitted:
                continue
            try:
                age = (now - datetime.fromisoformat(submitted)).total_seconds()
            except (TypeError, ValueError):
                continue
            if age > self.order_stuck_seconds:
                stuck.append({"order_id": rec.order_id, "symbol": rec.symbol,
                              "action": rec.action, "age_seconds": int(age)})
                if rec.order_id not in self._stuck_alerted:
                    self._stuck_alerted.add(rec.order_id)
                    self._raise_alert(
                        "stuck_order",
                        f"Order {rec.order_id} ({rec.action} {rec.quantity:g} "
                        f"{rec.symbol} @ {rec.order_type}) unresolved for "
                        f"{age / 60:.0f} min — no fill or terminal status; "
                        f"check TWS",
                        order_id=rec.order_id, symbol=rec.symbol,
                        action=rec.action, age_seconds=int(age),
                    )
        # Forget orders that are no longer pending so a reused id can re-alert
        self._stuck_alerted &= pending_ids
        return stuck

    def _maybe_reconcile(self) -> bool:
        """Run holdings/account reconciliation on its own interval."""
        if not self._executive or not self.portfolio:
            return False
        if not getattr(self.portfolio, "connected", False):
            return False
        if time.time() - self._last_reconcile < self.reconcile_interval_seconds:
            return False

        self._last_reconcile = time.time()
        try:
            report = self._executive.reconcile_with_account()
        except Exception as e:
            logger.error(f"Watchdog reconciliation failed: {e}")
            return False

        if report.get("error"):
            # Refused (typically: position snapshot not ready during a
            # reconnect window). Not a failure and not a full interval's wait:
            # roll the clock back so the next watchdog tick retries, once the
            # snapshot has re-arrived.
            logger.warning(f"Watchdog reconciliation deferred: {report['error']}")
            self._last_reconcile = 0.0
            return False

        discrepancies = report.get("discrepancies", [])
        if discrepancies:
            kinds = sorted({d.get("type", "?") for d in discrepancies})
            self._raise_alert(
                "reconciliation_drift",
                f"Periodic reconciliation found {len(discrepancies)} "
                f"discrepancy(ies) ({', '.join(kinds)}); "
                f"{len(report.get('adjustments', []))} adjustment(s) applied — "
                f"review with 'ibctl reconcile'",
                discrepancies=len(discrepancies),
                types=kinds,
            )
        return True

    def _feed_snapshot(self) -> List[Dict]:
        """Registered keepUpToDate feeds with age, for get_status visibility."""
        if not self.portfolio or not hasattr(self.portfolio, "keep_up_to_date_feeds"):
            return []
        try:
            return [
                {"symbol": f["symbol"],
                 "seconds_since_last_bar": round(f["seconds_since_last_bar"], 1)}
                for f in self.portfolio.keep_up_to_date_feeds()
            ]
        except Exception:
            return []

    def _in_rth(self, now: Optional[datetime] = None) -> bool:
        """US equity regular trading hours (America/New_York), weekdays.
        Market holidays are not modeled (rare false positives)."""
        now = now or datetime.now(_NY)
        if now.weekday() >= 5:
            return False
        minutes = now.hour * 60 + now.minute
        return (_RTH_START[0] * 60 + _RTH_START[1]) <= minutes < (_RTH_END[0] * 60 + _RTH_END[1])

    # =========================================================================
    # ALERT RAISING + SINK
    # =========================================================================

    def _raise_alert(self, kind: str, message: str, **data) -> None:
        """Log at ERROR and publish to the 'alerts' channel."""
        self._alerts_raised += 1
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
        else:
            # No bus (standalone use) — still sink to file so nothing is lost.
            self._write_alert_line({
                "received_at": datetime.now(timezone.utc).isoformat(),
                "channel": "alerts", "publisher": self.name,
                "message_type": "alert",
                "payload": {"kind": kind, "message": message, **data},
            })

    def _on_alert(self, message) -> None:
        """Sink for every message on the 'alerts' channel (incl. our own)."""
        self._alerts_sunk += 1
        entry = {
            "received_at":  datetime.now(timezone.utc).isoformat(),
            "channel":      message.channel,
            "publisher":    message.metadata.source_plugin,
            "message_type": message.metadata.message_type,
            "payload":      message.payload,
        }
        self._write_alert_line(entry)
        if self.webhook_url:
            self._webhook_queue.put(entry)

    def _write_alert_line(self, entry: Dict) -> None:
        try:
            self.plugin_dir.mkdir(parents=True, exist_ok=True)
            with open(self.plugin_dir / "alerts.jsonl", "a") as f:
                f.write(json.dumps(entry, default=str) + "\n")
        except Exception as e:
            logger.error(f"Watchdog failed to write alerts.jsonl: {e}")

    def _drain_webhook_queue(self) -> None:
        while True:
            try:
                entry = self._webhook_queue.get_nowait()
            except queue.Empty:
                return
            try:
                req = urllib.request.Request(
                    self.webhook_url,
                    data=json.dumps(entry, default=str).encode(),
                    headers={"Content-Type": "application/json"},
                )
                urllib.request.urlopen(req, timeout=5.0)
            except Exception as e:
                # Dropped, not retried: a dead webhook must not pile up work.
                # The alert is already in alerts.jsonl and the log.
                self._webhook_errors += 1
                logger.warning(f"Watchdog webhook delivery failed: {e}")

    # =========================================================================
    # SIGNALS (unused — watchdog never trades)
    # =========================================================================

    def calculate_signals(self) -> List[TradeSignal]:
        return []

    # =========================================================================
    # REQUESTS / CLI
    # =========================================================================

    def handle_request(self, request_type: str, payload: Dict) -> Dict:
        if request_type == "get_status":
            return {
                "success": True,
                "data": {
                    "version":         self.VERSION,
                    "monitor_running": bool(self._monitor_thread
                                            and self._monitor_thread.is_alive()),
                    "last_check_at":   self._last_check_at,
                    "checks_run":      self._checks_run,
                    "alerts_raised":   self._alerts_raised,
                    "alerts_sunk":     self._alerts_sunk,
                    "webhook_errors":  self._webhook_errors,
                    "stale_feeds_active":  sorted(self._stale_alerted),
                    "stuck_orders_active": sorted(self._stuck_alerted),
                    "in_rth":          self._in_rth(),
                    "remediations":    self._remediations,
                    "last_remediation": self._last_remediation or None,
                    # The feeds actually registered — an empty list during
                    # market hours means no plugin has a live bar
                    # subscription at all (e.g. a plugin loaded without a
                    # portfolio), which the staleness check cannot see:
                    # zero feeds and all-feeds-fresh look identical to it.
                    "live_bar_feeds": self._feed_snapshot(),
                    "in_declared_session": self._in_declared_session(),
                    "in_blackout":     self._in_blackout(),
                    "stale_since":     self._stale_since,
                    "reconnects":      self._reconnects,
                    "last_reconnect":  self._last_reconnect or None,
                    "relaunches":      self._relaunches,
                    "last_relaunch":   self._last_relaunch or None,
                },
            }

        if request_type == "get_parameters":
            return {
                "success": True,
                "data": {
                    "check_interval_seconds":     self.check_interval_seconds,
                    "bar_staleness_seconds":      self.bar_staleness_seconds,
                    "order_stuck_seconds":        self.order_stuck_seconds,
                    "reconcile_interval_seconds": self.reconcile_interval_seconds,
                    "rth_only":                   self.rth_only,
                    "webhook_url":                self.webhook_url,
                    "auto_remediate_stale_feeds":  self.auto_remediate_stale_feeds,
                    "remediation_cooldown_seconds": self.remediation_cooldown_seconds,
                    "auto_reconnect_on_stale":     self.auto_reconnect_on_stale,
                    "reconnect_in_session_timeout_seconds": self.reconnect_in_session_timeout_seconds,
                    "reconnect_off_hours_timeout_seconds":  self.reconnect_off_hours_timeout_seconds,
                    "reconnect_warmup_seconds":    self.reconnect_warmup_seconds,
                    "reconnect_cooldown_seconds":  self.reconnect_cooldown_seconds,
                    "auto_relaunch_tws":           self.auto_relaunch_tws,
                    "relaunch_script_path":        self.relaunch_script_path,
                    "relaunch_in_session_timeout_seconds": self.relaunch_in_session_timeout_seconds,
                    "relaunch_off_hours_timeout_seconds":  self.relaunch_off_hours_timeout_seconds,
                    "blackout_windows":            self.blackout_windows,
                    "relaunch_warmup_seconds":     self.relaunch_warmup_seconds,
                    "relaunch_cooldown_seconds":   self.relaunch_cooldown_seconds,
                },
            }

        if request_type == "set_parameter":
            key, value = payload.get("key"), payload.get("value")
            if not key or value is None:
                return {"success": False, "message": "Requires 'key' and 'value'"}
            return self._set_parameter(key, value)

        if request_type == "check_now":
            try:
                summary = self.run_checks()
            except Exception as e:
                return {"success": False, "message": f"Check failed: {e}"}
            self._drain_webhook_queue()
            return {"success": True, "data": summary}

        if request_type == "get_alerts":
            count = int(payload.get("count", 20))
            path = self.plugin_dir / "alerts.jsonl"
            if not path.exists():
                return {"success": True, "data": {"alerts": []}}
            try:
                lines = path.read_text().strip().splitlines()[-count:]
                return {"success": True,
                        "data": {"alerts": [json.loads(l) for l in lines]}}
            except Exception as e:
                return {"success": False, "message": f"Failed to read alerts: {e}"}

        return {"success": False, "message": f"Unknown request: {request_type}"}

    def _set_parameter(self, key: str, value) -> Dict:
        try:
            if key == "check_interval_seconds":
                self.check_interval_seconds = max(5.0, float(value))
            elif key == "bar_staleness_seconds":
                self.bar_staleness_seconds = max(60.0, float(value))
            elif key == "order_stuck_seconds":
                self.order_stuck_seconds = max(60.0, float(value))
            elif key == "reconcile_interval_seconds":
                self.reconcile_interval_seconds = max(300.0, float(value))
            elif key == "rth_only":
                self.rth_only = bool(value) if not isinstance(value, str) \
                    else value.strip().lower() in ("1", "true", "yes", "on")
            elif key == "auto_remediate_stale_feeds":
                self.auto_remediate_stale_feeds = bool(value) if not isinstance(value, str) \
                    else value.strip().lower() in ("1", "true", "yes", "on")
            elif key == "remediation_cooldown_seconds":
                self.remediation_cooldown_seconds = max(60.0, float(value))
            elif key == "webhook_url":
                url = str(value).strip()
                if url and not url.startswith(("http://", "https://")):
                    return {"success": False,
                            "message": "webhook_url must be http(s) or empty"}
                self.webhook_url = url
            elif key == "auto_reconnect_on_stale":
                self.auto_reconnect_on_stale = bool(value) if not isinstance(value, str) \
                    else value.strip().lower() in ("1", "true", "yes", "on")
            elif key == "reconnect_in_session_timeout_seconds":
                self.reconnect_in_session_timeout_seconds = max(60.0, float(value))
            elif key == "reconnect_off_hours_timeout_seconds":
                self.reconnect_off_hours_timeout_seconds = max(60.0, float(value))
            elif key == "reconnect_warmup_seconds":
                self.reconnect_warmup_seconds = max(0.0, float(value))
            elif key == "reconnect_cooldown_seconds":
                self.reconnect_cooldown_seconds = max(0.0, float(value))
            elif key == "auto_relaunch_tws":
                self.auto_relaunch_tws = bool(value) if not isinstance(value, str) \
                    else value.strip().lower() in ("1", "true", "yes", "on")
            elif key == "relaunch_script_path":
                self.relaunch_script_path = str(value)
            elif key == "relaunch_in_session_timeout_seconds":
                self.relaunch_in_session_timeout_seconds = max(60.0, float(value))
            elif key == "relaunch_off_hours_timeout_seconds":
                self.relaunch_off_hours_timeout_seconds = max(60.0, float(value))
            elif key == "relaunch_warmup_seconds":
                self.relaunch_warmup_seconds = max(0.0, float(value))
            elif key == "relaunch_cooldown_seconds":
                self.relaunch_cooldown_seconds = max(0.0, float(value))
            elif key == "blackout_windows":
                windows = value
                if isinstance(windows, str):
                    windows = json.loads(windows)
                if not isinstance(windows, list):
                    return {"success": False,
                            "message": "blackout_windows must be a list of "
                                       "{'start': 'HH:MM', 'end': 'HH:MM'}"}
                for w in windows:
                    if not isinstance(w, dict) or "start" not in w or "end" not in w:
                        return {"success": False,
                                "message": f"Invalid blackout window entry: {w}"}
                    dt_time.fromisoformat(w["start"])   # raises ValueError if malformed
                    dt_time.fromisoformat(w["end"])
                self.blackout_windows = windows
            else:
                return {"success": False, "message": f"Unknown parameter: {key}"}
        except (TypeError, ValueError) as exc:
            return {"success": False, "message": f"Invalid value for {key}: {exc}"}
        self._save_state()
        logger.info(f"Watchdog parameter updated: {key}={value}")
        return {"success": True, "message": f"Set {key}={value}"}

    def cli_help(self) -> str:
        return (
            "watchdog commands:\n"
            "  plugin request watchdog get_status {}\n"
            "  plugin request watchdog get_parameters {}\n"
            "  plugin request watchdog check_now {}\n"
            "  plugin request watchdog get_alerts '{\"count\": 20}'\n"
            "  plugin request watchdog set_parameter '{\"key\": \"bar_staleness_seconds\",      \"value\": 600}'\n"
            "  plugin request watchdog set_parameter '{\"key\": \"order_stuck_seconds\",        \"value\": 1800}'\n"
            "  plugin request watchdog set_parameter '{\"key\": \"reconcile_interval_seconds\", \"value\": 3600}'\n"
            "  plugin request watchdog set_parameter '{\"key\": \"check_interval_seconds\",     \"value\": 60}'\n"
            "  plugin request watchdog set_parameter '{\"key\": \"rth_only\",                   \"value\": true}'\n"
            "  plugin request watchdog set_parameter '{\"key\": \"webhook_url\",                \"value\": \"https://...\"}'\n"
        )
