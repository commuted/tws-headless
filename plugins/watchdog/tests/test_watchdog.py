"""
Unit tests for plugins/watchdog/plugin.py
"""
import json
import time
from datetime import datetime, timedelta
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pytest

from plugins.watchdog.plugin import WatchdogPlugin, _NY


def _make_plugin(tmp_path, **kwargs):
    plugin = WatchdogPlugin(base_path=tmp_path / "watchdog", **kwargs)
    plugin.rth_only = False   # tests must not depend on the wall clock
    return plugin


def _alerts(plugin):
    path = plugin.plugin_dir / "alerts.jsonl"
    if not path.exists():
        return []
    return [json.loads(l) for l in path.read_text().strip().splitlines()]


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

class TestLifecycle:
    def test_start_launches_monitor_thread(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin.start() is True
        assert plugin._monitor_thread is not None
        assert plugin._monitor_thread.is_alive()
        plugin.stop()

    def test_stop_stops_monitor_thread(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        assert plugin.stop() is True
        assert plugin._monitor_thread is None

    def test_freeze_stops_resume_restarts(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        assert plugin.freeze() is True
        assert plugin._monitor_thread is None
        assert plugin.resume() is True
        assert plugin._monitor_thread.is_alive()
        plugin.stop()

    def test_parameters_persist_across_restart(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        plugin.handle_request("set_parameter",
                              {"key": "bar_staleness_seconds", "value": 900})
        plugin.stop()

        plugin2 = _make_plugin(tmp_path)
        plugin2.start()
        assert plugin2.bar_staleness_seconds == 900.0
        plugin2.stop()


# ---------------------------------------------------------------------------
# RTH detection
# ---------------------------------------------------------------------------

class TestInRth:
    def test_weekday_midday_is_rth(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        # Wed 2026-05-13 12:00 New York
        assert plugin._in_rth(datetime(2026, 5, 13, 12, 0, tzinfo=_NY)) is True

    def test_weekday_before_open_is_not_rth(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin._in_rth(datetime(2026, 5, 13, 9, 29, tzinfo=_NY)) is False

    def test_weekday_after_close_is_not_rth(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin._in_rth(datetime(2026, 5, 13, 16, 0, tzinfo=_NY)) is False

    def test_weekend_is_not_rth(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        # Sat 2026-05-16
        assert plugin._in_rth(datetime(2026, 5, 16, 12, 0, tzinfo=_NY)) is False


# ---------------------------------------------------------------------------
# Feed staleness
# ---------------------------------------------------------------------------

class TestFeedStaleness:
    def _portfolio(self, feeds):
        p = Mock()
        p.connected = True
        p.keep_up_to_date_feeds.return_value = feeds
        p.pending_orders = []
        return p

    def test_stale_feed_raises_alert(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.portfolio = self._portfolio(
            [{"req_id": 7, "symbol": "GLD", "seconds_since_last_bar": 1000}]
        )
        stale = plugin._check_feed_staleness()
        assert len(stale) == 1
        alerts = _alerts(plugin)
        assert len(alerts) == 1
        assert alerts[0]["payload"]["kind"] == "stale_feed"
        assert alerts[0]["payload"]["symbol"] == "GLD"

    def test_stale_feed_alerts_once(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.portfolio = self._portfolio(
            [{"req_id": 7, "symbol": "GLD", "seconds_since_last_bar": 1000}]
        )
        plugin._check_feed_staleness()
        plugin._check_feed_staleness()
        assert len(_alerts(plugin)) == 1

    def test_recovered_feed_can_alert_again(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        stale = [{"req_id": 7, "symbol": "GLD", "seconds_since_last_bar": 1000}]
        fresh = [{"req_id": 7, "symbol": "GLD", "seconds_since_last_bar": 5}]
        plugin.portfolio = self._portfolio(stale)
        plugin._check_feed_staleness()
        plugin.portfolio = self._portfolio(fresh)
        plugin._check_feed_staleness()          # recovers, clears dedupe
        plugin.portfolio = self._portfolio(stale)
        plugin._check_feed_staleness()          # goes stale again
        assert len(_alerts(plugin)) == 2

    def test_fresh_feed_no_alert(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.portfolio = self._portfolio(
            [{"req_id": 7, "symbol": "GLD", "seconds_since_last_bar": 30}]
        )
        assert plugin._check_feed_staleness() == []
        assert _alerts(plugin) == []

    def test_disconnected_portfolio_skipped(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        p = self._portfolio(
            [{"req_id": 7, "symbol": "GLD", "seconds_since_last_bar": 1000}]
        )
        p.connected = False
        plugin.portfolio = p
        assert plugin._check_feed_staleness() == []
        assert _alerts(plugin) == []

    def test_outside_rth_skipped_when_rth_only(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.rth_only = True
        plugin._in_rth = lambda now=None: False
        plugin.portfolio = self._portfolio(
            [{"req_id": 7, "symbol": "GLD", "seconds_since_last_bar": 9999}]
        )
        assert plugin._check_feed_staleness() == []
        assert _alerts(plugin) == []

    def test_vanished_req_ids_pruned_from_dedupe(self, tmp_path):
        """A resubscription replaces req_ids; stale-alert dedupe entries for
        vanished ids must not linger (they'd block alerts on nothing)."""
        plugin = _make_plugin(tmp_path)
        plugin.portfolio = self._portfolio(
            [{"req_id": 7, "symbol": "GLD", "seconds_since_last_bar": 1000}]
        )
        plugin._check_feed_staleness()
        assert plugin._stale_alerted == {7}
        # feed 7 replaced by feed 30 (fresh)
        plugin.portfolio = self._portfolio(
            [{"req_id": 30, "symbol": "GLD", "seconds_since_last_bar": 5}]
        )
        plugin._check_feed_staleness()
        assert plugin._stale_alerted == set()


# ---------------------------------------------------------------------------
# Stale-feed auto-remediation — detection closes into action
# ---------------------------------------------------------------------------

class TestStaleFeedRemediation:
    def _stale_plugin(self, tmp_path, **kw):
        plugin = _make_plugin(tmp_path, **kw)
        p = Mock()
        p.connected = True
        p.keep_up_to_date_feeds.return_value = [
            {"req_id": 24, "symbol": "UUP", "seconds_since_last_bar": 29156},
            {"req_id": 25, "symbol": "TLT", "seconds_since_last_bar": 29156},
        ]
        p.pending_orders = []
        plugin.portfolio = p
        executive = Mock()
        executive.request_feed_resubscription.return_value = ["gld_usd_swap"]
        plugin.set_executive(executive)
        return plugin, executive

    def test_stale_feeds_trigger_resubscription(self, tmp_path):
        plugin, executive = self._stale_plugin(tmp_path)
        plugin._check_feed_staleness()
        executive.request_feed_resubscription.assert_called_once()
        reason = executive.request_feed_resubscription.call_args[0][0]
        assert "TLT" in reason and "UUP" in reason
        kinds = [a["payload"]["kind"] for a in _alerts(plugin)]
        assert "stale_feed_remediation" in kinds

    def test_cooldown_limits_nudges(self, tmp_path):
        plugin, executive = self._stale_plugin(tmp_path)
        plugin._check_feed_staleness()
        plugin._check_feed_staleness()     # still stale, inside cooldown
        assert executive.request_feed_resubscription.call_count == 1

    def test_nudges_again_after_cooldown(self, tmp_path):
        import time as _time
        plugin, executive = self._stale_plugin(tmp_path)
        plugin._check_feed_staleness()
        plugin._last_remediation = _time.time() - plugin.remediation_cooldown_seconds - 1
        plugin._stale_alerted.clear()      # feeds re-alert after replacement
        plugin._check_feed_staleness()
        assert executive.request_feed_resubscription.call_count == 2

    def test_disabled_flag_respected(self, tmp_path):
        plugin, executive = self._stale_plugin(tmp_path)
        plugin.auto_remediate_stale_feeds = False
        plugin._check_feed_staleness()
        executive.request_feed_resubscription.assert_not_called()

    def test_no_executive_is_safe(self, tmp_path):
        plugin, _ = self._stale_plugin(tmp_path)
        plugin._executive = None
        plugin._check_feed_staleness()     # must not raise
        assert plugin._remediations == 0


# ---------------------------------------------------------------------------
# Stuck orders
# ---------------------------------------------------------------------------

def _order(order_id, minutes_old, symbol="GLD", action="BUY"):
    return Mock(
        order_id=order_id, symbol=symbol, action=action,
        quantity=40.0, order_type="MOC",
        submitted_time=(datetime.now() - timedelta(minutes=minutes_old)).isoformat(),
    )


class TestStuckOrders:
    def test_old_pending_order_raises_alert(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.portfolio = Mock(pending_orders=[_order(42, minutes_old=45)])
        stuck = plugin._check_stuck_orders()
        assert [s["order_id"] for s in stuck] == [42]
        alerts = _alerts(plugin)
        assert len(alerts) == 1
        assert alerts[0]["payload"]["kind"] == "stuck_order"

    def test_stuck_order_alerts_once(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.portfolio = Mock(pending_orders=[_order(42, minutes_old=45)])
        plugin._check_stuck_orders()
        plugin._check_stuck_orders()
        assert len(_alerts(plugin)) == 1

    def test_recent_order_no_alert(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.portfolio = Mock(pending_orders=[_order(42, minutes_old=5)])
        assert plugin._check_stuck_orders() == []
        assert _alerts(plugin) == []

    def test_resolved_order_dropped_from_dedupe(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.portfolio = Mock(pending_orders=[_order(42, minutes_old=45)])
        plugin._check_stuck_orders()
        plugin.portfolio = Mock(pending_orders=[])
        plugin._check_stuck_orders()
        assert plugin._stuck_alerted == set()


# ---------------------------------------------------------------------------
# Periodic reconciliation
# ---------------------------------------------------------------------------

class TestPeriodicReconcile:
    def _wired(self, tmp_path, discrepancies):
        plugin = _make_plugin(tmp_path)
        plugin.portfolio = Mock(connected=True)
        executive = Mock()
        executive.reconcile_with_account.return_value = {
            "discrepancies": discrepancies,
            "adjustments": [],
        }
        plugin.set_executive(executive)
        plugin._last_reconcile = 0.0   # due now
        return plugin, executive

    def test_reconcile_runs_when_due(self, tmp_path):
        plugin, executive = self._wired(tmp_path, [])
        assert plugin._maybe_reconcile() is True
        executive.reconcile_with_account.assert_called_once()

    def test_discrepancies_raise_alert(self, tmp_path):
        plugin, _ = self._wired(
            tmp_path, [{"type": "unclaimed_position", "symbol": "GLD"}]
        )
        plugin._maybe_reconcile()
        alerts = _alerts(plugin)
        assert len(alerts) == 1
        assert alerts[0]["payload"]["kind"] == "reconciliation_drift"

    def test_clean_reconcile_no_alert(self, tmp_path):
        plugin, _ = self._wired(tmp_path, [])
        plugin._maybe_reconcile()
        assert _alerts(plugin) == []

    def test_not_due_skipped(self, tmp_path):
        plugin, executive = self._wired(tmp_path, [])
        plugin._last_reconcile = time.time()
        assert plugin._maybe_reconcile() is False
        executive.reconcile_with_account.assert_not_called()

    def test_disconnected_skipped(self, tmp_path):
        plugin, executive = self._wired(tmp_path, [])
        plugin.portfolio.connected = False
        assert plugin._maybe_reconcile() is False
        executive.reconcile_with_account.assert_not_called()


# ---------------------------------------------------------------------------
# Alert sink (message bus consumer)
# ---------------------------------------------------------------------------

def _bus_message(payload, publisher="plugin_executive"):
    m = Mock()
    m.channel = "alerts"
    m.payload = payload
    m.metadata = Mock(source_plugin=publisher, message_type="alert")
    return m


class TestAlertSink:
    def test_alert_written_to_jsonl(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._on_alert(_bus_message({"kind": "ib_error", "error_code": 201}))
        alerts = _alerts(plugin)
        assert len(alerts) == 1
        assert alerts[0]["publisher"] == "plugin_executive"
        assert alerts[0]["payload"]["kind"] == "ib_error"

    def test_webhook_queued_when_url_set(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.webhook_url = "https://example.invalid/hook"
        plugin._on_alert(_bus_message({"kind": "stale_feed"}))
        assert plugin._webhook_queue.qsize() == 1

    def test_no_webhook_queue_without_url(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._on_alert(_bus_message({"kind": "stale_feed"}))
        assert plugin._webhook_queue.qsize() == 0

    def test_webhook_drain_posts_json(self, tmp_path, monkeypatch):
        posted = []
        monkeypatch.setattr(
            "plugins.watchdog.plugin.urllib.request.urlopen",
            lambda req, timeout: posted.append(req),
        )
        plugin = _make_plugin(tmp_path)
        plugin.webhook_url = "https://example.invalid/hook"
        plugin._on_alert(_bus_message({"kind": "stale_feed"}))
        plugin._drain_webhook_queue()
        assert len(posted) == 1
        body = json.loads(posted[0].data)
        assert body["payload"]["kind"] == "stale_feed"

    def test_webhook_failure_dropped_not_retried(self, tmp_path, monkeypatch):
        def _boom(req, timeout):
            raise OSError("connection refused")
        monkeypatch.setattr(
            "plugins.watchdog.plugin.urllib.request.urlopen", _boom
        )
        plugin = _make_plugin(tmp_path)
        plugin.webhook_url = "https://example.invalid/hook"
        plugin._on_alert(_bus_message({"kind": "stale_feed"}))
        plugin._drain_webhook_queue()
        assert plugin._webhook_queue.qsize() == 0
        assert plugin._webhook_errors == 1

    def test_end_to_end_via_real_message_bus(self, tmp_path):
        """An alert published by another component reaches alerts.jsonl."""
        from ib.message_bus import MessageBus

        bus = MessageBus()
        plugin = _make_plugin(tmp_path, message_bus=bus)
        plugin.start()
        try:
            bus.publish(
                channel="alerts",
                payload={"kind": "reconnected", "message": "test"},
                publisher="plugin_executive",
                message_type="alert",
            )
            alerts = _alerts(plugin)
            assert len(alerts) == 1
            assert alerts[0]["payload"]["kind"] == "reconnected"
        finally:
            plugin.stop()


# ---------------------------------------------------------------------------
# Requests / parameters
# ---------------------------------------------------------------------------

class TestRequests:
    def test_get_status(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        result = plugin.handle_request("get_status", {})
        assert result["success"] is True
        assert result["data"]["monitor_running"] is True
        plugin.stop()

    def test_get_status_exposes_feed_snapshot(self, tmp_path):
        """Zero feeds and all-feeds-fresh are identical to the staleness
        check; get_status must show what is actually registered."""
        plugin = _make_plugin(tmp_path)
        plugin.portfolio = Mock()
        plugin.portfolio.keep_up_to_date_feeds.return_value = [
            {"req_id": 7, "symbol": "GLD", "seconds_since_last_bar": 12.34},
        ]
        result = plugin.handle_request("get_status", {})
        feeds = result["data"]["live_bar_feeds"]
        assert feeds == [{"symbol": "GLD", "seconds_since_last_bar": 12.3}]

    def test_get_status_empty_feed_snapshot_without_portfolio(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("get_status", {})
        assert result["data"]["live_bar_feeds"] == []

    def test_check_now_runs_checks(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.portfolio = Mock(
            connected=True, pending_orders=[],
        )
        plugin.portfolio.keep_up_to_date_feeds.return_value = []
        result = plugin.handle_request("check_now", {})
        assert result["success"] is True
        assert plugin._checks_run == 1

    def test_get_alerts_tails_file(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        for i in range(5):
            plugin._on_alert(_bus_message({"kind": f"k{i}"}))
        result = plugin.handle_request("get_alerts", {"count": 2})
        assert result["success"] is True
        kinds = [a["payload"]["kind"] for a in result["data"]["alerts"]]
        assert kinds == ["k3", "k4"]

    def test_set_invalid_webhook_url_rejected(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request(
            "set_parameter", {"key": "webhook_url", "value": "ftp://nope"}
        )
        assert result["success"] is False

    def test_unknown_parameter_rejected(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request(
            "set_parameter", {"key": "nonexistent", "value": 1}
        )
        assert result["success"] is False

    def test_unknown_request(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("nonexistent", {})
        assert result["success"] is False


# ---------------------------------------------------------------------------
# Automatic TWS relaunch escalation
# ---------------------------------------------------------------------------

class TestRelaunchEscalation:
    def _stale_plugin(self, tmp_path, seconds_since_last_bar=99999, **kw):
        plugin = _make_plugin(tmp_path, **kw)
        plugin.auto_relaunch_tws = True
        p = Mock()
        p.connected = True
        p.keep_up_to_date_feeds.return_value = [
            {"req_id": 1, "symbol": "GLD", "seconds_since_last_bar": seconds_since_last_bar},
        ]
        p.pending_orders = []
        plugin.portfolio = p
        plugin._relaunch_tws = Mock(wraps=plugin._relaunch_tws)
        return plugin

    def test_disabled_by_default(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin.auto_relaunch_tws is False

    def test_no_escalation_while_fresh(self, tmp_path):
        plugin = self._stale_plugin(tmp_path, seconds_since_last_bar=5)
        plugin.relaunch_in_session_timeout_seconds = 1.0
        plugin._check_stale_feed_escalation()
        plugin._relaunch_tws.assert_not_called()

    def test_escalates_once_timeout_exceeded(self, tmp_path, monkeypatch):
        plugin = self._stale_plugin(tmp_path)
        plugin.relaunch_in_session_timeout_seconds = 1.0
        plugin.relaunch_off_hours_timeout_seconds = 1.0
        plugin._in_declared_session = lambda now=None: True
        plugin._in_blackout = lambda now=None: False
        # First check starts the stale-since clock; nothing fires yet.
        plugin._check_stale_feed_escalation()
        plugin._relaunch_tws.assert_not_called()
        # Advance past the 1s timeout.
        plugin._stale_since = time.time() - 2.0
        plugin._check_stale_feed_escalation()
        plugin._relaunch_tws.assert_called_once()

    def test_off_hours_uses_looser_timeout(self, tmp_path):
        """A duration that would trigger in-session must NOT trigger
        off-hours when the off-hours timeout is longer — "timeout, not
        keep-out" only works if the two clocks are actually independent."""
        plugin = self._stale_plugin(tmp_path)
        plugin.relaunch_in_session_timeout_seconds = 5.0
        plugin.relaunch_off_hours_timeout_seconds = 99999.0
        plugin._in_declared_session = lambda now=None: False
        plugin._in_blackout = lambda now=None: False
        plugin._stale_since = time.time() - 10.0   # past in-session, not off-hours
        plugin._check_stale_feed_escalation()
        plugin._relaunch_tws.assert_not_called()

    def test_blackout_suppresses_without_resetting_clock(self, tmp_path):
        plugin = self._stale_plugin(tmp_path)
        plugin.relaunch_in_session_timeout_seconds = 1.0
        plugin._in_declared_session = lambda now=None: True
        plugin._in_blackout = lambda now=None: True
        started = time.time() - 5.0
        plugin._stale_since = started
        plugin._check_stale_feed_escalation()
        plugin._relaunch_tws.assert_not_called()
        assert plugin._stale_since == started   # clock kept running, not reset

    def test_cooldown_blocks_second_relaunch(self, tmp_path):
        plugin = self._stale_plugin(tmp_path)
        plugin.relaunch_in_session_timeout_seconds = 1.0
        plugin.relaunch_cooldown_seconds = 99999.0
        plugin.relaunch_warmup_seconds = 0.0
        plugin._in_declared_session = lambda now=None: True
        plugin._in_blackout = lambda now=None: False
        plugin._last_relaunch = time.time() - 10.0   # a "recent" relaunch
        plugin._stale_since = time.time() - 5.0
        plugin._check_stale_feed_escalation()
        plugin._relaunch_tws.assert_not_called()

    def test_warmup_suppresses_evaluation_and_resets_clock(self, tmp_path):
        plugin = self._stale_plugin(tmp_path)
        plugin.relaunch_warmup_seconds = 99999.0
        plugin._last_relaunch = time.time() - 5.0
        plugin._stale_since = time.time() - 999.0   # would otherwise be well past any timeout
        plugin._check_stale_feed_escalation()
        plugin._relaunch_tws.assert_not_called()
        assert plugin._stale_since is None

    def test_disconnected_never_escalates(self, tmp_path):
        plugin = self._stale_plugin(tmp_path)
        plugin.portfolio.connected = False
        plugin.relaunch_in_session_timeout_seconds = 1.0
        plugin._stale_since = time.time() - 999.0
        plugin._check_stale_feed_escalation()
        plugin._relaunch_tws.assert_not_called()

    def test_recovery_clears_stale_since(self, tmp_path):
        plugin = self._stale_plugin(tmp_path, seconds_since_last_bar=99999)
        plugin._check_stale_feed_escalation()
        assert plugin._stale_since is not None
        plugin.portfolio.keep_up_to_date_feeds.return_value = [
            {"req_id": 1, "symbol": "GLD", "seconds_since_last_bar": 5},
        ]
        plugin._check_stale_feed_escalation()
        assert plugin._stale_since is None

    def test_relaunch_invokes_configured_script(self, tmp_path, monkeypatch):
        """_relaunch_tws must actually shell out to relaunch_script_path,
        detached from our own process group, and record the attempt."""
        plugin = _make_plugin(tmp_path)
        plugin.relaunch_script_path = "/bin/true"
        captured = {}

        def _fake_popen(args, **kwargs):
            captured["args"] = args
            captured["kwargs"] = kwargs
            return Mock()

        monkeypatch.setattr("plugins.watchdog.plugin.subprocess.Popen", _fake_popen)
        before = plugin._relaunches
        plugin._relaunch_tws("test reason")

        assert captured["args"] == ["/bin/true"]
        assert captured["kwargs"]["start_new_session"] is True
        assert plugin._relaunches == before + 1
        assert plugin._stale_since is None
        alerts = _alerts(plugin)
        assert alerts[-1]["payload"]["kind"] == "tws_relaunch_triggered"

    def test_relaunch_failure_does_not_raise(self, tmp_path, monkeypatch):
        plugin = _make_plugin(tmp_path)

        def _boom(*a, **kw):
            raise OSError("no such file")

        monkeypatch.setattr("plugins.watchdog.plugin.subprocess.Popen", _boom)
        plugin._relaunch_tws("test reason")   # must not raise


# ---------------------------------------------------------------------------
# Reconnect escalation — the cheap, no-credentials tier between
# resubscription and a full TWS relaunch
# ---------------------------------------------------------------------------

class TestReconnectEscalation:
    def _stale_plugin(self, tmp_path, seconds_since_last_bar=99999, **kw):
        plugin = _make_plugin(tmp_path, **kw)
        plugin.auto_reconnect_on_stale = True
        p = Mock()
        p.connected = True
        p.keep_up_to_date_feeds.return_value = [
            {"req_id": 1, "symbol": "GLD", "seconds_since_last_bar": seconds_since_last_bar},
        ]
        p.pending_orders = []
        plugin.portfolio = p
        plugin._force_reconnect = Mock(wraps=plugin._force_reconnect)
        plugin._executive = Mock()
        plugin._executive.force_reconnect = Mock(return_value=True)
        return plugin

    def test_disabled_by_default(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin.auto_reconnect_on_stale is False

    def test_escalates_once_timeout_exceeded(self, tmp_path):
        plugin = self._stale_plugin(tmp_path)
        plugin.reconnect_in_session_timeout_seconds = 1.0
        plugin._in_declared_session = lambda now=None: True
        plugin._in_blackout = lambda now=None: False
        plugin._check_stale_feed_escalation()
        plugin._force_reconnect.assert_not_called()
        plugin._stale_since = time.time() - 2.0
        plugin._check_stale_feed_escalation()
        plugin._force_reconnect.assert_called_once()

    def test_off_hours_uses_looser_timeout(self, tmp_path):
        plugin = self._stale_plugin(tmp_path)
        plugin.reconnect_in_session_timeout_seconds = 5.0
        plugin.reconnect_off_hours_timeout_seconds = 99999.0
        plugin._in_declared_session = lambda now=None: False
        plugin._in_blackout = lambda now=None: False
        plugin._stale_since = time.time() - 10.0
        plugin._check_stale_feed_escalation()
        plugin._force_reconnect.assert_not_called()

    def test_cooldown_blocks_second_reconnect(self, tmp_path):
        plugin = self._stale_plugin(tmp_path)
        plugin.reconnect_in_session_timeout_seconds = 1.0
        plugin.reconnect_cooldown_seconds = 99999.0
        plugin.reconnect_warmup_seconds = 0.0
        plugin._in_declared_session = lambda now=None: True
        plugin._in_blackout = lambda now=None: False
        plugin._last_reconnect = time.time() - 10.0
        plugin._stale_since = time.time() - 5.0
        plugin._check_stale_feed_escalation()
        plugin._force_reconnect.assert_not_called()

    def test_warmup_suppresses_evaluation_and_resets_clock(self, tmp_path):
        plugin = self._stale_plugin(tmp_path)
        plugin.reconnect_warmup_seconds = 99999.0
        plugin._last_reconnect = time.time() - 5.0
        plugin._stale_since = time.time() - 999.0
        plugin._check_stale_feed_escalation()
        plugin._force_reconnect.assert_not_called()
        assert plugin._stale_since is None

    def test_force_reconnect_invokes_executive_and_records_attempt(self, tmp_path):
        plugin = self._stale_plugin(tmp_path)
        before = plugin._reconnects
        plugin._force_reconnect("test reason")

        plugin._executive.force_reconnect.assert_called_once_with("test reason")
        assert plugin._reconnects == before + 1
        assert plugin._stale_since is None
        alerts = _alerts(plugin)
        assert alerts[-1]["payload"]["kind"] == "tws_reconnect_triggered"

    def test_force_reconnect_without_executive_does_not_raise(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._executive = None
        plugin._force_reconnect("test reason")   # must not raise

    def test_force_reconnect_executive_failure_does_not_raise(self, tmp_path):
        plugin = self._stale_plugin(tmp_path)
        plugin._executive.force_reconnect = Mock(side_effect=RuntimeError("boom"))
        plugin._force_reconnect("test reason")   # must not raise


# ---------------------------------------------------------------------------
# Escalation ladder ordering — reconnect (cheap) must always get first
# refusal before relaunch (expensive, may need a human to log back in)
# ---------------------------------------------------------------------------

class TestEscalationLadderOrdering:
    def _plugin(self, tmp_path, seconds_since_last_bar=99999):
        plugin = _make_plugin(tmp_path)
        p = Mock()
        p.connected = True
        p.keep_up_to_date_feeds.return_value = [
            {"req_id": 1, "symbol": "GLD", "seconds_since_last_bar": seconds_since_last_bar},
        ]
        p.pending_orders = []
        plugin.portfolio = p
        plugin._executive = Mock()
        plugin._executive.force_reconnect = Mock(return_value=True)
        plugin._force_reconnect = Mock(wraps=plugin._force_reconnect)
        plugin._relaunch_tws = Mock(wraps=plugin._relaunch_tws)
        plugin._in_declared_session = lambda now=None: True
        plugin._in_blackout = lambda now=None: False
        return plugin

    def test_reconnect_fires_before_relaunch_threshold_reached(self, tmp_path):
        """Both enabled, duration past reconnect's timeout but not yet
        relaunch's (much larger) one: only reconnect should fire."""
        plugin = self._plugin(tmp_path)
        plugin.auto_reconnect_on_stale = True
        plugin.auto_relaunch_tws = True
        plugin.reconnect_in_session_timeout_seconds = 10.0
        plugin.relaunch_in_session_timeout_seconds = 99999.0
        plugin._stale_since = time.time() - 20.0

        plugin._check_stale_feed_escalation()

        plugin._force_reconnect.assert_called_once()
        plugin._relaunch_tws.assert_not_called()

    def test_relaunch_fires_once_its_own_threshold_reached(self, tmp_path):
        """Once duration passes relaunch's (larger) threshold too, relaunch
        fires instead of a redundant reconnect — reconnect already had its
        chance on an earlier cycle."""
        plugin = self._plugin(tmp_path)
        plugin.auto_reconnect_on_stale = True
        plugin.auto_relaunch_tws = True
        plugin.reconnect_in_session_timeout_seconds = 10.0
        plugin.relaunch_in_session_timeout_seconds = 20.0
        plugin._stale_since = time.time() - 30.0

        plugin._check_stale_feed_escalation()

        plugin._relaunch_tws.assert_called_once()
        plugin._force_reconnect.assert_not_called()

    def test_relaunch_only_flag_skips_reconnect_tier_entirely(self, tmp_path):
        """auto_reconnect_on_stale off, auto_relaunch_tws on: relaunch must
        still fire on its own — the reconnect tier being disabled must not
        block the ladder (this is exactly the pre-existing, tested
        single-tier behavior; it must survive the ladder refactor)."""
        plugin = self._plugin(tmp_path)
        plugin.auto_reconnect_on_stale = False
        plugin.auto_relaunch_tws = True
        plugin.relaunch_in_session_timeout_seconds = 10.0
        plugin._stale_since = time.time() - 20.0

        plugin._check_stale_feed_escalation()

        plugin._relaunch_tws.assert_called_once()

    def test_reconnect_only_flag_never_escalates_to_relaunch(self, tmp_path):
        plugin = self._plugin(tmp_path)
        plugin.auto_reconnect_on_stale = True
        plugin.auto_relaunch_tws = False
        plugin.reconnect_in_session_timeout_seconds = 10.0
        plugin._stale_since = time.time() - 99999.0   # would be well past any relaunch timeout

        plugin._check_stale_feed_escalation()

        plugin._force_reconnect.assert_called_once()
        plugin._relaunch_tws.assert_not_called()

    def test_relaunch_warmup_also_suppresses_reconnect_tier(self, tmp_path):
        """A relaunch just fired: the whole ladder should hold off, not just
        the relaunch tier — a fresh TWS process needs time to settle before
        ANY further action is judged, including the cheaper one."""
        plugin = self._plugin(tmp_path)
        plugin.auto_reconnect_on_stale = True
        plugin.auto_relaunch_tws = True
        plugin.reconnect_in_session_timeout_seconds = 1.0
        plugin.relaunch_warmup_seconds = 99999.0
        plugin._last_relaunch = time.time() - 5.0
        plugin._stale_since = time.time() - 999.0

        plugin._check_stale_feed_escalation()

        plugin._force_reconnect.assert_not_called()
        plugin._relaunch_tws.assert_not_called()
        assert plugin._stale_since is None

    def test_reconnect_warmup_also_suppresses_relaunch_tier(self, tmp_path):
        """Symmetric case: a reconnect just fired, relaunch's own threshold
        has technically been exceeded already, but the ladder should still
        give the cheap reconnect a chance to prove itself first."""
        plugin = self._plugin(tmp_path)
        plugin.auto_reconnect_on_stale = True
        plugin.auto_relaunch_tws = True
        plugin.relaunch_in_session_timeout_seconds = 1.0
        plugin.reconnect_warmup_seconds = 99999.0
        plugin._last_reconnect = time.time() - 5.0
        plugin._stale_since = time.time() - 999.0

        plugin._check_stale_feed_escalation()

        plugin._force_reconnect.assert_not_called()
        plugin._relaunch_tws.assert_not_called()
        assert plugin._stale_since is None


# ---------------------------------------------------------------------------
# In-session / blackout window evaluation
# ---------------------------------------------------------------------------

class TestSessionAndBlackoutWindows:
    def _at(self, hour, minute):
        return datetime(2026, 7, 20, hour, minute, tzinfo=_NY)

    def test_default_blackout_covers_0530_et(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin._in_blackout(self._at(5, 30)) is True
        assert plugin._in_blackout(self._at(6, 0)) is False

    def test_custom_blackout_window(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.blackout_windows = [{"start": "12:00", "end": "12:15"}]
        assert plugin._in_blackout(self._at(12, 5)) is True
        assert plugin._in_blackout(self._at(5, 30)) is False   # default replaced, not merged

    def test_falls_back_to_regular_hours_with_no_executive(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._executive = None
        assert plugin._in_declared_session(self._at(10, 0)) is True
        assert plugin._in_declared_session(self._at(20, 0)) is False

    def test_uses_plugin_aggregated_window_when_available(self, tmp_path):
        from datetime import time as dt_time
        plugin = _make_plugin(tmp_path)
        executive = Mock()
        executive.aggregate_trading_windows.return_value = [
            (dt_time(4, 0), dt_time(9, 30)),   # pre-market only, no regular session
        ]
        plugin.set_executive(executive)
        assert plugin._in_declared_session(self._at(6, 0)) is True
        assert plugin._in_declared_session(self._at(11, 0)) is False

    def test_aggregate_error_falls_back_to_default(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        executive = Mock()
        executive.aggregate_trading_windows.side_effect = RuntimeError("boom")
        plugin.set_executive(executive)
        assert plugin._in_declared_session(self._at(10, 0)) is True
