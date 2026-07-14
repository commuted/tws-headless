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
