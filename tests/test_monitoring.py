"""
Tests for the operational monitoring chain added for live readiness:

  - Portfolio keepUpToDate feed-activity tracking (staleness data source)
  - ConnectionManager.on_reconnected (fires once after an unexpected drop)
  - PluginExecutive.publish_alert / notify_reconnected / IB-error alerts
"""

import time
from pathlib import Path
from unittest.mock import Mock, MagicMock

import pytest

from ib.portfolio import Portfolio
from ib.connection_manager import ConnectionManager, ConnectionConfig
from ib.plugin_executive import PluginExecutive, ExecutionMode
from ib.message_bus import MessageBus
from plugins.base import PluginBase, PluginState


# ---------------------------------------------------------------------------
# Portfolio keepUpToDate feed tracking
# ---------------------------------------------------------------------------

def _bare_portfolio():
    p = Portfolio.__new__(Portfolio)
    p._historical_requests = {}
    p._kutd_symbols = {}
    p._kutd_last_bar = {}
    return p


class TestKeepUpToDateFeedTracking:
    def test_keep_up_to_date_request_registers_feed(self):
        p = _bare_portfolio()
        p.get_next_req_id = lambda: 33
        p.reqHistoricalData = Mock()

        p.request_historical_data(
            contract=Mock(symbol="GLD"), keep_up_to_date=True,
        )

        assert p._kutd_symbols == {33: "GLD"}
        feeds = p.keep_up_to_date_feeds()
        assert feeds[0]["symbol"] == "GLD"
        assert feeds[0]["seconds_since_last_bar"] < 1.0

    def test_one_shot_request_not_registered(self):
        p = _bare_portfolio()
        p.get_next_req_id = lambda: 34
        p.reqHistoricalData = Mock()

        p.request_historical_data(contract=Mock(symbol="GLD"))

        assert p._kutd_symbols == {}
        assert p.keep_up_to_date_feeds() == []

    def test_bar_arrival_refreshes_activity(self):
        p = _bare_portfolio()
        p._historical_requests[9] = (None, None, None, True, [])
        p._kutd_symbols[9] = "GLD"
        p._kutd_last_bar[9] = time.time() - 500

        assert p.keep_up_to_date_feeds()[0]["seconds_since_last_bar"] == \
            pytest.approx(500, abs=5)

        p.historicalDataUpdate(9, MagicMock())
        assert p.keep_up_to_date_feeds()[0]["seconds_since_last_bar"] < 1.0

    def test_backfill_bar_also_refreshes_activity(self):
        p = _bare_portfolio()
        p._historical_requests[9] = (None, None, None, True, [])
        p._kutd_symbols[9] = "GLD"
        p._kutd_last_bar[9] = time.time() - 500

        p.historicalData(9, MagicMock())
        assert p.keep_up_to_date_feeds()[0]["seconds_since_last_bar"] < 1.0

    def test_cancel_removes_feed(self):
        p = _bare_portfolio()
        p.cancelHistoricalData = Mock()
        p._historical_requests[9] = (None, None, None, True, [])
        p._kutd_symbols[9] = "GLD"
        p._kutd_last_bar[9] = time.time()

        p.cancel_historical_data(9)
        assert p.keep_up_to_date_feeds() == []


# ---------------------------------------------------------------------------
# ConnectionManager.on_reconnected
# ---------------------------------------------------------------------------

def _manager():
    portfolio = Mock()
    portfolio._callbacks = {}
    portfolio._stream_subscriptions = {}
    portfolio._bar_subscriptions = {}
    manager = ConnectionManager(
        portfolio, ConnectionConfig(auto_reconnect=False),
    )
    # Neutralize asyncio task startup and stream recovery for sync testing
    manager._start_keepalive_task = lambda: None
    manager._start_health_task = lambda: None
    manager._recover_streams = lambda: None
    return manager


class TestOnReconnected:
    def test_not_fired_on_initial_connect(self):
        manager = _manager()
        fired = []
        manager.on_reconnected = lambda: fired.append(True)

        manager._on_connected()
        assert fired == []

    def test_fired_once_after_unexpected_drop(self):
        manager = _manager()
        fired = []
        manager.on_reconnected = lambda: fired.append(True)

        manager._on_connected()          # initial connect
        manager._handle_disconnection()  # unexpected drop
        manager._on_connected()          # recovered
        assert fired == [True]

        manager._on_connected()          # later connect without a drop
        assert fired == [True]

    def test_fired_after_each_drop(self):
        manager = _manager()
        fired = []
        manager.on_reconnected = lambda: fired.append(True)

        for _ in range(2):
            manager._handle_disconnection()
            manager._on_connected()
        assert fired == [True, True]


# ---------------------------------------------------------------------------
# PluginExecutive alerts + reconnect notification
# ---------------------------------------------------------------------------

class ReconnectProbePlugin(PluginBase):
    """Minimal plugin recording on_reconnect calls."""

    def __init__(self, name, base_path):
        super().__init__(name, base_path=base_path)
        self.reconnect_calls = 0

    @property
    def description(self):
        return "reconnect probe"

    def start(self):
        return True

    def stop(self):
        return True

    def freeze(self):
        return True

    def resume(self):
        return True

    def handle_request(self, request_type, payload):
        return {"success": False}

    def calculate_signals(self):
        return []

    def on_reconnect(self):
        self.reconnect_calls += 1


class TestExecutiveAlerts:
    def _wired(self, tmp_path):
        bus = MessageBus()
        received = []
        bus.subscribe("alerts", received.append, subscriber="test")
        executive = PluginExecutive(None, None, message_bus=bus)
        return executive, received, tmp_path

    def test_publish_alert_reaches_channel(self, tmp_path):
        executive, received, _ = self._wired(tmp_path)
        executive.publish_alert("test_kind", {"detail": 1})

        assert len(received) == 1
        payload = received[0].payload
        assert payload["kind"] == "test_kind"
        assert payload["detail"] == 1
        assert "timestamp" in payload
        assert received[0].metadata.message_type == "alert"

    def test_notify_reconnected_calls_started_plugins_only(self, tmp_path):
        executive, received, base = self._wired(tmp_path)
        started = ReconnectProbePlugin("p_started", base / "a")
        frozen = ReconnectProbePlugin("p_frozen", base / "b")
        executive.register_plugin(started, execution_mode=ExecutionMode.MANUAL)
        executive.register_plugin(frozen, execution_mode=ExecutionMode.MANUAL)
        started._state = PluginState.STARTED
        frozen._state = PluginState.FROZEN

        executive.notify_reconnected()

        assert started.reconnect_calls == 1
        assert frozen.reconnect_calls == 0
        kinds = [m.payload["kind"] for m in received]
        assert "reconnected" in kinds
        alert = next(m for m in received if m.payload["kind"] == "reconnected")
        # The executive's built-in _unassigned system plugin is also STARTED
        assert "p_started" in alert.payload["plugins_notified"]
        assert "p_frozen" not in alert.payload["plugins_notified"]

    def test_request_feed_resubscription_nudges_started_plugins(self, tmp_path):
        """The watchdog's stale-feed remediation lever: same on_reconnect
        contract, distinct alert kind so the record distinguishes a nudge
        from a reconnect."""
        executive, received, base = self._wired(tmp_path)
        started = ReconnectProbePlugin("p_live", base / "a")
        executive.register_plugin(started, execution_mode=ExecutionMode.MANUAL)
        started._state = PluginState.STARTED

        notified = executive.request_feed_resubscription("watchdog: feeds stale (UUP)")

        assert started.reconnect_calls == 1
        assert "p_live" in notified
        alert = next(m for m in received
                     if m.payload["kind"] == "feed_resubscription")
        assert "UUP" in alert.payload["message"]

    def test_notify_reconnected_survives_plugin_error(self, tmp_path):
        executive, received, base = self._wired(tmp_path)
        bad = ReconnectProbePlugin("p_bad", base / "a")
        good = ReconnectProbePlugin("p_good", base / "b")
        bad.on_reconnect = Mock(side_effect=RuntimeError("boom"))
        executive.register_plugin(bad, execution_mode=ExecutionMode.MANUAL)
        executive.register_plugin(good, execution_mode=ExecutionMode.MANUAL)
        bad._state = PluginState.STARTED
        good._state = PluginState.STARTED

        executive.notify_reconnected()
        assert good.reconnect_calls == 1

    def test_plugin_attributed_ib_error_publishes_alert(self, tmp_path):
        executive, received, base = self._wired(tmp_path)
        plugin = ReconnectProbePlugin("p_orders", base / "a")
        executive.register_plugin(plugin, execution_mode=ExecutionMode.MANUAL)
        executive.register_order_for_plugin(55, "p_orders")

        executive._handle_ib_error_for_plugins(55, 201, "Order rejected")

        kinds = [m.payload["kind"] for m in received]
        assert kinds == ["ib_error"]
        payload = received[0].payload
        assert payload["error_code"] == 201
        assert payload["plugins"] == ["p_orders"]
        assert payload["is_order_error"] is True

    def test_unattributed_ib_error_no_alert(self, tmp_path):
        executive, received, _ = self._wired(tmp_path)
        executive._handle_ib_error_for_plugins(999, 201, "Order rejected")
        assert received == []


class TestRegisterPluginAttachesPortfolio:
    """Plugins loaded from file or a factory arrive without a portfolio and
    would otherwise run in 'test mode' — no warm-up, no subscriptions, no
    orders — while reporting a healthy started state."""

    def test_bare_plugin_gets_executive_portfolio(self, tmp_path):
        portfolio = MagicMock()
        executive = PluginExecutive(portfolio, None, message_bus=MessageBus())
        plugin = ReconnectProbePlugin("p_bare", tmp_path / "a")
        assert plugin.portfolio is None

        executive.register_plugin(plugin, execution_mode=ExecutionMode.MANUAL)
        assert plugin.portfolio is portfolio

    def test_explicit_portfolio_not_overridden(self, tmp_path):
        exec_portfolio = MagicMock()
        own_portfolio = Mock()
        executive = PluginExecutive(exec_portfolio, None, message_bus=MessageBus())
        plugin = ReconnectProbePlugin("p_own", tmp_path / "a")
        plugin.portfolio = own_portfolio

        executive.register_plugin(plugin, execution_mode=ExecutionMode.MANUAL)
        assert plugin.portfolio is own_portfolio

    def test_no_executive_portfolio_leaves_none(self, tmp_path):
        executive = PluginExecutive(None, None, message_bus=MessageBus())
        plugin = ReconnectProbePlugin("p_none", tmp_path / "a")

        executive.register_plugin(plugin, execution_mode=ExecutionMode.MANUAL)
        assert plugin.portfolio is None


class TestAutoSaveDoesNotClobber:
    """The executive's periodic auto-save must never overwrite a plugin's
    state.json with a generic stub — that destroyed strategy state (regime,
    counters, pending-order crash-recovery records) every health cycle.
    Plugins without get_state_for_save() are skipped entirely."""

    def _registered(self, tmp_path, plugin):
        executive = PluginExecutive(None, None, message_bus=MessageBus())
        executive.register_plugin(plugin, execution_mode=ExecutionMode.MANUAL)
        plugin._state = PluginState.STARTED
        return executive

    def test_plugin_without_hook_is_skipped(self, tmp_path):
        plugin = ReconnectProbePlugin("p_plain", tmp_path / "a")
        assert not hasattr(plugin, "get_state_for_save")
        executive = self._registered(tmp_path, plugin)

        plugin.save_state({"my": "state"})     # plugin-managed content
        executive._auto_save_all_states()

        assert plugin.load_state() == {"my": "state"}   # untouched

    def test_plugin_with_hook_is_saved(self, tmp_path):
        plugin = ReconnectProbePlugin("p_hooked", tmp_path / "a")
        plugin.get_state_for_save = lambda: {"holding": True, "pending": {"118": "BUY"}}
        executive = self._registered(tmp_path, plugin)

        executive._auto_save_all_states()

        assert plugin.load_state() == {"holding": True, "pending": {"118": "BUY"}}

    def test_stopped_plugin_not_saved(self, tmp_path):
        plugin = ReconnectProbePlugin("p_stopped", tmp_path / "a")
        plugin.get_state_for_save = lambda: {"x": 1}
        executive = self._registered(tmp_path, plugin)
        plugin._state = PluginState.STOPPED

        executive._auto_save_all_states()
        assert plugin.load_state() == {}   # nothing written


class TestLocalizeClassPath:
    """The registry stores absolute class_paths in a home-scoped DB that
    outlives checkouts. A slot registered from another working copy must
    reload from the ACTIVE plugin directory — observed live: a months-old
    tree's plugin code auto-reloaded for weeks, silently missing every
    safety fix."""

    def _executive(self):
        bus = MessageBus()
        received = []
        bus.subscribe("alerts", received.append, subscriber="test")
        return PluginExecutive(None, None, message_bus=bus), received

    def test_path_under_active_dir_unchanged(self, tmp_path, monkeypatch):
        monkeypatch.setenv("IB_PLUGIN_DIR", str(tmp_path))
        local = tmp_path / "myplug" / "plugin.py"
        local.parent.mkdir(parents=True)
        local.write_text("# plugin")
        executive, received = self._executive()

        assert executive._localize_class_path("myplug", str(local)) == str(local)
        assert received == []

    def test_foreign_path_remapped_to_local_copy(self, tmp_path, monkeypatch):
        monkeypatch.setenv("IB_PLUGIN_DIR", str(tmp_path / "active"))
        local = tmp_path / "active" / "gld_usd_swap" / "__init__.py"
        local.parent.mkdir(parents=True)
        local.write_text("# current")
        foreign = tmp_path / "stale_tree" / "gld_usd_swap" / "__init__.py"
        foreign.parent.mkdir(parents=True)
        foreign.write_text("# ancient")
        executive, received = self._executive()

        result = executive._localize_class_path("gld_usd_swap", str(foreign))

        assert result == str(local)
        kinds = [m.payload["kind"] for m in received]
        assert kinds == ["stale_plugin_path"]

    def test_foreign_path_remaps_to_conventional_entry_file(self, tmp_path, monkeypatch):
        """Different entry filename locally (plugin.py vs stored __init__.py)
        still resolves to the local copy."""
        monkeypatch.setenv("IB_PLUGIN_DIR", str(tmp_path / "active"))
        local = tmp_path / "active" / "gld_usd_swap" / "plugin.py"
        local.parent.mkdir(parents=True)
        local.write_text("# current")
        executive, received = self._executive()

        result = executive._localize_class_path(
            "gld_usd_swap", str(tmp_path / "elsewhere" / "gld_usd_swap" / "__init__.py")
        )
        assert result == str(local)

    def test_foreign_path_without_local_copy_kept(self, tmp_path, monkeypatch):
        """Deliberately-external plugins (no local counterpart) stay loadable."""
        monkeypatch.setenv("IB_PLUGIN_DIR", str(tmp_path / "active"))
        (tmp_path / "active").mkdir()
        external = tmp_path / "external" / "special" / "plugin.py"
        external.parent.mkdir(parents=True)
        external.write_text("# external")
        executive, received = self._executive()

        result = executive._localize_class_path("special", str(external))
        assert result == str(external)
        assert received == []
