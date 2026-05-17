"""
Unit tests for plugins/test_plugin/plugin.py
"""
from unittest.mock import Mock
import pytest

from plugins.test_plugin.plugin import TestPlugin, TestPluginState
from plugins.base import PluginInstrument, PluginState


def _make_plugin(tmp_path, name="test_plugin"):
    plugin = TestPlugin(name=name, base_path=tmp_path / name)
    return plugin


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

class TestLifecycle:
    def test_start_returns_true(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin.start() is True

    def test_stop_returns_true(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        assert plugin.stop() is True

    def test_freeze_returns_true(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        assert plugin.freeze() is True

    def test_resume_returns_true(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        plugin.freeze()
        assert plugin.resume() is True

    def test_lifecycle_log(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        plugin.freeze()
        plugin.resume()
        plugin.stop()
        assert plugin.lifecycle_log == ["start", "freeze", "resume", "stop"]

    def test_on_unload_summary(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._pstate.signal_count = 5
        plugin._pstate.fill_count = 2
        summary = plugin.on_unload()
        assert "5" in summary
        assert "2" in summary
        assert "unload" in plugin.lifecycle_log

    def test_state_persisted_across_restart(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        plugin._pstate.signal_count = 12
        plugin._pstate.custom_value = "hello"
        plugin.stop()

        plugin2 = _make_plugin(tmp_path)
        plugin2.start()
        assert plugin2._pstate.signal_count == 12
        assert plugin2._pstate.custom_value == "hello"


# ---------------------------------------------------------------------------
# calculate_signals
# ---------------------------------------------------------------------------

class TestCalculateSignals:
    def test_all_hold_signals(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.add_instrument(PluginInstrument("SPY", "S&P", weight=60.0))
        plugin.add_instrument(PluginInstrument("TLT", "Bonds", weight=40.0))
        signals = plugin.calculate_signals()
        assert all(s.action == "HOLD" for s in signals)

    def test_signal_count_matches_instruments(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.add_instrument(PluginInstrument("SPY", "Test", weight=100.0))
        signals = plugin.calculate_signals()
        assert len(signals) == 1

    def test_signal_count_increments(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.add_instrument(PluginInstrument("SPY", "Test", weight=100.0))
        plugin.calculate_signals()
        plugin.calculate_signals()
        assert plugin._pstate.signal_count == 2

    def test_suspended_returns_empty(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.add_instrument(PluginInstrument("SPY", "Test", weight=100.0))
        plugin._pstate.alerts_suspended = True
        signals = plugin.calculate_signals()
        assert signals == []


# ---------------------------------------------------------------------------
# handle_request
# ---------------------------------------------------------------------------

class TestHandleRequest:
    def test_get_stats(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.add_instrument(PluginInstrument("SPY", "Test", weight=100.0))
        plugin.calculate_signals()
        result = plugin.handle_request("get_stats", {})
        assert result["success"] is True
        assert result["signal_count"] == 1
        assert result["fill_count"] == 0

    def test_set_custom_value(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("set_custom_value", {"value": "test_string"})
        assert result["success"] is True
        assert plugin._pstate.custom_value == "test_string"

    def test_reset_clears_state(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._pstate.signal_count = 99
        plugin._pstate.custom_value = "populated"
        result = plugin.handle_request("reset", {})
        assert result["success"] is True
        assert plugin._pstate.signal_count == 0
        assert plugin._pstate.custom_value == ""

    def test_suspend_alerts(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("suspend_alerts", {})
        assert result["success"] is True
        assert plugin._pstate.alerts_suspended is True

    def test_resume_alerts(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._pstate.alerts_suspended = True
        result = plugin.handle_request("resume_alerts", {})
        assert result["success"] is True
        assert plugin._pstate.alerts_suspended is False

    def test_unknown_request(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("nonexistent", {})
        assert result["success"] is False


# ---------------------------------------------------------------------------
# MessageBus
# ---------------------------------------------------------------------------

class TestMessageBus:
    def test_risk_alert_subscription(self, tmp_path):
        from ib.message_bus import MessageBus
        bus = MessageBus()
        plugin = TestPlugin(name="test_plugin",
                             base_path=tmp_path / "test_plugin",
                             message_bus=bus)
        plugin.start()
        # Publishing to risk_alert channel should not raise
        bus.publish("risk_alert", {"reason": "test"}, "test")

    def test_without_message_bus_no_error(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()  # message_bus is None → should not crash


# ---------------------------------------------------------------------------
# INSTRUMENT_COMPLIANCE
# ---------------------------------------------------------------------------

class TestInstrumentCompliance:
    def test_compliance_true(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin.INSTRUMENT_COMPLIANCE is True


# ---------------------------------------------------------------------------
# TestPluginState dataclass
# ---------------------------------------------------------------------------

class TestPluginStateDataclass:
    def test_defaults(self):
        state = TestPluginState()
        assert state.signal_count == 0
        assert state.fill_count == 0
        assert state.custom_value == ""
        assert state.alerts_suspended is False

    def test_to_dict_round_trip(self):
        state = TestPluginState(signal_count=5, custom_value="hello")
        d = state.to_dict()
        restored = TestPluginState.from_dict(d)
        assert restored.signal_count == 5
        assert restored.custom_value == "hello"
