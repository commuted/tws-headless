"""
Unit tests for plugins/dummy/plugin.py
"""
import pytest
from plugins.dummy.plugin import DummyPlugin
from plugins.base import PluginInstrument, PluginState


def _make_plugin(tmp_path):
    plugin = DummyPlugin(base_path=tmp_path / "dummy")
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

    def test_run_counter_persisted_across_restart(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        plugin.add_instrument(PluginInstrument("SPY", "S&P 500", weight=100.0))
        plugin.calculate_signals()  # run_counter = 1
        plugin.stop()

        plugin2 = _make_plugin(tmp_path)
        plugin2.start()
        assert plugin2._run_counter == 1


# ---------------------------------------------------------------------------
# calculate_signals
# ---------------------------------------------------------------------------

class TestCalculateSignals:
    def test_all_signals_are_hold(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.add_instrument(PluginInstrument("SPY", "S&P 500", weight=60.0))
        plugin.add_instrument(PluginInstrument("BND", "Bonds", weight=40.0))
        signals = plugin.calculate_signals()
        assert all(s.action == "HOLD" for s in signals)

    def test_signal_count_matches_enabled_instruments(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.add_instrument(PluginInstrument("SPY", "Test", weight=60.0))
        plugin.add_instrument(PluginInstrument("BND", "Test", weight=40.0))
        signals = plugin.calculate_signals()
        assert len(signals) == 2

    def test_disabled_instrument_excluded(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.add_instrument(PluginInstrument("SPY", "Test", weight=60.0))
        inst = PluginInstrument("BND", "Test", weight=40.0, enabled=False)
        plugin.add_instrument(inst)
        signals = plugin.calculate_signals()
        assert len(signals) == 1
        assert signals[0].symbol == "SPY"

    def test_no_instruments_returns_empty(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        signals = plugin.calculate_signals()
        assert signals == []

    def test_run_counter_increments(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.add_instrument(PluginInstrument("SPY", "Test", weight=100.0))
        plugin.calculate_signals()
        plugin.calculate_signals()
        assert plugin._run_counter == 2

    def test_last_signals_stored(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.add_instrument(PluginInstrument("SPY", "S&P 500", weight=100.0))
        plugin.calculate_signals()
        assert len(plugin._last_signals) == 1
        assert plugin._last_signals[0]["symbol"] == "SPY"
        assert plugin._last_signals[0]["action"] == "HOLD"

    def test_target_weight_in_signal(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.add_instrument(PluginInstrument("SPY", "Test", weight=75.0))
        signals = plugin.calculate_signals()
        assert signals[0].target_weight == 75.0


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
        assert result["data"]["run_counter"] == 1
        assert result["data"]["instruments"] == 1

    def test_get_last_signals_empty(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("get_last_signals", {})
        assert result["success"] is True
        assert result["data"]["signals"] == []

    def test_get_last_signals_after_run(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.add_instrument(PluginInstrument("SPY", "Test", weight=100.0))
        plugin.calculate_signals()
        result = plugin.handle_request("get_last_signals", {})
        assert len(result["data"]["signals"]) == 1

    def test_reset_counter(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.add_instrument(PluginInstrument("SPY", "Test", weight=100.0))
        plugin.calculate_signals()
        plugin.calculate_signals()
        result = plugin.handle_request("reset_counter", {})
        assert result["success"] is True
        assert plugin._run_counter == 0

    def test_unknown_request(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("does_not_exist", {})
        assert result["success"] is False
