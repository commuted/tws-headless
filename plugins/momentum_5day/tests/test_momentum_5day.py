"""
Unit tests for plugins/momentum_5day/plugin.py
"""
from unittest.mock import Mock, MagicMock
import pytest

from plugins.momentum_5day.plugin import Momentum5DayPlugin, MomentumMetrics
from plugins.base import PluginInstrument, Holdings, HoldingPosition


def _make_plugin(tmp_path, **kwargs):
    plugin = Momentum5DayPlugin(base_path=tmp_path / "momentum_5day", **kwargs)
    return plugin


def _add_instruments(plugin, *symbols):
    for sym in symbols:
        plugin.add_instrument(PluginInstrument(sym, sym, weight=100.0 / len(symbols)))


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

class TestLifecycle:
    def test_start_returns_true(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.portfolio = Mock()
        plugin.portfolio.subscribe_realtime_bars = Mock(return_value=1)
        assert plugin.start() is True

    def test_stop_saves_state(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.portfolio = None  # no live subscriptions
        plugin.start()
        plugin._run_counter = 5
        plugin.stop()

        plugin2 = _make_plugin(tmp_path)
        plugin2.start()
        assert plugin2._run_counter == 5

    def test_freeze_and_resume(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.portfolio = None
        plugin.start()
        assert plugin.freeze() is True
        assert plugin.resume() is True

    def test_on_unload_summary(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._run_counter = 3
        plugin._fill_count = 1
        summary = plugin.on_unload()
        assert "3" in summary
        assert "1" in summary


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

class TestParameters:
    def test_get_parameters(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        params = plugin.get_parameters()
        assert "lookback_days" in params
        assert "rebalance_threshold" in params
        assert "momentum_weight" in params
        assert "min_position_size" in params

    def test_set_lookback_days(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin.set_parameter("lookback_days", 10) is True
        assert plugin.lookback_days == 10

    def test_set_rebalance_threshold(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin.set_parameter("rebalance_threshold", 3.0) is True
        assert plugin.rebalance_threshold == 3.0

    def test_set_momentum_weight_clamped(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.set_parameter("momentum_weight", 1.5)  # clamped to 1.0
        assert plugin.momentum_weight == 1.0

    def test_set_momentum_weight_min(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.set_parameter("momentum_weight", -0.5)  # clamped to 0.0
        assert plugin.momentum_weight == 0.0

    def test_set_min_position_size(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin.set_parameter("min_position_size", 500.0) is True
        assert plugin.min_position_size == 500.0

    def test_set_unknown_parameter(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin.set_parameter("nonexistent", 42) is False


# ---------------------------------------------------------------------------
# Risk gate
# ---------------------------------------------------------------------------

class TestRiskGate:
    def test_signals_suspended_initially_false(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin._signals_suspended is False

    def test_risk_alert_suspends_signals(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        msg = Mock()
        msg.payload = {"reason": "drawdown limit", "level": "critical"}
        plugin._on_risk_alert(msg)
        assert plugin._signals_suspended is True

    def test_reset_alerts_clears_gate(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._signals_suspended = True
        result = plugin.handle_request("reset_alerts", {})
        assert result["success"] is True
        assert plugin._signals_suspended is False

    def test_suspended_calculate_signals_returns_empty(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        _add_instruments(plugin, "SPY")
        plugin._signals_suspended = True
        signals = plugin.calculate_signals()
        assert signals == []


# ---------------------------------------------------------------------------
# handle_request
# ---------------------------------------------------------------------------

class TestHandleRequest:
    def test_get_stats(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("get_stats", {})
        assert result["success"] is True
        data = result["data"]
        assert "run_counter" in data
        assert "signals_suspended" in data

    def test_get_metrics_empty(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("get_metrics", {})
        assert result["success"] is True
        assert result["data"] == {}

    def test_get_metrics_with_data(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._momentum_metrics["SPY"] = MomentumMetrics(
            symbol="SPY", returns_5d=0.02, momentum_score=0.5, trend="up"
        )
        result = plugin.handle_request("get_metrics", {})
        assert "SPY" in result["data"]
        assert result["data"]["SPY"]["trend"] == "up"

    def test_get_parameters(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("get_parameters", {})
        assert result["success"] is True
        assert "lookback_days" in result["data"]

    def test_set_parameter(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("set_parameter",
                                       {"key": "lookback_days", "value": 7})
        assert result["success"] is True
        assert plugin.lookback_days == 7

    def test_set_parameter_missing_key(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("set_parameter", {"value": 7})
        assert result["success"] is False

    def test_get_signals_history_empty(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("get_signals_history", {})
        assert result["success"] is True
        assert result["data"]["history"] == []

    def test_get_signals_history_limited(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._signals_history = [{"run": i} for i in range(20)]
        result = plugin.handle_request("get_signals_history", {"count": 5})
        assert len(result["data"]["history"]) == 5

    def test_get_momentum_summary(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._momentum_metrics["SPY"] = MomentumMetrics(
            symbol="SPY", returns_5d=0.03, momentum_score=0.8, trend="up"
        )
        result = plugin.handle_request("get_momentum_summary", {})
        assert result["success"] is True
        assert "SPY" in result["data"]["summary"]

    def test_reset_alerts_via_request(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._signals_suspended = True
        result = plugin.handle_request("reset_alerts", {})
        assert result["success"] is True
        assert plugin._signals_suspended is False

    def test_unknown_request(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("nonexistent", {})
        assert result["success"] is False


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

class TestStatePersistence:
    def test_fill_count_persisted(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.portfolio = None
        plugin.start()
        plugin._fill_count = 9
        plugin.stop()

        plugin2 = _make_plugin(tmp_path)
        plugin2.start()
        assert plugin2._fill_count == 9

    def test_suspended_flag_persisted(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.portfolio = None
        plugin.start()
        plugin._signals_suspended = True
        plugin.stop()

        plugin2 = _make_plugin(tmp_path)
        plugin2.start()
        assert plugin2._signals_suspended is True
