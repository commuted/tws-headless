"""
Unit tests for plugins/panic/plugin.py
"""
from unittest.mock import Mock
import pytest

from plugins.panic.plugin import PanicPlugin
from plugins.base import PluginState


def _make_plugin(tmp_path):
    plugin = PanicPlugin(base_path=tmp_path / "panic")
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
        assert plugin.resume() is True

    def test_queue_restored_on_restart(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        plugin.handle_request("deposit", {
            "positions": [{"symbol": "SPY", "quantity": 100, "urgency": 3}]
        })
        plugin.stop()

        plugin2 = _make_plugin(tmp_path)
        plugin2.start()
        assert len(plugin2._queued_positions) == 1
        assert plugin2._queued_positions[0]["symbol"] == "SPY"


# ---------------------------------------------------------------------------
# Deposit
# ---------------------------------------------------------------------------

class TestDeposit:
    def test_deposit_single_position(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("deposit", {
            "positions": [{"symbol": "spy", "quantity": 100, "urgency": 2}]
        })
        assert result["success"] is True
        assert result["data"]["added"] == 1
        assert plugin._queued_positions[0]["symbol"] == "SPY"  # uppercased

    def test_deposit_multiple_positions(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("deposit", {
            "positions": [
                {"symbol": "SPY",  "quantity": 200, "urgency": 3},
                {"symbol": "QQQ",  "quantity": 50,  "urgency": 1},
            ]
        })
        assert result["data"]["added"] == 2
        assert len(plugin._queued_positions) == 2

    def test_deposit_defaults_urgency_to_2(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.handle_request("deposit", {
            "positions": [{"symbol": "AAPL", "quantity": 10}]
        })
        assert plugin._queued_positions[0]["urgency"] == 2

    def test_deposit_invalid_urgency_rejected(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("deposit", {
            "positions": [{"symbol": "SPY", "quantity": 10, "urgency": 5}]
        })
        assert result["data"]["added"] == 0
        assert "errors" in result

    def test_deposit_zero_quantity_rejected(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("deposit", {
            "positions": [{"symbol": "SPY", "quantity": 0, "urgency": 2}]
        })
        assert result["data"]["added"] == 0

    def test_deposit_missing_symbol_rejected(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("deposit", {
            "positions": [{"quantity": 10, "urgency": 2}]
        })
        assert result["data"]["added"] == 0

    def test_deposit_empty_positions_list(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("deposit", {"positions": []})
        assert result["success"] is True
        assert result["data"]["added"] == 0

    def test_deposit_missing_positions_key(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("deposit", {})
        assert result["success"] is False


# ---------------------------------------------------------------------------
# calculate_signals — urgency ordering
# ---------------------------------------------------------------------------

class TestCalculateSignals:
    def test_no_queue_returns_empty(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        signals = plugin.calculate_signals()
        assert signals == []

    def test_signals_ordered_by_urgency_descending(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.handle_request("deposit", {
            "positions": [
                {"symbol": "AAPL", "quantity": 10, "urgency": 1},
                {"symbol": "SPY",  "quantity": 100, "urgency": 3},
                {"symbol": "QQQ",  "quantity": 50,  "urgency": 2},
            ]
        })
        signals = plugin.calculate_signals()
        urgencies = [s.urgency for s in signals]
        # Signals should be Urgent (3), Normal (2), Patient (1)
        assert urgencies[0] == "Urgent"
        assert urgencies[1] == "Normal"
        assert urgencies[2] == "Patient"

    def test_all_signals_are_sell(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.handle_request("deposit", {
            "positions": [
                {"symbol": "SPY", "quantity": 100, "urgency": 2},
                {"symbol": "BND", "quantity": 50,  "urgency": 1},
            ]
        })
        signals = plugin.calculate_signals()
        assert all(s.action == "SELL" for s in signals)

    def test_queue_cleared_after_signals(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.handle_request("deposit", {
            "positions": [{"symbol": "SPY", "quantity": 100, "urgency": 2}]
        })
        plugin.calculate_signals()
        assert len(plugin._queued_positions) == 0

    def test_positions_moved_to_history(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.handle_request("deposit", {
            "positions": [{"symbol": "SPY", "quantity": 100, "urgency": 2}]
        })
        plugin.calculate_signals()
        assert len(plugin._closed_history) == 1
        assert plugin._closed_history[0]["symbol"] == "SPY"

    def test_signal_quantity_matches_deposit(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.handle_request("deposit", {
            "positions": [{"symbol": "SPY", "quantity": 77, "urgency": 3}]
        })
        signals = plugin.calculate_signals()
        assert float(signals[0].quantity) == 77.0


# ---------------------------------------------------------------------------
# handle_request
# ---------------------------------------------------------------------------

class TestHandleRequest:
    def test_get_queue(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.handle_request("deposit", {
            "positions": [{"symbol": "SPY", "quantity": 100, "urgency": 3}]
        })
        result = plugin.handle_request("get_queue", {})
        assert result["success"] is True
        assert result["data"]["count"] == 1

    def test_get_history_empty(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("get_history", {})
        assert result["success"] is True
        assert result["data"]["count"] == 0

    def test_get_history_after_signals(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.handle_request("deposit", {
            "positions": [{"symbol": "SPY", "quantity": 100, "urgency": 2}]
        })
        plugin.calculate_signals()
        result = plugin.handle_request("get_history", {})
        assert result["data"]["count"] == 1

    def test_clear_queue(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.handle_request("deposit", {
            "positions": [{"symbol": "SPY", "quantity": 100, "urgency": 2}]
        })
        result = plugin.handle_request("clear_queue", {})
        assert result["success"] is True
        assert len(plugin._queued_positions) == 0

    def test_clear_queue_does_not_affect_history(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.handle_request("deposit", {
            "positions": [{"symbol": "SPY", "quantity": 100, "urgency": 2}]
        })
        plugin.calculate_signals()
        plugin.handle_request("deposit", {
            "positions": [{"symbol": "QQQ", "quantity": 50, "urgency": 1}]
        })
        plugin.handle_request("clear_queue", {})
        # History from first batch still intact
        assert len(plugin._closed_history) == 1

    def test_unknown_request(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("nonexistent", {})
        assert result["success"] is False
