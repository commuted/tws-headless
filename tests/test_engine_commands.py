"""
Tests for EngineCommandHandler in run_engine.py

Tests all socket command handlers for the trading engine.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from typing import List, Optional, Dict, Any

from ib.run_engine import EngineCommandHandler
from ib.command_server import CommandResult, CommandStatus


class MockPosition:
    """Mock portfolio position"""

    def __init__(self, symbol: str, quantity: float, price: float, pnl: float = 0.0):
        self.symbol = symbol
        self.quantity = quantity
        self.current_price = price
        self.market_value = quantity * price
        self.unrealized_pnl = pnl
        self.allocation_pct = 10.0
        self.contract = Mock()


class MockAccountSummary:
    """Mock account summary"""

    def __init__(self):
        self.is_valid = True
        self.account_id = "U123456"
        self.net_liquidation = 100000.0
        self.available_funds = 25000.0
        self.buying_power = 50000.0


class MockPortfolio:
    """Mock portfolio for testing"""

    def __init__(self):
        self.positions: List[MockPosition] = []
        self.total_value = 100000.0
        self.total_pnl = 5000.0
        self._account_summary = MockAccountSummary()
        self._last_order_id = 0

    def get_account_summary(self):
        return self._account_summary

    def get_position(self, symbol: str) -> Optional[MockPosition]:
        for p in self.positions:
            if p.symbol == symbol:
                return p
        return None

    def place_market_order(self, contract, action: str, quantity: float) -> Optional[int]:
        self._last_order_id += 1
        return self._last_order_id


class MockPluginExecutive:
    """Mock plugin executive for testing"""

    def __init__(self):
        self._plugins = {
            "momentum_5day": Mock(
                spec=["state", "enabled", "run_count", "is_system_plugin"],
                state="STARTED", enabled=True, run_count=10, is_system_plugin=False,
            ),
            "mean_reversion": Mock(
                spec=["state", "enabled", "run_count", "is_system_plugin"],
                state="STOPPED", enabled=False, run_count=5, is_system_plugin=False,
            ),
        }
        self.execute_manual_trade_calls = []

    @property
    def plugins(self):
        """Return list of plugin names (matches real PluginExecutive)"""
        return list(self._plugins.keys())

    def get_plugin_status(self, name: str) -> Optional[Dict[str, Any]]:
        if name not in self._plugins:
            return None
        p = self._plugins[name]
        return {
            "name": name,
            "slot": name,
            "state": p.state,
            "is_system_plugin": getattr(p, "is_system_plugin", False),
            "enabled": p.enabled,
            "run_count": p.run_count,
        }

    def _resolve_plugin(self, name_or_id: str):
        if name_or_id in self._plugins:
            config = self._plugins[name_or_id]
            return name_or_id, config
        return None, None

    def get_holdings_summary(self) -> Dict[str, Any]:
        return {
            "plugins": {
                "momentum_5day": {"total_value": 50000.0, "cash": 5000.0},
                "mean_reversion": {"total_value": 30000.0, "cash": 3000.0},
            },
            "unassigned": {"cash": 15000.0, "total_value": 20000.0, "positions": []},
        }

    def sync_unassigned_holdings(self):
        pass

    def start_plugin(self, name: str) -> bool:
        return name in self._plugins

    def stop_plugin(self, name: str) -> bool:
        return name in self._plugins

    def freeze_plugin(self, name: str) -> bool:
        return name in self._plugins

    def resume_plugin(self, name: str) -> bool:
        return name in self._plugins

    def enable_plugin(self, name: str, enabled: bool) -> bool:
        return name in self._plugins

    def trigger_plugin(self, name: str):
        if name not in self._plugins:
            return None
        result = Mock()
        result.signals = [Mock()]
        result.actionable_signals = [Mock()]
        return result

    def execute_manual_trade(
        self,
        plugin_name: str,
        symbol: str,
        action: str,
        quantity: int,
        reason: str = "manual_trade",
        dry_run: bool = True,
    ):
        self.execute_manual_trade_calls.append({
            "plugin_name": plugin_name,
            "symbol": symbol,
            "action": action,
            "quantity": quantity,
            "reason": reason,
            "dry_run": dry_run,
        })

        if plugin_name not in self._plugins:
            return False, None, f"Plugin '{plugin_name}' not found"

        if dry_run:
            return True, None, f"[DRY RUN] Would {action} {quantity} {symbol}"

        return True, 12345, f"[EXECUTED] {action} {quantity} {symbol}"


class MockEngine:
    """Mock trading engine for testing"""

    def __init__(self):
        self.portfolio = MockPortfolio()
        self.plugin_executive: Optional[MockPluginExecutive] = None
        self._paused = False
        self._stopped = False
        self._loop = None           # no running loop in tests
        self._shutdown_event = Mock()  # fallback path in handle_stop

    def get_status(self) -> Dict[str, Any]:
        return {
            "state": "RUNNING",
            "connected": True,
            "portfolio": {"positions": len(self.portfolio.positions)},
        }

    def pause(self):
        self._paused = True

    def resume(self):
        self._paused = False

    def stop(self):
        self._stopped = True


class TestEngineCommandHandlerStatus:
    """Tests for status command"""

    def setup_method(self):
        self.engine = MockEngine()
        self.handler = EngineCommandHandler(self.engine)

    def test_status_basic(self):
        """Test basic status command"""
        result = self.handler.handle_status([])
        assert result.status == CommandStatus.SUCCESS
        assert "RUNNING" in result.message
        assert result.data["state"] == "RUNNING"
        assert result.data["connected"] == True

    def test_status_with_positions(self):
        """Test status shows position count"""
        self.engine.portfolio.positions = [
            MockPosition("SPY", 100, 450.0),
            MockPosition("QQQ", 50, 380.0),
        ]
        result = self.handler.handle_status([])
        assert result.status == CommandStatus.SUCCESS
        assert "Positions: 2" in result.message


class TestEngineCommandHandlerPositions:
    """Tests for positions command"""

    def setup_method(self):
        self.engine = MockEngine()
        self.handler = EngineCommandHandler(self.engine)

    def test_positions_empty(self):
        """Test positions when none exist"""
        result = self.handler.handle_positions([])
        assert result.status == CommandStatus.SUCCESS
        assert "0 positions" in result.message
        assert result.data["positions"] == []

    def test_positions_with_data(self):
        """Test positions returns position data"""
        self.engine.portfolio.positions = [
            MockPosition("SPY", 100, 450.0, 500.0),
            MockPosition("QQQ", 50, 380.0, -200.0),
        ]
        result = self.handler.handle_positions([])
        assert result.status == CommandStatus.SUCCESS
        assert "2 positions" in result.message
        assert len(result.data["positions"]) == 2

        # Positions should be sorted by market value (descending)
        pos = result.data["positions"]
        assert pos[0]["symbol"] == "SPY"
        assert pos[0]["quantity"] == 100
        assert pos[0]["value"] == 45000.0

    def test_positions_includes_pnl(self):
        """Test positions includes P&L data"""
        self.engine.portfolio.positions = [
            MockPosition("AAPL", 25, 175.0, 250.0),
        ]
        result = self.handler.handle_positions([])
        pos = result.data["positions"][0]
        assert pos["pnl"] == 250.0


class TestEngineCommandHandlerSummary:
    """Tests for summary command"""

    def setup_method(self):
        self.engine = MockEngine()
        self.handler = EngineCommandHandler(self.engine)

    def test_summary_basic(self):
        """Test basic summary command"""
        result = self.handler.handle_summary([])
        assert result.status == CommandStatus.SUCCESS
        assert "ACCOUNT SUMMARY" in result.message
        assert result.data["account"]["total_value"] == 100000.0

    def test_summary_json_output(self):
        """Test summary with --json flag"""
        result = self.handler.handle_summary(["--json"])
        assert result.status == CommandStatus.SUCCESS
        # JSON output should be parseable
        import json
        data = json.loads(result.message)
        assert "account" in data
        assert "portfolio" in data

    def test_summary_with_plugin_holdings(self):
        """Test summary includes plugin holdings when available"""
        self.engine.plugin_executive = MockPluginExecutive()
        result = self.handler.handle_summary([])
        assert result.status == CommandStatus.SUCCESS
        assert "PLUGIN HOLDINGS" in result.message
        assert result.data["plugins"]["momentum_5day"]["total_value"] == 50000.0

    def test_summary_shows_unassigned(self):
        """Test summary shows unassigned holdings"""
        self.engine.plugin_executive = MockPluginExecutive()
        result = self.handler.handle_summary([])
        assert result.status == CommandStatus.SUCCESS
        assert "UNASSIGNED" in result.message
        assert result.data["unassigned"]["cash"] == 15000.0


class TestEngineCommandHandlerLiquidate:
    """Tests for liquidate command"""

    def setup_method(self):
        self.engine = MockEngine()
        self.handler = EngineCommandHandler(self.engine)

    def test_liquidate_no_positions(self):
        """Test liquidate when no positions exist"""
        result = self.handler.handle_liquidate([])
        assert result.status == CommandStatus.SUCCESS
        assert "No positions to liquidate" in result.message

    def test_liquidate_dry_run(self):
        """Test liquidate without --confirm shows preview"""
        self.engine.portfolio.positions = [
            MockPosition("SPY", 100, 450.0),
        ]
        result = self.handler.handle_liquidate([])
        assert result.status == CommandStatus.SUCCESS
        assert "Would sell" in result.message
        assert result.data["dry_run"] == True

    def test_liquidate_specific_symbol(self):
        """Test liquidate specific symbol"""
        self.engine.portfolio.positions = [
            MockPosition("SPY", 100, 450.0),
            MockPosition("QQQ", 50, 380.0),
        ]
        result = self.handler.handle_liquidate(["SPY"])
        assert result.status == CommandStatus.SUCCESS
        assert "SPY" in result.message
        assert len(result.data["positions"]) == 1

    def test_liquidate_symbol_not_found(self):
        """Test liquidate with unknown symbol"""
        self.engine.portfolio.positions = [
            MockPosition("SPY", 100, 450.0),
        ]
        result = self.handler.handle_liquidate(["AAPL"])
        assert result.status == CommandStatus.ERROR
        assert "not found" in result.message

    def test_liquidate_with_confirm(self):
        """Test liquidate with --confirm executes orders"""
        self.engine.portfolio.positions = [
            MockPosition("SPY", 100, 450.0),
        ]
        result = self.handler.handle_liquidate(["--confirm"])
        assert result.status == CommandStatus.SUCCESS
        assert "initiated" in result.message
        assert len(result.data["order_ids"]) == 1

    def test_liquidate_already_in_progress(self):
        """Test liquidate rejects when already in progress"""
        self.handler._liquidation_in_progress = True
        result = self.handler.handle_liquidate(["--confirm"])
        assert result.status == CommandStatus.ERROR
        assert "already in progress" in result.message


class TestEngineCommandHandlerSell:
    """Tests for sell command"""

    def setup_method(self):
        self.engine = MockEngine()
        self.engine.portfolio.positions = [
            MockPosition("SPY", 100, 450.0),
        ]
        self.handler = EngineCommandHandler(self.engine)

    def test_sell_missing_args(self):
        """Test sell with missing arguments"""
        result = self.handler.handle_sell([])
        assert result.status == CommandStatus.ERROR
        assert "Usage:" in result.message

        result = self.handler.handle_sell(["SPY"])
        assert result.status == CommandStatus.ERROR

    def test_sell_no_position(self):
        """Test sell when no position exists"""
        result = self.handler.handle_sell(["AAPL", "10"])
        assert result.status == CommandStatus.ERROR
        assert "No position" in result.message

    def test_sell_dry_run(self):
        """Test sell without --confirm shows preview"""
        result = self.handler.handle_sell(["SPY", "50"])
        assert result.status == CommandStatus.SUCCESS
        assert "Would sell" in result.message
        assert result.data["dry_run"] == True
        assert result.data["quantity"] == 50

    def test_sell_all(self):
        """Test sell with 'all' quantity"""
        result = self.handler.handle_sell(["SPY", "all"])
        assert result.status == CommandStatus.SUCCESS
        assert result.data["quantity"] == 100

    def test_sell_invalid_quantity(self):
        """Test sell with invalid quantity"""
        result = self.handler.handle_sell(["SPY", "abc"])
        assert result.status == CommandStatus.ERROR
        assert "Invalid quantity" in result.message

    def test_sell_exceeds_position(self):
        """Test sell more than position size"""
        result = self.handler.handle_sell(["SPY", "200"])
        assert result.status == CommandStatus.ERROR
        assert "Cannot sell" in result.message

    def test_sell_with_confirm(self):
        """Test sell with --confirm executes order"""
        result = self.handler.handle_sell(["SPY", "50", "--confirm"])
        assert result.status == CommandStatus.SUCCESS
        assert "order placed" in result.message
        assert result.data["order_id"] is not None


class TestEngineCommandHandlerBuy:
    """Tests for buy command"""

    def setup_method(self):
        self.engine = MockEngine()
        self.engine.portfolio.positions = [
            MockPosition("SPY", 100, 450.0),
        ]
        self.handler = EngineCommandHandler(self.engine)

    def test_buy_missing_args(self):
        """Test buy with missing arguments"""
        result = self.handler.handle_buy([])
        assert result.status == CommandStatus.ERROR
        assert "Usage:" in result.message

    def test_buy_no_existing_position(self):
        """Test buy when no existing position (can't determine contract)"""
        result = self.handler.handle_buy(["AAPL", "10"])
        assert result.status == CommandStatus.ERROR
        assert "No existing position" in result.message

    def test_buy_dry_run(self):
        """Test buy without --confirm shows preview"""
        result = self.handler.handle_buy(["SPY", "50"])
        assert result.status == CommandStatus.SUCCESS
        assert "Would buy" in result.message
        assert result.data["dry_run"] == True
        assert result.data["quantity"] == 50

    def test_buy_invalid_quantity(self):
        """Test buy with invalid quantity"""
        result = self.handler.handle_buy(["SPY", "abc"])
        assert result.status == CommandStatus.ERROR
        assert "Invalid quantity" in result.message

    def test_buy_with_confirm(self):
        """Test buy with --confirm executes order"""
        result = self.handler.handle_buy(["SPY", "25", "--confirm"])
        assert result.status == CommandStatus.SUCCESS
        assert "order placed" in result.message
        assert result.data["order_id"] is not None


class TestEngineCommandHandlerTrade:
    """Tests for trade command (plugin-attributed trades)"""

    def setup_method(self):
        self.engine = MockEngine()
        self.engine.plugin_executive = MockPluginExecutive()
        self.handler = EngineCommandHandler(self.engine)

    def test_trade_no_plugin_executive(self):
        """Test trade command requires plugin executive"""
        self.engine.plugin_executive = None
        result = self.handler.handle_trade(["momentum_5day", "BUY", "SPY", "100"])
        assert result.status == CommandStatus.ERROR
        assert "requires plugin executive" in result.message

    def test_trade_missing_args(self):
        """Test trade with missing arguments"""
        result = self.handler.handle_trade(["momentum_5day"])
        assert result.status == CommandStatus.ERROR
        assert "Usage:" in result.message

    def test_trade_invalid_action(self):
        """Test trade with invalid action"""
        result = self.handler.handle_trade(["momentum_5day", "HOLD", "SPY", "100"])
        assert result.status == CommandStatus.ERROR
        assert "Invalid action" in result.message

    def test_trade_invalid_quantity(self):
        """Test trade with invalid quantity"""
        result = self.handler.handle_trade(["momentum_5day", "BUY", "SPY", "abc"])
        assert result.status == CommandStatus.ERROR
        assert "Invalid quantity" in result.message

    def test_trade_negative_quantity(self):
        """Test trade with negative quantity"""
        result = self.handler.handle_trade(["momentum_5day", "BUY", "SPY", "-10"])
        assert result.status == CommandStatus.ERROR
        assert "Must be positive" in result.message

    def test_trade_dry_run(self):
        """Test trade without --confirm is dry run"""
        result = self.handler.handle_trade(["momentum_5day", "BUY", "SPY", "100"])
        assert result.status == CommandStatus.SUCCESS
        assert result.data["dry_run"] == True

        calls = self.engine.plugin_executive.execute_manual_trade_calls
        assert len(calls) == 1
        assert calls[0]["dry_run"] == True

    def test_trade_with_confirm(self):
        """Test trade with --confirm executes"""
        result = self.handler.handle_trade(["momentum_5day", "BUY", "SPY", "100", "--confirm"])
        assert result.status == CommandStatus.SUCCESS
        assert result.data["dry_run"] == False
        assert result.data["order_id"] == 12345

    def test_trade_with_reason(self):
        """Test trade with --reason parameter"""
        result = self.handler.handle_trade([
            "momentum_5day", "BUY", "SPY", "100",
            "--reason", "manual entry point",
            "--confirm"
        ])
        assert result.status == CommandStatus.SUCCESS

        calls = self.engine.plugin_executive.execute_manual_trade_calls
        assert calls[0]["reason"] == "manual entry point"

    def test_trade_unknown_plugin(self):
        """Test trade with unknown plugin"""
        result = self.handler.handle_trade(["unknown_plugin", "BUY", "SPY", "100"])
        assert result.status == CommandStatus.ERROR
        assert "not found" in result.message


class TestEngineCommandHandlerPauseResume:
    """Tests for pause and resume commands"""

    def setup_method(self):
        self.engine = MockEngine()
        self.handler = EngineCommandHandler(self.engine)

    def test_pause(self):
        """Test pause command"""
        result = self.handler.handle_pause([])
        assert result.status == CommandStatus.SUCCESS
        assert "paused" in result.message
        assert self.engine._paused == True

    def test_resume(self):
        """Test resume command"""
        self.engine._paused = True
        result = self.handler.handle_resume([])
        assert result.status == CommandStatus.SUCCESS
        assert "resumed" in result.message
        assert self.engine._paused == False


class TestEngineCommandHandlerStop:
    """Tests for stop command"""

    def setup_method(self):
        self.engine = MockEngine()
        self.handler = EngineCommandHandler(self.engine)

    def test_stop(self):
        """Test stop command — no live event loop, so fallback sets shutdown event."""
        result = self.handler.handle_stop([])
        assert result.status == CommandStatus.SUCCESS
        assert "Shutdown" in result.message
        self.engine._shutdown_event.set.assert_called_once()


class TestEngineCommandHandlerPlugin:
    """Tests for plugin command"""

    def setup_method(self):
        self.engine = MockEngine()
        self.engine.plugin_executive = MockPluginExecutive()
        self.handler = EngineCommandHandler(self.engine)

    def test_plugin_no_executive(self):
        """Test plugin command when executive not available"""
        self.engine.plugin_executive = None
        result = self.handler.handle_plugin(["list"])
        assert result.status == CommandStatus.ERROR
        assert "not available" in result.message

    def test_plugin_missing_subcommand(self):
        """Test plugin without subcommand"""
        result = self.handler.handle_plugin([])
        assert result.status == CommandStatus.ERROR
        assert "Usage:" in result.message

    def test_plugin_list(self):
        """Test plugin list subcommand"""
        result = self.handler.handle_plugin(["list"])
        assert result.status == CommandStatus.SUCCESS
        assert "2 plugins:" in result.message
        assert "momentum_5day" in result.message
        assert "momentum_5day" in result.data["plugins"]

    def test_plugin_status(self):
        """Test plugin status subcommand"""
        result = self.handler.handle_plugin(["status", "momentum_5day"])
        assert result.status == CommandStatus.SUCCESS
        assert result.data["state"] == "STARTED"
        assert result.data["enabled"] == True

    def test_plugin_status_not_found(self):
        """Test plugin status with unknown plugin"""
        result = self.handler.handle_plugin(["status", "unknown"])
        assert result.status == CommandStatus.ERROR
        assert "not found" in result.message

    def test_plugin_start(self):
        """Test plugin start subcommand"""
        result = self.handler.handle_plugin(["start", "momentum_5day"])
        assert result.status == CommandStatus.SUCCESS
        assert "started" in result.message

    def test_plugin_start_not_found(self):
        """Test plugin start with unknown plugin"""
        result = self.handler.handle_plugin(["start", "unknown"])
        assert result.status == CommandStatus.ERROR
        assert "Failed to start" in result.message

    def test_plugin_stop(self):
        """Test plugin stop subcommand"""
        result = self.handler.handle_plugin(["stop", "momentum_5day"])
        assert result.status == CommandStatus.SUCCESS
        assert "stopped" in result.message

    def test_plugin_freeze(self):
        """Test plugin freeze subcommand"""
        result = self.handler.handle_plugin(["freeze", "momentum_5day"])
        assert result.status == CommandStatus.SUCCESS
        assert "frozen" in result.message

    def test_plugin_resume(self):
        """Test plugin resume subcommand"""
        result = self.handler.handle_plugin(["resume", "momentum_5day"])
        assert result.status == CommandStatus.SUCCESS
        assert "resumed" in result.message

    def test_plugin_enable(self):
        """Test plugin enable subcommand"""
        result = self.handler.handle_plugin(["enable", "mean_reversion"])
        assert result.status == CommandStatus.SUCCESS
        assert "enabled" in result.message

    def test_plugin_disable(self):
        """Test plugin disable subcommand"""
        result = self.handler.handle_plugin(["disable", "momentum_5day"])
        assert result.status == CommandStatus.SUCCESS
        assert "disabled" in result.message

    def test_plugin_trigger(self):
        """Test plugin trigger subcommand"""
        result = self.handler.handle_plugin(["trigger", "momentum_5day"])
        assert result.status == CommandStatus.SUCCESS
        assert "triggered" in result.message
        assert "1 signals" in result.message

    def test_plugin_trigger_not_found(self):
        """Test plugin trigger with unknown plugin"""
        result = self.handler.handle_plugin(["trigger", "unknown"])
        assert result.status == CommandStatus.ERROR
        assert "not found" in result.message

    def test_plugin_unknown_subcommand(self):
        """Test plugin with unknown subcommand"""
        result = self.handler.handle_plugin(["invalid"])
        assert result.status == CommandStatus.ERROR
        assert "Unknown plugin subcommand" in result.message


class TestPluginListCommand:
    """Comprehensive tests for 'plugin list' command"""

    def setup_method(self):
        self.engine = MockEngine()
        self.engine.plugin_executive = MockPluginExecutive()
        self.handler = EngineCommandHandler(self.engine)

    def test_plugin_list_shows_all_plugins_by_name(self):
        """Test plugin list shows each plugin name"""
        result = self.handler.handle_plugin(["list"])
        assert result.status == CommandStatus.SUCCESS
        # Both plugins should be listed in message
        assert "momentum_5day" in result.message
        assert "mean_reversion" in result.message

    def test_plugin_list_shows_plugin_count(self):
        """Test plugin list shows correct count"""
        result = self.handler.handle_plugin(["list"])
        assert "2 plugins:" in result.message

    def test_plugin_list_shows_state(self):
        """Test plugin list shows plugin state (STARTED, STOPPED, etc.)"""
        result = self.handler.handle_plugin(["list"])
        # momentum_5day is STARTED, mean_reversion is STOPPED
        assert "[STARTED]" in result.message
        assert "[STOPPED]" in result.message

    def test_plugin_list_shows_enabled_disabled(self):
        """Test plugin list flags disabled plugins"""
        result = self.handler.handle_plugin(["list"])
        # mean_reversion is disabled — should be flagged in the output
        assert "[disabled]" in result.message
        # momentum_5day is enabled — no flag expected
        assert result.data["plugins"]["momentum_5day"]["enabled"] is True

    def test_plugin_list_shows_system_flag(self):
        """Test plugin list shows (system) for system plugins"""
        # Add a system plugin to the mock
        system_plugin = Mock(state="STARTED", enabled=True, run_count=0, is_system_plugin=True)
        self.engine.plugin_executive._plugins["_unassigned"] = system_plugin

        result = self.handler.handle_plugin(["list"])
        assert result.status == CommandStatus.SUCCESS
        assert "_unassigned" in result.message
        assert "(system)" in result.message

    def test_plugin_list_data_contains_all_plugins(self):
        """Test plugin list data dict contains all plugin info"""
        result = self.handler.handle_plugin(["list"])
        data = result.data["plugins"]
        assert "momentum_5day" in data
        assert "mean_reversion" in data
        assert data["momentum_5day"]["state"] == "STARTED"
        assert data["momentum_5day"]["enabled"] == True
        assert data["mean_reversion"]["state"] == "STOPPED"
        assert data["mean_reversion"]["enabled"] == False

    def test_plugin_list_data_includes_system_flag(self):
        """Test plugin list data includes is_system_plugin field"""
        result = self.handler.handle_plugin(["list"])
        data = result.data["plugins"]
        # Default mock plugins are not system plugins
        assert data["momentum_5day"]["is_system_plugin"] == False
        assert data["mean_reversion"]["is_system_plugin"] == False

    def test_plugin_list_with_system_plugin_data(self):
        """Test plugin list data correctly marks system plugins"""
        # Add system plugin
        system_plugin = Mock(state="STARTED", enabled=True, run_count=0, is_system_plugin=True)
        self.engine.plugin_executive._plugins["_unassigned"] = system_plugin

        result = self.handler.handle_plugin(["list"])
        data = result.data["plugins"]
        assert "_unassigned" in data
        assert data["_unassigned"]["is_system_plugin"] == True

    def test_plugin_list_empty(self):
        """Test plugin list with no plugins"""
        self.engine.plugin_executive._plugins = {}
        result = self.handler.handle_plugin(["list"])
        assert result.status == CommandStatus.SUCCESS
        assert "0 plugins:" in result.message

    def test_plugin_list_sorted_alphabetically(self):
        """Test plugin list is sorted alphabetically"""
        # Add more plugins with names that would be out of order
        self.engine.plugin_executive._plugins["alpha_plugin"] = Mock(
            state="LOADED", enabled=True, run_count=0, is_system_plugin=False
        )
        self.engine.plugin_executive._plugins["zebra_plugin"] = Mock(
            state="LOADED", enabled=False, run_count=0, is_system_plugin=False
        )

        result = self.handler.handle_plugin(["list"])
        lines = result.message.split("\n")[1:]  # Skip "X plugins:" header
        plugin_names = [line.split()[0] for line in lines if line.strip()]
        assert plugin_names == sorted(plugin_names)


class TestEngineCommandHandlerRegister:
    """Tests for command registration"""

    def setup_method(self):
        self.engine = MockEngine()
        self.handler = EngineCommandHandler(self.engine)

    def test_register_commands(self):
        """Test all commands are registered"""
        server = Mock()
        self.handler.register_commands(server)

        # Check all handlers were registered
        calls = {call[0][0] for call in server.register_handler.call_args_list}
        expected = {
            "status", "positions", "summary", "liquidate", "stop", "shutdown",
            "sell", "buy", "trade", "pause", "resume", "plugin"
        }
        assert expected.issubset(calls)


class TestEngineCommandHandlerErrorHandling:
    """Tests for error handling"""

    def setup_method(self):
        self.engine = MockEngine()
        self.handler = EngineCommandHandler(self.engine)

    def test_status_exception(self):
        """Test status handles exceptions"""
        self.engine.get_status = Mock(side_effect=Exception("Connection lost"))
        result = self.handler.handle_status([])
        assert result.status == CommandStatus.ERROR
        assert "Failed to get status" in result.message

    def test_positions_exception(self):
        """Test positions handles exceptions"""
        self.engine.portfolio.positions = Mock(side_effect=Exception("DB error"))
        result = self.handler.handle_positions([])
        assert result.status == CommandStatus.ERROR
        assert "Failed to get positions" in result.message

    def test_summary_exception(self):
        """Test summary handles exceptions"""
        self.engine.portfolio.get_account_summary = Mock(side_effect=Exception("API error"))
        result = self.handler.handle_summary([])
        assert result.status == CommandStatus.ERROR
        assert "Failed to get summary" in result.message


class TestEnginePluginDump:
    """Tests for 'plugin dump' command in EngineCommandHandler"""

    def setup_method(self):
        self.engine = MockEngine()
        self.engine.plugin_executive = MockPluginExecutive()

        # Set up _pending_orders (not in default MockPluginExecutive)
        self.engine.plugin_executive._pending_orders = {}

        # Set up plugin objects with get_effective_holdings
        mock_plugin = Mock()
        mock_plugin.name = "momentum_5day"
        mock_plugin.slot = "momentum_5day"
        mock_plugin.get_effective_holdings.return_value = {
            "plugin": "momentum_5day",
            "cash": 5000.0,
            "positions": [
                {
                    "symbol": "SPY",
                    "quantity": 100,
                    "cost_basis": 44000.0,
                    "current_price": 450.0,
                    "market_value": 45000.0,
                },
            ],
            "total_value": 50000.0,
        }
        self.engine.plugin_executive._plugins["momentum_5day"] = Mock(
            state="STARTED", enabled=True, run_count=10,
            is_system_plugin=False, plugin=mock_plugin,
        )

        mock_plugin2 = Mock()
        mock_plugin2.name = "mean_reversion"
        mock_plugin2.slot = "mean_reversion"
        mock_plugin2.get_effective_holdings.return_value = {
            "plugin": "mean_reversion",
            "cash": 3000.0,
            "positions": [],
            "total_value": 3000.0,
        }
        self.engine.plugin_executive._plugins["mean_reversion"] = Mock(
            state="STOPPED", enabled=False, run_count=5,
            is_system_plugin=False, plugin=mock_plugin2,
        )

        self.handler = EngineCommandHandler(self.engine)

    def test_dump_with_positions(self):
        """Test dump shows plugin positions and cash"""
        result = self.handler.handle_plugin(["dump", "momentum_5day"])
        assert result.status == CommandStatus.SUCCESS
        assert result.data["plugin"] == "momentum_5day"
        assert result.data["cash"] == 5000.0
        assert len(result.data["positions"]) == 1
        assert result.data["positions"][0]["symbol"] == "SPY"
        assert result.data["positions"][0]["quantity"] == 100
        assert result.data["positions"][0]["market_value"] == 45000.0

    def test_dump_empty_positions(self):
        """Test dump with no positions"""
        result = self.handler.handle_plugin(["dump", "mean_reversion"])
        assert result.status == CommandStatus.SUCCESS
        assert result.data["cash"] == 3000.0
        assert result.data["positions"] == []
        assert "(none)" in result.message

    def test_dump_with_open_orders(self):
        """Test dump shows open orders filtered by plugin"""
        from datetime import datetime
        pending = Mock()
        pending.plugin_name = "momentum_5day"
        pending.signal = Mock(symbol="QQQ", action="BUY", quantity=50)
        pending.status = "pending"
        pending.created_at = datetime(2025, 1, 15, 10, 30, 0)

        self.engine.plugin_executive._pending_orders = {101: pending}

        result = self.handler.handle_plugin(["dump", "momentum_5day"])
        assert result.status == CommandStatus.SUCCESS
        assert len(result.data["open_orders"]) == 1
        order = result.data["open_orders"][0]
        assert order["order_id"] == 101
        assert order["symbol"] == "QQQ"
        assert order["action"] == "BUY"
        assert order["quantity"] == 50
        assert order["status"] == "pending"

    def test_dump_filters_orders_by_plugin(self):
        """Test dump only shows orders for the requested plugin"""
        from datetime import datetime
        pending1 = Mock()
        pending1.plugin_name = "momentum_5day"
        pending1.signal = Mock(symbol="SPY", action="SELL", quantity=10)
        pending1.status = "pending"
        pending1.created_at = datetime(2025, 1, 15, 10, 0, 0)

        pending2 = Mock()
        pending2.plugin_name = "mean_reversion"
        pending2.signal = Mock(symbol="QQQ", action="BUY", quantity=20)
        pending2.status = "pending"
        pending2.created_at = datetime(2025, 1, 15, 11, 0, 0)

        self.engine.plugin_executive._pending_orders = {101: pending1, 102: pending2}

        result = self.handler.handle_plugin(["dump", "momentum_5day"])
        assert len(result.data["open_orders"]) == 1
        assert result.data["open_orders"][0]["symbol"] == "SPY"

        result2 = self.handler.handle_plugin(["dump", "mean_reversion"])
        assert len(result2.data["open_orders"]) == 1
        assert result2.data["open_orders"][0]["symbol"] == "QQQ"

    def test_dump_no_open_orders(self):
        """Test dump when no pending orders"""
        result = self.handler.handle_plugin(["dump", "momentum_5day"])
        assert result.status == CommandStatus.SUCCESS
        assert result.data["open_orders"] == []
        assert "Open orders: (none)" in result.message

    def test_dump_not_found(self):
        """Test dump with unknown plugin returns error"""
        result = self.handler.handle_plugin(["dump", "nonexistent"])
        assert result.status == CommandStatus.ERROR
        assert "not found" in result.message

    def test_dump_missing_name(self):
        """Test dump without plugin name"""
        result = self.handler.handle_plugin(["dump"])
        assert result.status == CommandStatus.ERROR

    def test_dump_message_includes_cash(self):
        """Test dump message includes cash balance"""
        result = self.handler.handle_plugin(["dump", "momentum_5day"])
        assert "$5,000.00" in result.message

    def test_dump_message_includes_positions(self):
        """Test dump message includes position details"""
        result = self.handler.handle_plugin(["dump", "momentum_5day"])
        assert "SPY" in result.message
        assert "Positions (1)" in result.message

    def test_dump_message_includes_order_details(self):
        """Test dump message includes order details when present"""
        from datetime import datetime
        pending = Mock()
        pending.plugin_name = "momentum_5day"
        pending.signal = Mock(symbol="QQQ", action="BUY", quantity=50)
        pending.status = "pending"
        pending.created_at = datetime(2025, 1, 15, 10, 30, 0)

        self.engine.plugin_executive._pending_orders = {101: pending}

        result = self.handler.handle_plugin(["dump", "momentum_5day"])
        assert "Open orders (1)" in result.message
        assert "#101" in result.message
        assert "BUY" in result.message
        assert "QQQ" in result.message
