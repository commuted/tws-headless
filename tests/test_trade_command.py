"""
Tests for the trade socket command - trade execution with plugin attribution
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from typing import List, Optional

from ib.main import CommandHandler
from ib.command_server import CommandResult, CommandStatus


class MockPluginExecutive:
    """Mock plugin executive for testing"""

    def __init__(self):
        self._plugins = {
            "momentum_5day": Mock(),
            "mean_reversion": Mock(),
        }
        self.execute_manual_trade_calls = []

    def execute_manual_trade(
        self,
        plugin_name: str,
        symbol: str,
        action: str,
        quantity: int,
        reason: str = "manual_trade",
        dry_run: bool = True,
    ):
        """Mock execute_manual_trade"""
        self.execute_manual_trade_calls.append({
            "plugin_name": plugin_name,
            "symbol": symbol,
            "action": action,
            "quantity": quantity,
            "reason": reason,
            "dry_run": dry_run,
        })

        # Simulate failure for unknown plugin
        if plugin_name not in self._plugins:
            return False, None, f"Plugin '{plugin_name}' not found. Available: {list(self._plugins.keys())}"

        # Simulate successful dry run
        if dry_run:
            message = (
                f"[DRY RUN] Would execute for plugin '{plugin_name}':\n"
                f"  Action: {action} {quantity} {symbol}\n"
                f"  Estimated Value: $0.00\n"
                f"  Use --confirm to execute"
            )
            return True, None, message

        # Simulate successful execution
        order_id = 12345
        message = (
            f"[EXECUTED] Trade for plugin '{plugin_name}':\n"
            f"  Order ID: {order_id}\n"
            f"  Action: {action} {quantity} {symbol}\n"
            f"  Status: Submitted"
        )
        return True, order_id, message


class MockPortfolio:
    """Mock portfolio for testing"""

    def __init__(self):
        self.positions = []

    def get_position(self, symbol: str):
        return None


class MockShutdownManager:
    """Mock shutdown manager for testing"""
    pass


class TestTradeCommandParsing:
    """Tests for trade command argument parsing"""

    def setup_method(self):
        """Set up test fixtures"""
        self.portfolio = MockPortfolio()
        self.shutdown_mgr = MockShutdownManager()
        self.plugin_executive = MockPluginExecutive()
        self.handler = CommandHandler(
            self.portfolio,
            self.shutdown_mgr,
            plugin_executive=self.plugin_executive,
        )

    def test_parse_basic_trade(self):
        """Test parsing: trade momentum_5day BUY SPY 100"""
        result = self.handler.handle_trade(["momentum_5day", "BUY", "SPY", "100"])

        assert result.status == CommandStatus.SUCCESS
        assert len(self.plugin_executive.execute_manual_trade_calls) == 1

        call = self.plugin_executive.execute_manual_trade_calls[0]
        assert call["plugin_name"] == "momentum_5day"
        assert call["action"] == "BUY"
        assert call["symbol"] == "SPY"
        assert call["quantity"] == 100
        assert call["dry_run"] == True  # Default to dry run

    def test_parse_with_confirm(self):
        """Test parsing: trade momentum_5day BUY SPY 100 --confirm"""
        result = self.handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "100", "--confirm"]
        )

        assert result.status == CommandStatus.SUCCESS
        call = self.plugin_executive.execute_manual_trade_calls[0]
        assert call["dry_run"] == False

    def test_parse_with_reason(self):
        """Test parsing: trade momentum_5day BUY SPY 100 --reason "manual entry" """
        result = self.handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "100", "--reason", "manual entry"]
        )

        assert result.status == CommandStatus.SUCCESS
        call = self.plugin_executive.execute_manual_trade_calls[0]
        assert call["reason"] == "manual entry"

    def test_parse_with_confirm_and_reason(self):
        """Test parsing: trade plugin BUY SPY 100 --confirm --reason "test" """
        result = self.handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "100", "--confirm", "--reason", "test trade"]
        )

        assert result.status == CommandStatus.SUCCESS
        call = self.plugin_executive.execute_manual_trade_calls[0]
        assert call["dry_run"] == False
        assert call["reason"] == "test trade"

    def test_parse_sell_action(self):
        """Test parsing: trade plugin SELL QQQ 50"""
        result = self.handler.handle_trade(
            ["momentum_5day", "SELL", "QQQ", "50"]
        )

        assert result.status == CommandStatus.SUCCESS
        call = self.plugin_executive.execute_manual_trade_calls[0]
        assert call["action"] == "SELL"
        assert call["symbol"] == "QQQ"
        assert call["quantity"] == 50

    def test_case_insensitive_action(self):
        """Test that action is case insensitive"""
        result = self.handler.handle_trade(
            ["momentum_5day", "buy", "SPY", "100"]
        )

        assert result.status == CommandStatus.SUCCESS
        call = self.plugin_executive.execute_manual_trade_calls[0]
        assert call["action"] == "BUY"

    def test_case_insensitive_symbol(self):
        """Test that symbol is uppercased"""
        result = self.handler.handle_trade(
            ["momentum_5day", "BUY", "spy", "100"]
        )

        assert result.status == CommandStatus.SUCCESS
        call = self.plugin_executive.execute_manual_trade_calls[0]
        assert call["symbol"] == "SPY"


class TestTradeCommandValidation:
    """Tests for trade command validation"""

    def setup_method(self):
        """Set up test fixtures"""
        self.portfolio = MockPortfolio()
        self.shutdown_mgr = MockShutdownManager()
        self.plugin_executive = MockPluginExecutive()
        self.handler = CommandHandler(
            self.portfolio,
            self.shutdown_mgr,
            plugin_executive=self.plugin_executive,
        )

    def test_missing_arguments(self):
        """Test error when too few arguments"""
        result = self.handler.handle_trade([])
        assert result.status == CommandStatus.ERROR
        assert "Usage:" in result.message

        result = self.handler.handle_trade(["plugin"])
        assert result.status == CommandStatus.ERROR

        result = self.handler.handle_trade(["plugin", "BUY"])
        assert result.status == CommandStatus.ERROR

        result = self.handler.handle_trade(["plugin", "BUY", "SPY"])
        assert result.status == CommandStatus.ERROR

    def test_invalid_action(self):
        """Test error for invalid action (not BUY/SELL)"""
        result = self.handler.handle_trade(
            ["momentum_5day", "HOLD", "SPY", "100"]
        )

        assert result.status == CommandStatus.ERROR
        assert "Invalid action" in result.message
        assert "BUY or SELL" in result.message

    def test_invalid_quantity_not_integer(self):
        """Test error for non-integer quantity"""
        result = self.handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "abc"]
        )

        assert result.status == CommandStatus.ERROR
        assert "Invalid quantity" in result.message

    def test_invalid_quantity_negative(self):
        """Test error for negative quantity"""
        result = self.handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "-100"]
        )

        assert result.status == CommandStatus.ERROR
        assert "Invalid quantity" in result.message
        assert "positive" in result.message.lower()

    def test_invalid_quantity_zero(self):
        """Test error for zero quantity"""
        result = self.handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "0"]
        )

        assert result.status == CommandStatus.ERROR
        assert "Invalid quantity" in result.message

    def test_invalid_quantity_float(self):
        """Test error for float quantity"""
        result = self.handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "100.5"]
        )

        assert result.status == CommandStatus.ERROR
        assert "Invalid quantity" in result.message

    def test_invalid_plugin_returns_error(self):
        """Test error when plugin doesn't exist"""
        result = self.handler.handle_trade(
            ["nonexistent_plugin", "BUY", "SPY", "100"]
        )

        assert result.status == CommandStatus.ERROR
        assert "not found" in result.message.lower()

    def test_plugin_executive_not_configured(self):
        """Test error when plugin executive not available"""
        handler = CommandHandler(
            self.portfolio,
            self.shutdown_mgr,
            plugin_executive=None,
        )

        result = handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "100"]
        )

        assert result.status == CommandStatus.ERROR
        assert "not configured" in result.message.lower()


class TestTradeCommandExecution:
    """Tests for trade command execution"""

    def setup_method(self):
        """Set up test fixtures"""
        self.portfolio = MockPortfolio()
        self.shutdown_mgr = MockShutdownManager()
        self.plugin_executive = MockPluginExecutive()
        self.handler = CommandHandler(
            self.portfolio,
            self.shutdown_mgr,
            plugin_executive=self.plugin_executive,
        )

    def test_dry_run_no_execution(self):
        """Test dry run doesn't place order"""
        result = self.handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "100"]
        )

        assert result.status == CommandStatus.SUCCESS
        assert result.data["dry_run"] == True
        assert result.data["order_id"] is None
        assert "DRY RUN" in result.message

    def test_confirmed_trade_executes(self):
        """Test --confirm flag triggers execution"""
        result = self.handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "100", "--confirm"]
        )

        assert result.status == CommandStatus.SUCCESS
        assert result.data["dry_run"] == False
        assert result.data["order_id"] == 12345
        assert "EXECUTED" in result.message

    def test_result_data_contains_trade_details(self):
        """Test result data includes all trade details"""
        result = self.handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "100", "--confirm"]
        )

        assert result.status == CommandStatus.SUCCESS
        assert result.data["plugin"] == "momentum_5day"
        assert result.data["action"] == "BUY"
        assert result.data["symbol"] == "SPY"
        assert result.data["quantity"] == 100


class TestTradeCommandAttribution:
    """Tests for plugin attribution"""

    def setup_method(self):
        """Set up test fixtures"""
        self.portfolio = MockPortfolio()
        self.shutdown_mgr = MockShutdownManager()
        self.plugin_executive = MockPluginExecutive()
        self.handler = CommandHandler(
            self.portfolio,
            self.shutdown_mgr,
            plugin_executive=self.plugin_executive,
        )

    def test_trade_attributed_to_correct_plugin(self):
        """Test trade is attributed to specified plugin"""
        result = self.handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "100"]
        )

        call = self.plugin_executive.execute_manual_trade_calls[0]
        assert call["plugin_name"] == "momentum_5day"

        # Test with different plugin
        result = self.handler.handle_trade(
            ["mean_reversion", "SELL", "QQQ", "50"]
        )

        call = self.plugin_executive.execute_manual_trade_calls[1]
        assert call["plugin_name"] == "mean_reversion"

    def test_reason_passed_to_execution(self):
        """Test reason is passed to plugin executive"""
        result = self.handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "100", "--reason", "manual entry signal"]
        )

        call = self.plugin_executive.execute_manual_trade_calls[0]
        assert call["reason"] == "manual entry signal"


class TestTradeCommandIntegration:
    """Integration-style tests with more realistic mocks"""

    def test_full_trade_workflow(self):
        """Test complete trade workflow from command to execution"""
        portfolio = MockPortfolio()
        shutdown_mgr = MockShutdownManager()
        plugin_executive = MockPluginExecutive()
        handler = CommandHandler(
            portfolio,
            shutdown_mgr,
            plugin_executive=plugin_executive,
        )

        # First do a dry run
        result = handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "100"]
        )
        assert result.status == CommandStatus.SUCCESS
        assert "DRY RUN" in result.message
        assert result.data["order_id"] is None

        # Then confirm the trade
        result = handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "100", "--confirm"]
        )
        assert result.status == CommandStatus.SUCCESS
        assert "EXECUTED" in result.message
        assert result.data["order_id"] == 12345

    def test_multiple_trades_different_plugins(self):
        """Test multiple trades to different plugins"""
        portfolio = MockPortfolio()
        shutdown_mgr = MockShutdownManager()
        plugin_executive = MockPluginExecutive()
        handler = CommandHandler(
            portfolio,
            shutdown_mgr,
            plugin_executive=plugin_executive,
        )

        # Trade 1: momentum plugin
        result1 = handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "100", "--confirm"]
        )
        assert result1.status == CommandStatus.SUCCESS

        # Trade 2: mean reversion plugin
        result2 = handler.handle_trade(
            ["mean_reversion", "SELL", "QQQ", "50", "--confirm"]
        )
        assert result2.status == CommandStatus.SUCCESS

        # Verify both were called
        assert len(plugin_executive.execute_manual_trade_calls) == 2
        assert plugin_executive.execute_manual_trade_calls[0]["plugin_name"] == "momentum_5day"
        assert plugin_executive.execute_manual_trade_calls[1]["plugin_name"] == "mean_reversion"


class TestTradeCommandErrorHandling:
    """Tests for error handling in trade command"""

    def setup_method(self):
        """Set up test fixtures"""
        self.portfolio = MockPortfolio()
        self.shutdown_mgr = MockShutdownManager()

    def test_exception_in_execute_manual_trade(self):
        """Test handling of exception during execution"""
        plugin_executive = MockPluginExecutive()

        # Make execute_manual_trade raise an exception
        def raise_error(*args, **kwargs):
            raise Exception("Connection failed")

        plugin_executive.execute_manual_trade = raise_error

        handler = CommandHandler(
            self.portfolio,
            self.shutdown_mgr,
            plugin_executive=plugin_executive,
        )

        result = handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "100", "--confirm"]
        )

        assert result.status == CommandStatus.ERROR
        assert "failed" in result.message.lower() or "error" in result.message.lower()

    def test_execution_failure_returns_error(self):
        """Test that execution failure returns error status"""
        plugin_executive = MockPluginExecutive()

        # Make execute_manual_trade return failure
        def return_failure(*args, **kwargs):
            return False, None, "Order rejected by broker"

        plugin_executive.execute_manual_trade = return_failure

        handler = CommandHandler(
            self.portfolio,
            self.shutdown_mgr,
            plugin_executive=plugin_executive,
        )

        result = handler.handle_trade(
            ["momentum_5day", "BUY", "SPY", "100", "--confirm"]
        )

        assert result.status == CommandStatus.ERROR
        assert "rejected" in result.message.lower()
