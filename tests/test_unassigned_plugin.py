"""
Tests for UnassignedPlugin - System plugin for unattributed positions and cash
"""

import pytest
import tempfile
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, MagicMock

from plugins.unassigned.plugin import UnassignedPlugin, UNASSIGNED_PLUGIN_NAME
from plugins.base import PluginState, Holdings, HoldingPosition


class MockPosition:
    """Mock portfolio position"""
    def __init__(self, symbol, quantity, price, cost=None):
        self.symbol = symbol
        self.quantity = quantity
        self.current_price = price
        self.average_cost = cost or price
        self.avg_cost = cost or price
        self.market_value = quantity * price


class MockAccountSummary:
    """Mock account summary"""
    def __init__(self, available_funds=10000.0):
        self.is_valid = True
        self.available_funds = available_funds
        self.net_liquidation = 100000.0
        self.buying_power = 50000.0


class MockPortfolio:
    """Mock portfolio for testing"""
    def __init__(self):
        self.positions = []
        self.cash = 10000.0
        self._account_summary = MockAccountSummary()

    def get_account_summary(self):
        return self._account_summary

    def add_position(self, symbol, quantity, price, cost=None):
        self.positions.append(MockPosition(symbol, quantity, price, cost))


class TestUnassignedPluginBasic:
    """Basic tests for UnassignedPlugin"""

    def test_plugin_name(self):
        """Test plugin has correct reserved name"""
        plugin = UnassignedPlugin()
        assert plugin.name == UNASSIGNED_PLUGIN_NAME
        assert plugin.name == "_unassigned"

    def test_is_system_plugin(self):
        """Test plugin is marked as system plugin"""
        plugin = UnassignedPlugin()
        assert plugin.IS_SYSTEM_PLUGIN == True
        assert plugin.is_system_plugin == True

    def test_description(self):
        """Test plugin has a description"""
        plugin = UnassignedPlugin()
        assert "unattributed" in plugin.description.lower()

    def test_load(self):
        """Test plugin can be loaded"""
        with tempfile.TemporaryDirectory() as tmpdir:
            plugin = UnassignedPlugin(base_path=Path(tmpdir))
            assert plugin.load() == True
            assert plugin.state == PluginState.LOADED

    def test_start_stop(self):
        """Test plugin lifecycle"""
        with tempfile.TemporaryDirectory() as tmpdir:
            plugin = UnassignedPlugin(base_path=Path(tmpdir))
            plugin.load()

            assert plugin.start() == True
            assert plugin.state == PluginState.STARTED

            assert plugin.stop() == True
            assert plugin.state == PluginState.STOPPED

    def test_freeze_resume(self):
        """Test freeze and resume"""
        with tempfile.TemporaryDirectory() as tmpdir:
            plugin = UnassignedPlugin(base_path=Path(tmpdir))
            plugin.load()
            plugin.start()

            assert plugin.freeze() == True
            assert plugin.state == PluginState.FROZEN

            assert plugin.resume() == True
            assert plugin.state == PluginState.STARTED


class TestUnassignedPluginNoSignals:
    """Test that UnassignedPlugin doesn't generate signals"""

    def test_calculate_signals_empty(self):
        """Test calculate_signals returns empty list"""
        plugin = UnassignedPlugin()
        signals = plugin.calculate_signals({})
        assert signals == []

    def test_calculate_signals_with_data(self):
        """Test calculate_signals ignores market data"""
        plugin = UnassignedPlugin()
        market_data = {
            "SPY": [{"close": 450.0}],
            "QQQ": [{"close": 380.0}],
        }
        signals = plugin.calculate_signals(market_data)
        assert signals == []

    def test_required_bars_zero(self):
        """Test no bars are required"""
        plugin = UnassignedPlugin()
        assert plugin.required_bars == 0


class TestUnassignedPluginSync:
    """Test syncing with portfolio"""

    def test_sync_no_portfolio(self):
        """Test sync fails without portfolio"""
        plugin = UnassignedPlugin()
        assert plugin.sync_from_portfolio() == False

    def test_sync_with_portfolio(self):
        """Test sync from portfolio"""
        portfolio = MockPortfolio()
        portfolio.add_position("AAPL", 100, 175.0)
        portfolio.add_position("MSFT", 50, 380.0)

        plugin = UnassignedPlugin(portfolio=portfolio)
        plugin.load()
        plugin.start()

        # Sync with no claimed symbols - all positions should be unassigned
        assert plugin.sync_from_portfolio(claimed_symbols=set()) == True

        holdings = plugin.get_effective_holdings()
        assert holdings["cash"] == 10000.0  # From mock account
        assert len(holdings["positions"]) == 2

    def test_sync_with_claimed_symbols(self):
        """Test sync excludes claimed symbols"""
        portfolio = MockPortfolio()
        portfolio.add_position("AAPL", 100, 175.0)
        portfolio.add_position("MSFT", 50, 380.0)
        portfolio.add_position("SPY", 200, 450.0)

        plugin = UnassignedPlugin(portfolio=portfolio)
        plugin.load()
        plugin.start()

        # Claim AAPL and MSFT
        claimed = {"AAPL", "MSFT"}
        assert plugin.sync_from_portfolio(claimed_symbols=claimed) == True

        holdings = plugin.get_effective_holdings()
        # Only SPY should be unassigned
        assert len(holdings["positions"]) == 1
        assert holdings["positions"][0]["symbol"] == "SPY"

    def test_sync_with_claimed_cash(self):
        """Test sync excludes claimed cash"""
        portfolio = MockPortfolio()
        plugin = UnassignedPlugin(portfolio=portfolio)
        plugin.load()
        plugin.start()

        # Total available is 10000, claim 3000
        assert plugin.sync_from_portfolio(claimed_symbols=set(), claimed_cash=3000.0) == True

        holdings = plugin.get_effective_holdings()
        assert holdings["cash"] == 7000.0  # 10000 - 3000


class TestUnassignedPluginCashTracking:
    """Test cash balance tracking"""

    def test_initial_cash_zero(self):
        """Test initial cash is zero"""
        plugin = UnassignedPlugin()
        assert plugin.cash_balance == 0.0

    def test_set_cash_balance(self):
        """Test setting cash balance directly"""
        plugin = UnassignedPlugin()
        plugin.load()

        plugin.set_cash_balance(5000.0)
        assert plugin.cash_balance == 5000.0

    def test_cash_in_holdings(self):
        """Test cash appears in holdings"""
        with tempfile.TemporaryDirectory() as tmpdir:
            plugin = UnassignedPlugin(base_path=Path(tmpdir) / "_unassigned")
            plugin.load()
            plugin.set_cash_balance(5000.0)

            holdings = plugin.get_effective_holdings()
            assert holdings["cash"] == 5000.0
            assert holdings["total_value"] == 5000.0


class TestUnassignedPluginClaimedSymbols:
    """Test claimed symbols tracking"""

    def test_initial_claimed_empty(self):
        """Test initial claimed symbols is empty"""
        plugin = UnassignedPlugin()
        assert len(plugin.claimed_symbols) == 0

    def test_set_claimed_symbols(self):
        """Test setting claimed symbols"""
        plugin = UnassignedPlugin()
        plugin.set_claimed_symbols({"SPY", "QQQ"})
        assert plugin.claimed_symbols == {"SPY", "QQQ"}

    def test_add_claimed_symbol(self):
        """Test adding a claimed symbol"""
        plugin = UnassignedPlugin()
        plugin.add_claimed_symbol("SPY")
        plugin.add_claimed_symbol("qqq")  # Should be uppercased
        assert "SPY" in plugin.claimed_symbols
        assert "QQQ" in plugin.claimed_symbols

    def test_remove_claimed_symbol(self):
        """Test removing a claimed symbol"""
        plugin = UnassignedPlugin()
        plugin.set_claimed_symbols({"SPY", "QQQ", "AAPL"})
        plugin.remove_claimed_symbol("QQQ")
        assert plugin.claimed_symbols == {"SPY", "AAPL"}


class TestUnassignedPluginRequests:
    """Test custom request handling"""

    def test_get_cash_request(self):
        """Test get_cash request"""
        plugin = UnassignedPlugin()
        plugin.load()
        plugin.set_cash_balance(12345.67)

        response = plugin.handle_request("get_cash", {})
        assert response["success"] == True
        assert response["cash"] == 12345.67

    def test_get_unassigned_request(self):
        """Test get_unassigned request"""
        plugin = UnassignedPlugin()
        plugin.load()
        plugin.set_cash_balance(5000.0)

        response = plugin.handle_request("get_unassigned", {})
        assert response["success"] == True
        assert response["cash"] == 5000.0
        assert isinstance(response["positions"], list)

    def test_unknown_request(self):
        """Test unknown request returns error"""
        plugin = UnassignedPlugin()
        response = plugin.handle_request("unknown_command", {})
        assert response["success"] == False


class TestUnassignedPluginStatus:
    """Test status reporting"""

    def test_status_includes_system_flag(self):
        """Test status includes is_system_plugin"""
        plugin = UnassignedPlugin()
        plugin.load()

        status = plugin.get_status()
        assert status["is_system_plugin"] == True

    def test_status_includes_cash(self):
        """Test status includes cash balance"""
        plugin = UnassignedPlugin()
        plugin.load()
        plugin.set_cash_balance(7500.0)

        status = plugin.get_status()
        assert status["cash_balance"] == 7500.0

    def test_holdings_includes_system_flag(self):
        """Test holdings includes is_system_plugin"""
        plugin = UnassignedPlugin()
        plugin.load()

        holdings = plugin.get_effective_holdings()
        assert holdings["is_system_plugin"] == True


class TestUnassignedPluginStatePersistence:
    """Test state save/load"""

    def test_state_persistence(self):
        """Test state is saved and restored"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create and configure plugin
            plugin1 = UnassignedPlugin(base_path=Path(tmpdir))
            plugin1.load()
            plugin1.start()
            plugin1.set_cash_balance(8000.0)
            plugin1.set_claimed_symbols({"AAPL", "MSFT"})
            plugin1.stop()

            # Create new plugin instance and load state
            plugin2 = UnassignedPlugin(base_path=Path(tmpdir))
            plugin2.load()
            plugin2.start()

            assert plugin2.cash_balance == 8000.0
            assert plugin2.claimed_symbols == {"AAPL", "MSFT"}
