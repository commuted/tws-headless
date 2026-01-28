"""
Tests for internal transfers between plugins (cash and positions)
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from typing import Optional, Dict, Any, List

from ib.plugins.base import Holdings, HoldingPosition, PluginBase, PluginState


class TestHoldingsTransferMethods:
    """Test Holdings class transfer support methods"""

    def test_add_cash_positive(self):
        """Test adding cash to holdings"""
        holdings = Holdings(plugin_name="test", current_cash=1000.0)
        holdings.add_cash(500.0)
        assert holdings.current_cash == 1500.0

    def test_add_cash_negative(self):
        """Test subtracting cash from holdings"""
        holdings = Holdings(plugin_name="test", current_cash=1000.0)
        holdings.add_cash(-300.0)
        assert holdings.current_cash == 700.0

    def test_add_cash_updates_timestamp(self):
        """Test add_cash updates last_updated"""
        holdings = Holdings(plugin_name="test", current_cash=1000.0)
        old_time = holdings.last_updated
        holdings.add_cash(100.0)
        assert holdings.last_updated is not None
        assert holdings.last_updated != old_time

    def test_add_position_new(self):
        """Test adding a new position"""
        holdings = Holdings(plugin_name="test")
        holdings.add_position("SPY", 100, cost_basis=450.0, current_price=455.0)

        assert len(holdings.current_positions) == 1
        pos = holdings.get_position("SPY")
        assert pos.quantity == 100
        assert pos.cost_basis == 450.0
        assert pos.current_price == 455.0

    def test_add_position_existing(self):
        """Test adding to an existing position"""
        holdings = Holdings(
            plugin_name="test",
            current_positions=[
                HoldingPosition(symbol="SPY", quantity=100, cost_basis=450.0, current_price=455.0)
            ]
        )

        # Add 50 more at different cost
        holdings.add_position("SPY", 50, cost_basis=460.0, current_price=455.0)

        pos = holdings.get_position("SPY")
        assert pos.quantity == 150
        # Cost basis should be weighted average: (100*450 + 50*460) / 150 = 453.33
        expected_cost = (100 * 450.0 + 50 * 460.0) / 150
        assert abs(pos.cost_basis - expected_cost) < 0.01

    def test_remove_position_partial(self):
        """Test removing part of a position"""
        holdings = Holdings(
            plugin_name="test",
            current_positions=[
                HoldingPosition(symbol="SPY", quantity=100, cost_basis=450.0)
            ]
        )

        result = holdings.remove_position("SPY", 30)
        assert result == True

        pos = holdings.get_position("SPY")
        assert pos.quantity == 70

    def test_remove_position_full(self):
        """Test removing entire position"""
        holdings = Holdings(
            plugin_name="test",
            current_positions=[
                HoldingPosition(symbol="SPY", quantity=100, cost_basis=450.0)
            ]
        )

        result = holdings.remove_position("SPY", 100)
        assert result == True
        assert holdings.get_position("SPY") is None
        assert len(holdings.current_positions) == 0

    def test_remove_position_insufficient(self):
        """Test removing more than available fails"""
        holdings = Holdings(
            plugin_name="test",
            current_positions=[
                HoldingPosition(symbol="SPY", quantity=100, cost_basis=450.0)
            ]
        )

        result = holdings.remove_position("SPY", 150)
        assert result == False
        # Position should be unchanged
        assert holdings.get_position("SPY").quantity == 100

    def test_remove_position_not_found(self):
        """Test removing non-existent position fails"""
        holdings = Holdings(plugin_name="test")
        result = holdings.remove_position("SPY", 50)
        assert result == False


class MockPlugin:
    """Mock plugin for testing transfers"""

    def __init__(self, name: str, cash: float = 0.0, positions: List[Dict] = None):
        self.name = name
        self._holdings = Holdings(
            plugin_name=name,
            current_cash=cash,
            current_positions=[
                HoldingPosition(
                    symbol=p["symbol"],
                    quantity=p["quantity"],
                    cost_basis=p.get("cost_basis", 0.0),
                    current_price=p.get("current_price", 0.0),
                )
                for p in (positions or [])
            ],
            created_at=datetime.now(),
        )
        self.is_system_plugin = False
        self._state = PluginState.STARTED

    @property
    def holdings(self):
        return self._holdings

    @property
    def state(self):
        return self._state

    def get_effective_cash(self) -> float:
        return self._holdings.current_cash

    def get_effective_position(self, symbol: str):
        pos = self._holdings.get_position(symbol)
        if pos:
            return (pos.quantity, pos.market_value)
        return (0.0, 0.0)

    def get_effective_holdings(self) -> Dict:
        return {
            "plugin": self.name,
            "cash": self._holdings.current_cash,
            "positions": [
                {
                    "symbol": p.symbol,
                    "quantity": p.quantity,
                    "current_price": p.current_price,
                    "market_value": p.market_value,
                }
                for p in self._holdings.current_positions
            ],
            "total_value": self._holdings.total_value,
        }

    def save_holdings(self):
        pass  # No-op for tests


class MockPluginConfig:
    """Mock plugin config"""
    def __init__(self, plugin):
        self.plugin = plugin
        self.enabled = True


class MockPortfolio:
    """Mock portfolio for testing"""
    def __init__(self):
        self.positions = []

    def get_position(self, symbol: str):
        for p in self.positions:
            if p.symbol == symbol:
                return p
        return None


class MockPosition:
    """Mock portfolio position"""
    def __init__(self, symbol: str, price: float):
        self.symbol = symbol
        self.current_price = price


class TestPluginExecutiveTransfers:
    """Test PluginExecutive transfer methods"""

    def setup_method(self):
        """Set up test fixtures"""
        # Create mock plugin executive
        from ib.plugin_executive import PluginExecutive

        self.portfolio = MockPortfolio()

        # We'll patch the executive methods directly
        self.pe = Mock(spec=PluginExecutive)
        self.pe.portfolio = self.portfolio
        self.pe._lock = MagicMock()
        self.pe._plugins = {}

        # Create test plugins
        self.plugin_a = MockPlugin("plugin_a", cash=10000.0, positions=[
            {"symbol": "SPY", "quantity": 100, "cost_basis": 450.0, "current_price": 455.0},
            {"symbol": "QQQ", "quantity": 50, "cost_basis": 380.0, "current_price": 385.0},
        ])
        self.plugin_b = MockPlugin("plugin_b", cash=5000.0, positions=[])

        self.pe._plugins = {
            "plugin_a": MockPluginConfig(self.plugin_a),
            "plugin_b": MockPluginConfig(self.plugin_b),
        }

    def test_transfer_cash_success(self):
        """Test successful cash transfer"""
        # Use real transfer_cash method from PluginExecutive
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        pe._lock = MagicMock()
        pe._plugins = self.pe._plugins
        pe.portfolio = self.portfolio

        success, message = pe.transfer_cash("plugin_a", "plugin_b", 3000.0)

        assert success == True
        assert self.plugin_a.holdings.current_cash == 7000.0
        assert self.plugin_b.holdings.current_cash == 8000.0

    def test_transfer_cash_insufficient(self):
        """Test cash transfer with insufficient funds"""
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        pe._lock = MagicMock()
        pe._plugins = self.pe._plugins
        pe.portfolio = self.portfolio

        success, message = pe.transfer_cash("plugin_a", "plugin_b", 15000.0)

        assert success == False
        assert "Insufficient" in message
        # Balances unchanged
        assert self.plugin_a.holdings.current_cash == 10000.0
        assert self.plugin_b.holdings.current_cash == 5000.0

    def test_transfer_cash_invalid_amount(self):
        """Test cash transfer with invalid amount"""
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        pe._lock = MagicMock()
        pe._plugins = self.pe._plugins
        pe.portfolio = self.portfolio

        success, message = pe.transfer_cash("plugin_a", "plugin_b", -100.0)

        assert success == False
        assert "positive" in message.lower()

    def test_transfer_cash_source_not_found(self):
        """Test cash transfer from non-existent plugin"""
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        pe._lock = MagicMock()
        pe._plugins = self.pe._plugins
        pe.portfolio = self.portfolio

        success, message = pe.transfer_cash("unknown", "plugin_b", 100.0)

        assert success == False
        assert "not found" in message

    def test_transfer_cash_dest_not_found(self):
        """Test cash transfer to non-existent plugin"""
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        pe._lock = MagicMock()
        pe._plugins = self.pe._plugins
        pe.portfolio = self.portfolio

        success, message = pe.transfer_cash("plugin_a", "unknown", 100.0)

        assert success == False
        assert "not found" in message

    def test_transfer_position_success(self):
        """Test successful position transfer"""
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        pe._lock = MagicMock()
        pe._plugins = self.pe._plugins
        pe.portfolio = self.portfolio

        success, message = pe.transfer_position("plugin_a", "plugin_b", "SPY", 30, price=455.0)

        assert success == True

        # Check source
        source_pos = self.plugin_a.holdings.get_position("SPY")
        assert source_pos.quantity == 70

        # Check destination
        dest_pos = self.plugin_b.holdings.get_position("SPY")
        assert dest_pos.quantity == 30
        assert dest_pos.cost_basis == 450.0  # Preserved from source

    def test_transfer_position_full(self):
        """Test transferring entire position"""
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        pe._lock = MagicMock()
        pe._plugins = self.pe._plugins
        pe.portfolio = self.portfolio

        success, message = pe.transfer_position("plugin_a", "plugin_b", "QQQ", 50)

        assert success == True

        # Source should have no QQQ
        assert self.plugin_a.holdings.get_position("QQQ") is None

        # Destination should have all QQQ
        dest_pos = self.plugin_b.holdings.get_position("QQQ")
        assert dest_pos.quantity == 50

    def test_transfer_position_insufficient(self):
        """Test position transfer with insufficient quantity"""
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        pe._lock = MagicMock()
        pe._plugins = self.pe._plugins
        pe.portfolio = self.portfolio

        success, message = pe.transfer_position("plugin_a", "plugin_b", "SPY", 150)

        assert success == False
        assert "Insufficient" in message

        # Position unchanged
        assert self.plugin_a.holdings.get_position("SPY").quantity == 100

    def test_transfer_position_not_found(self):
        """Test transferring non-existent position"""
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        pe._lock = MagicMock()
        pe._plugins = self.pe._plugins
        pe.portfolio = self.portfolio

        success, message = pe.transfer_position("plugin_a", "plugin_b", "AAPL", 50)

        assert success == False
        assert "Insufficient" in message

    def test_transfer_position_invalid_quantity(self):
        """Test position transfer with invalid quantity"""
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        pe._lock = MagicMock()
        pe._plugins = self.pe._plugins
        pe.portfolio = self.portfolio

        success, message = pe.transfer_position("plugin_a", "plugin_b", "SPY", -10)

        assert success == False
        assert "positive" in message.lower()

    def test_transfer_position_uses_portfolio_price(self):
        """Test position transfer gets price from portfolio"""
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        pe._lock = MagicMock()
        pe._plugins = self.pe._plugins

        # Add position to portfolio with current price
        self.portfolio.positions = [MockPosition("SPY", 460.0)]
        pe.portfolio = self.portfolio

        success, message = pe.transfer_position("plugin_a", "plugin_b", "SPY", 20)

        assert success == True
        dest_pos = self.plugin_b.holdings.get_position("SPY")
        assert dest_pos.current_price == 460.0

    def test_get_transferable_cash(self):
        """Test getting transferable cash"""
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        pe._lock = MagicMock()
        pe._plugins = self.pe._plugins

        cash = pe.get_transferable_cash("plugin_a")
        assert cash == 10000.0

    def test_get_transferable_cash_not_found(self):
        """Test getting transferable cash from unknown plugin"""
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        pe._lock = MagicMock()
        pe._plugins = self.pe._plugins

        cash = pe.get_transferable_cash("unknown")
        assert cash == 0.0

    def test_get_transferable_positions(self):
        """Test getting transferable positions"""
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        pe._lock = MagicMock()
        pe._plugins = self.pe._plugins

        positions = pe.get_transferable_positions("plugin_a")
        assert len(positions) == 2

        spy = next(p for p in positions if p["symbol"] == "SPY")
        assert spy["quantity"] == 100

    def test_get_transferable_positions_empty(self):
        """Test getting transferable positions from plugin with none"""
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        pe._lock = MagicMock()
        pe._plugins = self.pe._plugins

        positions = pe.get_transferable_positions("plugin_b")
        assert positions == []


class TestTransferCommand:
    """Test transfer socket command"""

    def setup_method(self):
        from ib.run_engine import EngineCommandHandler

        # Create mock engine
        self.engine = Mock()
        self.engine.portfolio = MockPortfolio()

        # Create mock plugin executive
        self.plugin_a = MockPlugin("plugin_a", cash=10000.0, positions=[
            {"symbol": "SPY", "quantity": 100, "cost_basis": 450.0, "current_price": 455.0},
        ])
        self.plugin_b = MockPlugin("plugin_b", cash=5000.0)

        self.pe = Mock()
        self.pe._plugins = {
            "plugin_a": MockPluginConfig(self.plugin_a),
            "plugin_b": MockPluginConfig(self.plugin_b),
        }
        self.pe.get_transferable_cash.return_value = 10000.0
        self.pe.get_transferable_positions.return_value = [
            {"symbol": "SPY", "quantity": 100, "value": 45500.0}
        ]
        self.pe.transfer_cash.return_value = (True, "Transfer successful")
        self.pe.transfer_position.return_value = (True, "Transfer successful")

        self.engine.plugin_executive = self.pe

        self.handler = EngineCommandHandler(self.engine)

    def test_transfer_no_plugin_executive(self):
        """Test transfer requires plugin executive"""
        self.engine.plugin_executive = None
        result = self.handler.handle_transfer(["cash", "a", "b", "100"])
        assert result.status.value == "error"
        assert "requires plugin executive" in result.message

    def test_transfer_no_args(self):
        """Test transfer without arguments shows usage"""
        result = self.handler.handle_transfer([])
        assert result.status.value == "error"
        assert "Usage" in result.message

    def test_transfer_list(self):
        """Test transfer list command"""
        result = self.handler.handle_transfer(["list", "plugin_a"])
        assert result.status.value == "success"
        assert "plugin_a" in result.message
        assert "Cash:" in result.message
        assert "SPY" in result.message

    def test_transfer_list_missing_plugin(self):
        """Test transfer list without plugin name"""
        result = self.handler.handle_transfer(["list"])
        assert result.status.value == "error"
        assert "Usage" in result.message

    def test_transfer_cash_dry_run(self):
        """Test cash transfer dry run"""
        result = self.handler.handle_transfer(["cash", "plugin_a", "plugin_b", "1000"])
        assert result.status.value == "success"
        assert "DRY RUN" in result.message
        assert result.data["dry_run"] == True

    def test_transfer_cash_confirm(self):
        """Test cash transfer with confirm"""
        result = self.handler.handle_transfer(["cash", "plugin_a", "plugin_b", "1000", "--confirm"])
        assert result.status.value == "success"
        self.pe.transfer_cash.assert_called_once_with("plugin_a", "plugin_b", 1000.0)

    def test_transfer_cash_invalid_amount(self):
        """Test cash transfer with invalid amount"""
        result = self.handler.handle_transfer(["cash", "plugin_a", "plugin_b", "abc"])
        assert result.status.value == "error"
        assert "Invalid amount" in result.message

    def test_transfer_position_dry_run(self):
        """Test position transfer dry run"""
        result = self.handler.handle_transfer(["position", "plugin_a", "plugin_b", "SPY", "50"])
        assert result.status.value == "success"
        assert "DRY RUN" in result.message

    def test_transfer_position_confirm(self):
        """Test position transfer with confirm"""
        result = self.handler.handle_transfer(["position", "plugin_a", "plugin_b", "SPY", "50", "--confirm"])
        assert result.status.value == "success"
        self.pe.transfer_position.assert_called_once_with("plugin_a", "plugin_b", "SPY", 50.0)

    def test_transfer_position_invalid_quantity(self):
        """Test position transfer with invalid quantity"""
        result = self.handler.handle_transfer(["position", "plugin_a", "plugin_b", "SPY", "abc"])
        assert result.status.value == "error"
        assert "Invalid quantity" in result.message

    def test_transfer_unknown_subcommand(self):
        """Test transfer with unknown subcommand"""
        result = self.handler.handle_transfer(["invalid"])
        assert result.status.value == "error"
        assert "Unknown transfer subcommand" in result.message

    def test_transfer_cash_missing_args(self):
        """Test cash transfer with missing arguments"""
        result = self.handler.handle_transfer(["cash", "plugin_a"])
        assert result.status.value == "error"
        assert "Usage" in result.message

    def test_transfer_position_missing_args(self):
        """Test position transfer with missing arguments"""
        result = self.handler.handle_transfer(["position", "plugin_a", "plugin_b"])
        assert result.status.value == "error"
        assert "Usage" in result.message
