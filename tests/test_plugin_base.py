"""
Tests for plugins/base.py - Plugin base class and data structures
"""

import pytest
import json
import tempfile
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from plugins.base import (
    PluginBase,
    PluginState,
    HoldingPosition,
    Holdings,
    PluginInstrument,
    TradeSignal,
    PluginResult,
    AlgorithmInstrument,
    AlgorithmResult,
)
from ib.message_bus import MessageBus


class TestPluginState:
    """Tests for PluginState enum"""

    def test_state_values(self):
        """Test that all expected states exist"""
        assert PluginState.UNLOADED.value == "unloaded"
        assert PluginState.LOADED.value == "loaded"
        assert PluginState.STARTED.value == "started"
        assert PluginState.FROZEN.value == "frozen"
        assert PluginState.STOPPED.value == "stopped"
        assert PluginState.ERROR.value == "error"


class TestHoldingPosition:
    """Tests for HoldingPosition dataclass"""

    def test_create_position(self):
        """Test creating a holding position"""
        pos = HoldingPosition(
            symbol="SPY",
            quantity=100,
            cost_basis=45000.0,
            current_price=455.0,
            market_value=45500.0,
        )

        assert pos.symbol == "SPY"
        assert pos.quantity == 100
        assert pos.cost_basis == 45000.0
        assert pos.current_price == 455.0
        assert pos.market_value == 45500.0

    def test_default_values(self):
        """Test default values"""
        pos = HoldingPosition("SPY", 100)
        assert pos.cost_basis == 0.0
        assert pos.current_price == 0.0
        assert pos.market_value == 0.0

    def test_to_dict(self):
        """Test conversion to dict"""
        pos = HoldingPosition("SPY", 100, cost_basis=45000.0, market_value=45500.0)
        d = pos.to_dict()

        assert d["symbol"] == "SPY"
        assert d["quantity"] == 100
        assert d["cost_basis"] == 45000.0
        assert d["market_value"] == 45500.0

    def test_from_dict(self):
        """Test creation from dict"""
        data = {
            "symbol": "SPY",
            "quantity": 100,
            "cost_basis": 45000.0,
            "current_price": 455.0,
            "market_value": 45500.0,
        }
        pos = HoldingPosition.from_dict(data)

        assert pos.symbol == "SPY"
        assert pos.quantity == 100
        assert pos.market_value == 45500.0


class TestHoldings:
    """Tests for Holdings dataclass"""

    def test_create_holdings(self):
        """Test creating holdings"""
        holdings = Holdings(plugin_name="test_plugin", initial_cash=10000.0)
        assert holdings.plugin_name == "test_plugin"
        assert holdings.initial_cash == 10000.0
        assert holdings.current_positions == []

    def test_total_value(self):
        """Test total value calculation"""
        holdings = Holdings(plugin_name="test", current_cash=5000.0)
        holdings.current_positions = [
            HoldingPosition("SPY", 100, market_value=45500.0),
            HoldingPosition("QQQ", 50, market_value=19250.0),
        ]

        # 5000 + 45500 + 19250 = 69750
        assert holdings.total_value == 69750.0

    def test_get_position(self):
        """Test getting a position by symbol"""
        holdings = Holdings(plugin_name="test")
        holdings.current_positions = [
            HoldingPosition("SPY", 100, market_value=45500.0),
            HoldingPosition("QQQ", 50, market_value=19250.0),
        ]

        pos = holdings.get_position("SPY")
        assert pos is not None
        assert pos.quantity == 100

        missing = holdings.get_position("AAPL")
        assert missing is None

    def test_to_dict(self):
        """Test conversion to dict"""
        holdings = Holdings(
            plugin_name="test",
            initial_cash=10000.0,
            current_cash=5000.0,
        )

        d = holdings.to_dict()
        assert d["plugin"] == "test"
        assert d["initial_funding"]["cash"] == 10000.0
        assert d["current_holdings"]["cash"] == 5000.0


class TestPluginInstrument:
    """Tests for PluginInstrument dataclass"""

    def test_create_instrument(self):
        """Test creating an instrument"""
        inst = PluginInstrument(
            symbol="SPY",
            name="S&P 500 ETF",
            weight=60.0,
        )

        assert inst.symbol == "SPY"
        assert inst.name == "S&P 500 ETF"
        assert inst.weight == 60.0
        assert inst.enabled is True

    def test_to_contract(self):
        """Test creating IB contract from instrument"""
        inst = PluginInstrument("SPY", "Test", exchange="ARCA")
        contract = inst.to_contract()

        assert contract.symbol == "SPY"
        assert contract.exchange == "ARCA"
        assert contract.secType == "STK"

    def test_to_dict(self):
        """Test conversion to dict"""
        inst = PluginInstrument("SPY", "S&P 500", weight=50.0)
        d = inst.to_dict()

        assert d["symbol"] == "SPY"
        assert d["weight"] == 50.0
        assert d["enabled"] is True

    def test_algorithm_instrument_alias(self):
        """Test that AlgorithmInstrument is an alias"""
        assert AlgorithmInstrument is PluginInstrument


class TestTradeSignal:
    """Tests for TradeSignal dataclass"""

    def test_create_buy_signal(self):
        """Test creating a buy signal"""
        signal = TradeSignal(
            symbol="SPY",
            action="BUY",
            quantity=10,
            reason="Momentum signal",
        )

        assert signal.symbol == "SPY"
        assert signal.action == "BUY"
        assert signal.quantity == 10
        assert signal.reason == "Momentum signal"

    def test_create_with_target_weight(self):
        """Test creating with target weight"""
        signal = TradeSignal(
            symbol="SPY",
            action="REBALANCE",
            target_weight=0.6,
            reason="Target allocation",
        )

        assert signal.target_weight == 0.6

    def test_is_actionable(self):
        """Test is_actionable property"""
        buy_signal = TradeSignal("SPY", "BUY", quantity=10)
        assert buy_signal.is_actionable is True

        sell_signal = TradeSignal("SPY", "SELL", quantity=5)
        assert sell_signal.is_actionable is True

        hold_signal = TradeSignal("SPY", "HOLD", quantity=10)
        assert hold_signal.is_actionable is False

        zero_qty = TradeSignal("SPY", "BUY", quantity=0)
        assert zero_qty.is_actionable is False


class TestPluginResult:
    """Tests for PluginResult dataclass"""

    def test_create_success_result(self):
        """Test creating a successful result"""
        signals = [TradeSignal("SPY", "BUY", quantity=10)]
        result = PluginResult(
            plugin_name="test",
            timestamp=datetime.now(),
            signals=signals,
            success=True,
        )

        assert result.success is True
        assert len(result.signals) == 1
        assert result.error is None

    def test_create_error_result(self):
        """Test creating an error result"""
        result = PluginResult(
            plugin_name="test",
            timestamp=datetime.now(),
            success=False,
            error="Failed to calculate",
        )

        assert result.success is False
        assert result.error == "Failed to calculate"

    def test_actionable_signals(self):
        """Test actionable signals filter"""
        signals = [
            TradeSignal("SPY", "BUY", quantity=10),
            TradeSignal("QQQ", "HOLD", quantity=5),
            TradeSignal("AAPL", "SELL", quantity=20),
        ]
        result = PluginResult(
            plugin_name="test",
            timestamp=datetime.now(),
            signals=signals,
        )

        actionable = result.actionable_signals
        assert len(actionable) == 2  # BUY and SELL are actionable

    def test_algorithm_result_alias(self):
        """Test that AlgorithmResult is an alias"""
        assert AlgorithmResult is PluginResult


class ConcreteTestPlugin(PluginBase):
    """Concrete plugin implementation for testing"""

    def __init__(self, name="test_plugin", **kwargs):
        super().__init__(name, **kwargs)
        self._started = False
        self._stopped = False
        self._frozen = False

    @property
    def description(self) -> str:
        return "A test plugin for unit testing"

    def start(self) -> bool:
        self._started = True
        return True

    def stop(self) -> bool:
        self._stopped = True
        return True

    def freeze(self) -> bool:
        self._frozen = True
        return True

    def resume(self) -> bool:
        self._frozen = False
        return True

    def handle_request(self, request_type: str, payload: dict) -> dict:
        if request_type == "echo":
            return {"success": True, "echo": payload}
        return {"success": False, "error": "Unknown request"}

    def calculate_signals(self, market_data: dict) -> list:
        return [TradeSignal("SPY", "HOLD", reason="Test")]


class TestPluginBase:
    """Tests for PluginBase abstract class"""

    def test_create_plugin(self):
        """Test creating a plugin"""
        plugin = ConcreteTestPlugin("my_plugin")

        assert plugin.name == "my_plugin"
        assert plugin.state == PluginState.UNLOADED

    def test_plugin_with_message_bus(self):
        """Test plugin with MessageBus"""
        bus = MessageBus()
        plugin = ConcreteTestPlugin(message_bus=bus)

        assert plugin._message_bus is bus

    def test_add_instrument(self):
        """Test adding instruments"""
        plugin = ConcreteTestPlugin()
        inst = PluginInstrument("SPY", "S&P 500", weight=60.0)

        plugin.add_instrument(inst)

        assert len(plugin.instruments) == 1
        assert plugin.instruments[0].symbol == "SPY"

    def test_remove_instrument(self):
        """Test removing instruments"""
        plugin = ConcreteTestPlugin()
        plugin.add_instrument(PluginInstrument("SPY", "Test"))
        plugin.add_instrument(PluginInstrument("QQQ", "Test"))

        result = plugin.remove_instrument("SPY")

        assert result is True
        assert len(plugin.instruments) == 1
        assert plugin.instruments[0].symbol == "QQQ"

    def test_get_instrument(self):
        """Test getting instrument by symbol"""
        plugin = ConcreteTestPlugin()
        plugin.add_instrument(PluginInstrument("SPY", "S&P 500"))

        inst = plugin.get_instrument("SPY")
        assert inst is not None
        assert inst.name == "S&P 500"

        missing = plugin.get_instrument("QQQ")
        assert missing is None

    def test_load_creates_holdings(self):
        """Test that load creates holdings"""
        plugin = ConcreteTestPlugin()
        plugin.add_instrument(PluginInstrument("SPY", "Test"))

        result = plugin.load()

        assert result is True
        assert plugin.is_loaded is True
        assert plugin.holdings is not None

    def test_run_calculates_signals(self):
        """Test running plugin calculates signals"""
        plugin = ConcreteTestPlugin()
        plugin.add_instrument(PluginInstrument("SPY", "Test"))
        plugin.load()

        result = plugin.run({})

        assert result.success is True
        assert len(result.signals) == 1

    def test_lifecycle_methods(self):
        """Test lifecycle methods are called"""
        plugin = ConcreteTestPlugin()

        plugin.start()
        assert plugin._started is True

        plugin.freeze()
        assert plugin._frozen is True

        plugin.resume()
        assert plugin._frozen is False

        plugin.stop()
        assert plugin._stopped is True

    def test_handle_request(self):
        """Test custom request handling"""
        plugin = ConcreteTestPlugin()

        response = plugin.handle_request("echo", {"msg": "hello"})
        assert response["success"] is True
        assert response["echo"]["msg"] == "hello"

        response = plugin.handle_request("unknown", {})
        assert response["success"] is False


class TestPluginStatePersistence:
    """Tests for plugin state persistence"""

    def test_save_and_load_state(self):
        """Test saving and loading state"""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"

            plugin = ConcreteTestPlugin()
            plugin._state_file = state_file

            # Save state
            plugin.save_state({"counter": 42, "data": [1, 2, 3]})

            # Load state
            loaded = plugin.load_state()

            assert loaded["counter"] == 42
            assert loaded["data"] == [1, 2, 3]

    def test_load_state_missing_file(self):
        """Test loading state when file doesn't exist"""
        plugin = ConcreteTestPlugin()
        plugin._state_file = Path("/nonexistent/state.json")

        loaded = plugin.load_state()
        assert loaded == {}

    def test_state_file_location(self):
        """Test default state file location"""
        plugin = ConcreteTestPlugin("my_plugin")

        # State file should be in plugin directory
        assert "my_plugin" in str(plugin._state_file)
        assert plugin._state_file.name == "state.json"


class TestPluginMessageBusIntegration:
    """Tests for MessageBus integration in plugins"""

    def test_publish_message(self):
        """Test publishing messages"""
        bus = MessageBus()
        received = []

        bus.subscribe("test_channel", lambda m: received.append(m), "subscriber")

        plugin = ConcreteTestPlugin(message_bus=bus)
        plugin.publish("test_channel", {"value": 42})

        assert len(received) == 1
        assert received[0].payload["value"] == 42

    def test_publish_without_bus(self):
        """Test publishing without MessageBus"""
        plugin = ConcreteTestPlugin()

        # Should not raise, just return False
        result = plugin.publish("test", {"data": 1})
        assert result is False

    def test_subscribe_to_channel(self):
        """Test subscribing to channels"""
        bus = MessageBus()
        plugin = ConcreteTestPlugin(message_bus=bus)
        received = []

        plugin.subscribe("external_signals", lambda m: received.append(m))
        bus.publish("external_signals", {"signal": "BUY"}, "other_plugin")

        assert len(received) == 1

    def test_unsubscribe_from_channel(self):
        """Test unsubscribing from channels"""
        bus = MessageBus()
        plugin = ConcreteTestPlugin(message_bus=bus)
        received = []

        plugin.subscribe("test", lambda m: received.append(m))
        bus.publish("test", {"first": True}, "pub")

        plugin.unsubscribe("test")
        bus.publish("test", {"second": True}, "pub")

        assert len(received) == 1

    def test_unsubscribe_all(self):
        """Test unsubscribing from all channels"""
        bus = MessageBus()
        plugin = ConcreteTestPlugin(message_bus=bus)

        plugin.subscribe("channel1", lambda m: None)
        plugin.subscribe("channel2", lambda m: None)
        plugin.subscribe("channel3", lambda m: None)

        count = plugin.unsubscribe_all()

        assert count == 3


class TestPluginWithHoldings:
    """Tests for plugin holdings management"""

    def test_holdings_created_on_load(self):
        """Test that holdings are created on load"""
        plugin = ConcreteTestPlugin()
        plugin.add_instrument(PluginInstrument("SPY", "Test", weight=60))
        plugin.add_instrument(PluginInstrument("QQQ", "Test", weight=40))

        plugin.load()

        assert plugin.holdings is not None
        assert plugin.holdings.plugin_name == "test_plugin"

    def test_get_effective_total_value(self):
        """Test getting effective total value"""
        plugin = ConcreteTestPlugin()
        plugin.add_instrument(PluginInstrument("SPY", "Test"))
        plugin.load()

        # Set some holdings
        plugin._holdings.current_cash = 10000.0
        plugin._holdings.current_positions = [
            HoldingPosition("SPY", 100, market_value=45500.0)
        ]

        value = plugin.get_effective_total_value()
        assert value == 55500.0  # 10000 + 45500

    def test_get_position_via_holdings(self):
        """Test getting position via holdings"""
        plugin = ConcreteTestPlugin()
        plugin.add_instrument(PluginInstrument("SPY", "Test"))
        plugin.load()

        plugin._holdings.current_positions = [
            HoldingPosition("SPY", 100, market_value=45500.0)
        ]

        pos = plugin.holdings.get_position("SPY")
        assert pos is not None
        assert pos.market_value == 45500.0

        missing = plugin.holdings.get_position("QQQ")
        assert missing is None
