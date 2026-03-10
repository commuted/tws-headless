"""
Tests for plugins/base.py - Plugin base class and data structures
"""

import pytest
import json
import tempfile
from decimal import Decimal
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
            quantity=Decimal("10"),
            reason="Momentum signal",
        )

        assert signal.symbol == "SPY"
        assert signal.action == "BUY"
        assert signal.quantity == Decimal("10")
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
        buy_signal = TradeSignal("SPY", "BUY", quantity=Decimal("10"))
        assert buy_signal.is_actionable is True

        sell_signal = TradeSignal("SPY", "SELL", quantity=Decimal("5"))
        assert sell_signal.is_actionable is True

        hold_signal = TradeSignal("SPY", "HOLD", quantity=Decimal("10"))
        assert hold_signal.is_actionable is False

        zero_qty = TradeSignal("SPY", "BUY", quantity=Decimal("0"))
        assert zero_qty.is_actionable is False


class TestPluginResult:
    """Tests for PluginResult dataclass"""

    def test_create_success_result(self):
        """Test creating a successful result"""
        signals = [TradeSignal("SPY", "BUY", quantity=Decimal("10"))]
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
            TradeSignal("SPY", "BUY", quantity=Decimal("10")),
            TradeSignal("QQQ", "HOLD", quantity=Decimal("5")),
            TradeSignal("AAPL", "SELL", quantity=Decimal("20")),
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

    def calculate_signals(self) -> list:
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

        result = plugin.run()

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


class TestNewCallbacks:
    """Tests for on_commission and on_pnl callback signatures"""

    def test_on_commission_exists_and_callable(self):
        """on_commission exists as a callable no-op on PluginBase."""
        plugin = ConcreteTestPlugin()
        assert hasattr(plugin, "on_commission")
        assert callable(plugin.on_commission)

    def test_on_commission_signature(self):
        """on_commission accepts (exec_id, commission, realized_pnl, currency)."""
        plugin = ConcreteTestPlugin()
        # Should not raise
        plugin.on_commission("exec_abc", 1.23, 0.0, "USD")

    def test_on_pnl_exists_and_callable(self):
        """on_pnl exists as a callable no-op on PluginBase."""
        plugin = ConcreteTestPlugin()
        assert hasattr(plugin, "on_pnl")
        assert callable(plugin.on_pnl)

    def test_on_pnl_signature(self):
        """on_pnl accepts a pnl_data argument."""
        from unittest.mock import Mock
        plugin = ConcreteTestPlugin()
        # Should not raise
        plugin.on_pnl(Mock())

    def test_on_pnl_accepts_pnl_data_instance(self):
        """on_pnl works with an actual PnLData object."""
        from ib.models import PnLData
        plugin = ConcreteTestPlugin()
        data = PnLData(account="DU1", daily_pnl=10.0, unrealized_pnl=50.0, realized_pnl=0.0)
        plugin.on_pnl(data)  # base no-op should not raise

    def test_on_commission_accepts_all_four_args(self):
        """on_commission works with all four expected arguments."""
        plugin = ConcreteTestPlugin()
        plugin.on_commission("exec_001", 1.25, 0.0, "USD")  # should not raise


# =============================================================================
# Slot / instance key
# =============================================================================


class TestPluginSlot:
    def test_slot_defaults_to_name(self):
        plugin = ConcreteTestPlugin("my_plugin")
        assert plugin.slot == "my_plugin"

    def test_slot_can_be_overridden(self):
        plugin = ConcreteTestPlugin("my_plugin")
        plugin.slot = "spy_momentum"
        assert plugin.slot == "spy_momentum"
        assert plugin.name == "my_plugin"  # name unchanged

    def test_slot_used_as_storage_key(self):
        """State is stored and retrieved under the slot key."""
        plugin = ConcreteTestPlugin("my_plugin")
        plugin.slot = "spy_momentum"

        plugin.save_state({"x": 99})
        loaded = plugin.load_state()
        assert loaded == {"x": 99}

    def test_two_slots_independent_state(self):
        """Two instances with different slots have independent state."""
        p1 = ConcreteTestPlugin("my_plugin")
        p1.slot = "slot_a"
        p2 = ConcreteTestPlugin("my_plugin")
        p2.slot = "slot_b"

        p1.save_state({"from": "a"})
        p2.save_state({"from": "b"})

        assert p1.load_state() == {"from": "a"}
        assert p2.load_state() == {"from": "b"}

    def test_slot_in_get_status(self):
        plugin = ConcreteTestPlugin("my_plugin")
        plugin.slot = "spy_momentum"
        status = plugin.get_status()
        assert status["slot"] == "spy_momentum"
        assert status["name"] == "my_plugin"


# =============================================================================
# INSTRUMENT_COMPLIANCE
# =============================================================================


class TestInstrumentCompliance:
    def test_default_compliance_false(self):
        plugin = ConcreteTestPlugin()
        assert plugin.INSTRUMENT_COMPLIANCE is False

    def test_compliance_class_attribute(self):
        class CompliantPlugin(ConcreteTestPlugin):
            INSTRUMENT_COMPLIANCE = True

        p = CompliantPlugin()
        assert p.INSTRUMENT_COMPLIANCE is True

    def test_compliance_in_get_status(self):
        class CompliantPlugin(ConcreteTestPlugin):
            INSTRUMENT_COMPLIANCE = True

        p = CompliantPlugin()
        assert p.get_status()["instrument_compliance"] is True

    def test_non_compliant_status(self):
        plugin = ConcreteTestPlugin()
        assert plugin.get_status()["instrument_compliance"] is False


# =============================================================================
# cli_help
# =============================================================================


class TestCLIHelp:
    def test_default_cli_help_returns_string(self):
        plugin = ConcreteTestPlugin("my_plugin")
        result = plugin.cli_help()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_override_cli_help(self):
        class HelpfulPlugin(ConcreteTestPlugin):
            def cli_help(self) -> str:
                return "helpful_plugin commands:\n  request helpful_plugin ping {}"

        p = HelpfulPlugin()
        assert "ping" in p.cli_help()

    def test_default_mentions_plugin_name(self):
        plugin = ConcreteTestPlugin("my_plugin")
        assert "my_plugin" in plugin.cli_help()


# =============================================================================
# Instrument SQLite persistence
# =============================================================================


class TestInstrumentSQLitePersistence:
    """Tests that add_instrument, remove_instrument, and save_instruments
    write through to SQLite and that reload_instruments reads back."""

    def test_add_instrument_persists(self, _isolated_plugin_store):
        """add_instrument writes to store immediately."""
        plugin = ConcreteTestPlugin("persist_test")
        plugin.add_instrument(PluginInstrument("SPY", "S&P 500"))
        stored = _isolated_plugin_store.load_instruments(plugin.slot)
        assert stored is not None
        assert any(i.symbol == "SPY" for i in stored)

    def test_remove_instrument_persists(self, _isolated_plugin_store):
        """remove_instrument deletes from store immediately."""
        plugin = ConcreteTestPlugin("persist_test")
        plugin.add_instrument(PluginInstrument("SPY", "S&P 500"))
        plugin.add_instrument(PluginInstrument("QQQ", "Nasdaq"))
        plugin.remove_instrument("SPY")
        stored = _isolated_plugin_store.load_instruments(plugin.slot)
        assert not any(i.symbol == "SPY" for i in stored)
        assert any(i.symbol == "QQQ" for i in stored)

    def test_save_instruments_writes_all(self, _isolated_plugin_store):
        """save_instruments replaces all rows."""
        plugin = ConcreteTestPlugin("persist_test")
        plugin.add_instrument(PluginInstrument("SPY", "S&P 500"))
        plugin.add_instrument(PluginInstrument("QQQ", "Nasdaq"))
        # Overwrite with single instrument
        plugin._instruments.clear()
        plugin._instruments["AAPL"] = PluginInstrument("AAPL", "Apple")
        plugin.save_instruments()
        stored = _isolated_plugin_store.load_instruments(plugin.slot)
        assert len(stored) == 1
        assert stored[0].symbol == "AAPL"

    def test_reload_instruments_reads_from_store(self, _isolated_plugin_store):
        """reload_instruments re-reads SQLite into memory."""
        plugin = ConcreteTestPlugin("persist_test")
        plugin.add_instrument(PluginInstrument("SPY", "S&P 500"))
        # Directly inject a new instrument into the store bypassing memory
        _isolated_plugin_store.upsert_instrument(
            plugin.slot, PluginInstrument("TSLA", "Tesla")
        )
        plugin.reload_instruments()
        assert "TSLA" in plugin._instruments
        assert "SPY" in plugin._instruments

    def test_slot_isolates_instruments(self, _isolated_plugin_store):
        """Two plugins with different slots store instruments independently."""
        p1 = ConcreteTestPlugin("shared_name")
        p1.slot = "slot_a"
        p2 = ConcreteTestPlugin("shared_name")
        p2.slot = "slot_b"
        p1.add_instrument(PluginInstrument("SPY", "S&P 500"))
        p2.add_instrument(PluginInstrument("QQQ", "Nasdaq"))
        stored_a = _isolated_plugin_store.load_instruments("slot_a")
        stored_b = _isolated_plugin_store.load_instruments("slot_b")
        assert stored_a[0].symbol == "SPY"
        assert stored_b[0].symbol == "QQQ"


# =============================================================================
# config property
# =============================================================================


class TestConfigProperty:
    def test_config_property_returns_dict_when_descriptor_is_dict(self):
        plugin = ConcreteTestPlugin("cfg_plugin")
        plugin.descriptor = {"symbol": "SPY", "threshold": 0.5}
        assert plugin.config == {"symbol": "SPY", "threshold": 0.5}

    def test_config_property_non_dict_string_returns_none(self):
        plugin = ConcreteTestPlugin("str_plugin")
        plugin.descriptor = "some_string_descriptor"
        assert plugin.config is None

    def test_config_property_none_descriptor_returns_none(self):
        plugin = ConcreteTestPlugin("none_plugin")
        plugin.descriptor = None
        assert plugin.config is None

    def test_config_property_list_descriptor_returns_none(self):
        plugin = ConcreteTestPlugin("list_plugin")
        plugin.descriptor = ["a", "b"]
        assert plugin.config is None

    def test_config_property_int_descriptor_returns_none(self):
        plugin = ConcreteTestPlugin("int_plugin")
        plugin.descriptor = 42
        assert plugin.config is None


# =============================================================================
# migration directory guard
# =============================================================================


class TestMigrationDirectoryGuard:
    def test_migration_skipped_when_dir_absent(self, tmp_path):
        """_run_migration_if_needed must not call store when directory doesn't exist."""
        plugin = ConcreteTestPlugin("guard_plugin")
        plugin._base_path = tmp_path / "nonexistent_dir"
        plugin._migration_done = False

        mock_store = Mock()
        plugin._store = mock_store

        plugin._run_migration_if_needed()

        assert plugin._migration_done is True
        mock_store.migrate_from_json.assert_not_called()
        mock_store.migrate_instruments_from_json.assert_not_called()

    def test_migration_runs_when_dir_exists(self, tmp_path):
        """_run_migration_if_needed must call store methods when directory exists."""
        plugin_dir = tmp_path / "my_plugin"
        plugin_dir.mkdir()

        plugin = ConcreteTestPlugin("dir_plugin")
        plugin._base_path = plugin_dir
        plugin._migration_done = False

        mock_store = Mock()
        plugin._store = mock_store

        plugin._run_migration_if_needed()

        assert plugin._migration_done is True
        mock_store.migrate_from_json.assert_called_once_with("dir_plugin", plugin_dir)
        mock_store.migrate_instruments_from_json.assert_called_once_with(
            "dir_plugin", plugin_dir
        )

    def test_migration_runs_only_once(self, tmp_path):
        """_run_migration_if_needed must be idempotent."""
        plugin_dir = tmp_path / "once_plugin"
        plugin_dir.mkdir()

        plugin = ConcreteTestPlugin("once_plugin")
        plugin._base_path = plugin_dir
        plugin._migration_done = False

        mock_store = Mock()
        plugin._store = mock_store

        plugin._run_migration_if_needed()
        plugin._run_migration_if_needed()

        assert mock_store.migrate_from_json.call_count == 1
        assert mock_store.migrate_instruments_from_json.call_count == 1

    def test_migration_skipped_when_base_path_is_none(self):
        """If _base_path is None, no migration attempted."""
        plugin = ConcreteTestPlugin("null_path_plugin")
        plugin._base_path = None
        plugin._migration_done = False

        mock_store = Mock()
        plugin._store = mock_store

        plugin._run_migration_if_needed()

        assert plugin._migration_done is True
        mock_store.migrate_from_json.assert_not_called()
