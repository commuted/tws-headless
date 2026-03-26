"""
Tests for plugin_executive.py - Plugin lifecycle manager
"""

import asyncio
import pytest
from datetime import datetime
from unittest.mock import Mock

from ib.plugin_executive import (
    PluginExecutive,
    ExecutionMode,
    OrderExecutionMode,
    PluginConfig,
    CircuitBreaker,
    DepartureEntry,
)
from plugins.base import (
    PluginBase,
    PluginState,
    PluginInstrument,
    TradeSignal,
)
from ib.message_bus import MessageBus


class MockPlugin(PluginBase):
    """Mock plugin for testing"""

    def __init__(self, name="mock_plugin", **kwargs):
        super().__init__(name, **kwargs)
        self.start_called = False
        self.stop_called = False
        self.freeze_called = False
        self.resume_called = False
        self._fail_start = False

    @property
    def description(self) -> str:
        return "A mock plugin for testing"

    def start(self) -> bool:
        self.start_called = True
        return not self._fail_start

    def stop(self) -> bool:
        self.stop_called = True
        return True

    def freeze(self) -> bool:
        self.freeze_called = True
        return True

    def resume(self) -> bool:
        self.resume_called = True
        return True

    def handle_request(self, request_type: str, payload: dict) -> dict:
        if request_type == "get_status":
            return {"success": True, "status": "running"}
        return {"success": True, "data": payload}

    def on_unload(self) -> str:
        return f"Goodbye from {self.name}!"

    def calculate_signals(self) -> list:
        return [TradeSignal("SPY", "HOLD", reason="Mock signal")]

    def get_state_for_save(self) -> dict:
        return {"mock_key": "mock_value"}


class TestCircuitBreaker:
    """Tests for CircuitBreaker dataclass"""

    def test_create_circuit_breaker(self):
        """Test creating a circuit breaker"""
        cb = CircuitBreaker()
        assert cb.state == "closed"
        assert cb.consecutive_failures == 0

    def test_record_success(self):
        """Test recording success resets failure count"""
        cb = CircuitBreaker()
        cb.consecutive_failures = 3
        cb.record_success()
        assert cb.consecutive_failures == 0

    def test_record_failure_trips_breaker(self):
        """Test that enough failures trip the breaker"""
        cb = CircuitBreaker(max_failures=3)
        cb.record_failure()
        cb.record_failure()
        cb.record_failure()
        assert cb.state == "open"

    def test_state_check(self):
        """Test state check"""
        cb = CircuitBreaker()
        assert cb.state == "closed"
        cb.state = "open"
        assert cb.state == "open"


class TestPluginExecutiveBasic:
    """Basic PluginExecutive tests"""

    def test_create_executive(self):
        """Test creating PluginExecutive"""
        executive = PluginExecutive(
            portfolio=None,
            data_feed=None,
        )
        assert executive is not None

    def test_create_with_message_bus(self):
        """Test creating with MessageBus"""
        bus = MessageBus()
        executive = PluginExecutive(
            portfolio=None,
            data_feed=None,
            message_bus=bus,
        )
        assert executive.message_bus is bus


class TestPluginRegistration:
    """Tests for plugin registration"""

    def test_register_plugin(self):
        """Test registering a plugin"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin("test")

        result = executive.register_plugin(plugin)

        assert result is True
        assert plugin.state == PluginState.LOADED

    def test_unregister_plugin(self):
        """Test unregistering a plugin"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin()
        executive.register_plugin(plugin)

        result = executive.unregister_plugin("mock_plugin")

        assert result is True



class TestPluginLifecycle:
    """Tests for plugin lifecycle management"""

    def test_start_plugin(self):
        """Test starting a plugin"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin()
        executive.register_plugin(plugin)

        result = executive.start_plugin("mock_plugin")

        assert result is True
        assert plugin.start_called is True
        assert plugin.state == PluginState.STARTED

    def test_stop_plugin(self):
        """Test stopping a plugin"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin()
        executive.register_plugin(plugin)
        executive.start_plugin("mock_plugin")

        result = executive.stop_plugin("mock_plugin")

        assert result is True
        assert plugin.stop_called is True
        assert plugin.state == PluginState.STOPPED

    def test_freeze_plugin(self):
        """Test freezing a plugin"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin()
        executive.register_plugin(plugin)
        executive.start_plugin("mock_plugin")

        result = executive.freeze_plugin("mock_plugin")

        assert result is True
        assert plugin.freeze_called is True
        assert plugin.state == PluginState.FROZEN

    def test_resume_plugin(self):
        """Test resuming a frozen plugin"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin()
        executive.register_plugin(plugin)
        executive.start_plugin("mock_plugin")
        executive.freeze_plugin("mock_plugin")

        result = executive.resume_plugin("mock_plugin")

        assert result is True
        assert plugin.resume_called is True
        assert plugin.state == PluginState.STARTED


class TestPluginRequests:
    """Tests for custom plugin requests"""

    def test_send_request(self):
        """Test sending custom request to plugin"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin()
        executive.register_plugin(plugin)
        executive.start_plugin("mock_plugin")

        response = executive.send_request(
            "mock_plugin",
            "get_status",
            {},
        )

        assert response["success"] is True


class TestPluginStatus:
    """Tests for plugin status reporting"""

    def test_get_plugin_status(self):
        """Test getting plugin status"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin()
        executive.register_plugin(plugin)

        status = executive.get_plugin_status("mock_plugin")

        assert status["name"] == "mock_plugin"
        assert status["state"] == "loaded"


class TestPluginWithMessageBus:
    """Tests for MessageBus integration"""

    def test_plugin_receives_message_bus(self):
        """Test that registered plugins receive MessageBus"""
        bus = MessageBus()
        executive = PluginExecutive(None, None, message_bus=bus)
        plugin = MockPlugin()

        executive.register_plugin(plugin)

        assert plugin._message_bus is bus

    def test_list_feeds(self):
        """Test listing MessageBus feeds"""
        bus = MessageBus()
        executive = PluginExecutive(None, None, message_bus=bus)

        bus.create_channel("signals_channel", "Trading signals")
        bus.create_channel("metrics_channel", "Plugin metrics")

        feeds = executive.list_feeds()

        assert len(feeds) == 2


class TestPluginInstanceId:
    """Tests for plugin instance_id and multiple instances"""

    def test_plugin_has_unique_instance_id(self):
        """Test that each plugin instance gets a unique UUID"""
        p1 = MockPlugin("my_plugin")
        p2 = MockPlugin("my_plugin")
        assert p1.instance_id != p2.instance_id
        assert len(p1.instance_id) == 36  # UUID4 format

    def test_register_same_name_multiple_instances(self):
        """Test registering multiple instances of the same plugin name"""
        executive = PluginExecutive(None, None)
        base_count = len(executive.plugins)  # _unassigned system plugin
        p1 = MockPlugin("same_name")
        p2 = MockPlugin("same_name")

        assert executive.register_plugin(p1) is True
        assert executive.register_plugin(p2) is True

        # Both should be registered (keyed by different instance_ids)
        assert len(executive.plugins) == base_count + 2

    def test_resolve_by_instance_id(self):
        """Test that plugins can be looked up by instance_id"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin("test")
        executive.register_plugin(plugin)

        status = executive.get_plugin_status(plugin.instance_id)

        assert status is not None
        assert status["name"] == "test"
        assert status["instance_id"] == plugin.instance_id

    def test_resolve_by_name(self):
        """Test that plugins can still be looked up by name"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin("test")
        executive.register_plugin(plugin)

        status = executive.get_plugin_status("test")

        assert status is not None
        assert status["name"] == "test"
        assert status["instance_id"] == plugin.instance_id

    def test_lifecycle_by_instance_id(self):
        """Test full lifecycle using instance_id instead of name"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin("test")
        executive.register_plugin(plugin)

        iid = plugin.instance_id
        assert executive.start_plugin(iid) is True
        assert plugin.state == PluginState.STARTED

        assert executive.freeze_plugin(iid) is True
        assert plugin.state == PluginState.FROZEN

        assert executive.resume_plugin(iid) is True
        assert plugin.state == PluginState.STARTED

        assert executive.stop_plugin(iid) is True
        assert plugin.state == PluginState.STOPPED

    def test_unload_specific_instance(self):
        """Test unloading one instance while another with the same name stays"""
        executive = PluginExecutive(None, None)
        base_count = len(executive.plugins)  # _unassigned system plugin
        p1 = MockPlugin("same_name")
        p2 = MockPlugin("same_name")
        executive.register_plugin(p1)
        executive.register_plugin(p2)

        # Unload first instance by instance_id
        assert executive.unload_plugin(p1.instance_id) is True

        # Second instance should still be there
        assert len(executive.plugins) == base_count + 1
        status = executive.get_plugin_status(p2.instance_id)
        assert status is not None
        assert status["name"] == "same_name"

    def test_send_request_by_instance_id(self):
        """Test sending requests using instance_id"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin("test")
        executive.register_plugin(plugin)
        executive.start_plugin("test")

        response = executive.send_request(
            plugin.instance_id,
            "get_status",
            {},
        )
        assert response["success"] is True

    def test_enable_disable_by_instance_id(self):
        """Test enable/disable using instance_id"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin("test")
        executive.register_plugin(plugin)

        assert executive.enable_plugin(plugin.instance_id, False) is True

        status = executive.get_plugin_status(plugin.instance_id)
        assert status["enabled"] is False

    def test_instance_id_in_status(self):
        """Test that instance_id appears in plugin status"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin("test")
        executive.register_plugin(plugin)

        status = executive.get_plugin_status("test")
        assert "instance_id" in status
        assert status["instance_id"] == plugin.instance_id

    def test_instance_id_in_overall_status(self):
        """Test that instance_id appears in overall executive status"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin("test")
        executive.register_plugin(plugin)

        status = executive.get_status()
        assert "test" in status["plugins"]
        assert "instance_id" in status["plugins"]["test"]


class TestPluginDescriptor:
    """Tests for plugin descriptor passthrough"""

    def test_plugin_descriptor_default_none(self):
        """Test that descriptor defaults to None"""
        plugin = MockPlugin("test")
        assert plugin.descriptor is None

    def test_set_descriptor(self):
        """Test setting descriptor on a plugin"""
        plugin = MockPlugin("test")
        plugin.descriptor = {"mode": "backtest", "params": [1, 2, 3]}
        assert plugin.descriptor["mode"] == "backtest"

    def test_descriptor_in_get_status(self):
        """Test that descriptor appears in plugin's get_status()"""
        plugin = MockPlugin("test")
        plugin.descriptor = "run_quick_test"
        plugin.load()

        status = plugin.get_status()
        assert status["descriptor"] == "run_quick_test"

    def test_descriptor_opaque_types(self):
        """Test that descriptor accepts any type (opaque)"""
        plugin = MockPlugin("test")

        # String
        plugin.descriptor = "simple_string"
        assert plugin.descriptor == "simple_string"

        # Dict
        plugin.descriptor = {"key": "value"}
        assert plugin.descriptor == {"key": "value"}

        # List
        plugin.descriptor = [1, 2, 3]
        assert plugin.descriptor == [1, 2, 3]

        # Nested
        plugin.descriptor = {"symbols": ["AAPL", "GOOG"], "config": {"fast": True}}
        assert plugin.descriptor["symbols"] == ["AAPL", "GOOG"]

    def test_executive_set_descriptor_before_register(self):
        """Test setting descriptor on plugin and registering with executive"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin("test")
        plugin.descriptor = {"test_id": 42}

        executive.register_plugin(plugin)

        status = executive.get_plugin_status("test")
        assert status is not None

        # Plugin retains its descriptor
        assert plugin.descriptor == {"test_id": 42}

    def test_multiple_instances_different_descriptors(self):
        """Test multiple instances of same plugin with different descriptors"""
        executive = PluginExecutive(None, None)

        p1 = MockPlugin("strategy")
        p1.descriptor = {"symbol": "AAPL", "period": 20}

        p2 = MockPlugin("strategy")
        p2.descriptor = {"symbol": "GOOG", "period": 50}

        executive.register_plugin(p1)
        executive.register_plugin(p2)

        assert p1.descriptor["symbol"] == "AAPL"
        assert p2.descriptor["symbol"] == "GOOG"
        assert p1.instance_id != p2.instance_id


class TestPluginRequestUnload:
    """Tests for plugin self-unload via instance_id"""

    async def test_request_unload_uses_instance_id(self):
        """Test that request_unload passes instance_id, not name"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin("test")
        executive.register_plugin(plugin)

        # Mock deferred_unload_plugin to capture what's passed
        called_with = []
        original = executive.deferred_unload_plugin
        def capture(name_or_id):
            called_with.append(name_or_id)
            original(name_or_id)
        executive.deferred_unload_plugin = capture

        plugin.request_unload()
        # Allow the deferred task to run
        await asyncio.sleep(0)

        assert len(called_with) == 1
        assert called_with[0] == plugin.instance_id


class TestPluginDepartures:
    """Tests for the plugin departure status board"""

    def test_unload_records_departure(self):
        """Unloading a plugin creates a departure entry"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin("depart_test")
        executive.register_plugin(plugin)

        executive.unload_plugin("depart_test")

        departures = executive.get_departures()
        assert len(departures) == 1
        key = list(departures.keys())[0]
        assert key.startswith("depart_test:")
        entry = departures[key]
        assert entry["plugin_name"] == "depart_test"
        assert entry["instance_id"] == plugin.instance_id
        assert isinstance(entry["unloaded_at"], float)

    def test_departure_message_from_on_unload(self):
        """Departure message comes from on_unload() return value"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin("msg_test")
        executive.register_plugin(plugin)

        executive.unload_plugin("msg_test")

        departures = executive.get_departures()
        key = list(departures.keys())[0]
        assert departures[key]["message"] == "Goodbye from msg_test!"

    def test_default_on_unload_message(self):
        """Plugin without on_unload override gets default message"""

        class BarePlugin(PluginBase):
            @property
            def description(self):
                return "bare"

            def start(self):
                return True

            def stop(self):
                return True

            def freeze(self):
                return True

            def resume(self):
                return True

            def handle_request(self, request_type, payload):
                return {"success": True}

            def calculate_signals(self):
                return []

        executive = PluginExecutive(None, None)
        plugin = BarePlugin("bare_plugin")
        executive.register_plugin(plugin)

        executive.unload_plugin("bare_plugin")

        departures = executive.get_departures()
        key = list(departures.keys())[0]
        assert departures[key]["message"] == "Plugin 'bare_plugin' unloaded"

    def test_on_unload_exception_captured(self):
        """If on_unload() raises, departure message contains the error"""

        class FailUnloadPlugin(MockPlugin):
            def on_unload(self):
                raise RuntimeError("kaboom")

        executive = PluginExecutive(None, None)
        plugin = FailUnloadPlugin("fail_unload")
        executive.register_plugin(plugin)

        executive.unload_plugin("fail_unload")

        departures = executive.get_departures()
        key = list(departures.keys())[0]
        assert "on_unload() failed" in departures[key]["message"]
        assert "kaboom" in departures[key]["message"]

    def test_get_departures_with_clear(self):
        """get_departures(clear=True) returns entries then clears"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin("clear_test")
        executive.register_plugin(plugin)

        executive.unload_plugin("clear_test")

        departures = executive.get_departures(clear=True)
        assert len(departures) == 1

        # Board should now be empty
        assert len(executive.get_departures()) == 0

    def test_clear_departures(self):
        """clear_departures() empties board, returns count"""
        executive = PluginExecutive(None, None)
        for i in range(3):
            p = MockPlugin(f"plugin_{i}")
            executive.register_plugin(p)
            executive.unload_plugin(f"plugin_{i}")

        count = executive.clear_departures()
        assert count == 3
        assert len(executive.get_departures()) == 0

    def test_departure_cap_evicts_oldest(self):
        """Exceeding max_departures evicts oldest entries"""
        executive = PluginExecutive(None, None)
        executive._max_departures = 5  # Lower cap for testing

        for i in range(7):
            p = MockPlugin(f"cap_{i}")
            executive.register_plugin(p)
            executive.unload_plugin(f"cap_{i}")

        departures = executive.get_departures()
        assert len(departures) == 5
        # Oldest two (cap_0, cap_1) should have been evicted
        keys = list(departures.keys())
        assert all(k.startswith("cap_") for k in keys)
        names = [departures[k]["plugin_name"] for k in keys]
        assert "cap_0" not in names
        assert "cap_1" not in names
        assert "cap_6" in names

    def test_self_unload_records_departure(self):
        """Plugin calling request_unload() gets departure recorded"""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin("self_unload")
        executive.register_plugin(plugin)

        # Use direct unload (deferred_unload uses a thread which is harder to test)
        executive.unload_plugin(plugin.instance_id)

        departures = executive.get_departures()
        assert len(departures) == 1
        key = list(departures.keys())[0]
        assert departures[key]["plugin_name"] == "self_unload"
        assert departures[key]["message"] == "Goodbye from self_unload!"


class TestCommissionAndPnL:
    """Tests for execDetails wiring and P&L dispatch"""

    def test_exec_details_wires_commission(self):
        """_handle_exec_details_for_commission populates _exec_id_to_order."""
        executive = PluginExecutive(None, None)

        mock_contract = Mock()
        mock_execution = Mock()
        mock_execution.orderId = 101
        mock_execution.execId = "exec_abc"

        executive._handle_exec_details_for_commission(0, mock_contract, mock_execution)

        assert executive._exec_id_to_order.get("exec_abc") == 101

    def test_exec_details_callback_registered_on_portfolio(self):
        """portfolio._callbacks['execDetails'] is set to the handler at init."""
        portfolio = Mock()
        portfolio._callbacks = {}
        portfolio._on_commission = None
        portfolio._on_pnl = None

        ex = PluginExecutive(portfolio=portfolio, data_feed=None)

        assert "execDetails" in portfolio._callbacks
        assert portfolio._callbacks["execDetails"] == ex._handle_exec_details_for_commission

    def test_pnl_dispatch_to_started_plugins(self):
        """_dispatch_pnl calls on_pnl on every started plugin."""
        from ib.models import PnLData

        executive = PluginExecutive(None, None)

        received = []

        class PnLPlugin(MockPlugin):
            def on_pnl(self, pnl_data):
                received.append(pnl_data)

        plugin = PnLPlugin("pnl_plugin")
        executive.register_plugin(plugin)
        executive.start_plugin("pnl_plugin")

        pnl_data = PnLData(
            account="DU123456",
            daily_pnl=50.0,
            unrealized_pnl=200.0,
            realized_pnl=10.0,
        )
        executive._dispatch_pnl(pnl_data)

        assert len(received) == 1
        assert received[0].daily_pnl == 50.0

    def test_pnl_dispatch_skips_non_started_plugins(self):
        """_dispatch_pnl does not call on_pnl on stopped or frozen plugins."""
        from ib.models import PnLData

        executive = PluginExecutive(None, None)

        received = []

        class PnLPlugin(MockPlugin):
            def on_pnl(self, pnl_data):
                received.append(pnl_data)

        plugin = PnLPlugin("pnl_plugin2")
        executive.register_plugin(plugin)
        # Plugin is in LOADED state, not STARTED

        pnl_data = PnLData(
            account="DU123456",
            daily_pnl=10.0,
            unrealized_pnl=0.0,
            realized_pnl=0.0,
        )
        executive._dispatch_pnl(pnl_data)

        assert received == []

    def test_on_pnl_registered_on_portfolio(self):
        """portfolio._on_pnl is set to _dispatch_pnl at init."""
        portfolio = Mock()
        portfolio._callbacks = {}
        portfolio._on_commission = None
        portfolio._on_pnl = None

        ex = PluginExecutive(portfolio=portfolio, data_feed=None)

        assert portfolio._on_pnl == ex._dispatch_pnl

    def test_pnl_dispatch_exception_in_one_plugin_does_not_stop_others(self):
        """An exception in one plugin's on_pnl must not prevent other plugins receiving it."""
        from ib.models import PnLData

        executive = PluginExecutive(None, None)
        received = []

        class BrokenPlugin(MockPlugin):
            def on_pnl(self, pnl_data):
                raise RuntimeError("plugin bug")

        class GoodPlugin(MockPlugin):
            def on_pnl(self, pnl_data):
                received.append(pnl_data)

        broken = BrokenPlugin("broken_plugin")
        good = GoodPlugin("good_plugin")
        executive.register_plugin(broken)
        executive.register_plugin(good)
        executive.start_plugin("broken_plugin")
        executive.start_plugin("good_plugin")

        pnl_data = PnLData(account="DU1", daily_pnl=1.0, unrealized_pnl=0.0, realized_pnl=0.0)
        # Should not raise
        executive._dispatch_pnl(pnl_data)

        assert len(received) == 1
        assert received[0] is pnl_data

    def test_commission_currency_reaches_plugin(self):
        """on_commission is called with all 4 args including currency."""
        executive = PluginExecutive(None, None)
        received = []

        class CommPlugin(MockPlugin):
            def on_commission(self, exec_id, commission, realized_pnl, currency):
                received.append((exec_id, commission, realized_pnl, currency))

        plugin = CommPlugin("comm_plugin")
        executive.register_plugin(plugin)
        executive.start_plugin("comm_plugin")

        # Build the minimal _pending_commissions entry
        order_id = 1001
        signal_stub = Mock()
        signal_stub.algorithm_name = "comm_plugin"
        executive._pending_commissions[order_id] = {
            "symbol": "SPY",
            "action": "BUY",
            "net_quantity": 100,
            "contributing_signals": [signal_stub],
        }
        executive._exec_id_to_order["exec_abc"] = order_id

        # Mock reconciler and log writer so they don't fail
        executive._reconciler = Mock()
        executive._reconciler.get_allocation_percentages.return_value = {"comm_plugin": 1.0}
        executive._execution_log_writer = Mock()
        executive._execution_log_writer.write.return_value = True

        executive._handle_commission_report("exec_abc", 2.50, 100.0, "USD")

        assert len(received) == 1
        assert received[0] == ("exec_abc", 2.50, 100.0, "USD")


class TestPluginAbandon:
    """Tests for the compulsory abandon command."""

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _make_executive_with_plugin(self, name="abandon_test"):
        executive = PluginExecutive(None, None)
        plugin = MockPlugin(name)
        executive.register_plugin(plugin)
        executive.start_plugin(name)
        return executive, plugin

    def _make_mock_portfolio(self, orders: dict):
        """Return a mock portfolio whose get_order() returns OrderRecord stubs.

        orders: {order_id: is_complete}  — True means already terminal.
        """
        from ib.models import OrderRecord, OrderStatus

        portfolio = Mock()
        portfolio.connected = True

        def get_order(oid):
            if oid not in orders:
                return None
            rec = Mock(spec=OrderRecord)
            rec.is_complete = orders[oid]
            return rec

        portfolio.get_order = get_order
        portfolio.cancel_order = Mock(return_value=True)
        return portfolio

    # ------------------------------------------------------------------
    # Unknown plugin
    # ------------------------------------------------------------------

    def test_abandon_unknown_plugin_returns_failure(self):
        executive = PluginExecutive(None, None)
        result = executive.abandon_plugin("no_such_plugin")
        assert result["success"] is False
        assert "not found" in result["message"]

    # ------------------------------------------------------------------
    # Order cancellation
    # ------------------------------------------------------------------

    def test_abandon_cancels_non_terminal_orders(self):
        executive, plugin = self._make_executive_with_plugin()
        # Both orders non-terminal throughout — cancel_order must be called for each
        portfolio = self._make_mock_portfolio({10: False, 11: False})
        plugin.portfolio = portfolio

        executive.register_order_for_plugin(10, plugin.name)
        executive.register_order_for_plugin(11, plugin.name)

        # Short timeout so the poll loop exits quickly
        result = executive.abandon_plugin(plugin.name, timeout=0.1)

        assert result["success"] is True
        assert portfolio.cancel_order.call_count == 2
        called_ids = {c.args[0] for c in portfolio.cancel_order.call_args_list}
        assert called_ids == {10, 11}

    def test_abandon_skips_already_terminal_orders(self):
        executive, plugin = self._make_executive_with_plugin()
        # Order 10 already terminal, order 11 non-terminal
        portfolio = self._make_mock_portfolio({10: True, 11: False})
        plugin.portfolio = portfolio

        executive.register_order_for_plugin(10, plugin.name)
        executive.register_order_for_plugin(11, plugin.name)

        result = executive.abandon_plugin(plugin.name, timeout=0.1)

        assert result["success"] is True
        assert result["orders_already_terminal"] == [10]
        assert 11 in result["orders_cancelled"]
        called_ids = {c.args[0] for c in portfolio.cancel_order.call_args_list}
        assert 10 not in called_ids
        assert 11 in called_ids

    def test_abandon_no_portfolio_skips_order_steps(self):
        executive, plugin = self._make_executive_with_plugin()
        plugin.portfolio = None

        executive.register_order_for_plugin(10, plugin.name)

        result = executive.abandon_plugin(plugin.name, timeout=1.0)

        assert result["success"] is True
        # No portfolio → entire order section is skipped
        assert result["orders_cancelled"] == []
        assert result["orders_already_terminal"] == []

    def test_abandon_no_orders_registered(self):
        executive, plugin = self._make_executive_with_plugin()
        plugin.portfolio = Mock()

        result = executive.abandon_plugin(plugin.name, timeout=1.0)

        assert result["success"] is True
        assert result["orders_cancelled"] == []
        assert result["orders_already_terminal"] == []

    def test_abandon_reports_unacknowledged_orders(self):
        """Orders that never go terminal within timeout appear in unacknowledged."""
        executive, plugin = self._make_executive_with_plugin()

        portfolio = Mock()
        portfolio.connected = True
        portfolio.cancel_order = Mock(return_value=True)

        from ib.models import OrderRecord
        rec = Mock(spec=OrderRecord)
        rec.is_complete = False          # never becomes terminal
        portfolio.get_order = Mock(return_value=rec)

        plugin.portfolio = portfolio
        executive.register_order_for_plugin(42, plugin.name)

        result = executive.abandon_plugin(plugin.name, timeout=0.1)

        assert result["success"] is True
        assert 42 in result["orders_unacknowledged"]

    # ------------------------------------------------------------------
    # Holdings release
    # ------------------------------------------------------------------

    def test_abandon_clears_holdings(self):
        from plugins.base import Holdings, HoldingPosition
        executive, plugin = self._make_executive_with_plugin()
        plugin.portfolio = None

        plugin._holdings = Holdings(plugin_name=plugin.name, current_cash=5000.0)
        plugin._holdings.current_positions = [
            HoldingPosition("SPY", 10, cost_basis=500.0),
            HoldingPosition("QQQ", 5, cost_basis=400.0),
        ]
        plugin.save_holdings = Mock()

        result = executive.abandon_plugin(plugin.name, timeout=1.0)

        assert result["success"] is True
        assert set(result["positions_released"]) == {"SPY", "QQQ"}
        assert result["cash_released"] == 5000.0
        assert plugin._holdings.current_positions == []
        assert plugin._holdings.current_cash == 0.0
        plugin.save_holdings.assert_called_once()

    def test_abandon_no_holdings_does_not_crash(self):
        executive, plugin = self._make_executive_with_plugin()
        plugin.portfolio = None
        plugin._holdings = None

        result = executive.abandon_plugin(plugin.name, timeout=1.0)

        assert result["success"] is True
        assert result["positions_released"] == []
        assert result["cash_released"] == 0.0

    # ------------------------------------------------------------------
    # send_request intercept
    # ------------------------------------------------------------------

    def test_abandon_intercepted_by_send_request(self):
        """send_request('abandon') is handled at the executive level,
        not delegated to plugin.handle_request."""
        executive, plugin = self._make_executive_with_plugin()
        plugin.portfolio = None
        plugin.handle_request = Mock(side_effect=AssertionError("should not be called"))

        result = executive.send_request(plugin.name, "abandon", {})

        assert result["success"] is True
        plugin.handle_request.assert_not_called()

    def test_abandon_timeout_param_passed_through(self):
        """send_request passes 'timeout' payload key to abandon_plugin."""
        executive, plugin = self._make_executive_with_plugin()
        plugin.portfolio = None
        executive.abandon_plugin = Mock(return_value={"success": True, "message": ""})

        executive.send_request(plugin.name, "abandon", {"timeout": 99.0})

        executive.abandon_plugin.assert_called_once_with(plugin.name, timeout=99.0)

    # ------------------------------------------------------------------
    # Streams and unload
    # ------------------------------------------------------------------

    def test_abandon_cancels_streams(self):
        executive, plugin = self._make_executive_with_plugin()
        plugin.portfolio = None
        executive.stream_manager.cancel_all_streams = Mock()
        # Mock deferred_unload so it doesn't trigger a second cancel_all_streams call
        executive.deferred_unload_plugin = Mock()

        executive.abandon_plugin(plugin.name, timeout=1.0)

        executive.stream_manager.cancel_all_streams.assert_called_once_with(plugin.name)

    def test_abandon_triggers_unload(self):
        executive, plugin = self._make_executive_with_plugin()
        plugin.portfolio = None
        executive.deferred_unload_plugin = Mock()

        executive.abandon_plugin(plugin.name, timeout=1.0)

        executive.deferred_unload_plugin.assert_called_once_with(plugin.instance_id)


# =============================================================================
# Registry status tracking tests
# =============================================================================


class TestPluginRegistryTracking:
    """Lifecycle transitions write the correct status to plugin_registry."""

    def _make_tracked(self, tmp_path, slot="reg_plugin"):
        """Return (executive, plugin, store) wired to a temp DB."""
        from unittest.mock import patch
        from pathlib import Path
        from ib.plugin_store import PluginStore

        store = PluginStore(db_path=tmp_path / "track.db")

        with patch("ib.plugin_executive.get_plugin_store", return_value=store):
            executive = PluginExecutive(None, None)
            plugin = MockPlugin(slot)
            executive.register_plugin(plugin)
            executive._plugins[plugin.instance_id].source_file = Path("/fake/plugin.py")
            # Simulate what load_plugin_from_file does after register
            executive._update_registry_status(plugin, "unloaded")

        return executive, plugin, store

    def test_load_writes_registry_unloaded(self, tmp_path):
        from unittest.mock import patch
        from pathlib import Path
        from ib.plugin_store import PluginStore

        store = PluginStore(db_path=tmp_path / "t.db")
        with patch("ib.plugin_executive.get_plugin_store", return_value=store):
            executive = PluginExecutive(None, None)
            plugin = MockPlugin("load_test")
            executive.register_plugin(plugin)
            executive._plugins[plugin.instance_id].source_file = Path("/fake/plugin.py")
            executive._update_registry_status(plugin, "unloaded")

        entry = store.get_registry_entry("load_test")
        assert entry is not None
        assert entry["status"] == "unloaded"
        assert entry["class_path"] == "/fake/plugin.py"

    def test_start_writes_registry_started(self, tmp_path):
        from unittest.mock import patch
        from pathlib import Path
        from ib.plugin_store import PluginStore

        store = PluginStore(db_path=tmp_path / "t.db")
        with patch("ib.plugin_executive.get_plugin_store", return_value=store):
            executive = PluginExecutive(None, None)
            plugin = MockPlugin("start_test")
            executive.register_plugin(plugin)
            executive._plugins[plugin.instance_id].source_file = Path("/fake/plugin.py")
            executive._update_registry_status(plugin, "unloaded")

            executive.start_plugin("start_test")

        entry = store.get_registry_entry("start_test")
        assert entry["status"] == "started"

    def test_freeze_writes_registry_frozen(self, tmp_path):
        from unittest.mock import patch
        from pathlib import Path
        from ib.plugin_store import PluginStore

        store = PluginStore(db_path=tmp_path / "t.db")
        with patch("ib.plugin_executive.get_plugin_store", return_value=store):
            executive = PluginExecutive(None, None)
            plugin = MockPlugin("freeze_test")
            executive.register_plugin(plugin)
            executive._plugins[plugin.instance_id].source_file = Path("/fake/plugin.py")
            executive._update_registry_status(plugin, "unloaded")
            executive.start_plugin("freeze_test")

            executive.freeze_plugin("freeze_test")

        entry = store.get_registry_entry("freeze_test")
        assert entry["status"] == "frozen"

    def test_stop_writes_registry_unloaded(self, tmp_path):
        from unittest.mock import patch
        from pathlib import Path
        from ib.plugin_store import PluginStore

        store = PluginStore(db_path=tmp_path / "t.db")
        with patch("ib.plugin_executive.get_plugin_store", return_value=store):
            executive = PluginExecutive(None, None)
            plugin = MockPlugin("stop_test")
            executive.register_plugin(plugin)
            executive._plugins[plugin.instance_id].source_file = Path("/fake/plugin.py")
            executive._update_registry_status(plugin, "unloaded")
            executive.start_plugin("stop_test")

            executive.stop_plugin("stop_test")

        entry = store.get_registry_entry("stop_test")
        assert entry["status"] == "unloaded"

    def test_reload_auto_starts_started_slots(self, tmp_path):
        from unittest.mock import patch
        from ib.plugin_store import PluginStore

        store = PluginStore(db_path=tmp_path / "t.db")
        fake_file = tmp_path / "myplugin.py"
        fake_file.write_text("# placeholder")
        store.upsert_registry("my_slot", str(fake_file), "1.0", "started")

        with patch("ib.plugin_executive.get_plugin_store", return_value=store):
            executive = PluginExecutive(None, None)
            executive.load_plugin_from_file = Mock(return_value={
                "plugin_name": "my_slot", "slot": "my_slot",
                "instance_id": "abc123", "descriptor": None,
            })
            executive.start_plugin = Mock(return_value=True)

            result = executive.reload_registered_plugins()

            executive.load_plugin_from_file.assert_called_once_with(
                str(fake_file), slot="my_slot", descriptor=None,
            )
            executive.start_plugin.assert_called_once_with("my_slot")
            assert "my_slot" in result["reloaded"]

    def test_reload_skips_missing_file(self, tmp_path):
        from unittest.mock import patch
        from ib.plugin_store import PluginStore

        store = PluginStore(db_path=tmp_path / "t.db")
        store.upsert_registry("dead_slot", "/nonexistent/plugin.py", "1.0", "started")

        with patch("ib.plugin_executive.get_plugin_store", return_value=store):
            executive = PluginExecutive(None, None)
            result = executive.reload_registered_plugins()

        assert "dead_slot" in result["skipped"]
        assert "dead_slot" not in result["reloaded"]

    def test_reload_skips_already_loaded_slot(self, tmp_path):
        from unittest.mock import patch
        from ib.plugin_store import PluginStore

        store = PluginStore(db_path=tmp_path / "t.db")
        fake_file = tmp_path / "plugin.py"
        fake_file.write_text("# placeholder")
        store.upsert_registry("already_loaded", str(fake_file), "1.0", "started")

        with patch("ib.plugin_executive.get_plugin_store", return_value=store):
            executive = PluginExecutive(None, None)
            plugin = MockPlugin("already_loaded")
            executive.register_plugin(plugin)

            result = executive.reload_registered_plugins()

        assert "already_loaded" in result["skipped"]
        assert "already_loaded" not in result["reloaded"]


class TestExportImport:
    """Tests for export_plugin / import_plugin."""

    def _make_executive_with_plugin(self, tmp_path):
        """Return (executive, plugin) with instruments and holdings loaded."""
        executive = PluginExecutive(None, None)
        plugin = MockPlugin("export_test")
        plugin._base_path = tmp_path / "export_test"
        plugin._base_path.mkdir(parents=True)
        plugin._instruments_file = plugin._base_path / "instruments.json"
        plugin._holdings_file   = plugin._base_path / "holdings.json"
        plugin._state_file      = plugin._base_path / "state.json"

        inst = PluginInstrument("SPY", "S&P 500 ETF", weight=60.0, min_weight=10.0, max_weight=100.0)
        plugin._instruments["SPY"] = inst

        executive.register_plugin(plugin)

        # Set holdings AFTER load() so they aren't reset by load()
        from plugins.base import Holdings, HoldingPosition
        plugin._holdings = Holdings(
            plugin_name="export_test",
            initial_cash=50000.0,
            current_cash=30000.0,
            current_positions=[
                HoldingPosition(symbol="SPY", quantity=50.0, cost_basis=450.0)
            ],
        )

        cfg = executive._plugins[plugin.instance_id]
        cfg.source_file = tmp_path / "export_test"  # stand-in for class path
        return executive, plugin

    def test_export_returns_dict(self, tmp_path):
        executive, plugin = self._make_executive_with_plugin(tmp_path)
        data = executive.export_plugin("export_test")

        assert data is not None
        assert data["slot"] == "export_test"
        assert data["export_version"] == "1.0"
        assert len(data["instruments"]) == 1
        assert data["instruments"][0]["symbol"] == "SPY"
        assert data["holdings"]["current_holdings"]["cash"] == 30000.0

    def test_export_unknown_plugin_returns_none(self, tmp_path):
        executive = PluginExecutive(None, None)
        assert executive.export_plugin("no_such_plugin") is None

    def test_export_contains_state(self, tmp_path):
        executive, plugin = self._make_executive_with_plugin(tmp_path)
        data = executive.export_plugin("export_test")

        assert "state" in data
        assert "exported_at" in data
        assert "plugin_version" in data

    def _write_minimal_plugin(self, path: "Path") -> "Path":
        """Write a self-contained plugin file loadable without package context."""
        plugin_file = path / "minimal_plugin.py"
        plugin_file.write_text(
            "from plugins.base import PluginBase\n\n"
            "class MinimalImportPlugin(PluginBase):\n"
            "    description = 'Minimal plugin for import tests'\n"
            "    def __init__(self, name='minimal_import', **kw): super().__init__(name, **kw)\n"
            "    def start(self): return True\n"
            "    def stop(self): return True\n"
            "    def freeze(self): return True\n"
            "    def resume(self): return True\n"
            "    def calculate_signals(self): return []\n"
            "    def handle_request(self, rtype, payload): return {'success': True}\n"
            "    def get_state_for_save(self): return {}\n"
        )
        return plugin_file

    def test_import_restores_instruments(self, tmp_path):
        executive, plugin = self._make_executive_with_plugin(tmp_path)
        data = executive.export_plugin("export_test")

        # New executive simulates a fresh engine start
        executive2 = PluginExecutive(None, None)

        plugin_file = self._write_minimal_plugin(tmp_path)
        data["class_path"] = str(plugin_file)
        data["slot"] = "import_test"

        result = executive2.import_plugin(data)

        assert result is not None
        assert result["slot"] == "import_test"

        instruments = executive2.get_plugin_instruments("import_test")
        assert instruments is not None
        assert any(i.symbol == "SPY" for i in instruments)

    def test_import_missing_class_path_fails(self, tmp_path):
        executive2 = PluginExecutive(None, None)
        data = {
            "export_version": "1.0",
            "class_path": str(tmp_path / "nonexistent.py"),
            "slot": "bad_import",
            "account_id": "",
            "instruments": [],
            "holdings": None,
            "state": {},
        }
        result = executive2.import_plugin(data)
        assert result is None

    def test_export_import_roundtrip_slot_override(self, tmp_path):
        """Slot override lets the same export create independent instances."""
        executive, plugin = self._make_executive_with_plugin(tmp_path)
        data = executive.export_plugin("export_test")

        plugin_file = self._write_minimal_plugin(tmp_path)
        data["class_path"] = str(plugin_file)

        result = executive.import_plugin(data, slot_override="cloned_slot")

        assert result is not None
        assert result["slot"] == "cloned_slot"
        # Both instances exist independently
        assert executive.get_plugin_instruments("export_test") is not None
        assert executive.get_plugin_instruments("cloned_slot") is not None
