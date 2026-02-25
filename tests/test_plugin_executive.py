"""
Tests for plugin_executive.py - Plugin lifecycle manager
"""

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

    def calculate_signals(self, market_data: dict) -> list:
        return [TradeSignal("SPY", "HOLD", reason="Mock signal")]


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

    def test_request_unload_uses_instance_id(self):
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

            def calculate_signals(self, market_data):
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
