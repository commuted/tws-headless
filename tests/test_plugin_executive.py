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
)
from ib.plugins.base import (
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
