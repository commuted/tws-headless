"""
Tests for connection_manager.py - Robust connection management
"""

import pytest
import time
import threading
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch, PropertyMock

from connection_manager import (
    ConnectionManager,
    ConnectionConfig,
    ConnectionState,
    StreamSubscription,
)


def create_mock_portfolio():
    """Create a mock Portfolio for testing"""
    portfolio = Mock()
    portfolio.host = "127.0.0.1"
    portfolio.port = 7497
    portfolio.connected = False
    portfolio._callbacks = {}
    portfolio._stream_subscriptions = {}
    portfolio._stream_req_ids = {}
    portfolio._bar_subscriptions = {}
    portfolio._bar_req_ids = {}

    def register_callback(name, callback):
        portfolio._callbacks[name] = callback

    portfolio.register_callback = register_callback
    portfolio.connect = Mock(return_value=True)
    portfolio.disconnect = Mock()
    portfolio.shutdown = Mock()
    portfolio.reqCurrentTime = Mock()
    portfolio.get_position = Mock(return_value=None)
    portfolio.stream_symbol = Mock(return_value=True)
    portfolio.bar_stream_symbol = Mock(return_value=True)

    return portfolio


class TestConnectionConfig:
    """Tests for ConnectionConfig dataclass"""

    def test_default_values(self):
        """Test default configuration values"""
        config = ConnectionConfig()

        assert config.auto_reconnect is True
        assert config.reconnect_delay_initial == 1.0
        assert config.reconnect_delay_max == 60.0
        assert config.reconnect_delay_multiplier == 2.0
        assert config.max_reconnect_attempts == 0
        assert config.keepalive_enabled is True
        assert config.keepalive_interval == 30.0
        assert config.keepalive_timeout == 10.0
        assert config.health_check_interval == 5.0
        assert config.connection_timeout == 10.0

    def test_custom_values(self):
        """Test custom configuration values"""
        config = ConnectionConfig(
            auto_reconnect=False,
            reconnect_delay_initial=2.0,
            keepalive_interval=60.0,
            max_reconnect_attempts=5,
        )

        assert config.auto_reconnect is False
        assert config.reconnect_delay_initial == 2.0
        assert config.keepalive_interval == 60.0
        assert config.max_reconnect_attempts == 5


class TestStreamSubscription:
    """Tests for StreamSubscription dataclass"""

    def test_default_values(self):
        """Test default stream subscription values"""
        from ibapi.contract import Contract
        contract = Contract()
        contract.symbol = "SPY"

        sub = StreamSubscription(symbol="SPY", contract=contract)

        assert sub.symbol == "SPY"
        assert sub.req_id == 0
        assert sub.stream_type == "tick"
        assert sub.what_to_show == "TRADES"
        assert sub.use_rth is True
        assert sub.created_at is not None

    def test_custom_values(self):
        """Test custom stream subscription values"""
        from ibapi.contract import Contract
        contract = Contract()

        sub = StreamSubscription(
            symbol="AAPL",
            contract=contract,
            req_id=123,
            stream_type="bar",
            what_to_show="MIDPOINT",
            use_rth=False,
        )

        assert sub.symbol == "AAPL"
        assert sub.req_id == 123
        assert sub.stream_type == "bar"
        assert sub.what_to_show == "MIDPOINT"
        assert sub.use_rth is False


class TestConnectionState:
    """Tests for ConnectionState enum"""

    def test_state_values(self):
        """Test connection state values"""
        assert ConnectionState.DISCONNECTED.value == "disconnected"
        assert ConnectionState.CONNECTING.value == "connecting"
        assert ConnectionState.CONNECTED.value == "connected"
        assert ConnectionState.RECONNECTING.value == "reconnecting"
        assert ConnectionState.SHUTTING_DOWN.value == "shutting_down"


class TestConnectionManagerInit:
    """Tests for ConnectionManager initialization"""

    def test_default_initialization(self):
        """Test default initialization"""
        portfolio = create_mock_portfolio()
        manager = ConnectionManager(portfolio)

        assert manager.portfolio is portfolio
        assert manager.config is not None
        assert manager.state == ConnectionState.DISCONNECTED
        assert manager.is_connected is False
        assert manager.reconnect_attempts == 0

    def test_custom_config(self):
        """Test initialization with custom config"""
        portfolio = create_mock_portfolio()
        config = ConnectionConfig(keepalive_interval=60.0)
        manager = ConnectionManager(portfolio, config)

        assert manager.config.keepalive_interval == 60.0

    def test_callbacks_initialized_none(self):
        """Test that callbacks are initialized to None"""
        portfolio = create_mock_portfolio()
        manager = ConnectionManager(portfolio)

        assert manager.on_connected is None
        assert manager.on_disconnected is None
        assert manager.on_reconnecting is None
        assert manager.on_error is None


class TestConnectionManagerStart:
    """Tests for starting connection manager"""

    def test_start_success(self):
        """Test successful start"""
        portfolio = create_mock_portfolio()
        portfolio.connect.return_value = True
        portfolio.connected = True

        config = ConnectionConfig(keepalive_enabled=False)
        manager = ConnectionManager(portfolio, config)

        result = manager.start()

        assert result is True
        assert portfolio.connect.called

    def test_start_when_not_disconnected(self):
        """Test start fails when not in disconnected state"""
        portfolio = create_mock_portfolio()
        manager = ConnectionManager(portfolio)
        manager._state = ConnectionState.CONNECTED

        result = manager.start()

        assert result is False

    def test_start_connection_failed_with_auto_reconnect(self):
        """Test start with failed connection and auto-reconnect enabled"""
        portfolio = create_mock_portfolio()
        portfolio.connect.return_value = False

        config = ConnectionConfig(auto_reconnect=True)
        manager = ConnectionManager(portfolio, config)

        result = manager.start()

        # Should return True because auto-reconnect will handle it
        assert result is True
        # State should be reconnecting, not disconnected
        # (reconnect thread will be started)

    def test_start_connection_failed_no_auto_reconnect(self):
        """Test start with failed connection and auto-reconnect disabled"""
        portfolio = create_mock_portfolio()
        portfolio.connect.return_value = False

        config = ConnectionConfig(auto_reconnect=False)
        manager = ConnectionManager(portfolio, config)

        result = manager.start()

        assert result is False
        assert manager.state == ConnectionState.DISCONNECTED


class TestConnectionManagerStop:
    """Tests for stopping connection manager"""

    def test_stop_when_disconnected(self):
        """Test stop when already disconnected"""
        portfolio = create_mock_portfolio()
        manager = ConnectionManager(portfolio)

        manager.stop()

        assert manager.state == ConnectionState.DISCONNECTED

    def test_stop_disconnects_portfolio(self):
        """Test that stop disconnects portfolio"""
        portfolio = create_mock_portfolio()
        portfolio.connected = True

        manager = ConnectionManager(portfolio)
        manager._state = ConnectionState.CONNECTED

        manager.stop()

        assert portfolio.shutdown.called
        assert portfolio.disconnect.called
        assert manager.state == ConnectionState.DISCONNECTED


class TestConnectionManagerState:
    """Tests for connection state management"""

    def test_state_property_thread_safe(self):
        """Test that state property is thread-safe"""
        portfolio = create_mock_portfolio()
        manager = ConnectionManager(portfolio)

        # State should be readable without deadlock
        state = manager.state
        assert state == ConnectionState.DISCONNECTED

    def test_is_connected_property(self):
        """Test is_connected property"""
        portfolio = create_mock_portfolio()
        manager = ConnectionManager(portfolio)

        assert manager.is_connected is False

        manager._state = ConnectionState.CONNECTED
        assert manager.is_connected is True

        manager._state = ConnectionState.RECONNECTING
        assert manager.is_connected is False

    def test_set_state(self):
        """Test internal _set_state method"""
        portfolio = create_mock_portfolio()
        manager = ConnectionManager(portfolio)

        manager._set_state(ConnectionState.CONNECTING)
        assert manager.state == ConnectionState.CONNECTING


class TestConnectionManagerCallbacks:
    """Tests for connection callbacks"""

    def test_on_connected_callback(self):
        """Test on_connected callback is invoked"""
        portfolio = create_mock_portfolio()
        portfolio.connect.return_value = True
        portfolio.connected = True

        config = ConnectionConfig(keepalive_enabled=False)
        manager = ConnectionManager(portfolio, config)

        callback_called = []
        manager.on_connected = lambda: callback_called.append(True)

        manager.start()

        assert len(callback_called) == 1

    def test_on_disconnected_callback(self):
        """Test on_disconnected callback is invoked"""
        portfolio = create_mock_portfolio()
        config = ConnectionConfig(auto_reconnect=False)
        manager = ConnectionManager(portfolio, config)
        manager._state = ConnectionState.CONNECTED

        callback_called = []
        manager.on_disconnected = lambda: callback_called.append(True)

        manager._handle_disconnection()

        assert len(callback_called) == 1

    def test_on_reconnecting_callback(self):
        """Test on_reconnecting callback is invoked"""
        portfolio = create_mock_portfolio()
        portfolio.connect.return_value = False

        config = ConnectionConfig(
            auto_reconnect=True,
            reconnect_delay_initial=0.01,
            max_reconnect_attempts=1,
        )
        manager = ConnectionManager(portfolio, config)

        callback_attempts = []
        manager.on_reconnecting = lambda attempt: callback_attempts.append(attempt)

        # Start and wait briefly for reconnect attempt
        manager._state = ConnectionState.DISCONNECTED
        manager._start_reconnect_thread()
        time.sleep(0.1)
        manager._shutdown_event.set()

        # Should have recorded at least one attempt
        assert len(callback_attempts) >= 1
        assert callback_attempts[0] == 1

    def test_callback_exception_handled(self):
        """Test that callback exceptions don't crash the manager"""
        portfolio = create_mock_portfolio()
        portfolio.connect.return_value = True
        portfolio.connected = True

        config = ConnectionConfig(keepalive_enabled=False)
        manager = ConnectionManager(portfolio, config)

        def bad_callback():
            raise Exception("Callback error")

        manager.on_connected = bad_callback

        # Should not raise
        result = manager.start()
        assert result is True


class TestConnectionManagerReconnect:
    """Tests for reconnection logic"""

    def test_reconnect_attempts_tracked(self):
        """Test reconnect attempts are tracked"""
        portfolio = create_mock_portfolio()
        manager = ConnectionManager(portfolio)

        assert manager.reconnect_attempts == 0
        manager._reconnect_attempts = 5
        assert manager.reconnect_attempts == 5

    def test_reconnect_attempts_reset_on_success(self):
        """Test reconnect attempts are reset on successful connection"""
        portfolio = create_mock_portfolio()
        portfolio.connect.return_value = True
        portfolio.connected = True

        config = ConnectionConfig(keepalive_enabled=False)
        manager = ConnectionManager(portfolio, config)
        manager._reconnect_attempts = 5

        manager._connect()
        manager._on_connected()

        assert manager._reconnect_attempts == 0

    def test_max_reconnect_attempts(self):
        """Test max reconnect attempts is respected"""
        portfolio = create_mock_portfolio()
        portfolio.connect.return_value = False

        config = ConnectionConfig(
            auto_reconnect=True,
            reconnect_delay_initial=0.01,
            max_reconnect_attempts=2,
        )
        manager = ConnectionManager(portfolio, config)

        # Simulate being past max attempts
        manager._reconnect_attempts = 3
        manager._state = ConnectionState.DISCONNECTED

        # Start reconnect thread
        manager._start_reconnect_thread()
        time.sleep(0.1)

        # Should have stopped after exceeding max attempts
        assert manager.state == ConnectionState.DISCONNECTED


class TestConnectionManagerKeepalive:
    """Tests for keepalive functionality"""

    def test_keepalive_disabled(self):
        """Test keepalive thread not started when disabled"""
        portfolio = create_mock_portfolio()
        config = ConnectionConfig(keepalive_enabled=False)
        manager = ConnectionManager(portfolio, config)

        manager._start_keepalive_thread()

        assert manager._keepalive_thread is None

    def test_keepalive_sends_request(self):
        """Test keepalive sends current time request"""
        portfolio = create_mock_portfolio()
        portfolio.connect.return_value = True
        portfolio.connected = True

        config = ConnectionConfig(
            keepalive_enabled=True,
            keepalive_interval=0.05,
            keepalive_timeout=0.1,
        )
        manager = ConnectionManager(portfolio, config)

        manager._state = ConnectionState.CONNECTED
        manager._start_keepalive_thread()

        # Wait for at least one keepalive cycle
        time.sleep(0.15)
        manager._shutdown_event.set()

        # Should have called reqCurrentTime at least once
        assert portfolio.reqCurrentTime.called


class TestConnectionManagerStreamRecovery:
    """Tests for stream state preservation and recovery"""

    def test_save_stream_state(self):
        """Test stream state is saved"""
        portfolio = create_mock_portfolio()
        portfolio._stream_subscriptions = {1: "SPY", 2: "AAPL"}
        portfolio._bar_subscriptions = {3: "QQQ"}

        # Create mock positions with contracts
        from ibapi.contract import Contract
        spy_contract = Contract()
        spy_contract.symbol = "SPY"
        aapl_contract = Contract()
        aapl_contract.symbol = "AAPL"
        qqq_contract = Contract()
        qqq_contract.symbol = "QQQ"

        def get_position(symbol):
            mock_pos = Mock()
            if symbol == "SPY":
                mock_pos.contract = spy_contract
            elif symbol == "AAPL":
                mock_pos.contract = aapl_contract
            elif symbol == "QQQ":
                mock_pos.contract = qqq_contract
            else:
                return None
            return mock_pos

        portfolio.get_position = get_position

        config = ConnectionConfig(auto_reconnect=False)
        manager = ConnectionManager(portfolio, config)

        manager._save_stream_state()

        assert len(manager._saved_tick_streams) == 2
        assert len(manager._saved_bar_streams) == 1
        assert "SPY" in manager._saved_tick_streams
        assert "AAPL" in manager._saved_tick_streams
        assert "QQQ" in manager._saved_bar_streams

    def test_recover_streams(self):
        """Test stream recovery after reconnect"""
        from ibapi.contract import Contract

        portfolio = create_mock_portfolio()
        portfolio._stream_subscriptions = {}
        portfolio._stream_req_ids = {}
        portfolio._bar_subscriptions = {}
        portfolio._bar_req_ids = {}

        config = ConnectionConfig(auto_reconnect=False)
        manager = ConnectionManager(portfolio, config)

        # Set up saved streams
        contract = Contract()
        contract.symbol = "SPY"
        manager._saved_tick_streams["SPY"] = StreamSubscription(
            symbol="SPY",
            contract=contract,
            req_id=1,
        )

        manager._recover_streams()

        # Should have called stream_symbol
        portfolio.stream_symbol.assert_called()

        # Saved streams should be cleared after recovery
        assert len(manager._saved_tick_streams) == 0


class TestConnectionManagerStatus:
    """Tests for status reporting"""

    def test_get_status(self):
        """Test get_status returns correct information"""
        portfolio = create_mock_portfolio()
        config = ConnectionConfig(keepalive_interval=45.0)
        manager = ConnectionManager(portfolio, config)

        status = manager.get_status()

        assert "state" in status
        assert status["state"] == "disconnected"
        assert "connected" in status
        assert status["connected"] is False
        assert "reconnect_attempts" in status
        assert "config" in status
        assert status["config"]["keepalive_interval"] == 45.0

    def test_get_status_with_times(self):
        """Test get_status includes timestamps when set"""
        portfolio = create_mock_portfolio()
        manager = ConnectionManager(portfolio)

        manager._last_connect_time = datetime.now()
        manager._last_keepalive_time = datetime.now()

        status = manager.get_status()

        assert status["last_connect_time"] is not None
        assert status["last_keepalive_time"] is not None


class TestConnectionManagerPortfolioCallbacks:
    """Tests for portfolio callback setup"""

    def test_connection_closed_callback_setup(self):
        """Test connectionClosed callback is registered"""
        portfolio = create_mock_portfolio()
        manager = ConnectionManager(portfolio)

        assert "connectionClosed" in portfolio._callbacks

    def test_current_time_wrapper_setup(self):
        """Test currentTime wrapper is set up"""
        portfolio = create_mock_portfolio()
        manager = ConnectionManager(portfolio)

        # Should have a currentTime method set
        assert hasattr(portfolio, 'currentTime')


class TestConnectionManagerHandleDisconnection:
    """Tests for disconnection handling"""

    def test_handle_disconnection_during_shutdown(self):
        """Test disconnection is ignored during shutdown"""
        portfolio = create_mock_portfolio()
        manager = ConnectionManager(portfolio)
        manager._state = ConnectionState.SHUTTING_DOWN

        callback_called = []
        manager.on_disconnected = lambda: callback_called.append(True)

        manager._handle_disconnection()

        # Callback should not be called during shutdown
        assert len(callback_called) == 0

    def test_handle_disconnection_saves_stream_state(self):
        """Test disconnection saves stream state"""
        portfolio = create_mock_portfolio()
        config = ConnectionConfig(auto_reconnect=False)
        manager = ConnectionManager(portfolio, config)
        manager._state = ConnectionState.CONNECTED

        # Mock stream subscriptions
        portfolio._stream_subscriptions = {1: "SPY"}

        manager._handle_disconnection()

        # State should be saved (empty in this case since no positions)
        assert manager.state == ConnectionState.DISCONNECTED


class TestConnectionManagerConnect:
    """Tests for connection attempts"""

    def test_connect_success(self):
        """Test successful connection"""
        portfolio = create_mock_portfolio()
        portfolio.connect.return_value = True

        config = ConnectionConfig(keepalive_enabled=False)
        manager = ConnectionManager(portfolio, config)

        result = manager._connect()

        assert result is True
        assert portfolio.connect.called

    def test_connect_failure(self):
        """Test failed connection"""
        portfolio = create_mock_portfolio()
        portfolio.connect.return_value = False

        manager = ConnectionManager(portfolio)

        result = manager._connect()

        assert result is False

    def test_connect_exception(self):
        """Test connection with exception"""
        portfolio = create_mock_portfolio()
        portfolio.connect.side_effect = Exception("Connection error")

        manager = ConnectionManager(portfolio)

        error_called = []
        manager.on_error = lambda e: error_called.append(e)

        result = manager._connect()

        assert result is False
        assert len(error_called) == 1


class TestConnectionManagerHealthMonitor:
    """Tests for health monitoring"""

    def test_health_check_detects_disconnection(self):
        """Test health check detects disconnection"""
        portfolio = create_mock_portfolio()
        portfolio.connected = False  # Simulate disconnection

        config = ConnectionConfig(
            auto_reconnect=False,
            health_check_interval=0.05,
        )
        manager = ConnectionManager(portfolio, config)
        manager._state = ConnectionState.CONNECTED

        callback_called = []
        manager.on_disconnected = lambda: callback_called.append(True)

        manager._start_health_thread()

        # Wait for health check
        time.sleep(0.15)
        manager._shutdown_event.set()

        # Should have detected disconnection
        assert len(callback_called) >= 1


class TestThreadSafety:
    """Tests for thread safety"""

    def test_concurrent_state_access(self):
        """Test concurrent state access is thread-safe"""
        portfolio = create_mock_portfolio()
        manager = ConnectionManager(portfolio)
        errors = []

        def access_state():
            try:
                for _ in range(100):
                    _ = manager.state
                    _ = manager.is_connected
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=access_state) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
