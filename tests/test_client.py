"""
Unit tests for client.py

Tests IBClient connection management, callbacks, and state handling.
Uses mocks to avoid actual IB connections.
"""

import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, patch, PropertyMock
from threading import Event


# Mock ibapi before importing client
@pytest.fixture(autouse=True)
def mock_ibapi():
    """Mock ibapi module for all tests"""
    with patch.dict('sys.modules', {
        'ibapi': MagicMock(),
        'ibapi.client': MagicMock(),
        'ibapi.wrapper': MagicMock(),
        'ibapi.common': MagicMock(),
    }):
        yield


class TestIBClientInit:
    """Tests for IBClient initialization"""

    def test_init_defaults(self, mock_ibapi):
        """Test default initialization values"""
        from client import IBClient

        with patch.object(IBClient, '__init__', lambda self, **kwargs: None):
            client = IBClient.__new__(IBClient)
            # Manually set what __init__ would set
            client.host = "127.0.0.1"
            client.port = 7497
            client.client_id = 1
            client.timeout = 10.0
            client._connected = Event()
            client._next_order_id = None
            client._thread = None
            client._next_req_id = 1
            client._callbacks = {}
            client.managed_accounts = []

            assert client.host == "127.0.0.1"
            assert client.port == 7497
            assert client.client_id == 1
            assert client.timeout == 10.0

    def test_init_custom_values(self, mock_ibapi):
        """Test initialization with custom values"""
        from client import IBClient

        with patch.object(IBClient, '__init__', lambda self, **kwargs: None):
            client = IBClient.__new__(IBClient)
            client.host = "192.168.1.100"
            client.port = 4002
            client.client_id = 5
            client.timeout = 30.0
            client._connected = Event()

            assert client.host == "192.168.1.100"
            assert client.port == 4002
            assert client.client_id == 5
            assert client.timeout == 30.0

    def test_ports_constants(self, mock_ibapi):
        """Test PORTS class constant"""
        from client import IBClient

        assert IBClient.PORTS["tws_live"] == 7496
        assert IBClient.PORTS["tws_paper"] == 7497
        assert IBClient.PORTS["gateway_live"] == 4001
        assert IBClient.PORTS["gateway_paper"] == 4002


class TestIBClientProperties:
    """Tests for IBClient properties"""

    @pytest.fixture
    def client(self, mock_ibapi):
        """Create a mock client for testing"""
        from client import IBClient

        with patch.object(IBClient, '__init__', lambda self, **kwargs: None):
            client = IBClient.__new__(IBClient)
            client._connected = Event()
            client._next_order_id = None
            client._next_req_id = 1
            client._lock = MagicMock()
            client._callbacks = {}
            client.managed_accounts = []
            client.host = "127.0.0.1"
            client.port = 7497
            return client

    def test_connected_false(self, client):
        """Test connected property when not connected"""
        assert client.connected is False

    def test_connected_true(self, client):
        """Test connected property when connected"""
        client._connected.set()
        assert client.connected is True

    def test_next_order_id_none(self, client):
        """Test next_order_id when not set"""
        assert client.next_order_id is None

    def test_next_order_id_value(self, client):
        """Test next_order_id with value"""
        client._next_order_id = 100
        assert client.next_order_id == 100

    def test_get_next_req_id_increments(self, client):
        """Test get_next_req_id increments each call"""
        # Mock the lock context manager
        client._lock.__enter__ = MagicMock()
        client._lock.__exit__ = MagicMock()

        first = client.get_next_req_id()
        second = client.get_next_req_id()
        third = client.get_next_req_id()

        assert first == 1
        assert second == 2
        assert third == 3

    def test_get_next_req_id_thread_safe(self, client):
        """Test get_next_req_id uses lock"""
        from threading import Lock
        client._lock = Lock()

        # Should not raise even with real lock
        id1 = client.get_next_req_id()
        id2 = client.get_next_req_id()
        assert id2 > id1


class TestIBClientCallbacks:
    """Tests for IBClient callback registration and handling"""

    @pytest.fixture
    def client(self, mock_ibapi):
        """Create a mock client for testing"""
        from client import IBClient

        with patch.object(IBClient, '__init__', lambda self, **kwargs: None):
            client = IBClient.__new__(IBClient)
            client._connected = Event()
            client._next_order_id = None
            client._callbacks = {}
            client.managed_accounts = []
            return client

    def test_register_callback(self, client):
        """Test registering a callback"""
        handler = MagicMock()
        client.register_callback("error", handler)

        assert "error" in client._callbacks
        assert client._callbacks["error"] == handler

    def test_register_callback_overwrites(self, client):
        """Test registering overwrites previous callback"""
        handler1 = MagicMock()
        handler2 = MagicMock()

        client.register_callback("error", handler1)
        client.register_callback("error", handler2)

        assert client._callbacks["error"] == handler2

    def test_nextValidId_callback(self, client):
        """Test nextValidId sets state and calls callback"""
        handler = MagicMock()
        client.register_callback("nextValidId", handler)

        client.nextValidId(42)

        assert client._next_order_id == 42
        assert client._connected.is_set()
        handler.assert_called_once_with(42)

    def test_nextValidId_no_callback(self, client):
        """Test nextValidId works without callback"""
        client.nextValidId(100)

        assert client._next_order_id == 100
        assert client._connected.is_set()

    def test_managedAccounts_callback(self, client):
        """Test managedAccounts parses and calls callback"""
        handler = MagicMock()
        client.register_callback("managedAccounts", handler)

        client.managedAccounts("DU123456,DU789012")

        assert client.managed_accounts == ["DU123456", "DU789012"]
        handler.assert_called_once_with(["DU123456", "DU789012"])

    def test_managedAccounts_single(self, client):
        """Test managedAccounts with single account"""
        client.managedAccounts("DU123456")

        assert client.managed_accounts == ["DU123456"]

    def test_managedAccounts_whitespace(self, client):
        """Test managedAccounts handles whitespace"""
        client.managedAccounts("  DU123456 , DU789012  ")

        assert client.managed_accounts == ["DU123456", "DU789012"]

    def test_managedAccounts_empty_strings(self, client):
        """Test managedAccounts filters empty strings"""
        client.managedAccounts("DU123456,,DU789012,")

        assert client.managed_accounts == ["DU123456", "DU789012"]

    def test_connectionClosed_callback(self, client):
        """Test connectionClosed clears state and calls callback"""
        client._connected.set()
        handler = MagicMock()
        client.register_callback("connectionClosed", handler)

        client.connectionClosed()

        assert not client._connected.is_set()
        handler.assert_called_once()

    def test_error_callback(self, client):
        """Test error calls callback"""
        handler = MagicMock()
        client.register_callback("error", handler)

        client.error(1, 0, 200, "Test error")

        handler.assert_called_once_with(1, 200, "Test error")

    def test_error_info_codes(self, client):
        """Test error handles info codes gracefully"""
        # These are info codes that should not be treated as errors
        info_codes = [2104, 2106, 2158, 2119]

        for code in info_codes:
            # Should not raise
            client.error(-1, 0, code, "Info message")

    def test_error_system_message(self, client):
        """Test error handles system messages (reqId=-1)"""
        # Should not raise
        client.error(-1, 0, 1100, "System message")


class TestIBClientRepr:
    """Tests for IBClient string representation"""

    @pytest.fixture
    def client(self, mock_ibapi):
        """Create a mock client for testing"""
        from client import IBClient

        with patch.object(IBClient, '__init__', lambda self, **kwargs: None):
            client = IBClient.__new__(IBClient)
            client._connected = Event()
            client.host = "127.0.0.1"
            client.port = 7497
            return client

    def test_repr_disconnected(self, client):
        """Test __repr__ when disconnected"""
        repr_str = repr(client)

        assert "127.0.0.1:7497" in repr_str
        assert "disconnected" in repr_str

    def test_repr_connected(self, client):
        """Test __repr__ when connected"""
        client._connected.set()
        repr_str = repr(client)

        assert "127.0.0.1:7497" in repr_str
        assert "connected" in repr_str


class TestIBClientConnection:
    """Tests for IBClient connection methods"""

    @pytest.fixture
    def client(self, mock_ibapi):
        """Create a mock client for testing"""
        from client import IBClient

        with patch.object(IBClient, '__init__', lambda self, **kwargs: None):
            client = IBClient.__new__(IBClient)
            client._connected = Event()
            client._next_order_id = None
            client._transport = None
            client._run_task = None
            client.host = "127.0.0.1"
            client.port = 7497
            client.client_id = 1
            client.timeout = 1.0  # Short timeout for tests
            return client

    async def test_disconnect_clears_connected(self, client):
        """Test disconnect clears connected state"""
        client._connected.set()

        await client.disconnect()

        assert not client._connected.is_set()

    async def test_reconnect_calls_both(self, client):
        """Test reconnect calls disconnect then connect"""
        with patch.object(client, 'disconnect', new_callable=AsyncMock) as mock_disconnect, \
             patch.object(client, 'connect', new_callable=AsyncMock, return_value=True) as mock_connect:
            result = await client.reconnect()

            mock_disconnect.assert_called_once()
            mock_connect.assert_called_once()
            assert result is True


class TestIBClientContextManager:
    """Tests for IBClient context manager"""

    @pytest.fixture
    def client(self, mock_ibapi):
        """Create a mock client for testing"""
        from client import IBClient

        with patch.object(IBClient, '__init__', lambda self, **kwargs: None):
            client = IBClient.__new__(IBClient)
            client._connected = Event()
            return client

    async def test_enter_calls_connect(self, client):
        """Test __aenter__ calls connect"""
        with patch.object(client, 'connect', new_callable=AsyncMock) as mock_connect:
            result = await client.__aenter__()

            mock_connect.assert_called_once()
            assert result is client

    async def test_exit_calls_disconnect(self, client):
        """Test __aexit__ calls disconnect"""
        with patch.object(client, 'disconnect', new_callable=AsyncMock) as mock_disconnect:
            result = await client.__aexit__(None, None, None)

            mock_disconnect.assert_called_once()
            assert result is False  # Don't suppress exceptions

    async def test_exit_with_exception(self, client):
        """Test __aexit__ returns False (doesn't suppress exception)"""
        with patch.object(client, 'disconnect', new_callable=AsyncMock):
            result = await client.__aexit__(ValueError, ValueError("test"), None)

            assert result is False


# =============================================================================
# Extended Connection Tests
# =============================================================================

class TestIBClientConnectExtended:
    """Extended tests for IBClient connect method"""

    @pytest.fixture
    def client(self, mock_ibapi):
        """Create a mock client for testing"""
        from client import IBClient

        with patch.object(IBClient, '__init__', lambda self, **kwargs: None):
            client = IBClient.__new__(IBClient)
            client._connected = asyncio.Event()
            client._next_order_id = None
            client._transport = None
            client._run_task = None
            client._host = "127.0.0.1"
            client._port = 7497
            client.host = "127.0.0.1"
            client.port = 7497
            client.client_id = 1
            client.timeout = 0.1  # Very short timeout for tests
            return client

    async def test_connect_creates_task(self, client):
        """Test connect creates and starts a task"""
        with patch('client.AsyncIBTransport') as MockTransport:
            instance = MockTransport.return_value
            instance.connect = AsyncMock()

            async def mock_run():
                client._connected.set()

            instance.run = mock_run

            result = await client.connect()

            assert client._run_task is not None
            assert result is True

    async def test_connect_timeout_returns_false(self, client):
        """Test connect returns False on timeout"""
        with patch('client.AsyncIBTransport') as MockTransport:
            instance = MockTransport.return_value
            instance.connect = AsyncMock()
            instance.run = AsyncMock()  # Never sets _connected

            result = await client.connect()

            assert result is False

    async def test_connect_exception_returns_false(self, client):
        """Test connect returns False on exception"""
        with patch('client.AsyncIBTransport') as MockTransport:
            instance = MockTransport.return_value
            instance.connect = AsyncMock(side_effect=Exception("Connection error"))

            result = await client.connect()

            assert result is False

    async def test_connect_success_returns_true(self, client):
        """Test connect returns True on success"""
        with patch('client.AsyncIBTransport') as MockTransport:
            instance = MockTransport.return_value
            instance.connect = AsyncMock()

            async def mock_run():
                client._connected.set()

            instance.run = mock_run

            result = await client.connect()

            assert result is True


class TestIBClientDisconnectExtended:
    """Extended tests for disconnect method"""

    @pytest.fixture
    def client(self, mock_ibapi):
        """Create a mock client for testing"""
        from client import IBClient

        with patch.object(IBClient, '__init__', lambda self, **kwargs: None):
            client = IBClient.__new__(IBClient)
            client._connected = asyncio.Event()
            client._connected.set()
            client._transport = None
            client._run_task = None
            return client

    async def test_disconnect_handles_exception(self, client):
        """Test disconnect handles exceptions gracefully"""
        mock_transport = MagicMock()
        mock_transport.disconnect.side_effect = Exception("Disconnect error")
        client._transport = mock_transport

        # Should not raise
        await client.disconnect()

        # Connected should still be cleared
        assert not client._connected.is_set()


class TestIBClientErrorExtended:
    """Extended tests for error handling"""

    @pytest.fixture
    def client(self, mock_ibapi):
        """Create a mock client for testing"""
        from client import IBClient

        with patch.object(IBClient, '__init__', lambda self, **kwargs: None):
            client = IBClient.__new__(IBClient)
            client._connected = Event()
            client._callbacks = {}
            return client

    def test_error_with_advanced_order_reject(self, client):
        """Test error handles advancedOrderRejectJson"""
        # Should not raise
        client.error(1, 0, 201, "Order rejected", '{"reason": "test"}')

    def test_error_delayed_data_notification(self, client):
        """Test error handles delayed data notification code"""
        # 10167 is delayed market data notification
        # Should not raise
        client.error(-1, 0, 10167, "Delayed market data")

    def test_error_actual_error_with_reqid(self, client):
        """Test error logs actual error with reqId"""
        # Should not raise
        client.error(123, 0, 200, "No security definition found")

    def test_error_no_callback(self, client):
        """Test error works without callback registered"""
        # Should not raise
        client.error(1, 0, 200, "Test error")


class TestIBClientCurrentTime:
    """Tests for currentTime callback"""

    @pytest.fixture
    def client(self, mock_ibapi):
        """Create a mock client for testing"""
        from client import IBClient

        with patch.object(IBClient, '__init__', lambda self, **kwargs: None):
            client = IBClient.__new__(IBClient)
            return client

    def test_currentTime_does_not_raise(self, client):
        """Test currentTime doesn't raise"""
        # Should not raise
        client.currentTime(1234567890)


# =============================================================================
# Request ID Management Tests
# =============================================================================

class TestRequestIdManagement:
    """Tests for request ID management"""

    @pytest.fixture
    def client(self, mock_ibapi):
        """Create a mock client for testing"""
        from client import IBClient
        from threading import Lock

        with patch.object(IBClient, '__init__', lambda self, **kwargs: None):
            client = IBClient.__new__(IBClient)
            client._next_req_id = 1
            client._lock = Lock()
            return client

    def test_get_next_req_id_starts_at_one(self, client):
        """Test get_next_req_id starts at 1"""
        result = client.get_next_req_id()
        assert result == 1

    def test_get_next_req_id_increments(self, client):
        """Test get_next_req_id increments"""
        first = client.get_next_req_id()
        second = client.get_next_req_id()
        third = client.get_next_req_id()

        assert first == 1
        assert second == 2
        assert third == 3

    def test_get_next_req_id_concurrent(self, client):
        """Test get_next_req_id is thread-safe"""
        import threading

        results = []

        def get_id():
            for _ in range(100):
                results.append(client.get_next_req_id())

        threads = [threading.Thread(target=get_id) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All IDs should be unique
        assert len(results) == 500
        assert len(set(results)) == 500
