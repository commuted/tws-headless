"""
Unit tests for command_server.py

Tests CommandResult, CommandServer command handling, and socket communication.
"""

import asyncio
import json
import os
import socket
import tempfile
import threading
import time
import pytest
from unittest.mock import MagicMock, patch

from command_server import (
    CommandStatus,
    CommandResult,
    CommandServer,
    RequestEntry,
    RequestQueue,
    send_command,
    DEFAULT_SOCKET_PATH,
)


# =============================================================================
# CommandResult Tests
# =============================================================================

class TestCommandResult:
    """Tests for CommandResult dataclass"""

    def test_success_result(self):
        """Test creating a success result"""
        result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="Operation completed",
            data={"value": 42},
        )
        assert result.status == CommandStatus.SUCCESS
        assert result.message == "Operation completed"
        assert result.data == {"value": 42}

    def test_error_result(self):
        """Test creating an error result"""
        result = CommandResult(
            status=CommandStatus.ERROR,
            message="Something went wrong",
        )
        assert result.status == CommandStatus.ERROR
        assert result.data == {}

    def test_to_dict(self):
        """Test to_dict produces correct dictionary"""
        result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="OK",
            data={"key": "value"},
        )
        d = result.to_dict()

        assert d["status"] == "success"
        assert d["message"] == "OK"
        assert d["data"] == {"key": "value"}

    def test_to_dict_error(self):
        """Test to_dict with error status"""
        result = CommandResult(
            status=CommandStatus.ERROR,
            message="Failed",
        )
        d = result.to_dict()

        assert d["status"] == "error"
        assert d["message"] == "Failed"

    def test_to_json(self):
        """Test to_json produces valid JSON"""
        result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="Test",
            data={"count": 10},
        )
        json_str = result.to_json()

        # Should be valid JSON
        parsed = json.loads(json_str)
        assert parsed["status"] == "success"
        assert parsed["message"] == "Test"
        assert parsed["data"]["count"] == 10

    def test_to_json_special_characters(self):
        """Test to_json handles special characters"""
        result = CommandResult(
            status=CommandStatus.SUCCESS,
            message='Message with "quotes" and \\ backslash',
            data={"text": "line1\nline2"},
        )
        json_str = result.to_json()

        # Should be valid JSON
        parsed = json.loads(json_str)
        assert "quotes" in parsed["message"]


# =============================================================================
# CommandServer Unit Tests (no socket)
# =============================================================================

class TestCommandServerUnit:
    """Unit tests for CommandServer (no actual socket operations)"""

    def test_register_handler(self):
        """Test registering a command handler"""
        server = CommandServer()
        handler = MagicMock(return_value=CommandResult(
            status=CommandStatus.SUCCESS,
            message="OK",
        ))

        server.register_handler("test", handler)

        assert "test" in server.commands

    def test_register_handler_case_insensitive(self):
        """Test handlers are stored case-insensitively"""
        server = CommandServer()
        handler = MagicMock()

        server.register_handler("TestCmd", handler)

        assert "testcmd" in server.commands

    def test_unregister_handler(self):
        """Test unregistering a command handler"""
        server = CommandServer()
        handler = MagicMock()

        server.register_handler("test", handler)
        assert "test" in server.commands

        server.unregister_handler("test")
        assert "test" not in server.commands

    def test_unregister_nonexistent(self):
        """Test unregistering a handler that doesn't exist"""
        server = CommandServer()
        # Should not raise
        server.unregister_handler("nonexistent")

    def test_commands_property(self):
        """Test commands property returns sorted list"""
        server = CommandServer()
        server.register_handler("zebra", MagicMock())
        server.register_handler("alpha", MagicMock())
        server.register_handler("middle", MagicMock())

        commands = server.commands
        # Should include built-in commands plus registered ones
        assert "alpha" in commands
        assert "help" in commands
        assert "ping" in commands
        # Should be sorted
        assert commands == sorted(commands)

    def test_builtin_commands(self):
        """Test built-in commands are registered"""
        server = CommandServer()
        assert "help" in server.commands
        assert "ping" in server.commands

    def test_execute_command_success(self):
        """Test executing a registered command"""
        server = CommandServer()
        handler = MagicMock(return_value=CommandResult(
            status=CommandStatus.SUCCESS,
            message="Handler executed",
            data={"result": "ok"},
        ))
        server.register_handler("mycommand", handler)

        result = server._execute_command("mycommand arg1 arg2")

        handler.assert_called_once_with(["arg1", "arg2"])
        assert result.status == CommandStatus.SUCCESS
        assert result.message == "Handler executed"

    def test_execute_command_unknown(self):
        """Test executing an unknown command"""
        server = CommandServer()

        result = server._execute_command("unknown_command")

        assert result.status == CommandStatus.ERROR
        assert "Unknown command" in result.message

    def test_execute_command_empty(self):
        """Test executing empty command"""
        server = CommandServer()

        result = server._execute_command("")

        assert result.status == CommandStatus.ERROR
        assert "Empty command" in result.message

    def test_execute_command_whitespace_only(self):
        """Test executing whitespace-only command"""
        server = CommandServer()

        result = server._execute_command("   ")

        assert result.status == CommandStatus.ERROR

    def test_execute_command_exception(self):
        """Test command that raises exception"""
        server = CommandServer()
        handler = MagicMock(side_effect=ValueError("Handler error"))
        server.register_handler("failing", handler)

        result = server._execute_command("failing")

        assert result.status == CommandStatus.ERROR
        assert "Command failed" in result.message

    def test_handle_help(self):
        """Test built-in help command"""
        server = CommandServer()
        server.register_handler("custom", MagicMock())

        result = server._handle_help([])

        assert result.status == CommandStatus.SUCCESS
        assert "custom" in result.message
        assert "help" in result.message
        assert "ping" in result.message
        assert "commands" in result.data

    def test_handle_ping(self):
        """Test built-in ping command"""
        server = CommandServer()

        result = server._handle_ping([])

        assert result.status == CommandStatus.SUCCESS
        assert result.message == "pong"


# =============================================================================
# CommandServer Integration Tests (with socket)
# =============================================================================

class TestCommandServerIntegration:
    """Integration tests for CommandServer with actual socket communication"""

    @pytest.fixture
    def temp_socket_path(self):
        """Create a temporary socket path"""
        # Use tempfile to get a unique path
        fd, path = tempfile.mkstemp(suffix=".sock")
        os.close(fd)
        os.unlink(path)  # Remove the file, we just want the path
        yield path
        # Cleanup
        if os.path.exists(path):
            os.unlink(path)

    @pytest.fixture
    async def running_server(self, temp_socket_path):
        """Create and start a server, yield it, then stop"""
        server = CommandServer(socket_path=temp_socket_path)

        # Add a test handler
        def test_handler(args):
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Received args: {args}",
                data={"args": args},
            )

        server.register_handler("test", test_handler)

        started = server.start()
        assert started, "Server failed to start"

        # Give event loop time to create socket
        await asyncio.sleep(0.1)

        yield server, temp_socket_path

        server.stop()
        await asyncio.sleep(0.05)

    async def test_server_start_stop(self, temp_socket_path):
        """Test server starts and stops cleanly"""
        server = CommandServer(socket_path=temp_socket_path)

        assert server.start() is True
        # Yield control so _serve() can bind the socket
        await asyncio.sleep(0.1)
        assert os.path.exists(temp_socket_path)

        server.stop()
        # Socket file cleaned up by _serve() finally block after cancellation
        await asyncio.sleep(0.05)
        assert not os.path.exists(temp_socket_path)

    async def test_server_socket_permissions(self, temp_socket_path):
        """Test socket has correct permissions (owner only)"""
        server = CommandServer(socket_path=temp_socket_path)
        server.start()
        await asyncio.sleep(0.1)

        mode = os.stat(temp_socket_path).st_mode & 0o777
        assert mode == 0o600, f"Socket permissions should be 0600, got {oct(mode)}"

        server.stop()
        await asyncio.sleep(0.05)

    async def test_send_command_ping(self, running_server):
        """Test sending ping command via socket"""
        server, socket_path = running_server

        result = await asyncio.to_thread(send_command, "ping", socket_path)

        assert result.status == CommandStatus.SUCCESS
        assert result.message == "pong"

    async def test_send_command_help(self, running_server):
        """Test sending help command via socket"""
        server, socket_path = running_server

        result = await asyncio.to_thread(send_command, "help", socket_path)

        assert result.status == CommandStatus.SUCCESS
        assert "test" in result.message  # Our registered handler
        assert "commands" in result.data

    async def test_send_command_custom(self, running_server):
        """Test sending custom command via socket"""
        server, socket_path = running_server

        result = await asyncio.to_thread(send_command, "test arg1 arg2", socket_path)

        assert result.status == CommandStatus.SUCCESS
        assert result.data["args"] == ["arg1", "arg2"]

    async def test_send_command_unknown(self, running_server):
        """Test sending unknown command"""
        server, socket_path = running_server

        result = await asyncio.to_thread(send_command, "nonexistent", socket_path)

        assert result.status == CommandStatus.ERROR
        assert "Unknown command" in result.message

    async def test_send_command_server_not_running(self, temp_socket_path):
        """Test sending command when server is not running"""
        result = await asyncio.to_thread(send_command, "ping", temp_socket_path)

        assert result.status == CommandStatus.ERROR
        assert "not running" in result.message.lower() or "not found" in result.message.lower()

    async def test_multiple_clients(self, running_server):
        """Test multiple clients can connect"""
        server, socket_path = running_server

        # send_command is sync/blocking — run each in a thread
        tasks = [asyncio.to_thread(send_command, "ping", socket_path) for _ in range(5)]
        results = await asyncio.gather(*tasks)

        assert len(results) == 5
        assert all(r.status == CommandStatus.SUCCESS for r in results)


# =============================================================================
# send_command Function Tests
# =============================================================================

class TestSendCommand:
    """Tests for send_command helper function"""

    def test_send_command_connection_refused(self):
        """Test send_command when connection is refused"""
        # Use a path that doesn't exist
        result = send_command("ping", socket_path="/nonexistent/path.sock")

        assert result.status == CommandStatus.ERROR

    def test_send_command_timeout(self):
        """Test send_command with very short timeout on slow operation"""
        # This is tricky to test without a real slow server
        # Just verify the parameter is accepted
        result = send_command(
            "ping",
            socket_path="/nonexistent.sock",
            timeout=0.001,
        )
        assert result.status == CommandStatus.ERROR


# =============================================================================
# Authentication Tests
# =============================================================================

class TestCommandServerAuthentication:
    """Tests for CommandServer authentication functionality"""

    def test_unauthorized_status_exists(self):
        """Test UNAUTHORIZED status exists"""
        assert hasattr(CommandStatus, 'UNAUTHORIZED')
        assert CommandStatus.UNAUTHORIZED.value == "unauthorized"

    def test_server_auth_disabled_by_default(self):
        """Test authentication is disabled by default"""
        server = CommandServer()
        assert server.auth_enabled is False

    def test_server_with_token_file_enables_auth(self):
        """Test authentication is enabled with token file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            from pathlib import Path
            from auth import TokenStore

            token_path = Path(tmpdir) / "test.token"
            store = TokenStore(token_path)
            store.generate_and_save()

            server = CommandServer(token_file=token_path)

            assert server.auth_enabled is True

    def test_execute_command_without_auth_when_disabled(self):
        """Test commands work without auth when auth is disabled"""
        server = CommandServer()

        result = server._execute_command("ping")

        assert result.status == CommandStatus.SUCCESS
        assert result.message == "pong"

    def test_execute_command_without_auth_when_required(self):
        """Test commands fail without auth when auth is required"""
        with tempfile.TemporaryDirectory() as tmpdir:
            from pathlib import Path
            from auth import TokenStore

            token_path = Path(tmpdir) / "test.token"
            store = TokenStore(token_path)
            store.generate_and_save()

            server = CommandServer(token_file=token_path)

            result = server._execute_command("ping")

            assert result.status == CommandStatus.UNAUTHORIZED
            assert "Authentication required" in result.message

    def test_execute_command_with_valid_token(self):
        """Test commands succeed with valid token"""
        with tempfile.TemporaryDirectory() as tmpdir:
            from pathlib import Path
            from auth import TokenStore

            token_path = Path(tmpdir) / "test.token"
            store = TokenStore(token_path)
            token = store.generate_and_save()

            server = CommandServer(token_file=token_path)

            result = server._execute_command(f"AUTH {token} ping")

            assert result.status == CommandStatus.SUCCESS
            assert result.message == "pong"

    def test_execute_command_with_invalid_token(self):
        """Test commands fail with invalid token"""
        with tempfile.TemporaryDirectory() as tmpdir:
            from pathlib import Path
            from auth import TokenStore

            token_path = Path(tmpdir) / "test.token"
            store = TokenStore(token_path)
            store.generate_and_save()

            server = CommandServer(token_file=token_path)

            result = server._execute_command("AUTH wrong_token ping")

            assert result.status == CommandStatus.UNAUTHORIZED
            assert "Invalid" in result.message

    def test_execute_command_with_token_and_args(self):
        """Test authenticated commands with arguments work"""
        with tempfile.TemporaryDirectory() as tmpdir:
            from pathlib import Path
            from auth import TokenStore

            token_path = Path(tmpdir) / "test.token"
            store = TokenStore(token_path)
            token = store.generate_and_save()

            server = CommandServer(token_file=token_path)

            # Register a test handler
            test_args = []
            def test_handler(args):
                test_args.extend(args)
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message="OK",
                    data={"args": args},
                )
            server.register_handler("test", test_handler)

            result = server._execute_command(f"AUTH {token} test arg1 arg2")

            assert result.status == CommandStatus.SUCCESS
            assert test_args == ["arg1", "arg2"]


class TestSendCommandWithToken:
    """Tests for send_command with authentication token"""

    @pytest.fixture
    def temp_socket_path(self):
        """Create a temporary socket path"""
        fd, path = tempfile.mkstemp(suffix=".sock")
        os.close(fd)
        os.unlink(path)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.fixture
    async def auth_server(self, temp_socket_path):
        """Create a server with authentication enabled"""
        from pathlib import Path
        from auth import TokenStore

        # Create token in temporary directory
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "test.token"
            store = TokenStore(token_path)
            token = store.generate_and_save()

            server = CommandServer(socket_path=temp_socket_path, token_file=token_path)
            started = server.start()
            assert started, "Server failed to start"
            await asyncio.sleep(0.1)

            yield server, temp_socket_path, token

            server.stop()
            await asyncio.sleep(0.05)

    async def test_send_command_with_token(self, auth_server):
        """Test sending command with authentication token"""
        server, socket_path, token = auth_server

        result = await asyncio.to_thread(send_command, "ping", socket_path=socket_path, token=token)

        assert result.status == CommandStatus.SUCCESS
        assert result.message == "pong"

    async def test_send_command_without_token_fails(self, auth_server):
        """Test sending command without token fails when auth required"""
        server, socket_path, token = auth_server

        result = await asyncio.to_thread(send_command, "ping", socket_path=socket_path)

        assert result.status == CommandStatus.UNAUTHORIZED

    async def test_send_command_with_wrong_token_fails(self, auth_server):
        """Test sending command with wrong token fails"""
        server, socket_path, token = auth_server

        result = await asyncio.to_thread(send_command, "ping", socket_path=socket_path, token="wrong_token")

        assert result.status == CommandStatus.UNAUTHORIZED


# =============================================================================
# RequestEntry Tests
# =============================================================================

class TestRequestEntry:
    """Tests for RequestEntry dataclass"""

    def test_creation_with_defaults(self):
        """Test creating a RequestEntry with default values"""
        entry = RequestEntry(token="abc123", command="ping")
        assert entry.token == "abc123"
        assert entry.command == "ping"
        assert entry.status == "active"
        assert entry.result is None
        assert entry.completed_at is None

    def test_timestamp_present(self):
        """Test that created_at timestamp is set automatically"""
        before = time.time()
        entry = RequestEntry(token="abc123", command="ping")
        after = time.time()
        assert before <= entry.created_at <= after


# =============================================================================
# RequestQueue Tests
# =============================================================================

class TestRequestQueue:
    """Tests for RequestQueue"""

    def test_enqueue_unique_token_succeeds(self):
        """Test enqueue with unique token returns None (success)"""
        queue = RequestQueue()
        result = queue.try_enqueue("token1", "ping")
        assert result is None

    def test_duplicate_active_token_returns_suggestion(self):
        """Test duplicate active token returns a suggested alternative"""
        queue = RequestQueue()
        queue.try_enqueue("token1", "ping")
        suggested = queue.try_enqueue("token1", "status")
        assert suggested is not None
        assert isinstance(suggested, str)
        assert suggested != "token1"

    def test_duplicate_completed_token_returns_suggestion(self):
        """Test duplicate token in completed queue returns suggestion"""
        queue = RequestQueue()
        queue.try_enqueue("token1", "ping")
        queue.complete("token1")
        suggested = queue.try_enqueue("token1", "status")
        assert suggested is not None
        assert suggested != "token1"

    def test_complete_moves_to_completed(self):
        """Test complete moves entry from active to completed"""
        queue = RequestQueue()
        queue.try_enqueue("token1", "ping")
        result = CommandResult(status=CommandStatus.SUCCESS, message="pong")
        queue.complete("token1", result)
        # token1 should no longer be active but still in completed
        assert queue.size == 1

    def test_max_completed_retained(self):
        """Test only MAX_COMPLETED entries retained in completed queue"""
        queue = RequestQueue()
        for i in range(12):
            token = f"token_{i}"
            queue.try_enqueue(token, "ping")
            queue.complete(token)
        # Should have 8 completed (MAX_COMPLETED), 0 active
        assert queue.size == RequestQueue.MAX_COMPLETED

    def test_evicted_token_can_be_reused(self):
        """Test that evicted tokens can be reused"""
        queue = RequestQueue()
        # Fill up completed queue and evict token_0
        for i in range(RequestQueue.MAX_COMPLETED + 1):
            token = f"token_{i}"
            queue.try_enqueue(token, "ping")
            queue.complete(token)
        # token_0 was evicted, should be reusable
        result = queue.try_enqueue("token_0", "ping")
        assert result is None  # Success

    def test_generated_tokens_are_unique(self):
        """Test generate_token produces unique tokens"""
        queue = RequestQueue()
        tokens = {queue.generate_token() for _ in range(50)}
        assert len(tokens) == 50

    def test_thread_safety(self):
        """Test concurrent enqueue/complete operations"""
        queue = RequestQueue()
        errors = []

        def worker(worker_id):
            try:
                for i in range(20):
                    token = f"w{worker_id}_t{i}"
                    result = queue.try_enqueue(token, "ping")
                    if result is not None:
                        errors.append(f"Unexpected duplicate: {token}")
                    queue.complete(token)
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)

        assert len(errors) == 0, f"Thread safety errors: {errors}"


# =============================================================================
# Request Token Parsing Tests
# =============================================================================

class TestRequestTokenParsing:
    """Tests for _parse_request_token"""

    def setup_method(self):
        self.server = CommandServer()

    def test_req_token_command_extracted(self):
        """Test REQ token command extracts token and remaining"""
        token, remaining = self.server._parse_request_token("REQ mytoken ping")
        assert token == "mytoken"
        assert remaining == "ping"

    def test_case_insensitive_req(self):
        """Test req prefix is case-insensitive"""
        token, remaining = self.server._parse_request_token("req mytoken ping")
        assert token == "mytoken"
        assert remaining == "ping"

    def test_req_with_auth_preserved(self):
        """Test REQ token AUTH authtoken command strips REQ, preserves AUTH"""
        token, remaining = self.server._parse_request_token(
            "REQ mytoken AUTH secret123 ping"
        )
        assert token == "mytoken"
        assert remaining == "AUTH secret123 ping"

    def test_no_req_generates_token(self):
        """Test no REQ prefix generates a token and preserves command"""
        token, remaining = self.server._parse_request_token("ping")
        assert isinstance(token, str)
        assert len(token) > 0
        assert remaining == "ping"

    def test_no_req_with_auth_preserved(self):
        """Test no REQ with AUTH generates token and preserves AUTH line"""
        token, remaining = self.server._parse_request_token("AUTH secret123 ping")
        assert isinstance(token, str)
        assert len(token) > 0
        assert remaining == "AUTH secret123 ping"

    def test_req_with_multi_word_args(self):
        """Test REQ with multi-word arguments"""
        token, remaining = self.server._parse_request_token(
            "REQ mytoken sell SPY 100 --confirm"
        )
        assert token == "mytoken"
        assert remaining == "sell SPY 100 --confirm"

    def test_req_with_token_only(self):
        """Test REQ with token but no remaining command"""
        token, remaining = self.server._parse_request_token("REQ mytoken")
        assert token == "mytoken"
        assert remaining == ""


# =============================================================================
# Request Token Integration Tests
# =============================================================================

class TestRequestTokenIntegration:
    """Integration tests for request tokens with actual socket communication"""

    @pytest.fixture
    def temp_socket_path(self):
        """Create a temporary socket path"""
        fd, path = tempfile.mkstemp(suffix=".sock")
        os.close(fd)
        os.unlink(path)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.fixture
    async def running_server(self, temp_socket_path):
        """Create and start a server, yield it, then stop"""
        server = CommandServer(socket_path=temp_socket_path)
        server.register_handler("test", lambda args: CommandResult(
            status=CommandStatus.SUCCESS,
            message=f"args={args}",
        ))
        started = server.start()
        assert started
        await asyncio.sleep(0.1)
        yield server, temp_socket_path
        server.stop()
        await asyncio.sleep(0.05)

    async def test_client_provided_token_in_response(self, running_server):
        """Test client-provided token appears in response"""
        server, socket_path = running_server
        result = await asyncio.to_thread(send_command, "ping", socket_path=socket_path, request_token="my-req-1")
        assert result.request_token == "my-req-1"
        assert result.status == CommandStatus.SUCCESS

    async def test_server_generates_token_when_none_provided(self, running_server):
        """Test server generates token when client doesn't provide one"""
        server, socket_path = running_server
        result = await asyncio.to_thread(send_command, "ping", socket_path=socket_path)
        assert result.request_token is not None
        assert len(result.request_token) > 0

    async def test_duplicate_token_returns_error(self, running_server):
        """Test duplicate token returns error with suggested alternative"""
        server, socket_path = running_server

        # Send first request — it will complete before the second arrives
        # so we need to use a token that lands in the completed queue
        result1 = await asyncio.to_thread(send_command, "ping", socket_path=socket_path, request_token="dup-token")
        assert result1.status == CommandStatus.SUCCESS

        # Send second request with same token — should fail (token in completed queue)
        result2 = await asyncio.to_thread(send_command, "ping", socket_path=socket_path, request_token="dup-token")
        assert result2.status == CommandStatus.ERROR
        assert "Duplicate" in result2.message
        assert "suggested_token" in result2.data
