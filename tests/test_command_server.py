"""
Unit tests for command_server.py

Tests CommandResult, CommandServer command handling, and socket communication.
"""

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
    def running_server(self, temp_socket_path):
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

        # Give server time to start listening
        time.sleep(0.1)

        yield server, temp_socket_path

        server.stop()

    def test_server_start_stop(self, temp_socket_path):
        """Test server starts and stops cleanly"""
        server = CommandServer(socket_path=temp_socket_path)

        assert server.start() is True
        assert os.path.exists(temp_socket_path)

        server.stop()
        # Socket file should be cleaned up
        assert not os.path.exists(temp_socket_path)

    def test_server_socket_permissions(self, temp_socket_path):
        """Test socket has correct permissions (owner only)"""
        server = CommandServer(socket_path=temp_socket_path)
        server.start()

        mode = os.stat(temp_socket_path).st_mode & 0o777
        assert mode == 0o600, f"Socket permissions should be 0600, got {oct(mode)}"

        server.stop()

    def test_send_command_ping(self, running_server):
        """Test sending ping command via socket"""
        server, socket_path = running_server

        result = send_command("ping", socket_path=socket_path)

        assert result.status == CommandStatus.SUCCESS
        assert result.message == "pong"

    def test_send_command_help(self, running_server):
        """Test sending help command via socket"""
        server, socket_path = running_server

        result = send_command("help", socket_path=socket_path)

        assert result.status == CommandStatus.SUCCESS
        assert "test" in result.message  # Our registered handler
        assert "commands" in result.data

    def test_send_command_custom(self, running_server):
        """Test sending custom command via socket"""
        server, socket_path = running_server

        result = send_command("test arg1 arg2", socket_path=socket_path)

        assert result.status == CommandStatus.SUCCESS
        assert result.data["args"] == ["arg1", "arg2"]

    def test_send_command_unknown(self, running_server):
        """Test sending unknown command"""
        server, socket_path = running_server

        result = send_command("nonexistent", socket_path=socket_path)

        assert result.status == CommandStatus.ERROR
        assert "Unknown command" in result.message

    def test_send_command_server_not_running(self, temp_socket_path):
        """Test sending command when server is not running"""
        result = send_command("ping", socket_path=temp_socket_path)

        assert result.status == CommandStatus.ERROR
        assert "not running" in result.message.lower() or "not found" in result.message.lower()

    def test_multiple_clients(self, running_server):
        """Test multiple clients can connect"""
        server, socket_path = running_server

        results = []

        def send_and_store():
            result = send_command("ping", socket_path=socket_path)
            results.append(result)

        threads = [threading.Thread(target=send_and_store) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)

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
    def auth_server(self, temp_socket_path):
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
            time.sleep(0.1)

            yield server, temp_socket_path, token

            server.stop()

    def test_send_command_with_token(self, auth_server):
        """Test sending command with authentication token"""
        server, socket_path, token = auth_server

        result = send_command("ping", socket_path=socket_path, token=token)

        assert result.status == CommandStatus.SUCCESS
        assert result.message == "pong"

    def test_send_command_without_token_fails(self, auth_server):
        """Test sending command without token fails when auth required"""
        server, socket_path, token = auth_server

        result = send_command("ping", socket_path=socket_path)

        assert result.status == CommandStatus.UNAUTHORIZED

    def test_send_command_with_wrong_token_fails(self, auth_server):
        """Test sending command with wrong token fails"""
        server, socket_path, token = auth_server

        result = send_command("ping", socket_path=socket_path, token="wrong_token")

        assert result.status == CommandStatus.UNAUTHORIZED
