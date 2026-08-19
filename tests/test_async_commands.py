"""
Unit tests for async command interface

Tests the token request/query pattern for non-blocking command execution.
"""

import asyncio
import pytest
import time
from unittest.mock import MagicMock

from command_server import (
    CommandStatus,
    CommandResult,
    CommandServer,
    RequestQueue,
)


class TestRequestQueue:
    """Tests for RequestQueue async functionality"""

    def test_complete_keeps_in_active(self):
        """Test complete marks as completed but keeps in active queue"""
        queue = RequestQueue()
        queue.try_enqueue("token1", "ping")
        result = CommandResult(status=CommandStatus.SUCCESS, message="pong")
        
        queue.complete("token1", result)
        
        # Should still be in active (not moved yet)
        status = queue.get_status("token1")
        assert status == "completed"

    def test_retrieve_result_moves_to_completed(self):
        """Test retrieve_result moves entry from active to completed"""
        queue = RequestQueue()
        queue.try_enqueue("token1", "ping")
        result = CommandResult(status=CommandStatus.SUCCESS, message="pong")
        queue.complete("token1", result)
        
        entry = queue.retrieve_result("token1")
        
        assert entry is not None
        assert entry.token == "token1"
        assert entry.result == result
        # Should now be in completed queue
        assert queue.get_status("token1") == "completed"

    def test_retrieve_result_active_returns_none(self):
        """Test retrieve_result returns None for active requests"""
        queue = RequestQueue()
        queue.try_enqueue("token1", "ping")
        
        entry = queue.retrieve_result("token1")
        
        assert entry is None

    def test_retrieve_result_not_found(self):
        """Test retrieve_result returns None for unknown token"""
        queue = RequestQueue()
        
        entry = queue.retrieve_result("unknown")
        
        assert entry is None

    def test_get_status_active(self):
        """Test get_status returns 'active' for active requests"""
        queue = RequestQueue()
        queue.try_enqueue("token1", "ping")
        
        status = queue.get_status("token1")
        
        assert status == "active"

    def test_get_status_completed(self):
        """Test get_status returns 'completed' for completed requests"""
        queue = RequestQueue()
        queue.try_enqueue("token1", "ping")
        queue.complete("token1")
        
        status = queue.get_status("token1")
        
        assert status == "completed"

    def test_get_status_not_found(self):
        """Test get_status returns None for unknown token"""
        queue = RequestQueue()
        
        status = queue.get_status("unknown")
        
        assert status is None


class TestAsyncCommandHandlers:
    """Tests for async command handlers"""

    def test_request_token_handler(self):
        """Test request_token handler generates unique token"""
        server = CommandServer()
        
        result = server._handle_request_token([])
        
        assert result.status == CommandStatus.SUCCESS
        assert "token" in result.data
        assert len(result.data["token"]) > 0

    def test_request_token_generates_unique(self):
        """Test request_token generates different tokens"""
        server = CommandServer()
        
        result1 = server._handle_request_token([])
        result2 = server._handle_request_token([])
        
        assert result1.data["token"] != result2.data["token"]

    def test_query_result_no_args(self):
        """Test query_result with no arguments returns error"""
        server = CommandServer()
        
        result = server._handle_query_result([])
        
        assert result.status == CommandStatus.ERROR
        assert "Usage" in result.message

    def test_query_result_not_found(self):
        """Test query_result for unknown token returns error"""
        server = CommandServer()
        
        result = server._handle_query_result(["unknown_token"])
        
        assert result.status == CommandStatus.ERROR
        assert "No result found" in result.message

    def test_query_result_still_active(self):
        """Test query_result for active request returns PENDING"""
        server = CommandServer()
        # Manually enqueue a request
        server._request_queue.try_enqueue("token1", "ping")
        
        result = server._handle_query_result(["token1"])
        
        assert result.status == CommandStatus.PENDING
        assert "still processing" in result.message

    def test_query_result_completed(self):
        """Test query_result for completed request returns result"""
        server = CommandServer()
        # Manually enqueue and complete a request
        server._request_queue.try_enqueue("token1", "ping")
        expected_result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="pong",
        )
        server._request_queue.complete("token1", expected_result)
        
        result = server._handle_query_result(["token1"])
        
        assert result.status == CommandStatus.SUCCESS
        assert result.message == "pong"


class TestAsyncCommandExecution:
    """Integration tests for async command execution"""

    @pytest.fixture
    def temp_socket_path(self, tmp_path):
        """Create a temporary socket path"""
        return str(tmp_path / "test.sock")

    @pytest.fixture
    async def running_server(self, temp_socket_path):
        """Create and start a server"""
        server = CommandServer(socket_path=temp_socket_path)
        
        # Add a slow test handler
        def slow_handler(args):
            time.sleep(0.2)  # Simulate slow operation
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message="Slow operation completed",
                data={"args": args},
            )
        
        server.register_handler("slow", slow_handler)
        
        started = server.start()
        assert started
        await asyncio.sleep(0.1)
        
        yield server, temp_socket_path
        
        server.stop()
        await asyncio.sleep(0.05)

    async def test_all_commands_execute_synchronously(self, running_server):
        """Test that all commands execute synchronously (backward compatible)"""
        from command_server import send_command
        server, socket_path = running_server
        
        # All commands return results immediately
        result = await asyncio.to_thread(send_command, "ping", socket_path)
        assert result.status == CommandStatus.SUCCESS
        assert result.message == "pong"
        
        result = await asyncio.to_thread(send_command, "help", socket_path)
        assert result.status == CommandStatus.SUCCESS
        
        # Even slow commands complete synchronously
        result = await asyncio.to_thread(send_command, "slow arg1", socket_path)
        assert result.status == CommandStatus.SUCCESS
        assert "Slow operation completed" in result.message

    async def test_query_result_retrieves_archived(self, running_server):
        """Test that query_result can retrieve archived results"""
        from command_server import send_command
        server, socket_path = running_server
        
        # Execute command (completes immediately)
        result = await asyncio.to_thread(send_command, "slow arg1", socket_path)
        assert result.status == CommandStatus.SUCCESS
        token = result.request_token
        
        # Query the archived result
        archived = await asyncio.to_thread(
            send_command, f"query_result {token}", socket_path
        )
        
        # Should retrieve the same result from archive
        assert archived.status == CommandStatus.SUCCESS
        assert "Slow operation completed" in archived.message

    async def test_timestamp_tokens_are_unique(self, running_server):
        """Test that timestamp-based tokens are unique"""
        from command_server import send_command
        server, socket_path = running_server
        
        # Execute multiple commands rapidly
        tokens = []
        for _ in range(5):
            result = await asyncio.to_thread(send_command, "ping", socket_path)
            tokens.append(result.request_token)
        
        # All tokens should be unique
        assert len(tokens) == len(set(tokens))
        
        # Tokens should be timestamp+counter format: "timestamp.microseconds-counter"
        for token in tokens:
            assert "-" in token  # Has counter separator
            parts = token.split("-")
            assert len(parts) == 2
            float(parts[0])  # Timestamp part should be a float
            int(parts[1])    # Counter part should be an integer


class TestIbctlAsyncMode:
    """Tests for ibctl.py async mode"""

    def test_send_command_sync_mode(self):
        """Test send_command in sync mode (original behavior)"""
        # This would require a running server, so we'll just test the interface
        from ibctl import send_command
        
        # Verify function signature accepts async_mode parameter
        import inspect
        sig = inspect.signature(send_command)
        assert "async_mode" in sig.parameters
        assert "poll_interval" in sig.parameters

    def test_send_sync_command_helper(self):
        """Test _send_sync_command helper exists"""
        from ibctl import _send_sync_command
        
        # Verify function exists and has correct signature
        import inspect
        sig = inspect.signature(_send_sync_command)
        assert "command" in sig.parameters
        assert "socket_path" in sig.parameters
        assert "timeout" in sig.parameters
        assert "token" in sig.parameters
        assert "request_token" in sig.parameters


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
