"""
Unit tests for ibctl.py

Tests the command-line client for IB Portfolio Rebalancer.
"""

import json
import pytest
from unittest.mock import MagicMock, patch


# =============================================================================
# format_result Tests
# =============================================================================

class TestFormatResult:
    """Tests for format_result function"""

    def test_format_result_success(self):
        """Test format_result with success status"""
        from ibctl import format_result
        from command_server import CommandStatus, CommandResult

        result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="Operation completed",
        )

        with patch('builtins.print') as mock_print:
            format_result(result)

        # Check that [OK] is printed
        call_args = str(mock_print.call_args)
        assert "[OK]" in call_args

    def test_format_result_error(self):
        """Test format_result with error status"""
        from ibctl import format_result
        from command_server import CommandStatus, CommandResult

        result = CommandResult(
            status=CommandStatus.ERROR,
            message="Something went wrong",
        )

        with patch('builtins.print') as mock_print:
            format_result(result)

        call_args = str(mock_print.call_args)
        assert "[ERROR]" in call_args

    def test_format_result_pending(self):
        """Test format_result with pending status"""
        from ibctl import format_result
        from command_server import CommandStatus, CommandResult

        result = CommandResult(
            status=CommandStatus.PENDING,
            message="In progress",
        )

        with patch('builtins.print') as mock_print:
            format_result(result)

        call_args = str(mock_print.call_args)
        assert "[PENDING]" in call_args

    def test_format_result_verbose_shows_data(self):
        """Test format_result in verbose mode shows data"""
        from ibctl import format_result
        from command_server import CommandStatus, CommandResult

        result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="OK",
            data={"key": "value"},
        )

        with patch('builtins.print') as mock_print:
            format_result(result, verbose=True)

        # Should have multiple print calls, one for data
        assert mock_print.call_count >= 2

    def test_format_result_positions_data(self):
        """Test format_result formats positions data"""
        from ibctl import format_result
        from command_server import CommandStatus, CommandResult

        result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="2 positions",
            data={
                "positions": [
                    {
                        "symbol": "SPY",
                        "quantity": 100,
                        "price": 450.0,
                        "value": 45000.0,
                        "pnl": 500.0,
                        "allocation": 60.0,
                    },
                    {
                        "symbol": "BND",
                        "quantity": 200,
                        "price": 75.0,
                        "value": 15000.0,
                        "pnl": -100.0,
                        "allocation": 40.0,
                    }
                ]
            },
        )

        with patch('builtins.print') as mock_print:
            format_result(result, verbose=False)

        # Should print position table
        calls = [str(call) for call in mock_print.call_args_list]
        assert any("SPY" in c for c in calls)
        assert any("BND" in c for c in calls)

    def test_format_result_empty_positions(self):
        """Test format_result with empty positions"""
        from ibctl import format_result
        from command_server import CommandStatus, CommandResult

        result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="0 positions",
            data={"positions": []},
        )

        with patch('builtins.print') as mock_print:
            format_result(result, verbose=False)

        # Should not print table for empty positions
        assert mock_print.call_count == 1


# =============================================================================
# main() Function Tests
# =============================================================================

class TestIbctlMain:
    """Tests for ibctl main() function"""

    def test_main_no_command_shows_help(self):
        """Test main with no command shows help"""
        from ibctl import main

        with patch('sys.argv', ['ibctl']), \
             patch('argparse.ArgumentParser.print_help') as mock_help, \
             patch('sys.exit'):
            main()

        mock_help.assert_called_once()

    def test_main_sends_command(self):
        """Test main sends command to server"""
        from ibctl import main
        from command_server import CommandStatus, CommandResult

        mock_result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="pong",
        )

        with patch('sys.argv', ['ibctl', 'ping']), \
             patch('ibctl.send_command', return_value=mock_result) as mock_send, \
             patch('builtins.print'):
            main()

        mock_send.assert_called_once()
        call_kwargs = mock_send.call_args
        assert "ping" in str(call_kwargs)

    def test_main_sends_command_with_args(self):
        """Test main sends command with arguments"""
        from ibctl import main
        from command_server import CommandStatus, CommandResult

        mock_result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="Would sell...",
        )

        with patch('sys.argv', ['ibctl', 'sell', 'SPY', '10']), \
             patch('ibctl.send_command', return_value=mock_result) as mock_send, \
             patch('builtins.print'):
            main()

        call_args = mock_send.call_args
        assert "sell SPY 10" in str(call_args)

    def test_main_json_output(self):
        """Test main with --json flag outputs JSON"""
        from ibctl import main
        from command_server import CommandStatus, CommandResult

        mock_result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="OK",
            data={"test": "value"},
        )

        with patch('sys.argv', ['ibctl', '--json', 'ping']), \
             patch('ibctl.send_command', return_value=mock_result), \
             patch('builtins.print') as mock_print:
            main()

        # Should print JSON
        call_args = str(mock_print.call_args)
        assert "status" in call_args or "test" in call_args

    def test_main_custom_socket(self):
        """Test main with custom socket path"""
        from ibctl import main
        from command_server import CommandStatus, CommandResult

        mock_result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="pong",
        )

        with patch('sys.argv', ['ibctl', '-s', '/custom/path.sock', 'ping']), \
             patch('ibctl.send_command', return_value=mock_result) as mock_send, \
             patch('builtins.print'):
            main()

        call_kwargs = mock_send.call_args
        assert "/custom/path.sock" in str(call_kwargs)

    def test_main_custom_timeout(self):
        """Test main with custom timeout"""
        from ibctl import main
        from command_server import CommandStatus, CommandResult

        mock_result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="pong",
        )

        with patch('sys.argv', ['ibctl', '-t', '30', 'ping']), \
             patch('ibctl.send_command', return_value=mock_result) as mock_send, \
             patch('builtins.print'):
            main()

        call_kwargs = mock_send.call_args
        assert "timeout=30" in str(call_kwargs)

    def test_main_error_exits_with_code_1(self):
        """Test main exits with code 1 on error"""
        from ibctl import main
        from command_server import CommandStatus, CommandResult

        mock_result = CommandResult(
            status=CommandStatus.ERROR,
            message="Command failed",
        )

        with patch('sys.argv', ['ibctl', 'unknown']), \
             patch('ibctl.send_command', return_value=mock_result), \
             patch('builtins.print'), \
             patch('sys.exit') as mock_exit:
            main()

        mock_exit.assert_called_once_with(1)

    def test_main_success_no_exit_code(self):
        """Test main doesn't exit with error code on success"""
        from ibctl import main
        from command_server import CommandStatus, CommandResult

        mock_result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="OK",
        )

        with patch('sys.argv', ['ibctl', 'ping']), \
             patch('ibctl.send_command', return_value=mock_result), \
             patch('builtins.print'), \
             patch('sys.exit') as mock_exit:
            main()

        # Should not call sys.exit for success
        mock_exit.assert_not_called()

    def test_main_verbose_flag(self):
        """Test main with verbose flag"""
        from ibctl import main
        from command_server import CommandStatus, CommandResult

        mock_result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="OK",
            data={"detail": "info"},
        )

        with patch('sys.argv', ['ibctl', '-v', 'status']), \
             patch('ibctl.send_command', return_value=mock_result), \
             patch('ibctl.format_result') as mock_format:
            main()

        # format_result should be called with verbose=True
        call_args = mock_format.call_args
        assert call_args[1].get('verbose') is True


# =============================================================================
# Command Building Tests
# =============================================================================

class TestCommandBuilding:
    """Tests for command string building"""

    def test_multiple_word_command(self):
        """Test command with multiple words"""
        from ibctl import main
        from command_server import CommandStatus, CommandResult

        mock_result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="OK",
        )

        with patch('sys.argv', ['ibctl', 'liquidate', 'SPY']), \
             patch('ibctl.send_command', return_value=mock_result) as mock_send, \
             patch('builtins.print'):
            main()

        # Command should be joined with spaces
        call_args = mock_send.call_args
        assert "liquidate SPY" in str(call_args)

    def test_help_command(self):
        """Test help command"""
        from ibctl import main
        from command_server import CommandStatus, CommandResult

        mock_result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="Available commands...",
            data={"commands": ["help", "ping", "status"]},
        )

        with patch('sys.argv', ['ibctl', 'help']), \
             patch('ibctl.send_command', return_value=mock_result) as mock_send, \
             patch('builtins.print'):
            main()

        call_args = mock_send.call_args
        assert "help" in str(call_args)


# =============================================================================
# Integration-style Tests
# =============================================================================

class TestIbctlIntegration:
    """Integration-style tests for ibctl"""

    def test_status_command_flow(self):
        """Test full status command flow"""
        from ibctl import main
        from command_server import CommandStatus, CommandResult

        mock_result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="Portfolio: $100,000.00 (5 positions, P&L: $1,500.00)",
            data={
                "total_value": 100000.0,
                "total_pnl": 1500.0,
                "position_count": 5,
                "connected": True,
            },
        )

        with patch('sys.argv', ['ibctl', 'status']), \
             patch('ibctl.send_command', return_value=mock_result), \
             patch('builtins.print') as mock_print:
            main()

        call_args = str(mock_print.call_args)
        assert "100,000" in call_args or "OK" in call_args

    def test_sell_command_flow(self):
        """Test full sell command flow"""
        from ibctl import main
        from command_server import CommandStatus, CommandResult

        mock_result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="Would sell 10 SPY (~$4,500.00). Use --confirm to execute.",
            data={"dry_run": True, "symbol": "SPY", "quantity": 10},
        )

        with patch('sys.argv', ['ibctl', 'sell', 'SPY', '10']), \
             patch('ibctl.send_command', return_value=mock_result), \
             patch('builtins.print') as mock_print:
            main()

        call_args = str(mock_print.call_args)
        assert "SPY" in call_args or "OK" in call_args

    def test_stop_command_flow(self):
        """Test stop command flow"""
        from ibctl import main
        from command_server import CommandStatus, CommandResult

        mock_result = CommandResult(
            status=CommandStatus.SUCCESS,
            message="Shutdown initiated",
        )

        with patch('sys.argv', ['ibctl', 'stop']), \
             patch('ibctl.send_command', return_value=mock_result), \
             patch('builtins.print') as mock_print:
            main()

        call_args = str(mock_print.call_args)
        assert "Shutdown" in call_args or "OK" in call_args
