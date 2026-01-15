"""
Unit tests for main.py

Tests ShutdownManager, CommandHandler, and main functions.
"""

import signal
import time
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from threading import Event


# Mock ibapi before importing main
@pytest.fixture(autouse=True)
def mock_ibapi():
    """Mock ibapi module for all tests"""
    with patch.dict('sys.modules', {
        'ibapi': MagicMock(),
        'ibapi.client': MagicMock(),
        'ibapi.wrapper': MagicMock(),
        'ibapi.common': MagicMock(),
        'ibapi.contract': MagicMock(),
        'ibapi.order': MagicMock(),
    }):
        yield


# =============================================================================
# ShutdownManager Tests
# =============================================================================

class TestShutdownManagerInit:
    """Tests for ShutdownManager initialization"""

    def test_init_defaults(self, mock_ibapi):
        """Test default initialization values"""
        from main import ShutdownManager

        mgr = ShutdownManager()

        assert mgr._shutdown_event is not None
        assert not mgr._shutdown_event.is_set()
        assert mgr._portfolio is None
        assert mgr._original_sigint is None
        assert mgr._original_sigterm is None
        assert mgr._sigint_count == 0
        assert mgr._first_sigint_time is None
        assert mgr._shutdown_initiated is False

    def test_constants(self, mock_ibapi):
        """Test class constants"""
        from main import ShutdownManager

        assert ShutdownManager.REQUIRED_SIGNALS == 3
        assert ShutdownManager.RESET_TIMEOUT == 10.0


class TestShutdownManagerProperties:
    """Tests for ShutdownManager properties"""

    @pytest.fixture
    def manager(self, mock_ibapi):
        """Create a fresh ShutdownManager for testing"""
        from main import ShutdownManager
        return ShutdownManager()

    def test_should_shutdown_false(self, manager):
        """Test should_shutdown when not shutdown"""
        assert manager.should_shutdown is False

    def test_should_shutdown_true(self, manager):
        """Test should_shutdown when shutdown requested"""
        manager._shutdown_event.set()
        assert manager.should_shutdown is True


class TestShutdownManagerPortfolioRegistration:
    """Tests for portfolio registration"""

    @pytest.fixture
    def manager(self, mock_ibapi):
        """Create a fresh ShutdownManager for testing"""
        from main import ShutdownManager
        return ShutdownManager()

    def test_register_portfolio(self, manager):
        """Test registering a portfolio"""
        mock_portfolio = MagicMock()
        manager.register_portfolio(mock_portfolio)

        assert manager._portfolio is mock_portfolio

    def test_register_portfolio_none(self, manager):
        """Test registering None"""
        manager.register_portfolio(None)

        assert manager._portfolio is None


class TestShutdownManagerSignalHandlers:
    """Tests for signal handler installation and restoration"""

    @pytest.fixture
    def manager(self, mock_ibapi):
        """Create a fresh ShutdownManager for testing"""
        from main import ShutdownManager
        return ShutdownManager()

    def test_install_handlers(self, manager):
        """Test signal handlers are installed"""
        with patch('signal.signal') as mock_signal, \
             patch('atexit.register') as mock_atexit, \
             patch('builtins.print'):
            # Return a mock handler
            mock_signal.return_value = MagicMock()

            manager.install_handlers()

            # Check signal.signal was called for SIGINT and SIGTERM
            calls = mock_signal.call_args_list
            signals_installed = [call[0][0] for call in calls]
            assert signal.SIGINT in signals_installed
            assert signal.SIGTERM in signals_installed

            # Check atexit was registered
            mock_atexit.assert_called_once()

    def test_restore_handlers(self, manager):
        """Test signal handlers are restored"""
        original_sigint = MagicMock()
        original_sigterm = MagicMock()
        manager._original_sigint = original_sigint
        manager._original_sigterm = original_sigterm

        with patch('signal.signal') as mock_signal:
            manager.restore_handlers()

            # Check handlers were restored
            calls = mock_signal.call_args_list
            assert len(calls) == 2

    def test_restore_handlers_when_none(self, manager):
        """Test restore_handlers when no handlers installed"""
        with patch('signal.signal') as mock_signal:
            manager.restore_handlers()

            # Should not call signal.signal if originals are None
            mock_signal.assert_not_called()


class TestShutdownManagerSignalHandler:
    """Tests for the actual signal handling logic"""

    @pytest.fixture
    def manager(self, mock_ibapi):
        """Create a fresh ShutdownManager for testing"""
        from main import ShutdownManager
        return ShutdownManager()

    def test_sigterm_immediate_shutdown(self, manager):
        """Test SIGTERM triggers immediate shutdown"""
        with patch.object(manager, '_initiate_shutdown') as mock_shutdown, \
             patch('builtins.print'):
            manager._signal_handler(signal.SIGTERM, None)

            mock_shutdown.assert_called_once()

    def test_first_sigint_increments_counter(self, manager):
        """Test first SIGINT increments counter"""
        with patch('builtins.print'):
            manager._signal_handler(signal.SIGINT, None)

            assert manager._sigint_count == 1
            assert manager._first_sigint_time is not None

    def test_second_sigint_increments_counter(self, manager):
        """Test second SIGINT increments counter"""
        with patch('builtins.print'):
            manager._signal_handler(signal.SIGINT, None)
            manager._signal_handler(signal.SIGINT, None)

            assert manager._sigint_count == 2

    def test_third_sigint_triggers_shutdown(self, manager):
        """Test third SIGINT triggers shutdown"""
        with patch.object(manager, '_initiate_shutdown') as mock_shutdown, \
             patch('builtins.print'):
            manager._signal_handler(signal.SIGINT, None)
            manager._signal_handler(signal.SIGINT, None)
            manager._signal_handler(signal.SIGINT, None)

            mock_shutdown.assert_called_once()

    def test_sigint_counter_resets_after_timeout(self, manager):
        """Test SIGINT counter resets after timeout"""
        with patch('time.time') as mock_time, \
             patch('builtins.print'):
            # First signal at time 0
            mock_time.return_value = 0.0
            manager._signal_handler(signal.SIGINT, None)
            assert manager._sigint_count == 1

            # Second signal after timeout (15 seconds later)
            mock_time.return_value = 15.0
            manager._signal_handler(signal.SIGINT, None)
            # Counter should have reset and then incremented
            assert manager._sigint_count == 1


class TestShutdownManagerInitiateShutdown:
    """Tests for shutdown initiation"""

    @pytest.fixture
    def manager(self, mock_ibapi):
        """Create a fresh ShutdownManager for testing"""
        from main import ShutdownManager
        return ShutdownManager()

    def test_initiate_shutdown_sets_event(self, manager):
        """Test _initiate_shutdown sets shutdown event"""
        with patch.object(manager, '_cleanup'):
            manager._initiate_shutdown()

            assert manager._shutdown_event.is_set()
            assert manager._shutdown_initiated is True

    def test_initiate_shutdown_calls_cleanup(self, manager):
        """Test _initiate_shutdown calls cleanup"""
        with patch.object(manager, '_cleanup') as mock_cleanup:
            manager._initiate_shutdown()

            mock_cleanup.assert_called_once()

    def test_second_initiate_shutdown_exits(self, manager):
        """Test second _initiate_shutdown forces exit"""
        manager._shutdown_initiated = True

        with patch('sys.exit') as mock_exit, \
             patch('builtins.print'):
            manager._initiate_shutdown()

            mock_exit.assert_called_once_with(1)


class TestShutdownManagerCleanup:
    """Tests for cleanup"""

    @pytest.fixture
    def manager(self, mock_ibapi):
        """Create a fresh ShutdownManager for testing"""
        from main import ShutdownManager
        return ShutdownManager()

    def test_cleanup_calls_portfolio_shutdown(self, manager):
        """Test _cleanup calls portfolio.shutdown()"""
        mock_portfolio = MagicMock()
        manager._portfolio = mock_portfolio

        manager._cleanup()

        mock_portfolio.shutdown.assert_called_once()

    def test_cleanup_handles_portfolio_exception(self, manager):
        """Test _cleanup handles exception from portfolio"""
        mock_portfolio = MagicMock()
        mock_portfolio.shutdown.side_effect = Exception("Shutdown error")
        manager._portfolio = mock_portfolio

        # Should not raise
        manager._cleanup()

    def test_cleanup_no_portfolio(self, manager):
        """Test _cleanup when no portfolio registered"""
        # Should not raise
        manager._cleanup()


class TestShutdownManagerWait:
    """Tests for wait methods"""

    @pytest.fixture
    def manager(self, mock_ibapi):
        """Create a fresh ShutdownManager for testing"""
        from main import ShutdownManager
        return ShutdownManager()

    def test_wait_returns_true_when_set(self, manager):
        """Test wait returns True when shutdown is set"""
        manager._shutdown_event.set()

        result = manager.wait(timeout=0.1)

        assert result is True

    def test_wait_returns_false_on_timeout(self, manager):
        """Test wait returns False on timeout"""
        result = manager.wait(timeout=0.01)

        assert result is False

    def test_wait_interruptible_exits_on_shutdown(self, manager):
        """Test wait_interruptible exits when shutdown is set"""
        import threading

        # Set shutdown after a short delay
        def set_shutdown():
            time.sleep(0.05)
            manager._shutdown_event.set()

        thread = threading.Thread(target=set_shutdown)
        thread.start()

        start = time.time()
        manager.wait_interruptible(duration=0, poll_interval=0.01)
        elapsed = time.time() - start

        thread.join()

        # Should have exited shortly after shutdown was set
        assert elapsed < 0.5

    def test_wait_interruptible_duration(self, manager):
        """Test wait_interruptible with duration"""
        start = time.time()
        manager.wait_interruptible(duration=0.1, poll_interval=0.01)
        elapsed = time.time() - start

        # Should have waited approximately the duration
        assert 0.08 < elapsed < 0.3


# =============================================================================
# CommandHandler Tests
# =============================================================================

class TestCommandHandlerInit:
    """Tests for CommandHandler initialization"""

    def test_init(self, mock_ibapi):
        """Test initialization"""
        from main import CommandHandler, ShutdownManager

        mock_portfolio = MagicMock()
        mock_shutdown = MagicMock()

        handler = CommandHandler(mock_portfolio, mock_shutdown)

        assert handler.portfolio is mock_portfolio
        assert handler.shutdown_mgr is mock_shutdown
        assert handler._liquidation_in_progress is False


class TestCommandHandlerRegistration:
    """Tests for command registration"""

    @pytest.fixture
    def handler(self, mock_ibapi):
        """Create CommandHandler for testing"""
        from main import CommandHandler

        mock_portfolio = MagicMock()
        mock_shutdown = MagicMock()
        return CommandHandler(mock_portfolio, mock_shutdown)

    def test_register_commands(self, handler):
        """Test all commands are registered"""
        mock_server = MagicMock()

        handler.register_commands(mock_server)

        # Check all handlers registered
        calls = mock_server.register_handler.call_args_list
        command_names = [call[0][0] for call in calls]

        assert "status" in command_names
        assert "positions" in command_names
        assert "liquidate" in command_names
        assert "stop" in command_names
        assert "shutdown" in command_names
        assert "sell" in command_names
        assert "buy" in command_names


class TestCommandHandlerStatus:
    """Tests for handle_status command"""

    @pytest.fixture
    def handler(self, mock_ibapi):
        """Create CommandHandler for testing"""
        from main import CommandHandler

        mock_portfolio = MagicMock()
        mock_portfolio.positions = []
        mock_portfolio.total_value = 100000.0
        mock_portfolio.total_pnl = 1500.0
        mock_portfolio.connected = True
        mock_portfolio.is_streaming = False
        mock_portfolio.is_bar_streaming = False
        mock_portfolio.get_account_summary.return_value = None

        mock_shutdown = MagicMock()
        return CommandHandler(mock_portfolio, mock_shutdown)

    def test_handle_status_success(self, handler):
        """Test handle_status returns success"""
        from command_server import CommandStatus

        result = handler.handle_status([])

        assert result.status == CommandStatus.SUCCESS
        assert "100,000" in result.message
        assert result.data["total_value"] == 100000.0

    def test_handle_status_with_account(self, handler):
        """Test handle_status includes account data"""
        from command_server import CommandStatus

        mock_account = MagicMock()
        mock_account.is_valid = True
        mock_account.account_id = "DU123456"
        mock_account.net_liquidation = 150000.0
        mock_account.available_funds = 50000.0
        mock_account.buying_power = 100000.0
        handler.portfolio.get_account_summary.return_value = mock_account

        result = handler.handle_status([])

        assert result.status == CommandStatus.SUCCESS
        assert result.data["account"]["account_id"] == "DU123456"

    def test_handle_status_exception(self, handler):
        """Test handle_status handles exception"""
        from command_server import CommandStatus

        handler.portfolio.positions = property(lambda self: exec('raise Exception("Test")'))
        type(handler.portfolio).positions = PropertyMock(side_effect=Exception("Test"))

        result = handler.handle_status([])

        assert result.status == CommandStatus.ERROR
        assert "Failed" in result.message


class TestCommandHandlerPositions:
    """Tests for handle_positions command"""

    @pytest.fixture
    def handler(self, mock_ibapi):
        """Create CommandHandler for testing"""
        from main import CommandHandler

        mock_pos = MagicMock()
        mock_pos.symbol = "SPY"
        mock_pos.quantity = 100
        mock_pos.current_price = 450.0
        mock_pos.market_value = 45000.0
        mock_pos.unrealized_pnl = 500.0
        mock_pos.allocation_pct = 45.0

        mock_portfolio = MagicMock()
        mock_portfolio.positions = [mock_pos]

        mock_shutdown = MagicMock()
        return CommandHandler(mock_portfolio, mock_shutdown)

    def test_handle_positions_success(self, handler):
        """Test handle_positions returns positions"""
        from command_server import CommandStatus

        result = handler.handle_positions([])

        assert result.status == CommandStatus.SUCCESS
        assert len(result.data["positions"]) == 1
        assert result.data["positions"][0]["symbol"] == "SPY"


class TestCommandHandlerLiquidate:
    """Tests for handle_liquidate command"""

    @pytest.fixture
    def handler(self, mock_ibapi):
        """Create CommandHandler for testing"""
        from main import CommandHandler

        mock_pos = MagicMock()
        mock_pos.symbol = "SPY"
        mock_pos.quantity = 100
        mock_pos.current_price = 450.0
        mock_pos.market_value = 45000.0
        mock_pos.contract = MagicMock()

        mock_portfolio = MagicMock()
        mock_portfolio.positions = [mock_pos]

        mock_shutdown = MagicMock()
        return CommandHandler(mock_portfolio, mock_shutdown)

    def test_handle_liquidate_dry_run(self, handler):
        """Test handle_liquidate in dry-run mode"""
        from command_server import CommandStatus

        result = handler.handle_liquidate([])

        assert result.status == CommandStatus.SUCCESS
        assert result.data["dry_run"] is True
        assert "--confirm" in result.message

    def test_handle_liquidate_confirm(self, handler):
        """Test handle_liquidate with confirm"""
        from command_server import CommandStatus

        handler.portfolio.place_market_order.return_value = 12345

        result = handler.handle_liquidate(["--confirm"])

        assert result.status == CommandStatus.SUCCESS
        assert "order" in result.message.lower()
        handler.portfolio.place_market_order.assert_called_once()

    def test_handle_liquidate_no_positions(self, handler):
        """Test handle_liquidate with no positions"""
        from command_server import CommandStatus

        handler.portfolio.positions = []

        result = handler.handle_liquidate([])

        assert result.status == CommandStatus.SUCCESS
        assert "No positions" in result.message

    def test_handle_liquidate_specific_symbol(self, handler):
        """Test handle_liquidate specific symbol"""
        from command_server import CommandStatus

        result = handler.handle_liquidate(["SPY"])

        assert result.status == CommandStatus.SUCCESS
        assert "SPY" in result.message

    def test_handle_liquidate_symbol_not_found(self, handler):
        """Test handle_liquidate with unknown symbol"""
        from command_server import CommandStatus

        result = handler.handle_liquidate(["UNKNOWN"])

        assert result.status == CommandStatus.ERROR
        assert "not found" in result.message.lower()

    def test_handle_liquidate_in_progress(self, handler):
        """Test handle_liquidate when already in progress"""
        from command_server import CommandStatus

        handler._liquidation_in_progress = True

        result = handler.handle_liquidate(["--confirm"])

        assert result.status == CommandStatus.ERROR
        assert "already in progress" in result.message


class TestCommandHandlerSell:
    """Tests for handle_sell command"""

    @pytest.fixture
    def handler(self, mock_ibapi):
        """Create CommandHandler for testing"""
        from main import CommandHandler

        mock_pos = MagicMock()
        mock_pos.symbol = "SPY"
        mock_pos.quantity = 100
        mock_pos.current_price = 450.0
        mock_pos.contract = MagicMock()

        mock_portfolio = MagicMock()
        mock_portfolio.get_position.return_value = mock_pos

        mock_shutdown = MagicMock()
        return CommandHandler(mock_portfolio, mock_shutdown)

    def test_handle_sell_dry_run(self, handler):
        """Test handle_sell in dry-run mode"""
        from command_server import CommandStatus

        result = handler.handle_sell(["SPY", "50"])

        assert result.status == CommandStatus.SUCCESS
        assert result.data["dry_run"] is True
        assert result.data["quantity"] == 50.0

    def test_handle_sell_confirm(self, handler):
        """Test handle_sell with confirm"""
        from command_server import CommandStatus

        handler.portfolio.place_market_order.return_value = 12345

        result = handler.handle_sell(["SPY", "50", "--confirm"])

        assert result.status == CommandStatus.SUCCESS
        handler.portfolio.place_market_order.assert_called_once()

    def test_handle_sell_all(self, handler):
        """Test handle_sell all shares"""
        from command_server import CommandStatus

        result = handler.handle_sell(["SPY", "all"])

        assert result.status == CommandStatus.SUCCESS
        assert result.data["quantity"] == 100

    def test_handle_sell_missing_args(self, handler):
        """Test handle_sell with missing arguments"""
        from command_server import CommandStatus

        result = handler.handle_sell(["SPY"])

        assert result.status == CommandStatus.ERROR
        assert "Usage" in result.message

    def test_handle_sell_invalid_quantity(self, handler):
        """Test handle_sell with invalid quantity"""
        from command_server import CommandStatus

        result = handler.handle_sell(["SPY", "abc"])

        assert result.status == CommandStatus.ERROR
        assert "Invalid quantity" in result.message

    def test_handle_sell_quantity_exceeds_position(self, handler):
        """Test handle_sell with quantity > position"""
        from command_server import CommandStatus

        result = handler.handle_sell(["SPY", "200"])

        assert result.status == CommandStatus.ERROR
        assert "Cannot sell" in result.message

    def test_handle_sell_no_position(self, handler):
        """Test handle_sell when position doesn't exist"""
        from command_server import CommandStatus

        handler.portfolio.get_position.return_value = None

        result = handler.handle_sell(["UNKNOWN", "10"])

        assert result.status == CommandStatus.ERROR
        assert "No position" in result.message


class TestCommandHandlerBuy:
    """Tests for handle_buy command"""

    @pytest.fixture
    def handler(self, mock_ibapi):
        """Create CommandHandler for testing"""
        from main import CommandHandler

        mock_pos = MagicMock()
        mock_pos.symbol = "SPY"
        mock_pos.quantity = 100
        mock_pos.current_price = 450.0
        mock_pos.contract = MagicMock()

        mock_portfolio = MagicMock()
        mock_portfolio.get_position.return_value = mock_pos

        mock_shutdown = MagicMock()
        return CommandHandler(mock_portfolio, mock_shutdown)

    def test_handle_buy_dry_run(self, handler):
        """Test handle_buy in dry-run mode"""
        from command_server import CommandStatus

        result = handler.handle_buy(["SPY", "10"])

        assert result.status == CommandStatus.SUCCESS
        assert result.data["dry_run"] is True
        assert result.data["quantity"] == 10.0

    def test_handle_buy_confirm(self, handler):
        """Test handle_buy with confirm"""
        from command_server import CommandStatus

        handler.portfolio.place_market_order.return_value = 12345

        result = handler.handle_buy(["SPY", "10", "--confirm"])

        assert result.status == CommandStatus.SUCCESS
        handler.portfolio.place_market_order.assert_called_once()

    def test_handle_buy_missing_args(self, handler):
        """Test handle_buy with missing arguments"""
        from command_server import CommandStatus

        result = handler.handle_buy(["SPY"])

        assert result.status == CommandStatus.ERROR
        assert "Usage" in result.message

    def test_handle_buy_invalid_quantity(self, handler):
        """Test handle_buy with invalid quantity"""
        from command_server import CommandStatus

        result = handler.handle_buy(["SPY", "abc"])

        assert result.status == CommandStatus.ERROR
        assert "Invalid quantity" in result.message

    def test_handle_buy_no_existing_position(self, handler):
        """Test handle_buy when no existing position"""
        from command_server import CommandStatus

        handler.portfolio.get_position.return_value = None

        result = handler.handle_buy(["UNKNOWN", "10"])

        assert result.status == CommandStatus.ERROR
        assert "No existing position" in result.message


class TestCommandHandlerStop:
    """Tests for handle_stop command"""

    @pytest.fixture
    def handler(self, mock_ibapi):
        """Create CommandHandler for testing"""
        from main import CommandHandler

        mock_portfolio = MagicMock()
        mock_shutdown = MagicMock()
        mock_shutdown._shutdown_event = Event()

        return CommandHandler(mock_portfolio, mock_shutdown)

    def test_handle_stop(self, handler):
        """Test handle_stop initiates shutdown"""
        from command_server import CommandStatus

        result = handler.handle_stop([])

        assert result.status == CommandStatus.SUCCESS
        assert "Shutdown" in result.message
        assert handler.shutdown_mgr._shutdown_event.is_set()


# =============================================================================
# Helper Function Tests
# =============================================================================

class TestShowPortfolio:
    """Tests for show_portfolio function"""

    def test_show_portfolio_no_positions(self, mock_ibapi):
        """Test show_portfolio with no positions"""
        from main import show_portfolio

        mock_portfolio = MagicMock()
        mock_portfolio.positions = []

        with patch('builtins.print') as mock_print:
            show_portfolio(mock_portfolio)

            # Should print "No positions found"
            calls = [str(call) for call in mock_print.call_args_list]
            assert any("No positions" in c for c in calls)

    def test_show_portfolio_with_positions(self, mock_ibapi):
        """Test show_portfolio with positions"""
        from main import show_portfolio
        from models import AssetType

        mock_pos = MagicMock()
        mock_pos.symbol = "SPY"
        mock_pos.asset_type = AssetType.ETF
        mock_pos.quantity = 100
        mock_pos.current_price = 450.0
        mock_pos.market_value = 45000.0
        mock_pos.unrealized_pnl = 500.0
        mock_pos.allocation_pct = 100.0

        mock_portfolio = MagicMock()
        mock_portfolio.positions = [mock_pos]
        mock_portfolio.total_value = 45000.0
        mock_portfolio.total_pnl = 500.0
        mock_portfolio.get_account_summary.return_value = None

        with patch('builtins.print') as mock_print:
            show_portfolio(mock_portfolio)

            # Should print position info
            calls = [str(call) for call in mock_print.call_args_list]
            assert any("SPY" in c for c in calls)


class TestShowTargets:
    """Tests for show_targets function"""

    def test_show_targets(self, mock_ibapi):
        """Test show_targets displays targets"""
        from main import show_targets
        from models import TargetAllocation

        targets = [
            TargetAllocation(symbol="VTI", target_pct=50.0),
            TargetAllocation(symbol="BND", target_pct=50.0),
        ]

        with patch('builtins.print') as mock_print:
            show_targets(targets)

            calls = [str(call) for call in mock_print.call_args_list]
            assert any("VTI" in c for c in calls)
            assert any("BND" in c for c in calls)


class TestParseArgs:
    """Tests for parse_args function"""

    def test_parse_args_defaults(self, mock_ibapi):
        """Test parse_args with no arguments"""
        from main import parse_args

        with patch('sys.argv', ['main.py']):
            args = parse_args()

            assert args.host == "127.0.0.1"
            assert args.port is None  # None means use default based on --live
            assert args.client_id == 1

    def test_parse_args_custom_port(self, mock_ibapi):
        """Test parse_args with custom port"""
        from main import parse_args

        with patch('sys.argv', ['main.py', '--port', '4002']):
            args = parse_args()

            assert args.port == 4002

    def test_parse_args_port_overrides_live(self, mock_ibapi):
        """Test explicit --port overrides --live"""
        from main import parse_args

        with patch('sys.argv', ['main.py', '--live', '--port', '4002']):
            args = parse_args()

            assert args.port == 4002
            assert args.live is True

    def test_parse_args_stream(self, mock_ibapi):
        """Test parse_args with --stream"""
        from main import parse_args

        with patch('sys.argv', ['main.py', '--stream']):
            args = parse_args()

            assert args.stream is True

    def test_parse_args_bars(self, mock_ibapi):
        """Test parse_args with --bars"""
        from main import parse_args

        with patch('sys.argv', ['main.py', '--bars']):
            args = parse_args()

            assert args.bars is True

    def test_parse_args_rebalance(self, mock_ibapi):
        """Test parse_args with --rebalance"""
        from main import parse_args

        with patch('sys.argv', ['main.py', '--rebalance']):
            args = parse_args()

            assert args.rebalance is True

    def test_parse_args_execute(self, mock_ibapi):
        """Test parse_args with --execute"""
        from main import parse_args

        with patch('sys.argv', ['main.py', '--rebalance', '--execute']):
            args = parse_args()

            assert args.execute is True

    def test_parse_args_threshold(self, mock_ibapi):
        """Test parse_args with --threshold"""
        from main import parse_args

        with patch('sys.argv', ['main.py', '--threshold', '3.0']):
            args = parse_args()

            assert args.threshold == 3.0

    def test_parse_args_no_server(self, mock_ibapi):
        """Test parse_args with --no-server"""
        from main import parse_args

        with patch('sys.argv', ['main.py', '--no-server']):
            args = parse_args()

            assert args.no_server is True

    def test_parse_args_verbose(self, mock_ibapi):
        """Test parse_args with --verbose"""
        from main import parse_args

        with patch('sys.argv', ['main.py', '-v']):
            args = parse_args()

            assert args.verbose is True

    def test_parse_args_quiet(self, mock_ibapi):
        """Test parse_args with --quiet"""
        from main import parse_args

        with patch('sys.argv', ['main.py', '-q']):
            args = parse_args()

            assert args.quiet is True

    def test_parse_args_live(self, mock_ibapi):
        """Test parse_args with --live connects to live port"""
        from main import parse_args, DEFAULT_PORT_LIVE

        with patch('sys.argv', ['main.py', '--live']):
            args = parse_args()

            assert args.live is True
            assert args.port is None  # Port determined in main() based on --live
            assert args.dry_run is True  # Still default until main() sets it

    def test_port_selection_paper_default(self, mock_ibapi):
        """Test default port is paper trading (7497)"""
        from main import DEFAULT_PORT_PAPER, DEFAULT_PORT_LIVE

        assert DEFAULT_PORT_PAPER == 7497
        assert DEFAULT_PORT_LIVE == 7496


# =============================================================================
# Stream Prices Function Tests
# =============================================================================

class TestStreamPrices:
    """Tests for stream_prices function"""

    def test_stream_prices_starts_streaming(self, mock_ibapi):
        """Test stream_prices starts streaming"""
        from main import stream_prices, shutdown_manager

        mock_portfolio = MagicMock()

        # Ensure shutdown manager is in clean state
        shutdown_manager._shutdown_event.clear()

        with patch.object(shutdown_manager, 'wait_interruptible', return_value=None):
            stream_prices(mock_portfolio, duration=0)

        mock_portfolio.start_streaming.assert_called_once()
        mock_portfolio.stop_streaming.assert_called_once()

    def test_stream_prices_with_duration(self, mock_ibapi):
        """Test stream_prices with duration"""
        from main import stream_prices, shutdown_manager

        mock_portfolio = MagicMock()
        mock_portfolio.get_position.return_value = None

        shutdown_manager._shutdown_event.clear()

        with patch.object(shutdown_manager, 'wait_interruptible') as mock_wait, \
             patch('builtins.print'):
            stream_prices(mock_portfolio, duration=10)

        mock_wait.assert_called_once_with(duration=10)


# =============================================================================
# Stream Bars Function Tests
# =============================================================================

class TestStreamBars:
    """Tests for stream_bars function"""

    def test_stream_bars_starts_streaming(self, mock_ibapi):
        """Test stream_bars starts bar streaming"""
        from main import stream_bars, shutdown_manager

        mock_portfolio = MagicMock()
        mock_portfolio.positions = []

        shutdown_manager._shutdown_event.clear()

        with patch.object(shutdown_manager, 'wait_interruptible', return_value=None), \
             patch('builtins.print'):
            stream_bars(mock_portfolio, duration=0)

        mock_portfolio.start_bar_streaming.assert_called_once()
        mock_portfolio.stop_bar_streaming.assert_called_once()


# =============================================================================
# Calculate Rebalance Function Tests
# =============================================================================

class TestCalculateRebalance:
    """Tests for calculate_rebalance function"""

    def test_calculate_rebalance_creates_rebalancer(self, mock_ibapi):
        """Test calculate_rebalance creates and uses rebalancer"""
        from main import calculate_rebalance
        from rebalancer import RebalanceConfig

        mock_portfolio = MagicMock()
        targets = []
        config = RebalanceConfig()

        with patch('main.Rebalancer') as MockRebalancer, \
             patch('builtins.print'):
            mock_rebalancer = MockRebalancer.return_value
            mock_result = MagicMock()
            mock_rebalancer.calculate.return_value = mock_result

            calculate_rebalance(mock_portfolio, targets, config)

            MockRebalancer.assert_called_once()
            mock_rebalancer.set_targets.assert_called_once_with(targets)
            mock_rebalancer.calculate.assert_called_once()
            mock_rebalancer.preview.assert_called_once()


# =============================================================================
# Execute Rebalance Function Tests
# =============================================================================

class TestExecuteRebalance:
    """Tests for execute_rebalance function"""

    def test_execute_rebalance_no_trades(self, mock_ibapi):
        """Test execute_rebalance with no trades"""
        from main import execute_rebalance
        from rebalancer import RebalanceConfig

        mock_portfolio = MagicMock()
        targets = []
        config = RebalanceConfig()

        with patch('main.Rebalancer') as MockRebalancer, \
             patch('builtins.print'), \
             patch('builtins.input', return_value='no'):
            mock_rebalancer = MockRebalancer.return_value
            mock_result = MagicMock()
            mock_result.actionable_trades = []
            mock_rebalancer.calculate.return_value = mock_result

            execute_rebalance(mock_portfolio, targets, config)

            mock_rebalancer.execute.assert_not_called()

    def test_execute_rebalance_user_confirms(self, mock_ibapi):
        """Test execute_rebalance when user confirms"""
        from main import execute_rebalance
        from rebalancer import RebalanceConfig

        mock_portfolio = MagicMock()
        targets = []
        config = RebalanceConfig()

        with patch('main.Rebalancer') as MockRebalancer, \
             patch('builtins.print'), \
             patch('builtins.input', return_value='yes'):
            mock_rebalancer = MockRebalancer.return_value
            mock_result = MagicMock()
            mock_result.actionable_trades = [MagicMock()]
            mock_result.trade_count = 1
            mock_rebalancer.calculate.return_value = mock_result

            execute_rebalance(mock_portfolio, targets, config)

            mock_rebalancer.execute.assert_called_once()

    def test_execute_rebalance_user_cancels(self, mock_ibapi):
        """Test execute_rebalance when user cancels"""
        from main import execute_rebalance
        from rebalancer import RebalanceConfig

        mock_portfolio = MagicMock()
        targets = []
        config = RebalanceConfig()

        with patch('main.Rebalancer') as MockRebalancer, \
             patch('builtins.print'), \
             patch('builtins.input', return_value='no'):
            mock_rebalancer = MockRebalancer.return_value
            mock_result = MagicMock()
            mock_result.actionable_trades = [MagicMock()]
            mock_result.trade_count = 1
            mock_rebalancer.calculate.return_value = mock_result

            execute_rebalance(mock_portfolio, targets, config)

            mock_rebalancer.execute.assert_not_called()


# =============================================================================
# Main Function Tests
# =============================================================================

class TestMainFunction:
    """Tests for main() function"""

    def test_main_connection_failure(self, mock_ibapi):
        """Test main exits on connection failure"""
        with patch('main.parse_args') as mock_args, \
             patch('main.Portfolio') as MockPortfolio, \
             patch('main.shutdown_manager') as mock_shutdown, \
             patch('sys.exit') as mock_exit:

            mock_args.return_value = MagicMock(
                host="127.0.0.1",
                port=7497,
                client_id=1,
                verbose=False,
                quiet=False,
                threshold=5.0,
                min_trade=100.0,
                live=False,
                no_server=True,
                stream=False,
                bars=False,
                rebalance=False,
            )
            MockPortfolio.return_value.connect.return_value = False

            from main import main
            main()

            mock_exit.assert_called_once_with(1)

    def test_main_shows_portfolio(self, mock_ibapi):
        """Test main shows portfolio by default"""
        with patch('main.parse_args') as mock_args, \
             patch('main.Portfolio') as MockPortfolio, \
             patch('main.shutdown_manager') as mock_shutdown, \
             patch('main.show_portfolio') as mock_show:

            mock_shutdown.should_shutdown = False
            mock_args.return_value = MagicMock(
                host="127.0.0.1",
                port=7497,
                client_id=1,
                verbose=False,
                quiet=False,
                threshold=5.0,
                min_trade=100.0,
                live=False,
                no_server=True,
                stream=False,
                bars=False,
                rebalance=False,
            )
            mock_portfolio = MockPortfolio.return_value
            mock_portfolio.connect.return_value = True

            from main import main
            main()

            mock_show.assert_called_once()


# =============================================================================
# Global Shutdown Manager Tests
# =============================================================================

class TestGlobalShutdownManager:
    """Tests for the global shutdown_manager instance"""

    def test_global_shutdown_manager_exists(self, mock_ibapi):
        """Test global shutdown_manager exists"""
        from main import shutdown_manager

        assert shutdown_manager is not None

    def test_global_shutdown_manager_is_correct_type(self, mock_ibapi):
        """Test global shutdown_manager is ShutdownManager"""
        from main import shutdown_manager, ShutdownManager

        assert isinstance(shutdown_manager, ShutdownManager)

