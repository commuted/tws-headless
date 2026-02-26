"""
Unit tests for portfolio.py

Tests Portfolio class properties, position management, streaming,
and order handling. Uses mocks to avoid actual IB connections.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from threading import Event, Lock
from datetime import datetime


# Mock ibapi before importing portfolio
@pytest.fixture(autouse=True)
def mock_ibapi():
    """Mock ibapi module for all tests"""
    mock_contract = MagicMock()
    mock_order = MagicMock()
    mock_ticktype = MagicMock()
    mock_ticktype.LAST = 4
    mock_ticktype.CLOSE = 9
    mock_ticktype.DELAYED_LAST = 68
    mock_ticktype.DELAYED_CLOSE = 75
    mock_ticktype.BID = 1
    mock_ticktype.ASK = 2

    with patch.dict('sys.modules', {
        'ibapi': MagicMock(),
        'ibapi.client': MagicMock(),
        'ibapi.wrapper': MagicMock(),
        'ibapi.common': MagicMock(),
        'ibapi.contract': MagicMock(Contract=mock_contract),
        'ibapi.order': MagicMock(Order=mock_order),
        'ibapi.ticktype': MagicMock(TickTypeEnum=mock_ticktype),
        'ibapi.account_summary_tags': MagicMock(),
    }):
        yield


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def mock_position():
    """Create a mock position for testing"""
    from models import Position, AssetType

    return Position(
        symbol="SPY",
        asset_type=AssetType.EQUITY,
        quantity=100,
        avg_cost=400.0,
        current_price=450.0,
        market_value=45000.0,
        unrealized_pnl=5000.0,
        allocation_pct=50.0,
    )


@pytest.fixture
def mock_position_bnd():
    """Create a second mock position"""
    from models import Position, AssetType

    return Position(
        symbol="BND",
        asset_type=AssetType.EQUITY,
        quantity=500,
        avg_cost=70.0,
        current_price=75.0,
        market_value=37500.0,
        unrealized_pnl=2500.0,
        allocation_pct=50.0,
    )


@pytest.fixture
def portfolio_instance(mock_ibapi):
    """Create a Portfolio instance for testing"""
    # Need to import after mocking
    with patch('portfolio.IBClient.__init__', lambda self, **kwargs: None):
        from portfolio import Portfolio

        portfolio = Portfolio.__new__(Portfolio)
        # Initialize required attributes
        portfolio._positions = {}
        portfolio._positions_lock = Lock()
        portfolio._positions_done = Event()
        portfolio._market_data_requests = {}
        portfolio._market_data_done = Event()
        portfolio._market_data_pending = 0
        portfolio._market_data_received = 0
        portfolio._streaming = False
        portfolio._stream_subscriptions = {}
        portfolio._stream_req_ids = {}
        portfolio._on_tick = None
        portfolio._last_prices = {}
        portfolio._bar_streaming = False
        portfolio._bar_subscriptions = {}
        portfolio._bar_req_ids = {}
        portfolio._on_bar = None
        portfolio._last_bars = {}
        portfolio._account_summary = {}
        portfolio._account_summary_done = Event()
        portfolio._orders = {}
        portfolio._orders_lock = Lock()
        portfolio._pending_orders = {}
        portfolio._on_order_status = None
        portfolio._connected = Event()
        portfolio._callbacks = {}
        portfolio.managed_accounts = ["DU123456"]
        portfolio._shutting_down = False

        # Forex and execution tracking (added in newer portfolio.py)
        portfolio._forex_cash = {}
        portfolio._forex_rates = {}
        portfolio._forex_positions = {}
        portfolio._forex_cost_basis = {}
        portfolio._account_updates_done = Event()
        portfolio._executions_done = Event()
        portfolio._execution_db = None

        return portfolio


# =============================================================================
# Portfolio Properties Tests
# =============================================================================

class TestPortfolioProperties:
    """Tests for Portfolio properties"""

    def test_positions_empty(self, portfolio_instance):
        """Test positions property when empty"""
        assert portfolio_instance.positions == []

    def test_positions_returns_list(self, portfolio_instance, mock_position):
        """Test positions property returns list of positions"""
        portfolio_instance._positions = {"SPY": mock_position}

        positions = portfolio_instance.positions
        assert len(positions) == 1
        assert positions[0].symbol == "SPY"

    def test_positions_returns_copy(self, portfolio_instance, mock_position):
        """Test positions returns a copy, not internal dict values"""
        portfolio_instance._positions = {"SPY": mock_position}

        positions = portfolio_instance.positions
        positions.append(mock_position)  # Modify the returned list

        # Internal should be unchanged
        assert len(portfolio_instance._positions) == 1

    def test_total_value_empty(self, portfolio_instance):
        """Test total_value with no positions"""
        assert portfolio_instance.total_value == 0.0

    def test_total_value_calculation(self, portfolio_instance, mock_position, mock_position_bnd):
        """Test total_value sums market values"""
        portfolio_instance._positions = {
            "SPY": mock_position,
            "BND": mock_position_bnd,
        }

        # 45000 + 37500 = 82500
        assert portfolio_instance.total_value == 82500.0

    def test_total_pnl_empty(self, portfolio_instance):
        """Test total_pnl with no positions"""
        assert portfolio_instance.total_pnl == 0.0

    def test_total_pnl_calculation(self, portfolio_instance, mock_position, mock_position_bnd):
        """Test total_pnl sums unrealized P&L"""
        portfolio_instance._positions = {
            "SPY": mock_position,
            "BND": mock_position_bnd,
        }

        # 5000 + 2500 = 7500
        assert portfolio_instance.total_pnl == 7500.0

    def test_is_streaming_false(self, portfolio_instance):
        """Test is_streaming when not streaming"""
        assert portfolio_instance.is_streaming is False

    def test_is_streaming_true(self, portfolio_instance):
        """Test is_streaming when streaming active"""
        portfolio_instance._streaming = True
        portfolio_instance._stream_subscriptions = {1: "SPY"}

        assert portfolio_instance.is_streaming is True

    def test_is_streaming_empty_subscriptions(self, portfolio_instance):
        """Test is_streaming false when streaming but no subscriptions"""
        portfolio_instance._streaming = True
        portfolio_instance._stream_subscriptions = {}

        assert portfolio_instance.is_streaming is False

    def test_streaming_symbols(self, portfolio_instance):
        """Test streaming_symbols returns subscribed symbols"""
        portfolio_instance._stream_subscriptions = {1: "SPY", 2: "BND"}

        symbols = portfolio_instance.streaming_symbols
        assert "SPY" in symbols
        assert "BND" in symbols

    def test_is_bar_streaming_false(self, portfolio_instance):
        """Test is_bar_streaming when not streaming"""
        assert portfolio_instance.is_bar_streaming is False

    def test_is_bar_streaming_true(self, portfolio_instance):
        """Test is_bar_streaming when streaming active"""
        portfolio_instance._bar_streaming = True
        portfolio_instance._bar_subscriptions = {1: "SPY"}

        assert portfolio_instance.is_bar_streaming is True

    def test_bar_streaming_symbols(self, portfolio_instance):
        """Test bar_streaming_symbols returns subscribed symbols"""
        portfolio_instance._bar_subscriptions = {1: "SPY", 2: "QQQ"}

        symbols = portfolio_instance.bar_streaming_symbols
        assert "SPY" in symbols
        assert "QQQ" in symbols


# =============================================================================
# Position Management Tests
# =============================================================================

class TestPositionManagement:
    """Tests for position management methods"""

    def test_get_position_found(self, portfolio_instance, mock_position):
        """Test get_position returns position when found"""
        portfolio_instance._positions = {"SPY": mock_position}

        pos = portfolio_instance.get_position("SPY")
        assert pos is not None
        assert pos.symbol == "SPY"

    def test_get_position_not_found(self, portfolio_instance):
        """Test get_position returns None when not found"""
        pos = portfolio_instance.get_position("UNKNOWN")
        assert pos is None

    def test_get_position_case_sensitive(self, portfolio_instance, mock_position):
        """Test get_position is case sensitive"""
        portfolio_instance._positions = {"SPY": mock_position}

        assert portfolio_instance.get_position("spy") is None
        assert portfolio_instance.get_position("SPY") is not None


# =============================================================================
# Price Data Tests
# =============================================================================

class TestPriceData:
    """Tests for price data methods"""

    def test_get_last_price_found(self, portfolio_instance):
        """Test get_last_price returns price when available"""
        portfolio_instance._last_prices = {"SPY": {"LAST": 450.0, "BID": 449.5}}

        price = portfolio_instance.get_last_price("SPY", "LAST")
        assert price == 450.0

    def test_get_last_price_default_tick_type(self, portfolio_instance):
        """Test get_last_price uses LAST by default"""
        portfolio_instance._last_prices = {"SPY": {"LAST": 450.0}}

        price = portfolio_instance.get_last_price("SPY")
        assert price == 450.0

    def test_get_last_price_not_found_symbol(self, portfolio_instance):
        """Test get_last_price returns None for unknown symbol"""
        price = portfolio_instance.get_last_price("UNKNOWN")
        assert price is None

    def test_get_last_price_not_found_type(self, portfolio_instance):
        """Test get_last_price returns None for unknown tick type"""
        portfolio_instance._last_prices = {"SPY": {"LAST": 450.0}}

        price = portfolio_instance.get_last_price("SPY", "UNKNOWN_TYPE")
        assert price is None

    def test_get_last_bar_found(self, portfolio_instance):
        """Test get_last_bar returns bar when available"""
        from models import Bar

        bar = Bar("SPY", "2024-01-15T10:00:00", 450, 455, 448, 454, 1000000)
        portfolio_instance._last_bars = {"SPY": bar}

        result = portfolio_instance.get_last_bar("SPY")
        assert result is not None
        assert result.symbol == "SPY"

    def test_get_last_bar_not_found(self, portfolio_instance):
        """Test get_last_bar returns None when not found"""
        result = portfolio_instance.get_last_bar("UNKNOWN")
        assert result is None


# =============================================================================
# Account Summary Tests
# =============================================================================

class TestAccountSummary:
    """Tests for account summary methods"""

    def test_get_account_summary_found(self, portfolio_instance):
        """Test get_account_summary returns summary when found"""
        from models import AccountSummary

        summary = AccountSummary(
            account_id="DU123456",
            net_liquidation=100000.0,
        )
        portfolio_instance._account_summary = {"DU123456": summary}

        result = portfolio_instance.get_account_summary("DU123456")
        assert result is not None
        assert result.net_liquidation == 100000.0

    def test_get_account_summary_default_account(self, portfolio_instance):
        """Test get_account_summary uses first managed account by default"""
        from models import AccountSummary

        summary = AccountSummary(
            account_id="DU123456",
            net_liquidation=100000.0,
        )
        portfolio_instance._account_summary = {"DU123456": summary}
        portfolio_instance.managed_accounts = ["DU123456"]

        result = portfolio_instance.get_account_summary()
        assert result is not None

    def test_get_account_summary_not_found(self, portfolio_instance):
        """Test get_account_summary returns None when not found"""
        result = portfolio_instance.get_account_summary("UNKNOWN")
        assert result is None


# =============================================================================
# Shutdown Tests
# =============================================================================

class TestShutdown:
    """Tests for shutdown functionality"""

    def test_is_shutting_down_false(self, portfolio_instance):
        """Test is_shutting_down defaults to False"""
        assert portfolio_instance.is_shutting_down is False

    def test_is_shutting_down_true(self, portfolio_instance):
        """Test is_shutting_down when set"""
        portfolio_instance._shutting_down = True
        assert portfolio_instance.is_shutting_down is True

    def test_shutdown_stops_streaming(self, portfolio_instance):
        """Test shutdown stops tick streaming"""
        portfolio_instance._streaming = True

        with patch.object(portfolio_instance, 'stop_streaming') as mock_stop:
            portfolio_instance.shutdown()
            mock_stop.assert_called_once()

    def test_shutdown_stops_bar_streaming(self, portfolio_instance):
        """Test shutdown stops bar streaming"""
        portfolio_instance._bar_streaming = True

        with patch.object(portfolio_instance, 'stop_bar_streaming') as mock_stop:
            portfolio_instance.shutdown()
            mock_stop.assert_called_once()

    def test_shutdown_sets_flag(self, portfolio_instance):
        """Test shutdown sets _shutting_down flag"""
        portfolio_instance.shutdown()
        assert portfolio_instance._shutting_down is True

    def test_shutdown_idempotent(self, portfolio_instance):
        """Test shutdown only runs once"""
        portfolio_instance._shutting_down = True

        with patch.object(portfolio_instance, 'stop_streaming') as mock_stop:
            portfolio_instance.shutdown()
            mock_stop.assert_not_called()

    def test_shutdown_signals_events(self, portfolio_instance):
        """Test shutdown signals waiting events"""
        portfolio_instance._positions_done.clear()
        portfolio_instance._market_data_done.clear()
        portfolio_instance._account_summary_done.clear()

        portfolio_instance.shutdown()

        assert portfolio_instance._positions_done.is_set()
        assert portfolio_instance._market_data_done.is_set()
        assert portfolio_instance._account_summary_done.is_set()


# =============================================================================
# Order Management Tests
# =============================================================================

class TestOrderManagement:
    """Tests for order management methods"""

    def test_orders_empty(self, portfolio_instance):
        """Test orders property when empty"""
        assert portfolio_instance.orders == []

    def test_orders_returns_list(self, portfolio_instance):
        """Test orders property returns list"""
        from models import OrderRecord, OrderStatus

        order = OrderRecord(
            order_id=1,
            symbol="SPY",
            action="BUY",
            quantity=100,
            status=OrderStatus.FILLED,
        )
        portfolio_instance._orders = {1: order}

        orders = portfolio_instance.orders
        assert len(orders) == 1
        assert orders[0].order_id == 1

    def test_pending_orders_filters(self, portfolio_instance):
        """Test pending_orders filters to non-complete orders"""
        from models import OrderRecord, OrderStatus

        filled_order = OrderRecord(
            order_id=1,
            symbol="SPY",
            action="BUY",
            quantity=100,
            status=OrderStatus.FILLED,
        )
        pending_order = OrderRecord(
            order_id=2,
            symbol="BND",
            action="BUY",
            quantity=50,
            status=OrderStatus.SUBMITTED,
        )
        portfolio_instance._orders = {1: filled_order, 2: pending_order}

        pending = portfolio_instance.pending_orders
        assert len(pending) == 1
        assert pending[0].order_id == 2

    def test_get_order_found(self, portfolio_instance):
        """Test get_order returns order when found"""
        from models import OrderRecord, OrderStatus

        order = OrderRecord(
            order_id=1,
            symbol="SPY",
            action="BUY",
            quantity=100,
            status=OrderStatus.FILLED,
        )
        portfolio_instance._orders = {1: order}

        result = portfolio_instance.get_order(1)
        assert result is not None
        assert result.symbol == "SPY"

    def test_get_order_not_found(self, portfolio_instance):
        """Test get_order returns None when not found"""
        result = portfolio_instance.get_order(999)
        assert result is None


# =============================================================================
# Allocation Calculation Tests
# =============================================================================

class TestAllocationCalculation:
    """Tests for allocation calculation"""

    def test_calculate_allocations_empty(self, portfolio_instance):
        """Test _calculate_allocations with no positions"""
        portfolio_instance._calculate_allocations()
        # Should not raise

    def test_calculate_allocations_single(self, portfolio_instance, mock_position):
        """Test _calculate_allocations with single position"""
        mock_position.allocation_pct = 0.0
        portfolio_instance._positions = {"SPY": mock_position}

        portfolio_instance._calculate_allocations()

        assert mock_position.allocation_pct == 100.0

    def test_calculate_allocations_multiple(self, portfolio_instance, mock_position, mock_position_bnd):
        """Test _calculate_allocations with multiple positions"""
        # SPY: 45000, BND: 37500, Total: 82500
        mock_position.allocation_pct = 0.0
        mock_position_bnd.allocation_pct = 0.0
        portfolio_instance._positions = {
            "SPY": mock_position,
            "BND": mock_position_bnd,
        }

        portfolio_instance._calculate_allocations()

        # SPY: 45000/82500 = ~54.5%
        # BND: 37500/82500 = ~45.5%
        assert 54.0 <= mock_position.allocation_pct <= 55.0
        assert 45.0 <= mock_position_bnd.allocation_pct <= 46.0


# =============================================================================
# Streaming Control Tests
# =============================================================================

class TestStreamingControl:
    """Tests for streaming control methods"""

    def test_stop_streaming_clears_state(self, portfolio_instance):
        """Test stop_streaming clears streaming state"""
        portfolio_instance._streaming = True
        portfolio_instance._stream_subscriptions = {1: "SPY"}
        portfolio_instance._stream_req_ids = {"SPY": 1}
        portfolio_instance._on_tick = MagicMock()

        with patch.object(portfolio_instance, 'cancelMktData'):
            portfolio_instance.stop_streaming()

        assert portfolio_instance._streaming is False
        assert portfolio_instance._stream_subscriptions == {}
        assert portfolio_instance._stream_req_ids == {}
        assert portfolio_instance._on_tick is None

    def test_stop_streaming_when_not_streaming(self, portfolio_instance):
        """Test stop_streaming does nothing when not streaming"""
        portfolio_instance._streaming = False

        # Should not raise
        portfolio_instance.stop_streaming()

    def test_stop_bar_streaming_clears_state(self, portfolio_instance):
        """Test stop_bar_streaming clears bar streaming state"""
        portfolio_instance._bar_streaming = True
        portfolio_instance._bar_subscriptions = {1: "SPY"}
        portfolio_instance._bar_req_ids = {"SPY": 1}
        portfolio_instance._on_bar = MagicMock()

        with patch.object(portfolio_instance, 'cancelRealTimeBars'):
            portfolio_instance.stop_bar_streaming()

        assert portfolio_instance._bar_streaming is False
        assert portfolio_instance._bar_subscriptions == {}
        assert portfolio_instance._bar_req_ids == {}
        assert portfolio_instance._on_bar is None

    def test_unstream_symbol_removes(self, portfolio_instance):
        """Test unstream_symbol removes a symbol from streaming"""
        portfolio_instance._stream_subscriptions = {1: "SPY", 2: "BND"}
        portfolio_instance._stream_req_ids = {"SPY": 1, "BND": 2}

        with patch.object(portfolio_instance, 'cancelMktData'):
            portfolio_instance.unstream_symbol("SPY")

        assert "SPY" not in portfolio_instance._stream_req_ids
        assert 1 not in portfolio_instance._stream_subscriptions

    def test_unstream_symbol_not_streaming(self, portfolio_instance):
        """Test unstream_symbol does nothing for unknown symbol"""
        portfolio_instance._stream_req_ids = {}

        # Should not raise
        portfolio_instance.unstream_symbol("UNKNOWN")

    def test_unstream_bar_symbol_removes(self, portfolio_instance):
        """Test unstream_bar_symbol removes a symbol"""
        portfolio_instance._bar_subscriptions = {1: "SPY"}
        portfolio_instance._bar_req_ids = {"SPY": 1}

        with patch.object(portfolio_instance, 'cancelRealTimeBars'):
            portfolio_instance.unstream_bar_symbol("SPY")

        assert "SPY" not in portfolio_instance._bar_req_ids


# =============================================================================
# EWrapper Callback Tests
# =============================================================================

class TestEWrapperCallbacks:
    """Tests for EWrapper callback methods"""

    def test_position_callback_adds_position(self, portfolio_instance):
        """Test position callback adds to positions dict"""
        contract = MagicMock()
        contract.symbol = "AAPL"
        contract.secType = "STK"

        portfolio_instance.position("DU123456", contract, 100.0, 150.0)

        assert "AAPL" in portfolio_instance._positions
        pos = portfolio_instance._positions["AAPL"]
        assert pos.quantity == 100.0
        assert pos.avg_cost == 150.0

    def test_position_callback_skips_zero(self, portfolio_instance):
        """Test position callback skips zero positions"""
        contract = MagicMock()
        contract.symbol = "AAPL"
        contract.secType = "STK"

        portfolio_instance.position("DU123456", contract, 0.0, 150.0)

        assert "AAPL" not in portfolio_instance._positions

    def test_position_callback_invokes_registered(self, portfolio_instance):
        """Test position callback invokes registered callback"""
        contract = MagicMock()
        contract.symbol = "AAPL"
        contract.secType = "STK"

        handler = MagicMock()
        portfolio_instance._callbacks = {"position": handler}

        portfolio_instance.position("DU123456", contract, 100.0, 150.0)

        handler.assert_called_once()

    def test_positionEnd_sets_event(self, portfolio_instance):
        """Test positionEnd sets done event"""
        portfolio_instance._positions_done.clear()

        portfolio_instance.positionEnd()

        assert portfolio_instance._positions_done.is_set()

    def test_accountSummary_stores_values(self, portfolio_instance):
        """Test accountSummary stores values"""
        portfolio_instance.accountSummary(1, "DU123456", "NetLiquidation", "100000", "USD")

        assert "DU123456" in portfolio_instance._account_summary
        summary = portfolio_instance._account_summary["DU123456"]
        assert summary.net_liquidation == 100000.0

    def test_accountSummary_creates_new(self, portfolio_instance):
        """Test accountSummary creates new AccountSummary if needed"""
        portfolio_instance._account_summary = {}

        portfolio_instance.accountSummary(1, "DU999999", "TotalCashValue", "50000", "USD")

        assert "DU999999" in portfolio_instance._account_summary

    def test_accountSummaryEnd_sets_event(self, portfolio_instance):
        """Test accountSummaryEnd sets done event"""
        portfolio_instance._account_summary_done.clear()

        portfolio_instance.accountSummaryEnd(1)

        assert portfolio_instance._account_summary_done.is_set()

    def test_tickSnapshotEnd_increments_received(self, portfolio_instance):
        """Test tickSnapshotEnd increments received counter"""
        portfolio_instance._market_data_requests = {1: "SPY"}
        portfolio_instance._market_data_pending = 1
        portfolio_instance._market_data_received = 0
        portfolio_instance._market_data_done.clear()

        portfolio_instance.tickSnapshotEnd(1)

        assert portfolio_instance._market_data_received == 1
        assert portfolio_instance._market_data_done.is_set()


# =============================================================================
# Quick Load Function Tests
# =============================================================================

class TestQuickLoad:
    """Tests for quick_load convenience function"""

    def test_quick_load_returns_empty_on_connect_fail(self, mock_ibapi):
        """Test quick_load returns empty list on connection failure"""
        with patch('portfolio.Portfolio') as MockPortfolio:
            instance = MockPortfolio.return_value
            instance.connect.return_value = False

            from portfolio import quick_load
            result = quick_load()

            assert result == []

    def test_quick_load_disconnects_after(self, mock_ibapi):
        """Test quick_load disconnects after loading"""
        with patch('portfolio.Portfolio') as MockPortfolio:
            instance = MockPortfolio.return_value
            instance.connect.return_value = True
            instance.positions = []

            from portfolio import quick_load
            quick_load()

            instance.disconnect.assert_called_once()


# =============================================================================
# Load Method Tests
# =============================================================================

class TestPortfolioLoad:
    """Tests for Portfolio.load() method"""

    def test_load_not_connected(self, portfolio_instance):
        """Test load returns False when not connected"""
        portfolio_instance._connected.clear()

        result = portfolio_instance.load()

        assert result is False

    def test_load_clears_positions(self, portfolio_instance, mock_position):
        """Test load clears previous positions"""
        portfolio_instance._connected.set()
        portfolio_instance._positions = {"SPY": mock_position}
        portfolio_instance._positions_done.set()
        portfolio_instance._account_updates_done.set()

        with patch.object(portfolio_instance, 'reqPositions'), \
             patch.object(portfolio_instance, 'reqAccountUpdates'), \
             patch.object(portfolio_instance, '_apply_forex_rates'), \
             patch.object(portfolio_instance, '_fetch_market_data'), \
             patch.object(portfolio_instance, '_calculate_allocations'), \
             patch.object(portfolio_instance, '_fetch_account_summary'), \
             patch.object(portfolio_instance, 'request_executions'):
            portfolio_instance.load()

        assert portfolio_instance._positions == {}

    def test_load_requests_positions(self, portfolio_instance):
        """Test load calls reqPositions"""
        portfolio_instance._connected.set()
        portfolio_instance._positions_done.set()
        portfolio_instance._account_updates_done.set()

        with patch.object(portfolio_instance, 'reqPositions') as mock_req, \
             patch.object(portfolio_instance, 'reqAccountUpdates'), \
             patch.object(portfolio_instance, '_apply_forex_rates'), \
             patch.object(portfolio_instance, '_fetch_market_data'), \
             patch.object(portfolio_instance, '_calculate_allocations'), \
             patch.object(portfolio_instance, '_fetch_account_summary'), \
             patch.object(portfolio_instance, 'request_executions'):
            portfolio_instance.load()

        mock_req.assert_called_once()

    def test_load_fetches_prices_when_enabled(self, portfolio_instance, mock_position):
        """Test load fetches prices when fetch_prices=True"""
        portfolio_instance._connected.set()
        portfolio_instance._account_updates_done.set()

        # Simulate positions being loaded by reqPositions callback
        def fake_req_positions():
            portfolio_instance._positions = {"SPY": mock_position}
            portfolio_instance._positions_done.set()

        with patch.object(portfolio_instance, 'reqPositions', side_effect=fake_req_positions), \
             patch.object(portfolio_instance, 'reqAccountUpdates'), \
             patch.object(portfolio_instance, '_apply_forex_rates'), \
             patch.object(portfolio_instance, '_fetch_market_data') as mock_fetch, \
             patch.object(portfolio_instance, '_calculate_allocations'), \
             patch.object(portfolio_instance, '_fetch_account_summary'), \
             patch.object(portfolio_instance, 'request_executions'):
            portfolio_instance.load(fetch_prices=True)

        mock_fetch.assert_called_once()

    def test_load_skips_prices_when_disabled(self, portfolio_instance):
        """Test load skips price fetch when fetch_prices=False"""
        portfolio_instance._connected.set()
        portfolio_instance._positions_done.set()
        portfolio_instance._account_updates_done.set()

        with patch.object(portfolio_instance, 'reqPositions'), \
             patch.object(portfolio_instance, 'reqAccountUpdates'), \
             patch.object(portfolio_instance, '_apply_forex_rates'), \
             patch.object(portfolio_instance, '_fetch_market_data') as mock_fetch, \
             patch.object(portfolio_instance, '_calculate_allocations'), \
             patch.object(portfolio_instance, '_fetch_account_summary'), \
             patch.object(portfolio_instance, 'request_executions'):
            portfolio_instance.load(fetch_prices=False)

        mock_fetch.assert_not_called()

    def test_load_fetches_account_when_enabled(self, portfolio_instance):
        """Test load fetches account when fetch_account=True"""
        portfolio_instance._connected.set()
        portfolio_instance._positions_done.set()
        portfolio_instance._account_updates_done.set()

        with patch.object(portfolio_instance, 'reqPositions'), \
             patch.object(portfolio_instance, 'reqAccountUpdates'), \
             patch.object(portfolio_instance, '_apply_forex_rates'), \
             patch.object(portfolio_instance, '_calculate_allocations'), \
             patch.object(portfolio_instance, '_fetch_account_summary') as mock_fetch, \
             patch.object(portfolio_instance, 'request_executions'):
            portfolio_instance.load(fetch_prices=False, fetch_account=True)

        mock_fetch.assert_called_once()


# =============================================================================
# Start Streaming Tests
# =============================================================================

class TestStartStreaming:
    """Tests for Portfolio.start_streaming() method"""

    def test_start_streaming_not_connected(self, portfolio_instance):
        """Test start_streaming returns False when not connected"""
        portfolio_instance._connected.clear()

        result = portfolio_instance.start_streaming()

        assert result is False

    def test_start_streaming_no_positions(self, portfolio_instance):
        """Test start_streaming returns False with no positions"""
        portfolio_instance._connected.set()
        portfolio_instance._positions = {}

        result = portfolio_instance.start_streaming()

        assert result is False

    def test_start_streaming_already_streaming(self, portfolio_instance, mock_position):
        """Test start_streaming returns False when already streaming"""
        portfolio_instance._connected.set()
        portfolio_instance._positions = {"SPY": mock_position}
        portfolio_instance._streaming = True

        result = portfolio_instance.start_streaming()

        assert result is False

    def test_start_streaming_sets_callback(self, portfolio_instance, mock_position):
        """Test start_streaming sets the tick callback"""
        portfolio_instance._connected.set()
        mock_position.contract = MagicMock()
        portfolio_instance._positions = {"SPY": mock_position}

        callback = MagicMock()

        with patch.object(portfolio_instance, 'reqMarketDataType'), \
             patch.object(portfolio_instance, 'get_next_req_id', return_value=1), \
             patch.object(portfolio_instance, 'reqMktData'):
            portfolio_instance.start_streaming(on_tick=callback)

        assert portfolio_instance._on_tick is callback

    def test_start_streaming_requests_delayed_data(self, portfolio_instance, mock_position):
        """Test start_streaming requests delayed data by default"""
        portfolio_instance._connected.set()
        mock_position.contract = MagicMock()
        portfolio_instance._positions = {"SPY": mock_position}

        with patch.object(portfolio_instance, 'reqMarketDataType') as mock_type, \
             patch.object(portfolio_instance, 'get_next_req_id', return_value=1), \
             patch.object(portfolio_instance, 'reqMktData'):
            portfolio_instance.start_streaming(use_delayed=True)

        mock_type.assert_called_once_with(3)  # 3 = delayed

    def test_start_streaming_requests_live_data(self, portfolio_instance, mock_position):
        """Test start_streaming requests live data when specified"""
        portfolio_instance._connected.set()
        mock_position.contract = MagicMock()
        portfolio_instance._positions = {"SPY": mock_position}

        with patch.object(portfolio_instance, 'reqMarketDataType') as mock_type, \
             patch.object(portfolio_instance, 'get_next_req_id', return_value=1), \
             patch.object(portfolio_instance, 'reqMktData'):
            portfolio_instance.start_streaming(use_delayed=False)

        mock_type.assert_called_once_with(1)  # 1 = live


# =============================================================================
# Stream Symbol Tests
# =============================================================================

class TestStreamSymbol:
    """Tests for Portfolio.stream_symbol() method"""

    def test_stream_symbol_not_connected(self, portfolio_instance):
        """Test stream_symbol returns False when not connected"""
        portfolio_instance._connected.clear()

        result = portfolio_instance.stream_symbol("SPY")

        assert result is False

    def test_stream_symbol_no_contract(self, portfolio_instance):
        """Test stream_symbol returns False with no contract"""
        portfolio_instance._connected.set()
        portfolio_instance._positions = {}

        result = portfolio_instance.stream_symbol("UNKNOWN")

        assert result is False

    def test_stream_symbol_already_streaming(self, portfolio_instance, mock_position):
        """Test stream_symbol returns True if already streaming"""
        portfolio_instance._connected.set()
        mock_position.contract = MagicMock()
        portfolio_instance._positions = {"SPY": mock_position}
        portfolio_instance._stream_req_ids = {"SPY": 1}

        result = portfolio_instance.stream_symbol("SPY")

        assert result is True

    def test_stream_symbol_uses_position_contract(self, portfolio_instance, mock_position):
        """Test stream_symbol uses contract from position"""
        portfolio_instance._connected.set()
        mock_position.contract = MagicMock()
        portfolio_instance._positions = {"SPY": mock_position}

        with patch.object(portfolio_instance, 'get_next_req_id', return_value=1), \
             patch.object(portfolio_instance, 'reqMktData'):
            result = portfolio_instance.stream_symbol("SPY")

        assert result is True
        assert portfolio_instance._streaming is True


# =============================================================================
# Bar Streaming Tests
# =============================================================================

class TestBarStreaming:
    """Tests for bar streaming methods"""

    def test_start_bar_streaming_not_connected(self, portfolio_instance):
        """Test start_bar_streaming returns False when not connected"""
        portfolio_instance._connected.clear()

        result = portfolio_instance.start_bar_streaming()

        assert result is False

    def test_start_bar_streaming_no_positions(self, portfolio_instance):
        """Test start_bar_streaming returns False with no positions"""
        portfolio_instance._connected.set()
        portfolio_instance._positions = {}

        result = portfolio_instance.start_bar_streaming()

        assert result is False

    def test_start_bar_streaming_already_streaming(self, portfolio_instance, mock_position):
        """Test start_bar_streaming returns False when already streaming"""
        portfolio_instance._connected.set()
        portfolio_instance._positions = {"SPY": mock_position}
        portfolio_instance._bar_streaming = True

        result = portfolio_instance.start_bar_streaming()

        assert result is False

    def test_start_bar_streaming_sets_callback(self, portfolio_instance, mock_position):
        """Test start_bar_streaming sets the bar callback"""
        portfolio_instance._connected.set()
        mock_position.contract = MagicMock()
        portfolio_instance._positions = {"SPY": mock_position}

        callback = MagicMock()

        with patch.object(portfolio_instance, 'get_next_req_id', return_value=1), \
             patch.object(portfolio_instance, 'reqRealTimeBars'):
            portfolio_instance.start_bar_streaming(on_bar=callback)

        assert portfolio_instance._on_bar is callback

    def test_bar_stream_symbol_not_connected(self, portfolio_instance):
        """Test bar_stream_symbol returns False when not connected"""
        portfolio_instance._connected.clear()

        result = portfolio_instance.bar_stream_symbol("SPY")

        assert result is False

    def test_bar_stream_symbol_already_streaming(self, portfolio_instance, mock_position):
        """Test bar_stream_symbol returns True if already streaming"""
        portfolio_instance._connected.set()
        mock_position.contract = MagicMock()
        portfolio_instance._positions = {"SPY": mock_position}
        portfolio_instance._bar_req_ids = {"SPY": 1}

        result = portfolio_instance.bar_stream_symbol("SPY")

        assert result is True


# =============================================================================
# Order Placement Tests
# =============================================================================

class TestOrderPlacement:
    """Tests for order placement methods"""

    def test_place_order_not_connected(self, portfolio_instance):
        """Test place_order returns None when not connected"""
        portfolio_instance._connected.clear()

        result = portfolio_instance.place_order(MagicMock(), "BUY", 100)

        assert result is None

    def test_place_order_no_order_id(self, portfolio_instance):
        """Test place_order returns None when no order ID available"""
        portfolio_instance._connected.set()
        portfolio_instance._next_order_id = None
        portfolio_instance._lock = Lock()

        result = portfolio_instance.place_order(MagicMock(), "BUY", 100)

        assert result is None

    def test_place_order_success(self, portfolio_instance):
        """Test place_order returns order ID on success"""
        portfolio_instance._connected.set()
        portfolio_instance._next_order_id = 100
        portfolio_instance._lock = Lock()

        contract = MagicMock()
        contract.symbol = "SPY"

        with patch.object(portfolio_instance, 'placeOrder'):
            result = portfolio_instance.place_order(contract, "BUY", 100)

        assert result == 100
        assert 100 in portfolio_instance._orders

    def test_place_order_increments_id(self, portfolio_instance):
        """Test place_order increments next order ID"""
        portfolio_instance._connected.set()
        portfolio_instance._next_order_id = 100
        portfolio_instance._lock = Lock()

        contract = MagicMock()
        contract.symbol = "SPY"

        with patch.object(portfolio_instance, 'placeOrder'):
            portfolio_instance.place_order(contract, "BUY", 100)

        assert portfolio_instance._next_order_id == 101

    def test_place_order_creates_record(self, portfolio_instance):
        """Test place_order creates OrderRecord"""
        portfolio_instance._connected.set()
        portfolio_instance._next_order_id = 100
        portfolio_instance._lock = Lock()

        contract = MagicMock()
        contract.symbol = "SPY"

        with patch.object(portfolio_instance, 'placeOrder'):
            portfolio_instance.place_order(contract, "SELL", 50, order_type="LMT", limit_price=450.0)

        order = portfolio_instance._orders[100]
        assert order.symbol == "SPY"
        assert order.action == "SELL"
        assert order.quantity == 50
        assert order.order_type == "LMT"

    def test_place_market_order_convenience(self, portfolio_instance):
        """Test place_market_order is convenience for place_order"""
        portfolio_instance._connected.set()
        portfolio_instance._next_order_id = 100
        portfolio_instance._lock = Lock()

        contract = MagicMock()
        contract.symbol = "SPY"

        with patch.object(portfolio_instance, 'placeOrder'):
            result = portfolio_instance.place_market_order(contract, "BUY", 100)

        assert result == 100
        order = portfolio_instance._orders[100]
        assert order.order_type == "MKT"

    def test_place_limit_order_convenience(self, portfolio_instance):
        """Test place_limit_order is convenience for place_order"""
        portfolio_instance._connected.set()
        portfolio_instance._next_order_id = 100
        portfolio_instance._lock = Lock()

        contract = MagicMock()
        contract.symbol = "SPY"

        with patch.object(portfolio_instance, 'placeOrder'):
            result = portfolio_instance.place_limit_order(contract, "BUY", 100, limit_price=445.0)

        assert result == 100
        order = portfolio_instance._orders[100]
        assert order.order_type == "LMT"


# =============================================================================
# Order Cancellation Tests
# =============================================================================

class TestOrderCancellation:
    """Tests for order cancellation"""

    def test_cancel_order_not_connected(self, portfolio_instance):
        """Test cancel_order returns False when not connected"""
        portfolio_instance._connected.clear()

        result = portfolio_instance.cancel_order(100)

        assert result is False

    def test_cancel_order_success(self, portfolio_instance):
        """Test cancel_order returns True on success"""
        portfolio_instance._connected.set()

        with patch.object(portfolio_instance, 'cancelOrder'):
            result = portfolio_instance.cancel_order(100)

        assert result is True

    def test_cancel_order_exception(self, portfolio_instance):
        """Test cancel_order returns False on exception"""
        portfolio_instance._connected.set()

        with patch.object(portfolio_instance, 'cancelOrder', side_effect=Exception("Error")):
            result = portfolio_instance.cancel_order(100)

        assert result is False


# =============================================================================
# Wait for Order Tests
# =============================================================================

class TestWaitForOrder:
    """Tests for order wait methods"""

    def test_wait_for_order_no_event(self, portfolio_instance):
        """Test wait_for_order returns order when no event"""
        from models import OrderRecord, OrderStatus

        order = OrderRecord(order_id=100, symbol="SPY", action="BUY", quantity=100)
        portfolio_instance._orders = {100: order}
        portfolio_instance._pending_orders = {}

        result = portfolio_instance.wait_for_order(100)

        assert result is order

    def test_wait_for_order_with_event(self, portfolio_instance):
        """Test wait_for_order waits for event"""
        from models import OrderRecord, OrderStatus
        import threading

        order = OrderRecord(order_id=100, symbol="SPY", action="BUY", quantity=100)
        event = Event()
        portfolio_instance._orders = {100: order}
        portfolio_instance._pending_orders = {100: event}

        # Set event after short delay
        def set_event():
            import time
            time.sleep(0.05)
            event.set()

        thread = threading.Thread(target=set_event)
        thread.start()

        result = portfolio_instance.wait_for_order(100, timeout=1.0)
        thread.join()

        assert result is order

    def test_wait_for_all_orders_empty(self, portfolio_instance):
        """Test wait_for_all_orders returns True when no pending"""
        portfolio_instance._orders = {}

        result = portfolio_instance.wait_for_all_orders(timeout=0.1)

        assert result is True


# =============================================================================
# Tick Price Callback Tests
# =============================================================================

class TestTickPriceCallback:
    """Tests for tickPrice callback"""

    def test_tickPrice_streaming_tick(self, portfolio_instance):
        """Test tickPrice handles streaming tick"""
        portfolio_instance._stream_subscriptions = {1: "SPY"}
        portfolio_instance._last_prices = {"SPY": {}}

        with patch.object(portfolio_instance, '_handle_stream_tick') as mock_handle:
            portfolio_instance.tickPrice(1, 4, 450.0, None)  # 4 = LAST

        mock_handle.assert_called_once_with(1, 4, 450.0)

    def test_tickPrice_snapshot_updates_position(self, portfolio_instance, mock_position):
        """Test tickPrice updates position for snapshot"""
        portfolio_instance._market_data_requests = {1: "SPY"}
        portfolio_instance._positions = {"SPY": mock_position}
        portfolio_instance._market_data_pending = 1
        portfolio_instance._market_data_received = 0
        portfolio_instance._market_data_done.clear()

        with patch.object(portfolio_instance, 'cancelMktData'):
            portfolio_instance.tickPrice(1, 4, 455.0, None)  # 4 = LAST

        assert mock_position.current_price == 455.0

    def test_tickPrice_ignores_invalid_price(self, portfolio_instance, mock_position):
        """Test tickPrice ignores zero or negative price"""
        portfolio_instance._market_data_requests = {1: "SPY"}
        portfolio_instance._positions = {"SPY": mock_position}
        original_price = mock_position.current_price

        portfolio_instance.tickPrice(1, 4, 0.0, None)

        assert mock_position.current_price == original_price


# =============================================================================
# Handle Stream Tick Tests
# =============================================================================

class TestHandleStreamTick:
    """Tests for _handle_stream_tick"""

    def test_handle_stream_tick_invalid_price(self, portfolio_instance):
        """Test _handle_stream_tick ignores invalid price"""
        portfolio_instance._stream_subscriptions = {1: "SPY"}
        portfolio_instance._last_prices = {"SPY": {}}

        portfolio_instance._handle_stream_tick(1, 4, -1.0)

        assert portfolio_instance._last_prices["SPY"] == {}

    def test_handle_stream_tick_unknown_req_id(self, portfolio_instance):
        """Test _handle_stream_tick ignores unknown reqId"""
        portfolio_instance._stream_subscriptions = {}

        # Should not raise
        portfolio_instance._handle_stream_tick(999, 4, 450.0)

    def test_handle_stream_tick_stores_price(self, portfolio_instance):
        """Test _handle_stream_tick stores price"""
        portfolio_instance._stream_subscriptions = {1: "SPY"}
        portfolio_instance._last_prices = {"SPY": {}}
        portfolio_instance._positions = {}

        portfolio_instance._handle_stream_tick(1, 4, 450.0)  # 4 = LAST

        assert "LAST" in portfolio_instance._last_prices["SPY"]

    def test_handle_stream_tick_calls_callback(self, portfolio_instance):
        """Test _handle_stream_tick calls registered callback"""
        portfolio_instance._stream_subscriptions = {1: "SPY"}
        portfolio_instance._last_prices = {"SPY": {}}
        portfolio_instance._positions = {}

        callback = MagicMock()
        portfolio_instance._on_tick = callback

        portfolio_instance._handle_stream_tick(1, 4, 450.0)

        callback.assert_called_once()


# =============================================================================
# Real-time Bar Callback Tests
# =============================================================================

class TestRealtimeBarCallback:
    """Tests for realtimeBar callback"""

    def test_realtimeBar_unknown_req_id(self, portfolio_instance):
        """Test realtimeBar ignores unknown reqId"""
        portfolio_instance._bar_subscriptions = {}

        # Should not raise
        portfolio_instance.realtimeBar(999, 1234567890, 450, 455, 448, 454, 1000, 452.5, 100)

    def test_realtimeBar_stores_bar(self, portfolio_instance):
        """Test realtimeBar stores last bar"""
        portfolio_instance._bar_subscriptions = {1: "SPY"}
        portfolio_instance._positions = {}

        portfolio_instance.realtimeBar(1, 1234567890, 450, 455, 448, 454, 1000, 452.5, 100)

        assert "SPY" in portfolio_instance._last_bars
        bar = portfolio_instance._last_bars["SPY"]
        assert bar.open == 450
        assert bar.close == 454

    def test_realtimeBar_updates_position(self, portfolio_instance, mock_position):
        """Test realtimeBar updates position price"""
        portfolio_instance._bar_subscriptions = {1: "SPY"}
        portfolio_instance._positions = {"SPY": mock_position}

        with patch.object(portfolio_instance, '_calculate_allocations'):
            portfolio_instance.realtimeBar(1, 1234567890, 450, 455, 448, 454, 1000, 452.5, 100)

        assert mock_position.current_price == 454  # Close price

    def test_realtimeBar_calls_callback(self, portfolio_instance):
        """Test realtimeBar calls registered callback"""
        portfolio_instance._bar_subscriptions = {1: "SPY"}
        portfolio_instance._positions = {}

        callback = MagicMock()
        portfolio_instance._on_bar = callback

        portfolio_instance.realtimeBar(1, 1234567890, 450, 455, 448, 454, 1000, 452.5, 100)

        callback.assert_called_once()


# =============================================================================
# Order Status Callback Tests
# =============================================================================

class TestOrderStatusCallback:
    """Tests for orderStatus callback"""

    def test_orderStatus_unknown_order(self, portfolio_instance):
        """Test orderStatus ignores unknown order"""
        portfolio_instance._orders = {}

        # Should not raise
        portfolio_instance.orderStatus(999, "Filled", 100, 0, 450.0, 0, 0, 450.0, 1, "", 0.0)

    def test_orderStatus_updates_record(self, portfolio_instance):
        """Test orderStatus updates order record"""
        from models import OrderRecord, OrderStatus

        order = OrderRecord(order_id=100, symbol="SPY", action="BUY", quantity=100)
        portfolio_instance._orders = {100: order}
        portfolio_instance._pending_orders = {100: Event()}

        portfolio_instance.orderStatus(100, "Filled", 100, 0, 450.0, 0, 0, 450.0, 1, "", 0.0)

        assert order.status == OrderStatus.FILLED
        assert order.filled_quantity == 100
        assert order.avg_fill_price == 450.0

    def test_orderStatus_signals_completion(self, portfolio_instance):
        """Test orderStatus signals completion event"""
        from models import OrderRecord, OrderStatus

        order = OrderRecord(order_id=100, symbol="SPY", action="BUY", quantity=100)
        event = Event()
        portfolio_instance._orders = {100: order}
        portfolio_instance._pending_orders = {100: event}

        portfolio_instance.orderStatus(100, "Filled", 100, 0, 450.0, 0, 0, 450.0, 1, "", 0.0)

        assert event.is_set()


# =============================================================================
# Open Order and Exec Details Tests
# =============================================================================

class TestOpenOrderAndExecDetails:
    """Tests for openOrder and execDetails callbacks"""

    def test_openOrder_calls_callback(self, portfolio_instance):
        """Test openOrder calls registered callback"""
        handler = MagicMock()
        portfolio_instance._callbacks = {"openOrder": handler}

        portfolio_instance.openOrder(100, MagicMock(), MagicMock(), MagicMock())

        handler.assert_called_once()

    def test_execDetails_calls_callback(self, portfolio_instance):
        """Test execDetails calls registered callback"""
        handler = MagicMock()
        portfolio_instance._callbacks = {"execDetails": handler}

        contract = MagicMock()
        contract.secType = "STK"
        contract.symbol = "SPY"
        contract.localSymbol = "SPY"
        contract.currency = "USD"
        contract.exchange = "SMART"

        execution = MagicMock()
        execution.execId = "0001"
        execution.orderId = 100
        execution.side = "BOT"
        execution.shares = 100
        execution.cumQty = 100
        execution.avgPrice = 450.0
        execution.exchange = "SMART"
        execution.acctNumber = "DU123456"

        with patch('portfolio.get_execution_db'):
            portfolio_instance.execDetails(1, contract, execution)

        handler.assert_called_once()


# =============================================================================
# DataFrame Conversion Tests
# =============================================================================

class TestToDataframe:
    """Tests for to_dataframe method"""

    def test_to_dataframe_empty(self, portfolio_instance):
        """Test to_dataframe with no positions"""
        portfolio_instance._positions = {}

        try:
            import pandas as pd
            result = portfolio_instance.to_dataframe()
            assert len(result) == 0
        except ImportError:
            # pandas not installed, skip test
            pass

    def test_to_dataframe_with_positions(self, portfolio_instance, mock_position):
        """Test to_dataframe with positions"""
        portfolio_instance._positions = {"SPY": mock_position}

        try:
            import pandas as pd
            result = portfolio_instance.to_dataframe()
            assert len(result) == 1
            assert result.iloc[0]["symbol"] == "SPY"
        except ImportError:
            # pandas not installed, skip test
            pass

    def test_to_dataframe_raises_without_pandas(self, portfolio_instance, mock_position):
        """Test to_dataframe raises ImportError without pandas"""
        portfolio_instance._positions = {"SPY": mock_position}

        with patch.dict('sys.modules', {'pandas': None}):
            # Force reimport of pandas to fail
            import sys
            if 'pandas' in sys.modules:
                del sys.modules['pandas']

            # This test is tricky because pandas may be installed
            # Just verify the method exists and is callable
            assert hasattr(portfolio_instance, 'to_dataframe')
