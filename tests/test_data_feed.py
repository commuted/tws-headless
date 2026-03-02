"""
Tests for data_feed.py - Algorithm-centric data streaming
"""

import pytest
import time
import threading
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch
from collections import deque

from ibapi.contract import Contract

from data_feed import (
    DataFeed,
    DataType,
    TickData,
    InstrumentSubscription,
    DataBuffer,
    BarAggregator,
)
from models import Bar


def create_mock_portfolio():
    """Create a mock Portfolio for testing"""
    portfolio = Mock()
    portfolio.connected = True
    portfolio._on_tick = None
    portfolio._on_bar = None
    portfolio.stream_symbol = Mock(return_value=True)
    portfolio.bar_stream_symbol = Mock(return_value=True)
    portfolio.unstream_symbol = Mock()
    portfolio.unstream_bar_symbol = Mock()
    return portfolio


def create_contract(symbol: str) -> Contract:
    """Helper to create a test contract"""
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    return contract


def create_bar(symbol: str, timestamp: str, open_: float, high: float, low: float, close: float, volume: int = 100) -> Bar:
    """Helper to create a test bar"""
    return Bar(
        symbol=symbol,
        timestamp=timestamp,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        wap=0.0,
        bar_count=1,
    )


class TestDataType:
    """Tests for DataType enum"""

    def test_data_type_values(self):
        """Test DataType enum values"""
        assert DataType.TICK.value == "tick"
        assert DataType.BAR_5SEC.value == "bar_5sec"
        assert DataType.BAR_1MIN.value == "bar_1min"
        assert DataType.BAR_5MIN.value == "bar_5min"
        assert DataType.BAR_15MIN.value == "bar_15min"
        assert DataType.BAR_1HOUR.value == "bar_1hour"


class TestTickData:
    """Tests for TickData dataclass"""

    def test_tick_data_creation(self):
        """Test creating TickData"""
        tick = TickData(
            symbol="SPY",
            price=450.50,
            tick_type="LAST",
        )

        assert tick.symbol == "SPY"
        assert tick.price == 450.50
        assert tick.tick_type == "LAST"
        assert isinstance(tick.timestamp, datetime)

    def test_tick_data_with_timestamp(self):
        """Test creating TickData with specific timestamp"""
        ts = datetime(2024, 1, 15, 10, 30, 0)
        tick = TickData(
            symbol="AAPL",
            price=180.25,
            tick_type="BID",
            timestamp=ts,
        )

        assert tick.timestamp == ts


class TestInstrumentSubscription:
    """Tests for InstrumentSubscription dataclass"""

    def test_default_values(self):
        """Test default subscription values"""
        contract = create_contract("SPY")
        sub = InstrumentSubscription(symbol="SPY", contract=contract)

        assert sub.symbol == "SPY"
        assert sub.contract == contract
        assert sub.data_types == set()
        assert sub.active is False
        assert sub.subscribers == set()

    def test_with_data_types(self):
        """Test subscription with data types"""
        contract = create_contract("SPY")
        sub = InstrumentSubscription(
            symbol="SPY",
            contract=contract,
            data_types={DataType.TICK, DataType.BAR_1MIN},
        )

        assert DataType.TICK in sub.data_types
        assert DataType.BAR_1MIN in sub.data_types

    def test_with_subscribers(self):
        """Test subscription with subscribers"""
        contract = create_contract("SPY")
        sub = InstrumentSubscription(
            symbol="SPY",
            contract=contract,
            subscribers={"algo1", "algo2"},
        )

        assert "algo1" in sub.subscribers
        assert "algo2" in sub.subscribers


class TestDataBuffer:
    """Tests for DataBuffer dataclass"""

    def test_default_buffer_sizes(self):
        """Test default buffer sizes"""
        buffer = DataBuffer()

        assert buffer.max_ticks == 10000
        assert buffer.max_bars == 1000
        assert len(buffer.ticks) == 0
        assert len(buffer.bars_5sec) == 0

    def test_custom_buffer_sizes(self):
        """Test custom buffer sizes"""
        buffer = DataBuffer(max_ticks=500, max_bars=100)

        # Add items to test maxlen
        for i in range(600):
            buffer.ticks.append(i)

        # Should be limited to 500
        assert len(buffer.ticks) == 500

    def test_buffer_maxlen_enforced(self):
        """Test that buffer maxlen is enforced"""
        buffer = DataBuffer(max_ticks=10, max_bars=5)

        # Add more than max items
        for i in range(20):
            buffer.bars_5sec.append(f"bar_{i}")

        # Should only have last 5
        assert len(buffer.bars_5sec) == 5


class TestBarAggregator:
    """Tests for BarAggregator class"""

    def test_aggregator_creation(self):
        """Test creating a BarAggregator"""
        agg = BarAggregator("SPY")
        assert agg.symbol == "SPY"

    def test_aggregate_first_bar(self):
        """Test adding first bar"""
        agg = BarAggregator("SPY")

        bar = create_bar("SPY", "2024-01-15T10:00:00", 450.0, 451.0, 449.0, 450.5)
        result = agg.add_bar(bar)

        # First bar should not complete any aggregated bars
        assert result[DataType.BAR_1MIN] is None
        assert result[DataType.BAR_5MIN] is None

    def test_aggregate_1min_bar(self):
        """Test 1-minute bar aggregation"""
        agg = BarAggregator("SPY")

        # Add bars at 10:00:00 to 10:00:55 (12 5-second bars)
        for i in range(12):
            ts = f"2024-01-15T10:00:{i*5:02d}"
            bar = create_bar("SPY", ts, 450.0 + i, 451.0 + i, 449.0, 450.5 + i, 100)
            result = agg.add_bar(bar)

        # Add bar at next minute boundary to complete the 1-min bar
        bar = create_bar("SPY", "2024-01-15T10:01:00", 462.0, 463.0, 461.0, 462.5, 100)
        result = agg.add_bar(bar)

        # Should have completed 1-minute bar
        assert result[DataType.BAR_1MIN] is not None
        completed = result[DataType.BAR_1MIN]
        assert completed.symbol == "SPY"
        assert completed.open == 450.0  # First bar's open
        assert completed.close == 461.5  # Last bar before boundary

    def test_aggregate_5min_bar(self):
        """Test 5-minute bar aggregation"""
        agg = BarAggregator("SPY")

        # Add bars for 5 minutes
        for minute in range(5):
            for sec in range(0, 60, 5):
                ts = f"2024-01-15T10:{minute:02d}:{sec:02d}"
                bar = create_bar("SPY", ts, 450.0, 451.0, 449.0, 450.5, 100)
                agg.add_bar(bar)

        # Add bar at 5-minute boundary
        bar = create_bar("SPY", "2024-01-15T10:05:00", 450.0, 451.0, 449.0, 450.5)
        result = agg.add_bar(bar)

        assert result[DataType.BAR_5MIN] is not None

    def test_aggregate_high_low_tracking(self):
        """Test that high/low are tracked correctly across bars"""
        agg = BarAggregator("SPY")

        # Add bars with varying high/low
        bars_data = [
            ("2024-01-15T10:00:00", 450.0, 455.0, 449.0, 452.0),
            ("2024-01-15T10:00:05", 452.0, 458.0, 450.0, 456.0),  # Higher high
            ("2024-01-15T10:00:10", 456.0, 457.0, 445.0, 448.0),  # Lower low
        ]

        for ts, o, h, l, c in bars_data:
            bar = create_bar("SPY", ts, o, h, l, c)
            agg.add_bar(bar)

        # Complete the bar
        bar = create_bar("SPY", "2024-01-15T10:01:00", 450.0, 451.0, 449.0, 450.0)
        result = agg.add_bar(bar)

        completed = result[DataType.BAR_1MIN]
        assert completed.high == 458.0  # Highest high
        assert completed.low == 445.0   # Lowest low

    def test_aggregate_volume_summed(self):
        """Test that volume is summed across bars"""
        agg = BarAggregator("SPY")

        # Add 3 bars with known volumes
        for i, vol in enumerate([100, 200, 300]):
            ts = f"2024-01-15T10:00:{i*5:02d}"
            bar = create_bar("SPY", ts, 450.0, 451.0, 449.0, 450.5, vol)
            agg.add_bar(bar)

        # Complete the bar
        bar = create_bar("SPY", "2024-01-15T10:01:00", 450.0, 451.0, 449.0, 450.0, 50)
        result = agg.add_bar(bar)

        completed = result[DataType.BAR_1MIN]
        assert completed.volume == 600  # 100 + 200 + 300

    def test_get_boundary(self):
        """Test boundary calculation"""
        agg = BarAggregator("SPY")

        ts = datetime(2024, 1, 15, 10, 23, 45)

        boundary_1min = agg._get_boundary(ts, 1)
        assert boundary_1min == datetime(2024, 1, 15, 10, 23, 0)

        boundary_5min = agg._get_boundary(ts, 5)
        assert boundary_5min == datetime(2024, 1, 15, 10, 20, 0)

        boundary_15min = agg._get_boundary(ts, 15)
        assert boundary_15min == datetime(2024, 1, 15, 10, 15, 0)


class TestDataFeedInit:
    """Tests for DataFeed initialization"""

    def test_default_initialization(self):
        """Test default initialization"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)

        assert feed.portfolio is portfolio
        assert feed.use_delayed_data is True
        assert feed.is_running is False
        assert len(feed.subscriptions) == 0

    def test_custom_delayed_data_setting(self):
        """Test custom delayed data setting"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio, use_delayed_data=False)

        assert feed.use_delayed_data is False

    def test_callbacks_initialized_none(self):
        """Test callbacks are initialized to None"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)

        assert feed.on_tick is None
        assert feed.on_bar is None
        assert feed.on_error is None


class TestDataFeedSubscribe:
    """Tests for subscription management"""

    def test_subscribe_new_symbol(self):
        """Test subscribing to a new symbol"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")

        result = feed.subscribe("SPY", contract, {DataType.TICK})

        assert result is True
        assert "SPY" in feed.subscriptions
        assert len(feed._buffers) == 1
        assert len(feed._aggregators) == 1

    def test_subscribe_with_subscriber_name(self):
        """Test subscribing with a specific subscriber name"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")

        feed.subscribe("SPY", contract, subscriber="algo1")

        assert "algo1" in feed._subscriptions["SPY"].subscribers

    def test_subscribe_multiple_subscribers_same_symbol(self):
        """Test multiple subscribers to the same symbol"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")

        feed.subscribe("SPY", contract, subscriber="algo1")
        feed.subscribe("SPY", contract, subscriber="algo2")

        sub = feed._subscriptions["SPY"]
        assert "algo1" in sub.subscribers
        assert "algo2" in sub.subscribers
        # Should still be only one buffer
        assert len(feed._buffers) == 1

    def test_subscribe_default_data_types(self):
        """Test default data types when none specified"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")

        feed.subscribe("SPY", contract)

        sub = feed._subscriptions["SPY"]
        assert DataType.TICK in sub.data_types
        assert DataType.BAR_5SEC in sub.data_types

    def test_subscribe_starts_stream_when_running(self):
        """Test that subscribe starts stream when feed is running"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        feed._running = True

        contract = create_contract("SPY")
        feed.subscribe("SPY", contract, {DataType.TICK, DataType.BAR_5SEC})

        assert portfolio.stream_symbol.called
        assert portfolio.bar_stream_symbol.called


class TestDataFeedUnsubscribe:
    """Tests for unsubscription management"""

    def test_unsubscribe_removes_subscriber(self):
        """Test unsubscribing removes the subscriber"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")

        feed.subscribe("SPY", contract, subscriber="algo1")
        feed.subscribe("SPY", contract, subscriber="algo2")

        feed.unsubscribe("SPY", subscriber="algo1")

        sub = feed._subscriptions["SPY"]
        assert "algo1" not in sub.subscribers
        assert "algo2" in sub.subscribers

    def test_unsubscribe_all_removes_subscription(self):
        """Test unsubscribing last subscriber removes subscription"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")

        feed.subscribe("SPY", contract, subscriber="algo1")
        feed.unsubscribe("SPY", subscriber="algo1")

        assert "SPY" not in feed._subscriptions
        assert "SPY" not in feed._buffers

    def test_unsubscribe_nonexistent_symbol(self):
        """Test unsubscribing from nonexistent symbol"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)

        # Should not raise
        feed.unsubscribe("NONEXISTENT")


class TestDataFeedStartStop:
    """Tests for starting and stopping the feed"""

    def test_start_success(self):
        """Test successful start"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)

        result = feed.start()

        assert result is True
        assert feed.is_running is True
        assert feed._subscriptions["SPY"].active is True

    def test_start_when_already_running(self):
        """Test start when already running"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        feed._running = True

        result = feed.start()

        assert result is True

    def test_start_when_disconnected(self):
        """Test start fails when portfolio not connected"""
        portfolio = create_mock_portfolio()
        portfolio.connected = False
        feed = DataFeed(portfolio)

        result = feed.start()

        assert result is False

    def test_stop(self):
        """Test stopping the feed"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)
        feed.start()

        feed.stop()

        assert feed.is_running is False
        assert feed._subscriptions["SPY"].active is False

    def test_stop_when_not_running(self):
        """Test stop when not running does nothing"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)

        # Should not raise
        feed.stop()


class TestDataFeedTickHandling:
    """Tests for tick data handling"""

    def test_handle_tick_buffers_data(self):
        """Test that ticks are buffered"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract, {DataType.TICK})

        feed._handle_tick("SPY", 450.50, "LAST")

        assert len(feed._buffers["SPY"].ticks) == 1
        tick = feed._buffers["SPY"].ticks[0]
        assert tick.price == 450.50
        assert tick.tick_type == "LAST"

    def test_handle_tick_invokes_callback(self):
        """Test that tick callback is invoked"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract, {DataType.TICK})

        callback_data = []
        feed.on_tick = lambda s, t: callback_data.append((s, t))

        feed._handle_tick("SPY", 450.50, "LAST")

        assert len(callback_data) == 1
        assert callback_data[0][0] == "SPY"
        assert callback_data[0][1].price == 450.50

    def test_handle_tick_updates_stats(self):
        """Test that stats are updated"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)

        feed._handle_tick("SPY", 450.50, "LAST")

        assert feed.stats["ticks_received"] == 1


class TestDataFeedTickSizeHandling:
    """Tests for _handle_tick_size"""

    def test_handle_tick_size_buffers_data(self):
        """Test that size ticks are buffered"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract, {DataType.TICK})

        feed._handle_tick_size("SPY", 500, "LAST_SIZE")

        assert len(feed._buffers["SPY"].ticks) == 1
        tick = feed._buffers["SPY"].ticks[0]
        assert tick.size == 500
        assert tick.tick_type == "LAST_SIZE"
        assert tick.price == 0.0

    def test_handle_tick_size_calls_on_tick_callback(self):
        """Test that on_tick is called with TickData containing size"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract, {DataType.TICK})

        callback_data = []
        feed.on_tick = lambda s, t: callback_data.append((s, t))

        feed._handle_tick_size("SPY", 200, "BID_SIZE")

        assert len(callback_data) == 1
        assert callback_data[0][0] == "SPY"
        tick = callback_data[0][1]
        assert tick.size == 200
        assert tick.tick_type == "BID_SIZE"
        assert tick.price == 0.0

    def test_handle_tick_size_updates_stats(self):
        """Test that stats are updated for size ticks"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)

        feed._handle_tick_size("SPY", 1000, "VOLUME")

        assert feed.stats["ticks_received"] == 1


class TestDataFeedBarHandling:
    """Tests for bar data handling"""

    def test_handle_bar_buffers_data(self):
        """Test that bars are buffered"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract, {DataType.BAR_5SEC})

        bar = create_bar("SPY", "2024-01-15T10:00:00", 450.0, 451.0, 449.0, 450.5)
        feed._handle_bar(bar)

        assert len(feed._buffers["SPY"].bars_5sec) == 1

    def test_handle_bar_invokes_callback(self):
        """Test that bar callback is invoked"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract, {DataType.BAR_5SEC})

        callback_data = []
        feed.on_bar = lambda s, b, t: callback_data.append((s, b, t))

        bar = create_bar("SPY", "2024-01-15T10:00:00", 450.0, 451.0, 449.0, 450.5)
        feed._handle_bar(bar)

        assert len(callback_data) == 1
        assert callback_data[0][0] == "SPY"
        assert callback_data[0][2] == DataType.BAR_5SEC

    def test_handle_bar_aggregates_to_larger_timeframes(self):
        """Test that bars are aggregated"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract, {DataType.BAR_5SEC, DataType.BAR_1MIN})

        aggregated_bars = []
        feed.on_bar = lambda s, b, t: aggregated_bars.append((s, b, t))

        # Add enough bars to complete a 1-minute bar
        for i in range(12):
            ts = f"2024-01-15T10:00:{i*5:02d}"
            bar = create_bar("SPY", ts, 450.0, 451.0, 449.0, 450.5)
            feed._handle_bar(bar)

        # Add bar at next minute to trigger completion
        bar = create_bar("SPY", "2024-01-15T10:01:00", 450.0, 451.0, 449.0, 450.5)
        feed._handle_bar(bar)

        # Should have 1-minute bars
        one_min_bars = [b for s, b, t in aggregated_bars if t == DataType.BAR_1MIN]
        assert len(one_min_bars) >= 1


class TestDataFeedDataAccess:
    """Tests for data access methods"""

    def test_get_ticks(self):
        """Test getting ticks"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)

        # Add some ticks
        for i in range(10):
            feed._handle_tick("SPY", 450.0 + i, "LAST")

        ticks = feed.get_ticks("SPY")
        assert len(ticks) == 10

    def test_get_ticks_with_count(self):
        """Test getting ticks with count limit"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)

        for i in range(10):
            feed._handle_tick("SPY", 450.0 + i, "LAST")

        ticks = feed.get_ticks("SPY", count=5)
        assert len(ticks) == 5

    def test_get_ticks_nonexistent_symbol(self):
        """Test getting ticks for nonexistent symbol"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)

        ticks = feed.get_ticks("NONEXISTENT")
        assert ticks == []

    def test_get_bars(self):
        """Test getting bars"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)

        # Add some bars
        for i in range(5):
            ts = f"2024-01-15T10:00:{i*5:02d}"
            bar = create_bar("SPY", ts, 450.0 + i, 451.0, 449.0, 450.5)
            feed._handle_bar(bar)

        bars = feed.get_bars("SPY", DataType.BAR_5SEC)
        assert len(bars) == 5

    def test_get_bars_with_count(self):
        """Test getting bars with count limit"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)

        for i in range(10):
            ts = f"2024-01-15T10:00:{i*5:02d}"
            bar = create_bar("SPY", ts, 450.0, 451.0, 449.0, 450.5)
            feed._handle_bar(bar)

        bars = feed.get_bars("SPY", DataType.BAR_5SEC, count=3)
        assert len(bars) == 3

    def test_get_last_tick(self):
        """Test getting last tick"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)

        feed._handle_tick("SPY", 450.0, "LAST")
        feed._handle_tick("SPY", 451.0, "LAST")
        feed._handle_tick("SPY", 452.0, "LAST")

        tick = feed.get_last_tick("SPY")
        assert tick.price == 452.0

    def test_get_last_tick_empty(self):
        """Test getting last tick when none exist"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)

        tick = feed.get_last_tick("SPY")
        assert tick is None

    def test_get_last_bar(self):
        """Test getting last bar"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)

        for i in range(3):
            ts = f"2024-01-15T10:00:{i*5:02d}"
            bar = create_bar("SPY", ts, 450.0 + i, 451.0, 449.0, 450.5)
            feed._handle_bar(bar)

        last_bar = feed.get_last_bar("SPY", DataType.BAR_5SEC)
        assert last_bar.open == 452.0  # Last bar's open

    def test_get_last_price_from_tick(self):
        """Test getting last price from tick"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)

        feed._handle_tick("SPY", 450.50, "LAST")

        price = feed.get_last_price("SPY")
        assert price == 450.50

    def test_get_last_price_from_bar(self):
        """Test getting last price from bar when no tick"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)

        bar = create_bar("SPY", "2024-01-15T10:00:00", 450.0, 451.0, 449.0, 450.75)
        feed._handle_bar(bar)

        price = feed.get_last_price("SPY")
        assert price == 450.75  # Bar's close


class TestDataFeedClearBuffers:
    """Tests for clearing buffers"""

    def test_clear_all_buffers(self):
        """Test clearing all buffers"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)

        feed.subscribe("SPY", create_contract("SPY"))
        feed.subscribe("AAPL", create_contract("AAPL"))

        feed._handle_tick("SPY", 450.0, "LAST")
        feed._handle_tick("AAPL", 180.0, "LAST")

        feed.clear_buffers()

        assert len(feed._buffers["SPY"].ticks) == 0
        assert len(feed._buffers["AAPL"].ticks) == 0

    def test_clear_specific_symbol(self):
        """Test clearing specific symbol's buffer"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)

        feed.subscribe("SPY", create_contract("SPY"))
        feed.subscribe("AAPL", create_contract("AAPL"))

        feed._handle_tick("SPY", 450.0, "LAST")
        feed._handle_tick("AAPL", 180.0, "LAST")

        feed.clear_buffers("SPY")

        assert len(feed._buffers["SPY"].ticks) == 0
        assert len(feed._buffers["AAPL"].ticks) == 1


class TestDataFeedStatus:
    """Tests for status reporting"""

    def test_get_status(self):
        """Test getting feed status"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract, {DataType.TICK, DataType.BAR_1MIN})

        feed._handle_tick("SPY", 450.0, "LAST")

        status = feed.get_status()

        assert "running" in status
        assert "subscriptions" in status
        assert "stats" in status
        assert "SPY" in status["subscriptions"]
        assert status["subscriptions"]["SPY"]["buffer_sizes"]["ticks"] == 1

    def test_stats_property(self):
        """Test stats property returns copy"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)

        stats1 = feed.stats
        stats1["ticks_received"] = 999

        stats2 = feed.stats
        assert stats2["ticks_received"] != 999


class TestDataFeedCallbackErrors:
    """Tests for callback error handling"""

    def test_tick_callback_error_handled(self):
        """Test that tick callback errors are handled"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)

        def bad_callback(s, t):
            raise Exception("Callback error")

        feed.on_tick = bad_callback

        # Should not raise
        feed._handle_tick("SPY", 450.0, "LAST")
        assert feed.stats["errors"] == 1

    def test_bar_callback_error_handled(self):
        """Test that bar callback errors are handled"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract, {DataType.BAR_5SEC})

        def bad_callback(s, b, t):
            raise Exception("Callback error")

        feed.on_bar = bad_callback

        bar = create_bar("SPY", "2024-01-15T10:00:00", 450.0, 451.0, 449.0, 450.5)
        feed._handle_bar(bar)

        assert feed.stats["errors"] == 1

    def test_error_callback_invoked(self):
        """Test that error callback is invoked on callback error"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)

        errors = []
        feed.on_tick = lambda s, t: 1/0  # Will raise
        feed.on_error = lambda s, e: errors.append((s, e))

        feed._handle_tick("SPY", 450.0, "LAST")

        assert len(errors) == 1
        assert errors[0][0] == "SPY"


class TestThreadSafety:
    """Tests for thread safety"""

    def test_concurrent_subscribe(self):
        """Test concurrent subscriptions are thread-safe"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        errors = []

        def subscribe_symbols(prefix, count):
            try:
                for i in range(count):
                    symbol = f"{prefix}{i}"
                    feed.subscribe(symbol, create_contract(symbol))
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=subscribe_symbols, args=(f"SYM{t}_", 20))
            for t in range(5)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(feed.subscriptions) == 100  # 5 threads * 20 symbols

    def test_concurrent_tick_handling(self):
        """Test concurrent tick handling is thread-safe"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)
        errors = []

        def add_ticks(count):
            try:
                for i in range(count):
                    feed._handle_tick("SPY", 450.0 + i * 0.01, "LAST")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=add_ticks, args=(100,)) for _ in range(5)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert feed.stats["ticks_received"] == 500  # 5 threads * 100 ticks


class TestResetStats:
    """Tests for reset_stats functionality"""

    def test_reset_stats_clears_counters(self):
        """Test reset_stats clears all counters"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)

        # Generate some activity
        for i in range(10):
            feed._handle_tick("SPY", 450.0 + i, "LAST")

        # Verify stats have values
        assert feed.stats["ticks_received"] == 10

        # Reset stats
        feed.reset_stats()

        # Counters should be zero
        assert feed.stats["ticks_received"] == 0
        assert feed.stats["bars_received"] == 0
        assert feed.stats["bars_aggregated"] == 0
        assert feed.stats["errors"] == 0

    def test_reset_stats_preserves_started_at(self):
        """Test reset_stats preserves started_at timestamp"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)
        contract = create_contract("SPY")
        feed.subscribe("SPY", contract)
        feed.start()

        original_started_at = feed.stats["started_at"]
        assert original_started_at is not None

        feed.reset_stats()

        assert feed.stats["started_at"] == original_started_at

    def test_reset_stats_sets_last_reset(self):
        """Test reset_stats sets last_reset timestamp"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)

        assert feed.stats["last_reset"] is None

        feed.reset_stats()

        assert feed.stats["last_reset"] is not None

    def test_reset_stats_updates_last_reset(self):
        """Test reset_stats updates last_reset on each call"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)

        feed.reset_stats()
        first_reset = feed.stats["last_reset"]

        # Small delay to ensure different timestamps
        import time
        time.sleep(0.01)

        feed.reset_stats()
        second_reset = feed.stats["last_reset"]

        assert first_reset != second_reset

    def test_stats_include_last_reset(self):
        """Test stats include last_reset field"""
        portfolio = create_mock_portfolio()
        feed = DataFeed(portfolio)

        stats = feed.stats

        assert "last_reset" in stats
