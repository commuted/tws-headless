"""
data_feed.py - Real-time market data streaming

Provides a data feed system that:
- Subscribes to market data for instruments
- Buffers and aggregates data for plugins
- Routes data to registered subscribers
- Supports multiple data types (ticks, bars, aggregated bars)
- Handles data during reconnection gracefully
"""

import logging
from threading import Thread, Event, Lock, RLock
from typing import Optional, Callable, Dict, List, Set, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import deque
from enum import Enum
import time

from ibapi.contract import Contract

from .models import Bar, BarSize

logger = logging.getLogger(__name__)


class DataType(Enum):
    """Types of market data"""
    TICK = "tick"
    BAR_5SEC = "bar_5sec"
    BAR_1MIN = "bar_1min"
    BAR_5MIN = "bar_5min"
    BAR_15MIN = "bar_15min"
    BAR_1HOUR = "bar_1hour"
    TICK_BY_TICK_LAST = "tick_by_tick_last"
    TICK_BY_TICK_BIDASK = "tick_by_tick_bidask"
    TICK_BY_TICK_MIDPOINT = "tick_by_tick_midpoint"
    MARKET_DEPTH = "market_depth"


@dataclass
class TickData:
    """A single tick update"""
    symbol: str
    price: float
    tick_type: str
    size: Optional[int] = None  # set for size ticks (BID_SIZE, ASK_SIZE, LAST_SIZE, VOLUME)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class TickByTickData:
    """A single tick-by-tick event from reqTickByTick"""
    symbol: str
    tick_type: str       # "Last", "AllLast", "BidAsk", "MidPoint"
    timestamp: datetime
    # Last / AllLast fields
    price: float = 0.0
    size: int = 0
    exchange: str = ""
    special_conditions: str = ""
    past_limit: bool = False
    unreported: bool = False
    # BidAsk fields
    bid_price: float = 0.0
    ask_price: float = 0.0
    bid_size: int = 0
    ask_size: int = 0
    bid_past_low: bool = False
    ask_past_high: bool = False
    # MidPoint field
    mid_point: float = 0.0


@dataclass
class DepthLevel:
    """One price level in an order book"""
    price: float
    size: int
    market_maker: str = ""  # populated for L2 (reqMktDepthL2)


@dataclass
class MarketDepth:
    """L2 order book snapshot for a symbol"""
    symbol: str
    bids: List["DepthLevel"]   # best bid first (highest price)
    asks: List["DepthLevel"]   # best ask first (lowest price)
    timestamp: datetime = field(default_factory=datetime.now)
    is_smart_depth: bool = False


@dataclass
class InstrumentSubscription:
    """Subscription for an instrument's data"""
    symbol: str
    contract: Contract
    data_types: Set[DataType] = field(default_factory=set)
    what_to_show: str = "TRADES"
    use_rth: bool = True
    active: bool = False
    subscribed_at: datetime = field(default_factory=datetime.now)
    # Track which algorithms/subscribers want this data
    subscribers: Set[str] = field(default_factory=set)
    # Per-subtype active flags for tick-by-tick and depth
    tbt_last_active: bool = False
    tbt_bidask_active: bool = False
    tbt_midpoint_active: bool = False
    depth_active: bool = False


@dataclass
class DataBuffer:
    """
    Circular buffer for market data.

    Stores recent data points for an instrument, with configurable
    maximum size to prevent memory issues.
    """
    max_ticks: int = 10000
    max_bars: int = 1000

    ticks: deque = field(default_factory=lambda: deque(maxlen=10000))
    bars_5sec: deque = field(default_factory=lambda: deque(maxlen=1000))
    bars_1min: deque = field(default_factory=lambda: deque(maxlen=1000))
    bars_5min: deque = field(default_factory=lambda: deque(maxlen=500))
    bars_15min: deque = field(default_factory=lambda: deque(maxlen=200))
    bars_1hour: deque = field(default_factory=lambda: deque(maxlen=100))
    tbt_ticks: deque = field(default_factory=lambda: deque(maxlen=10000))
    depth: Optional["MarketDepth"] = None

    def __post_init__(self):
        # Ensure maxlen is set
        self.ticks = deque(maxlen=self.max_ticks)
        self.bars_5sec = deque(maxlen=self.max_bars)
        self.bars_1min = deque(maxlen=self.max_bars)
        self.bars_5min = deque(maxlen=self.max_bars // 2)
        self.bars_15min = deque(maxlen=self.max_bars // 5)
        self.bars_1hour = deque(maxlen=self.max_bars // 10)
        self.tbt_ticks = deque(maxlen=10000)


class BarAggregator:
    """
    Aggregates 5-second bars into larger timeframes.

    IB only provides 5-second real-time bars, so we aggregate
    them into 1-minute, 5-minute, etc. bars for plugins.
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        self._lock = Lock()

        # Current bars being built
        self._current_1min: Optional[Bar] = None
        self._current_5min: Optional[Bar] = None
        self._current_15min: Optional[Bar] = None
        self._current_1hour: Optional[Bar] = None

        # Track bar boundaries
        self._last_1min_boundary: Optional[datetime] = None
        self._last_5min_boundary: Optional[datetime] = None
        self._last_15min_boundary: Optional[datetime] = None
        self._last_1hour_boundary: Optional[datetime] = None

    def add_bar(self, bar: Bar) -> Dict[DataType, Optional[Bar]]:
        """
        Add a 5-second bar and return any completed aggregated bars.

        Args:
            bar: The 5-second bar to add

        Returns:
            Dictionary mapping DataType to completed Bar (or None if not complete)
        """
        with self._lock:
            completed = {
                DataType.BAR_1MIN: None,
                DataType.BAR_5MIN: None,
                DataType.BAR_15MIN: None,
                DataType.BAR_1HOUR: None,
            }

            try:
                ts = datetime.fromisoformat(bar.timestamp)
            except (ValueError, TypeError):
                ts = datetime.now()

            # Aggregate into each timeframe
            completed[DataType.BAR_1MIN] = self._aggregate_1min(bar, ts)
            completed[DataType.BAR_5MIN] = self._aggregate_5min(bar, ts)
            completed[DataType.BAR_15MIN] = self._aggregate_15min(bar, ts)
            completed[DataType.BAR_1HOUR] = self._aggregate_1hour(bar, ts)

            return completed

    def _get_boundary(self, ts: datetime, minutes: int) -> datetime:
        """Get the bar boundary for a given timestamp and minute interval"""
        return ts.replace(
            minute=(ts.minute // minutes) * minutes,
            second=0,
            microsecond=0
        )

    def _aggregate_bar(
        self,
        bar: Bar,
        ts: datetime,
        minutes: int,
        current_bar: Optional[Bar],
        last_boundary: Optional[datetime],
    ) -> Tuple[Optional[Bar], Optional[Bar], datetime]:
        """
        Generic bar aggregation logic.

        Returns:
            Tuple of (completed_bar, new_current_bar, new_boundary)
        """
        boundary = self._get_boundary(ts, minutes)
        completed = None

        if last_boundary is None or boundary > last_boundary:
            # New bar period - return previous bar if exists
            if current_bar is not None:
                completed = current_bar

            # Start new bar
            current_bar = Bar(
                symbol=self.symbol,
                timestamp=boundary.isoformat(),
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                wap=bar.wap,
                bar_count=1,
            )
        else:
            # Update current bar
            if current_bar is not None:
                current_bar.high = max(current_bar.high, bar.high)
                current_bar.low = min(current_bar.low, bar.low)
                current_bar.close = bar.close
                current_bar.volume += bar.volume
                current_bar.bar_count += 1
                if bar.wap > 0:
                    # Weighted average of WAP
                    current_bar.wap = (
                        (current_bar.wap * (current_bar.bar_count - 1) + bar.wap)
                        / current_bar.bar_count
                    )

        return completed, current_bar, boundary

    def _aggregate_1min(self, bar: Bar, ts: datetime) -> Optional[Bar]:
        """Aggregate into 1-minute bars"""
        completed, self._current_1min, self._last_1min_boundary = (
            self._aggregate_bar(
                bar, ts, 1, self._current_1min, self._last_1min_boundary
            )
        )
        return completed

    def _aggregate_5min(self, bar: Bar, ts: datetime) -> Optional[Bar]:
        """Aggregate into 5-minute bars"""
        completed, self._current_5min, self._last_5min_boundary = (
            self._aggregate_bar(
                bar, ts, 5, self._current_5min, self._last_5min_boundary
            )
        )
        return completed

    def _aggregate_15min(self, bar: Bar, ts: datetime) -> Optional[Bar]:
        """Aggregate into 15-minute bars"""
        completed, self._current_15min, self._last_15min_boundary = (
            self._aggregate_bar(
                bar, ts, 15, self._current_15min, self._last_15min_boundary
            )
        )
        return completed

    def _aggregate_1hour(self, bar: Bar, ts: datetime) -> Optional[Bar]:
        """Aggregate into 1-hour bars"""
        boundary = ts.replace(minute=0, second=0, microsecond=0)
        completed = None

        if self._last_1hour_boundary is None or boundary > self._last_1hour_boundary:
            if self._current_1hour is not None:
                completed = self._current_1hour

            self._current_1hour = Bar(
                symbol=self.symbol,
                timestamp=boundary.isoformat(),
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                wap=bar.wap,
                bar_count=1,
            )
            self._last_1hour_boundary = boundary
        else:
            if self._current_1hour is not None:
                self._current_1hour.high = max(self._current_1hour.high, bar.high)
                self._current_1hour.low = min(self._current_1hour.low, bar.low)
                self._current_1hour.close = bar.close
                self._current_1hour.volume += bar.volume
                self._current_1hour.bar_count += 1

        return completed


class DataFeed:
    """
    Real-time market data feed.

    Provides a unified interface for plugins to receive market data.
    Handles subscription management, data buffering, bar aggregation,
    and routing data to registered handlers.

    Usage:
        feed = DataFeed(portfolio)

        # Register for data callbacks
        feed.on_tick = lambda symbol, tick: algo.process_tick(tick)
        feed.on_bar = lambda symbol, bar, timeframe: algo.process_bar(bar)

        # Subscribe to instruments
        feed.subscribe("AAPL", contract, {DataType.TICK, DataType.BAR_1MIN})

        # Start the feed
        feed.start()

        # Get buffered data
        bars = feed.get_bars("AAPL", DataType.BAR_1MIN, count=100)
    """

    def __init__(self, portfolio, use_delayed_data: bool = True):
        """
        Initialize data feed.

        Args:
            portfolio: Portfolio instance for market data
            use_delayed_data: Use delayed (free) vs live market data
        """
        self.portfolio = portfolio
        self.use_delayed_data = use_delayed_data

        # State
        self._running = False
        self._lock = RLock()

        # Subscriptions
        self._subscriptions: Dict[str, InstrumentSubscription] = {}

        # Data buffers per symbol
        self._buffers: Dict[str, DataBuffer] = {}

        # Bar aggregators per symbol
        self._aggregators: Dict[str, BarAggregator] = {}

        # Callbacks
        self.on_tick: Optional[Callable[[str, TickData], None]] = None
        self.on_bar: Optional[Callable[[str, Bar, DataType], None]] = None
        self.on_error: Optional[Callable[[str, Exception], None]] = None
        self.on_tick_by_tick: Optional[Callable[[str, "TickByTickData"], None]] = None
        self.on_depth: Optional[Callable[[str, "MarketDepth"], None]] = None

        # Statistics
        self._stats = {
            "ticks_received": 0,
            "bars_received": 0,
            "bars_aggregated": 0,
            "errors": 0,
            "started_at": None,
            "last_reset": None,
        }

    @property
    def is_running(self) -> bool:
        """Check if feed is running"""
        return self._running

    @property
    def subscriptions(self) -> List[str]:
        """Get list of subscribed symbols"""
        with self._lock:
            return list(self._subscriptions.keys())

    @property
    def stats(self) -> Dict[str, Any]:
        """Get feed statistics"""
        return self._stats.copy()

    def subscribe(
        self,
        symbol: str,
        contract: Contract,
        data_types: Optional[Set[DataType]] = None,
        subscriber: str = "default",
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> bool:
        """
        Subscribe to market data for an instrument.

        Multiple subscribers can subscribe to the same symbol - only one
        IB stream is created, and data is routed to all subscribers.

        Args:
            symbol: Symbol identifier
            contract: IB Contract
            data_types: Set of data types to subscribe to
                       (defaults to TICK and BAR_5SEC)
            subscriber: Subscriber identifier (e.g., algorithm name)
            what_to_show: IB data type (TRADES, MIDPOINT, BID, ASK)
            use_rth: Regular trading hours only

        Returns:
            True if subscription added successfully
        """
        if data_types is None:
            data_types = {DataType.TICK, DataType.BAR_5SEC}

        with self._lock:
            # Create or update subscription
            if symbol in self._subscriptions:
                sub = self._subscriptions[symbol]
                sub.data_types.update(data_types)
                sub.subscribers.add(subscriber)
                # Warn if settings differ from first subscriber
                if sub.what_to_show != what_to_show:
                    logger.warning(
                        f"Subscriber '{subscriber}' requested what_to_show='{what_to_show}' "
                        f"for {symbol}, but existing stream uses '{sub.what_to_show}'"
                    )
                if sub.use_rth != use_rth:
                    logger.warning(
                        f"Subscriber '{subscriber}' requested use_rth={use_rth} "
                        f"for {symbol}, but existing stream uses use_rth={sub.use_rth}"
                    )
                logger.debug(f"Added subscriber '{subscriber}' to existing {symbol} stream")
            else:
                sub = InstrumentSubscription(
                    symbol=symbol,
                    contract=contract,
                    data_types=data_types,
                    what_to_show=what_to_show,
                    use_rth=use_rth,
                    subscribers={subscriber},
                )
                self._subscriptions[symbol] = sub

                # Create buffer and aggregator
                self._buffers[symbol] = DataBuffer()
                self._aggregators[symbol] = BarAggregator(symbol)

            # Start subscription if running (only starts once per symbol)
            if self._running and not sub.active:
                self._start_subscription(symbol, sub)

            logger.info(f"Subscribed '{subscriber}' to {symbol}: {[d.value for d in data_types]}")
            return True

    def unsubscribe(self, symbol: str, subscriber: str = "default"):
        """
        Unsubscribe from market data for an instrument.

        Only stops the IB stream when all subscribers have unsubscribed.

        Args:
            symbol: Symbol to unsubscribe
            subscriber: Subscriber identifier to remove
        """
        with self._lock:
            if symbol not in self._subscriptions:
                return

            sub = self._subscriptions[symbol]

            # Remove subscriber
            sub.subscribers.discard(subscriber)
            logger.debug(f"Removed subscriber '{subscriber}' from {symbol}")

            # Only stop stream if no subscribers remain
            if not sub.subscribers:
                # Stop subscriptions
                if sub.active:
                    self._stop_subscription(symbol, sub)

                # Clean up
                del self._subscriptions[symbol]
                if symbol in self._buffers:
                    del self._buffers[symbol]
                if symbol in self._aggregators:
                    del self._aggregators[symbol]

                logger.info(f"Unsubscribed from {symbol} (no subscribers remain)")
            else:
                logger.debug(f"{symbol} still has {len(sub.subscribers)} subscriber(s)")

    def start(self) -> bool:
        """
        Start the data feed.

        Activates all subscriptions and begins receiving data.

        Returns:
            True if started successfully
        """
        if self._running:
            logger.warning("Data feed already running")
            return True

        if not self.portfolio.connected:
            logger.error("Portfolio not connected - cannot start feed")
            return False

        self._running = True
        self._stats["started_at"] = datetime.now().isoformat()

        # Set up portfolio callbacks
        self._setup_callbacks()

        # Start all subscriptions
        with self._lock:
            for symbol, sub in self._subscriptions.items():
                self._start_subscription(symbol, sub)

        logger.info(f"Data feed started with {len(self._subscriptions)} subscriptions")
        return True

    def stop(self):
        """
        Stop the data feed.

        Deactivates all subscriptions and stops receiving data.
        Data buffers are preserved.
        """
        if not self._running:
            return

        self._running = False

        # Stop all subscriptions
        with self._lock:
            for symbol, sub in self._subscriptions.items():
                if sub.active:
                    self._stop_subscription(symbol, sub)

        logger.info("Data feed stopped")

    def _setup_callbacks(self):
        """Set up callbacks on portfolio for data routing"""
        # Store original callbacks
        self._original_on_tick = self.portfolio._on_tick
        self._original_on_tick_size = getattr(self.portfolio, '_on_tick_size', None)
        self._original_on_bar = self.portfolio._on_bar
        self._original_on_tick_by_tick = getattr(self.portfolio, '_on_tick_by_tick', None)
        self._original_on_depth = getattr(self.portfolio, '_on_depth', None)

        # Set our handlers
        self.portfolio._on_tick = self._handle_tick
        self.portfolio._on_tick_size = self._handle_tick_size
        self.portfolio._on_bar = self._handle_bar
        self.portfolio._on_tick_by_tick = self._handle_tick_by_tick
        self.portfolio._on_depth = self._handle_depth

    def _teardown_callbacks(self):
        """Restore original portfolio callbacks"""
        if hasattr(self, '_original_on_tick'):
            self.portfolio._on_tick = self._original_on_tick
        if hasattr(self, '_original_on_tick_size'):
            self.portfolio._on_tick_size = self._original_on_tick_size
        if hasattr(self, '_original_on_bar'):
            self.portfolio._on_bar = self._original_on_bar
        if hasattr(self, '_original_on_tick_by_tick'):
            self.portfolio._on_tick_by_tick = self._original_on_tick_by_tick
        if hasattr(self, '_original_on_depth'):
            self.portfolio._on_depth = self._original_on_depth

    def _start_subscription(self, symbol: str, sub: InstrumentSubscription):
        """Start data subscription for a symbol"""
        if sub.active:
            return

        # Start tick stream if needed
        if DataType.TICK in sub.data_types:
            self.portfolio.stream_symbol(symbol, sub.contract)

        # Start bar stream if any bar types needed
        bar_types = {DataType.BAR_5SEC, DataType.BAR_1MIN, DataType.BAR_5MIN,
                    DataType.BAR_15MIN, DataType.BAR_1HOUR}
        if sub.data_types & bar_types:
            self.portfolio.bar_stream_symbol(
                symbol, sub.contract, sub.what_to_show, sub.use_rth
            )

        # Start tick-by-tick streams
        if DataType.TICK_BY_TICK_LAST in sub.data_types and not sub.tbt_last_active:
            self.portfolio.request_tick_by_tick(symbol, sub.contract, "Last")
            sub.tbt_last_active = True
        if DataType.TICK_BY_TICK_BIDASK in sub.data_types and not sub.tbt_bidask_active:
            self.portfolio.request_tick_by_tick(symbol, sub.contract, "BidAsk")
            sub.tbt_bidask_active = True
        if DataType.TICK_BY_TICK_MIDPOINT in sub.data_types and not sub.tbt_midpoint_active:
            self.portfolio.request_tick_by_tick(symbol, sub.contract, "MidPoint")
            sub.tbt_midpoint_active = True

        # Start market depth stream
        if DataType.MARKET_DEPTH in sub.data_types and not sub.depth_active:
            self.portfolio.request_market_depth(symbol, sub.contract)
            sub.depth_active = True

        sub.active = True
        logger.debug(f"Started subscription for {symbol}")

    def _stop_subscription(self, symbol: str, sub: InstrumentSubscription):
        """Stop data subscription for a symbol"""
        if not sub.active:
            return

        # Stop streams
        if DataType.TICK in sub.data_types:
            self.portfolio.unstream_symbol(symbol)

        bar_types = {DataType.BAR_5SEC, DataType.BAR_1MIN, DataType.BAR_5MIN,
                    DataType.BAR_15MIN, DataType.BAR_1HOUR}
        if sub.data_types & bar_types:
            self.portfolio.unstream_bar_symbol(symbol)

        # Stop tick-by-tick streams
        if sub.tbt_last_active or sub.tbt_bidask_active or sub.tbt_midpoint_active:
            self.portfolio.cancel_tick_by_tick(symbol)
            sub.tbt_last_active = False
            sub.tbt_bidask_active = False
            sub.tbt_midpoint_active = False

        # Stop market depth stream
        if sub.depth_active:
            self.portfolio.cancel_market_depth(symbol)
            sub.depth_active = False

        sub.active = False
        logger.debug(f"Stopped subscription for {symbol}")

    def _handle_tick(self, symbol: str, price: float, tick_type: str):
        """Handle incoming tick data"""
        self._stats["ticks_received"] += 1

        tick = TickData(
            symbol=symbol,
            price=price,
            tick_type=tick_type,
        )

        # Buffer the tick
        with self._lock:
            if symbol in self._buffers:
                self._buffers[symbol].ticks.append(tick)

        # Route to callback
        if self.on_tick:
            try:
                self.on_tick(symbol, tick)
            except Exception as e:
                self._stats["errors"] += 1
                logger.error(f"Error in tick callback for {symbol}: {e}")
                if self.on_error:
                    self.on_error(symbol, e)

        # Also call original callback if set
        if hasattr(self, '_original_on_tick') and self._original_on_tick:
            try:
                self._original_on_tick(symbol, price, tick_type)
            except Exception as e:
                logger.error(f"Error in original tick callback: {e}")

    def _handle_tick_size(self, symbol: str, size: int, tick_type: str):
        """Handle incoming size tick data"""
        self._stats["ticks_received"] += 1

        tick = TickData(
            symbol=symbol,
            price=0.0,
            tick_type=tick_type,
            size=size,
        )

        # Buffer the tick
        with self._lock:
            if symbol in self._buffers:
                self._buffers[symbol].ticks.append(tick)

        # Route to callback
        if self.on_tick:
            try:
                self.on_tick(symbol, tick)
            except Exception as e:
                self._stats["errors"] += 1
                logger.error(f"Error in size tick callback for {symbol}: {e}")
                if self.on_error:
                    self.on_error(symbol, e)

    def _handle_bar(self, bar: Bar):
        """Handle incoming 5-second bar data"""
        self._stats["bars_received"] += 1
        symbol = bar.symbol

        with self._lock:
            if symbol not in self._buffers:
                return

            buffer = self._buffers[symbol]
            aggregator = self._aggregators.get(symbol)

            # Buffer the 5-second bar
            buffer.bars_5sec.append(bar)

            # Route 5-second bar
            self._route_bar(symbol, bar, DataType.BAR_5SEC)

            # Aggregate into larger timeframes
            if aggregator:
                completed_bars = aggregator.add_bar(bar)

                for data_type, completed_bar in completed_bars.items():
                    if completed_bar is not None:
                        self._stats["bars_aggregated"] += 1

                        # Buffer aggregated bar
                        if data_type == DataType.BAR_1MIN:
                            buffer.bars_1min.append(completed_bar)
                        elif data_type == DataType.BAR_5MIN:
                            buffer.bars_5min.append(completed_bar)
                        elif data_type == DataType.BAR_15MIN:
                            buffer.bars_15min.append(completed_bar)
                        elif data_type == DataType.BAR_1HOUR:
                            buffer.bars_1hour.append(completed_bar)

                        # Route aggregated bar
                        self._route_bar(symbol, completed_bar, data_type)

        # Call original callback if set
        if hasattr(self, '_original_on_bar') and self._original_on_bar:
            try:
                self._original_on_bar(bar)
            except Exception as e:
                logger.error(f"Error in original bar callback: {e}")

    def _route_bar(self, symbol: str, bar: Bar, data_type: DataType):
        """Route a bar to the callback if subscribed"""
        with self._lock:
            sub = self._subscriptions.get(symbol)
            if not sub or data_type not in sub.data_types:
                return

        if self.on_bar:
            try:
                self.on_bar(symbol, bar, data_type)
            except Exception as e:
                self._stats["errors"] += 1
                logger.error(f"Error in bar callback for {symbol}: {e}")
                if self.on_error:
                    self.on_error(symbol, e)

    def _handle_tick_by_tick(self, symbol: str, tbt: "TickByTickData"):
        """Handle incoming tick-by-tick data"""
        # Buffer the event
        with self._lock:
            if symbol in self._buffers:
                self._buffers[symbol].tbt_ticks.append(tbt)

        # Route to callback
        if self.on_tick_by_tick:
            try:
                self.on_tick_by_tick(symbol, tbt)
            except Exception as e:
                self._stats["errors"] += 1
                logger.error(f"Error in tick_by_tick callback for {symbol}: {e}")
                if self.on_error:
                    self.on_error(symbol, e)

    def _handle_depth(self, symbol: str, depth: "MarketDepth"):
        """Handle incoming market depth (L2) update"""
        # Store latest snapshot
        with self._lock:
            if symbol in self._buffers:
                self._buffers[symbol].depth = depth

        # Route to callback
        if self.on_depth:
            try:
                self.on_depth(symbol, depth)
            except Exception as e:
                self._stats["errors"] += 1
                logger.error(f"Error in depth callback for {symbol}: {e}")
                if self.on_error:
                    self.on_error(symbol, e)

    # =========================================================================
    # Data Access Methods
    # =========================================================================

    def get_ticks(
        self,
        symbol: str,
        count: Optional[int] = None,
        since: Optional[datetime] = None,
    ) -> List[TickData]:
        """
        Get buffered ticks for a symbol.

        Args:
            symbol: Symbol to get ticks for
            count: Maximum number of ticks (None = all)
            since: Only ticks after this time (None = all)

        Returns:
            List of TickData objects (most recent last)
        """
        with self._lock:
            if symbol not in self._buffers:
                return []

            ticks = list(self._buffers[symbol].ticks)

        if since:
            ticks = [t for t in ticks if t.timestamp >= since]

        if count:
            ticks = ticks[-count:]

        return ticks

    def get_bars(
        self,
        symbol: str,
        data_type: DataType = DataType.BAR_5SEC,
        count: Optional[int] = None,
        since: Optional[datetime] = None,
    ) -> List[Bar]:
        """
        Get buffered bars for a symbol.

        Args:
            symbol: Symbol to get bars for
            data_type: Type of bars (BAR_5SEC, BAR_1MIN, etc.)
            count: Maximum number of bars (None = all)
            since: Only bars after this time (None = all)

        Returns:
            List of Bar objects (most recent last)
        """
        with self._lock:
            if symbol not in self._buffers:
                return []

            buffer = self._buffers[symbol]

            if data_type == DataType.BAR_5SEC:
                bars = list(buffer.bars_5sec)
            elif data_type == DataType.BAR_1MIN:
                bars = list(buffer.bars_1min)
            elif data_type == DataType.BAR_5MIN:
                bars = list(buffer.bars_5min)
            elif data_type == DataType.BAR_15MIN:
                bars = list(buffer.bars_15min)
            elif data_type == DataType.BAR_1HOUR:
                bars = list(buffer.bars_1hour)
            else:
                return []

        if since:
            bars = [
                b for b in bars
                if datetime.fromisoformat(b.timestamp) >= since
            ]

        if count:
            bars = bars[-count:]

        return bars

    def get_last_tick(self, symbol: str) -> Optional[TickData]:
        """Get the most recent tick for a symbol"""
        with self._lock:
            if symbol not in self._buffers:
                return None
            ticks = self._buffers[symbol].ticks
            return ticks[-1] if ticks else None

    def get_last_bar(
        self,
        symbol: str,
        data_type: DataType = DataType.BAR_5SEC,
    ) -> Optional[Bar]:
        """Get the most recent bar for a symbol"""
        bars = self.get_bars(symbol, data_type, count=1)
        return bars[0] if bars else None

    def get_last_price(self, symbol: str) -> Optional[float]:
        """Get the most recent price for a symbol"""
        tick = self.get_last_tick(symbol)
        if tick:
            return tick.price

        bar = self.get_last_bar(symbol)
        if bar:
            return bar.close

        return None

    def get_tick_by_ticks(
        self,
        symbol: str,
        count: Optional[int] = None,
        since: Optional[datetime] = None,
    ) -> List["TickByTickData"]:
        """
        Get buffered tick-by-tick events for a symbol.

        Args:
            symbol: Symbol to get events for
            count: Maximum number of events (None = all)
            since: Only events after this time (None = all)

        Returns:
            List of TickByTickData objects (most recent last)
        """
        with self._lock:
            if symbol not in self._buffers:
                return []
            tbt_ticks = list(self._buffers[symbol].tbt_ticks)

        if since:
            tbt_ticks = [t for t in tbt_ticks if t.timestamp >= since]

        if count:
            tbt_ticks = tbt_ticks[-count:]

        return tbt_ticks

    def get_depth(self, symbol: str) -> Optional["MarketDepth"]:
        """
        Get the latest market depth snapshot for a symbol.

        Args:
            symbol: Symbol to get depth for

        Returns:
            MarketDepth snapshot or None if not available
        """
        with self._lock:
            if symbol not in self._buffers:
                return None
            return self._buffers[symbol].depth

    def clear_buffers(self, symbol: Optional[str] = None):
        """
        Clear data buffers.

        Args:
            symbol: Symbol to clear (None = clear all)
        """
        with self._lock:
            if symbol:
                if symbol in self._buffers:
                    self._buffers[symbol] = DataBuffer()
                    self._aggregators[symbol] = BarAggregator(symbol)
            else:
                for sym in self._buffers:
                    self._buffers[sym] = DataBuffer()
                    self._aggregators[sym] = BarAggregator(sym)

        logger.info(f"Cleared buffers for {symbol or 'all symbols'}")

    def reset_stats(self):
        """
        Reset statistics counters.

        Preserves started_at but resets all counters and sets last_reset.
        """
        started_at = self._stats.get("started_at")
        self._stats = {
            "ticks_received": 0,
            "bars_received": 0,
            "bars_aggregated": 0,
            "errors": 0,
            "started_at": started_at,
            "last_reset": datetime.now().isoformat(),
        }
        logger.info("Data feed stats reset")

    def get_status(self) -> Dict[str, Any]:
        """
        Get feed status.

        Returns:
            Dictionary with status information
        """
        with self._lock:
            subs_status = {
                symbol: {
                    "active": sub.active,
                    "data_types": [d.value for d in sub.data_types],
                    "buffer_sizes": {
                        "ticks": len(self._buffers[symbol].ticks)
                            if symbol in self._buffers else 0,
                        "bars_5sec": len(self._buffers[symbol].bars_5sec)
                            if symbol in self._buffers else 0,
                        "bars_1min": len(self._buffers[symbol].bars_1min)
                            if symbol in self._buffers else 0,
                    }
                }
                for symbol, sub in self._subscriptions.items()
            }

        return {
            "running": self._running,
            "subscriptions": subs_status,
            "stats": self._stats,
        }
