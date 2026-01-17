"""
data_feed.py - Algorithm-centric data streaming

Provides a data feed system that:
- Subscribes to market data for instruments
- Buffers and aggregates data for algorithms
- Routes data to registered algorithm handlers
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


@dataclass
class TickData:
    """A single tick update"""
    symbol: str
    price: float
    tick_type: str
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class InstrumentSubscription:
    """Subscription for an instrument's data"""
    symbol: str
    contract: Contract
    data_types: Set[DataType] = field(default_factory=set)
    active: bool = False
    subscribed_at: datetime = field(default_factory=datetime.now)
    # Track which algorithms/subscribers want this data
    subscribers: Set[str] = field(default_factory=set)


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

    def __post_init__(self):
        # Ensure maxlen is set
        self.ticks = deque(maxlen=self.max_ticks)
        self.bars_5sec = deque(maxlen=self.max_bars)
        self.bars_1min = deque(maxlen=self.max_bars)
        self.bars_5min = deque(maxlen=self.max_bars // 2)
        self.bars_15min = deque(maxlen=self.max_bars // 5)
        self.bars_1hour = deque(maxlen=self.max_bars // 10)


class BarAggregator:
    """
    Aggregates 5-second bars into larger timeframes.

    IB only provides 5-second real-time bars, so we aggregate
    them into 1-minute, 5-minute, etc. bars for algorithms.
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
    Algorithm-centric market data feed.

    Provides a unified interface for algorithms to receive market data.
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
                # Already streaming - just add subscriber
                logger.debug(f"Added subscriber '{subscriber}' to existing {symbol} stream")
            else:
                sub = InstrumentSubscription(
                    symbol=symbol,
                    contract=contract,
                    data_types=data_types,
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
        self._original_on_bar = self.portfolio._on_bar

        # Set our handlers
        self.portfolio._on_tick = self._handle_tick
        self.portfolio._on_bar = self._handle_bar

    def _teardown_callbacks(self):
        """Restore original portfolio callbacks"""
        if hasattr(self, '_original_on_tick'):
            self.portfolio._on_tick = self._original_on_tick
        if hasattr(self, '_original_on_bar'):
            self.portfolio._on_bar = self._original_on_bar

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
            self.portfolio.bar_stream_symbol(symbol, sub.contract)

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
