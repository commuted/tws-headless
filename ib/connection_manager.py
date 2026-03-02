"""
connection_manager.py - Robust connection management with auto-reconnect

Provides persistent, fault-tolerant connection to IB TWS/Gateway with:
- Automatic reconnection with exponential backoff
- Keepalive heartbeat to prevent idle timeouts
- Stream state preservation and recovery
- Connection health monitoring
"""

import asyncio
import logging
import time
from typing import Optional, Callable, Dict, List, Set, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum

from ibapi.contract import Contract

logger = logging.getLogger(__name__)


class ConnectionState(Enum):
    """Connection state machine states"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    SHUTTING_DOWN = "shutting_down"


@dataclass
class StreamSubscription:
    """Represents a market data stream subscription"""
    symbol: str
    contract: Contract
    req_id: int = 0
    stream_type: str = "tick"  # "tick" or "bar"
    what_to_show: str = "TRADES"
    use_rth: bool = True
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class ConnectionConfig:
    """Configuration for connection management"""
    # Reconnection settings
    auto_reconnect: bool = True
    reconnect_delay_initial: float = 1.0  # Initial delay in seconds
    reconnect_delay_max: float = 60.0  # Maximum delay in seconds
    reconnect_delay_multiplier: float = 2.0  # Exponential backoff multiplier
    max_reconnect_attempts: int = 0  # 0 = unlimited

    # Keepalive settings
    keepalive_enabled: bool = True
    keepalive_interval: float = 30.0  # Seconds between keepalive requests
    keepalive_timeout: float = 10.0  # Timeout for keepalive response

    # Health monitoring
    health_check_interval: float = 5.0  # Seconds between health checks
    connection_timeout: float = 10.0  # Timeout for initial connection


class ConnectionManager:
    """
    Robust connection manager for IB API with auto-reconnect and keepalive.

    Wraps a Portfolio instance and manages its connection lifecycle,
    automatically reconnecting on failures and recovering streams.

    Usage:
        from portfolio import Portfolio

        portfolio = Portfolio()
        manager = ConnectionManager(portfolio)

        # Define what to do on connection events
        manager.on_connected = lambda: print("Connected!")
        manager.on_disconnected = lambda: print("Disconnected!")

        # Start managed connection
        await manager.start()

        # ... use portfolio normally ...

        # When done
        await manager.stop()
    """

    def __init__(
        self,
        portfolio,  # Portfolio instance
        config: Optional[ConnectionConfig] = None,
    ):
        """
        Initialize connection manager.

        Args:
            portfolio: Portfolio instance to manage
            config: Connection configuration (uses defaults if None)
        """
        self.portfolio = portfolio
        self.config = config or ConnectionConfig()

        # State
        self._state = ConnectionState.DISCONNECTED
        self._shutdown_event = asyncio.Event()

        # Tasks (replaces Threads)
        self._keepalive_task: Optional[asyncio.Task] = None
        self._health_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None

        # Reconnection tracking
        self._reconnect_attempts = 0
        self._last_connect_time: Optional[datetime] = None
        self._last_keepalive_time: Optional[datetime] = None
        self._keepalive_response_received = asyncio.Event()

        # Stream preservation
        self._saved_tick_streams: Dict[str, StreamSubscription] = {}
        self._saved_bar_streams: Dict[str, StreamSubscription] = {}

        # Callbacks
        self.on_connected: Optional[Callable[[], None]] = None
        self.on_disconnected: Optional[Callable[[], None]] = None
        self.on_reconnecting: Optional[Callable[[int], None]] = None  # arg: attempt number
        self.on_error: Optional[Callable[[Exception], None]] = None

        # Register for portfolio connection events
        self._setup_portfolio_callbacks()

    @property
    def state(self) -> ConnectionState:
        """Get current connection state"""
        return self._state

    @property
    def is_connected(self) -> bool:
        """Check if currently connected"""
        return self.state == ConnectionState.CONNECTED

    @property
    def reconnect_attempts(self) -> int:
        """Get number of reconnection attempts since last successful connect"""
        return self._reconnect_attempts

    def _set_state(self, new_state: ConnectionState):
        """Set connection state"""
        old_state = self._state
        self._state = new_state
        logger.info(f"Connection state: {old_state.value} -> {new_state.value}")

    def _setup_portfolio_callbacks(self):
        """Set up callbacks on the portfolio for connection events"""
        original_connection_closed = self.portfolio._callbacks.get("connectionClosed")

        def on_connection_closed():
            if original_connection_closed:
                original_connection_closed()
            self._handle_disconnection()

        self.portfolio.register_callback("connectionClosed", on_connection_closed)

        # Handle currentTime for keepalive
        def on_current_time(server_time: int):
            self._keepalive_response_received.set()
            self._last_keepalive_time = datetime.now()
            logger.debug(f"Keepalive response: server time {server_time}")

        # Override currentTime handler
        original_current_time = getattr(self.portfolio, 'currentTime', None)
        def current_time_wrapper(time: int):
            if original_current_time:
                original_current_time(time)
            on_current_time(time)

        self.portfolio.currentTime = current_time_wrapper

    async def start(self) -> bool:
        """
        Start managed connection.

        Connects to IB and starts keepalive/health monitoring tasks.

        Returns:
            True if initial connection successful
        """
        if self.state != ConnectionState.DISCONNECTED:
            logger.warning(f"Cannot start: already in state {self.state.value}")
            return False

        self._shutdown_event.clear()
        self._set_state(ConnectionState.CONNECTING)

        # Initial connection
        if not await self._connect():
            if self.config.auto_reconnect:
                # Start reconnection in background
                self._start_reconnect_task()
                return True  # Return True since we're handling it
            else:
                self._set_state(ConnectionState.DISCONNECTED)
                return False

        return True

    async def stop(self):
        """
        Stop managed connection and all tasks.

        Gracefully shuts down connection, stops all streams,
        and terminates management tasks.
        """
        logger.info("Connection manager stopping...")
        self._set_state(ConnectionState.SHUTTING_DOWN)
        self._shutdown_event.set()

        # Cancel all tasks
        tasks = [
            t for t in [self._keepalive_task, self._health_task, self._reconnect_task]
            if t and not t.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Disconnect portfolio
        if self.portfolio.connected:
            self.portfolio.shutdown()
            await self.portfolio.disconnect()

        self._set_state(ConnectionState.DISCONNECTED)
        logger.info("Connection manager stopped")

    async def _connect(self) -> bool:
        """Attempt to connect to IB"""
        try:
            logger.info(f"Connecting to IB at {self.portfolio._host}:{self.portfolio._port}...")

            if await self.portfolio.connect():
                self._on_connected()
                return True
            else:
                logger.error("Connection failed")
                return False

        except Exception as e:
            logger.error(f"Connection error: {e}")
            if self.on_error:
                self.on_error(e)
            return False

    def _on_connected(self):
        """Handle successful connection"""
        self._set_state(ConnectionState.CONNECTED)
        self._reconnect_attempts = 0
        self._last_connect_time = datetime.now()

        # Start management tasks
        self._start_keepalive_task()
        self._start_health_task()

        # Recover streams if we have saved subscriptions
        self._recover_streams()

        # Invoke callback
        if self.on_connected:
            try:
                self.on_connected()
            except Exception as e:
                logger.error(f"Error in on_connected callback: {e}")

    def _handle_disconnection(self):
        """Handle unexpected disconnection"""
        if self.state == ConnectionState.SHUTTING_DOWN:
            return  # Expected disconnection

        logger.warning("Unexpected disconnection detected")

        # Save current stream state before reconnecting
        self._save_stream_state()

        self._set_state(ConnectionState.DISCONNECTED)

        # Invoke callback
        if self.on_disconnected:
            try:
                self.on_disconnected()
            except Exception as e:
                logger.error(f"Error in on_disconnected callback: {e}")

        # Start reconnection if enabled
        if self.config.auto_reconnect:
            self._start_reconnect_task()

    def _save_stream_state(self):
        """Save current stream subscriptions for recovery"""
        # Save tick streams
        self._saved_tick_streams.clear()
        for req_id, symbol in self.portfolio._stream_subscriptions.items():
            pos = self.portfolio.get_position(symbol)
            if pos and pos.contract:
                self._saved_tick_streams[symbol] = StreamSubscription(
                    symbol=symbol,
                    contract=pos.contract,
                    req_id=req_id,
                    stream_type="tick",
                )

        # Save bar streams
        self._saved_bar_streams.clear()
        for req_id, symbol in self.portfolio._bar_subscriptions.items():
            pos = self.portfolio.get_position(symbol)
            if pos and pos.contract:
                self._saved_bar_streams[symbol] = StreamSubscription(
                    symbol=symbol,
                    contract=pos.contract,
                    req_id=req_id,
                    stream_type="bar",
                )

        if self._saved_tick_streams or self._saved_bar_streams:
            logger.info(
                f"Saved stream state: {len(self._saved_tick_streams)} tick streams, "
                f"{len(self._saved_bar_streams)} bar streams"
            )

    def _recover_streams(self):
        """Recover saved stream subscriptions after reconnect"""
        if not self._saved_tick_streams and not self._saved_bar_streams:
            return

        logger.info("Recovering stream subscriptions...")

        # Clear portfolio's internal stream state
        self.portfolio._stream_subscriptions.clear()
        self.portfolio._stream_req_ids.clear()
        self.portfolio._bar_subscriptions.clear()
        self.portfolio._bar_req_ids.clear()

        # Recover tick streams
        for symbol, sub in self._saved_tick_streams.items():
            try:
                if self.portfolio.stream_symbol(symbol, sub.contract):
                    logger.debug(f"Recovered tick stream for {symbol}")
                else:
                    logger.warning(f"Failed to recover tick stream for {symbol}")
            except Exception as e:
                logger.error(f"Error recovering tick stream for {symbol}: {e}")

        # Recover bar streams
        for symbol, sub in self._saved_bar_streams.items():
            try:
                if self.portfolio.bar_stream_symbol(
                    symbol, sub.contract, sub.what_to_show, sub.use_rth
                ):
                    logger.debug(f"Recovered bar stream for {symbol}")
                else:
                    logger.warning(f"Failed to recover bar stream for {symbol}")
            except Exception as e:
                logger.error(f"Error recovering bar stream for {symbol}: {e}")

        # Clear saved state
        self._saved_tick_streams.clear()
        self._saved_bar_streams.clear()

        logger.info("Stream recovery complete")

    def _start_reconnect_task(self):
        """Start reconnection task"""
        if self._reconnect_task and not self._reconnect_task.done():
            return  # Already reconnecting

        self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self):
        """Background reconnection coroutine with exponential backoff"""
        self._set_state(ConnectionState.RECONNECTING)
        delay = self.config.reconnect_delay_initial

        while not self._shutdown_event.is_set():
            self._reconnect_attempts += 1

            # Check max attempts
            if (self.config.max_reconnect_attempts > 0 and
                self._reconnect_attempts > self.config.max_reconnect_attempts):
                logger.error(
                    f"Max reconnection attempts ({self.config.max_reconnect_attempts}) exceeded"
                )
                self._set_state(ConnectionState.DISCONNECTED)
                return

            logger.info(
                f"Reconnection attempt {self._reconnect_attempts} "
                f"(delay: {delay:.1f}s)"
            )

            # Invoke callback
            if self.on_reconnecting:
                try:
                    self.on_reconnecting(self._reconnect_attempts)
                except Exception as e:
                    logger.error(f"Error in on_reconnecting callback: {e}")

            # Wait before attempting (interruptible by shutdown)
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=delay)
                return  # Shutdown requested
            except asyncio.TimeoutError:
                pass

            # Attempt connection
            try:
                # Ensure clean state
                if self.portfolio.connected:
                    await self.portfolio.disconnect()

                if await self._connect():
                    logger.info("Reconnection successful")
                    return  # Success!

            except Exception as e:
                logger.error(f"Reconnection attempt failed: {e}")

            # Increase delay (exponential backoff)
            delay = min(delay * self.config.reconnect_delay_multiplier,
                       self.config.reconnect_delay_max)

    def _start_keepalive_task(self):
        """Start keepalive task"""
        if not self.config.keepalive_enabled:
            return

        if self._keepalive_task and not self._keepalive_task.done():
            return

        self._keepalive_task = asyncio.create_task(self._keepalive_loop())

    async def _keepalive_loop(self):
        """Background keepalive coroutine"""
        logger.debug("Keepalive task started")

        while not self._shutdown_event.is_set():
            # Wait for interval (interruptible by shutdown)
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.config.keepalive_interval,
                )
                break  # Shutdown requested
            except asyncio.TimeoutError:
                pass

            if self.state != ConnectionState.CONNECTED:
                continue

            # Send keepalive request
            try:
                self._keepalive_response_received.clear()
                self.portfolio.reqCurrentTime()

                # Wait for response
                try:
                    await asyncio.wait_for(
                        self._keepalive_response_received.wait(),
                        timeout=self.config.keepalive_timeout,
                    )
                except asyncio.TimeoutError:
                    logger.warning("Keepalive timeout - connection may be stale")
                    # Don't immediately trigger reconnect; let health check handle it

            except Exception as e:
                logger.error(f"Error sending keepalive: {e}")

        logger.debug("Keepalive task stopped")

    def _start_health_task(self):
        """Start health monitoring task"""
        if self._health_task and not self._health_task.done():
            return

        self._health_task = asyncio.create_task(self._health_loop())

    async def _health_loop(self):
        """Background health monitoring coroutine"""
        logger.debug("Health monitor task started")

        while not self._shutdown_event.is_set():
            # Wait for interval (interruptible by shutdown)
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.config.health_check_interval,
                )
                break  # Shutdown requested
            except asyncio.TimeoutError:
                pass

            if self.state != ConnectionState.CONNECTED:
                continue

            # Check connection health
            if not self.portfolio.connected:
                logger.warning("Health check: portfolio reports disconnected")
                self._handle_disconnection()

        logger.debug("Health monitor task stopped")

    def get_status(self) -> Dict[str, Any]:
        """
        Get connection manager status.

        Returns:
            Dictionary with status information
        """
        return {
            "state": self.state.value,
            "connected": self.is_connected,
            "reconnect_attempts": self._reconnect_attempts,
            "last_connect_time": (
                self._last_connect_time.isoformat()
                if self._last_connect_time else None
            ),
            "last_keepalive_time": (
                self._last_keepalive_time.isoformat()
                if self._last_keepalive_time else None
            ),
            "saved_tick_streams": len(self._saved_tick_streams),
            "saved_bar_streams": len(self._saved_bar_streams),
            "config": {
                "auto_reconnect": self.config.auto_reconnect,
                "keepalive_enabled": self.config.keepalive_enabled,
                "keepalive_interval": self.config.keepalive_interval,
            }
        }
