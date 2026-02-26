"""
connection_manager.py - Robust connection management with auto-reconnect

Provides persistent, fault-tolerant connection to IB TWS/Gateway with:
- Automatic reconnection with exponential backoff
- Keepalive heartbeat to prevent idle timeouts
- Stream state preservation and recovery
- Connection health monitoring
"""

import logging
import time
from threading import Thread, Event, Lock
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
        manager.start()

        # ... use portfolio normally ...

        # When done
        manager.stop()
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
        self._state_lock = Lock()
        self._shutdown_event = Event()

        # Threads
        self._keepalive_thread: Optional[Thread] = None
        self._health_thread: Optional[Thread] = None
        self._reconnect_thread: Optional[Thread] = None

        # Reconnection tracking
        self._reconnect_attempts = 0
        self._last_connect_time: Optional[datetime] = None
        self._last_keepalive_time: Optional[datetime] = None
        self._keepalive_response_received = Event()

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
        with self._state_lock:
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
        """Set connection state (thread-safe)"""
        with self._state_lock:
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

    def start(self) -> bool:
        """
        Start managed connection.

        Connects to IB and starts keepalive/health monitoring threads.

        Returns:
            True if initial connection successful
        """
        if self.state != ConnectionState.DISCONNECTED:
            logger.warning(f"Cannot start: already in state {self.state.value}")
            return False

        self._shutdown_event.clear()
        self._set_state(ConnectionState.CONNECTING)

        # Initial connection
        if not self._connect():
            if self.config.auto_reconnect:
                # Start reconnection in background
                self._start_reconnect_thread()
                return True  # Return True since we're handling it
            else:
                self._set_state(ConnectionState.DISCONNECTED)
                return False

        return True

    def stop(self):
        """
        Stop managed connection and all threads.

        Gracefully shuts down connection, stops all streams,
        and terminates management threads.
        """
        logger.info("Connection manager stopping...")
        self._set_state(ConnectionState.SHUTTING_DOWN)
        self._shutdown_event.set()

        # Wait for threads to finish
        threads = [
            self._keepalive_thread,
            self._health_thread,
            self._reconnect_thread,
        ]

        for thread in threads:
            if thread and thread.is_alive():
                thread.join(timeout=5.0)

        # Disconnect portfolio
        if self.portfolio.connected:
            self.portfolio.shutdown()
            self.portfolio.disconnect()

        self._set_state(ConnectionState.DISCONNECTED)
        logger.info("Connection manager stopped")

    def _connect(self) -> bool:
        """Attempt to connect to IB"""
        try:
            logger.info(f"Connecting to IB at {self.portfolio._host}:{self.portfolio._port}...")

            if self.portfolio.connect():
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

        # Start management threads
        self._start_keepalive_thread()
        self._start_health_thread()

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
            self._start_reconnect_thread()

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

    def _start_reconnect_thread(self):
        """Start reconnection thread"""
        if self._reconnect_thread and self._reconnect_thread.is_alive():
            return  # Already reconnecting

        self._reconnect_thread = Thread(
            target=self._reconnect_loop,
            daemon=True,
            name="ConnectionManager-Reconnect"
        )
        self._reconnect_thread.start()

    def _reconnect_loop(self):
        """Background reconnection loop with exponential backoff"""
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

            # Wait before attempting
            if self._shutdown_event.wait(delay):
                return  # Shutdown requested

            # Attempt connection
            try:
                # Ensure clean state
                if self.portfolio.connected:
                    self.portfolio.disconnect()

                if self._connect():
                    logger.info("Reconnection successful")
                    return  # Success!

            except Exception as e:
                logger.error(f"Reconnection attempt failed: {e}")

            # Increase delay (exponential backoff)
            delay = min(delay * self.config.reconnect_delay_multiplier,
                       self.config.reconnect_delay_max)

    def _start_keepalive_thread(self):
        """Start keepalive thread"""
        if not self.config.keepalive_enabled:
            return

        if self._keepalive_thread and self._keepalive_thread.is_alive():
            return

        self._keepalive_thread = Thread(
            target=self._keepalive_loop,
            daemon=True,
            name="ConnectionManager-Keepalive"
        )
        self._keepalive_thread.start()

    def _keepalive_loop(self):
        """Background keepalive loop"""
        logger.debug("Keepalive thread started")

        while not self._shutdown_event.is_set():
            # Wait for interval
            if self._shutdown_event.wait(self.config.keepalive_interval):
                break  # Shutdown requested

            if self.state != ConnectionState.CONNECTED:
                continue

            # Send keepalive request
            try:
                self._keepalive_response_received.clear()
                self.portfolio.reqCurrentTime()

                # Wait for response
                if not self._keepalive_response_received.wait(
                    self.config.keepalive_timeout
                ):
                    logger.warning("Keepalive timeout - connection may be stale")
                    # Don't immediately trigger reconnect; let health check handle it

            except Exception as e:
                logger.error(f"Error sending keepalive: {e}")

        logger.debug("Keepalive thread stopped")

    def _start_health_thread(self):
        """Start health monitoring thread"""
        if self._health_thread and self._health_thread.is_alive():
            return

        self._health_thread = Thread(
            target=self._health_loop,
            daemon=True,
            name="ConnectionManager-Health"
        )
        self._health_thread.start()

    def _health_loop(self):
        """Background health monitoring loop"""
        logger.debug("Health monitor thread started")

        while not self._shutdown_event.is_set():
            # Wait for interval
            if self._shutdown_event.wait(self.config.health_check_interval):
                break  # Shutdown requested

            if self.state != ConnectionState.CONNECTED:
                continue

            # Check connection health
            if not self.portfolio.connected:
                logger.warning("Health check: portfolio reports disconnected")
                self._handle_disconnection()

        logger.debug("Health monitor thread stopped")

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
