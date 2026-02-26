"""
trading_engine.py - Unified trading engine

Combines ConnectionManager, DataFeed, and PluginExecutive into a single
easy-to-use interface for continuous algorithmic trading.

Provides:
- Robust connection with auto-reconnect
- Real-time market data streaming
- Continuous plugin execution
- Order execution and management
- Health monitoring and recovery
- MessageBus integration for plugin communication
"""

import logging
import signal
import sys
from threading import Event
from typing import Optional, Callable, Dict, List, Any, Set
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

from .portfolio import Portfolio
from .connection_manager import ConnectionManager, ConnectionConfig, ConnectionState
from .data_feed import DataFeed, DataType, TickData
from .models import Bar
from .message_bus import MessageBus
from .plugin_executive import PluginExecutive, ExecutionMode, OrderExecutionMode, ExecutionResult
from plugins.base import PluginBase, TradeSignal, PluginState

logger = logging.getLogger(__name__)


class EngineState(Enum):
    """Trading engine state"""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    ERROR = "error"


@dataclass
class EngineConfig:
    """Configuration for the trading engine"""
    # Connection settings
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 1

    # Connection manager settings
    auto_reconnect: bool = True
    keepalive_enabled: bool = True
    keepalive_interval: float = 30.0

    # Data feed settings
    use_delayed_data: bool = True

    # Plugin execution settings
    order_mode: OrderExecutionMode = OrderExecutionMode.DRY_RUN
    default_execution_mode: ExecutionMode = ExecutionMode.ON_BAR
    default_bar_timeframe: DataType = DataType.BAR_1MIN

    # Rate limiting settings (IB compliance)
    order_rate_limit: float = 10.0  # Orders per second
    order_burst_size: int = 10  # Max burst size

    # Engine settings
    load_portfolio_on_start: bool = True
    fetch_prices_on_start: bool = True

    # Plugin system settings
    enable_message_bus: bool = True  # Enable MessageBus for plugin communication


class TradingEngine:
    """
    Unified trading engine for continuous algorithmic trading.

    Combines all components needed for live algorithmic trading:
    - ConnectionManager: Robust IB connection with auto-reconnect
    - DataFeed: Real-time market data streaming
    - PluginExecutive: Plugin lifecycle and execution
    - MessageBus: Pub/Sub communication between plugins

    Usage:
        config = EngineConfig(
            port=4002,  # Paper trading gateway
            order_mode=OrderExecutionMode.IMMEDIATE,
        )
        engine = TradingEngine(config)
        engine.add_plugin(plugin1)
        engine.add_plugin(plugin2)
        engine.start()

        # Or dynamically load plugins
        engine.load_plugin("/path/to/my_plugin.py")

        # Run until interrupted
        engine.run_forever()

        # Or check status periodically
        while engine.is_running:
            status = engine.get_status()
            time.sleep(60)
    """

    def __init__(self, config: Optional[EngineConfig] = None):
        """
        Initialize the trading engine.

        Args:
            config: Engine configuration (uses defaults if None)
        """
        self.config = config or EngineConfig()
        self._state = EngineState.STOPPED
        self._shutdown_event = Event()

        # Create components
        self._portfolio = Portfolio(
            host=self.config.host,
            port=self.config.port,
            client_id=self.config.client_id,
        )

        self._connection_config = ConnectionConfig(
            auto_reconnect=self.config.auto_reconnect,
            keepalive_enabled=self.config.keepalive_enabled,
            keepalive_interval=self.config.keepalive_interval,
        )
        self._connection_manager = ConnectionManager(
            self._portfolio,
            self._connection_config,
        )

        self._data_feed = DataFeed(
            self._portfolio,
            use_delayed_data=self.config.use_delayed_data,
        )

        # Wire DataFeed callbacks BEFORE creating PluginExecutive.
        # StreamManager (created inside PluginExecutive.__init__) captures
        # data_feed.on_tick/on_bar at init time.  If we set them afterward
        # (as _setup_callbacks() used to do), StreamManager is bypassed and
        # plugin request_stream() callbacks never fire.
        self._data_feed.on_tick = self._on_tick
        self._data_feed.on_bar = self._on_bar
        self._data_feed.on_error = self._on_data_error

        # Create MessageBus for plugin communication
        self._message_bus = MessageBus() if self.config.enable_message_bus else None

        # Create plugin executive
        self._plugin_executive = PluginExecutive(
            self._portfolio,
            self._data_feed,
            message_bus=self._message_bus,
            order_mode=self.config.order_mode,
            order_rate_limit=self.config.order_rate_limit,
            order_burst_size=self.config.order_burst_size,
        )

        # Pending plugins (to be registered after start)
        self._pending_plugins: List[tuple] = []

        # Instrument subscriptions
        self._subscribed_symbols: Set[str] = set()

        # Callbacks
        self.on_started: Optional[Callable[[], None]] = None
        self.on_stopped: Optional[Callable[[], None]] = None
        self.on_error: Optional[Callable[[Exception], None]] = None
        self.on_signal: Optional[Callable[[str, TradeSignal], None]] = None
        self.on_execution: Optional[Callable[[ExecutionResult], None]] = None
        self.on_tick: Optional[Callable[[str, TickData], None]] = None
        self.on_bar: Optional[Callable[[str, Bar, DataType], None]] = None
        self.on_plugin_state_change: Optional[Callable[[str, PluginState], None]] = None

        # Set up internal callbacks
        self._setup_callbacks()

    @property
    def state(self) -> EngineState:
        """Get current engine state"""
        return self._state

    @property
    def is_running(self) -> bool:
        """Check if engine is running"""
        return self._state == EngineState.RUNNING

    @property
    def is_connected(self) -> bool:
        """Check if connected to IB"""
        return self._connection_manager.is_connected

    @property
    def portfolio(self) -> Portfolio:
        """Get the portfolio instance"""
        return self._portfolio

    @property
    def data_feed(self) -> DataFeed:
        """Get the data feed instance"""
        return self._data_feed

    @property
    def plugin_executive(self) -> Optional[PluginExecutive]:
        """Get the plugin executive instance"""
        return self._plugin_executive

    @property
    def message_bus(self) -> Optional[MessageBus]:
        """Get the MessageBus instance"""
        return self._message_bus

    def _setup_callbacks(self):
        """Set up internal callbacks between components"""
        # Connection manager callbacks
        self._connection_manager.on_connected = self._on_connected
        self._connection_manager.on_disconnected = self._on_disconnected
        self._connection_manager.on_reconnecting = self._on_reconnecting

        # Plugin executive callbacks
        if self._plugin_executive:
            self._plugin_executive.on_signal = self._on_signal
            self._plugin_executive.on_execution = self._on_execution
            self._plugin_executive.on_error = self._on_runner_error
            self._plugin_executive.on_plugin_state_change = self._on_plugin_state_change

    def _on_connected(self):
        """Handle connection established"""
        logger.info("Trading engine: Connected to IB")

        # Load portfolio if configured
        if self.config.load_portfolio_on_start:
            try:
                self._portfolio.load(
                    fetch_prices=self.config.fetch_prices_on_start,
                    fetch_account=True,
                )
            except Exception as e:
                logger.error(f"Failed to load portfolio: {e}")

        # Start data feed if not already running
        if not self._data_feed.is_running:
            self._data_feed.start()

        # Start plugin executive if not already running
        if self._plugin_executive and not self._plugin_executive.is_running:
            self._plugin_executive.start()

        # Resubscribe to instruments
        self._resubscribe_instruments()

    def _on_disconnected(self):
        """Handle connection lost"""
        logger.warning("Trading engine: Disconnected from IB")

    def _on_reconnecting(self, attempt: int):
        """Handle reconnection attempt"""
        logger.info(f"Trading engine: Reconnection attempt {attempt}")

    def _on_tick(self, symbol: str, tick: TickData):
        """Handle tick data"""
        if self.on_tick:
            try:
                self.on_tick(symbol, tick)
            except Exception as e:
                logger.error(f"Error in on_tick callback: {e}")

    def _on_bar(self, symbol: str, bar: Bar, data_type: DataType):
        """Handle bar data"""
        if self.on_bar:
            try:
                self.on_bar(symbol, bar, data_type)
            except Exception as e:
                logger.error(f"Error in on_bar callback: {e}")

    def _on_data_error(self, symbol: str, error: Exception):
        """Handle data feed error"""
        logger.error(f"Data feed error for {symbol}: {error}")
        if self.on_error:
            try:
                self.on_error(error)
            except:
                pass

    def _on_signal(self, algorithm_name: str, signal: TradeSignal):
        """Handle algorithm signal"""
        if self.on_signal:
            try:
                self.on_signal(algorithm_name, signal)
            except Exception as e:
                logger.error(f"Error in on_signal callback: {e}")

    def _on_execution(self, result: ExecutionResult):
        """Handle order execution"""
        if self.on_execution:
            try:
                self.on_execution(result)
            except Exception as e:
                logger.error(f"Error in on_execution callback: {e}")

    def _on_runner_error(self, algorithm_name: str, error: Exception):
        """Handle runner error"""
        logger.error(f"Algorithm/Plugin '{algorithm_name}' error: {error}")
        if self.on_error:
            try:
                self.on_error(error)
            except:
                pass

    def _on_plugin_state_change(self, plugin_name: str, new_state: PluginState):
        """Handle plugin state change"""
        logger.info(f"Plugin '{plugin_name}' state changed to {new_state.value}")
        if self.on_plugin_state_change:
            try:
                self.on_plugin_state_change(plugin_name, new_state)
            except Exception as e:
                logger.error(f"Error in on_plugin_state_change callback: {e}")

    def add_plugin(
        self,
        plugin: PluginBase,
        execution_mode: Optional[ExecutionMode] = None,
        bar_timeframe: Optional[DataType] = None,
        enabled: bool = True,
        auto_subscribe: bool = True,
        auto_start: bool = True,
    ) -> bool:
        """
        Add a plugin to the engine.

        Args:
            plugin: Plugin instance
            execution_mode: When to trigger (uses config default if None)
            bar_timeframe: Bar timeframe for ON_BAR mode (uses config default if None)
            enabled: Whether plugin is enabled
            auto_subscribe: Automatically subscribe to plugin's instruments
            auto_start: Automatically start the plugin after registration

        Returns:
            True if added successfully
        """
        if not self._plugin_executive:
            logger.error("Cannot add plugin: engine not configured for plugin executive")
            return False

        if execution_mode is None:
            execution_mode = self.config.default_execution_mode
        if bar_timeframe is None:
            bar_timeframe = self.config.default_bar_timeframe

        # Load if not loaded
        if not plugin.is_loaded:
            if not plugin.load():
                logger.error(f"Failed to load plugin '{plugin.name}'")
                return False

        # Set up MessageBus
        if self._message_bus:
            plugin.set_message_bus(self._message_bus)

        # If engine is running, register immediately
        if self.is_running:
            success = self._plugin_executive.register_plugin(
                plugin,
                execution_mode=execution_mode,
                bar_timeframe=bar_timeframe,
                enabled=enabled,
            )
            if success:
                if auto_subscribe:
                    self._subscribe_plugin_instruments(plugin)
                if auto_start:
                    self._plugin_executive.start_plugin(plugin.name)
            return success
        else:
            # Queue for registration after start
            self._pending_plugins.append((
                plugin, execution_mode, bar_timeframe, enabled, auto_subscribe, auto_start
            ))
            logger.info(f"Queued plugin '{plugin.name}' for registration")
            return True

    def load_plugin(
        self,
        file_path: str,
        execution_mode: Optional[ExecutionMode] = None,
        bar_timeframe: Optional[DataType] = None,
        enabled: bool = True,
        auto_subscribe: bool = True,
        auto_start: bool = True,
    ) -> Optional[str]:
        """
        Load and add a plugin from a file.

        Args:
            file_path: Path to the Python file containing the plugin
            execution_mode: When to trigger
            bar_timeframe: Bar timeframe for ON_BAR mode
            enabled: Whether plugin is enabled
            auto_subscribe: Automatically subscribe to plugin's instruments
            auto_start: Automatically start the plugin

        Returns:
            Plugin name if loaded successfully, None otherwise
        """
        if not self._plugin_executive:
            logger.error("Cannot load plugin: engine not configured for plugin executive")
            return None

        if execution_mode is None:
            execution_mode = self.config.default_execution_mode
        if bar_timeframe is None:
            bar_timeframe = self.config.default_bar_timeframe

        name = self._plugin_executive.load_plugin_from_file(
            file_path,
            execution_mode=execution_mode,
            bar_timeframe=bar_timeframe,
            enabled=enabled,
        )

        if name:
            if auto_subscribe:
                # Get plugin and subscribe to its instruments
                status = self._plugin_executive.get_plugin_status(name)
                if status:
                    # Subscribe using plugin instruments would require access to the plugin
                    pass
            if auto_start:
                self._plugin_executive.start_plugin(name)

        return name

    def remove_plugin(self, name: str) -> bool:
        """
        Remove a plugin from the engine.

        Args:
            name: Plugin name

        Returns:
            True if removed successfully
        """
        if not self._plugin_executive:
            logger.warning("Cannot remove plugin: plugin executive not available")
            return False
        return self._plugin_executive.unload_plugin(name)

    def start_plugin(self, name: str) -> bool:
        """Start a plugin"""
        if self._plugin_executive:
            return self._plugin_executive.start_plugin(name)
        return False

    def stop_plugin(self, name: str) -> bool:
        """Stop a plugin"""
        if self._plugin_executive:
            return self._plugin_executive.stop_plugin(name)
        return False

    def freeze_plugin(self, name: str) -> bool:
        """Freeze a plugin"""
        if self._plugin_executive:
            return self._plugin_executive.freeze_plugin(name)
        return False

    def resume_plugin(self, name: str) -> bool:
        """Resume a frozen plugin"""
        if self._plugin_executive:
            return self._plugin_executive.resume_plugin(name)
        return False

    def _subscribe_plugin_instruments(self, plugin: PluginBase):
        """Subscribe to data for a plugin's instruments"""
        for instrument in plugin.enabled_instruments:
            symbol = instrument.symbol
            contract = instrument.to_contract()
            data_types = {DataType.TICK, DataType.BAR_5SEC, DataType.BAR_1MIN}
            self._data_feed.subscribe(symbol, contract, data_types, subscriber=plugin.name)
            self._subscribed_symbols.add(symbol)
            logger.debug(f"Subscribed plugin '{plugin.name}' to {symbol}")

    def _resubscribe_instruments(self):
        """Resubscribe to all instruments after reconnect"""
        for symbol in self._subscribed_symbols:
            # Try to get contract from portfolio
            pos = self._portfolio.get_position(symbol)
            if pos and pos.contract:
                data_types = {DataType.TICK, DataType.BAR_5SEC, DataType.BAR_1MIN}
                self._data_feed.subscribe(symbol, pos.contract, data_types)

    def subscribe(
        self,
        symbol: str,
        contract,
        data_types: Optional[Set[DataType]] = None,
    ):
        """
        Subscribe to market data for a symbol.

        Args:
            symbol: Symbol identifier
            contract: IB Contract
            data_types: Data types to subscribe to
        """
        if data_types is None:
            data_types = {DataType.TICK, DataType.BAR_5SEC, DataType.BAR_1MIN}

        self._data_feed.subscribe(symbol, contract, data_types)
        self._subscribed_symbols.add(symbol)

    def unsubscribe(self, symbol: str):
        """
        Unsubscribe from market data.

        Args:
            symbol: Symbol to unsubscribe
        """
        self._data_feed.unsubscribe(symbol)
        self._subscribed_symbols.discard(symbol)

    def start(self) -> bool:
        """
        Start the trading engine.

        Connects to IB, loads portfolio, starts data feed and plugin executive.

        Returns:
            True if started successfully
        """
        if self._state != EngineState.STOPPED:
            logger.warning(f"Cannot start: engine in state {self._state.value}")
            return False

        self._state = EngineState.STARTING
        self._shutdown_event.clear()

        logger.info("Starting trading engine...")

        try:
            # Start connection manager
            if not self._connection_manager.start():
                # Will retry in background if auto_reconnect enabled
                if not self.config.auto_reconnect:
                    self._state = EngineState.ERROR
                    return False

            # Register pending plugins
            if self._plugin_executive:
                for (plugin, mode, timeframe, enabled, auto_sub, auto_start) in self._pending_plugins:
                    self._plugin_executive.register_plugin(
                        plugin,
                        execution_mode=mode,
                        bar_timeframe=timeframe,
                        enabled=enabled,
                    )
                    if auto_sub:
                        self._subscribe_plugin_instruments(plugin)
                    if auto_start:
                        self._plugin_executive.start_plugin(plugin.name)
                self._pending_plugins.clear()

            self._state = EngineState.RUNNING
            logger.info("Trading engine started")

            if self.on_started:
                try:
                    self.on_started()
                except Exception as e:
                    logger.error(f"Error in on_started callback: {e}")

            return True

        except Exception as e:
            logger.error(f"Failed to start trading engine: {e}")
            self._state = EngineState.ERROR
            if self.on_error:
                self.on_error(e)
            return False

    def stop(self):
        """Stop the trading engine gracefully"""
        if self._state in (EngineState.STOPPED, EngineState.STOPPING):
            return

        self._state = EngineState.STOPPING
        self._shutdown_event.set()

        logger.info("Stopping trading engine...")

        try:
            # Stop in reverse order
            if self._plugin_executive:
                self._plugin_executive.stop()
            self._data_feed.stop()
            self._connection_manager.stop()

            self._state = EngineState.STOPPED
            logger.info("Trading engine stopped")

            if self.on_stopped:
                try:
                    self.on_stopped()
                except Exception as e:
                    logger.error(f"Error in on_stopped callback: {e}")

        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            self._state = EngineState.ERROR

    def pause(self):
        """Pause plugin execution (data still flows)"""
        if self._state == EngineState.RUNNING:
            if self._plugin_executive:
                self._plugin_executive.pause()
            self._state = EngineState.PAUSED
            logger.info("Trading engine paused")

    def resume(self):
        """Resume plugin execution"""
        if self._state == EngineState.PAUSED:
            if self._plugin_executive:
                self._plugin_executive.resume()
            self._state = EngineState.RUNNING
            logger.info("Trading engine resumed")

    def run_forever(self, handle_signals: bool = True, required_signals: int = 3):
        """
        Run the engine until interrupted.

        Args:
            handle_signals: Install signal handlers for graceful shutdown
            required_signals: Number of Ctrl-C presses required to stop (default 3)
        """
        import time as _time

        sigint_count = 0
        first_sigint_time = None
        reset_timeout = 10.0  # Reset counter after 10 seconds

        if handle_signals:
            def signal_handler(signum, frame):
                nonlocal sigint_count, first_sigint_time

                now = _time.time()

                # Reset counter if too much time has passed
                if first_sigint_time and (now - first_sigint_time) > reset_timeout:
                    sigint_count = 0
                    first_sigint_time = None

                sigint_count += 1
                if first_sigint_time is None:
                    first_sigint_time = now

                remaining = required_signals - sigint_count

                if remaining > 0:
                    logger.warning(
                        f"Ctrl-C received ({sigint_count}/{required_signals}). "
                        f"Press {remaining} more time(s) within {reset_timeout:.0f}s to stop."
                    )
                else:
                    logger.info("Shutdown confirmed, stopping engine...")
                    self.stop()

            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)

        try:
            # Block until shutdown
            while not self._shutdown_event.is_set():
                self._shutdown_event.wait(timeout=1.0)

        except KeyboardInterrupt:
            pass  # Handled by signal handler

    def get_status(self) -> Dict[str, Any]:
        """
        Get comprehensive engine status.

        Returns:
            Status dictionary
        """
        status = {
            "state": self._state.value,
            "connected": self.is_connected,
            "connection": self._connection_manager.get_status(),
            "data_feed": self._data_feed.get_status(),
            "subscribed_symbols": list(self._subscribed_symbols),
            "portfolio": {
                "positions": len(self._portfolio.positions),
                "total_value": self._portfolio.total_value,
            } if self.is_connected else None,
        }

        # Add plugin executive status
        if self._plugin_executive:
            status["plugin_executive"] = self._plugin_executive.get_status()
        if self._message_bus:
            status["message_bus"] = self._message_bus.get_stats()

        return status

    def get_positions(self) -> List[Dict]:
        """Get current portfolio positions"""
        return [p.to_dict() for p in self._portfolio.positions]

    def get_bars(
        self,
        symbol: str,
        data_type: DataType = DataType.BAR_1MIN,
        count: int = 100,
    ) -> List[Bar]:
        """Get buffered bars for a symbol"""
        return self._data_feed.get_bars(symbol, data_type, count=count)

    def get_last_price(self, symbol: str) -> Optional[float]:
        """Get the last price for a symbol"""
        return self._data_feed.get_last_price(symbol)


def create_engine(
    port: int = 7497,
    order_mode: str = "dry_run",
    **kwargs,
) -> TradingEngine:
    """
    Create a trading engine with common configurations.

    Args:
        port: IB Gateway/TWS port (7497=paper TWS, 4002=paper gateway)
        order_mode: "dry_run", "immediate", or "queued"
        **kwargs: Additional EngineConfig parameters

    Returns:
        Configured TradingEngine instance
    """
    mode_map = {
        "dry_run": OrderExecutionMode.DRY_RUN,
        "immediate": OrderExecutionMode.IMMEDIATE,
        "queued": OrderExecutionMode.QUEUED,
    }

    config = EngineConfig(
        port=port,
        order_mode=mode_map.get(order_mode, OrderExecutionMode.DRY_RUN),
        **kwargs,
    )

    return TradingEngine(config)
