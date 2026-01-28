"""
plugin_executive.py - Plugin lifecycle manager and execution engine

Provides:
- Dynamic plugin loading/unloading via file paths
- Plugin lifecycle management (start, stop, freeze, resume)
- Custom request handling for plugins
- Continuous execution feeding real-time data
- Trade signal execution with order reconciliation
- Circuit breaker fault tolerance
- MessageBus integration for indicator feeds
"""

import logging
from threading import Thread, Event, Lock, RLock
from typing import Optional, Callable, Dict, List, Set, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from queue import Queue, Empty
from pathlib import Path
import time
import traceback

from ibapi.contract import Contract
from ibapi.order import Order

from .plugins.base import PluginBase, PluginResult, TradeSignal, PluginState
from .plugins.unassigned import UnassignedPlugin, UNASSIGNED_PLUGIN_NAME
from .data_feed import DataFeed, DataType, TickData
from .models import Bar
from .message_bus import MessageBus
from .order_reconciler import OrderReconciler, ReconciledOrder, ReconciliationMode
from .rate_limiter import OrderRateLimiter
from .plugin_execution_log import PluginExecutionLog, ExecutionLogWriter

logger = logging.getLogger(__name__)


class ExecutionMode(Enum):
    """How plugins are triggered"""
    ON_BAR = "on_bar"  # Execute on every new bar
    ON_TICK = "on_tick"  # Execute on every tick
    SCHEDULED = "scheduled"  # Execute on a schedule
    MANUAL = "manual"  # Manual trigger only


class OrderExecutionMode(Enum):
    """How plugin orders are executed"""
    IMMEDIATE = "immediate"  # Execute immediately
    QUEUED = "queued"  # Queue for execution
    DRY_RUN = "dry_run"  # Simulate only


@dataclass
class CircuitBreaker:
    """
    Circuit breaker for plugin fault tolerance.

    Automatically disables plugins that fail repeatedly,
    then auto-resets after a cooldown period.

    States:
        CLOSED: Normal operation, plugin runs
        OPEN: Tripped due to failures, plugin blocked
        HALF_OPEN: Testing if plugin recovered
    """
    max_failures: int = 5  # Failures before tripping
    reset_after_seconds: float = 300.0  # Auto-reset after 5 minutes
    half_open_max_failures: int = 2  # Failures in half-open before re-tripping

    # State tracking
    consecutive_failures: int = 0
    total_failures: int = 0
    last_failure_time: Optional[datetime] = None
    tripped_at: Optional[datetime] = None
    state: str = "closed"  # closed, open, half_open

    def record_success(self):
        """Record a successful execution"""
        self.consecutive_failures = 0
        if self.state == "half_open":
            # Recovered - close the circuit
            self.state = "closed"
            self.tripped_at = None
            logger.info("Circuit breaker closed (plugin recovered)")

    def record_failure(self) -> bool:
        """
        Record a failure. Returns True if circuit should trip.
        """
        self.consecutive_failures += 1
        self.total_failures += 1
        self.last_failure_time = datetime.now()

        if self.state == "closed":
            if self.consecutive_failures >= self.max_failures:
                self._trip()
                return True
        elif self.state == "half_open":
            if self.consecutive_failures >= self.half_open_max_failures:
                self._trip()
                return True

        return False

    def _trip(self):
        """Trip the circuit breaker"""
        self.state = "open"
        self.tripped_at = datetime.now()
        logger.warning(
            f"Circuit breaker TRIPPED after {self.consecutive_failures} consecutive failures"
        )

    def should_allow(self) -> bool:
        """Check if execution should be allowed"""
        if self.state == "closed":
            return True

        if self.state == "open":
            # Check if enough time has passed for auto-reset
            if self.tripped_at:
                elapsed = (datetime.now() - self.tripped_at).total_seconds()
                if elapsed >= self.reset_after_seconds:
                    # Move to half-open state to test
                    self.state = "half_open"
                    self.consecutive_failures = 0
                    logger.info("Circuit breaker entering HALF-OPEN state (testing recovery)")
                    return True
            return False

        # half_open - allow limited executions to test
        return True

    def reset(self):
        """Manually reset the circuit breaker"""
        self.consecutive_failures = 0
        self.state = "closed"
        self.tripped_at = None
        logger.info("Circuit breaker manually reset")

    def to_dict(self) -> Dict[str, Any]:
        """Get circuit breaker status as dictionary"""
        return {
            "state": self.state,
            "consecutive_failures": self.consecutive_failures,
            "total_failures": self.total_failures,
            "max_failures": self.max_failures,
            "last_failure_time": (
                self.last_failure_time.isoformat()
                if self.last_failure_time else None
            ),
            "tripped_at": (
                self.tripped_at.isoformat()
                if self.tripped_at else None
            ),
            "reset_after_seconds": self.reset_after_seconds,
        }


@dataclass
class PluginConfig:
    """Configuration for a registered plugin"""
    plugin: PluginBase
    execution_mode: ExecutionMode = ExecutionMode.ON_BAR
    bar_timeframe: DataType = DataType.BAR_1MIN
    enabled: bool = True
    max_signals_per_run: int = 10
    cooldown_seconds: float = 0.0  # Minimum time between runs
    last_run: Optional[datetime] = None
    run_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None

    # Circuit breaker for fault tolerance
    circuit_breaker: CircuitBreaker = field(default_factory=CircuitBreaker)

    # Runtime parameters (can be modified via commands)
    parameters: Dict[str, Any] = field(default_factory=dict)

    # Dynamic loading info
    source_file: Optional[Path] = None


@dataclass
class PendingOrder:
    """An order pending execution"""
    plugin_name: str
    signal: TradeSignal
    contract: Contract
    order: Order
    created_at: datetime = field(default_factory=datetime.now)
    status: str = "pending"


@dataclass
class ExecutionResult:
    """Result of order execution"""
    plugin_name: str
    symbol: str
    action: str
    quantity: int
    order_id: Optional[int] = None
    success: bool = True
    error: Optional[str] = None
    executed_at: datetime = field(default_factory=datetime.now)


class PluginExecutive:
    """
    Plugin lifecycle manager and execution engine.

    Manages the execution of trading plugins, including:
    - Dynamic loading/unloading from file paths
    - Lifecycle management (start, stop, freeze, resume)
    - Custom request handling
    - Feeding real-time market data
    - Executing trade signals
    - MessageBus integration for indicator feeds

    Usage:
        from portfolio import Portfolio
        from data_feed import DataFeed
        from message_bus import MessageBus

        portfolio = Portfolio()
        portfolio.connect()
        portfolio.load()

        feed = DataFeed(portfolio)
        bus = MessageBus()
        executive = PluginExecutive(portfolio, feed, message_bus=bus)

        # Load plugin from file
        executive.load_plugin_from_file("/path/to/my_plugin.py")

        # Or register directly
        plugin = MyPlugin()
        plugin.load()
        executive.register_plugin(plugin)

        # Control lifecycle
        executive.start_plugin("my_plugin")
        executive.freeze_plugin("my_plugin")
        executive.resume_plugin("my_plugin")
        executive.stop_plugin("my_plugin")

        # Send custom requests
        response = executive.send_request("my_plugin", "get_metrics", {})

        # Start continuous execution
        feed.start()
        executive.start()

        # ... runs continuously ...

        executive.stop()
        feed.stop()
    """

    def __init__(
        self,
        portfolio,
        data_feed: DataFeed,
        message_bus: Optional[MessageBus] = None,
        order_mode: OrderExecutionMode = OrderExecutionMode.DRY_RUN,
        reconciliation_mode: ReconciliationMode = ReconciliationMode.NET,
        circuit_breaker_failures: int = 5,
        circuit_breaker_reset_seconds: float = 300.0,
        health_check_interval: float = 5.0,
        order_rate_limit: float = 10.0,
        order_burst_size: int = 10,
        auto_save_interval: float = 300.0,  # Auto-save state every 5 minutes
    ):
        """
        Initialize plugin executive.

        Args:
            portfolio: Portfolio instance for order execution
            data_feed: DataFeed instance for market data
            message_bus: Optional MessageBus for pub/sub communication
            order_mode: How orders should be executed
            reconciliation_mode: How to reconcile orders from multiple plugins
            circuit_breaker_failures: Consecutive failures before disabling plugin
            circuit_breaker_reset_seconds: Seconds before auto-resetting circuit breaker
            health_check_interval: Seconds between thread health checks
            order_rate_limit: Maximum orders per second (IB compliance)
            order_burst_size: Maximum burst capacity for orders
            auto_save_interval: Seconds between automatic state saves
        """
        self.portfolio = portfolio
        self.data_feed = data_feed
        self.message_bus = message_bus or MessageBus()
        self.order_mode = order_mode

        # Circuit breaker defaults for new plugins
        self._default_cb_failures = circuit_breaker_failures
        self._default_cb_reset_seconds = circuit_breaker_reset_seconds

        # Health monitoring config
        self._health_check_interval = health_check_interval
        self._auto_save_interval = auto_save_interval

        # Order rate limiting for IB compliance
        self._order_rate_limiter = OrderRateLimiter(
            orders_per_second=order_rate_limit,
            burst_size=order_burst_size,
        )

        # Order reconciler for netting orders from multiple plugins
        self._reconciler = OrderReconciler(mode=reconciliation_mode)

        # State
        self._running = False
        self._paused = False
        self._shutdown_event = Event()
        self._lock = RLock()

        # Registered plugins
        self._plugins: Dict[str, PluginConfig] = {}

        # Order execution
        self._order_queue: Queue = Queue()
        self._pending_orders: Dict[int, PendingOrder] = {}
        self._execution_history: List[ExecutionResult] = []
        self._max_history = 1000

        # Threads
        self._runner_thread: Optional[Thread] = None
        self._executor_thread: Optional[Thread] = None
        self._health_thread: Optional[Thread] = None

        # Thread restart tracking
        self._executor_restart_count = 0
        self._max_executor_restarts = 10

        # Auto-save tracking
        self._last_auto_save: Optional[datetime] = None

        # Callbacks
        self.on_signal: Optional[Callable[[str, TradeSignal], None]] = None
        self.on_execution: Optional[Callable[[ExecutionResult], None]] = None
        self.on_reconciled: Optional[Callable[[ReconciledOrder], None]] = None
        self.on_error: Optional[Callable[[str, Exception], None]] = None
        self.on_circuit_breaker_trip: Optional[Callable[[str], None]] = None
        self.on_plugin_state_change: Optional[Callable[[str, PluginState], None]] = None

        # Execution logging with commission tracking
        self._execution_log_writer = ExecutionLogWriter()
        self._pending_commissions: Dict[int, Dict] = {}  # order_id -> execution info
        self._exec_id_to_order: Dict[str, int] = {}  # exec_id -> order_id

        # Register commission callback with portfolio if available
        if portfolio and hasattr(portfolio, "_on_commission"):
            portfolio._on_commission = self._handle_commission_report

        # Statistics
        self._stats = {
            "started_at": None,
            "total_runs": 0,
            "total_signals": 0,
            "total_orders": 0,
            "total_errors": 0,
            "shares_saved_by_netting": 0,
            "executor_restarts": 0,
            "circuit_breaker_trips": 0,
            "rate_limit_delays": 0,
            "rate_limit_rejects": 0,
            "plugins_loaded": 0,
            "plugins_unloaded": 0,
        }

        # Initialize the system unassigned plugin for tracking unattributed
        # positions and cash
        self._init_unassigned_plugin()

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def is_running(self) -> bool:
        """Check if executive is running"""
        return self._running

    @property
    def is_paused(self) -> bool:
        """Check if executive is paused"""
        return self._paused

    @property
    def plugins(self) -> List[str]:
        """Get list of registered plugin names"""
        with self._lock:
            return list(self._plugins.keys())

    @property
    def stats(self) -> Dict[str, Any]:
        """Get executive statistics"""
        return self._stats.copy()

    @property
    def unassigned_plugin(self) -> Optional[UnassignedPlugin]:
        """Get the system unassigned plugin"""
        with self._lock:
            config = self._plugins.get(UNASSIGNED_PLUGIN_NAME)
            if config:
                return config.plugin
            return None

    @property
    def account_cash(self) -> float:
        """Get account cash balance from unassigned plugin"""
        plugin = self.unassigned_plugin
        if plugin:
            return plugin.cash_balance
        return 0.0

    # =========================================================================
    # Unassigned Plugin Management
    # =========================================================================

    def _init_unassigned_plugin(self):
        """Initialize the system unassigned plugin"""
        try:
            plugin = UnassignedPlugin(
                portfolio=self.portfolio,
                message_bus=self.message_bus,
            )

            if plugin.load():
                # Register with MANUAL execution mode - it doesn't generate signals
                self.register_plugin(
                    plugin,
                    execution_mode=ExecutionMode.MANUAL,
                    enabled=True,
                )
                plugin.start()
                logger.info("Initialized system unassigned plugin")
            else:
                logger.error("Failed to load unassigned plugin")

        except Exception as e:
            logger.error(f"Failed to initialize unassigned plugin: {e}")

    def sync_unassigned_holdings(self) -> bool:
        """
        Sync unassigned plugin with current portfolio state.

        Calculates which symbols are claimed by other plugins and
        syncs the remainder (plus cash) to the unassigned plugin.

        Returns:
            True if sync successful
        """
        plugin = self.unassigned_plugin
        if not plugin:
            logger.warning("No unassigned plugin to sync")
            return False

        # Collect claimed symbols from all other plugins
        claimed_symbols = self._get_claimed_symbols()

        # Calculate claimed cash (sum of cash in other plugin holdings)
        claimed_cash = self._get_claimed_cash()

        # Sync unassigned plugin
        return plugin.sync_from_portfolio(claimed_symbols, claimed_cash)

    def _get_claimed_symbols(self) -> Set[str]:
        """Get all symbols claimed by non-system plugins"""
        claimed = set()

        with self._lock:
            for name, config in self._plugins.items():
                plugin = config.plugin

                # Skip system plugins
                if plugin.is_system_plugin:
                    continue

                # Add symbols from plugin instruments
                for inst in plugin.instruments:
                    claimed.add(inst.symbol.upper())

                # Add symbols from plugin holdings
                if plugin.holdings:
                    for pos in plugin.holdings.current_positions:
                        claimed.add(pos.symbol.upper())

        return claimed

    def _get_claimed_cash(self) -> float:
        """Get total cash claimed by non-system plugins"""
        total = 0.0

        with self._lock:
            for name, config in self._plugins.items():
                plugin = config.plugin

                # Skip system plugins
                if plugin.is_system_plugin:
                    continue

                # Add cash from plugin holdings
                if plugin.holdings:
                    total += plugin.holdings.current_cash

        return total

    def get_holdings_summary(self) -> Dict[str, Any]:
        """
        Get a summary of holdings across all plugins including cash.

        Returns:
            Dict with account overview, plugin breakdown, and unassigned
        """
        summary = {
            "account": {
                "total_value": 0.0,
                "total_cash": 0.0,
                "total_positions_value": 0.0,
            },
            "plugins": {},
            "unassigned": None,
        }

        # Get account totals from portfolio
        if self.portfolio:
            summary["account"]["total_value"] = self.portfolio.total_value
            summary["account"]["total_positions_value"] = sum(
                p.market_value for p in self.portfolio.positions
            )
            account = self.portfolio.get_account_summary()
            if account and account.is_valid:
                summary["account"]["total_cash"] = account.available_funds or 0.0

        with self._lock:
            for name, config in self._plugins.items():
                plugin = config.plugin
                holdings = plugin.get_effective_holdings()

                plugin_summary = {
                    "is_system_plugin": plugin.is_system_plugin,
                    "state": plugin.state.value,
                    "cash": holdings.get("cash", 0.0),
                    "positions": holdings.get("positions", []),
                    "total_value": holdings.get("total_value", 0.0),
                }

                if plugin.is_system_plugin:
                    summary["unassigned"] = plugin_summary
                else:
                    summary["plugins"][name] = plugin_summary

        return summary

    # =========================================================================
    # Internal Transfers (Bookkeeping Only - No Actual Trades)
    # =========================================================================

    def transfer_cash(
        self,
        from_plugin: str,
        to_plugin: str,
        amount: float,
    ) -> Tuple[bool, str]:
        """
        Transfer cash between plugins (internal bookkeeping only).

        Args:
            from_plugin: Source plugin name
            to_plugin: Destination plugin name
            amount: Amount to transfer (positive)

        Returns:
            Tuple of (success, message)
        """
        if amount <= 0:
            return False, "Transfer amount must be positive"

        with self._lock:
            # Get source plugin
            from_config = self._plugins.get(from_plugin)
            if not from_config:
                return False, f"Source plugin '{from_plugin}' not found"

            # Get destination plugin
            to_config = self._plugins.get(to_plugin)
            if not to_config:
                return False, f"Destination plugin '{to_plugin}' not found"

            from_plugin_obj = from_config.plugin
            to_plugin_obj = to_config.plugin

            # Check source has sufficient cash
            source_cash = from_plugin_obj.get_effective_cash()
            if source_cash < amount:
                return False, f"Insufficient cash in '{from_plugin}': ${source_cash:,.2f} < ${amount:,.2f}"

            # Perform transfer
            # For plugins with Holdings object
            if from_plugin_obj.holdings:
                from_plugin_obj.holdings.add_cash(-amount)
                from_plugin_obj.save_holdings()
            elif hasattr(from_plugin_obj, '_cash_balance'):
                # For UnassignedPlugin
                from_plugin_obj._cash_balance -= amount

            if to_plugin_obj.holdings:
                to_plugin_obj.holdings.add_cash(amount)
                to_plugin_obj.save_holdings()
            elif hasattr(to_plugin_obj, '_cash_balance'):
                to_plugin_obj._cash_balance += amount

            logger.info(f"Transferred ${amount:,.2f} cash: {from_plugin} -> {to_plugin}")
            return True, f"Transferred ${amount:,.2f} from '{from_plugin}' to '{to_plugin}'"

    def transfer_position(
        self,
        from_plugin: str,
        to_plugin: str,
        symbol: str,
        quantity: float,
        price: Optional[float] = None,
    ) -> Tuple[bool, str]:
        """
        Transfer a position between plugins (internal bookkeeping only).

        Args:
            from_plugin: Source plugin name
            to_plugin: Destination plugin name
            symbol: Symbol to transfer
            quantity: Quantity to transfer (positive)
            price: Current price (optional, will use portfolio price if available)

        Returns:
            Tuple of (success, message)
        """
        if quantity <= 0:
            return False, "Transfer quantity must be positive"

        symbol = symbol.upper()

        with self._lock:
            # Get source plugin
            from_config = self._plugins.get(from_plugin)
            if not from_config:
                return False, f"Source plugin '{from_plugin}' not found"

            # Get destination plugin
            to_config = self._plugins.get(to_plugin)
            if not to_config:
                return False, f"Destination plugin '{to_plugin}' not found"

            from_plugin_obj = from_config.plugin
            to_plugin_obj = to_config.plugin

            # Get current price from portfolio if not provided
            if price is None and self.portfolio:
                pos = self.portfolio.get_position(symbol)
                if pos:
                    price = pos.current_price
                else:
                    price = 0.0

            # Check source has sufficient quantity
            source_qty, _ = from_plugin_obj.get_effective_position(symbol)
            if source_qty < quantity:
                return False, f"Insufficient {symbol} in '{from_plugin}': {source_qty:.2f} < {quantity:.2f}"

            # Get cost basis from source
            cost_basis = 0.0
            if from_plugin_obj.holdings:
                pos = from_plugin_obj.holdings.get_position(symbol)
                if pos:
                    cost_basis = pos.cost_basis
            elif hasattr(from_plugin_obj, '_holdings') and from_plugin_obj._holdings:
                # For UnassignedPlugin with _holdings list
                for hp in from_plugin_obj._holdings.current_positions:
                    if hp.symbol == symbol:
                        cost_basis = hp.cost_basis
                        break

            # Perform transfer - remove from source
            if from_plugin_obj.holdings:
                if not from_plugin_obj.holdings.remove_position(symbol, quantity):
                    return False, f"Failed to remove {quantity} {symbol} from '{from_plugin}'"
                from_plugin_obj.save_holdings()
            elif hasattr(from_plugin_obj, '_holdings') and from_plugin_obj._holdings:
                # UnassignedPlugin uses _holdings directly
                if not from_plugin_obj._holdings.remove_position(symbol, quantity):
                    return False, f"Failed to remove {quantity} {symbol} from '{from_plugin}'"

            # Add to destination
            if to_plugin_obj.holdings:
                to_plugin_obj.holdings.add_position(
                    symbol=symbol,
                    quantity=quantity,
                    cost_basis=cost_basis,
                    current_price=price or 0.0,
                )
                to_plugin_obj.save_holdings()
            elif hasattr(to_plugin_obj, '_holdings') and to_plugin_obj._holdings:
                to_plugin_obj._holdings.add_position(
                    symbol=symbol,
                    quantity=quantity,
                    cost_basis=cost_basis,
                    current_price=price or 0.0,
                )

            value = quantity * (price or 0.0)
            logger.info(f"Transferred {quantity} {symbol} (${value:,.2f}): {from_plugin} -> {to_plugin}")
            return True, f"Transferred {quantity:.2f} {symbol} from '{from_plugin}' to '{to_plugin}'"

    def get_transferable_positions(self, plugin_name: str) -> List[Dict[str, Any]]:
        """
        Get positions that can be transferred from a plugin.

        Returns list of dicts with symbol, quantity, value.
        """
        with self._lock:
            config = self._plugins.get(plugin_name)
            if not config:
                return []

            plugin = config.plugin
            holdings = plugin.get_effective_holdings()
            positions = holdings.get("positions", [])

            return [
                {
                    "symbol": p.get("symbol", p.get("symbol")),
                    "quantity": p.get("quantity", 0),
                    "value": p.get("market_value", p.get("quantity", 0) * p.get("current_price", 0)),
                }
                for p in positions
                if p.get("quantity", 0) > 0
            ]

    def get_transferable_cash(self, plugin_name: str) -> float:
        """Get available cash that can be transferred from a plugin."""
        with self._lock:
            config = self._plugins.get(plugin_name)
            if not config:
                return 0.0

            return config.plugin.get_effective_cash()

    # =========================================================================
    # Dynamic Plugin Loading
    # =========================================================================

    def load_plugin_from_file(
        self,
        file_path: str,
        execution_mode: ExecutionMode = ExecutionMode.ON_BAR,
        bar_timeframe: DataType = DataType.BAR_1MIN,
        enabled: bool = True,
    ) -> Optional[str]:
        """
        Load a plugin from a Python file.

        The file must contain a class that inherits from PluginBase.
        The class will be instantiated and registered.

        Args:
            file_path: Path to the Python file containing the plugin
            execution_mode: When to trigger the plugin
            bar_timeframe: Which bar timeframe triggers ON_BAR mode
            enabled: Whether the plugin starts enabled

        Returns:
            Plugin name if loaded successfully, None otherwise
        """
        # Import plugin_loader here to avoid circular imports
        from plugin_loader import PluginLoader

        try:
            loader = PluginLoader()
            plugin = loader.load_from_file(file_path)

            if not plugin:
                logger.error(f"Failed to load plugin from {file_path}")
                return None

            # Set up MessageBus
            plugin.set_message_bus(self.message_bus)

            # Load plugin data (instruments, holdings)
            if not plugin.load():
                logger.error(f"Failed to load plugin data for {plugin.name}")
                return None

            # Register with executive
            if not self.register_plugin(
                plugin,
                execution_mode=execution_mode,
                bar_timeframe=bar_timeframe,
                enabled=enabled,
            ):
                return None

            # Track source file
            with self._lock:
                self._plugins[plugin.name].source_file = Path(file_path)

            self._stats["plugins_loaded"] += 1
            logger.info(f"Loaded plugin '{plugin.name}' from {file_path}")
            return plugin.name

        except Exception as e:
            logger.error(f"Error loading plugin from {file_path}: {e}")
            logger.debug(traceback.format_exc())
            return None

    def unload_plugin(self, name: str) -> bool:
        """
        Unload a plugin.

        Stops the plugin if running, then removes it from the executive.
        System plugins cannot be unloaded.

        Args:
            name: Plugin name

        Returns:
            True if unloaded successfully
        """
        with self._lock:
            if name not in self._plugins:
                logger.warning(f"Plugin '{name}' not found")
                return False

            config = self._plugins[name]

            # Prevent unloading system plugins
            if config.plugin.is_system_plugin:
                logger.warning(f"Cannot unload system plugin '{name}'")
                return False

            # Stop plugin if running
            if config.plugin.state in (PluginState.STARTED, PluginState.FROZEN):
                try:
                    config.plugin.stop()
                except Exception as e:
                    logger.error(f"Error stopping plugin '{name}': {e}")

            # Unsubscribe from all channels
            config.plugin.unsubscribe_all()

            # Remove from registry
            del self._plugins[name]

        self._stats["plugins_unloaded"] += 1
        logger.info(f"Unloaded plugin '{name}'")
        return True

    # =========================================================================
    # Plugin Registration
    # =========================================================================

    def register_plugin(
        self,
        plugin: PluginBase,
        execution_mode: ExecutionMode = ExecutionMode.ON_BAR,
        bar_timeframe: DataType = DataType.BAR_1MIN,
        enabled: bool = True,
        circuit_breaker_failures: Optional[int] = None,
        circuit_breaker_reset_seconds: Optional[float] = None,
    ) -> bool:
        """
        Register a plugin with the executive.

        Args:
            plugin: Plugin instance
            execution_mode: When to trigger the plugin
            bar_timeframe: Which bar timeframe triggers ON_BAR mode
            enabled: Whether the plugin is enabled
            circuit_breaker_failures: Custom failure threshold
            circuit_breaker_reset_seconds: Custom reset time

        Returns:
            True if registered successfully
        """
        if not plugin.is_loaded:
            logger.warning(f"Plugin '{plugin.name}' not loaded, loading now...")
            if not plugin.load():
                logger.error(f"Failed to load plugin '{plugin.name}'")
                return False

        # Set up MessageBus
        plugin.set_message_bus(self.message_bus)

        with self._lock:
            if plugin.name in self._plugins:
                logger.warning(f"Plugin '{plugin.name}' already registered")
                return False

            # Create circuit breaker with custom or default settings
            cb = CircuitBreaker(
                max_failures=circuit_breaker_failures or self._default_cb_failures,
                reset_after_seconds=circuit_breaker_reset_seconds or self._default_cb_reset_seconds,
            )

            config = PluginConfig(
                plugin=plugin,
                execution_mode=execution_mode,
                bar_timeframe=bar_timeframe,
                enabled=enabled,
                circuit_breaker=cb,
            )
            self._plugins[plugin.name] = config

        logger.info(
            f"Registered plugin '{plugin.name}' "
            f"(mode={execution_mode.value}, timeframe={bar_timeframe.value})"
        )
        return True

    def unregister_plugin(self, name: str) -> bool:
        """
        Unregister a plugin (alias for unload_plugin).

        Args:
            name: Plugin name

        Returns:
            True if unregistered
        """
        return self.unload_plugin(name)

    # =========================================================================
    # Plugin Lifecycle Control
    # =========================================================================

    def start_plugin(self, name: str) -> bool:
        """
        Start a plugin.

        Transitions from LOADED to STARTED state.

        Args:
            name: Plugin name

        Returns:
            True if started successfully
        """
        with self._lock:
            if name not in self._plugins:
                logger.warning(f"Plugin '{name}' not found")
                return False

            config = self._plugins[name]
            plugin = config.plugin

            if plugin.state not in (PluginState.LOADED, PluginState.STOPPED):
                logger.warning(
                    f"Plugin '{name}' cannot start from state {plugin.state.value}"
                )
                return False

        try:
            if plugin.start():
                plugin.state = PluginState.STARTED
                logger.info(f"Plugin '{name}' started")
                if self.on_plugin_state_change:
                    self.on_plugin_state_change(name, PluginState.STARTED)
                return True
            else:
                plugin.state = PluginState.ERROR
                logger.error(f"Plugin '{name}' failed to start")
                return False

        except Exception as e:
            plugin.state = PluginState.ERROR
            logger.error(f"Error starting plugin '{name}': {e}")
            logger.debug(traceback.format_exc())
            return False

    def stop_plugin(self, name: str) -> bool:
        """
        Stop a plugin.

        Transitions from STARTED/FROZEN to STOPPED state.

        Args:
            name: Plugin name

        Returns:
            True if stopped successfully
        """
        with self._lock:
            if name not in self._plugins:
                logger.warning(f"Plugin '{name}' not found")
                return False

            config = self._plugins[name]
            plugin = config.plugin

            if plugin.state not in (PluginState.STARTED, PluginState.FROZEN):
                logger.warning(
                    f"Plugin '{name}' cannot stop from state {plugin.state.value}"
                )
                return False

        try:
            if plugin.stop():
                plugin.state = PluginState.STOPPED
                logger.info(f"Plugin '{name}' stopped")
                if self.on_plugin_state_change:
                    self.on_plugin_state_change(name, PluginState.STOPPED)
                return True
            else:
                plugin.state = PluginState.ERROR
                logger.error(f"Plugin '{name}' failed to stop")
                return False

        except Exception as e:
            plugin.state = PluginState.ERROR
            logger.error(f"Error stopping plugin '{name}': {e}")
            logger.debug(traceback.format_exc())
            return False

    def freeze_plugin(self, name: str) -> bool:
        """
        Freeze a plugin.

        Transitions from STARTED to FROZEN state.
        Plugin will save its state and pause processing.

        Args:
            name: Plugin name

        Returns:
            True if frozen successfully
        """
        with self._lock:
            if name not in self._plugins:
                logger.warning(f"Plugin '{name}' not found")
                return False

            config = self._plugins[name]
            plugin = config.plugin

            if plugin.state != PluginState.STARTED:
                logger.warning(
                    f"Plugin '{name}' cannot freeze from state {plugin.state.value}"
                )
                return False

        try:
            if plugin.freeze():
                plugin.state = PluginState.FROZEN
                logger.info(f"Plugin '{name}' frozen")
                if self.on_plugin_state_change:
                    self.on_plugin_state_change(name, PluginState.FROZEN)
                return True
            else:
                plugin.state = PluginState.ERROR
                logger.error(f"Plugin '{name}' failed to freeze")
                return False

        except Exception as e:
            plugin.state = PluginState.ERROR
            logger.error(f"Error freezing plugin '{name}': {e}")
            logger.debug(traceback.format_exc())
            return False

    def resume_plugin(self, name: str) -> bool:
        """
        Resume a frozen plugin.

        Transitions from FROZEN to STARTED state.

        Args:
            name: Plugin name

        Returns:
            True if resumed successfully
        """
        with self._lock:
            if name not in self._plugins:
                logger.warning(f"Plugin '{name}' not found")
                return False

            config = self._plugins[name]
            plugin = config.plugin

            if plugin.state != PluginState.FROZEN:
                logger.warning(
                    f"Plugin '{name}' cannot resume from state {plugin.state.value}"
                )
                return False

        try:
            if plugin.resume():
                plugin.state = PluginState.STARTED
                logger.info(f"Plugin '{name}' resumed")
                if self.on_plugin_state_change:
                    self.on_plugin_state_change(name, PluginState.STARTED)
                return True
            else:
                plugin.state = PluginState.ERROR
                logger.error(f"Plugin '{name}' failed to resume")
                return False

        except Exception as e:
            plugin.state = PluginState.ERROR
            logger.error(f"Error resuming plugin '{name}': {e}")
            logger.debug(traceback.format_exc())
            return False

    # =========================================================================
    # Custom Request Handling
    # =========================================================================

    def send_request(
        self,
        name: str,
        request_type: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Send a custom request to a plugin.

        Args:
            name: Plugin name
            request_type: Type of request (e.g., "get_metrics", "set_config")
            payload: Request payload

        Returns:
            Response dictionary with at least "success" key
        """
        with self._lock:
            if name not in self._plugins:
                return {"success": False, "message": f"Plugin '{name}' not found"}

            plugin = self._plugins[name].plugin

        try:
            response = plugin.handle_request(request_type, payload)
            return response

        except Exception as e:
            logger.error(f"Error handling request for plugin '{name}': {e}")
            return {
                "success": False,
                "message": f"Error: {str(e)}",
            }

    # =========================================================================
    # Enable/Disable (for continuous execution)
    # =========================================================================

    def enable_plugin(self, name: str, enabled: bool = True) -> bool:
        """Enable or disable a plugin for continuous execution"""
        with self._lock:
            if name in self._plugins:
                self._plugins[name].enabled = enabled
                logger.info(f"Plugin '{name}' {'enabled' if enabled else 'disabled'}")
                return True
        return False

    # =========================================================================
    # Parameter Management
    # =========================================================================

    def set_plugin_parameter(
        self,
        name: str,
        key: str,
        value: Any,
    ) -> bool:
        """
        Set a runtime parameter for a plugin.

        Args:
            name: Plugin name
            key: Parameter key
            value: Parameter value

        Returns:
            True if set successfully
        """
        with self._lock:
            if name not in self._plugins:
                logger.warning(f"Plugin '{name}' not found")
                return False

            config = self._plugins[name]
            config.parameters[key] = value

            # Also try to set on the plugin itself
            try:
                if hasattr(config.plugin, 'set_parameter'):
                    config.plugin.set_parameter(key, value)
            except Exception as e:
                logger.warning(f"Plugin '{name}' set_parameter failed: {e}")

            logger.info(f"Plugin '{name}' parameter '{key}' set to '{value}'")
            return True

    def get_plugin_parameters(self, name: str) -> Optional[Dict[str, Any]]:
        """Get runtime parameters for a plugin"""
        with self._lock:
            if name not in self._plugins:
                return None

            config = self._plugins[name]
            params = dict(config.parameters)

            try:
                if hasattr(config.plugin, 'get_parameters'):
                    algo_params = config.plugin.get_parameters()
                    if algo_params:
                        algo_params.update(params)
                        params = algo_params
            except Exception as e:
                logger.warning(f"Plugin '{name}' get_parameters failed: {e}")

            return params

    # =========================================================================
    # Circuit Breaker Management
    # =========================================================================

    def reset_circuit_breaker(self, name: str) -> bool:
        """Manually reset a plugin's circuit breaker"""
        with self._lock:
            if name not in self._plugins:
                logger.warning(f"Plugin '{name}' not found")
                return False

            self._plugins[name].circuit_breaker.reset()
            logger.info(f"Circuit breaker reset for plugin '{name}'")
            return True

    def get_circuit_breaker_status(self, name: str) -> Optional[Dict[str, Any]]:
        """Get circuit breaker status for a plugin"""
        with self._lock:
            if name not in self._plugins:
                return None
            return self._plugins[name].circuit_breaker.to_dict()

    def get_all_circuit_breakers(self) -> Dict[str, Dict[str, Any]]:
        """Get circuit breaker status for all plugins"""
        with self._lock:
            return {
                name: config.circuit_breaker.to_dict()
                for name, config in self._plugins.items()
            }

    # =========================================================================
    # MessageBus / Feed Discovery
    # =========================================================================

    def list_feeds(self) -> List[Dict[str, Any]]:
        """
        List all available MessageBus channels (feeds).

        Returns:
            List of channel info dictionaries
        """
        channels = self.message_bus.list_channels()
        return [c.to_dict() for c in channels]

    def get_feed_history(
        self,
        channel: str,
        count: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Get message history for a channel.

        Args:
            channel: Channel name
            count: Maximum messages to return

        Returns:
            List of message dictionaries
        """
        messages = self.message_bus.get_history(channel, count=count)
        return [m.to_dict() for m in messages]

    # =========================================================================
    # Continuous Execution
    # =========================================================================

    def start(self) -> bool:
        """
        Start continuous execution.

        Returns:
            True if started successfully
        """
        if self._running:
            logger.warning("Executive already running")
            return True

        if not self.data_feed.is_running:
            logger.error("Data feed not running - start it first")
            return False

        self._running = True
        self._paused = False
        self._shutdown_event.clear()
        self._executor_restart_count = 0
        self._last_auto_save = datetime.now()
        self._stats["started_at"] = datetime.now().isoformat()

        # Set up data feed callbacks
        self._setup_data_callbacks()

        # Start executor thread
        self._start_executor_thread()

        # Start health monitoring thread
        self._health_thread = Thread(
            target=self._health_monitor_loop,
            daemon=True,
            name="PluginExecutive-Health"
        )
        self._health_thread.start()

        # Initial sync of unassigned holdings
        self.sync_unassigned_holdings()

        logger.info(f"Plugin executive started with {len(self._plugins)} plugins")
        return True

    def _start_executor_thread(self):
        """Start or restart the executor thread"""
        self._executor_thread = Thread(
            target=self._executor_loop_wrapper,
            daemon=True,
            name="PluginExecutive-Executor"
        )
        self._executor_thread.start()

    def stop(self):
        """Stop continuous execution"""
        if not self._running:
            return

        logger.info("Stopping plugin executive...")
        self._running = False
        self._shutdown_event.set()

        # Save state for all running plugins
        self._auto_save_all_states()

        # Wait for threads
        if self._executor_thread and self._executor_thread.is_alive():
            self._executor_thread.join(timeout=5.0)

        if self._health_thread and self._health_thread.is_alive():
            self._health_thread.join(timeout=5.0)

        # Process any remaining orders
        self._drain_order_queue()

        logger.info("Plugin executive stopped")

    def pause(self):
        """Pause continuous execution (data still flows)"""
        self._paused = True
        logger.info("Plugin executive paused")

    def resume(self):
        """Resume continuous execution"""
        self._paused = False
        logger.info("Plugin executive resumed")

    def trigger_plugin(self, name: str) -> Optional[PluginResult]:
        """
        Manually trigger a plugin run.

        Args:
            name: Plugin name

        Returns:
            PluginResult or None
        """
        with self._lock:
            config = self._plugins.get(name)
            if not config:
                logger.error(f"Plugin '{name}' not found")
                return None

        return self._run_plugin(config)

    # =========================================================================
    # Data Feed Callbacks
    # =========================================================================

    def _setup_data_callbacks(self):
        """Set up callbacks on data feed"""
        # Store original callbacks
        self._original_on_tick = self.data_feed.on_tick
        self._original_on_bar = self.data_feed.on_bar

        def on_tick(symbol: str, tick: TickData):
            if self._original_on_tick:
                self._original_on_tick(symbol, tick)
            if not self._paused:
                self._on_tick(symbol, tick)

        def on_bar(symbol: str, bar: Bar, data_type: DataType):
            if self._original_on_bar:
                self._original_on_bar(symbol, bar, data_type)
            if not self._paused:
                self._on_bar(symbol, bar, data_type)

        self.data_feed.on_tick = on_tick
        self.data_feed.on_bar = on_bar

    def _on_tick(self, symbol: str, tick: TickData):
        """Handle incoming tick - trigger ON_TICK plugins"""
        with self._lock:
            for name, config in self._plugins.items():
                if (config.enabled and
                    config.execution_mode == ExecutionMode.ON_TICK and
                    config.plugin.state == PluginState.STARTED):
                    self._schedule_run(config)

    def _on_bar(self, symbol: str, bar: Bar, data_type: DataType):
        """Handle incoming bar - trigger ON_BAR plugins"""
        with self._lock:
            for name, config in self._plugins.items():
                if (config.enabled and
                    config.execution_mode == ExecutionMode.ON_BAR and
                    config.bar_timeframe == data_type and
                    config.plugin.state == PluginState.STARTED):
                    self._schedule_run(config)

    def _schedule_run(self, config: PluginConfig):
        """Schedule a plugin run (respects cooldown and circuit breaker)"""
        now = datetime.now()

        # Check circuit breaker
        if not config.circuit_breaker.should_allow():
            return

        # Check cooldown
        if config.cooldown_seconds > 0 and config.last_run:
            elapsed = (now - config.last_run).total_seconds()
            if elapsed < config.cooldown_seconds:
                return

        # Run in executor thread
        try:
            self._order_queue.put(("RUN", config.plugin.name), block=False)
        except Exception as e:
            logger.error(f"Failed to schedule plugin run: {e}")

    def _run_plugin(self, config: PluginConfig) -> PluginResult:
        """Run a single plugin"""
        plugin = config.plugin
        name = plugin.name

        # Check plugin state
        if plugin.state != PluginState.STARTED:
            return PluginResult(
                plugin_name=name,
                timestamp=datetime.now(),
                success=False,
                error=f"Plugin in {plugin.state.value} state",
            )

        # Check circuit breaker
        if not config.circuit_breaker.should_allow():
            return PluginResult(
                plugin_name=name,
                timestamp=datetime.now(),
                success=False,
                error="Circuit breaker open",
            )

        try:
            # Prepare market data from feed
            market_data = self._prepare_market_data(plugin)

            # Run plugin
            result = plugin.run(market_data=market_data)

            config.last_run = datetime.now()
            config.run_count += 1
            self._stats["total_runs"] += 1

            if result.success:
                config.circuit_breaker.record_success()

                # Process signals
                signals = result.actionable_signals[:config.max_signals_per_run]
                self._stats["total_signals"] += len(signals)

                for signal in signals:
                    self._process_signal(name, signal)

                    if self.on_signal:
                        try:
                            self.on_signal(name, signal)
                        except Exception as e:
                            logger.error(f"Error in signal callback: {e}")

                # Reconcile and execute
                self.reconcile_and_execute()

            else:
                config.error_count += 1
                config.last_error = result.error
                self._stats["total_errors"] += 1
                logger.error(f"Plugin '{name}' failed: {result.error}")

                if config.circuit_breaker.record_failure():
                    self._stats["circuit_breaker_trips"] += 1
                    logger.warning(f"Circuit breaker tripped for plugin '{name}'")
                    if self.on_circuit_breaker_trip:
                        try:
                            self.on_circuit_breaker_trip(name)
                        except Exception as e:
                            logger.error(f"Error in circuit breaker callback: {e}")

            return result

        except Exception as e:
            config.error_count += 1
            config.last_error = str(e)
            self._stats["total_errors"] += 1
            logger.error(f"Error running plugin '{name}': {e}")
            logger.debug(traceback.format_exc())

            if config.circuit_breaker.record_failure():
                self._stats["circuit_breaker_trips"] += 1
                if self.on_circuit_breaker_trip:
                    try:
                        self.on_circuit_breaker_trip(name)
                    except:
                        pass

            if self.on_error:
                try:
                    self.on_error(name, e)
                except:
                    pass

            return PluginResult(
                plugin_name=name,
                timestamp=datetime.now(),
                success=False,
                error=str(e),
            )

    def _prepare_market_data(
        self,
        plugin: PluginBase,
    ) -> Dict[str, List[Dict]]:
        """Prepare market data for a plugin from the data feed"""
        market_data = {}

        for instrument in plugin.enabled_instruments:
            symbol = instrument.symbol

            # Get bars from feed
            bars = self.data_feed.get_bars(
                symbol,
                DataType.BAR_1MIN,
                count=plugin.required_bars * 2
            )

            if not bars:
                bars = self.data_feed.get_bars(
                    symbol,
                    DataType.BAR_5SEC,
                    count=plugin.required_bars * 12
                )

            market_data[symbol] = [
                {
                    "date": bar.timestamp,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                }
                for bar in bars
            ]

        return market_data

    def _process_signal(self, plugin_name: str, signal: TradeSignal):
        """Process a trade signal from a plugin"""
        if not signal.is_actionable:
            return

        logger.info(
            f"[{plugin_name}] Signal: {signal.action} {signal.quantity} {signal.symbol} "
            f"(confidence={signal.confidence:.2f}, reason={signal.reason})"
        )

        contract = self._get_contract(plugin_name, signal.symbol)
        if not contract:
            logger.error(f"No contract found for {signal.symbol}")
            return

        self._reconciler.add_signal(plugin_name, signal, contract)

    def reconcile_and_execute(self):
        """Reconcile pending signals and execute netted orders"""
        reconciled_orders = self._reconciler.reconcile()

        reconciler_stats = self._reconciler.stats
        self._stats["shares_saved_by_netting"] = reconciler_stats.get("shares_saved", 0)

        for reconciled in reconciled_orders:
            logger.info(
                f"Reconciled order: {reconciled.action} {reconciled.net_quantity} {reconciled.symbol}"
            )

            if self.on_reconciled:
                try:
                    self.on_reconciled(reconciled)
                except Exception as e:
                    logger.error(f"Error in on_reconciled callback: {e}")

            if self.order_mode == OrderExecutionMode.DRY_RUN:
                self._log_dry_run_reconciled(reconciled)
            else:
                try:
                    self._order_queue.put(("RECONCILED", reconciled), block=False)
                except Exception as e:
                    logger.error(f"Failed to queue reconciled order: {e}")

    def _log_dry_run_reconciled(self, reconciled: ReconciledOrder):
        """Log a dry run execution of a reconciled order"""
        for ps in reconciled.contributing_signals:
            result = ExecutionResult(
                plugin_name=ps.algorithm_name,
                symbol=reconciled.symbol,
                action=reconciled.action,
                quantity=ps.signal.quantity,
                success=True,
            )
            self._add_to_history(result)

            if self.on_execution:
                try:
                    self.on_execution(result)
                except Exception as e:
                    logger.error(f"Error in execution callback: {e}")

        plugin_names = [ps.algorithm_name for ps in reconciled.contributing_signals]
        logger.info(
            f"[DRY RUN] Net order: {reconciled.action} {reconciled.net_quantity} {reconciled.symbol} "
            f"(from: {', '.join(plugin_names)})"
        )

    def _get_contract(
        self,
        plugin_name: str,
        symbol: str,
    ) -> Optional[Contract]:
        """Get contract for a symbol from plugin instruments"""
        with self._lock:
            config = self._plugins.get(plugin_name)
            if not config:
                return None

            instrument = config.plugin.get_instrument(symbol)
            if instrument:
                return instrument.to_contract()

            pos = self.portfolio.get_position(symbol)
            if pos and pos.contract:
                return pos.contract

        return None

    # =========================================================================
    # Executor Thread
    # =========================================================================

    def _executor_loop_wrapper(self):
        """Wrapper around executor loop that catches fatal exceptions"""
        try:
            self._executor_loop()
        except Exception as e:
            logger.critical(f"Executor thread crashed: {e}")
            logger.critical(traceback.format_exc())

    def _executor_loop(self):
        """Background loop for order execution"""
        logger.debug("Executor thread started")

        while not self._shutdown_event.is_set():
            try:
                item = self._order_queue.get(timeout=0.1)

                if item[0] == "RUN":
                    plugin_name = item[1]
                    with self._lock:
                        config = self._plugins.get(plugin_name)
                    if config:
                        self._run_plugin(config)

                elif item[0] == "RECONCILED":
                    reconciled = item[1]
                    if self._acquire_rate_limit_token(timeout=5.0):
                        self._execute_reconciled_order(reconciled)
                    else:
                        logger.warning(
                            f"Order rate limit exceeded for {reconciled.symbol}, order dropped"
                        )

            except Empty:
                continue
            except Exception as e:
                logger.error(f"Executor error: {e}")
                logger.debug(traceback.format_exc())

        logger.debug("Executor thread stopped")

    def _acquire_rate_limit_token(self, timeout: float = 5.0) -> bool:
        """Acquire a rate limit token for order execution"""
        prev_delayed = self._order_rate_limiter._limiter._stats.requests_delayed

        result = self._order_rate_limiter.acquire(blocking=True, timeout=timeout)

        curr_delayed = self._order_rate_limiter._limiter._stats.requests_delayed
        if curr_delayed > prev_delayed:
            self._stats["rate_limit_delays"] += 1

        if not result:
            self._stats["rate_limit_rejects"] += 1

        return result

    def _execute_reconciled_order(self, reconciled: ReconciledOrder):
        """Execute a reconciled order"""
        try:
            order = self._reconciler.create_ib_order(reconciled)

            order_id = self.portfolio.place_order(
                reconciled.contract,
                reconciled.action,
                reconciled.net_quantity,
                order_type="MKT",
            )

            if order_id:
                self._reconciler.register_execution(order_id, reconciled)
                self._register_pending_execution(order_id, reconciled)
                self._stats["total_orders"] += 1

                for ps in reconciled.contributing_signals:
                    result = ExecutionResult(
                        plugin_name=ps.algorithm_name,
                        symbol=reconciled.symbol,
                        action=reconciled.action,
                        quantity=ps.signal.quantity,
                        order_id=order_id,
                        success=True,
                    )
                    self._add_to_history(result)

                    if self.on_execution:
                        try:
                            self.on_execution(result)
                        except Exception as e:
                            logger.error(f"Error in execution callback: {e}")

                plugin_names = [ps.algorithm_name for ps in reconciled.contributing_signals]
                logger.info(
                    f"[EXECUTED] Net order: {reconciled.action} {reconciled.net_quantity} "
                    f"{reconciled.symbol} (order_id={order_id}, from: {', '.join(plugin_names)})"
                )
            else:
                logger.error(f"Failed to place reconciled order for {reconciled.symbol}")
                for ps in reconciled.contributing_signals:
                    result = ExecutionResult(
                        plugin_name=ps.algorithm_name,
                        symbol=reconciled.symbol,
                        action=reconciled.action,
                        quantity=ps.signal.quantity,
                        success=False,
                        error="Failed to place order",
                    )
                    self._add_to_history(result)

        except Exception as e:
            logger.error(f"Error executing reconciled order: {e}")
            for ps in reconciled.contributing_signals:
                result = ExecutionResult(
                    plugin_name=ps.algorithm_name,
                    symbol=reconciled.symbol,
                    action=reconciled.action,
                    quantity=ps.signal.quantity,
                    success=False,
                    error=str(e),
                )
                self._add_to_history(result)

    # =========================================================================
    # Health Monitor
    # =========================================================================

    def _health_monitor_loop(self):
        """Background loop that monitors thread health"""
        logger.debug("Health monitor thread started")

        while not self._shutdown_event.is_set():
            try:
                if self._shutdown_event.wait(self._health_check_interval):
                    break

                # Check executor thread
                if self._executor_thread and not self._executor_thread.is_alive():
                    if self._running and not self._shutdown_event.is_set():
                        if self._executor_restart_count < self._max_executor_restarts:
                            self._executor_restart_count += 1
                            self._stats["executor_restarts"] += 1
                            logger.warning(
                                f"Executor thread died - restarting "
                                f"(attempt {self._executor_restart_count}/{self._max_executor_restarts})"
                            )
                            self._start_executor_thread()
                        else:
                            logger.critical(
                                f"Executor thread died and max restarts exceeded"
                            )

                # Auto-save state periodically
                if self._last_auto_save:
                    elapsed = (datetime.now() - self._last_auto_save).total_seconds()
                    if elapsed >= self._auto_save_interval:
                        self._auto_save_all_states()
                        self._last_auto_save = datetime.now()

                # Log circuit breaker status
                with self._lock:
                    for name, config in self._plugins.items():
                        cb = config.circuit_breaker
                        if cb.state == "open":
                            elapsed = 0
                            if cb.tripped_at:
                                elapsed = (datetime.now() - cb.tripped_at).total_seconds()
                            remaining = cb.reset_after_seconds - elapsed
                            if remaining > 0:
                                logger.debug(
                                    f"Circuit breaker for '{name}' is OPEN "
                                    f"(resets in {remaining:.0f}s)"
                                )

            except Exception as e:
                logger.error(f"Health monitor error: {e}")
                logger.debug(traceback.format_exc())

        logger.debug("Health monitor thread stopped")

    def _auto_save_all_states(self):
        """Auto-save state for all running plugins"""
        with self._lock:
            for name, config in self._plugins.items():
                plugin = config.plugin
                if plugin.state in (PluginState.STARTED, PluginState.FROZEN):
                    try:
                        # Let plugin save its own state
                        if hasattr(plugin, 'get_state_for_save'):
                            state = plugin.get_state_for_save()
                            plugin.save_state(state)
                        else:
                            # Basic state save
                            plugin.save_state({
                                "auto_saved": True,
                                "run_count": config.run_count,
                                "last_run": config.last_run.isoformat() if config.last_run else None,
                            })
                    except Exception as e:
                        logger.error(f"Error auto-saving state for plugin '{name}': {e}")

    def _add_to_history(self, result: ExecutionResult):
        """Add result to execution history"""
        self._execution_history.append(result)
        if len(self._execution_history) > self._max_history:
            self._execution_history = self._execution_history[-self._max_history:]

    def _drain_order_queue(self):
        """Process remaining orders in queue on shutdown"""
        while not self._order_queue.empty():
            try:
                item = self._order_queue.get_nowait()
                if item[0] == "RECONCILED":
                    reconciled = item[1]
                    logger.warning(
                        f"Discarding unexecuted order: "
                        f"{reconciled.action} {reconciled.net_quantity} {reconciled.symbol}"
                    )
            except Empty:
                break

    # =========================================================================
    # Status Methods
    # =========================================================================

    def get_plugin_status(self, name: str) -> Optional[Dict[str, Any]]:
        """Get status for a specific plugin"""
        with self._lock:
            config = self._plugins.get(name)
            if not config:
                return None

            plugin = config.plugin

            return {
                "name": name,
                "version": plugin.VERSION,
                "state": plugin.state.value,
                "is_system_plugin": plugin.is_system_plugin,
                "enabled": config.enabled,
                "execution_mode": config.execution_mode.value,
                "bar_timeframe": config.bar_timeframe.value,
                "run_count": config.run_count,
                "error_count": config.error_count,
                "last_run": config.last_run.isoformat() if config.last_run else None,
                "last_error": config.last_error,
                "circuit_breaker": config.circuit_breaker.to_dict(),
                "parameters": dict(config.parameters),
                "subscribed_channels": plugin.subscribed_channels,
                "source_file": str(config.source_file) if config.source_file else None,
            }

    def get_status(self) -> Dict[str, Any]:
        """Get overall executive status"""
        with self._lock:
            plugin_status = {
                name: {
                    "state": config.plugin.state.value,
                    "enabled": config.enabled,
                    "run_count": config.run_count,
                    "error_count": config.error_count,
                    "circuit_breaker_state": config.circuit_breaker.state,
                }
                for name, config in self._plugins.items()
            }

            open_circuit_breakers = [
                name for name, config in self._plugins.items()
                if config.circuit_breaker.state == "open"
            ]

        health_status = {
            "executor_thread_alive": (
                self._executor_thread.is_alive()
                if self._executor_thread else False
            ),
            "health_thread_alive": (
                self._health_thread.is_alive()
                if self._health_thread else False
            ),
            "executor_restart_count": self._executor_restart_count,
            "max_executor_restarts": self._max_executor_restarts,
        }

        return {
            "running": self._running,
            "paused": self._paused,
            "order_mode": self.order_mode.value,
            "plugins": plugin_status,
            "stats": self._stats,
            "pending_orders": len(self._pending_orders),
            "queue_size": self._order_queue.qsize(),
            "health": health_status,
            "open_circuit_breakers": open_circuit_breakers,
            "message_bus_channels": len(self.message_bus.list_channels()),
        }

    def get_execution_history(
        self,
        plugin_name: Optional[str] = None,
        count: int = 100,
    ) -> List[ExecutionResult]:
        """Get execution history"""
        history = self._execution_history

        if plugin_name:
            history = [r for r in history if r.plugin_name == plugin_name]

        return history[-count:]

    def get_rate_limiter_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics"""
        return self._order_rate_limiter.stats

    # =========================================================================
    # Commission Tracking and Execution Logging
    # =========================================================================

    def _register_pending_execution(
        self,
        order_id: int,
        reconciled: ReconciledOrder,
    ):
        """
        Register an order execution for commission tracking.

        Called when an order is placed to track the execution details
        needed when the commission report arrives.

        Args:
            order_id: The IB order ID
            reconciled: The reconciled order details
        """
        with self._lock:
            self._pending_commissions[order_id] = {
                "symbol": reconciled.symbol,
                "action": reconciled.action,
                "net_quantity": reconciled.net_quantity,
                "contributing_signals": reconciled.contributing_signals,
                "created_at": datetime.now(),
            }

    def _handle_commission_report(
        self,
        exec_id: str,
        commission: float,
        realized_pnl: float,
    ):
        """
        Handle commission report from Portfolio.

        This is called when Portfolio receives a commissionReport callback
        from IB. We use this to create PluginExecutionLog entries with
        commission apportionment for multi-plugin orders.

        Args:
            exec_id: The execution ID
            commission: Commission amount
            realized_pnl: Realized P&L for closing trades
        """
        logger.debug(
            f"Commission report received: exec_id={exec_id}, "
            f"commission=${commission:.4f}, pnl=${realized_pnl:.2f}"
        )

        # Find the order associated with this execution
        order_id = self._exec_id_to_order.get(exec_id)
        if not order_id:
            # Try to find by checking pending orders
            # This is a fallback if exec_id wasn't pre-registered
            logger.debug(f"No order found for exec_id={exec_id}, commission logged without allocation")
            return

        self._process_commission_for_order(order_id, exec_id, commission, realized_pnl)

    def _process_commission_for_order(
        self,
        order_id: int,
        exec_id: str,
        commission: float,
        realized_pnl: float,
    ):
        """
        Process commission for an order and write execution logs.

        Apportions commission among contributing plugins based on their
        allocation percentages.

        Args:
            order_id: The IB order ID
            exec_id: The execution ID
            commission: Total commission
            realized_pnl: Total realized P&L
        """
        with self._lock:
            pending = self._pending_commissions.get(order_id)
            if not pending:
                logger.debug(f"No pending commission info for order {order_id}")
                return

            # Get allocation percentages from reconciler
            allocation_pcts = self._reconciler.get_allocation_percentages(order_id)
            is_combined = len(allocation_pcts) > 1

            # Get order details
            symbol = pending["symbol"]
            action = pending["action"]
            total_qty = pending["net_quantity"]
            contributing_signals = pending["contributing_signals"]

            # Get fill price from portfolio if available
            fill_price = 0.0
            if self.portfolio and hasattr(self.portfolio, "_orders"):
                order_record = self.portfolio._orders.get(order_id)
                if order_record:
                    fill_price = order_record.avg_fill_price

            # Create execution log for each contributing plugin
            for ps in contributing_signals:
                plugin_name = ps.algorithm_name
                alloc_pct = allocation_pcts.get(plugin_name, 0.0)

                # If no allocation percentages, fall back to even split
                if alloc_pct == 0.0 and len(contributing_signals) > 0:
                    alloc_pct = 1.0 / len(contributing_signals)

                # Apportion commission and P&L
                plugin_commission = commission * alloc_pct
                plugin_pnl = realized_pnl * alloc_pct
                plugin_qty = int(total_qty * alloc_pct)

                # Get plugin's position info if available
                pos_before = 0
                pos_after = 0
                avg_cost_before = 0.0
                avg_cost_after = 0.0

                plugin_config = self._plugins.get(plugin_name)
                if plugin_config and plugin_config.plugin.holdings:
                    holdings = plugin_config.plugin.holdings
                    position = holdings.get_position(symbol)
                    if position:
                        # These would be the values after the trade
                        pos_after = position.quantity
                        avg_cost_after = position.cost_basis / position.quantity if position.quantity else 0.0

                # Create and write log entry
                log_entry = PluginExecutionLog(
                    timestamp=datetime.now(),
                    plugin_name=plugin_name,
                    order_id=order_id,
                    exec_id=exec_id,
                    symbol=symbol,
                    action=action,
                    quantity=plugin_qty,
                    fill_price=fill_price,
                    commission=plugin_commission,
                    fees=0.0,  # IB includes fees in commission
                    realized_pnl=plugin_pnl,
                    is_combined_order=is_combined,
                    allocation_pct=alloc_pct,
                    total_order_quantity=total_qty,
                    position_before=pos_before,
                    position_after=pos_after,
                    avg_cost_before=avg_cost_before,
                    avg_cost_after=avg_cost_after,
                )

                # Write to log file
                if self._execution_log_writer.write(log_entry):
                    logger.debug(
                        f"Logged execution for {plugin_name}: {action} {plugin_qty} {symbol} "
                        f"@ ${fill_price:.2f}, commission=${plugin_commission:.4f}"
                    )
                else:
                    logger.error(f"Failed to write execution log for {plugin_name}")

    def register_execution_for_commission(
        self,
        order_id: int,
        exec_id: str,
    ):
        """
        Register an execution ID for commission tracking.

        Call this when execDetails callback is received to link
        the exec_id to the order_id for later commission processing.

        Args:
            order_id: The IB order ID
            exec_id: The execution ID from IB
        """
        with self._lock:
            self._exec_id_to_order[exec_id] = order_id

    def get_execution_logs(
        self,
        plugin_name: Optional[str] = None,
    ) -> List[PluginExecutionLog]:
        """
        Get execution logs from the log file.

        Args:
            plugin_name: Filter by plugin name (None = all plugins)

        Returns:
            List of PluginExecutionLog entries
        """
        from .plugin_execution_log import ExecutionLogReader

        reader = ExecutionLogReader()
        if plugin_name:
            return reader.read_plugin(plugin_name)
        return reader.read_all()

    # =========================================================================
    # Manual Trade Execution with Plugin Attribution
    # =========================================================================

    def execute_manual_trade(
        self,
        plugin_name: str,
        symbol: str,
        action: str,
        quantity: int,
        reason: str = "manual_trade",
        dry_run: bool = True,
    ) -> Tuple[bool, Optional[int], str]:
        """
        Execute a manual trade attributed to a specific plugin.

        Routes the trade through the OrderReconciler to maintain full
        plugin attribution for commission tracking and P&L reporting.

        Args:
            plugin_name: Name of the plugin to attribute the trade to
            symbol: Trading symbol (e.g., "SPY")
            action: Trade action ("BUY" or "SELL")
            quantity: Number of shares
            reason: Reason for the trade (logged with signal)
            dry_run: If True, simulate only; if False, execute

        Returns:
            Tuple of (success, order_id, message)
            - success: True if trade was executed/simulated successfully
            - order_id: IB order ID if executed, None for dry run
            - message: Status message
        """
        action = action.upper()

        # Validate action
        if action not in ("BUY", "SELL"):
            return False, None, f"Invalid action: {action}. Must be BUY or SELL."

        # Validate plugin exists
        with self._lock:
            if plugin_name not in self._plugins:
                available = list(self._plugins.keys())
                return False, None, f"Plugin '{plugin_name}' not found. Available: {available}"

        # Validate quantity
        if quantity <= 0:
            return False, None, f"Invalid quantity: {quantity}. Must be positive."

        # Get contract for symbol
        contract = self._get_contract(plugin_name, symbol)
        if not contract:
            # Try to build a basic stock contract
            contract = self._build_stock_contract(symbol)
            if not contract:
                return False, None, f"Cannot get contract for symbol: {symbol}"

        # Create trade signal with attribution
        signal = TradeSignal(
            symbol=symbol,
            action=action,
            quantity=quantity,
            reason=f"[MANUAL] {reason}",
            confidence=1.0,
            urgency="Normal",
        )

        # Get current price for value estimate
        price_estimate = 0.0
        pos = self.portfolio.get_position(symbol) if self.portfolio else None
        if pos:
            price_estimate = pos.current_price
        value_estimate = quantity * price_estimate

        if dry_run:
            # Dry run - just return what would happen
            message = (
                f"[DRY RUN] Would execute for plugin '{plugin_name}':\n"
                f"  Action: {action} {quantity} {symbol}\n"
                f"  Estimated Value: ${value_estimate:,.2f}\n"
                f"  Use --confirm to execute"
            )
            return True, None, message

        # Add signal to reconciler with plugin attribution
        self._reconciler.add_signal(plugin_name, signal, contract)

        # Execute immediately (bypass normal batch window)
        reconciled_orders = self._reconciler.reconcile(symbol)

        if not reconciled_orders:
            return False, None, "No orders generated after reconciliation"

        # Execute the reconciled order
        order_id = None
        for reconciled in reconciled_orders:
            if self.order_mode == OrderExecutionMode.DRY_RUN:
                # System-level dry run mode
                self._log_dry_run_reconciled(reconciled)
                message = (
                    f"[DRY RUN - System Mode] Trade for plugin '{plugin_name}':\n"
                    f"  Action: {action} {quantity} {symbol}\n"
                    f"  System is in dry-run mode"
                )
                return True, None, message

            # Execute the order
            try:
                ib_order = self._reconciler.create_ib_order(reconciled)
                order_id = self.portfolio.place_order(
                    reconciled.contract,
                    reconciled.action,
                    reconciled.net_quantity,
                    order_type="MKT",
                )

                if order_id:
                    self._reconciler.register_execution(order_id, reconciled)
                    self._register_pending_execution(order_id, reconciled)
                    self._stats["total_orders"] += 1

                    # Log execution for each contributing plugin
                    for ps in reconciled.contributing_signals:
                        result = ExecutionResult(
                            plugin_name=ps.algorithm_name,
                            symbol=reconciled.symbol,
                            action=reconciled.action,
                            quantity=ps.signal.quantity,
                            order_id=order_id,
                            success=True,
                        )
                        self._add_to_history(result)

                        if self.on_execution:
                            try:
                                self.on_execution(result)
                            except Exception as e:
                                logger.error(f"Error in execution callback: {e}")

                    message = (
                        f"[EXECUTED] Trade for plugin '{plugin_name}':\n"
                        f"  Order ID: {order_id}\n"
                        f"  Action: {action} {quantity} {symbol}\n"
                        f"  Status: Submitted"
                    )
                    return True, order_id, message
                else:
                    return False, None, f"Failed to place order for {symbol}"

            except Exception as e:
                logger.error(f"Error executing manual trade: {e}")
                return False, None, f"Execution error: {e}"

        return False, None, "No orders executed"

    def _build_stock_contract(self, symbol: str) -> Optional[Contract]:
        """Build a basic stock contract for a symbol"""
        try:
            contract = Contract()
            contract.symbol = symbol
            contract.secType = "STK"
            contract.currency = "USD"
            contract.exchange = "SMART"
            return contract
        except Exception as e:
            logger.error(f"Failed to build contract for {symbol}: {e}")
            return None
