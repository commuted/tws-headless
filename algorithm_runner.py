"""
algorithm_runner.py - Continuous algorithm execution engine

Provides an engine that:
- Runs multiple algorithms continuously
- Feeds real-time data from DataFeed to algorithms
- Executes trades based on algorithm signals
- Handles algorithm lifecycle and error recovery
- Supports multiple execution modes (every bar, scheduled, manual)
"""

import logging
from threading import Thread, Event, Lock, RLock
from typing import Optional, Callable, Dict, List, Set, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from queue import Queue, Empty
import time
import traceback

from ibapi.contract import Contract
from ibapi.order import Order

from .algorithms.base import AlgorithmBase, AlgorithmResult, TradeSignal
from .data_feed import DataFeed, DataType, TickData
from .models import Bar
from .order_reconciler import OrderReconciler, ReconciledOrder, ReconciliationMode
from .rate_limiter import OrderRateLimiter

logger = logging.getLogger(__name__)


class ExecutionMode(Enum):
    """How algorithms are triggered"""
    ON_BAR = "on_bar"  # Execute on every new bar
    ON_TICK = "on_tick"  # Execute on every tick
    SCHEDULED = "scheduled"  # Execute on a schedule
    MANUAL = "manual"  # Manual trigger only


class OrderExecutionMode(Enum):
    """How algorithm orders are executed"""
    IMMEDIATE = "immediate"  # Execute immediately
    QUEUED = "queued"  # Queue for execution
    DRY_RUN = "dry_run"  # Simulate only


@dataclass
class CircuitBreaker:
    """
    Circuit breaker for algorithm fault tolerance.

    Automatically disables algorithms that fail repeatedly,
    then auto-resets after a cooldown period.

    States:
        CLOSED: Normal operation, algorithm runs
        OPEN: Tripped due to failures, algorithm blocked
        HALF_OPEN: Testing if algorithm recovered
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
            logger.info("Circuit breaker closed (algorithm recovered)")

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
class AlgorithmConfig:
    """Configuration for a registered algorithm"""
    algorithm: AlgorithmBase
    execution_mode: ExecutionMode = ExecutionMode.ON_BAR
    bar_timeframe: DataType = DataType.BAR_1MIN
    enabled: bool = True
    paused: bool = False  # Per-algorithm pause state
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


@dataclass
class PendingOrder:
    """An order pending execution"""
    algorithm_name: str
    signal: TradeSignal
    contract: Contract
    order: Order
    created_at: datetime = field(default_factory=datetime.now)
    status: str = "pending"


@dataclass
class ExecutionResult:
    """Result of order execution"""
    algorithm_name: str
    symbol: str
    action: str
    quantity: int
    order_id: Optional[int] = None
    success: bool = True
    error: Optional[str] = None
    executed_at: datetime = field(default_factory=datetime.now)


class AlgorithmRunner:
    """
    Continuous algorithm execution engine.

    Manages the execution of trading algorithms, feeding them market data
    and executing their trade signals.

    Usage:
        from portfolio import Portfolio
        from data_feed import DataFeed
        from algorithms import MyAlgorithm

        portfolio = Portfolio()
        portfolio.connect()
        portfolio.load()

        feed = DataFeed(portfolio)
        runner = AlgorithmRunner(portfolio, feed)

        # Register algorithm
        algo = MyAlgorithm()
        algo.load()
        runner.register_algorithm(algo)

        # Subscribe to instruments
        for inst in algo.instruments:
            feed.subscribe(inst.symbol, inst.to_contract())

        # Start
        feed.start()
        runner.start()

        # ... runs continuously ...

        runner.stop()
        feed.stop()
    """

    def __init__(
        self,
        portfolio,
        data_feed: DataFeed,
        order_mode: OrderExecutionMode = OrderExecutionMode.DRY_RUN,
        reconciliation_mode: ReconciliationMode = ReconciliationMode.NET,
        circuit_breaker_failures: int = 5,
        circuit_breaker_reset_seconds: float = 300.0,
        health_check_interval: float = 5.0,
        order_rate_limit: float = 10.0,
        order_burst_size: int = 10,
    ):
        """
        Initialize algorithm runner.

        Args:
            portfolio: Portfolio instance for order execution
            data_feed: DataFeed instance for market data
            order_mode: How orders should be executed
            reconciliation_mode: How to reconcile orders from multiple algorithms
            circuit_breaker_failures: Consecutive failures before disabling algorithm
            circuit_breaker_reset_seconds: Seconds before auto-resetting circuit breaker
            health_check_interval: Seconds between thread health checks
            order_rate_limit: Maximum orders per second (IB compliance)
            order_burst_size: Maximum burst capacity for orders
        """
        self.portfolio = portfolio
        self.data_feed = data_feed
        self.order_mode = order_mode

        # Circuit breaker defaults for new algorithms
        self._default_cb_failures = circuit_breaker_failures
        self._default_cb_reset_seconds = circuit_breaker_reset_seconds

        # Health monitoring config
        self._health_check_interval = health_check_interval

        # Order rate limiting for IB compliance
        self._order_rate_limiter = OrderRateLimiter(
            orders_per_second=order_rate_limit,
            burst_size=order_burst_size,
        )

        # Order reconciler for netting orders from multiple algorithms
        self._reconciler = OrderReconciler(mode=reconciliation_mode)

        # State
        self._running = False
        self._paused = False
        self._shutdown_event = Event()
        self._lock = RLock()

        # Registered algorithms
        self._algorithms: Dict[str, AlgorithmConfig] = {}

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
        self._max_executor_restarts = 10  # Max restarts before giving up

        # Callbacks
        self.on_signal: Optional[Callable[[str, TradeSignal], None]] = None
        self.on_execution: Optional[Callable[[ExecutionResult], None]] = None
        self.on_reconciled: Optional[Callable[[ReconciledOrder], None]] = None
        self.on_error: Optional[Callable[[str, Exception], None]] = None
        self.on_circuit_breaker_trip: Optional[Callable[[str], None]] = None  # New: algorithm name

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
        }

    @property
    def is_running(self) -> bool:
        """Check if runner is running"""
        return self._running

    @property
    def is_paused(self) -> bool:
        """Check if runner is paused"""
        return self._paused

    @property
    def algorithms(self) -> List[str]:
        """Get list of registered algorithm names"""
        with self._lock:
            return list(self._algorithms.keys())

    @property
    def stats(self) -> Dict[str, Any]:
        """Get runner statistics"""
        return self._stats.copy()

    def register_algorithm(
        self,
        algorithm: AlgorithmBase,
        execution_mode: ExecutionMode = ExecutionMode.ON_BAR,
        bar_timeframe: DataType = DataType.BAR_1MIN,
        enabled: bool = True,
        circuit_breaker_failures: Optional[int] = None,
        circuit_breaker_reset_seconds: Optional[float] = None,
    ) -> bool:
        """
        Register an algorithm with the runner.

        Args:
            algorithm: Algorithm instance
            execution_mode: When to trigger the algorithm
            bar_timeframe: Which bar timeframe triggers ON_BAR mode
            enabled: Whether the algorithm is enabled
            circuit_breaker_failures: Custom failure threshold (uses runner default if None)
            circuit_breaker_reset_seconds: Custom reset time (uses runner default if None)

        Returns:
            True if registered successfully
        """
        if not algorithm.is_loaded:
            logger.warning(f"Algorithm '{algorithm.name}' not loaded, loading now...")
            if not algorithm.load():
                logger.error(f"Failed to load algorithm '{algorithm.name}'")
                return False

        with self._lock:
            if algorithm.name in self._algorithms:
                logger.warning(f"Algorithm '{algorithm.name}' already registered")
                return False

            # Create circuit breaker with custom or default settings
            cb = CircuitBreaker(
                max_failures=circuit_breaker_failures or self._default_cb_failures,
                reset_after_seconds=circuit_breaker_reset_seconds or self._default_cb_reset_seconds,
            )

            config = AlgorithmConfig(
                algorithm=algorithm,
                execution_mode=execution_mode,
                bar_timeframe=bar_timeframe,
                enabled=enabled,
                circuit_breaker=cb,
            )
            self._algorithms[algorithm.name] = config

        logger.info(
            f"Registered algorithm '{algorithm.name}' "
            f"(mode={execution_mode.value}, timeframe={bar_timeframe.value}, "
            f"circuit_breaker={cb.max_failures} failures)"
        )
        return True

    def unregister_algorithm(self, name: str):
        """
        Unregister an algorithm.

        Args:
            name: Algorithm name to unregister
        """
        with self._lock:
            if name in self._algorithms:
                del self._algorithms[name]
                logger.info(f"Unregistered algorithm '{name}'")

    def enable_algorithm(self, name: str, enabled: bool = True):
        """Enable or disable an algorithm"""
        with self._lock:
            if name in self._algorithms:
                self._algorithms[name].enabled = enabled
                logger.info(f"Algorithm '{name}' {'enabled' if enabled else 'disabled'}")

    def pause_algorithm(self, name: str) -> bool:
        """
        Pause a specific algorithm.

        Unlike disable, pause is temporary and preserves state.

        Args:
            name: Algorithm name

        Returns:
            True if paused successfully
        """
        with self._lock:
            if name not in self._algorithms:
                logger.warning(f"Algorithm '{name}' not found")
                return False

            self._algorithms[name].paused = True
            logger.info(f"Algorithm '{name}' paused")
            return True

    def resume_algorithm(self, name: str) -> bool:
        """
        Resume a paused algorithm.

        Args:
            name: Algorithm name

        Returns:
            True if resumed successfully
        """
        with self._lock:
            if name not in self._algorithms:
                logger.warning(f"Algorithm '{name}' not found")
                return False

            self._algorithms[name].paused = False
            logger.info(f"Algorithm '{name}' resumed")
            return True

    def set_algorithm_parameter(
        self,
        name: str,
        key: str,
        value: Any,
    ) -> bool:
        """
        Set a runtime parameter for an algorithm.

        Parameters are stored in the config and can be accessed
        by the algorithm via get_parameters().

        Args:
            name: Algorithm name
            key: Parameter key
            value: Parameter value

        Returns:
            True if set successfully
        """
        with self._lock:
            if name not in self._algorithms:
                logger.warning(f"Algorithm '{name}' not found")
                return False

            config = self._algorithms[name]
            config.parameters[key] = value

            # Also try to set on the algorithm itself if it supports it
            try:
                if hasattr(config.algorithm, 'set_parameter'):
                    config.algorithm.set_parameter(key, value)
            except Exception as e:
                logger.warning(f"Algorithm '{name}' set_parameter failed: {e}")

            logger.info(f"Algorithm '{name}' parameter '{key}' set to '{value}'")
            return True

    def get_algorithm_parameters(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get runtime parameters for an algorithm.

        Args:
            name: Algorithm name

        Returns:
            Parameters dictionary or None if not found
        """
        with self._lock:
            if name not in self._algorithms:
                return None

            config = self._algorithms[name]

            # Merge config parameters with algorithm's own parameters
            params = dict(config.parameters)

            try:
                if hasattr(config.algorithm, 'get_parameters'):
                    algo_params = config.algorithm.get_parameters()
                    if algo_params:
                        # Algorithm params are base, config overrides
                        algo_params.update(params)
                        params = algo_params
            except Exception as e:
                logger.warning(f"Algorithm '{name}' get_parameters failed: {e}")

            return params

    def reset_circuit_breaker(self, name: str) -> bool:
        """
        Manually reset an algorithm's circuit breaker.

        Args:
            name: Algorithm name

        Returns:
            True if reset successfully
        """
        with self._lock:
            if name not in self._algorithms:
                logger.warning(f"Algorithm '{name}' not found")
                return False

            self._algorithms[name].circuit_breaker.reset()
            logger.info(f"Circuit breaker reset for algorithm '{name}'")
            return True

    def get_circuit_breaker_status(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get circuit breaker status for an algorithm.

        Args:
            name: Algorithm name

        Returns:
            Circuit breaker status dict or None
        """
        with self._lock:
            if name not in self._algorithms:
                return None
            return self._algorithms[name].circuit_breaker.to_dict()

    def get_all_circuit_breakers(self) -> Dict[str, Dict[str, Any]]:
        """
        Get circuit breaker status for all algorithms.

        Returns:
            Dict mapping algorithm name to circuit breaker status
        """
        with self._lock:
            return {
                name: config.circuit_breaker.to_dict()
                for name, config in self._algorithms.items()
            }

    def start(self) -> bool:
        """
        Start the algorithm runner.

        Returns:
            True if started successfully
        """
        if self._running:
            logger.warning("Runner already running")
            return True

        if not self.data_feed.is_running:
            logger.error("Data feed not running - start it first")
            return False

        self._running = True
        self._paused = False
        self._shutdown_event.clear()
        self._executor_restart_count = 0
        self._stats["started_at"] = datetime.now().isoformat()

        # Set up data feed callbacks
        self._setup_data_callbacks()

        # Start executor thread
        self._start_executor_thread()

        # Start health monitoring thread
        self._health_thread = Thread(
            target=self._health_monitor_loop,
            daemon=True,
            name="AlgorithmRunner-Health"
        )
        self._health_thread.start()

        logger.info(f"Algorithm runner started with {len(self._algorithms)} algorithms")
        return True

    def _start_executor_thread(self):
        """Start or restart the executor thread"""
        self._executor_thread = Thread(
            target=self._executor_loop_wrapper,
            daemon=True,
            name="AlgorithmRunner-Executor"
        )
        self._executor_thread.start()

    def stop(self):
        """Stop the algorithm runner"""
        if not self._running:
            return

        logger.info("Stopping algorithm runner...")
        self._running = False
        self._shutdown_event.set()

        # Wait for threads
        if self._executor_thread and self._executor_thread.is_alive():
            self._executor_thread.join(timeout=5.0)

        if self._health_thread and self._health_thread.is_alive():
            self._health_thread.join(timeout=5.0)

        # Process any remaining orders
        self._drain_order_queue()

        logger.info("Algorithm runner stopped")

    def pause(self):
        """Pause algorithm execution (data still flows)"""
        self._paused = True
        logger.info("Algorithm runner paused")

    def resume(self):
        """Resume algorithm execution"""
        self._paused = False
        logger.info("Algorithm runner resumed")

    def trigger_algorithm(self, name: str) -> Optional[AlgorithmResult]:
        """
        Manually trigger an algorithm run.

        Args:
            name: Algorithm name

        Returns:
            AlgorithmResult or None if not found
        """
        with self._lock:
            config = self._algorithms.get(name)
            if not config:
                logger.error(f"Algorithm '{name}' not found")
                return None

        return self._run_algorithm(config)

    def _setup_data_callbacks(self):
        """Set up callbacks on data feed"""
        # Store original callbacks
        self._original_on_tick = self.data_feed.on_tick
        self._original_on_bar = self.data_feed.on_bar

        # Set our handlers (chains to original)
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
        """Handle incoming tick - trigger ON_TICK algorithms"""
        with self._lock:
            for name, config in self._algorithms.items():
                if (config.enabled and
                    config.execution_mode == ExecutionMode.ON_TICK):
                    self._schedule_run(config)

    def _on_bar(self, symbol: str, bar: Bar, data_type: DataType):
        """Handle incoming bar - trigger ON_BAR algorithms"""
        with self._lock:
            for name, config in self._algorithms.items():
                if (config.enabled and
                    config.execution_mode == ExecutionMode.ON_BAR and
                    config.bar_timeframe == data_type):
                    self._schedule_run(config)

    def _schedule_run(self, config: AlgorithmConfig):
        """Schedule an algorithm run (respects pause, cooldown and circuit breaker)"""
        now = datetime.now()

        # Check if algorithm is paused
        if config.paused:
            return  # Algorithm is paused

        # Check circuit breaker
        if not config.circuit_breaker.should_allow():
            return  # Circuit breaker is open

        # Check cooldown
        if config.cooldown_seconds > 0 and config.last_run:
            elapsed = (now - config.last_run).total_seconds()
            if elapsed < config.cooldown_seconds:
                return  # Still in cooldown

        # Run in executor thread to avoid blocking data callbacks
        try:
            self._order_queue.put(("RUN", config.algorithm.name), block=False)
        except Exception as e:
            logger.error(f"Failed to schedule algorithm run: {e}")

    def _run_algorithm(self, config: AlgorithmConfig) -> AlgorithmResult:
        """
        Run a single algorithm.

        Args:
            config: Algorithm configuration

        Returns:
            AlgorithmResult
        """
        algorithm = config.algorithm
        name = algorithm.name

        # Check circuit breaker before running
        if not config.circuit_breaker.should_allow():
            logger.debug(f"Algorithm '{name}' blocked by circuit breaker")
            return AlgorithmResult(
                algorithm_name=name,
                timestamp=datetime.now(),
                success=False,
                error="Circuit breaker open",
            )

        try:
            # Prepare market data from feed
            market_data = self._prepare_market_data(algorithm)

            # Run algorithm
            result = algorithm.run(market_data=market_data)

            config.last_run = datetime.now()
            config.run_count += 1
            self._stats["total_runs"] += 1

            if result.success:
                # Record success with circuit breaker
                config.circuit_breaker.record_success()

                # Process signals
                signals = result.actionable_signals[:config.max_signals_per_run]
                self._stats["total_signals"] += len(signals)

                for signal in signals:
                    self._process_signal(name, signal)

                    # Invoke callback
                    if self.on_signal:
                        try:
                            self.on_signal(name, signal)
                        except Exception as e:
                            logger.error(f"Error in signal callback: {e}")

                # Reconcile and execute after processing all signals from this algorithm
                # This allows signals from the same algorithm to be netted
                self.reconcile_and_execute()

            else:
                config.error_count += 1
                config.last_error = result.error
                self._stats["total_errors"] += 1
                logger.error(f"Algorithm '{name}' failed: {result.error}")

                # Record failure with circuit breaker
                if config.circuit_breaker.record_failure():
                    self._stats["circuit_breaker_trips"] += 1
                    logger.warning(f"Circuit breaker tripped for algorithm '{name}'")
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
            logger.error(f"Error running algorithm '{name}': {e}")
            logger.debug(traceback.format_exc())

            # Record failure with circuit breaker
            if config.circuit_breaker.record_failure():
                self._stats["circuit_breaker_trips"] += 1
                logger.warning(f"Circuit breaker tripped for algorithm '{name}'")
                if self.on_circuit_breaker_trip:
                    try:
                        self.on_circuit_breaker_trip(name)
                    except Exception as e2:
                        logger.error(f"Error in circuit breaker callback: {e2}")

            if self.on_error:
                try:
                    self.on_error(name, e)
                except:
                    pass

            return AlgorithmResult(
                algorithm_name=name,
                timestamp=datetime.now(),
                success=False,
                error=str(e),
            )

    def _prepare_market_data(
        self,
        algorithm: AlgorithmBase,
    ) -> Dict[str, List[Dict]]:
        """
        Prepare market data for an algorithm from the data feed.

        Args:
            algorithm: Algorithm to prepare data for

        Returns:
            Dict mapping symbol to list of bar dictionaries
        """
        market_data = {}

        for instrument in algorithm.enabled_instruments:
            symbol = instrument.symbol

            # Get bars from feed (prefer 1-minute bars)
            bars = self.data_feed.get_bars(
                symbol,
                DataType.BAR_1MIN,
                count=algorithm.required_bars * 2  # Get extra for safety
            )

            if not bars:
                # Fall back to 5-second bars
                bars = self.data_feed.get_bars(
                    symbol,
                    DataType.BAR_5SEC,
                    count=algorithm.required_bars * 12  # 12 5-sec bars per minute
                )

            # Convert to dict format expected by algorithms
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

    def _process_signal(self, algorithm_name: str, signal: TradeSignal):
        """
        Process a trade signal from an algorithm.

        Signals are collected by the reconciler and netted before execution.

        Args:
            algorithm_name: Name of the algorithm
            signal: The trade signal
        """
        if not signal.is_actionable:
            return

        logger.info(
            f"[{algorithm_name}] Signal: {signal.action} {signal.quantity} {signal.symbol} "
            f"(confidence={signal.confidence:.2f}, reason={signal.reason})"
        )

        # Get contract for the symbol
        contract = self._get_contract(algorithm_name, signal.symbol)
        if not contract:
            logger.error(f"No contract found for {signal.symbol}")
            return

        # Add to reconciler instead of immediate execution
        self._reconciler.add_signal(algorithm_name, signal, contract)

    def reconcile_and_execute(self):
        """
        Reconcile pending signals and execute netted orders.

        Call this after all algorithms have run for a given bar/tick.
        """
        # Get reconciled orders
        reconciled_orders = self._reconciler.reconcile()

        # Update stats
        reconciler_stats = self._reconciler.stats
        self._stats["shares_saved_by_netting"] = reconciler_stats.get("shares_saved", 0)

        for reconciled in reconciled_orders:
            logger.info(
                f"Reconciled order: {reconciled.action} {reconciled.net_quantity} {reconciled.symbol} "
                f"(from {len(reconciled.contributing_signals)} signals)"
            )

            # Invoke callback
            if self.on_reconciled:
                try:
                    self.on_reconciled(reconciled)
                except Exception as e:
                    logger.error(f"Error in on_reconciled callback: {e}")

            # Execute the reconciled order
            if self.order_mode == OrderExecutionMode.DRY_RUN:
                self._log_dry_run_reconciled(reconciled)
            else:
                try:
                    self._order_queue.put(("RECONCILED", reconciled), block=False)
                except Exception as e:
                    logger.error(f"Failed to queue reconciled order: {e}")

    def _log_dry_run_reconciled(self, reconciled: ReconciledOrder):
        """Log a dry run execution of a reconciled order"""
        # Create execution results for each contributing algorithm
        for ps in reconciled.contributing_signals:
            result = ExecutionResult(
                algorithm_name=ps.algorithm_name,
                symbol=reconciled.symbol,
                action=reconciled.action,
                quantity=ps.signal.quantity,  # Original signal quantity
                success=True,
            )
            self._add_to_history(result)

            if self.on_execution:
                try:
                    self.on_execution(result)
                except Exception as e:
                    logger.error(f"Error in execution callback: {e}")

        # Log the net order
        algo_names = [ps.algorithm_name for ps in reconciled.contributing_signals]
        logger.info(
            f"[DRY RUN] Net order: {reconciled.action} {reconciled.net_quantity} {reconciled.symbol} "
            f"(from: {', '.join(algo_names)})"
        )

    def _get_contract(
        self,
        algorithm_name: str,
        symbol: str,
    ) -> Optional[Contract]:
        """Get contract for a symbol from algorithm instruments"""
        with self._lock:
            config = self._algorithms.get(algorithm_name)
            if not config:
                return None

            instrument = config.algorithm.get_instrument(symbol)
            if instrument:
                return instrument.to_contract()

            # Try to get from portfolio positions
            pos = self.portfolio.get_position(symbol)
            if pos and pos.contract:
                return pos.contract

        return None

    def _create_order(self, signal: TradeSignal) -> Order:
        """Create an IB Order from a signal"""
        order = Order()
        order.action = signal.action
        order.totalQuantity = signal.quantity
        order.orderType = "MKT"  # Default to market orders
        order.tif = "DAY"

        # Use adaptive algo for better execution
        if signal.urgency == "Patient":
            order.algoStrategy = "Adaptive"
            order.algoParams = [{"tag": "adaptivePriority", "value": "Patient"}]
        elif signal.urgency == "Urgent":
            order.algoStrategy = "Adaptive"
            order.algoParams = [{"tag": "adaptivePriority", "value": "Urgent"}]

        return order

    def _log_dry_run(self, pending: PendingOrder):
        """Log a dry run execution"""
        result = ExecutionResult(
            algorithm_name=pending.algorithm_name,
            symbol=pending.signal.symbol,
            action=pending.signal.action,
            quantity=pending.signal.quantity,
            success=True,
        )

        self._add_to_history(result)

        logger.info(
            f"[DRY RUN] {pending.algorithm_name}: "
            f"{pending.signal.action} {pending.signal.quantity} {pending.signal.symbol}"
        )

        if self.on_execution:
            try:
                self.on_execution(result)
            except Exception as e:
                logger.error(f"Error in execution callback: {e}")

    def _executor_loop_wrapper(self):
        """
        Wrapper around executor loop that catches fatal exceptions.

        If the executor loop crashes, this wrapper logs the error
        and allows the health monitor to restart the thread.
        """
        try:
            self._executor_loop()
        except Exception as e:
            logger.critical(f"Executor thread crashed: {e}")
            logger.critical(traceback.format_exc())
            # Don't re-raise - let health monitor handle restart

    def _executor_loop(self):
        """Background loop for order execution"""
        logger.debug("Executor thread started")

        while not self._shutdown_event.is_set():
            try:
                # Get next item from queue
                item = self._order_queue.get(timeout=0.1)

                if item[0] == "RUN":
                    # Run algorithm
                    algo_name = item[1]
                    with self._lock:
                        config = self._algorithms.get(algo_name)
                    if config:
                        self._run_algorithm(config)

                elif item[0] == "ORDER":
                    # Execute order (legacy, single algorithm)
                    pending = item[1]
                    if self._acquire_rate_limit_token(timeout=5.0):
                        self._execute_order(pending)
                    else:
                        logger.warning(
                            f"Order rate limit exceeded for {pending.signal.symbol}, "
                            "order dropped"
                        )

                elif item[0] == "RECONCILED":
                    # Execute reconciled/netted order
                    reconciled = item[1]
                    if self._acquire_rate_limit_token(timeout=5.0):
                        self._execute_reconciled_order(reconciled)
                    else:
                        logger.warning(
                            f"Order rate limit exceeded for {reconciled.symbol}, "
                            "order dropped"
                        )

            except Empty:
                continue
            except Exception as e:
                logger.error(f"Executor error: {e}")
                logger.debug(traceback.format_exc())
                # Continue running - don't let one error kill the thread

        logger.debug("Executor thread stopped")

    def _acquire_rate_limit_token(self, timeout: float = 5.0) -> bool:
        """
        Acquire a rate limit token for order execution.

        Updates statistics based on whether we had to wait or were rejected.

        Args:
            timeout: Maximum time to wait for token

        Returns:
            True if token acquired, False if rate limit exceeded
        """
        prev_delayed = self._order_rate_limiter._limiter._stats.requests_delayed

        result = self._order_rate_limiter.acquire(blocking=True, timeout=timeout)

        # Check if we had to wait (was rate limited)
        curr_delayed = self._order_rate_limiter._limiter._stats.requests_delayed
        if curr_delayed > prev_delayed:
            self._stats["rate_limit_delays"] += 1

        if not result:
            self._stats["rate_limit_rejects"] += 1

        return result

    def get_rate_limiter_stats(self) -> Dict[str, Any]:
        """
        Get rate limiter statistics.

        Returns:
            Rate limiter stats dictionary
        """
        return self._order_rate_limiter.stats

    def _health_monitor_loop(self):
        """
        Background loop that monitors thread health and restarts dead threads.

        Also monitors circuit breakers and logs status periodically.
        """
        logger.debug("Health monitor thread started")

        while not self._shutdown_event.is_set():
            try:
                # Wait for check interval
                if self._shutdown_event.wait(self._health_check_interval):
                    break  # Shutdown requested

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
                                f"Executor thread died and max restarts "
                                f"({self._max_executor_restarts}) exceeded - giving up"
                            )
                            # Could trigger shutdown here, but let's just log

                # Check circuit breakers and log any that are open
                with self._lock:
                    for name, config in self._algorithms.items():
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

    def _execute_reconciled_order(self, reconciled: ReconciledOrder):
        """Execute a reconciled order"""
        try:
            # Create IB order
            order = self._reconciler.create_ib_order(reconciled)

            # Place order through portfolio
            order_id = self.portfolio.place_order(
                reconciled.contract,
                reconciled.action,
                reconciled.net_quantity,
                order_type="MKT",
            )

            if order_id:
                # Register for allocation tracking
                self._reconciler.register_execution(order_id, reconciled)
                self._stats["total_orders"] += 1

                # Create execution results for each contributing algorithm
                for ps in reconciled.contributing_signals:
                    result = ExecutionResult(
                        algorithm_name=ps.algorithm_name,
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

                algo_names = [ps.algorithm_name for ps in reconciled.contributing_signals]
                logger.info(
                    f"[EXECUTED] Net order: {reconciled.action} {reconciled.net_quantity} "
                    f"{reconciled.symbol} (order_id={order_id}, from: {', '.join(algo_names)})"
                )
            else:
                logger.error(f"Failed to place reconciled order for {reconciled.symbol}")
                for ps in reconciled.contributing_signals:
                    result = ExecutionResult(
                        algorithm_name=ps.algorithm_name,
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
                    algorithm_name=ps.algorithm_name,
                    symbol=reconciled.symbol,
                    action=reconciled.action,
                    quantity=ps.signal.quantity,
                    success=False,
                    error=str(e),
                )
                self._add_to_history(result)

    def _execute_order(self, pending: PendingOrder):
        """Execute a pending order"""
        if self.order_mode == OrderExecutionMode.DRY_RUN:
            self._log_dry_run(pending)
            return

        try:
            # Place order through portfolio
            order_id = self.portfolio.place_order(
                pending.contract,
                pending.signal.action,
                pending.signal.quantity,
                order_type="MKT",
            )

            if order_id:
                pending.status = "submitted"
                self._pending_orders[order_id] = pending
                self._stats["total_orders"] += 1

                result = ExecutionResult(
                    algorithm_name=pending.algorithm_name,
                    symbol=pending.signal.symbol,
                    action=pending.signal.action,
                    quantity=pending.signal.quantity,
                    order_id=order_id,
                    success=True,
                )

                logger.info(
                    f"[EXECUTED] {pending.algorithm_name}: "
                    f"{pending.signal.action} {pending.signal.quantity} "
                    f"{pending.signal.symbol} (order_id={order_id})"
                )
            else:
                result = ExecutionResult(
                    algorithm_name=pending.algorithm_name,
                    symbol=pending.signal.symbol,
                    action=pending.signal.action,
                    quantity=pending.signal.quantity,
                    success=False,
                    error="Failed to place order",
                )
                logger.error(f"Failed to place order for {pending.signal.symbol}")

            self._add_to_history(result)

            if self.on_execution:
                try:
                    self.on_execution(result)
                except Exception as e:
                    logger.error(f"Error in execution callback: {e}")

        except Exception as e:
            logger.error(f"Error executing order: {e}")
            result = ExecutionResult(
                algorithm_name=pending.algorithm_name,
                symbol=pending.signal.symbol,
                action=pending.signal.action,
                quantity=pending.signal.quantity,
                success=False,
                error=str(e),
            )
            self._add_to_history(result)

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
                if item[0] == "ORDER":
                    pending = item[1]
                    logger.warning(
                        f"Discarding unexecuted order: "
                        f"{pending.signal.action} {pending.signal.quantity} {pending.signal.symbol}"
                    )
            except Empty:
                break

    def get_algorithm_status(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get status for a specific algorithm.

        Args:
            name: Algorithm name

        Returns:
            Status dictionary or None
        """
        with self._lock:
            config = self._algorithms.get(name)
            if not config:
                return None

            return {
                "name": name,
                "enabled": config.enabled,
                "paused": config.paused,
                "execution_mode": config.execution_mode.value,
                "bar_timeframe": config.bar_timeframe.value,
                "run_count": config.run_count,
                "error_count": config.error_count,
                "last_run": config.last_run.isoformat() if config.last_run else None,
                "last_error": config.last_error,
                "circuit_breaker": config.circuit_breaker.to_dict(),
                "parameters": dict(config.parameters),
            }

    def get_status(self) -> Dict[str, Any]:
        """
        Get overall runner status.

        Returns:
            Status dictionary
        """
        with self._lock:
            algo_status = {
                name: {
                    "enabled": config.enabled,
                    "run_count": config.run_count,
                    "error_count": config.error_count,
                    "circuit_breaker_state": config.circuit_breaker.state,
                }
                for name, config in self._algorithms.items()
            }

            # Count open circuit breakers
            open_circuit_breakers = [
                name for name, config in self._algorithms.items()
                if config.circuit_breaker.state == "open"
            ]

        # Thread health status
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
            "algorithms": algo_status,
            "stats": self._stats,
            "pending_orders": len(self._pending_orders),
            "queue_size": self._order_queue.qsize(),
            "health": health_status,
            "open_circuit_breakers": open_circuit_breakers,
        }

    def get_execution_history(
        self,
        algorithm_name: Optional[str] = None,
        count: int = 100,
    ) -> List[ExecutionResult]:
        """
        Get execution history.

        Args:
            algorithm_name: Filter by algorithm (None = all)
            count: Maximum results to return

        Returns:
            List of ExecutionResult objects
        """
        history = self._execution_history

        if algorithm_name:
            history = [r for r in history if r.algorithm_name == algorithm_name]

        return history[-count:]
