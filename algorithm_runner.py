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
class AlgorithmConfig:
    """Configuration for a registered algorithm"""
    algorithm: AlgorithmBase
    execution_mode: ExecutionMode = ExecutionMode.ON_BAR
    bar_timeframe: DataType = DataType.BAR_1MIN
    enabled: bool = True
    max_signals_per_run: int = 10
    cooldown_seconds: float = 0.0  # Minimum time between runs
    last_run: Optional[datetime] = None
    run_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None


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
    ):
        """
        Initialize algorithm runner.

        Args:
            portfolio: Portfolio instance for order execution
            data_feed: DataFeed instance for market data
            order_mode: How orders should be executed
            reconciliation_mode: How to reconcile orders from multiple algorithms
        """
        self.portfolio = portfolio
        self.data_feed = data_feed
        self.order_mode = order_mode

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

        # Callbacks
        self.on_signal: Optional[Callable[[str, TradeSignal], None]] = None
        self.on_execution: Optional[Callable[[ExecutionResult], None]] = None
        self.on_reconciled: Optional[Callable[[ReconciledOrder], None]] = None
        self.on_error: Optional[Callable[[str, Exception], None]] = None

        # Statistics
        self._stats = {
            "started_at": None,
            "total_runs": 0,
            "total_signals": 0,
            "total_orders": 0,
            "total_errors": 0,
            "shares_saved_by_netting": 0,
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
    ) -> bool:
        """
        Register an algorithm with the runner.

        Args:
            algorithm: Algorithm instance
            execution_mode: When to trigger the algorithm
            bar_timeframe: Which bar timeframe triggers ON_BAR mode
            enabled: Whether the algorithm is enabled

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

            config = AlgorithmConfig(
                algorithm=algorithm,
                execution_mode=execution_mode,
                bar_timeframe=bar_timeframe,
                enabled=enabled,
            )
            self._algorithms[algorithm.name] = config

        logger.info(
            f"Registered algorithm '{algorithm.name}' "
            f"(mode={execution_mode.value}, timeframe={bar_timeframe.value})"
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
        self._stats["started_at"] = datetime.now().isoformat()

        # Set up data feed callbacks
        self._setup_data_callbacks()

        # Start executor thread
        self._executor_thread = Thread(
            target=self._executor_loop,
            daemon=True,
            name="AlgorithmRunner-Executor"
        )
        self._executor_thread.start()

        logger.info(f"Algorithm runner started with {len(self._algorithms)} algorithms")
        return True

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
        """Schedule an algorithm run (respects cooldown)"""
        now = datetime.now()

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

        try:
            # Prepare market data from feed
            market_data = self._prepare_market_data(algorithm)

            # Run algorithm
            result = algorithm.run(market_data=market_data)

            config.last_run = datetime.now()
            config.run_count += 1
            self._stats["total_runs"] += 1

            if result.success:
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

            return result

        except Exception as e:
            config.error_count += 1
            config.last_error = str(e)
            self._stats["total_errors"] += 1
            logger.error(f"Error running algorithm '{name}': {e}")
            logger.debug(traceback.format_exc())

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
                    self._execute_order(pending)

                elif item[0] == "RECONCILED":
                    # Execute reconciled/netted order
                    reconciled = item[1]
                    self._execute_reconciled_order(reconciled)

            except Empty:
                continue
            except Exception as e:
                logger.error(f"Executor error: {e}")
                logger.debug(traceback.format_exc())

        logger.debug("Executor thread stopped")

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
                "execution_mode": config.execution_mode.value,
                "bar_timeframe": config.bar_timeframe.value,
                "run_count": config.run_count,
                "error_count": config.error_count,
                "last_run": config.last_run.isoformat() if config.last_run else None,
                "last_error": config.last_error,
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
                }
                for name, config in self._algorithms.items()
            }

        return {
            "running": self._running,
            "paused": self._paused,
            "order_mode": self.order_mode.value,
            "algorithms": algo_status,
            "stats": self._stats,
            "pending_orders": len(self._pending_orders),
            "queue_size": self._order_queue.qsize(),
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
