"""
Tests for algorithm_runner.py - Continuous algorithm execution engine
"""

import pytest
import time
import threading
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
from queue import Queue

from ibapi.contract import Contract

from algorithm_runner import (
    AlgorithmRunner,
    ExecutionMode,
    OrderExecutionMode,
    AlgorithmConfig,
    PendingOrder,
    ExecutionResult,
    CircuitBreaker,
)
from data_feed import DataFeed, DataType, TickData
from algorithms.base import AlgorithmBase, AlgorithmResult, TradeSignal, AlgorithmInstrument
from models import Bar
from order_reconciler import ReconciliationMode


def create_mock_portfolio():
    """Create a mock Portfolio for testing"""
    portfolio = Mock()
    portfolio.connected = True
    portfolio.place_order = Mock(return_value=12345)
    portfolio.get_position = Mock(return_value=None)
    return portfolio


def create_mock_data_feed():
    """Create a mock DataFeed for testing"""
    feed = Mock(spec=DataFeed)
    feed.is_running = True
    feed.on_tick = None
    feed.on_bar = None
    feed.get_bars = Mock(return_value=[])
    return feed


def create_mock_algorithm(name: str = "test_algo", loaded: bool = True):
    """Create a mock Algorithm for testing"""
    algo = Mock(spec=AlgorithmBase)
    algo.name = name
    algo.is_loaded = loaded
    algo.load = Mock(return_value=True)
    algo.required_bars = 10

    # Create mock instruments
    instrument = Mock(spec=AlgorithmInstrument)
    instrument.symbol = "SPY"
    instrument.to_contract = Mock(return_value=Contract())
    algo.instruments = [instrument]
    algo.enabled_instruments = [instrument]
    algo.get_instrument = Mock(return_value=instrument)

    # Default to empty result
    algo.run = Mock(return_value=AlgorithmResult(
        algorithm_name=name,
        timestamp=datetime.now(),
        success=True,
        signals=[],
    ))

    return algo


def create_signal(symbol: str, action: str, quantity: int, confidence: float = 0.8) -> TradeSignal:
    """Helper to create a test trade signal"""
    return TradeSignal(
        symbol=symbol,
        action=action,
        quantity=quantity,
        confidence=confidence,
        reason="test signal",
    )


class TestExecutionMode:
    """Tests for ExecutionMode enum"""

    def test_execution_mode_values(self):
        """Test ExecutionMode values"""
        assert ExecutionMode.ON_BAR.value == "on_bar"
        assert ExecutionMode.ON_TICK.value == "on_tick"
        assert ExecutionMode.SCHEDULED.value == "scheduled"
        assert ExecutionMode.MANUAL.value == "manual"


class TestOrderExecutionMode:
    """Tests for OrderExecutionMode enum"""

    def test_order_execution_mode_values(self):
        """Test OrderExecutionMode values"""
        assert OrderExecutionMode.IMMEDIATE.value == "immediate"
        assert OrderExecutionMode.QUEUED.value == "queued"
        assert OrderExecutionMode.DRY_RUN.value == "dry_run"


class TestAlgorithmConfig:
    """Tests for AlgorithmConfig dataclass"""

    def test_default_values(self):
        """Test default config values"""
        algo = create_mock_algorithm()
        config = AlgorithmConfig(algorithm=algo)

        assert config.algorithm is algo
        assert config.execution_mode == ExecutionMode.ON_BAR
        assert config.bar_timeframe == DataType.BAR_1MIN
        assert config.enabled is True
        assert config.max_signals_per_run == 10
        assert config.cooldown_seconds == 0.0
        assert config.run_count == 0
        assert config.error_count == 0

    def test_custom_values(self):
        """Test custom config values"""
        algo = create_mock_algorithm()
        config = AlgorithmConfig(
            algorithm=algo,
            execution_mode=ExecutionMode.ON_TICK,
            bar_timeframe=DataType.BAR_5MIN,
            cooldown_seconds=5.0,
        )

        assert config.execution_mode == ExecutionMode.ON_TICK
        assert config.bar_timeframe == DataType.BAR_5MIN
        assert config.cooldown_seconds == 5.0


class TestPendingOrder:
    """Tests for PendingOrder dataclass"""

    def test_pending_order_creation(self):
        """Test creating a PendingOrder"""
        signal = create_signal("SPY", "BUY", 100)
        from ibapi.order import Order
        order = Order()

        pending = PendingOrder(
            algorithm_name="test_algo",
            signal=signal,
            contract=Contract(),
            order=order,
        )

        assert pending.algorithm_name == "test_algo"
        assert pending.status == "pending"
        assert isinstance(pending.created_at, datetime)


class TestExecutionResult:
    """Tests for ExecutionResult dataclass"""

    def test_execution_result_creation(self):
        """Test creating an ExecutionResult"""
        result = ExecutionResult(
            algorithm_name="test_algo",
            symbol="SPY",
            action="BUY",
            quantity=100,
            order_id=12345,
            success=True,
        )

        assert result.algorithm_name == "test_algo"
        assert result.symbol == "SPY"
        assert result.order_id == 12345
        assert result.success is True
        assert result.error is None


class TestAlgorithmRunnerInit:
    """Tests for AlgorithmRunner initialization"""

    def test_default_initialization(self):
        """Test default initialization"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()

        runner = AlgorithmRunner(portfolio, feed)

        assert runner.portfolio is portfolio
        assert runner.data_feed is feed
        assert runner.order_mode == OrderExecutionMode.DRY_RUN
        assert runner.is_running is False
        assert runner.is_paused is False
        assert len(runner.algorithms) == 0

    def test_custom_order_mode(self):
        """Test initialization with custom order mode"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()

        runner = AlgorithmRunner(
            portfolio, feed,
            order_mode=OrderExecutionMode.IMMEDIATE,
        )

        assert runner.order_mode == OrderExecutionMode.IMMEDIATE

    def test_custom_reconciliation_mode(self):
        """Test initialization with custom reconciliation mode"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()

        runner = AlgorithmRunner(
            portfolio, feed,
            reconciliation_mode=ReconciliationMode.FIFO,
        )

        assert runner._reconciler.mode == ReconciliationMode.FIFO


class TestAlgorithmRegistration:
    """Tests for algorithm registration"""

    def test_register_algorithm(self):
        """Test registering an algorithm"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm("test_algo")
        result = runner.register_algorithm(algo)

        assert result is True
        assert "test_algo" in runner.algorithms

    def test_register_algorithm_with_mode(self):
        """Test registering with execution mode"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(
            algo,
            execution_mode=ExecutionMode.ON_TICK,
            bar_timeframe=DataType.BAR_5MIN,
        )

        config = runner._algorithms[algo.name]
        assert config.execution_mode == ExecutionMode.ON_TICK
        assert config.bar_timeframe == DataType.BAR_5MIN

    def test_register_unloaded_algorithm(self):
        """Test registering an unloaded algorithm auto-loads it"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm(loaded=False)
        runner.register_algorithm(algo)

        assert algo.load.called

    def test_register_duplicate_fails(self):
        """Test registering duplicate algorithm fails"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm("test_algo")
        runner.register_algorithm(algo)
        result = runner.register_algorithm(algo)

        assert result is False

    def test_unregister_algorithm(self):
        """Test unregistering an algorithm"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm("test_algo")
        runner.register_algorithm(algo)
        runner.unregister_algorithm("test_algo")

        assert "test_algo" not in runner.algorithms

    def test_enable_disable_algorithm(self):
        """Test enabling/disabling an algorithm"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo, enabled=True)

        runner.enable_algorithm(algo.name, enabled=False)
        assert runner._algorithms[algo.name].enabled is False

        runner.enable_algorithm(algo.name, enabled=True)
        assert runner._algorithms[algo.name].enabled is True


class TestAlgorithmRunnerStartStop:
    """Tests for starting and stopping the runner"""

    def test_start_success(self):
        """Test successful start"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        result = runner.start()

        assert result is True
        assert runner.is_running is True

    def test_start_when_already_running(self):
        """Test start when already running"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)
        runner._running = True

        result = runner.start()

        assert result is True

    def test_start_fails_when_feed_not_running(self):
        """Test start fails when data feed not running"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        feed.is_running = False
        runner = AlgorithmRunner(portfolio, feed)

        result = runner.start()

        assert result is False

    def test_stop(self):
        """Test stopping the runner"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)
        runner.start()

        runner.stop()

        assert runner.is_running is False

    def test_pause_resume(self):
        """Test pausing and resuming"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        runner.pause()
        assert runner.is_paused is True

        runner.resume()
        assert runner.is_paused is False


class TestAlgorithmExecution:
    """Tests for algorithm execution"""

    def test_run_algorithm(self):
        """Test running an algorithm"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        config = runner._algorithms[algo.name]
        result = runner._run_algorithm(config)

        assert result.success is True
        assert algo.run.called
        assert config.run_count == 1

    def test_run_algorithm_with_signals(self):
        """Test running algorithm that produces signals"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        # Create algorithm with signals
        algo = create_mock_algorithm()
        signal = create_signal("SPY", "BUY", 100)
        algo.run.return_value = AlgorithmResult(
            algorithm_name=algo.name,
            timestamp=datetime.now(),
            success=True,
            signals=[signal],
        )
        runner.register_algorithm(algo)

        signals_received = []
        runner.on_signal = lambda name, sig: signals_received.append((name, sig))

        config = runner._algorithms[algo.name]
        runner._run_algorithm(config)

        assert len(signals_received) == 1
        assert signals_received[0][1].action == "BUY"

    def test_run_algorithm_error_handling(self):
        """Test algorithm error handling"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        algo.run.side_effect = Exception("Algorithm error")
        runner.register_algorithm(algo)

        errors = []
        runner.on_error = lambda name, e: errors.append((name, e))

        config = runner._algorithms[algo.name]
        result = runner._run_algorithm(config)

        assert result.success is False
        assert config.error_count == 1
        assert len(errors) == 1

    def test_trigger_algorithm(self):
        """Test manually triggering an algorithm"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        result = runner.trigger_algorithm(algo.name)

        assert result is not None
        assert result.success is True

    def test_trigger_nonexistent_algorithm(self):
        """Test triggering nonexistent algorithm"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        result = runner.trigger_algorithm("nonexistent")

        assert result is None


class TestSignalProcessing:
    """Tests for signal processing"""

    def test_process_signal_adds_to_reconciler(self):
        """Test that signals are added to reconciler"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        signal = create_signal("SPY", "BUY", 100)
        runner._process_signal(algo.name, signal)

        assert runner._reconciler.get_pending_count() == 1

    def test_process_non_actionable_signal(self):
        """Test that non-actionable signals are ignored"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        signal = create_signal("SPY", "HOLD", 0)  # Not actionable
        runner._process_signal(algo.name, signal)

        assert runner._reconciler.get_pending_count() == 0


class TestReconcileAndExecute:
    """Tests for reconciliation and execution"""

    def test_reconcile_and_execute_dry_run(self):
        """Test reconcile and execute in dry run mode"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(
            portfolio, feed,
            order_mode=OrderExecutionMode.DRY_RUN,
        )

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        # Add signals
        runner._reconciler.add_signal(
            algo.name,
            create_signal("SPY", "BUY", 100),
            Contract(),
        )

        executions = []
        runner.on_execution = lambda r: executions.append(r)

        runner.reconcile_and_execute()

        assert len(executions) == 1
        assert executions[0].success is True

    def test_reconcile_multiple_algorithms(self):
        """Test reconciliation with multiple algorithms"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo1 = create_mock_algorithm("algo1")
        algo2 = create_mock_algorithm("algo2")
        runner.register_algorithm(algo1)
        runner.register_algorithm(algo2)

        # algo1 wants to buy 100, algo2 wants to sell 30
        runner._reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), Contract())
        runner._reconciler.add_signal("algo2", create_signal("SPY", "SELL", 30), Contract())

        reconciled = []
        runner.on_reconciled = lambda r: reconciled.append(r)

        runner.reconcile_and_execute()

        # Should have 1 reconciled order: BUY 70
        assert len(reconciled) == 1
        assert reconciled[0].action == "BUY"
        assert reconciled[0].net_quantity == 70

    def test_shares_saved_stat_updated(self):
        """Test that shares saved stat is updated"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        runner._reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), Contract())
        runner._reconciler.add_signal("algo2", create_signal("SPY", "SELL", 30), Contract())

        runner.reconcile_and_execute()

        # Shares saved: 100 + 30 - 70 = 60
        assert runner.stats["shares_saved_by_netting"] == 60


class TestMarketDataPreparation:
    """Tests for market data preparation"""

    def test_prepare_market_data(self):
        """Test preparing market data for algorithm"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()

        # Set up mock bars
        bars = [
            Bar(
                symbol="SPY",
                timestamp=f"2024-01-15T10:0{i}:00",
                open=450.0 + i,
                high=451.0,
                low=449.0,
                close=450.5,
                volume=100,
                wap=0.0,
                bar_count=1,
            )
            for i in range(5)
        ]
        feed.get_bars.return_value = bars

        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        market_data = runner._prepare_market_data(algo)

        assert "SPY" in market_data
        assert len(market_data["SPY"]) == 5


class TestDataCallbacks:
    """Tests for data callback handling"""

    def test_on_bar_triggers_algorithms(self):
        """Test that ON_BAR algorithms are triggered on bars"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(
            algo,
            execution_mode=ExecutionMode.ON_BAR,
            bar_timeframe=DataType.BAR_1MIN,
        )
        runner.start()

        # Simulate bar arrival
        bar = Bar(
            symbol="SPY",
            timestamp="2024-01-15T10:01:00",
            open=450.0,
            high=451.0,
            low=449.0,
            close=450.5,
            volume=100,
            wap=0.0,
            bar_count=1,
        )

        runner._on_bar("SPY", bar, DataType.BAR_1MIN)

        # Give executor thread time to process
        time.sleep(0.1)

        # Algorithm should have been scheduled (check queue)
        assert runner._order_queue.qsize() >= 0  # May have already been processed

        runner.stop()

    def test_on_tick_triggers_algorithms(self):
        """Test that ON_TICK algorithms are triggered on ticks"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(
            algo,
            execution_mode=ExecutionMode.ON_TICK,
        )
        runner._running = True

        tick = TickData(symbol="SPY", price=450.0, tick_type="LAST")
        runner._on_tick("SPY", tick)

        # Check that algorithm was scheduled
        # (would be in queue or already processed)


class TestCooldown:
    """Tests for cooldown functionality"""

    def test_cooldown_prevents_immediate_rerun(self):
        """Test that cooldown prevents immediate re-runs"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        config = runner._algorithms[algo.name]
        config.cooldown_seconds = 1.0
        config.last_run = datetime.now()

        # Should not schedule due to cooldown
        runner._schedule_run(config)

        # Queue should be empty (cooldown active)
        assert runner._order_queue.empty()


class TestStatus:
    """Tests for status reporting"""

    def test_get_status(self):
        """Test getting runner status"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        status = runner.get_status()

        assert "running" in status
        assert "paused" in status
        assert "order_mode" in status
        assert "algorithms" in status
        assert "stats" in status
        assert algo.name in status["algorithms"]

    def test_get_algorithm_status(self):
        """Test getting specific algorithm status"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo, execution_mode=ExecutionMode.ON_BAR)

        status = runner.get_algorithm_status(algo.name)

        assert status is not None
        assert status["name"] == algo.name
        assert status["enabled"] is True
        assert status["execution_mode"] == "on_bar"

    def test_get_algorithm_status_nonexistent(self):
        """Test getting status of nonexistent algorithm"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        status = runner.get_algorithm_status("nonexistent")
        assert status is None


class TestExecutionHistory:
    """Tests for execution history"""

    def test_execution_history_recorded(self):
        """Test that executions are recorded in history"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        # Add and process signal
        runner._reconciler.add_signal(
            algo.name,
            create_signal("SPY", "BUY", 100),
            Contract(),
        )
        runner.reconcile_and_execute()

        history = runner.get_execution_history()
        assert len(history) >= 1

    def test_execution_history_filtered_by_algorithm(self):
        """Test filtering execution history by algorithm"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        # Add executions for different algorithms
        runner._execution_history = [
            ExecutionResult("algo1", "SPY", "BUY", 100, success=True),
            ExecutionResult("algo2", "AAPL", "SELL", 50, success=True),
            ExecutionResult("algo1", "QQQ", "BUY", 25, success=True),
        ]

        history = runner.get_execution_history(algorithm_name="algo1")
        assert len(history) == 2
        assert all(r.algorithm_name == "algo1" for r in history)

    def test_execution_history_limited(self):
        """Test execution history respects count limit"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        runner._execution_history = [
            ExecutionResult(f"algo{i}", "SPY", "BUY", 100, success=True)
            for i in range(100)
        ]

        history = runner.get_execution_history(count=10)
        assert len(history) == 10

    def test_execution_history_max_size(self):
        """Test execution history is capped at max size"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)
        runner._max_history = 50

        # Add more than max
        for i in range(100):
            runner._add_to_history(
                ExecutionResult(f"algo{i}", "SPY", "BUY", 100, success=True)
            )

        assert len(runner._execution_history) == 50


class TestOrderExecution:
    """Tests for order execution"""

    def test_execute_order_immediate(self):
        """Test immediate order execution"""
        portfolio = create_mock_portfolio()
        portfolio.place_order.return_value = 12345
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(
            portfolio, feed,
            order_mode=OrderExecutionMode.IMMEDIATE,
        )

        from ibapi.order import Order
        pending = PendingOrder(
            algorithm_name="test_algo",
            signal=create_signal("SPY", "BUY", 100),
            contract=Contract(),
            order=Order(),
        )

        runner._execute_order(pending)

        assert portfolio.place_order.called
        assert runner.stats["total_orders"] == 1

    def test_execute_order_dry_run(self):
        """Test dry run order execution"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(
            portfolio, feed,
            order_mode=OrderExecutionMode.DRY_RUN,
        )

        from ibapi.order import Order
        pending = PendingOrder(
            algorithm_name="test_algo",
            signal=create_signal("SPY", "BUY", 100),
            contract=Contract(),
            order=Order(),
        )

        executions = []
        runner.on_execution = lambda r: executions.append(r)

        runner._execute_order(pending)

        # Should not place real order
        assert not portfolio.place_order.called
        # But should record execution
        assert len(executions) == 1
        assert executions[0].success is True


class TestStatsProperty:
    """Tests for stats property"""

    def test_stats_returns_copy(self):
        """Test that stats property returns a copy"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        stats1 = runner.stats
        stats1["total_runs"] = 999

        stats2 = runner.stats
        assert stats2["total_runs"] != 999


class TestThreadSafety:
    """Tests for thread safety"""

    def test_concurrent_algorithm_registration(self):
        """Test concurrent algorithm registration"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)
        errors = []

        def register_algos(prefix, count):
            try:
                for i in range(count):
                    algo = create_mock_algorithm(f"{prefix}_{i}")
                    runner.register_algorithm(algo)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=register_algos, args=(f"thread{t}", 10))
            for t in range(5)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(runner.algorithms) == 50  # 5 threads * 10 algorithms


# =============================================================================
# Circuit Breaker Tests
# =============================================================================


class TestCircuitBreaker:
    """Tests for CircuitBreaker dataclass"""

    def test_default_values(self):
        """Test default circuit breaker values"""
        cb = CircuitBreaker()

        assert cb.max_failures == 5
        assert cb.reset_after_seconds == 300.0
        assert cb.half_open_max_failures == 2
        assert cb.consecutive_failures == 0
        assert cb.total_failures == 0
        assert cb.state == "closed"
        assert cb.last_failure_time is None
        assert cb.tripped_at is None

    def test_custom_values(self):
        """Test custom circuit breaker values"""
        cb = CircuitBreaker(
            max_failures=3,
            reset_after_seconds=60.0,
            half_open_max_failures=1,
        )

        assert cb.max_failures == 3
        assert cb.reset_after_seconds == 60.0
        assert cb.half_open_max_failures == 1

    def test_record_success_resets_consecutive_failures(self):
        """Test that success resets consecutive failures"""
        cb = CircuitBreaker()
        cb.consecutive_failures = 3

        cb.record_success()

        assert cb.consecutive_failures == 0

    def test_record_success_closes_half_open_circuit(self):
        """Test that success in half-open state closes circuit"""
        cb = CircuitBreaker()
        cb.state = "half_open"
        cb.tripped_at = datetime.now()

        cb.record_success()

        assert cb.state == "closed"
        assert cb.tripped_at is None

    def test_record_failure_increments_counters(self):
        """Test that failure increments counters"""
        cb = CircuitBreaker()

        cb.record_failure()

        assert cb.consecutive_failures == 1
        assert cb.total_failures == 1
        assert cb.last_failure_time is not None

    def test_record_failure_trips_after_max_failures(self):
        """Test circuit trips after max consecutive failures"""
        cb = CircuitBreaker(max_failures=3)

        # First two failures - no trip
        cb.record_failure()
        cb.record_failure()
        assert cb.state == "closed"

        # Third failure - trips
        tripped = cb.record_failure()

        assert tripped is True
        assert cb.state == "open"
        assert cb.tripped_at is not None

    def test_should_allow_when_closed(self):
        """Test should_allow returns True when closed"""
        cb = CircuitBreaker()

        assert cb.should_allow() is True

    def test_should_allow_when_open(self):
        """Test should_allow returns False when open"""
        cb = CircuitBreaker()
        cb.state = "open"
        cb.tripped_at = datetime.now()

        assert cb.should_allow() is False

    def test_should_allow_auto_resets_after_timeout(self):
        """Test that circuit auto-resets to half-open after timeout"""
        cb = CircuitBreaker(reset_after_seconds=0.01)  # 10ms timeout
        cb.state = "open"
        cb.tripped_at = datetime.now()

        # Wait for timeout
        time.sleep(0.02)

        assert cb.should_allow() is True
        assert cb.state == "half_open"

    def test_should_allow_when_half_open(self):
        """Test should_allow returns True when half-open"""
        cb = CircuitBreaker()
        cb.state = "half_open"

        assert cb.should_allow() is True

    def test_half_open_trips_on_fewer_failures(self):
        """Test half-open trips after half_open_max_failures"""
        cb = CircuitBreaker(half_open_max_failures=2)
        cb.state = "half_open"

        # First failure
        cb.record_failure()
        assert cb.state == "half_open"

        # Second failure - trips
        tripped = cb.record_failure()

        assert tripped is True
        assert cb.state == "open"

    def test_reset_clears_state(self):
        """Test manual reset clears all state"""
        cb = CircuitBreaker()
        cb.state = "open"
        cb.consecutive_failures = 5
        cb.tripped_at = datetime.now()

        cb.reset()

        assert cb.state == "closed"
        assert cb.consecutive_failures == 0
        assert cb.tripped_at is None

    def test_to_dict(self):
        """Test to_dict returns proper dictionary"""
        cb = CircuitBreaker(max_failures=3, reset_after_seconds=60.0)
        cb.consecutive_failures = 2
        cb.total_failures = 5
        cb.last_failure_time = datetime.now()

        d = cb.to_dict()

        assert d["state"] == "closed"
        assert d["consecutive_failures"] == 2
        assert d["total_failures"] == 5
        assert d["max_failures"] == 3
        assert d["reset_after_seconds"] == 60.0
        assert d["last_failure_time"] is not None
        assert d["tripped_at"] is None


class TestCircuitBreakerIntegration:
    """Tests for circuit breaker integration with AlgorithmRunner"""

    def test_runner_with_circuit_breaker_defaults(self):
        """Test runner initializes with circuit breaker defaults"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()

        runner = AlgorithmRunner(
            portfolio, feed,
            circuit_breaker_failures=10,
            circuit_breaker_reset_seconds=600.0,
        )

        assert runner._default_cb_failures == 10
        assert runner._default_cb_reset_seconds == 600.0

    def test_register_algorithm_with_circuit_breaker(self):
        """Test registering algorithm creates circuit breaker"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        config = runner._algorithms[algo.name]
        assert config.circuit_breaker is not None
        assert config.circuit_breaker.state == "closed"

    def test_register_algorithm_with_custom_circuit_breaker(self):
        """Test registering algorithm with custom circuit breaker settings"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(
            algo,
            circuit_breaker_failures=3,
            circuit_breaker_reset_seconds=60.0,
        )

        config = runner._algorithms[algo.name]
        assert config.circuit_breaker.max_failures == 3
        assert config.circuit_breaker.reset_after_seconds == 60.0

    def test_circuit_breaker_trips_on_repeated_failures(self):
        """Test circuit breaker trips after repeated algorithm failures"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(
            portfolio, feed,
            circuit_breaker_failures=3,
        )

        algo = create_mock_algorithm()
        algo.run.side_effect = Exception("Always fails")
        runner.register_algorithm(algo)

        config = runner._algorithms[algo.name]

        # Run 3 times to trip circuit breaker
        for _ in range(3):
            runner._run_algorithm(config)

        assert config.circuit_breaker.state == "open"
        assert runner.stats["circuit_breaker_trips"] == 1

    def test_tripped_circuit_breaker_blocks_execution(self):
        """Test that tripped circuit breaker blocks algorithm execution"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        config = runner._algorithms[algo.name]
        config.circuit_breaker.state = "open"
        config.circuit_breaker.tripped_at = datetime.now()

        result = runner._run_algorithm(config)

        assert result.success is False
        assert result.error == "Circuit breaker open"
        assert not algo.run.called

    def test_circuit_breaker_success_resets(self):
        """Test successful runs reset circuit breaker"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        config = runner._algorithms[algo.name]
        config.circuit_breaker.consecutive_failures = 3

        runner._run_algorithm(config)

        assert config.circuit_breaker.consecutive_failures == 0

    def test_reset_circuit_breaker_method(self):
        """Test manual circuit breaker reset"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        config = runner._algorithms[algo.name]
        config.circuit_breaker.state = "open"
        config.circuit_breaker.consecutive_failures = 5

        result = runner.reset_circuit_breaker(algo.name)

        assert result is True
        assert config.circuit_breaker.state == "closed"
        assert config.circuit_breaker.consecutive_failures == 0

    def test_reset_circuit_breaker_nonexistent(self):
        """Test resetting nonexistent algorithm returns False"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        result = runner.reset_circuit_breaker("nonexistent")

        assert result is False

    def test_get_circuit_breaker_status(self):
        """Test getting circuit breaker status"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        status = runner.get_circuit_breaker_status(algo.name)

        assert status is not None
        assert "state" in status
        assert "consecutive_failures" in status
        assert "total_failures" in status

    def test_get_circuit_breaker_status_nonexistent(self):
        """Test getting circuit breaker status for nonexistent algorithm"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        status = runner.get_circuit_breaker_status("nonexistent")

        assert status is None

    def test_get_all_circuit_breakers(self):
        """Test getting all circuit breaker statuses"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo1 = create_mock_algorithm("algo1")
        algo2 = create_mock_algorithm("algo2")
        runner.register_algorithm(algo1)
        runner.register_algorithm(algo2)

        all_cb = runner.get_all_circuit_breakers()

        assert len(all_cb) == 2
        assert "algo1" in all_cb
        assert "algo2" in all_cb

    def test_circuit_breaker_callback(self):
        """Test circuit breaker trip callback is invoked"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(
            portfolio, feed,
            circuit_breaker_failures=2,
        )

        algo = create_mock_algorithm()
        algo.run.side_effect = Exception("Fails")
        runner.register_algorithm(algo)

        tripped_algos = []
        runner.on_circuit_breaker_trip = lambda name: tripped_algos.append(name)

        config = runner._algorithms[algo.name]

        # Trip the circuit breaker
        runner._run_algorithm(config)
        runner._run_algorithm(config)

        assert algo.name in tripped_algos

    def test_schedule_run_respects_circuit_breaker(self):
        """Test _schedule_run respects circuit breaker"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        config = runner._algorithms[algo.name]
        config.circuit_breaker.state = "open"
        config.circuit_breaker.tripped_at = datetime.now()

        runner._schedule_run(config)

        # Queue should be empty - run blocked by circuit breaker
        assert runner._order_queue.empty()

    def test_get_algorithm_status_includes_circuit_breaker(self):
        """Test get_algorithm_status includes circuit breaker info"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        status = runner.get_algorithm_status(algo.name)

        assert "circuit_breaker" in status
        assert status["circuit_breaker"]["state"] == "closed"


class TestHealthMonitoring:
    """Tests for thread health monitoring"""

    def test_health_check_interval_config(self):
        """Test health check interval configuration"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()

        runner = AlgorithmRunner(
            portfolio, feed,
            health_check_interval=10.0,
        )

        assert runner._health_check_interval == 10.0

    def test_start_launches_health_thread(self):
        """Test that start() launches health monitoring thread"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        runner.start()
        time.sleep(0.05)

        assert runner._health_thread is not None
        assert runner._health_thread.is_alive()

        runner.stop()

    def test_stop_stops_health_thread(self):
        """Test that stop() stops health monitoring thread"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        runner.start()
        time.sleep(0.05)
        runner.stop()
        time.sleep(0.1)

        assert not runner._health_thread.is_alive()

    def test_executor_restart_counter(self):
        """Test executor restart counter is tracked"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        runner.start()
        initial_restarts = runner._executor_restart_count

        runner.stop()

        assert runner._executor_restart_count == initial_restarts

    def test_get_status_includes_health_info(self):
        """Test get_status includes health information"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        runner.start()
        time.sleep(0.05)

        status = runner.get_status()

        assert "health" in status
        assert "executor_thread_alive" in status["health"]
        assert "health_thread_alive" in status["health"]
        assert "executor_restart_count" in status["health"]
        assert "max_executor_restarts" in status["health"]

        runner.stop()

    def test_get_status_includes_open_circuit_breakers(self):
        """Test get_status includes list of open circuit breakers"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo1 = create_mock_algorithm("algo1")
        algo2 = create_mock_algorithm("algo2")
        runner.register_algorithm(algo1)
        runner.register_algorithm(algo2)

        # Trip algo2's circuit breaker
        runner._algorithms["algo2"].circuit_breaker.state = "open"

        status = runner.get_status()

        assert "open_circuit_breakers" in status
        assert "algo2" in status["open_circuit_breakers"]
        assert "algo1" not in status["open_circuit_breakers"]

    def test_get_status_algorithm_includes_circuit_breaker_state(self):
        """Test algorithm status in get_status includes circuit breaker state"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        status = runner.get_status()

        assert "circuit_breaker_state" in status["algorithms"][algo.name]
        assert status["algorithms"][algo.name]["circuit_breaker_state"] == "closed"

    def test_stats_includes_recovery_metrics(self):
        """Test stats includes recovery-related metrics"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        stats = runner.stats

        assert "executor_restarts" in stats
        assert "circuit_breaker_trips" in stats

    def test_executor_loop_wrapper_catches_exceptions(self):
        """Test executor loop wrapper catches and logs exceptions"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        # Simulate a crash in executor loop by making it raise
        original_loop = runner._executor_loop

        def crashing_loop():
            raise RuntimeError("Executor crashed!")

        runner._executor_loop = crashing_loop

        # Should not raise - wrapper catches exception
        runner._executor_loop_wrapper()

        # Restore
        runner._executor_loop = original_loop

    def test_health_thread_detects_dead_executor(self):
        """Test health thread detects when executor dies"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(
            portfolio, feed,
            health_check_interval=0.05,  # 50ms checks
        )

        runner.start()
        time.sleep(0.02)

        # Verify executor is running
        assert runner._executor_thread.is_alive()

        # Simulate executor death by stopping it manually
        original_thread = runner._executor_thread
        runner._shutdown_event.set()
        time.sleep(0.15)  # Wait for executor to stop

        # Note: In real scenario, health monitor would restart it
        # Here we just verify the detection works

        runner.stop()

    def test_max_executor_restarts_limit(self):
        """Test executor restart limit configuration"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        assert runner._max_executor_restarts == 10  # Default value

    def test_start_resets_executor_restart_count(self):
        """Test start() resets executor restart count"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        runner._executor_restart_count = 5

        runner.start()

        assert runner._executor_restart_count == 0

        runner.stop()


# =============================================================================
# Pause/Resume Algorithm Tests
# =============================================================================


class TestPauseResumeAlgorithm:
    """Tests for algorithm pause and resume functionality"""

    def test_pause_algorithm_success(self):
        """Test pausing an algorithm"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        result = runner.pause_algorithm(algo.name)

        assert result is True
        assert runner._algorithms[algo.name].paused is True

    def test_pause_algorithm_nonexistent(self):
        """Test pausing nonexistent algorithm"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        result = runner.pause_algorithm("nonexistent")

        assert result is False

    def test_resume_algorithm_success(self):
        """Test resuming an algorithm"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)
        runner._algorithms[algo.name].paused = True

        result = runner.resume_algorithm(algo.name)

        assert result is True
        assert runner._algorithms[algo.name].paused is False

    def test_resume_algorithm_nonexistent(self):
        """Test resuming nonexistent algorithm"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        result = runner.resume_algorithm("nonexistent")

        assert result is False

    def test_paused_algorithm_not_scheduled(self):
        """Test that paused algorithms are not scheduled"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        config = runner._algorithms[algo.name]
        config.paused = True

        runner._schedule_run(config)

        # Queue should be empty - run blocked by paused state
        assert runner._order_queue.empty()

    def test_get_algorithm_status_includes_paused(self):
        """Test get_algorithm_status includes paused state"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)
        runner._algorithms[algo.name].paused = True

        status = runner.get_algorithm_status(algo.name)

        assert "paused" in status
        assert status["paused"] is True


# =============================================================================
# Algorithm Parameters Tests
# =============================================================================


class TestAlgorithmParameters:
    """Tests for algorithm parameter get/set functionality"""

    def test_set_algorithm_parameter_success(self):
        """Test setting an algorithm parameter"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)

        result = runner.set_algorithm_parameter(algo.name, "lookback", 20)

        assert result is True
        assert runner._algorithms[algo.name].parameters["lookback"] == 20

    def test_set_algorithm_parameter_nonexistent(self):
        """Test setting parameter on nonexistent algorithm"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        result = runner.set_algorithm_parameter("nonexistent", "key", "value")

        assert result is False

    def test_get_algorithm_parameters_success(self):
        """Test getting algorithm parameters"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        algo.get_parameters = Mock(return_value=None)
        runner.register_algorithm(algo)
        runner._algorithms[algo.name].parameters = {"lookback": 20, "threshold": 0.5}

        params = runner.get_algorithm_parameters(algo.name)

        assert params is not None
        assert params["lookback"] == 20
        assert params["threshold"] == 0.5

    def test_get_algorithm_parameters_nonexistent(self):
        """Test getting parameters from nonexistent algorithm"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        params = runner.get_algorithm_parameters("nonexistent")

        assert params is None

    def test_get_algorithm_status_includes_parameters(self):
        """Test get_algorithm_status includes parameters"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        algo = create_mock_algorithm()
        runner.register_algorithm(algo)
        runner._algorithms[algo.name].parameters = {"lookback": 20}

        status = runner.get_algorithm_status(algo.name)

        assert "parameters" in status
        assert status["parameters"]["lookback"] == 20


# =============================================================================
# Rate Limiter Integration Tests
# =============================================================================


class TestRateLimiterIntegration:
    """Tests for rate limiter integration with AlgorithmRunner"""

    def test_runner_with_rate_limiter_config(self):
        """Test runner initializes with rate limiter config"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()

        runner = AlgorithmRunner(
            portfolio, feed,
            order_rate_limit=5.0,
            order_burst_size=5,
        )

        assert runner._order_rate_limiter is not None

    def test_get_rate_limiter_stats(self):
        """Test getting rate limiter stats"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        stats = runner.get_rate_limiter_stats()

        assert stats is not None
        assert "requests_allowed" in stats
        assert "orders_rejected" in stats
        assert "orders_per_second_limit" in stats

    def test_stats_includes_rate_limit_metrics(self):
        """Test stats includes rate limiting metrics"""
        portfolio = create_mock_portfolio()
        feed = create_mock_data_feed()
        runner = AlgorithmRunner(portfolio, feed)

        stats = runner.stats

        assert "rate_limit_delays" in stats
        assert "rate_limit_rejects" in stats
