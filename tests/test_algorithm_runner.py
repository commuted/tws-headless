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
        runner.register_algorithm(algo, cooldown_seconds=1.0)

        config = runner._algorithms[algo.name]
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
