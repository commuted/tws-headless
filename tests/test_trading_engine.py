"""
Tests for trading_engine.py - Unified trading engine
"""

import pytest
import time
import threading
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch, PropertyMock

from ibapi.contract import Contract

from trading_engine import (
    TradingEngine,
    EngineConfig,
    EngineState,
    create_engine,
)
from algorithm_runner import ExecutionMode, OrderExecutionMode
from data_feed import DataType, TickData
from algorithms.base import AlgorithmBase, AlgorithmResult, TradeSignal, AlgorithmInstrument
from models import Bar


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


class TestEngineState:
    """Tests for EngineState enum"""

    def test_engine_state_values(self):
        """Test EngineState values"""
        assert EngineState.STOPPED.value == "stopped"
        assert EngineState.STARTING.value == "starting"
        assert EngineState.RUNNING.value == "running"
        assert EngineState.PAUSED.value == "paused"
        assert EngineState.STOPPING.value == "stopping"
        assert EngineState.ERROR.value == "error"


class TestEngineConfig:
    """Tests for EngineConfig dataclass"""

    def test_default_values(self):
        """Test default configuration values"""
        config = EngineConfig()

        assert config.host == "127.0.0.1"
        assert config.port == 7497
        assert config.client_id == 1
        assert config.auto_reconnect is True
        assert config.keepalive_enabled is True
        assert config.keepalive_interval == 30.0
        assert config.use_delayed_data is True
        assert config.order_mode == OrderExecutionMode.DRY_RUN
        assert config.default_execution_mode == ExecutionMode.ON_BAR
        assert config.default_bar_timeframe == DataType.BAR_1MIN
        assert config.load_portfolio_on_start is True
        assert config.fetch_prices_on_start is True

    def test_custom_values(self):
        """Test custom configuration values"""
        config = EngineConfig(
            host="192.168.1.100",
            port=4002,
            client_id=5,
            order_mode=OrderExecutionMode.IMMEDIATE,
            keepalive_interval=60.0,
        )

        assert config.host == "192.168.1.100"
        assert config.port == 4002
        assert config.client_id == 5
        assert config.order_mode == OrderExecutionMode.IMMEDIATE
        assert config.keepalive_interval == 60.0


class TestTradingEngineInit:
    """Tests for TradingEngine initialization"""

    def test_default_initialization(self):
        """Test default initialization"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()

            assert engine.state == EngineState.STOPPED
            assert engine.is_running is False
            assert engine.config is not None

    def test_custom_config(self):
        """Test initialization with custom config"""
        with patch('trading_engine.Portfolio'):
            config = EngineConfig(port=4002)
            engine = TradingEngine(config)

            assert engine.config.port == 4002

    def test_callbacks_initialized_none(self):
        """Test callbacks are initialized to None"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()

            assert engine.on_started is None
            assert engine.on_stopped is None
            assert engine.on_error is None
            assert engine.on_signal is None
            assert engine.on_execution is None
            assert engine.on_tick is None
            assert engine.on_bar is None


class TestTradingEngineProperties:
    """Tests for TradingEngine properties"""

    def test_state_property(self):
        """Test state property"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            assert engine.state == EngineState.STOPPED

            engine._state = EngineState.RUNNING
            assert engine.state == EngineState.RUNNING

    def test_is_running_property(self):
        """Test is_running property"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            assert engine.is_running is False

            engine._state = EngineState.RUNNING
            assert engine.is_running is True

            engine._state = EngineState.PAUSED
            assert engine.is_running is False

    def test_portfolio_property(self):
        """Test portfolio property"""
        with patch('trading_engine.Portfolio') as MockPortfolio:
            engine = TradingEngine()
            assert engine.portfolio is engine._portfolio

    def test_data_feed_property(self):
        """Test data_feed property"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            assert engine.data_feed is engine._data_feed

    def test_runner_property(self):
        """Test runner property"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            assert engine.runner is engine._runner


class TestAddAlgorithm:
    """Tests for adding algorithms"""

    def test_add_algorithm_before_start(self):
        """Test adding algorithm before engine starts"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            algo = create_mock_algorithm()

            result = engine.add_algorithm(algo)

            assert result is True
            assert len(engine._pending_algorithms) == 1

    def test_add_algorithm_with_execution_mode(self):
        """Test adding algorithm with custom execution mode"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            algo = create_mock_algorithm()

            engine.add_algorithm(
                algo,
                execution_mode=ExecutionMode.ON_TICK,
                bar_timeframe=DataType.BAR_5MIN,
            )

            pending = engine._pending_algorithms[0]
            assert pending[1] == ExecutionMode.ON_TICK
            assert pending[2] == DataType.BAR_5MIN

    def test_add_unloaded_algorithm(self):
        """Test adding unloaded algorithm auto-loads it"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            algo = create_mock_algorithm(loaded=False)

            engine.add_algorithm(algo)

            assert algo.load.called

    def test_add_algorithm_load_fails(self):
        """Test adding algorithm that fails to load"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            algo = create_mock_algorithm(loaded=False)
            algo.load.return_value = False

            result = engine.add_algorithm(algo)

            assert result is False

    def test_remove_algorithm(self):
        """Test removing an algorithm"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()

            # Mock runner's unregister method
            engine._runner.unregister_algorithm = Mock()

            engine.remove_algorithm("test_algo")

            engine._runner.unregister_algorithm.assert_called_with("test_algo")


class TestSubscription:
    """Tests for subscription management"""

    def test_subscribe(self):
        """Test subscribing to a symbol"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            contract = Contract()
            contract.symbol = "SPY"

            engine.subscribe("SPY", contract)

            assert "SPY" in engine._subscribed_symbols

    def test_subscribe_with_data_types(self):
        """Test subscribing with specific data types"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            contract = Contract()

            engine.subscribe("SPY", contract, {DataType.TICK, DataType.BAR_5MIN})

            engine._data_feed.subscribe.assert_called()

    def test_unsubscribe(self):
        """Test unsubscribing from a symbol"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._subscribed_symbols.add("SPY")

            engine.unsubscribe("SPY")

            assert "SPY" not in engine._subscribed_symbols


class TestStartStop:
    """Tests for starting and stopping the engine"""

    def test_start_when_stopped(self):
        """Test starting engine when stopped"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._connection_manager.start = Mock(return_value=True)

            result = engine.start()

            assert result is True
            assert engine.state == EngineState.RUNNING

    def test_start_when_not_stopped(self):
        """Test start fails when not in stopped state"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._state = EngineState.RUNNING

            result = engine.start()

            assert result is False

    def test_start_connection_fails_no_auto_reconnect(self):
        """Test start when connection fails and no auto-reconnect"""
        with patch('trading_engine.Portfolio'):
            config = EngineConfig(auto_reconnect=False)
            engine = TradingEngine(config)
            engine._connection_manager.start = Mock(return_value=False)

            result = engine.start()

            assert result is False
            assert engine.state == EngineState.ERROR

    def test_start_registers_pending_algorithms(self):
        """Test that pending algorithms are registered on start"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._connection_manager.start = Mock(return_value=True)

            algo = create_mock_algorithm()
            engine.add_algorithm(algo)

            engine.start()

            # Pending algorithms should be cleared
            assert len(engine._pending_algorithms) == 0
            # Algorithm should be registered with runner
            engine._runner.register_algorithm.assert_called()

    def test_start_callback_invoked(self):
        """Test on_started callback is invoked"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._connection_manager.start = Mock(return_value=True)

            started = []
            engine.on_started = lambda: started.append(True)

            engine.start()

            assert len(started) == 1

    def test_stop(self):
        """Test stopping the engine"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._state = EngineState.RUNNING

            engine.stop()

            assert engine.state == EngineState.STOPPED
            engine._runner.stop.assert_called()
            engine._data_feed.stop.assert_called()
            engine._connection_manager.stop.assert_called()

    def test_stop_when_already_stopped(self):
        """Test stop when already stopped does nothing"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._state = EngineState.STOPPED

            engine.stop()  # Should not raise

    def test_stop_callback_invoked(self):
        """Test on_stopped callback is invoked"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._state = EngineState.RUNNING

            stopped = []
            engine.on_stopped = lambda: stopped.append(True)

            engine.stop()

            assert len(stopped) == 1


class TestPauseResume:
    """Tests for pause and resume"""

    def test_pause(self):
        """Test pausing the engine"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._state = EngineState.RUNNING

            engine.pause()

            assert engine.state == EngineState.PAUSED
            engine._runner.pause.assert_called()

    def test_pause_when_not_running(self):
        """Test pause when not running does nothing"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._state = EngineState.STOPPED

            engine.pause()

            assert engine.state == EngineState.STOPPED

    def test_resume(self):
        """Test resuming the engine"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._state = EngineState.PAUSED

            engine.resume()

            assert engine.state == EngineState.RUNNING
            engine._runner.resume.assert_called()

    def test_resume_when_not_paused(self):
        """Test resume when not paused does nothing"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._state = EngineState.RUNNING

            engine.resume()

            assert engine.state == EngineState.RUNNING


class TestCallbackRouting:
    """Tests for callback routing"""

    def test_on_tick_callback(self):
        """Test tick callback routing"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()

            ticks = []
            engine.on_tick = lambda s, t: ticks.append((s, t))

            tick = TickData(symbol="SPY", price=450.0, tick_type="LAST")
            engine._on_tick("SPY", tick)

            assert len(ticks) == 1
            assert ticks[0][0] == "SPY"

    def test_on_bar_callback(self):
        """Test bar callback routing"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()

            bars = []
            engine.on_bar = lambda s, b, t: bars.append((s, b, t))

            bar = Bar(
                symbol="SPY",
                timestamp="2024-01-15T10:00:00",
                open=450.0,
                high=451.0,
                low=449.0,
                close=450.5,
                volume=100,
                wap=0.0,
                bar_count=1,
            )
            engine._on_bar("SPY", bar, DataType.BAR_1MIN)

            assert len(bars) == 1
            assert bars[0][0] == "SPY"

    def test_callback_error_handled(self):
        """Test that callback errors are handled"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()

            def bad_callback(s, t):
                raise Exception("Callback error")

            engine.on_tick = bad_callback

            tick = TickData(symbol="SPY", price=450.0, tick_type="LAST")
            # Should not raise
            engine._on_tick("SPY", tick)


class TestGetStatus:
    """Tests for status reporting"""

    def test_get_status(self):
        """Test getting engine status"""
        with patch('trading_engine.Portfolio') as MockPortfolio:
            engine = TradingEngine()
            engine._subscribed_symbols.add("SPY")

            # Mock connection manager status
            engine._connection_manager.get_status = Mock(return_value={
                "state": "connected",
                "connected": True,
            })
            engine._connection_manager.is_connected = True

            # Mock data feed status
            engine._data_feed.get_status = Mock(return_value={"running": True})

            # Mock runner status
            engine._runner.get_status = Mock(return_value={"running": True})

            status = engine.get_status()

            assert "state" in status
            assert "connected" in status
            assert "connection" in status
            assert "data_feed" in status
            assert "runner" in status
            assert "subscribed_symbols" in status
            assert "SPY" in status["subscribed_symbols"]


class TestDataAccess:
    """Tests for data access methods"""

    def test_get_positions(self):
        """Test getting positions"""
        with patch('trading_engine.Portfolio') as MockPortfolio:
            engine = TradingEngine()

            mock_position = Mock()
            mock_position.to_dict = Mock(return_value={"symbol": "SPY", "quantity": 100})
            engine._portfolio.positions = [mock_position]

            positions = engine.get_positions()

            assert len(positions) == 1
            assert positions[0]["symbol"] == "SPY"

    def test_get_bars(self):
        """Test getting bars"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()

            bars = engine.get_bars("SPY", DataType.BAR_1MIN, count=100)

            engine._data_feed.get_bars.assert_called_with(
                "SPY", DataType.BAR_1MIN, count=100
            )

    def test_get_last_price(self):
        """Test getting last price"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._data_feed.get_last_price = Mock(return_value=450.50)

            price = engine.get_last_price("SPY")

            assert price == 450.50


class TestOnConnected:
    """Tests for connection handling"""

    def test_on_connected_loads_portfolio(self):
        """Test portfolio is loaded on connection"""
        with patch('trading_engine.Portfolio'):
            config = EngineConfig(
                load_portfolio_on_start=True,
                fetch_prices_on_start=True,
            )
            engine = TradingEngine(config)

            engine._on_connected()

            engine._portfolio.load.assert_called_with(
                fetch_prices=True,
                fetch_account=True,
            )

    def test_on_connected_starts_data_feed(self):
        """Test data feed is started on connection"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._data_feed.is_running = False

            engine._on_connected()

            engine._data_feed.start.assert_called()

    def test_on_connected_starts_runner(self):
        """Test runner is started on connection"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._runner.is_running = False

            engine._on_connected()

            engine._runner.start.assert_called()


class TestCreateEngine:
    """Tests for create_engine factory function"""

    def test_create_engine_default(self):
        """Test create_engine with defaults"""
        with patch('trading_engine.Portfolio'):
            engine = create_engine()

            assert engine.config.port == 7497
            assert engine.config.order_mode == OrderExecutionMode.DRY_RUN

    def test_create_engine_paper_trading(self):
        """Test create_engine for paper trading"""
        with patch('trading_engine.Portfolio'):
            engine = create_engine(port=7497, order_mode="dry_run")

            assert engine.config.port == 7497
            assert engine.config.order_mode == OrderExecutionMode.DRY_RUN

    def test_create_engine_live_trading(self):
        """Test create_engine for live trading"""
        with patch('trading_engine.Portfolio'):
            engine = create_engine(port=7496, order_mode="immediate")

            assert engine.config.port == 7496
            assert engine.config.order_mode == OrderExecutionMode.IMMEDIATE

    def test_create_engine_with_kwargs(self):
        """Test create_engine with additional kwargs"""
        with patch('trading_engine.Portfolio'):
            engine = create_engine(
                port=4002,
                order_mode="queued",
                keepalive_interval=60.0,
            )

            assert engine.config.port == 4002
            assert engine.config.order_mode == OrderExecutionMode.QUEUED
            assert engine.config.keepalive_interval == 60.0


class TestRunForever:
    """Tests for run_forever functionality"""

    def test_run_forever_blocks_until_shutdown(self):
        """Test run_forever blocks until shutdown event"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()

            # Start a thread to stop the engine
            def stop_after_delay():
                time.sleep(0.1)
                engine._shutdown_event.set()

            stop_thread = threading.Thread(target=stop_after_delay)
            stop_thread.start()

            # This should return when shutdown event is set
            engine.run_forever(handle_signals=False)

            stop_thread.join()

    def test_run_forever_signal_handler(self):
        """Test signal handler requires multiple signals"""
        with patch('trading_engine.Portfolio'):
            with patch('signal.signal'):
                engine = TradingEngine()

                # Set up the signal handler
                def stop_engine():
                    time.sleep(0.1)
                    engine._shutdown_event.set()

                stop_thread = threading.Thread(target=stop_engine)
                stop_thread.start()

                engine.run_forever(handle_signals=True, required_signals=3)

                stop_thread.join()


class TestErrorHandling:
    """Tests for error handling"""

    def test_on_data_error(self):
        """Test data feed error handling"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()

            errors = []
            engine.on_error = lambda e: errors.append(e)

            error = Exception("Data error")
            engine._on_data_error("SPY", error)

            assert len(errors) == 1

    def test_on_runner_error(self):
        """Test runner error handling"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()

            errors = []
            engine.on_error = lambda e: errors.append(e)

            error = Exception("Runner error")
            engine._on_runner_error("test_algo", error)

            assert len(errors) == 1


class TestResubscribeInstruments:
    """Tests for instrument resubscription"""

    def test_resubscribe_on_reconnect(self):
        """Test instruments are resubscribed on reconnect"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._subscribed_symbols.add("SPY")

            # Mock portfolio position
            mock_pos = Mock()
            mock_pos.contract = Contract()
            engine._portfolio.get_position = Mock(return_value=mock_pos)

            engine._resubscribe_instruments()

            # Should have called subscribe on data feed
            engine._data_feed.subscribe.assert_called()


class TestSubscribeAlgorithmInstruments:
    """Tests for algorithm instrument subscription"""

    def test_subscribe_algorithm_instruments(self):
        """Test subscribing to algorithm instruments"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            algo = create_mock_algorithm()

            engine._subscribe_algorithm_instruments(algo)

            # Should subscribe with algorithm name as subscriber
            call_args = engine._data_feed.subscribe.call_args
            assert call_args[1]["subscriber"] == algo.name
            assert "SPY" in engine._subscribed_symbols
