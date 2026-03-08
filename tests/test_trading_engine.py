"""
Tests for trading_engine.py - Unified trading engine
"""

import asyncio
import pytest
import time
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch, PropertyMock, AsyncMock

from ibapi.contract import Contract

from trading_engine import (
    TradingEngine,
    EngineConfig,
    EngineState,
    create_engine,
)
from plugin_executive import ExecutionMode, OrderExecutionMode
from data_feed import DataType, TickData
from models import Bar


@pytest.fixture
def engine():
    """Create a TradingEngine with default config for testing"""
    with patch('trading_engine.Portfolio'):
        yield TradingEngine()


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
        assert config.enable_message_bus is True

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

    def test_default_initialization(self, engine):
        """Test default initialization"""
        assert engine.state == EngineState.STOPPED
        assert engine.is_running is False
        assert engine.config is not None

    def test_custom_config(self):
        """Test initialization with custom config"""
        with patch('trading_engine.Portfolio'):
            config = EngineConfig(port=4002)
            engine = TradingEngine(config)

            assert engine.config.port == 4002

    def test_callbacks_initialized_none(self, engine):
        """Test callbacks are initialized to None"""
        assert engine.on_started is None
        assert engine.on_stopped is None
        assert engine.on_error is None
        assert engine.on_signal is None
        assert engine.on_execution is None
        assert engine.on_tick is None
        assert engine.on_bar is None


class TestTradingEngineProperties:
    """Tests for TradingEngine properties"""

    def test_state_property(self, engine):
        """Test state property"""
        assert engine.state == EngineState.STOPPED

        engine._state = EngineState.RUNNING
        assert engine.state == EngineState.RUNNING

    def test_is_running_property(self, engine):
        """Test is_running property"""
        assert engine.is_running is False

        engine._state = EngineState.RUNNING
        assert engine.is_running is True

        engine._state = EngineState.PAUSED
        assert engine.is_running is False

    def test_portfolio_property(self, engine):
        """Test portfolio property"""
        assert engine.portfolio is engine._portfolio

    def test_data_feed_property(self, engine):
        """Test data_feed property"""
        assert engine.data_feed is engine._data_feed

class TestSubscription:
    """Tests for subscription management"""

    def test_subscribe(self, engine):
        """Test subscribing to a symbol"""
        contract = Contract()
        contract.symbol = "SPY"

        engine.subscribe("SPY", contract)

        assert "SPY" in engine._subscribed_symbols

    def test_subscribe_with_data_types(self, engine):
        """Test subscribing with specific data types"""
        engine._data_feed = Mock()
        contract = Contract()

        engine.subscribe("SPY", contract, {DataType.TICK, DataType.BAR_5MIN})

        engine._data_feed.subscribe.assert_called_once()

    def test_unsubscribe(self, engine):
        """Test unsubscribing from a symbol"""
        engine._subscribed_symbols.add("SPY")

        engine.unsubscribe("SPY")

        assert "SPY" not in engine._subscribed_symbols


class TestStartStop:
    """Tests for starting and stopping the engine"""

    async def test_start_when_stopped(self, engine):
        """Test starting engine when stopped"""
        engine._connection_manager.start = AsyncMock(return_value=True)

        result = await engine.start()

        assert result is True
        assert engine.state == EngineState.RUNNING

    async def test_start_when_not_stopped(self, engine):
        """Test start fails when not in stopped state"""
        engine._state = EngineState.RUNNING

        result = await engine.start()

        assert result is False

    async def test_start_connection_fails_no_auto_reconnect(self):
        """Test start when connection fails and no auto-reconnect"""
        with patch('trading_engine.Portfolio'):
            config = EngineConfig(auto_reconnect=False)
            engine = TradingEngine(config)
            engine._connection_manager.start = AsyncMock(return_value=False)

            result = await engine.start()

            assert result is False
            assert engine.state == EngineState.ERROR

    async def test_start_registers_pending_plugins(self):
        """Test that pending plugins are registered on start"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._connection_manager.start = AsyncMock(return_value=True)
            engine._plugin_executive = Mock()
            engine._data_feed = Mock()

            plugin = Mock()
            plugin.name = "test_plugin"
            plugin.is_loaded = True
            plugin.enabled_instruments = []
            engine._message_bus = None
            engine.add_plugin(plugin)

            await engine.start()

            # Pending plugins should be cleared
            assert len(engine._pending_plugins) == 0
            # Plugin should be registered with plugin executive
            engine._plugin_executive.register_plugin.assert_called_once()

    async def test_start_callback_invoked(self, engine):
        """Test on_started callback is invoked"""
        engine._connection_manager.start = AsyncMock(return_value=True)

        started = []
        engine.on_started = lambda: started.append(True)

        await engine.start()

        assert len(started) == 1

    async def test_stop(self):
        """Test stopping the engine"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._state = EngineState.RUNNING
            engine._plugin_executive = Mock()
            engine._plugin_executive.stop = AsyncMock()
            engine._data_feed = Mock()
            engine._connection_manager = Mock()
            engine._connection_manager.stop = AsyncMock()

            await engine.stop()

            assert engine.state == EngineState.STOPPED
            engine._plugin_executive.stop.assert_called_once()
            engine._data_feed.stop.assert_called_once()
            engine._connection_manager.stop.assert_called_once()

    async def test_stop_when_already_stopped(self, engine):
        """Test stop when already stopped does nothing"""
        engine._state = EngineState.STOPPED

        await engine.stop()  # Should not raise

    async def test_stop_callback_invoked(self, engine):
        """Test on_stopped callback is invoked"""
        engine._state = EngineState.RUNNING
        engine._plugin_executive = Mock()
        engine._plugin_executive.stop = AsyncMock()
        engine._connection_manager.stop = AsyncMock()
        engine._data_feed = Mock()

        stopped = []
        engine.on_stopped = lambda: stopped.append(True)

        await engine.stop()

        assert len(stopped) == 1


class TestPauseResume:
    """Tests for pause and resume"""

    def test_pause(self):
        """Test pausing the engine"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._state = EngineState.RUNNING
            engine._plugin_executive = Mock()

            engine.pause()

            assert engine.state == EngineState.PAUSED
            engine._plugin_executive.pause.assert_called_once()

    def test_pause_when_not_running(self, engine):
        """Test pause when not running does nothing"""
        engine._state = EngineState.STOPPED

        engine.pause()

        assert engine.state == EngineState.STOPPED

    def test_resume(self):
        """Test resuming the engine"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._state = EngineState.PAUSED
            engine._plugin_executive = Mock()

            engine.resume()

            assert engine.state == EngineState.RUNNING
            engine._plugin_executive.resume.assert_called_once()

    def test_resume_when_not_paused(self, engine):
        """Test resume when not paused does nothing"""
        engine._state = EngineState.RUNNING

        engine.resume()

        assert engine.state == EngineState.RUNNING


class TestCallbackRouting:
    """Tests for callback routing"""

    def test_on_tick_callback(self, engine):
        """Test tick callback routing"""
        ticks = []
        engine.on_tick = lambda s, t: ticks.append((s, t))

        tick = TickData(symbol="SPY", price=450.0, tick_type="LAST")
        engine._on_tick("SPY", tick)

        assert len(ticks) == 1
        assert ticks[0][0] == "SPY"

    def test_on_bar_callback(self, engine):
        """Test bar callback routing"""
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

    def test_callback_error_handled(self, engine):
        """Test that callback errors are handled"""
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

            # Replace components with mocks
            engine._connection_manager = Mock()
            engine._connection_manager.get_status = Mock(return_value={
                "state": "connected",
                "connected": True,
            })
            engine._connection_manager.is_connected = True

            engine._data_feed = Mock()
            engine._data_feed.get_status = Mock(return_value={"running": True})

            engine._plugin_executive = Mock()
            engine._plugin_executive.get_status = Mock(return_value={"running": True})

            status = engine.get_status()

            assert status["state"] == "stopped"
            assert status["connected"] is True
            assert "connection" in status
            assert "data_feed" in status
            assert "plugin_executive" in status
            assert status["subscribed_symbols"] == ["SPY"]


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
            engine._data_feed = Mock()

            bars = engine.get_bars("SPY", DataType.BAR_1MIN, count=100)

            engine._data_feed.get_bars.assert_called_with(
                "SPY", DataType.BAR_1MIN, count=100
            )

    def test_get_last_price(self, engine):
        """Test getting last price"""
        engine._data_feed.get_last_price = Mock(return_value=450.50)

        price = engine.get_last_price("SPY")

        assert price == 450.50


class TestOnConnected:
    """Tests for connection handling"""

    async def test_on_connected_loads_portfolio(self):
        """Test portfolio is loaded on connection"""
        with patch('trading_engine.Portfolio'):
            config = EngineConfig(
                load_portfolio_on_start=True,
                fetch_prices_on_start=True,
            )
            engine = TradingEngine(config)
            engine._portfolio.load = AsyncMock()
            engine._detect_market_data_type = AsyncMock(return_value=1)

            engine._on_connected()
            await asyncio.sleep(0)  # let the task run

            engine._portfolio.load.assert_called_with(
                fetch_prices=True,
                fetch_account=True,
            )

    async def test_on_connected_starts_data_feed(self):
        """Test data feed is started on connection"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._data_feed = Mock()
            engine._data_feed.is_running = False
            engine._plugin_executive = Mock()
            engine._plugin_executive.is_running = False
            engine._plugin_executive.start = AsyncMock()
            engine._portfolio.load = AsyncMock()
            engine._detect_market_data_type = AsyncMock(return_value=1)

            engine._on_connected()
            await asyncio.sleep(0)  # let the task run

            engine._data_feed.start.assert_called_once()

    async def test_on_connected_starts_plugin_executive(self):
        """Test plugin executive is started on connection"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()
            engine._data_feed = Mock()
            engine._data_feed.is_running = False
            engine._plugin_executive = Mock()
            engine._plugin_executive.is_running = False
            engine._plugin_executive.start = AsyncMock()
            engine._portfolio.load = AsyncMock()
            engine._detect_market_data_type = AsyncMock(return_value=1)

            engine._on_connected()
            await asyncio.sleep(0)  # let the task run

            engine._plugin_executive.start.assert_called_once()


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

    async def test_run_forever_blocks_until_shutdown(self):
        """Test run_forever blocks until shutdown event"""
        with patch('trading_engine.Portfolio'):
            engine = TradingEngine()

            async def stop_after_delay():
                await asyncio.sleep(0.05)
                engine._shutdown_event.set()

            asyncio.create_task(stop_after_delay())

            # This should return when shutdown event is set
            await engine.run_forever(handle_signals=False)

    async def test_run_forever_signal_handler(self):
        """Test signal handler can be installed (signals patched)"""
        with patch('trading_engine.Portfolio'):
            with patch('signal.signal'):
                engine = TradingEngine()

                async def stop_after_delay():
                    await asyncio.sleep(0.05)
                    engine._shutdown_event.set()

                asyncio.create_task(stop_after_delay())

                await engine.run_forever(handle_signals=True, required_signals=3)


class TestErrorHandling:
    """Tests for error handling"""

    def test_on_data_error(self, engine):
        """Test data feed error handling"""
        errors = []
        engine.on_error = lambda e: errors.append(e)

        error = Exception("Data error")
        engine._on_data_error("SPY", error)

        assert len(errors) == 1

    def test_on_runner_error(self, engine):
        """Test runner error handling"""
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
            engine._data_feed = Mock()

            # Mock portfolio position
            mock_pos = Mock()
            mock_pos.contract = Contract()
            engine._portfolio.get_position = Mock(return_value=mock_pos)

            engine._resubscribe_instruments()

            # Should have called subscribe on data feed
            engine._data_feed.subscribe.assert_called_once()


