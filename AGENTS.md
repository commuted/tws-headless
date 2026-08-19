# AGENTS.md - TWS Headless Trading System

This file provides an overview of the TWS Headless trading system architecture, components, and development guidelines for AI agents working in this codebase.

## System Overview

**Purpose**: Production algorithmic trading system for Interactive Brokers with portfolio management and plugin architecture.

## Architecture

```
IB TWS/Gateway (Port 7497)
    ↓
Portfolio (extends IBClient) - positions, account data, market data
    ↓
DataFeed - streams ticks, aggregates into bars (5sec, 1min, 5min, etc.)
    ↓
PluginExecutive - feeds data to plugins, manages lifecycle
    ↓
Trading Plugins - calculate TradeSignals
    ↓
OrderReconciler → RateLimiter → OrderBuilder → IBClient (place orders)
    ↓
ExecutionDB - logs all trades and commissions
```

## Core Components

### Portfolio (`ib/portfolio.py`)
- Extends IBClient for IB API communication
- Tracks positions and account data
- Streams real-time market data
- Manages contract subscriptions

### DataFeed (`ib/data_feed.py`)
- Real-time tick and bar streaming
- Circular buffers for historical data
- Multiple timeframes (5sec, 1min, 5min, 15min, 1hour, 1day)
- Tick-by-tick data support
- Market depth (Level 2) support

### PluginExecutive (`ib/plugin_executive.py`)
- Plugin lifecycle management (LOADED → STARTED → FROZEN → STOPPED)
- Feeds real-time data to plugins
- Executes trade signals
- Circuit breaker fault tolerance
- MessageBus integration for inter-plugin communication

### TradingEngine (`ib/trading_engine.py`)
- Unified interface combining all components
- ConnectionManager for IB API connection
- Coordinates Portfolio, DataFeed, and PluginExecutive

### CommandServer (`ib/command_server.py`)
- Unix socket interface for external control
- Supports status queries, position management, plugin control
- Used by `ibctl.py` command-line client

### MessageBus (`ib/message_bus.py`)
- Pub/Sub system for inter-plugin communication
- Plugins can publish indicators and subscribe to others
- Enables plugin composition and data sharing

### OrderReconciler (`ib/order_reconciler.py`)
- Reconciles trade signals into executable orders
- Handles position sizing and risk management
- Supports multiple reconciliation modes (IMMEDIATE, QUEUED, DRY_RUN)

### RateLimiter (`ib/rate_limiter.py`)
- Token bucket algorithm for order rate limiting
- Default: 10 orders/second
- Ensures IB API compliance

### ExecutionDB (`ib/execution_db.py`)
- SQLite database for trade logging
- Records all executions and commissions
- Provides trade history and performance analytics

## Plugin System

### Base Class (`plugins/base.py`)

All plugins extend `PluginBase` and implement:

```python
class MyPlugin(PluginBase):
    def on_start(self) -> PluginResult:
        """Called when plugin starts"""
        
    def on_bar(self, symbol: str, bar: Bar) -> PluginResult:
        """Called on new bar data"""
        
    def on_tick(self, symbol: str, tick: TickData) -> PluginResult:
        """Called on new tick data"""
        
    def on_stop(self) -> PluginResult:
        """Called when plugin stops"""
```

### Plugin Lifecycle States

- **LOADED**: Plugin loaded but not started
- **STARTED**: Plugin actively processing data
- **FROZEN**: Plugin paused (no data processing)
- **STOPPED**: Plugin stopped and cleaned up

### Plugin Directory Structure

Each plugin gets an isolated directory:
```
plugins/my_plugin/
├── __init__.py
├── plugin.py          # Main plugin class
├── state.json         # Persistent state
├── holdings.json      # Position tracking
└── instruments.json   # Subscribed instruments
```

### State Persistence

Plugins automatically save/load state:
- `state.json` - Custom plugin state
- `holdings.json` - Position tracking
- `instruments.json` - Subscribed symbols

### Circuit Breaker

Plugins have built-in fault tolerance:
- Auto-disable after repeated failures
- Half-open state for recovery attempts
- Prevents cascading failures

## Active Plugins

### gld_usd_swap
Gold/USD pair trading strategy with multiple backtesting scripts.

### momentum_5day
5-day momentum strategy for equity trading.

### orders
Order management plugin for manual trade execution.

### panic
Emergency liquidation plugin for risk management.

### paper_tests
Paper trading test suite for strategy validation.

### test_plugin
Development template for new plugins.

### unassigned
Container for unallocated trading strategies.

## Commands

### Trading Engine

```bash
# Start engine in dry run mode (no real orders)
python3 -m ib.run_engine --port 7497 --mode dry_run

# Start with plugins enabled
python3 -m ib.run_engine --port 7497 --mode dry_run --plugins

# Start in live mode (real orders)
python3 -m ib.run_engine --port 7497 --mode immediate --plugins
```

### Command-Line Control (ibctl.py)

```bash
# System status
./ibctl.py status

# Position management
./ibctl.py positions
./ibctl.py buy SPY 10 --confirm
./ibctl.py sell AAPL 5 --confirm

# Plugin management
./ibctl.py plugin list
./ibctl.py plugin start my_plugin
./ibctl.py plugin stop my_plugin
./ibctl.py plugin freeze my_plugin
./ibctl.py plugin resume my_plugin
```

### Testing

```bash
# Run all tests
python3 -m pytest tests/ -v

# Run specific test file
python3 -m pytest tests/test_portfolio.py -v

# Run single test
python3 -m pytest tests/test_portfolio.py::TestPortfolio::test_method_name -v

# Run with coverage
python3 -m pytest tests/ --cov=ib --cov-report=html
```

## Safety Features

### Rate Limiting
- Token bucket algorithm (default 10 orders/sec)
- Configurable per-plugin limits
- Prevents IB API throttling

### Circuit Breaker
- Auto-disable failing plugins
- Configurable failure thresholds
- Half-open state for recovery

### Order Modes
- **DRY_RUN**: Simulate orders without execution
- **IMMEDIATE**: Execute orders immediately
- **QUEUED**: Batch orders for execution

### Graceful Shutdown
- Requires 3 Ctrl+C within 10 seconds
- Allows plugins to clean up state
- Prevents data loss

## IB API Specifics

### Contract Types
- **STK**: Stocks
- **OPT**: Options
- **FUT**: Futures
- **CASH**: Forex
- **BOND**: Bonds

### Order Types
- Market
- Limit
- Stop
- Stop-Limit
- Trailing Stop
- MOC (Market on Close)
- LOC (Limit on Close)
- Bracket orders

### Tick Types
See `ib/const.py` for complete list:
- LAST
- BID
- ASK
- CLOSE
- DELAYED_LAST
- And many more...

## Test Infrastructure

### Test Organization
- 31 test files in `tests/` directory
- `conftest.py` mocks the `ibapi` module
- Module aliases for testing without IB connection

### Key Test Files
- `test_portfolio.py` - Portfolio and position tracking
- `test_data_feed.py` - Market data streaming
- `test_plugin_executive.py` - Plugin lifecycle
- `test_order_reconciler.py` - Order reconciliation
- `test_trading_engine.py` - Full system integration

### Test Timeouts
- Portfolio tests: 30-second timeout (position loading waits)
- Other tests: Standard pytest timeouts

## Development Guidelines

### Adding New Plugins

1. Create plugin directory in `plugins/`
2. Extend `PluginBase` in `plugin.py`
3. Implement lifecycle methods (`on_start`, `on_bar`, `on_stop`)
4. Add tests in `tests/test_my_plugin.py`
5. Document strategy in plugin README

### Modifying Core Components

1. Consult `CLAUDE.md` for architecture details
2. All changes require tests
3. Use `conftest.py` mocks for IB API
4. Follow circuit breaker pattern
5. Maintain backward compatibility

### Code Style

- Type hints for all function signatures
- Docstrings for public methods
- Logging for important events
- Error handling with circuit breaker

### Testing Requirements

- Unit tests for new functionality
- Integration tests for component interactions
- Mock IB API using `conftest.py`
- Maintain >80% code coverage

## File Organization

- **AGENTS.md** (this file) - System overview for AI agents
- **CLAUDE.md** - Detailed development guide
- **README.md** - User-facing documentation
- **PLUGIN_MANUAL.md** - Plugin development guide
- **ib/** - Core trading engine
- **plugins/** - Trading strategy plugins
- **tests/** - Test suite

## Version Information

- TWS Headless: Production system (no semantic versioning yet)
- Python: 3.12+
- IB API: ibapi (latest)
- Key dependencies: asyncio, pytest, peewee (SQLite ORM)

## Notes for AI Agents

When working in this codebase:

1. **Always consult CLAUDE.md** for detailed architecture
2. **Test everything** - Use pytest with mocks
3. **Follow plugin patterns** - Extend PluginBase correctly
4. **Respect safety features** - Don't bypass rate limiting or circuit breakers
5. **State preservation** - Plugins rely on JSON state files
6. **IB API quirks** - Check `conftest.py` for mocking patterns
7. **Async patterns** - System uses asyncio extensively
8. **Error handling** - Use circuit breaker pattern for fault tolerance

## Common Tasks

### Create New Plugin
1. Copy `plugins/test_plugin/` as template
2. Modify `plugin.py` with strategy logic
3. Add tests in `tests/test_my_plugin.py`
4. Update `instruments.json` with symbols
5. Test in dry run mode first

### Debug Plugin Issues
1. Check plugin logs in console output
2. Verify state files (`state.json`, `holdings.json`)
3. Use `ibctl.py plugin status my_plugin`
4. Check circuit breaker state
5. Review execution logs in ExecutionDB

### Add New Market Data Type
1. Extend `DataFeed` with new data type
2. Add callback in `PluginBase`
3. Update `PluginExecutive` to route data
4. Add tests for new data type
5. Document in CLAUDE.md

### Modify Order Reconciliation
1. Update `OrderReconciler` logic
2. Add tests in `test_order_reconciler.py`
3. Verify rate limiting still works
4. Test in dry run mode
5. Document changes in CLAUDE.md

This document provides a high-level overview. For detailed implementation guidance, always consult `CLAUDE.md` and the relevant source files.
