# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TWS Headless is an Interactive Brokers trading system with portfolio management, algorithmic trading, and a plugin architecture. It connects to IB TWS/Gateway via the ibapi Python library.

## Commands

```bash
# Run all tests
python3 -m pytest tests/ -v

# Run specific test file
python3 -m pytest tests/test_portfolio.py -v

# Run single test
python3 -m pytest tests/test_portfolio.py::TestPortfolio::test_method_name -v

# Start trading engine (dry run - no real orders)
python3 -m ib.run_engine --port 7497 --mode dry_run

# Start with plugins enabled
python3 -m ib.run_engine --port 7497 --mode dry_run --plugins

# Command-line client (talks to running engine via socket)
./ibctl.py status
./ibctl.py positions
./ibctl.py buy SPY 10 --confirm
./ibctl.py plugin list
```

## Architecture

### Core Data Flow

```
IB TWS/Gateway (Port 7497)
    ↓
Portfolio (extends IBClient) - positions, account data, market data
    ↓
DataFeed - streams ticks, aggregates into bars (5sec, 1min, 5min, etc.)
    ↓
AlgorithmRunner / PluginExecutive - feeds data to algorithms/plugins
    ↓
Algorithms/Plugins - calculate TradeSignals
    ↓
OrderReconciler → RateLimiter → OrderBuilder → IBClient (place orders)
    ↓
ExecutionDB - logs all trades and commissions
```

### Key Components

| Component | File(s) | Purpose |
|-----------|---------|---------|
| **Portfolio** | `portfolio.py` | Extends IBClient; loads positions, streams prices, tracks account |
| **DataFeed** | `data_feed.py` | Real-time tick/bar streaming with circular buffers |
| **AlgorithmRunner** | `algorithm_runner.py` | Executes algorithms on bar/tick events; circuit breaker for fault tolerance |
| **PluginExecutive** | `plugin_executive.py` | Plugin lifecycle (start/stop/freeze/resume); state persistence |
| **PluginBase** | `plugins/base.py` | Base class; `plugin_dir` property gives each plugin its own directory for state.json, holdings.json, instruments.json |
| **TradingEngine** | `trading_engine.py` | Unified interface combining ConnectionManager + DataFeed + Runner |
| **CommandServer** | `command_server.py` | Unix socket interface for external control |
| **MessageBus** | `message_bus.py` | Pub/Sub for inter-plugin communication |

### Two Execution Systems

1. **Algorithms** (`algorithms/`): Legacy system using `AlgorithmBase`, managed by `AlgorithmRunner`
2. **Plugins** (`plugins/`): Newer system using `PluginBase`, managed by `PluginExecutive`, with lifecycle states (LOADED, STARTED, FROZEN, STOPPED)

### Entry Points

- `main.py` - Portfolio snapshot and rebalancing CLI
- `run_engine.py` - Full trading engine with socket control
- `ibctl.py` - Standalone command-line client (no package imports)

## Key Patterns

- **Circuit Breaker**: Algorithms auto-disable after repeated failures, recover via half-open state
- **Rate Limiting**: Token bucket algorithm (default 10 orders/sec) for IB API compliance
- **State Persistence**: Plugin state saved to JSON files for recovery
- **Order Modes**: `DRY_RUN` (simulate), `IMMEDIATE` (execute now), `QUEUED` (batch)
- **Graceful Shutdown**: Requires 3 Ctrl+C within 10 seconds

## IB API Specifics

- **Contract types**: STK (stocks), OPT (options), FUT (futures), CASH (forex), BOND
- **Order types**: Market, Limit, Stop, Stop-Limit, Trailing Stop, MOC, LOC, bracket orders
- **Tick types**: LAST, BID, ASK, CLOSE, DELAYED_LAST, etc. (see `const.py`)
- `ibapi` module is mocked in tests via `tests/conftest.py`

## Test Infrastructure

- Tests are in `tests/` directory (31 files)
- `conftest.py` mocks the `ibapi` module and sets up module aliases
- Portfolio tests have 30-second timeouts due to position loading waits
