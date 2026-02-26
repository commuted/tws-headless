# Plan: Migrate IB Interface to asyncio (Drop Sync Threading Model)

## Context for Executing This Plan

**Working directory**: `/home/ron/claude/ib`
**Authoritative ibapi source**: `/home/ron/claude/pythonclient/ibapi`
**Run tests**: `source env_claude/bin/activate && python3 -m pytest tests/ -v`

The ibapi library (`/home/ron/claude/pythonclient/ibapi`) uses a synchronous threading
model: an `EReader` daemon thread reads socket bytes into a `queue.Queue`, and
`EClient.run()` is a blocking loop that dequeues messages and calls `EWrapper` callbacks.

The project wraps this with `threading.Event` objects to turn async callbacks into
blocking waits (e.g. `_positions_done.wait(timeout=30)`), and spawns ~6 daemon threads
for keepalive, order execution, plugin execution, and health monitoring.

The "sync wrapper" being dropped is the entire pattern:
- `EReader` thread + `queue.Queue` + `EClient.run()` blocking loop
- `threading.Event` waits throughout `portfolio.py`, `connection_manager.py`, `trading_engine.py`
- `threading.Thread` in `connection_manager.py`, `plugin_executive.py`, `trading_engine.py`

---

## Benefits / Costs Assessment

### Benefits

| Benefit | Impact |
|---------|--------|
| **Parallel IB requests** | `asyncio.gather()` lets position load + account summary run simultaneously; currently they are sequential with 30-second waits each | HIGH |
| **Simpler concurrency** | Six daemon threads collapse to one event loop; no thread-safety bugs, no lock contention | HIGH |
| **Cleaner shutdown** | `task.cancel()` with structured cleanup replaces `Thread.join(timeout=5)` guesswork | MEDIUM |
| **Better error propagation** | Exceptions propagate through `await` chains; currently threads die silently | MEDIUM |
| **Lower overhead** | Coroutines are cheaper than OS threads; relevant when many plugins are loaded | LOW |
| **Test clarity** | `pytest-asyncio` fixtures compose better than mock thread coordination | MEDIUM |

### Costs

| Cost | Impact |
|------|--------|
| **Large migration** | ~15 files, ~1 500 lines changed; every file that blocks on an Event must become async | HIGH |
| **Viral `async`** | Adding `async`/`await` to any method forces all callers to also be async, propagating up the call stack | HIGH |
| **Plugin API change** | `PluginBase.on_bar()`, `on_tick()`, `on_message()` must become async or be wrapped in `run_in_executor` | HIGH |
| **Test migration** | All async-touching tests need `@pytest.mark.asyncio` and `AsyncMock`; conftest.py needs `pytest-asyncio` | MEDIUM |
| **Custom transport layer** | Must write async socket reader to replace `EReader` + `EClient.run()` (~150 lines) | MEDIUM |
| **"Don't block the loop" discipline** | Any plugin doing blocking I/O (e.g. reading a file, calling a REST API) will freeze all callbacks | MEDIUM |
| **Execution DB** | `ib/execution_db.py` uses synchronous SQLite; needs `aiosqlite` or `run_in_executor` | LOW |
| **Reconnection complexity** | `ConnectionManager` reconnect state machine is already subtle; async version requires careful task cancellation | MEDIUM |

### Verdict

The migration is **worth doing** if the project will be actively developed. The benefits
are architectural (parallel requests, simpler threading model) and the costs are one-time
refactoring effort. The biggest risk is the plugin API change cascading to every plugin
in `plugins/`. If the plugin count is small and plugins are under your control, do the
full migration. If plugins are third-party or numerous, use the bridge approach in Phase 0.

---

## Architecture Decision: New Async Transport

The ibapi decoder (`ibapi/decoder.py`) and wire encoding (`ibapi/comm.py`) are pure
parsing logic — no threading, no I/O. They can be reused unchanged.

The plan is to **replace only the transport layer** (connection, reader, run loop) with
an asyncio equivalent, leaving the decoder and wrapper interfaces intact:

```
Before:                              After:
  EClient.connect()                    AsyncIBTransport.connect()  (asyncio socket)
  EReader Thread → queue.Queue         asyncio coroutine → asyncio.Queue
  EClient.run() blocking loop          AsyncIBTransport.run() coroutine
  ibapi.decoder.Decoder (unchanged) ── ibapi.decoder.Decoder (reused as-is)
  EWrapper callbacks (unchanged) ───── EWrapper callbacks (unchanged)
```

New file: `ib/async_transport.py` (~180 lines) replaces the thread+queue+run-loop.
`IBClient` will stop inheriting from `EClient` and instead compose `AsyncIBTransport`.

---

## Prerequisites

```bash
# Add to requirements / pyproject.toml:
pip install aiosqlite          # async SQLite for execution_db.py
pip install pytest-asyncio     # async test support

# In pytest.ini or pyproject.toml:
# [tool.pytest.ini_options]
# asyncio_mode = "auto"
```

---

## Phase 1 — New Async Transport Layer

### 1.1  Create `ib/async_transport.py`

This file replaces `ibapi.client.EClient` + `ibapi.reader.EReader` + `ibapi.connection.Connection`.

```python
"""
async_transport.py - Asyncio-native IB socket transport

Replaces EClient + EReader + Connection with a single coroutine-based
transport that reads length-prefixed messages from the IB socket and
dispatches them to an EWrapper via the ibapi Decoder.
"""
import asyncio
import logging
import struct
from typing import Optional

from ibapi import comm
from ibapi.decoder import Decoder
from ibapi.wrapper import EWrapper

logger = logging.getLogger(__name__)

_MAX_MSG_LEN = 0xFFFFFF  # 16 MB safety cap


class AsyncIBTransport:
    """
    Asyncio socket transport for the IB TWS/Gateway API.

    Connects to TWS, sends API requests (unchanged wire format),
    reads length-framed responses, and dispatches them to an EWrapper.

    Usage:
        transport = AsyncIBTransport(wrapper=my_wrapper)
        await transport.connect("127.0.0.1", 7497, client_id=1)
        asyncio.create_task(transport.run())  # starts message loop
    """

    API_SIGN = b"API\0"
    MIN_SERVER_VER = 100
    MAX_SERVER_VER = 178  # match ibapi version

    def __init__(self, wrapper: EWrapper):
        self.wrapper = wrapper
        self.serverVersion: Optional[int] = None
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected = False
        self._decoder: Optional[Decoder] = None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    async def connect(self, host: str, port: int, client_id: int) -> None:
        """Open connection and perform TWS handshake."""
        self._reader, self._writer = await asyncio.open_connection(host, port)
        # Send API signature + version range
        v_min, v_max = self.MIN_SERVER_VER, self.MAX_SERVER_VER
        prefix = self.API_SIGN + comm.make_field(f"v{v_min}..{v_max}")
        self._send_raw(prefix)
        # Receive server version + connection time
        msg = await self._recv_msg()
        fields = comm.read_fields(msg)
        self.serverVersion = int(fields[0])
        # Send startAPI
        flds = comm.make_field(71)         # START_API msg id
        flds += comm.make_field(2)         # version
        flds += comm.make_field(client_id)
        flds += comm.make_field("")        # optional capabilities
        self._send_framed(flds)
        self._decoder = Decoder(self.wrapper, self.serverVersion)
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False
        if self._writer:
            self._writer.close()

    def isConnected(self) -> bool:
        return self._connected

    # ------------------------------------------------------------------
    # Message Loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Main async message loop.

        Reads length-framed messages from the socket and dispatches each
        to the Decoder (which calls EWrapper methods synchronously).
        Run this as an asyncio Task: asyncio.create_task(transport.run())
        """
        try:
            while self._connected:
                try:
                    msg = await asyncio.wait_for(self._recv_msg(), timeout=5.0)
                except asyncio.TimeoutError:
                    await self._msg_loop_timeout()
                    continue
                if msg:
                    fields = comm.read_fields(msg)
                    if fields:
                        self._decoder.interpret(fields)
        except (asyncio.IncompleteReadError, ConnectionResetError):
            logger.warning("IB connection closed by remote")
        finally:
            self._connected = False
            self.wrapper.connectionClosed()

    async def _msg_loop_timeout(self) -> None:
        """Called when no message arrives within the poll interval."""
        pass  # hook for keepalive logic if needed

    # ------------------------------------------------------------------
    # Send helpers (synchronous — just write to buffer)
    # ------------------------------------------------------------------

    def send_msg(self, msg: bytes) -> None:
        """Send a pre-encoded message (called by all req* methods)."""
        if not self._writer:
            raise ConnectionError("Not connected")
        self._send_framed(msg)

    def _send_framed(self, data: bytes) -> None:
        size = struct.pack("!I", len(data))
        self._send_raw(size + data)

    def _send_raw(self, data: bytes) -> None:
        self._writer.write(data)
        # No await needed for small messages; asyncio buffers until drain

    # ------------------------------------------------------------------
    # Receive helpers
    # ------------------------------------------------------------------

    async def _recv_msg(self) -> bytes:
        """Read one length-framed message from the socket."""
        header = await self._reader.readexactly(4)
        size = struct.unpack("!I", header)[0]
        if size > _MAX_MSG_LEN:
            raise ValueError(f"Oversized IB message: {size} bytes")
        return await self._reader.readexactly(size)
```

**Note**: All `req*` methods from `EClient` (e.g. `reqPositions`, `reqMktData`) just
build a byte string and call `sendMsg`. Create `ib/ib_request_mixin.py` that copies
those methods verbatim from `ibapi/client.py`, replacing `self.conn.sendMsg(msg)` with
`self._transport.send_msg(msg)`. There are ~80 request methods; copy them once.

---

## Phase 2 — Refactor `ib/client.py` (IBClient)

**Current**: `class IBClient(EWrapper, EClient)` — inherits from both; spawns a thread.

**New**:
```python
class IBClient(EWrapper, IBRequestMixin):
    """
    Async IB client: asyncio transport + all EClient request methods.
    """
    def __init__(self, host, port, client_id, timeout=10.0):
        EWrapper.__init__(self)
        self._transport = AsyncIBTransport(wrapper=self)
        self._connected_event = asyncio.Event()
        self._next_order_id: Optional[int] = None
        self.host = host
        self.port = port
        self.client_id = client_id
        self.timeout = timeout
        self._run_task: Optional[asyncio.Task] = None

    async def connect(self) -> bool:
        await self._transport.connect(self.host, self.port, self.client_id)
        self._run_task = asyncio.create_task(self._transport.run())
        try:
            await asyncio.wait_for(self._connected_event.wait(), timeout=self.timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def disconnect(self) -> None:
        self._transport.disconnect()
        if self._run_task:
            self._run_task.cancel()
            try:
                await self._run_task
            except asyncio.CancelledError:
                pass

    # EWrapper callbacks (called from transport.run() — synchronous is fine)
    def nextValidId(self, orderId: int):
        self._next_order_id = orderId
        self._connected_event.set()   # asyncio.Event.set() is synchronous

    def isConnected(self) -> bool:
        return self._transport.isConnected()
```

**Key insight**: `asyncio.Event.set()` is synchronous — callbacks don't need to be
`async` just to signal a waiter.

---

## Phase 3 — Refactor `ib/portfolio.py`

Replace all `threading.Event` with `asyncio.Event` and all `.wait(timeout=X)` with
`await asyncio.wait_for(event.wait(), timeout=X)`.

**Pattern to apply throughout**:
```python
# Before
self._positions_done = threading.Event()
...
self.reqPositions()
if not self._positions_done.wait(timeout=30):
    logger.warning("Timeout waiting for positions")

# After
self._positions_done = asyncio.Event()
...
self.reqPositions()
try:
    await asyncio.wait_for(self._positions_done.wait(), timeout=30)
except asyncio.TimeoutError:
    logger.warning("Timeout waiting for positions")
```

**Events to convert** (all in `portfolio.py`):

| Old (threading.Event) | Set by callback |
|----------------------|-----------------|
| `_connected_event`   | `nextValidId`   |
| `_positions_done`    | `positionEnd`   |
| `_account_updates_done` | `accountDownloadEnd` |
| `_market_data_done`  | `tickSnapshotEnd` |
| `_account_summary_done` | `accountSummaryEnd` |
| `_executions_done`   | `execDetailsEnd` |
| `_pending_orders[orderId]` | `orderStatus` (filled/error) |

**`load()` becomes async** — and parallel with `asyncio.gather()`:
```python
async def load(self, fetch_prices=True, timeout=30.0):
    # Parallel: positions + account updates simultaneously
    await asyncio.gather(
        self._load_positions(timeout),
        self._load_account_updates(timeout),
    )
    if fetch_prices:
        await self._fetch_market_data(timeout)
```

**Order wait**:
```python
async def place_order_and_wait(self, contract, order, timeout=30.0):
    order_id = self._next_order_id
    self._pending_orders[order_id] = asyncio.Event()
    self.placeOrder(order_id, contract, order)
    try:
        await asyncio.wait_for(self._pending_orders[order_id].wait(), timeout)
    except asyncio.TimeoutError:
        logger.warning(f"Order {order_id} timed out")
    finally:
        del self._pending_orders[order_id]
```

---

## Phase 4 — Refactor `ib/connection_manager.py`

Replace all `threading.Thread` + `threading.Event` with `asyncio.Task` + `asyncio.Event`.

```python
# Before
self._keepalive_thread = Thread(target=self._keepalive_loop, daemon=True)
self._keepalive_thread.start()

# After
self._keepalive_task = asyncio.create_task(self._keepalive_loop())

# Before
def _keepalive_loop(self):
    while not self._shutdown_event.is_set():
        self.portfolio.reqCurrentTime()
        if not self._keepalive_response_received.wait(timeout=10.0):
            logger.error("Keepalive timeout - reconnecting")
            self._trigger_reconnect()
        if self._shutdown_event.wait(self.config.keepalive_interval):
            break

# After
async def _keepalive_loop(self):
    while True:
        self.portfolio.reqCurrentTime()
        try:
            await asyncio.wait_for(
                self._keepalive_response_received.wait(),
                timeout=10.0,
            )
        except asyncio.TimeoutError:
            logger.error("Keepalive timeout - reconnecting")
            await self._trigger_reconnect()
        try:
            await asyncio.wait_for(
                self._shutdown_event.wait(),
                timeout=self.config.keepalive_interval,
            )
            return  # shutdown
        except asyncio.TimeoutError:
            pass  # normal interval expiry
```

**Reconnection**: The reconnect loop already has a clear structure. Convert it to
`async def _reconnect_loop()` as an `asyncio.Task`. Use `asyncio.sleep(delay)` instead
of `shutdown_event.wait(delay)`.

**Threads to convert**:
- `_keepalive_thread` → `_keepalive_task`
- `_health_thread` → `_health_task`
- `_reconnect_thread` → `_reconnect_task`

**Shutdown**:
```python
async def stop(self):
    self._shutdown_event.set()
    tasks = [t for t in [self._keepalive_task, self._health_task, self._reconnect_task] if t]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await self.portfolio.disconnect()
```

---

## Phase 5 — Refactor `ib/plugin_executive.py`

The plugin executive has two threads: executor loop and health monitor.

### 5.1  Executor loop

```python
# Before
self._executor_thread = Thread(target=self._executor_loop_wrapper, daemon=True)
self._executor_thread.start()

# After
self._executor_task = asyncio.create_task(self._executor_loop())

async def _executor_loop(self):
    while self._running:
        try:
            item = await asyncio.wait_for(self._order_queue.get(), timeout=0.1)
        except asyncio.TimeoutError:
            continue
        # process item...
```

### 5.2  Order queue

```python
# Before
self._order_queue = queue.Queue()

# After
self._order_queue = asyncio.Queue()
```

### 5.3  Plugin execution

Plugins' `on_bar()` / `on_tick()` are called from the executor loop. Two options:

**Option A (simpler)**: Keep plugin callbacks synchronous; run them in the default
executor if they're slow:
```python
# Fast plugins (pure math): call directly
signal = plugin.on_bar(bar)

# Slow plugins (I/O): run in thread pool
signal = await asyncio.get_event_loop().run_in_executor(None, plugin.on_bar, bar)
```

**Option B (cleaner)**: Make `PluginBase.on_bar()` async — requires updating every
plugin but gives full async capability:
```python
# plugins/base.py
async def on_bar(self, symbol: str, bar: Bar, data_type: DataType) -> Optional[TradeSignal]:
    ...

# executor
signal = await plugin.on_bar(bar)
```

**Recommendation**: Start with Option A (backward compatible), migrate plugins to Option B
incrementally.

### 5.4  Health thread

```python
self._health_task = asyncio.create_task(self._health_monitor_loop())

async def _health_monitor_loop(self):
    while self._running:
        await asyncio.sleep(self.config.health_check_interval)
        self._check_circuit_breakers()
```

### 5.5  Shutdown event

```python
# Before
self._shutdown_event = threading.Event()
self._shutdown_event.set()

# After
self._shutdown_event = asyncio.Event()
self._shutdown_event.set()
```

---

## Phase 6 — Refactor `ib/trading_engine.py`

```python
# Before
def start(self) -> bool:
    self._connection_manager.start()
    self._plugin_executive.start()
    return True

def run_forever(self):
    while not self._shutdown_event.is_set():
        self._shutdown_event.wait(timeout=1.0)

# After
async def start(self) -> bool:
    await self._connection_manager.start()
    await self._plugin_executive.start()
    return True

async def run_forever(self):
    await self._shutdown_event.wait()

async def stop(self):
    self._shutdown_event.set()
    await self._plugin_executive.stop()
    await self._connection_manager.stop()
```

---

## Phase 7 — Refactor `ib/run_engine.py` and `ib/main.py`

**run_engine.py** entry point:
```python
# Before
if __name__ == "__main__":
    engine = TradingEngine(config)
    engine.start()
    engine.run_forever()

# After
async def async_main():
    engine = TradingEngine(config)
    await engine.start()
    await engine.run_forever()

if __name__ == "__main__":
    asyncio.run(async_main())
```

**main.py** (portfolio snapshot CLI):
```python
async def async_main():
    portfolio = Portfolio(...)
    await portfolio.connect()
    await portfolio.load()
    # ... print positions

if __name__ == "__main__":
    asyncio.run(async_main())
```

---

## Phase 8 — Refactor `ib/execution_db.py`

The execution DB uses synchronous SQLite. Replace with `aiosqlite`:

```python
# Before
import sqlite3

def log_execution(self, result):
    with sqlite3.connect(self.db_path) as conn:
        conn.execute("INSERT INTO executions ...", ...)

# After
import aiosqlite

async def log_execution(self, result):
    async with aiosqlite.connect(self.db_path) as conn:
        await conn.execute("INSERT INTO executions ...", ...)
        await conn.commit()
```

---

## Phase 9 — Migrate Tests

### 9.1  Install pytest-asyncio

```
pip install pytest-asyncio
```

### 9.2  `tests/conftest.py`

Add at the top:
```python
import pytest
import asyncio

# Use auto mode so all async tests are detected automatically
# In pyproject.toml: asyncio_mode = "auto"
```

Replace threading mock setup for IBClient/Portfolio:
```python
# Before
from unittest.mock import MagicMock, patch
mock_portfolio = MagicMock(spec=Portfolio)

# After — async mock
from unittest.mock import AsyncMock, MagicMock
mock_portfolio = MagicMock(spec=Portfolio)
mock_portfolio.connect = AsyncMock(return_value=True)
mock_portfolio.load = AsyncMock()
mock_portfolio.disconnect = AsyncMock()
```

### 9.3  Per-test migration pattern

```python
# Before
def test_portfolio_loads_positions(mock_portfolio):
    mock_portfolio._positions_done.set()
    result = mock_portfolio.load(timeout=1.0)
    assert ...

# After
@pytest.mark.asyncio
async def test_portfolio_loads_positions(mock_portfolio):
    mock_portfolio._positions_done.set()
    await mock_portfolio.load(timeout=1.0)
    assert ...
```

### 9.4  Tests requiring asyncio.Event

Replace `threading.Event()` fixtures with `asyncio.Event()`:
```python
@pytest.fixture
def done_event():
    return asyncio.Event()  # not threading.Event
```

---

## Phase 10 — Update `plugins/base.py` (Plugin API)

If going with **Option B** (async plugin callbacks):

```python
# plugins/base.py

class PluginBase(ABC):
    # Lifecycle (keep sync)
    def start(self): ...
    def stop(self): ...

    # Market data callbacks — make async
    async def on_bar(self, symbol: str, bar: Bar, data_type: DataType) -> Optional[TradeSignal]:
        return None

    async def on_tick(self, symbol: str, tick: TickData) -> Optional[TradeSignal]:
        return None

    async def on_message(self, channel: str, message: Any) -> None:
        pass
```

Update each plugin in `plugins/` to add `async` keyword. For simple plugins that return
immediately, `async def on_bar(...)` works without any other changes.

---

## Execution Order

| Phase | File(s) | Effort | Dependency |
|-------|---------|--------|------------|
| 1 | Create `ib/async_transport.py` + `ib/ib_request_mixin.py` | M | None |
| 2 | `ib/client.py` | M | Phase 1 |
| 3 | `ib/portfolio.py` | L | Phase 2 |
| 4 | `ib/connection_manager.py` | M | Phase 3 |
| 5 | `ib/plugin_executive.py` | L | Phase 4 |
| 6 | `ib/trading_engine.py` | S | Phases 4-5 |
| 7 | `ib/run_engine.py`, `ib/main.py` | S | Phase 6 |
| 8 | `ib/execution_db.py` | S | None (parallel) |
| 9 | `tests/conftest.py` + all test files | L | All above |
| 10 | `plugins/base.py` + each plugin | M | Phase 5 |

S = small (<2h), M = medium (2-4h), L = large (4-8h)

---

## Files Summary

| File | Action |
|------|--------|
| `ib/async_transport.py` | **Create** — asyncio socket reader replacing EReader+EClient.run |
| `ib/ib_request_mixin.py` | **Create** — copy ~80 req* methods from ibapi/client.py |
| `ib/client.py` | **Rewrite** — drop EClient inheritance, compose AsyncIBTransport |
| `ib/portfolio.py` | **Refactor** — threading.Event → asyncio.Event, methods become async |
| `ib/connection_manager.py` | **Refactor** — Thread → Task, Event patterns |
| `ib/plugin_executive.py` | **Refactor** — Thread → Task, Queue → asyncio.Queue |
| `ib/trading_engine.py` | **Refactor** — methods become async |
| `ib/run_engine.py` | **Refactor** — asyncio.run() entry point |
| `ib/main.py` | **Refactor** — asyncio.run() entry point |
| `ib/execution_db.py` | **Refactor** — sqlite3 → aiosqlite |
| `plugins/base.py` | **Refactor** — on_bar/on_tick become async (Option B) |
| `plugins/*/plugin.py` | **Refactor** — add async keyword to callbacks |
| `tests/conftest.py` | **Refactor** — add pytest-asyncio, AsyncMock |
| `tests/test_*.py` (all) | **Refactor** — @pytest.mark.asyncio, await calls |

---

## Verification

```bash
source env_claude/bin/activate

# Basic import smoke tests
python3 -c "from ib.async_transport import AsyncIBTransport; print('transport OK')"
python3 -c "from ib.client import IBClient; print('client OK')"
python3 -c "from ib.portfolio import Portfolio; print('portfolio OK')"
python3 -c "from ib.trading_engine import TradingEngine; print('engine OK')"

# Full test suite (target: all 1509 pass)
python3 -m pytest tests/ -v

# Dry-run smoke test (paper account on port 7497)
python3 -m ib.run_engine --port 7497 --mode dry_run
```
