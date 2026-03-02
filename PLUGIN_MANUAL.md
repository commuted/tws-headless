# Plugin Manual

Complete reference for writing plugins for the IB trading engine.

---

## Table of Contents

1. [Overview](#1-overview)
2. [File Layout](#2-file-layout)
3. [Class Skeleton](#3-class-skeleton)
4. [Class Attributes](#4-class-attributes)
5. [Constructor](#5-constructor)
6. [Lifecycle Methods](#6-lifecycle-methods)
7. [State Persistence](#7-state-persistence)
8. [Market Data Streams](#8-market-data-streams)
9. [Historical Data](#9-historical-data)
10. [MessageBus — Publish / Subscribe](#10-messagebus--publish--subscribe)
11. [Request Handling](#11-request-handling)
12. [Trade Signals](#12-trade-signals)
13. [Order Callbacks and Error Routing](#13-order-callbacks-and-error-routing)
14. [Portfolio Access](#14-portfolio-access)
15. [Contract Builder](#15-contract-builder)
16. [Self-Unload](#16-self-unload)
17. [Engine Commands](#17-engine-commands)
18. [Threading Rules](#18-threading-rules)
19. [Naming Conventions](#19-naming-conventions)
20. [Complete Examples](#20-complete-examples)

---

## 1. Overview

A plugin is a Python class that subclasses `PluginBase`. The engine
(PluginExecutive) manages its lifecycle, feeds it market data, routes
MessageBus messages to it, and executes the trade signals it returns.

Plugins are isolated from each other. They share market data
subscriptions (one IB request serves many plugins) but have independent
callback routing, state files, and holdings.

---

## 2. File Layout

```
plugins/
  my_plugin/
    __init__.py        # re-exports the class (required for dynamic loading)
    plugin.py          # plugin implementation
    instruments.json   # optional: tradable instruments with target weights
    holdings.json      # optional: persistent position tracking
    state.json         # written/read by save_state / load_state
```

`__init__.py` must re-export the plugin class so `plugin load` can find it:

```python
from .plugin import MyPlugin
__all__ = ["MyPlugin"]
```

---

## 3. Class Skeleton

```python
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional
from plugins.base import PluginBase, TradeSignal

class MyPlugin(PluginBase):

    VERSION = "1.0.0"
    IS_SYSTEM_PLUGIN = False

    def __init__(self, base_path=None, portfolio=None,
                 shared_holdings=None, message_bus=None):
        super().__init__("my_plugin", base_path, portfolio,
                         shared_holdings, message_bus)

    @property
    def description(self) -> str:
        return "One-line description shown in plugin list."

    def start(self) -> bool:   ...
    def stop(self) -> bool:    ...
    def freeze(self) -> bool:  ...
    def resume(self) -> bool:  ...

    def calculate_signals(self) -> List[TradeSignal]:
        return []

    def handle_request(self, request_type: str, payload: Dict) -> Dict:
        return {"success": False, "message": f"Unknown: {request_type}"}
```

All six lifecycle methods and `calculate_signals` are abstract —
they must be implemented even if they just return `True` / `[]`.

---

## 4. Class Attributes

| Attribute | Type | Default | Purpose |
|-----------|------|---------|---------|
| `VERSION` | `str` | `"1.0.0"` | Stored in state files; shown in `plugin list` |
| `IS_SYSTEM_PLUGIN` | `bool` | `False` | `True` prevents user unload/delete |

---

## 5. Constructor

```python
def __init__(
    self,
    base_path: Optional[Path] = None,
    portfolio=None,
    shared_holdings=None,
    message_bus=None,
):
    super().__init__(
        name,             # str  — unique name; used for file paths and registry
        base_path,        # Path — overrides default plugins/<name>/
        portfolio,        # Portfolio instance (may be None in tests)
        shared_holdings,  # SharedHoldings (optional, rarely needed)
        message_bus,      # MessageBus (set by executive at load time)
    )
```

The `name` argument to `super().__init__` is the key used everywhere:
log prefixes, state file paths, MessageBus publisher/subscriber identity,
and the `plugin request <name>` command target.

**Instance attributes set by `__init__` (read-only, do not reassign):**

| Attribute | Type | Description |
|-----------|------|-------------|
| `self.name` | `str` | Plugin name passed to super().__init__ |
| `self.instance_id` | `str` | UUID generated at construction; unique per load |
| `self.portfolio` | `Portfolio \| None` | IB connection; `None` in tests |
| `self._message_bus` | `MessageBus \| None` | Pub/sub bus; set by executive |
| `self._executive` | `PluginExecutive \| None` | Set by executive at load time |
| `self._base_path` | `Path` | Directory for all plugin files |
| `self._state_file` | `Path` | `<base_path>/state.json` |

---

## 6. Lifecycle Methods

The engine calls these in order. Each must return `True` on success or
`False` / raise on failure.

### State diagram

```
UNLOADED
   │ load()          (engine internal — do not override)
   ▼
LOADED
   │ start()
   ▼
STARTED ◄────────────────────────────────┐
   │ freeze()                             │ resume()
   ▼                                      │
FROZEN ─────────────────────────────────►┘
   │ stop()
   ▼
STOPPED
   │ unload()        (engine internal — do not override)
   ▼
UNLOADED
```

### `start(self) -> bool`

Called once after load. Set up streams, subscribe to MessageBus channels,
restore persisted state.

```python
def start(self) -> bool:
    saved = self.load_state()
    if saved:
        self._counter = saved.get("counter", 0)
    self.subscribe("indicators_rsi", self._on_rsi)
    self.request_stream(
        symbol="SPY",
        contract=ContractBuilder.us_stock("SPY", primary_exchange="ARCA"),
        data_types={DataType.BAR_5SEC},
        on_bar=self._on_bar,
    )
    return True
```

### `stop(self) -> bool`

Called on engine shutdown or explicit `plugin stop`. Cancel streams,
unsubscribe, save state.

```python
def stop(self) -> bool:
    self.cancel_stream("SPY")
    self.unsubscribe_all()
    self.save_state({"counter": self._counter})
    return True
```

### `freeze(self) -> bool`

Pause without destroying subscriptions. Save current state.
`calculate_signals` is **not** called while frozen.

```python
def freeze(self) -> bool:
    self.save_state({"counter": self._counter})
    return True
```

### `resume(self) -> bool`

Resume from frozen. Streams and subscriptions remain active through
freeze/resume so no re-subscription is needed.

```python
def resume(self) -> bool:
    return True
```

### `calculate_signals(self) -> List[TradeSignal]`

Called by the executive on every bar event (or tick, depending on
`ExecutionMode`). Plugins receive market data through stream callbacks
registered in `start()` and maintain their own internal state; this method
reads that state and returns signals.

Return a list of `TradeSignal` objects to request order execution, or an
empty list. Exceptions raised here are caught, logged, and counted toward
the circuit breaker.

```python
def calculate_signals(self) -> List[TradeSignal]:
    if self._signal_pending:
        self._signal_pending = False
        return [TradeSignal(symbol="SPY", action="BUY", quantity=Decimal("10"))]
    return []
```

---

## 7. State Persistence

State is saved to and loaded from `<base_path>/state.json`. The file is
JSON. Any JSON-serializable value is accepted as the dict value.

### `save_state(state: Dict[str, Any]) -> bool`

Overwrites the state file. The engine adds metadata automatically
(`plugin_name`, `plugin_version`, `saved_at`). Call in `stop()` and
`freeze()`.

```python
self.save_state({
    "positions": {"SPY": 10, "QQQ": 5},
    "last_signal": "BUY",
    "bar_count": 1042,
})
```

### `load_state() -> Dict[str, Any]`

Returns the saved dict, or `{}` if no file exists. Call in `start()`.

```python
saved = self.load_state()
self._bar_count = saved.get("bar_count", 0)
```

The metadata keys `plugin_name`, `plugin_version`, and `saved_at` are
present in the returned dict but should be ignored.

---

## 8. Market Data Streams

Streams are shared across plugins. The engine sends one IB request per
symbol; all plugins that request the same symbol receive all data via
their own callbacks. Cancelling a stream decrements a reference count —
the IB subscription is only torn down when no plugins remain subscribed.

### `request_stream(...) -> bool`

```python
self.request_stream(
    symbol="SPY",                          # str — key used for cancel_stream
    contract=ContractBuilder.us_stock("SPY"),
    data_types={DataType.BAR_5SEC},        # set of DataType values
    on_tick=self._on_tick,                 # optional callback
    on_bar=self._on_bar,                   # optional callback
    what_to_show="TRADES",                 # "TRADES" | "MIDPOINT" | "BID" | "ASK"
    use_rth=True,                          # regular trading hours only
)
```

**`data_types`** — pass a set of `DataType` enum values:

| DataType | IB source | Notes |
|----------|-----------|-------|
| `DataType.TICK` | `reqMktData` | Every price update |
| `DataType.BAR_5SEC` | `reqRealTimeBars` | 5-second OHLCV; market hours only |
| `DataType.BAR_1MIN` | aggregated from 5-sec | Completed when minute boundary passes |
| `DataType.BAR_5MIN` | aggregated from 5-sec | Completed at 5-min boundary |
| `DataType.BAR_15MIN` | aggregated from 5-sec | Completed at 15-min boundary |
| `DataType.BAR_1HOUR` | aggregated from 5-sec | Completed at hour boundary |

Default when `data_types` is `None`: `{DataType.TICK, DataType.BAR_5SEC}`.

**`what_to_show`** applies to both tick and bar subscriptions. For US
equity ticks on a paper account, use delayed mode (call
`self.portfolio.reqMarketDataType(3)` before subscribing). For
`reqRealTimeBars`, delayed mode silently suppresses all callbacks — use
live mode (`reqMarketDataType(1)`) for bars.

### Tick callback signature

The callback receives a single `TickData` object containing all tick fields:

```python
from ib.data_feed import TickData

def _on_tick(self, tick: TickData) -> None:
    # tick.symbol     str            — e.g. "SPY"
    # tick.tick_type  str            — e.g. "LAST", "BID", "ASK", "CLOSE",
    #                                        "DELAYED_LAST", "DELAYED_BID",
    #                                        "BID_SIZE", "ASK_SIZE", "LAST_SIZE",
    #                                        "VOLUME", "DELAYED_VOLUME", ...
    # tick.price      float          — last traded price; 0.0 for size-only ticks
    # tick.size       Optional[int]  — bid/ask/last size or volume; None for price ticks
    # tick.timestamp  datetime       — wall-clock time the tick was received
    pass
```

Price ticks (LAST, BID, ASK, CLOSE, …) have `tick.price > 0` and `tick.size is None`.
Size ticks (BID_SIZE, ASK_SIZE, LAST_SIZE, VOLUME, …) have `tick.size >= 0` and `tick.price == 0.0`.

```python
def _on_tick(self, tick: TickData) -> None:
    if tick.size is not None:
        logger.debug(f"{tick.symbol} {tick.tick_type}: size={tick.size}")
    else:
        logger.debug(f"{tick.symbol} {tick.tick_type}: price={tick.price:.4f}")
```

### Bar callback signature

```python
def _on_bar(self, bar) -> None:
    # bar is ib.models.Bar
    # bar.symbol    str
    # bar.timestamp str  ISO format, e.g. "2026-02-26T09:30:00"
    # bar.open      float
    # bar.high      float
    # bar.low       float
    # bar.close     float
    # bar.volume    int
    # bar.wap       float   weighted average price
    # bar.bar_count int     number of trades in bar
    #
    # Computed properties:
    # bar.range     float   high - low
    # bar.body      float   abs(close - open)
    # bar.is_bullish bool   close > open
    # bar.is_bearish bool   close < open
    # bar.mid       float   (high + low) / 2
```

The same `on_bar` callback receives bars from all `DataType` values the
plugin subscribed to. The bar's `timestamp` boundary identifies its
timeframe (e.g. `09:35:00` = 5-min bar opening at 09:35).

### `cancel_stream(symbol: str) -> bool`

```python
self.cancel_stream("SPY")
```

Call in `stop()` for every symbol requested in `start()`. The engine also
calls `cancel_all_streams` automatically when a plugin is stopped or
unloaded, so this is defensive cleanup.

---

## 9. Historical Data

Fetches OHLCV bars for a contract and returns when the request completes.
Blocks the calling thread. Each call is private — multiple concurrent
calls from different plugins never share data.

### `get_historical_data(...) -> Optional[List]`

```python
bars = self.get_historical_data(
    contract=ContractBuilder.us_stock("AAPL", primary_exchange="NASDAQ"),
    end_date_time="",           # "" = now; or "YYYYMMDD HH:MM:SS [tz]"
    duration_str="1 W",         # "N S|D|W|M|Y"  (Seconds/Days/Weeks/Months/Years)
    bar_size_setting="1 day",   # see Bar Size table below
    what_to_show="TRADES",      # TRADES | MIDPOINT | BID | ASK | ADJUSTED_LAST | etc.
    use_rth=True,               # True = regular hours only
    timeout=60.0,               # seconds before giving up
)
```

Returns a `list` of `ibapi.BarData` objects, or `None` on timeout/error.

**BarData attributes:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `.date` | `str` | Date/time string; format depends on bar size |
| `.open` | `float` | Open price |
| `.high` | `float` | High price |
| `.low` | `float` | Low price |
| `.close` | `float` | Close price |
| `.volume` | `Decimal` | Volume (may be -1 for non-TRADES data) |
| `.wap` | `Decimal` | Weighted average price |
| `.barCount` | `int` | Number of trades in bar |

**`duration_str` format:** `"N unit"` where unit is `S` (seconds),
`D` (days), `W` (weeks), `M` (months), `Y` (years). Examples:
`"30 D"`, `"1 W"`, `"3 M"`, `"1 Y"`.

**`bar_size_setting` valid values:**
`"1 secs"`, `"5 secs"`, `"10 secs"`, `"15 secs"`, `"30 secs"`,
`"1 min"`, `"2 mins"`, `"3 mins"`, `"5 mins"`, `"10 mins"`,
`"15 mins"`, `"20 mins"`, `"30 mins"`,
`"1 hour"`, `"2 hours"`, `"3 hours"`, `"4 hours"`, `"8 hours"`,
`"1 day"`, `"1 week"`, `"1 month"`.

**Important:** `reqRealTimeBars` (used for streaming) only works during
market hours. `get_historical_data` works at any time of day.

**what_to_show notes:**
- `TRADES`: requires live data subscription for US equities
- `MIDPOINT`: works without subscription; also works for forex
- `ADJUSTED_LAST`: adjusts for splits and dividends; stocks only

```python
bars = self.get_historical_data(
    contract=ContractBuilder.us_stock("SPY"),
    duration_str="1 W",
    bar_size_setting="1 day",
)
if bars is None:
    logger.error("Historical data request timed out")
    return
for b in bars:
    logger.info(f"{b.date}  O:{b.open}  H:{b.high}  L:{b.low}  C:{b.close}")
```

---

## 10. MessageBus — Publish / Subscribe

The MessageBus is a thread-safe pub/sub broker shared across all plugins.
Plugins communicate exclusively through it — no direct references between
plugins.

### `publish(channel, payload, message_type="data") -> bool`

```python
self.publish(
    channel="indicators_sma",          # str — channel name
    payload={"symbol": "SPY",          # any JSON-serializable value
             "sma": 541.32,
             "period": 20},
    message_type="data",               # "data" | "signal" | "alert" | "metric"
)
```

Returns `False` (with a warning log) if no MessageBus is configured.
Channels are created automatically on first publish.

### `subscribe(channel, callback) -> bool`

```python
def start(self) -> bool:
    self.subscribe("indicators_sma", self._on_sma)
    return True

def _on_sma(self, message) -> None:
    payload   = message.payload             # the dict passed to publish()
    source    = message.metadata.source_plugin  # name of publishing plugin
    ts        = message.metadata.timestamp      # datetime
    seq       = message.metadata.sequence_number  # int, per-channel counter
    channel   = message.channel
```

Subscribing the same plugin to the same channel a second time updates the
callback rather than adding a duplicate.

### `unsubscribe(channel) -> bool`

```python
self.unsubscribe("indicators_sma")
```

### `unsubscribe_all() -> int`

Unsubscribes from every channel this plugin has subscribed to. Returns the
count of channels removed. Call in `stop()`.

```python
def stop(self) -> bool:
    self.unsubscribe_all()
    ...
```

### Channel naming conventions

| Pattern | Purpose |
|---------|---------|
| `indicators_<name>` | Indicator values: `indicators_rsi`, `indicators_sma` |
| `<plugin>_signals` | Trading signals from a strategy plugin |
| `<plugin>_metrics` | Performance or health metrics |
| `synthetic_<name>` | Synthetic spreads or derived tickers |
| `alerts` | System-wide alerts |

### Message history

The MessageBus stores the last 1 000 messages per channel. Plugins do not
access this directly — it is used for debugging via `plugin request`.

---

## 11. Request Handling

`handle_request` is the plugin's external command interface. The engine
routes `plugin request <name> <type>` socket commands here.

```python
def handle_request(self, request_type: str, payload: Dict) -> Dict:
    if request_type == "get_status":
        return {
            "success": True,
            "data": {"bar_count": self._bar_count},
        }
    if request_type == "set_period":
        self._period = payload.get("period", self._period)
        return {"success": True}
    return {"success": False, "message": f"Unknown request: {request_type}"}
```

**Rules:**
- Always return a dict with a `"success"` key (`bool`).
- On success, put return data under `"data"` (any JSON-serializable value).
- On failure, put a human-readable string under `"message"`.
- `payload` is the parsed JSON body of the request; it may be `{}`.
- `handle_request` is called on the socket thread — keep it fast.
  Spawn a thread for long-running operations.

---

## 12. Trade Signals

`calculate_signals` returns a list of `TradeSignal` objects. The executive
reconciles signals from all plugins, applies rate limiting, and places
orders.

```python
from decimal import Decimal
from plugins.base import TradeSignal

TradeSignal(
    symbol="SPY",              # str     — ticker symbol
    action="BUY",              # str     — "BUY" | "SELL" | "HOLD"
    quantity=Decimal("10"),    # Decimal — number of shares/contracts
    target_weight=0.20,        # float   — optional: target portfolio weight 0.0–1.0
    current_weight=0.15,       # float   — optional: current portfolio weight
    reason="SMA crossover",    # str     — logged; shown in execution history
    confidence=0.85,           # float   — 0.0–1.0; used by reconciler
    urgency="Normal",          # str     — "Patient" | "Normal" | "Urgent"
)
```

`quantity` is a `Decimal`. Always construct it from a string literal
(`Decimal("10")`) or from `str()` of a computed value
(`Decimal(str(shares))`) to avoid floating-point rounding artefacts.
The default is `Decimal("0")`.

A signal is **actionable** (`signal.is_actionable == True`) when
`action` is `"BUY"` or `"SELL"` and `quantity > 0`. `"HOLD"` signals are
ignored by the executor.

**Order execution modes** (set at engine level, not per-plugin):

| Mode | Behaviour |
|------|-----------|
| `DRY_RUN` | Signals logged; no orders sent to IB |
| `IMMEDIATE` | Orders sent immediately |
| `QUEUED` | Orders batched for execution |

---

## 13. Order Callbacks and Error Routing

The executive routes IB order status updates and errors back to the plugin
that owns each request. Overriding these hooks is optional; the default
implementations are no-ops.

### Automatic routing for `calculate_signals` orders

Orders placed by the executive as a result of `calculate_signals` are
automatically associated with the originating plugin. No extra
registration is required.

### `register_order(order_id: int) -> None`

When a plugin places orders **directly** via
`self.portfolio.place_order_custom()` (bypassing the signal system), call
`register_order` immediately after so the executive can route callbacks to
this plugin.

```python
order_id = self.portfolio.place_order_custom(contract, order)
if order_id is not None:
    self.register_order(order_id)
```

### `on_order_fill(self, order_record) -> None`

Called when one of this plugin's orders reaches `FILLED` status.

```python
def on_order_fill(self, order_record) -> None:
    logger.info(
        f"Filled {order_record.order_id}: "
        f"{order_record.filled_quantity} × {order_record.symbol} "
        f"@ {order_record.avg_fill_price:.2f}"
    )
```

### `on_order_status(self, order_record) -> None`

Called on **every** IB status change for an order attributed to this
plugin (submitted, partially filled, filled, cancelled, etc.).

```python
def on_order_status(self, order_record) -> None:
    if order_record.is_complete and not order_record.is_filled:
        logger.warning(
            f"Order {order_record.order_id} ended without fill: "
            f"{order_record.status.value}"
        )
```

**`OrderRecord` attributes:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `order_id` | `int` | IB order ID |
| `symbol` | `str` | Ticker symbol |
| `action` | `str` | `"BUY"` or `"SELL"` |
| `quantity` | `float` | Requested quantity |
| `order_type` | `str` | `"MKT"`, `"LMT"`, etc. |
| `status` | `OrderStatus` | Current status enum value |
| `filled_quantity` | `float` | Shares filled so far |
| `avg_fill_price` | `float` | Average fill price (0.0 until filled) |
| `remaining` | `float` | Shares not yet filled |
| `is_filled` | `bool` | `True` when `status == FILLED` |
| `is_complete` | `bool` | `True` when filled, cancelled, or error |
| `fill_value` | `float` | `filled_quantity × avg_fill_price` |

**`OrderStatus` enum values:**

| Value | Meaning |
|-------|---------|
| `PENDING` | Not yet acknowledged by IB |
| `SUBMITTED` | Actively working at IB |
| `PARTIALLY_FILLED` | Some shares filled; order still open |
| `FILLED` | Completely filled |
| `CANCELLED` | Cancelled by user or IB |
| `INACTIVE` | Submitted but not actively working (e.g. outside hours) |
| `ERROR` | Rejected or other error |

### `on_ib_error(self, req_id: int, error_code: int, error_string: str) -> None`

Called when IB reports an error for a request owned by this plugin.
The executive filters out system messages and routine informational codes
before dispatching, so every call to this method represents an actionable
condition.

**Filtered before dispatch (never reach `on_ib_error`):**

| Code | Description |
|------|-------------|
| `req_id == -1` | System-level message not tied to any request |
| 2104 | Market data farm connection is OK |
| 2106 | HMDS data farm connection is OK |
| 2119 | Market data farm is connecting |
| 2158 | Sec-def data farm connection is OK |
| 10167 | Requested market data is not subscribed (delayed data switch) |

```python
def on_ib_error(self, req_id: int, error_code: int, error_string: str) -> None:
    logger.warning(f"IB error reqId={req_id} [{error_code}]: {error_string}")
    if error_code == 201:          # Order rejected
        self._handle_rejection(req_id)
    elif error_code in (10090, 10091):   # No market data permissions
        logger.error(f"Missing data subscription for reqId={req_id}")
```

**Routing logic:**

- If `req_id` matches a registered order ID, the error is routed to the
  plugin(s) that own that order.
- Otherwise, if `req_id` matches an active tick or bar stream subscription,
  the error is routed to all plugins subscribed to that symbol.
- Order routing takes priority if the same ID appears in both maps.
- If no plugin claims the request, the error is logged at DEBUG level
  and discarded.

---

## 14. Portfolio Access

`self.portfolio` is the live IB connection. It is `None` in unit tests.
Always guard with `if self.portfolio:` before use.

### Connection state

```python
self.portfolio.connected          # bool — True if connected to TWS/Gateway
self.portfolio.port               # int  — connection port (7497, 4001, 4002, etc.)
self.portfolio.managed_accounts   # List[str] — account IDs, e.g. ["DU1234567"]
```

Paper account ports: `7497` (TWS paper), `4002` (Gateway paper).
Paper account IDs start with `"D"`.

### Positions

```python
positions = self.portfolio.positions   # Dict[str, Position]
pos = positions.get("SPY")
if pos:
    pos.symbol          # str
    pos.quantity        # float
    pos.avg_cost        # float
    pos.current_price   # float
    pos.market_value    # float
    pos.unrealized_pnl  # float
    pos.allocation_pct  # float
```

### Order management

```python
# Place an order (returns order_id or None)
order_id = self.portfolio.place_order_custom(contract, order)

# Place with a pre-allocated ID
self.portfolio.place_order_raw(order_id, contract, order)

# Allocate consecutive IDs
ids = self.portfolio.allocate_order_ids(count)   # List[int]

# Cancel an order
self.portfolio.cancel_order(order_id)            # bool

# Look up an order record
rec = self.portfolio.get_order(order_id)
if rec:
    rec.is_filled        # bool
    rec.avg_fill_price   # float
    rec.status           # str
```

### Market data type (paper accounts)

```python
# Delayed ticks (use before reqMktData on paper without live subscription)
self.portfolio.reqMarketDataType(3)

# Live mode (required for reqRealTimeBars)
self.portfolio.reqMarketDataType(1)
```

### Historical data (low-level)

Use `self.get_historical_data()` (Section 9) instead of calling
`self.portfolio.request_historical_data()` directly. The PluginBase
wrapper handles threading and timeout automatically.

---

## 15. Contract Builder

`from ib.contract_builder import ContractBuilder`

All methods are `@staticmethod`.

### Equities

```python
ContractBuilder.us_stock("SPY")
ContractBuilder.us_stock("SPY", primary_exchange="ARCA")   # unambiguous for live data
ContractBuilder.us_stock("AAPL", primary_exchange="NASDAQ")
ContractBuilder.european_stock("SAP", currency="EUR")
ContractBuilder.etf("QQQ")
ContractBuilder.stock(symbol, exchange, currency, primary_exchange)
```

### Options

```python
ContractBuilder.option("AAPL", expiry="20260117", strike=200.0, right="C")
ContractBuilder.option_by_local_symbol("AAPL  260117C00200000")
ContractBuilder.option_chain_query("AAPL")   # for reqContractDetails
```

### Futures

```python
ContractBuilder.future("ES", expiry="202603", exchange="CME")
ContractBuilder.future_by_local_symbol("ESH6", exchange="CME")
ContractBuilder.continuous_future("ES", exchange="CME")
```

### Forex

```python
ContractBuilder.forex("EUR", "USD")       # EUR.USD on IDEALPRO
ContractBuilder.forex("GBP", "USD")
```

### Other instruments

```python
ContractBuilder.index("SPX", exchange="CBOE")
ContractBuilder.crypto("BTC", exchange="PAXOS")
ContractBuilder.bond_by_cusip("912828ZQ2")
ContractBuilder.bond_by_conid(12345678)
ContractBuilder.cfd("AAPL")
ContractBuilder.commodity("XAUUSD")
ContractBuilder.mutual_fund("VFINX")
ContractBuilder.warrant("DAI", expiry="20260101", strike=80.0, right="C", exchange="FWB")
ContractBuilder.futures_on_options("ES", expiry="20260320", strike=5000.0,
                                   right="C", exchange="CME")
```

### Spreads / combos

```python
leg1 = ContractBuilder.create_combo_leg(con_id=123, action="BUY",  ratio=1)
leg2 = ContractBuilder.create_combo_leg(con_id=456, action="SELL", ratio=1)
contract = ContractBuilder.combo("AAPL", legs=[leg1, leg2])

# Convenience helpers
ContractBuilder.stock_spread(leg1_conid, "BUY", leg2_conid, "SELL")
ContractBuilder.option_spread(leg1_conid, "BUY", leg2_conid, "SELL",
                              symbol="AAPL", exchange="SMART")
ContractBuilder.futures_spread(leg1_conid, "BUY", leg2_conid, "SELL",
                               symbol="ES", exchange="CME")
```

### By identifier

```python
ContractBuilder.by_conid(123456789)
ContractBuilder.by_isin("US78462F1030")
ContractBuilder.by_figi("BBG000B9XRY4")
```

---

## 16. Self-Unload

A one-shot plugin (e.g. a test or a single-execution strategy) can ask
the engine to unload it after completing its work.

```python
self.request_unload()
```

The unload is deferred to a separate thread so it is safe to call from
within `handle_request`, a stream callback, or `calculate_signals`.
The engine calls `stop()` then `unload()` on the plugin.

---

## 17. Engine Commands

Plugins are controlled via the `ibctl.py` CLI (or the socket directly).

```bash
# Load a plugin module (finds PluginBase subclass automatically)
plugin load plugins.my_package.my_plugin

# Lifecycle
plugin start  my_plugin
plugin freeze my_plugin
plugin resume my_plugin
plugin stop   my_plugin

# Send a custom request
plugin request my_plugin get_status
plugin request my_plugin set_period '{"period": 14}'

# List all loaded plugins with state
plugin list

# Show positions and open orders held by a plugin
plugin dump my_plugin

# Unload (calls stop then removes from registry)
plugin unload my_plugin
```

The `plugin load` command resolves the module, finds the first class that
is a non-abstract subclass of `PluginBase`, instantiates it (passing
`portfolio`, `message_bus`, and `base_path`), and registers it in
`MANUAL` execution mode. Call `plugin start` separately.

---

## 18. Threading Rules

- **Stream callbacks** (`on_tick`, `on_bar`) are called on the IB reader
  thread. They must return quickly. Do not call blocking operations or
  acquire long-held locks.

- **MessageBus callbacks** are called on the publisher's thread (the
  thread that called `publish()`). The same rules apply.

- **`handle_request`** is called on the command server socket thread.

- **`calculate_signals`** is called on the executive runner thread.

- **`start`, `stop`, `freeze`, `resume`** are called on the executive
  control thread.

- **`on_order_fill`, `on_order_status`** are called on the IB reader
  thread (the same thread that receives `orderStatus` callbacks from TWS).
  They must return quickly. Do not block or acquire long-held locks.
  Use a `threading.Event` or queue to hand off work to a waiting thread.

- **`on_ib_error`** is called on the IB reader thread (the thread that
  receives `error()` callbacks from TWS). The same fast-return rule
  applies.

- If a callback needs to do heavy work, post to a queue and process on a
  dedicated thread started in `start()` and stopped in `stop()`.

- **Never** call `request_stream`, `cancel_stream`, `publish`, or
  `save_state` from inside the IB reader thread (i.e. from an `on_tick`
  or `on_bar` callback) without confirming the operation is thread-safe.
  `publish()` acquires an `RLock` and is safe. `save_state()` does file
  I/O and should be off the hot path.

- The circuit breaker trips after 5 consecutive exceptions from
  `calculate_signals`. It resets automatically after 5 minutes (enters
  half-open) and closes on the first successful run. Exceptions in stream
  callbacks and MessageBus callbacks are caught and logged but do not
  affect the circuit breaker.

---

## 19. Naming Conventions

| Item | Convention | Example |
|------|------------|---------|
| Plugin directory | `snake_case` | `plugins/sma_publisher/` |
| Plugin name (passed to super) | `snake_case` | `"sma_publisher"` |
| Class name | `PascalCase` + `Plugin` suffix | `SMAPublisherPlugin` |
| MessageBus channel | `snake_case` with `_` separators | `"indicators_sma"` |
| State file keys | `snake_case` | `"bar_count"`, `"last_sma"` |
| `handle_request` type strings | `snake_case` | `"get_status"`, `"run_tests"` |

The plugin name must be unique across all loaded plugins. Loading a second
plugin with the same name is rejected by the engine.

---

## 20. Complete Examples

### One-shot plugin (runs, then self-unloads)

```python
from pathlib import Path
from typing import Dict, List, Optional
from ib.contract_builder import ContractBuilder
from plugins.base import PluginBase, TradeSignal

class DailyReportPlugin(PluginBase):
    VERSION = "1.0.0"
    IS_SYSTEM_PLUGIN = False

    def __init__(self, base_path=None, portfolio=None,
                 shared_holdings=None, message_bus=None):
        super().__init__("daily_report", base_path, portfolio,
                         shared_holdings, message_bus)

    @property
    def description(self) -> str:
        return "Fetches one week of SPY daily bars and logs a summary."

    def start(self) -> bool:
        return True

    def stop(self) -> bool:
        return True

    def freeze(self) -> bool:
        return True

    def resume(self) -> bool:
        return True

    def calculate_signals(self) -> List[TradeSignal]:
        return []

    def handle_request(self, request_type: str, payload: Dict) -> Dict:
        if request_type == "run":
            return self._run()
        return {"success": False, "message": f"Unknown: {request_type}"}

    def _run(self) -> Dict:
        bars = self.get_historical_data(
            contract=ContractBuilder.us_stock("SPY", primary_exchange="ARCA"),
            duration_str="1 W",
            bar_size_setting="1 day",
        )
        if bars is None:
            return {"success": False, "message": "Timeout fetching bars"}

        rows = [{"date": b.date, "close": b.close, "volume": float(b.volume)}
                for b in bars]
        self.request_unload()
        return {"success": True, "data": rows}
```

### Indicator publisher (runs continuously)

```python
from collections import deque
from ib.contract_builder import ContractBuilder
from ib.data_feed import DataType
from plugins.base import PluginBase, TradeSignal

class RSIPublisherPlugin(PluginBase):
    VERSION = "1.0.0"
    IS_SYSTEM_PLUGIN = False
    SYMBOL = "SPY"
    PERIOD = 14
    CHANNEL = "indicators_rsi"

    def __init__(self, base_path=None, portfolio=None,
                 shared_holdings=None, message_bus=None):
        super().__init__("rsi_publisher", base_path, portfolio,
                         shared_holdings, message_bus)
        self._gains = deque(maxlen=self.PERIOD)
        self._losses = deque(maxlen=self.PERIOD)
        self._prev_close: float = 0.0

    @property
    def description(self) -> str:
        return f"Publishes RSI({self.PERIOD}) for {self.SYMBOL} to '{self.CHANNEL}'."

    def start(self) -> bool:
        self.request_stream(
            symbol=self.SYMBOL,
            contract=ContractBuilder.us_stock(self.SYMBOL, primary_exchange="ARCA"),
            data_types={DataType.BAR_5SEC},
            on_bar=self._on_bar,
        )
        return True

    def stop(self) -> bool:
        self.cancel_stream(self.SYMBOL)
        return True

    def freeze(self) -> bool:
        return True

    def resume(self) -> bool:
        return True

    def calculate_signals(self):
        return []

    def handle_request(self, request_type, payload):
        return {"success": False, "message": f"Unknown: {request_type}"}

    def _on_bar(self, bar) -> None:
        if self._prev_close > 0:
            change = bar.close - self._prev_close
            self._gains.append(max(change, 0))
            self._losses.append(max(-change, 0))
        self._prev_close = bar.close

        if len(self._gains) < self.PERIOD:
            return

        avg_gain = sum(self._gains) / self.PERIOD
        avg_loss = sum(self._losses) / self.PERIOD
        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))

        self.publish(self.CHANNEL, {
            "symbol": self.SYMBOL,
            "period": self.PERIOD,
            "rsi": round(rsi, 2),
            "close": bar.close,
        })
```

### Subscriber / strategy plugin

```python
from decimal import Decimal
from plugins.base import PluginBase, TradeSignal

class RSIStrategyPlugin(PluginBase):
    VERSION = "1.0.0"
    IS_SYSTEM_PLUGIN = False

    def __init__(self, base_path=None, portfolio=None,
                 shared_holdings=None, message_bus=None):
        super().__init__("rsi_strategy", base_path, portfolio,
                         shared_holdings, message_bus)
        self._pending: list = []

    @property
    def description(self) -> str:
        return "Buys SPY when RSI < 30, sells when RSI > 70."

    def start(self) -> bool:
        saved = self.load_state()
        self._pending = saved.get("pending", [])
        self.subscribe("indicators_rsi", self._on_rsi)
        return True

    def stop(self) -> bool:
        self.save_state({"pending": self._pending})
        self.unsubscribe_all()
        return True

    def freeze(self) -> bool:
        self.save_state({"pending": self._pending})
        return True

    def resume(self) -> bool:
        return True

    def calculate_signals(self):
        signals, self._pending = self._pending[:], []
        return signals

    def handle_request(self, request_type, payload):
        if request_type == "get_status":
            return {"success": True,
                    "data": {"pending_signals": len(self._pending)}}
        return {"success": False, "message": f"Unknown: {request_type}"}

    def _on_rsi(self, message) -> None:
        rsi    = message.payload.get("rsi", 50)
        symbol = message.payload.get("symbol", "SPY")
        if rsi < 30:
            self._pending.append(
                TradeSignal(symbol=symbol, action="BUY", quantity=Decimal("10"),
                            reason=f"RSI oversold ({rsi:.1f})", confidence=0.8)
            )
        elif rsi > 70:
            self._pending.append(
                TradeSignal(symbol=symbol, action="SELL", quantity=Decimal("10"),
                            reason=f"RSI overbought ({rsi:.1f})", confidence=0.8)
            )
```
