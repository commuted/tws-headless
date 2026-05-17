# Orders Plugin (`_orders`)

System plugin providing direct order execution via the socket command interface. Supports all IB order types — market, limit, stop, stop-limit, trailing stop, MOC, LOC, MOO, and LOO — without requiring a strategy plugin. It is loaded automatically by the engine.

## Architecture

`_orders` is a stateless execution passthrough. It does not hold positions, generate signals, or claim symbols. Orders placed through it are tracked in memory for the session; use `list_orders` or `get_order` to monitor them.

```
ibctl order buy/sell  →  CommandServer  →  OrdersPlugin.execute_order()
                                         ↓
                                   ContractBuilder (STK SMART USD)
                                         ↓
                                   portfolio.place_order()  →  IB TWS
                                         ↓
                                   OrderRecord stored in memory
```

**Supported order types:**

| Type | IB Code | Description |
|------|---------|-------------|
| market | MKT | Execute at best available price |
| limit | LMT | Execute at limit price or better |
| stop | STP | Market order triggered at stop price |
| stop-limit | STP LMT | Limit order triggered at stop price |
| trail | TRAIL | Trailing stop by dollar amount or percent |
| trail-limit | TRAIL LIMIT | Trailing stop limit |
| moc | MOC | Market on Close (fills at 4:00 PM auction) |
| loc | LOC | Limit on Close |
| moo | MOO | Market on Open |
| loo | LOO | Limit on Open |

## Placing Orders

```bash
./ibctl.py order <buy|sell> SYMBOL QTY [TYPE [PRICE]] [--tif TIF] [--confirm]
```

Without `--confirm`, all orders are dry runs (no IB submission).

### Market orders

```bash
# Dry run
./ibctl.py order buy SPY 100

# Execute
./ibctl.py order buy SPY 100 --confirm
./ibctl.py order sell QQQ 50 --confirm
```

### Limit orders

```bash
./ibctl.py order buy  SPY  100 limit 510.00 --confirm
./ibctl.py order sell AAPL  25 limit 195.50 --confirm
```

### Stop orders

```bash
# Stop sell to protect a long position
./ibctl.py order sell SPY 100 stop 490.00 --confirm
```

### Stop-limit orders

```bash
# Stop triggers at $490, limit fills at $488 or better
./ibctl.py order sell SPY 100 stop-limit 490 488 --confirm
```

### Trailing stop orders

```bash
# Trail by $5.00 (dollar amount)
./ibctl.py order sell SPY 100 trail 5.00 --confirm

# Trail by 1% (percent)
./ibctl.py order sell SPY 100 trail 1% --confirm
```

### Market on Close / Limit on Close

```bash
# MOC — submits into the 4:00 PM closing auction
./ibctl.py order buy SPY 100 moc --confirm

# LOC — limit into the closing auction
./ibctl.py order sell QQQ 50 loc 490.00 --confirm
```

### Market on Open / Limit on Open

```bash
./ibctl.py order buy SPY 100 moo --confirm
./ibctl.py order buy SPY 100 loo 508.00 --confirm
```

### Time-in-Force

Default TIF is `DAY`. Override with `--tif`:

```bash
./ibctl.py order buy SPY 100 limit 510 --tif gtc --confirm   # Good till cancelled
./ibctl.py order sell QQQ 50 limit 490 --tif ioc --confirm   # Immediate or cancel
```

| TIF | Meaning |
|-----|---------|
| day | Good for day (default) |
| gtc | Good till cancelled |
| ioc | Immediate or cancel |
| fok | Fill or kill |
| gtd | Good till date |
| opg | At the open |

## Request Interface

```bash
./ibctl.py plugin request _orders <type> [json_payload]
```

### `list_orders`

Return all orders placed this session.

```bash
./ibctl.py plugin request _orders list_orders
```

```json
{
  "success": true,
  "orders": [
    {
      "order_id": 1001,
      "symbol": "SPY",
      "action": "BUY",
      "quantity": 100,
      "order_type": "MKT",
      "status": "SUBMITTED",
      "placed_at": "2026-05-16T09:35:02"
    },
    {
      "order_id": 1002,
      "symbol": "QQQ",
      "action": "SELL",
      "quantity": 50,
      "order_type": "LMT",
      "status": "FILLED",
      "placed_at": "2026-05-16T09:36:15"
    }
  ]
}
```

### `get_order`

Return details for a specific order.

```bash
./ibctl.py plugin request _orders get_order '{"order_id": 1001}'
```

```json
{
  "success": true,
  "order": {
    "order_id": 1001,
    "symbol": "SPY",
    "action": "BUY",
    "quantity": 100,
    "order_type": "MKT",
    "limit_price": null,
    "stop_price": null,
    "tif": "DAY",
    "status": "FILLED",
    "placed_at": "2026-05-16T09:35:02"
  }
}
```

## Notes

- `_orders` targets US equity stocks via `SMART` routing and `USD` currency by default. To trade other instrument types (options, futures, forex) use the `trade` command from a plugin that holds the appropriate contract.
- Orders placed through `ibctl order` bypass the rate limiter. For high-frequency submission use a strategy plugin.
- Order history is in-memory only and does not persist across engine restarts.
- `_orders` cannot be unloaded; it is always available while the engine is running.
