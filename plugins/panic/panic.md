# Panic Plugin

Emergency position liquidation plugin. Accepts positions deposited into its control and generates SELL signals to close them, prioritised by urgency. Designed for rapid risk reduction when a position must be exited quickly.

## Architecture

Positions are deposited into a queue via the `deposit` request. On each engine run `calculate_signals()` drains the queue, sorts by urgency (3 = highest, 1 = lowest), and emits SELL signals. The queue and history survive restarts via `state.json`.

```
deposit request  →  _queued_positions list
                  ↓
calculate_signals()  →  sorted SELL signals (urgency 3→1)
                  ↓
engine → OrderReconciler → IB order placement
                  ↓
_closed_history  →  state.json
```

**Urgency mapping:**

| Value | Label | Execution style |
|-------|-------|-----------------|
| 3 | Urgent | Processed first, aggressively filled |
| 2 | Normal | Middle priority (default) |
| 1 | Patient | Processed last |

**Files** (in `plugins/panic/`):

| File | Purpose |
|------|---------|
| `plugin.py` | `PanicPlugin` class |
| `state.json` | Persisted queue and history (auto-created) |

## Loading and Lifecycle

```bash
# Load the plugin
./ibctl.py plugin load plugins/panic/plugin.py

# Verify it loaded
./ibctl.py plugin list

# Start (required before deposits will be acted on)
./ibctl.py plugin start panic

# Stop (state saved — queue survives restart)
./ibctl.py plugin stop panic
```

The panic plugin does not require funding or instruments; positions are deposited directly by name.

## Depositing Positions for Liquidation

```bash
./ibctl.py plugin request panic deposit '<json>'
```

Single position (urgent):

```bash
./ibctl.py plugin request panic deposit '{"positions": [{"symbol": "SPY", "quantity": 100, "urgency": 3}]}'
```

Multiple positions with mixed urgency:

```bash
./ibctl.py plugin request panic deposit '{
  "positions": [
    {"symbol": "SPY",  "quantity": 200, "urgency": 3},
    {"symbol": "QQQ",  "quantity": 50,  "urgency": 2},
    {"symbol": "AAPL", "quantity": 25,  "urgency": 1}
  ]
}'
```

Deposit without urgency (defaults to 2 = Normal):

```bash
./ibctl.py plugin request panic deposit '{"positions": [{"symbol": "GLD", "quantity": 75}]}'
```

On the next engine run, signals are generated in order: SPY → QQQ → AAPL → GLD.

## Request Interface

```bash
./ibctl.py plugin request panic <type> [json_payload]
```

### `deposit`

Add positions to the liquidation queue.

```bash
./ibctl.py plugin request panic deposit '{"positions": [{"symbol": "TSLA", "quantity": 30, "urgency": 2}]}'
```

```json
{
  "success": true,
  "data": {
    "added": 1,
    "positions": [
      {"symbol": "TSLA", "quantity": 30, "urgency": 2, "deposited_at": "2026-05-16T09:35:00"}
    ]
  }
}
```

### `get_queue`

Inspect all positions currently waiting to be liquidated.

```bash
./ibctl.py plugin request panic get_queue
```

```json
{
  "success": true,
  "data": {
    "count": 2,
    "queued_positions": [
      {"symbol": "SPY",  "quantity": 200, "urgency": 3, "deposited_at": "2026-05-16T09:35:00"},
      {"symbol": "AAPL", "quantity": 25,  "urgency": 1, "deposited_at": "2026-05-16T09:35:00"}
    ]
  }
}
```

### `get_history`

Return the log of positions that have already been submitted for liquidation.

```bash
./ibctl.py plugin request panic get_history
```

```json
{
  "success": true,
  "data": {
    "count": 3,
    "closed_history": [
      {"symbol": "QQQ", "quantity": 50, "urgency": 2, "deposited_at": "...", "closed_at": "..."}
    ]
  }
}
```

### `clear_queue`

Remove all queued positions without sending any orders (abort a pending liquidation).

```bash
./ibctl.py plugin request panic clear_queue
```

```json
{"success": true, "message": "Cleared 2 positions from queue"}
```

## Common Workflows

### Emergency liquidation of a portfolio slice

```bash
# Deposit everything that must go, urgency 3
./ibctl.py plugin request panic deposit '{
  "positions": [
    {"symbol": "SPY",  "quantity": 500, "urgency": 3},
    {"symbol": "NVDA", "quantity": 100, "urgency": 3}
  ]
}'

# Trigger the plugin immediately (don't wait for next scheduled run)
./ibctl.py plugin trigger panic

# Confirm orders were generated
./ibctl.py plugin request panic get_history
```

### Staged liquidation over multiple runs

```bash
# First, queue urgent positions
./ibctl.py plugin request panic deposit '{"positions": [{"symbol": "SPY", "quantity": 500, "urgency": 3}]}'

# On next run, queue medium positions
./ibctl.py plugin request panic deposit '{"positions": [{"symbol": "BND", "quantity": 100, "urgency": 2}]}'
```

### Aborting before signals fire

```bash
# Check what's queued
./ibctl.py plugin request panic get_queue

# Cancel before the next engine run processes it
./ibctl.py plugin request panic clear_queue
```

## Notes

- The queue persists across engine restarts. If the engine stops with positions in the queue, they will be acted on when it restarts.
- `clear_queue` does not cancel orders already submitted to IB. Use `./ibctl.py order cancel <id>` for that.
- Positions deposited with `quantity` ≤ 0 or `urgency` outside 1–3 are rejected with an error message in the response.
