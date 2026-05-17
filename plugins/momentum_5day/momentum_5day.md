# Momentum 5-Day Plugin

A reallocation plugin that ranks instruments by their 5-day return momentum and adjusts target weights accordingly — overweighting positive-momentum assets and underweighting (or selling) negative-momentum ones. Signals are only generated when drift from the current target exceeds `rebalance_threshold`.

## Architecture

```
instruments.json  →  universe of instruments
                  ↓
subscribe_live_bars()  →  on_bar() callback  →  _bar_cache (daily bars)
                  ↓
calculate_signals()
  ├── compute MomentumMetrics per symbol (5d return, 1d return, volatility, score)
  ├── rank by momentum score
  ├── derive target weights (base weight × momentum adjustment)
  ├── compare to current holdings → drift check
  └── emit BUY / SELL / HOLD signals (filtered by rebalance_threshold)
                  ↓
OrderReconciler → rate limiter → IB orders
                  ↓
on_order_fill()  →  _fill_count, _last_fill_time
on_order_status()  →  terminal status cleanup
                  ↓
MessageBus subscribe: risk_alert  →  _signals_suspended gate
state.json  (run_counter, metrics, weights, fill count, suspended flag)
```

**Files** (in `plugins/momentum_5day/`):

| File | Purpose |
|------|---------|
| `plugin.py` | `Momentum5DayPlugin` class |
| `instruments.json` | Target instrument universe |
| `state.json` | Persisted state (auto-created) |
| `holdings.json` | Plugin holdings snapshot (auto-created) |

## Loading and Funding

```bash
# Load the plugin
./ibctl.py plugin load plugins/momentum_5day/plugin.py

# Verify
./ibctl.py plugin list

# Start (opens bar subscriptions, registers for risk_alert)
./ibctl.py plugin start momentum_5day

# Fund: transfer $50,000 from the unassigned pool
./ibctl.py transfer cash _unassigned momentum_5day 50000 --confirm

# Or transfer an existing position
./ibctl.py transfer position _unassigned momentum_5day SPY 100 --confirm
```

## Instruments

Define the universe in `instruments.json`. The momentum algorithm works best with 3–10 instruments that share a common economic theme (e.g., sector ETFs, factor ETFs).

```bash
# List current instruments
./ibctl.py plugin instruments list momentum_5day

# Add instruments
./ibctl.py plugin instruments add momentum_5day SPY --weight 25.0
./ibctl.py plugin instruments add momentum_5day QQQ --weight 25.0
./ibctl.py plugin instruments add momentum_5day IWM --weight 25.0
./ibctl.py plugin instruments add momentum_5day EFA --weight 25.0

# Disable one temporarily
./ibctl.py plugin instruments disable momentum_5day EFA

# Remove permanently
./ibctl.py plugin instruments remove momentum_5day IWM
```

Example `instruments.json` for a 4-sector momentum rotation:

```json
{
  "instruments": [
    {"symbol": "XLK", "name": "Technology",    "weight": 25.0, "enabled": true},
    {"symbol": "XLV", "name": "Healthcare",    "weight": 25.0, "enabled": true},
    {"symbol": "XLE", "name": "Energy",        "weight": 25.0, "enabled": true},
    {"symbol": "XLF", "name": "Financials",    "weight": 25.0, "enabled": true}
  ]
}
```

## Parameters

All parameters are tunable at runtime without restarting the plugin.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `lookback_days` | 5 | Days of daily bars used to compute returns |
| `rebalance_threshold` | 5.0 | Minimum % drift from target before a signal is generated |
| `momentum_weight` | 0.5 | Fraction (0–1) by which momentum score adjusts base weight |
| `min_position_size` | 1000.0 | Minimum position value in USD; smaller positions are skipped |

### Setting parameters

```bash
./ibctl.py plugin request momentum_5day set_parameter '{"key": "lookback_days", "value": 10}'
./ibctl.py plugin request momentum_5day set_parameter '{"key": "rebalance_threshold", "value": 3.0}'
./ibctl.py plugin request momentum_5day set_parameter '{"key": "momentum_weight", "value": 0.7}'
./ibctl.py plugin request momentum_5day set_parameter '{"key": "min_position_size", "value": 500}'
```

## Request Interface

```bash
./ibctl.py plugin request momentum_5day <type> [json_payload]
```

### `get_metrics`

Return current momentum metrics for each tracked instrument.

```bash
./ibctl.py plugin request momentum_5day get_metrics
```

```json
{
  "success": true,
  "data": {
    "XLK": {
      "symbol": "XLK",
      "returns_5d": 0.032,
      "returns_1d": 0.008,
      "avg_return": 0.0064,
      "volatility": 0.011,
      "momentum_score": 0.72,
      "trend": "up"
    },
    "XLE": {
      "symbol": "XLE",
      "returns_5d": -0.018,
      "returns_1d": -0.005,
      "avg_return": -0.0036,
      "volatility": 0.015,
      "momentum_score": -0.38,
      "trend": "down"
    }
  }
}
```

### `get_stats`

Return plugin operational statistics.

```bash
./ibctl.py plugin request momentum_5day get_stats
```

```json
{
  "success": true,
  "data": {
    "run_counter": 8,
    "instruments": 4,
    "enabled_instruments": 4,
    "state": "STARTED",
    "lookback_days": 5,
    "rebalance_threshold": 5.0,
    "momentum_weight": 0.5,
    "fill_count": 3,
    "last_fill_time": "2026-05-16T10:05:22",
    "live_subscriptions": 4,
    "cached_symbols": ["XLK", "XLV", "XLE", "XLF"],
    "signals_suspended": false
  }
}
```

### `get_parameters`

Return current parameter values.

```bash
./ibctl.py plugin request momentum_5day get_parameters
```

```json
{
  "success": true,
  "data": {
    "lookback_days": 5,
    "rebalance_threshold": 5.0,
    "momentum_weight": 0.5,
    "min_position_size": 1000.0
  }
}
```

### `set_parameter`

Update a single parameter at runtime.

```bash
./ibctl.py plugin request momentum_5day set_parameter '{"key": "rebalance_threshold", "value": 3.0}'
```

```json
{"success": true, "message": "rebalance_threshold set to 3.0"}
```

### `get_signals_history`

Return the last N signal runs (up to 100 stored).

```bash
./ibctl.py plugin request momentum_5day get_signals_history
```

```json
{
  "success": true,
  "data": {
    "history": [
      {
        "run": 8,
        "timestamp": "2026-05-16T10:05:00",
        "signals": [
          {"symbol": "XLK", "action": "BUY",  "target_weight": 32.5, "momentum_score": 0.72},
          {"symbol": "XLE", "action": "SELL", "target_weight": 17.5, "momentum_score": -0.38}
        ]
      }
    ]
  }
}
```

### `get_momentum_summary`

Return a human-readable momentum ranking table.

```bash
./ibctl.py plugin request momentum_5day get_momentum_summary
```

```json
{
  "success": true,
  "data": {
    "summary": [
      {"rank": 1, "symbol": "XLK", "momentum_score": 0.72,  "trend": "up",      "returns_5d": "3.20%"},
      {"rank": 2, "symbol": "XLV", "momentum_score": 0.15,  "trend": "neutral", "returns_5d": "0.60%"},
      {"rank": 3, "symbol": "XLF", "momentum_score": -0.10, "trend": "down",    "returns_5d": "-0.30%"},
      {"rank": 4, "symbol": "XLE", "momentum_score": -0.38, "trend": "down",    "returns_5d": "-1.80%"}
    ]
  }
}
```

### `reset_alerts`

Clear the risk-gate (`signals_suspended`) and resume normal signal generation. Use this after a `risk_alert` message has halted the plugin.

```bash
./ibctl.py plugin request momentum_5day reset_alerts
```

```json
{"success": true, "message": "Signals resumed"}
```

## Risk Alert Integration

The plugin subscribes to the `risk_alert` MessageBus channel. Any plugin or system component can publish to this channel to halt all signal generation:

```bash
# Suspend via bus (from another plugin or external publisher)
./ibctl.py plugin message momentum_5day '{"channel": "risk_alert", "payload": {"reason": "drawdown limit hit"}}'

# Confirm suspended
./ibctl.py plugin request momentum_5day get_stats
# "signals_suspended": true

# Re-enable
./ibctl.py plugin request momentum_5day reset_alerts
```

## Lifecycle

```bash
# Freeze (saves state, cancels bar subscriptions, halts signals)
./ibctl.py plugin freeze momentum_5day

# Resume (reopens subscriptions, restores in-memory state)
./ibctl.py plugin resume momentum_5day

# Stop (saves state to state.json)
./ibctl.py plugin stop momentum_5day
```

State persisted on stop/freeze: run counter, momentum metrics, last target weights, fill count, and the signals-suspended flag.
