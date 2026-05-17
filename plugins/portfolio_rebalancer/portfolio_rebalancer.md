# Portfolio Rebalancer Plugin

Full-account portfolio rebalancer. Manages the entire account against a set of target weights defined in `instruments.json`. Replaces the former standalone `ib/rebalancer.py` module with a proper plugin that integrates with the plugin lifecycle, message bus, and order-fill tracking.

## Strategy Overview

Four operating modes:

| Mode | Trigger | Behaviour |
|------|---------|-----------|
| `manual` | Explicit request only | No autonomous background thread |
| `threshold` | Any position drifts beyond `drift_threshold_pct` | Rebalances only drifted positions |
| `calendar` | Fixed schedule (daily/weekly/monthly) | Rebalances all positions to exact targets |
| `combined` | Drift OR schedule, whichever fires first | Either trigger initiates a full rebalance |

## Architecture

```
instruments.json  →  target weights (must sum ~100%)
                  ↓
5-min live bar subscriptions per instrument  →  _price_cache
                  ↓
_autonomous_loop (background thread, modes: threshold/calendar/combined)
  ├── _should_threshold_rebalance()  →  check drift per position
  └── _should_calendar_rebalance()  →  check daily/weekly/monthly schedule
                  ↓
_run_rebalance(dry_run)
  ├── mode=threshold  →  _compute_threshold_trades()  (only drifted positions)
  └── mode=calendar   →  _compute_exact_trades()      (all positions to targets)
                  ↓
sells first (generate cash), then buys by value descending
                  ↓
portfolio.place_order() → IB TWS  (skipped in dry_run)
register_order(oid) → on_order_fill() / on_order_status()
                  ↓
MessageBus publish: portfolio_rebalancer_result
                  ↓
state.json (all parameters, last calendar date, rebalance/fill counts)
```

**Files** (in `plugins/portfolio_rebalancer/`):

| File | Purpose |
|------|---------|
| `plugin.py` | `PortfolioRebalancerPlugin` class |
| `instruments.json` | Target allocations |
| `state.json` | Persisted parameters and counters (auto-created) |
| `holdings.json` | Position snapshot (auto-created) |

## Loading and Funding

The rebalancer manages the **entire account** — it is not restricted to a subset of symbols. Load it, give the account to it, then configure your targets.

```bash
# Load the plugin
./ibctl.py plugin load plugins/portfolio_rebalancer/plugin.py

# Start (loads saved state, starts bar subscriptions, starts background thread if mode != manual)
./ibctl.py plugin start portfolio_rebalancer

# Transfer the full unassigned pool to the rebalancer
./ibctl.py transfer list _unassigned
./ibctl.py transfer cash _unassigned portfolio_rebalancer 100000 --confirm

# Transfer any existing positions you want it to manage
./ibctl.py transfer position _unassigned portfolio_rebalancer SPY 200 --confirm
./ibctl.py transfer position _unassigned portfolio_rebalancer BND 100 --confirm
```

## Instruments and Target Weights

Weights in `instruments.json` must sum to approximately 100%. The plugin normalises them automatically if the sum is outside 98–102%.

```bash
# View current instruments
./ibctl.py plugin instruments list portfolio_rebalancer

# Add or update instruments
./ibctl.py plugin instruments add portfolio_rebalancer SPY --weight 50.0
./ibctl.py plugin instruments add portfolio_rebalancer VEA --weight 30.0
./ibctl.py plugin instruments add portfolio_rebalancer BND --weight 20.0

# Disable an instrument (position held, not rebalanced)
./ibctl.py plugin instruments disable portfolio_rebalancer VEA

# Reload after editing instruments.json directly
./ibctl.py plugin instruments reload portfolio_rebalancer
```

Example `instruments.json` for a three-fund portfolio:

```json
{
  "instruments": [
    {"symbol": "SPY", "name": "US equity",    "weight": 50.0, "enabled": true, "exchange": "SMART", "currency": "USD", "sec_type": "STK"},
    {"symbol": "VEA", "name": "Intl equity",  "weight": 30.0, "enabled": true, "exchange": "SMART", "currency": "USD", "sec_type": "STK"},
    {"symbol": "BND", "name": "Bonds",        "weight": 20.0, "enabled": true, "exchange": "SMART", "currency": "USD", "sec_type": "STK"}
  ]
}
```

## Parameters

All parameters are settable at runtime without restarting.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `mode` | `threshold` | Operating mode: `manual`, `threshold`, `calendar`, `combined` |
| `drift_threshold_pct` | 5.0 | Minimum drift (%) before threshold rebalance fires |
| `min_trade_value` | 100.0 | Ignore trades smaller than this USD value |
| `min_trade_shares` | 1 | Minimum shares per trade |
| `cash_buffer_pct` | 2.0 | Keep this % of portfolio as uninvested cash |
| `max_trades_per_run` | 20 | Cap orders per rebalance run |
| `dry_run` | `true` | If true, compute trades but do not submit to IB |
| `check_interval_secs` | 300 | Autonomous loop check frequency (seconds) |
| `calendar_schedule` | `daily` | Schedule for calendar/combined mode: `daily`, `weekly`, `monthly` |
| `manage_untracked` | `false` | Sell positions absent from instruments.json down to zero |

### Setting parameters via CLI

```bash
# Switch to live trading
./ibctl.py plugin request portfolio_rebalancer set_parameter '{"key": "dry_run", "value": false}'

# Set threshold to 3%
./ibctl.py plugin request portfolio_rebalancer set_parameter '{"key": "drift_threshold_pct", "value": 3.0}'

# Switch to combined (drift OR schedule) mode
./ibctl.py plugin request portfolio_rebalancer set_parameter '{"key": "mode", "value": "combined"}'

# Use monthly calendar
./ibctl.py plugin request portfolio_rebalancer set_parameter '{"key": "calendar_schedule", "value": "monthly"}'

# Sell unclaimed positions
./ibctl.py plugin request portfolio_rebalancer set_parameter '{"key": "manage_untracked", "value": true}'

# Check more frequently (every 2 minutes)
./ibctl.py plugin request portfolio_rebalancer set_parameter '{"key": "check_interval_secs", "value": 120}'
```

## Request Interface

```bash
./ibctl.py plugin request portfolio_rebalancer <type> [json_payload]
```

### `preview`

Compute what trades would be generated right now without submitting orders. Always runs in dry_run mode.

```bash
./ibctl.py plugin request portfolio_rebalancer preview
```

```json
{
  "success": true,
  "data": {
    "dry_run": true,
    "portfolio_value": 105000.00,
    "rebalance_count": 3,
    "trades": [
      {"symbol": "SPY", "action": "BUY",  "shares": 15, "value": 7500.00, "current_weight": 45.2, "target_weight": 50.0},
      {"symbol": "BND", "action": "SELL", "shares": 8,  "value": 680.00,  "current_weight": 20.6, "target_weight": 20.0}
    ]
  }
}
```

### `rebalance`

Execute a rebalance run. Default uses the plugin's `dry_run` setting; override with `{"dry_run": false}`.

```bash
# Dry run preview (respects current dry_run parameter)
./ibctl.py plugin request portfolio_rebalancer rebalance

# Force live execution regardless of dry_run setting
./ibctl.py plugin request portfolio_rebalancer rebalance '{"dry_run": false}'
```

```json
{
  "success": true,
  "data": {
    "dry_run": false,
    "portfolio_value": 105000.00,
    "rebalance_count": 3,
    "trades": [
      {"symbol": "SPY", "action": "BUY", "shares": 15, "value": 7500.00}
    ]
  }
}
```

### `get_status`

Current plugin state, mode, and runtime statistics.

```bash
./ibctl.py plugin request portfolio_rebalancer get_status
```

```json
{
  "success": true,
  "data": {
    "mode": "threshold",
    "dry_run": false,
    "drift_threshold_pct": 5.0,
    "calendar_schedule": "daily",
    "check_interval_secs": 300,
    "rebalance_count": 4,
    "fill_count": 12,
    "last_rebalance_time": "2026-05-16T10:00:00",
    "portfolio_value": 105000.00,
    "autonomous_thread_running": true
  }
}
```

### `get_targets`

Show current weights versus target weights for all instruments.

```bash
./ibctl.py plugin request portfolio_rebalancer get_targets
```

```json
{
  "success": true,
  "data": {
    "portfolio_value": 105000.00,
    "instruments": [
      {"symbol": "SPY", "target_weight": 50.0, "current_weight": 45.2, "drift": -4.8, "target_value": 52500, "current_value": 47460},
      {"symbol": "VEA", "target_weight": 30.0, "current_weight": 31.5, "drift":  1.5, "target_value": 31500, "current_value": 33075},
      {"symbol": "BND", "target_weight": 20.0, "current_weight": 20.6, "drift":  0.6, "target_value": 21000, "current_value": 21630}
    ]
  }
}
```

### `get_last_trades`

Return the trade list from the most recent rebalance run.

```bash
./ibctl.py plugin request portfolio_rebalancer get_last_trades
```

```json
{
  "success": true,
  "data": {
    "trades": [
      {"symbol": "SPY", "action": "BUY", "shares": 15, "value": 7500.00, "current_weight": 45.2, "target_weight": 50.0}
    ]
  }
}
```

### `get_parameters`

Return all current parameter values.

```bash
./ibctl.py plugin request portfolio_rebalancer get_parameters
```

```json
{
  "success": true,
  "data": {
    "mode": "threshold",
    "drift_threshold_pct": 5.0,
    "min_trade_value": 100.0,
    "min_trade_shares": 1,
    "cash_buffer_pct": 2.0,
    "max_trades_per_run": 20,
    "dry_run": false,
    "check_interval_secs": 300,
    "calendar_schedule": "daily",
    "manage_untracked": false
  }
}
```

### `set_parameter`

Update a single parameter.

```bash
./ibctl.py plugin request portfolio_rebalancer set_parameter '{"key": "drift_threshold_pct", "value": 3.0}'
```

```json
{"success": true, "message": "drift_threshold_pct set to 3.0"}
```

## Message Bus

The rebalancer subscribes to `portfolio_rebalancer_cmd` and publishes results to `portfolio_rebalancer_result`.

### Commands (subscribe channel: `portfolio_rebalancer_cmd`)

Other plugins or external publishers can trigger rebalances without the CLI:

```bash
# Trigger a live rebalance
./ibctl.py plugin message portfolio_rebalancer '{"command": "rebalance"}'

# Override dry_run for this one run
./ibctl.py plugin message portfolio_rebalancer '{"command": "rebalance", "dry_run": false}'

# Preview only
./ibctl.py plugin message portfolio_rebalancer '{"command": "preview"}'

# Change mode
./ibctl.py plugin message portfolio_rebalancer '{"command": "set_mode", "mode": "calendar"}'

# Set parameter
./ibctl.py plugin message portfolio_rebalancer '{"command": "set_parameter", "key": "drift_threshold_pct", "value": 2.5}'
```

### Results (publish channel: `portfolio_rebalancer_result`)

After every rebalance run the plugin publishes the trade list. Subscribe from another plugin to act on rebalance events:

```python
# In another plugin
self.subscribe("portfolio_rebalancer_result", self._on_rebalance)

def _on_rebalance(self, message):
    trades = message.payload.get("trades", [])
    print(f"Rebalancer placed {len(trades)} trades")
```

## Common Workflows

### First-time setup (threshold mode, live trading)

```bash
# 1. Load and start
./ibctl.py plugin load plugins/portfolio_rebalancer/plugin.py
./ibctl.py plugin start portfolio_rebalancer

# 2. Configure instruments
./ibctl.py plugin instruments add portfolio_rebalancer SPY --weight 60.0
./ibctl.py plugin instruments add portfolio_rebalancer BND --weight 40.0

# 3. Fund the plugin
./ibctl.py transfer cash _unassigned portfolio_rebalancer 100000 --confirm

# 4. Preview before going live
./ibctl.py plugin request portfolio_rebalancer preview

# 5. Switch to live and set threshold
./ibctl.py plugin request portfolio_rebalancer set_parameter '{"key": "dry_run", "value": false}'
./ibctl.py plugin request portfolio_rebalancer set_parameter '{"key": "drift_threshold_pct", "value": 5.0}'

# 6. Run the initial rebalance to establish positions
./ibctl.py plugin request portfolio_rebalancer rebalance '{"dry_run": false}'
```

### Monthly calendar rebalance

```bash
./ibctl.py plugin request portfolio_rebalancer set_parameter '{"key": "mode", "value": "calendar"}'
./ibctl.py plugin request portfolio_rebalancer set_parameter '{"key": "calendar_schedule", "value": "monthly"}'
./ibctl.py plugin request portfolio_rebalancer set_parameter '{"key": "dry_run", "value": false}'
```

The autonomous thread will fire a full rebalance on the first trading day of each new month.

### Manual-only rebalance (no background thread)

```bash
./ibctl.py plugin request portfolio_rebalancer set_parameter '{"key": "mode", "value": "manual"}'

# Trigger explicitly whenever desired
./ibctl.py plugin request portfolio_rebalancer rebalance '{"dry_run": false}'
```

### Clean up stale positions

```bash
# Enable manage_untracked to sell anything not in instruments.json
./ibctl.py plugin request portfolio_rebalancer set_parameter '{"key": "manage_untracked", "value": true}'
./ibctl.py plugin request portfolio_rebalancer rebalance '{"dry_run": false}'
```

## Lifecycle

```bash
# Freeze (stops autonomous thread, saves state — positions preserved)
./ibctl.py plugin freeze portfolio_rebalancer

# Resume (restarts thread if mode != manual, reopens bar subscriptions)
./ibctl.py plugin resume portfolio_rebalancer

# Stop (full state save including last_calendar_date for schedule continuity)
./ibctl.py plugin stop portfolio_rebalancer
```

State persisted on stop/freeze: all parameters, `last_calendar_date` (prevents double-firing on restart), `rebalance_count`, `fill_count`.
