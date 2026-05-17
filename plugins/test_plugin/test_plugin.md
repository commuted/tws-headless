# Test Plugin

Canonical reference plugin used by the test suite. It exercises every `PluginBase` feature — lifecycle callbacks, state persistence, live-bar subscriptions, MessageBus, fill/order/commission hooks, and custom request handling. It always generates HOLD signals and never places real orders.

Use it as:
- A reference implementation when building a new plugin
- A stable loading target to verify `PluginLoader` and `PluginExecutive` behaviour
- A slot-filler during development when a real strategy is not yet ready

## Architecture

```
instruments.json  →  SPY, TLT (default; overridable)
                  ↓
subscribe_live_bars()  →  on_bar() callback  →  _bar_cache
                  ↓
calculate_signals()  →  HOLD for every enabled instrument
                  ↓
MessageBus subscribe: risk_alert  →  _on_risk_alert
state.json  (signal_count, fill_count, custom_value, alerts_suspended)
```

**Files** (in `plugins/test_plugin/`):

| File | Purpose |
|------|---------|
| `plugin.py` | `TestPlugin` class |
| `instruments.json` | Default SPY/TLT instruments |
| `state.json` | Persisted counters (auto-created) |

## Loading and Lifecycle

```bash
# Load with default name "test_plugin"
./ibctl.py plugin load plugins/test_plugin/plugin.py

# Start
./ibctl.py plugin start test_plugin

# Trigger one run manually
./ibctl.py plugin trigger test_plugin

# Freeze (saves state, cancels bar subscriptions)
./ibctl.py plugin freeze test_plugin

# Resume (reopens subscriptions, restores state from memory)
./ibctl.py plugin resume test_plugin

# Stop (saves state to state.json, cancels subscriptions)
./ibctl.py plugin stop test_plugin

# Unload
./ibctl.py plugin unload test_plugin
```

## Request Interface

```bash
./ibctl.py plugin request test_plugin <type> [json_payload]
```

### `get_stats`

Return signal/fill counts, subscription state, and custom value.

```bash
./ibctl.py plugin request test_plugin get_stats
```

```json
{
  "success": true,
  "signal_count": 12,
  "fill_count": 0,
  "last_signal_time": "2026-05-16T09:45:00",
  "live_subscriptions": 2,
  "cached_symbols": ["SPY", "TLT"],
  "alerts_suspended": false,
  "custom_value": ""
}
```

### `set_custom_value`

Store an arbitrary string in persisted state (useful for testing state save/restore).

```bash
./ibctl.py plugin request test_plugin set_custom_value '{"value": "hello world"}'
```

```json
{"success": true, "custom_value": "hello world"}
```

### `reset`

Zero all counters and clear `custom_value`.

```bash
./ibctl.py plugin request test_plugin reset
```

```json
{"success": true}
```

### `suspend_alerts`

Stop signal generation. Simulates a risk halt — `calculate_signals()` returns an empty list until resumed.

```bash
./ibctl.py plugin request test_plugin suspend_alerts
```

```json
{"success": true}
```

### `resume_alerts`

Re-enable signal generation after a `suspend_alerts`.

```bash
./ibctl.py plugin request test_plugin resume_alerts
```

```json
{"success": true}
```

## CLI Help

```bash
./ibctl.py plugin help test_plugin
```

## MessageBus

The plugin subscribes to the `risk_alert` channel on start. Sending a risk alert from another plugin or the engine will be logged. This demonstrates the inter-plugin communication pattern:

```bash
# Send a risk alert from the command line
./ibctl.py plugin message test_plugin '{"channel": "risk_alert", "payload": {"reason": "manual test"}}'
```

## Instruments

Default instruments (SPY 60%, TLT 40%) are defined in `instruments.json`. They are never traded but appear in HOLD signals.

```bash
./ibctl.py plugin instruments list test_plugin

# Add a third symbol for testing
./ibctl.py plugin instruments add test_plugin QQQ --weight 20.0
```

## Notes

- `INSTRUMENT_COMPLIANCE = True` means the plugin will reject fill callbacks for symbols not in its instrument list.
- Live-bar subscriptions are opened on `start()` and `resume()`, cancelled on `stop()` and `freeze()`.
- `TestPluginState` (signal_count, fill_count, last_signal_time, custom_value, alerts_suspended) is the persisted state structure; examine `state.json` after a stop to see the format.
