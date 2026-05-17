# Dummy Plugin

A placeholder plugin that maintains static allocations and always returns HOLD signals. Use it as a template when building new plugins, a baseline for comparing plugin performance, or a slot-filler while other plugins are being developed.

## Architecture

The dummy plugin holds a set of instruments with target weights (defined in `instruments.json`) but never generates BUY or SELL signals — all signals are HOLD. It does persist its run counter and last signals across restarts via `state.json`.

```
instruments.json  →  target weights (not traded)
                  ↓
calculate_signals()  →  HOLD for every enabled instrument
                  ↓
MessageBus publish: dummy_signals  (run number, signals, timestamp)
                  ↓
state.json  (run_counter, last_signals on stop/freeze)
```

**Files** (in `plugins/dummy/`):

| File | Purpose |
|------|---------|
| `plugin.py` | `DummyPlugin` class |
| `instruments.json` | Target instruments and weights |
| `state.json` | Persisted run counter (auto-created) |
| `holdings.json` | Plugin holdings snapshot (auto-created) |

## Loading and Lifecycle

```bash
# Load the plugin (engine must be running)
./ibctl.py plugin load plugins/dummy/plugin.py

# Check it loaded
./ibctl.py plugin list

# Start it
./ibctl.py plugin start dummy

# Manually trigger one run (generates HOLD signals)
./ibctl.py plugin trigger dummy

# Pause without losing state
./ibctl.py plugin freeze dummy

# Restore from freeze
./ibctl.py plugin resume dummy

# Stop and persist state
./ibctl.py plugin stop dummy
```

## Funding

The dummy plugin does not trade, so funding is optional. If you want it to represent a slice of the account for tracking purposes, transfer cash from `_unassigned`:

```bash
# Show available unassigned assets
./ibctl.py transfer list _unassigned

# Allocate $50,000 to the dummy plugin
./ibctl.py transfer cash _unassigned dummy 50000 --confirm

# Transfer an existing position
./ibctl.py transfer position _unassigned dummy SPY 100 --confirm
```

## Instruments

The instruments list controls which symbols appear in HOLD signals. Weights do not affect trading but are recorded in signal output.

```bash
# View current instruments
./ibctl.py plugin instruments list dummy

# Add an instrument
./ibctl.py plugin instruments add dummy AAPL --weight 30.0

# Disable an instrument (excluded from signals)
./ibctl.py plugin instruments disable dummy AAPL

# Re-enable
./ibctl.py plugin instruments enable dummy AAPL

# Remove permanently
./ibctl.py plugin instruments remove dummy AAPL
```

Or edit `plugins/dummy/instruments.json` directly and reload:

```bash
./ibctl.py plugin instruments reload dummy
```

Example `instruments.json`:

```json
{
  "instruments": [
    {"symbol": "SPY", "name": "S&P 500 ETF", "weight": 60.0, "enabled": true},
    {"symbol": "BND", "name": "Total Bond ETF", "weight": 40.0, "enabled": true}
  ]
}
```

## Request Interface

```bash
./ibctl.py plugin request dummy <type> [json_payload]
```

### `get_stats`

Return run counter, instrument counts, and plugin state.

```bash
./ibctl.py plugin request dummy get_stats
```

```json
{
  "success": true,
  "data": {
    "run_counter": 42,
    "instruments": 2,
    "enabled_instruments": 2,
    "state": "STARTED"
  }
}
```

### `get_last_signals`

Return the signals generated on the most recent run.

```bash
./ibctl.py plugin request dummy get_last_signals
```

```json
{
  "success": true,
  "data": {
    "signals": [
      {"symbol": "SPY", "action": "HOLD", "target_weight": 60.0, "reason": "Dummy plugin - no action taken"},
      {"symbol": "BND", "action": "HOLD", "target_weight": 40.0, "reason": "Dummy plugin - no action taken"}
    ]
  }
}
```

### `reset_counter`

Reset the run counter to 0.

```bash
./ibctl.py plugin request dummy reset_counter
```

```json
{"success": true, "message": "Run counter reset to 0"}
```

## Message Bus

The dummy plugin publishes to the `dummy_signals` channel after each run. Other plugins can subscribe to observe activity:

```python
# Example from another plugin
self.subscribe("dummy_signals", self._on_dummy_signals)

def _on_dummy_signals(self, message):
    print(message.payload["run_number"], message.payload["signals"])
```

## Using as a Template

Copy the plugin directory and rename it:

```bash
cp -r plugins/dummy plugins/my_strategy
mv plugins/my_strategy/plugin.py plugins/my_strategy/plugin.py
```

Then in `plugin.py`:
1. Rename `DummyPlugin` to your class name
2. Replace the `calculate_signals()` body with your strategy logic
3. Update `instruments.json` with your target symbols
