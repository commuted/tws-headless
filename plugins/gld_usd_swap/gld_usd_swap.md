# GLD/USD Swap Plugin

A multi-factor gold/cash rotation strategy that captures two independent return streams from GLD: the unconditional overnight drift and a regime-conditional intraday edge.

## Strategy Overview

Two decisions per trading day:

**At open (09:30):** Check the composite regime saved at the prior close. If the regime is `gold`, hold GLD through the intraday session. If `cash`, sell at open and sit out intraday (re-buy at close via MOC).

**At close (15:45):** Place a MOC (Market on Close) order to buy GLD for the overnight hold — fills at the 16:00 closing auction. Save the current composite regime for tomorrow's open decision.

```
15:45 MOC buy  →  overnight GLD hold  →  09:30 open decision
                                          ├── regime=gold: hold intraday
                                          └── regime=cash: MKT sell, sit out,
                                              15:45 MOC re-buy at close
```

## Composite Regime Signal

Three-factor signal computed from 5-min bars of UUP, TLT, and RINF:

| Signal | Condition | Interpretation |
|--------|-----------|----------------|
| UUP | fast SMA < slow SMA | USD weakening → gold bullish |
| TLT | fast SMA > slow SMA | Nominal rates falling → gold bullish |
| RINF | fast SMA > slow SMA | Inflation expectations rising → gold bullish |
| Meta | GLD 20-bar SMA > 60-bar SMA | GLD structural uptrend active |

**Regime logic:**
- If GLD is in a structural uptrend (meta=True): `gold = UUP AND (TLT OR RINF)`
- Otherwise: `gold = UUP AND TLT`

## Architecture

```
5-min live bars: GLD, UUP, TLT, RINF
                  ↓
StreamingTriangleTooth smoother per ETF
                  ↓
fast/slow SMA → composite regime
                  ↓
09:30 bar:  _on_market_open()   → MKT sell (if regime=cash)
15:45 bar:  _on_market_close()  → MOC buy  + save regime
                  ↓
portfolio.place_order() → IB TWS
                  ↓
on_order_fill() / on_order_status() → fill tracking
state.json (regime, SMAs, trade count, allocation)
```

**Files** (in `plugins/gld_usd_swap/`):

| File | Purpose |
|------|---------|
| `plugin.py` | `GldUsdSwapPlugin` class |
| `instruments.json` | GLD + UUP, TLT, RINF (signal ETFs) |
| `state.json` | Persisted regime, SMAs, counters (auto-created) |
| `holdings.json` | GLD position snapshot (auto-created) |

## Loading and Funding

```bash
# Load the plugin
./ibctl.py plugin load plugins/gld_usd_swap/plugin.py

# Start (opens 5-min bar subscriptions for GLD, UUP, TLT, RINF)
./ibctl.py plugin start gld_usd_swap

# Fund: transfer allocation capital from _unassigned
# Default allocation_dollars = 10,000; ensure at least this much cash is available
./ibctl.py transfer cash _unassigned gld_usd_swap 10000 --confirm

# To trade an existing GLD position, transfer the shares too
./ibctl.py transfer position _unassigned gld_usd_swap GLD 25 --confirm
```

**Funding is enforced.** With a live portfolio the MOC buy is sized as
`int(min(allocation_dollars, plugin cash) / gld_price)` — an unfunded plugin
places no orders (an error is logged at each skipped close). Increase
`allocation_dollars` *and* transfer matching cash to scale up.

**Fills update holdings automatically.** Each BUY/SELL fill adds/removes the
GLD position in `holdings.json` and adjusts the plugin's cash by the fill
value, so the open-time sell always operates on exactly the shares this
plugin bought. (MOC fills at the closing auction, which can differ slightly
from the 15:45 price; small cash drift is corrected by account
reconciliation.)

## Parameters

All parameters are tunable at runtime.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `fast_bars` | 5 | Fast SMA window for UUP/TLT/RINF (25 min at 5-min bars) |
| `slow_bars` | 20 | Slow SMA window (100 min) |
| `meta_fast_bars` | 20 | GLD structural trend fast SMA |
| `meta_slow_bars` | 60 | GLD structural trend slow SMA |
| `vol_window` | 20 | Rolling window for derivative (spike) estimation |
| `derivative_percentile` | 50 | p50 of recent `|Δclose|` sets smoother slope limit |
| `allocation_dollars` | 10000 | USD capital allocated to GLD positions |

### Setting parameters

```bash
# Increase allocation
./ibctl.py plugin request gld_usd_swap set_parameter '{"key": "allocation_dollars", "value": 25000}'

# Shorten SMA windows for faster response
./ibctl.py plugin request gld_usd_swap set_parameter '{"key": "fast_bars", "value": 3}'
./ibctl.py plugin request gld_usd_swap set_parameter '{"key": "slow_bars", "value": 12}'

# Widen derivative percentile for less aggressive smoothing
./ibctl.py plugin request gld_usd_swap set_parameter '{"key": "derivative_percentile", "value": 75}'
```

## Request Interface

```bash
./ibctl.py plugin request gld_usd_swap <type> [json_payload]
```

### `get_status`

Full runtime status: position, regime, signal factors, price, trade stats.

```bash
./ibctl.py plugin request gld_usd_swap get_status
```

```json
{
  "success": true,
  "data": {
    "version": "3.0.0",
    "holding_gld": true,
    "regime": "gold",
    "regime_at_prior_close": "gold",
    "gld_price": 238.45,
    "uup_price": 27.31,
    "tlt_price": 88.40,
    "rinf_price": 22.15,
    "gld_in_uptrend": true,
    "gld_meta_fast": 235.12,
    "gld_meta_slow": 228.40,
    "uup_warmed_up": true,
    "tlt_warmed_up": true,
    "rinf_warmed_up": true,
    "meta_warmed_up": true,
    "uup_bars": 45,
    "tlt_bars": 45,
    "rinf_bars": 45,
    "uup_derivative": 0.00412,
    "tlt_derivative": 0.02100,
    "rinf_derivative": 0.00850,
    "signal_factors": {
      "mode": "UUP+TLT|RINF(meta)",
      "uup_fast": 27.28,
      "uup_slow": 27.35,
      "tlt_fast": 88.52,
      "tlt_slow": 88.10,
      "rinf_fast": 22.20,
      "rinf_slow": 22.05,
      "gld_uptrend": true,
      "regime": "gold"
    },
    "trade_count": 5,
    "overnight_holds": 12,
    "intraday_holds": 9,
    "last_trade_time": "2026-05-15T15:50:02"
  }
}
```

### `get_parameters`

Return current tunable parameters.

```bash
./ibctl.py plugin request gld_usd_swap get_parameters
```

```json
{
  "success": true,
  "data": {
    "fast_bars": 5,
    "slow_bars": 20,
    "meta_fast_bars": 20,
    "meta_slow_bars": 60,
    "vol_window": 20,
    "derivative_percentile": 50,
    "allocation_dollars": 10000
  }
}
```

### `set_parameter`

Update a single parameter.

```bash
./ibctl.py plugin request gld_usd_swap set_parameter '{"key": "allocation_dollars", "value": 20000}'
```

```json
{"success": true, "message": "allocation_dollars set to 20000"}
```

### `force_regime`

Override `regime_at_prior_close` — the value that determines tomorrow's open decision. Useful for testing or manual intervention.

Valid values: `"gold"`, `"cash"`, `"unknown"`.

```bash
# Force the plugin to treat tomorrow's open as a cash day
./ibctl.py plugin request gld_usd_swap force_regime '{"regime": "cash"}'
```

```json
{"success": true, "message": "regime_at_prior_close=cash"}
```

```bash
# Restore normal regime-driven behaviour
./ibctl.py plugin request gld_usd_swap force_regime '{"regime": "gold"}'
```

## Lifecycle

```bash
# Freeze (cancels bar subscriptions — no bars, no orders while frozen — and saves state)
./ibctl.py plugin freeze gld_usd_swap

# Resume (reopens subscriptions; SMAs warm up from fresh bars)
./ibctl.py plugin resume gld_usd_swap

# Stop (full state save)
./ibctl.py plugin stop gld_usd_swap
```

State persisted on stop/freeze **and at every session decision** (09:30 and
15:45): `holding_gld`, current regime, `regime_at_prior_close`,
`allocation_dollars`, all SMA parameters, trade/hold counts, and any
in-flight orders. The regime saved at close therefore survives an overnight
crash.

### Crash recovery

If the engine dies between the 15:45 MOC placement and the 16:00 fill, the
order is restored from state on the next start and treated conservatively:
the plugin assumes the buy filled (`holding_gld=True`) so it can never place
a duplicate overnight buy, and logs an error asking you to verify the fill
in TWS. If the order did fill while the engine was down, the shares land in
`_unassigned` at startup reconciliation — transfer them back:

```bash
./ibctl.py plugin request gld_usd_swap get_status   # check holding_gld / pending
./ibctl.py transfer position _unassigned gld_usd_swap GLD <qty> --confirm
```

If it did not fill, clear the stale flag with
`force_regime` / a fresh `plugin stop` + `start` once holdings are correct.

### Safety gating

Three layers, outermost last:

1. **Backfill/live separation** — session decisions fire only on bars
   delivered through the live-update callback (`historicalDataUpdate`) with
   a timestamp strictly newer than any bar seen. Backfill replay on
   startup/resume feeds indicator state only.
2. **Wall-clock validity windows** — even a bar that passes layer 1 is
   refused unless it is today's bar AND the New York wall clock is inside
   the decision's window: open 09:30–09:45 ET, close 15:45–15:55 ET (the
   MOC submission cutoff). This is the absolute bound: IB has been observed
   re-delivering the day's 15:45 bar through the live-update path on an
   after-hours restart, which would otherwise place an MOC that queues for
   the NEXT day's close. Refused decisions are logged warnings, never
   orders.
3. **Engine `--mode dry_run`** suppresses this plugin's orders at the
   portfolio level.

### Anomaly alerting

The plugin publishes to the MessageBus `alerts` channel (sunk by the
watchdog plugin to `alerts.jsonl` + optional webhook — see
`plugins/watchdog/watchdog.md`) on:

| Kind | Condition |
|------|-----------|
| `ib_error` | IB reports an error for one of this plugin's orders or data requests |
| `order_terminal` | An order is cancelled/rejected/errored without filling |
| `stuck_order` | A placed order has no fill or terminal status after 30 min (bar-driven; the watchdog also checks on wall clock, covering the overnight MOC window) |
| `bar_parse_failure` | 10 consecutive unparseable bar timestamps — session decisions cannot fire; check TWS date format/timezone |

On reconnect after a dropped TWS connection, the plugin automatically
re-creates its live-bar subscriptions (`on_reconnect`); without this it
would hold positions with a silently dead feed.

## Warm-Up Period

Signal factors require history before activating:
- UUP/TLT/RINF: `slow_bars` × 5-minute bars (100 min at default settings)
- Meta (GLD trend): `meta_slow_bars` × 5-minute bars (300 min ≈ ~1 trading day)

During warm-up the regime falls back progressively: `UUP(fallback)` → `UUP+TLT` → `UUP+TLT|RINF(meta)`. The `get_status` response shows `*_warmed_up` flags for each factor.

## Backtested Performance

| Period | Mode | Sharpe | Max DD |
|--------|------|--------|--------|
| 2016–2018 | UUP+TLT | 1.01 | 9% |
| 2019–2022 | UUP+TLT | 0.98 | 16% |
| 2023–2025 | UUP+(TLT\|RINF) meta-gated | 3.52 | 7% |

Overnight drift alone: ~+0.10%/night, Sharpe ~2.0 (unconditional).
