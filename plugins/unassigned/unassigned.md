# Unassigned Plugin (`_unassigned`)

System-managed plugin that tracks account positions and cash not attributed to any other plugin. It is the default "bucket" for everything the engine doesn't know which plugin owns. You cannot unload it; it is always present.

## Architecture

`_unassigned` is created automatically by `PluginExecutive` at engine start. It does not generate trade signals. Its only job is to hold a live view of unclaimed positions and cash so the transfer subsystem knows what is available.

```
PluginExecutive  →  sync_from_portfolio(claimed_symbols, claimed_cash)
                  ↓
_unassigned holds: all IB positions NOT in any other plugin's instruments
                   account available_funds minus cash claimed by other plugins
                  ↓
transfer cash / transfer position  →  moves assets to a named plugin
```

**Files** (in `plugins/_unassigned/` or `plugins/unassigned/`):

| File | Purpose |
|------|---------|
| `plugin.py` | `UnassignedPlugin` class |
| `state.json` | Persisted cash balance and claimed symbol set (auto-created) |
| `holdings.json` | Current unattributed positions (auto-created) |

## Viewing Unassigned Assets

```bash
# Show everything available to transfer
./ibctl.py transfer list _unassigned

# Get cash balance only
./ibctl.py plugin request _unassigned get_cash

# Get cash + all unassigned positions
./ibctl.py plugin request _unassigned get_unassigned
```

## Transferring Assets to Plugins

The primary use of `_unassigned` is as the source when funding other plugins.

### Transfer cash

```bash
# Transfer $25,000 to momentum_5day (dry run — no --confirm)
./ibctl.py transfer cash _unassigned momentum_5day 25000

# Execute the transfer
./ibctl.py transfer cash _unassigned momentum_5day 25000 --confirm

# Transfer $10,000 to portfolio_rebalancer
./ibctl.py transfer cash _unassigned portfolio_rebalancer 10000 --confirm
```

### Transfer a position

```bash
# Transfer 100 SPY shares to momentum_5day
./ibctl.py transfer position _unassigned momentum_5day SPY 100 --confirm

# Transfer 50 GLD shares to gld_usd_swap
./ibctl.py transfer position _unassigned gld_usd_swap GLD 50 --confirm
```

### Return assets to _unassigned

```bash
# Move cash back from a stopped plugin
./ibctl.py transfer cash momentum_5day _unassigned 25000 --confirm

# Return a position
./ibctl.py transfer position momentum_5day _unassigned SPY 100 --confirm
```

## Request Interface

```bash
./ibctl.py plugin request _unassigned <type> [json_payload]
```

### `get_cash`

Return the unassigned cash balance.

```bash
./ibctl.py plugin request _unassigned get_cash
```

```json
{"success": true, "cash": 47832.15}
```

### `get_unassigned`

Return cash and all positions not owned by any other plugin.

```bash
./ibctl.py plugin request _unassigned get_unassigned
```

```json
{
  "success": true,
  "cash": 47832.15,
  "positions": [
    {
      "symbol": "MSFT",
      "quantity": 30,
      "cost_basis": 380.00,
      "current_price": 415.20,
      "market_value": 12456.00
    }
  ]
}
```

### `sync`

Force a re-sync with the live portfolio state. Pass the set of symbols currently claimed by other plugins to exclude them from the unassigned view.

```bash
./ibctl.py plugin request _unassigned sync '{"claimed_symbols": ["SPY", "GLD", "BND"]}'
```

```json
{"success": true, "message": "Synced from portfolio"}
```

The engine reconciler (`./ibctl.py reconcile`) calls this automatically; manual use is only needed when debugging discrepancies.

## Notes

- `_unassigned` is read-only from a trading perspective — it never places orders.
- You cannot `plugin unload _unassigned` or `plugin stop _unassigned`; the system will reject those commands.
- After loading a new plugin, run `./ibctl.py reconcile` to refresh unassigned state so it reflects what the new plugin claims.
- The cash figure shown in `get_cash` is `account.available_funds` minus the sum of cash allocated to all other running plugins. If this seems low, check `./ibctl.py summary` for total account equity.
