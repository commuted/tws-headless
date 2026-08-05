
# TWS Headless

A headless, plugin-based algorithmic trading engine for Interactive Brokers. Connects to TWS or IB Gateway over the IB API, streams real-time market data, routes it to plugins, executes trade signals, and exposes a Unix socket command interface for external control.

📖 **[Wiki](https://github.com/commuted/tws-headless/wiki)** — [Theory of Operation](https://github.com/commuted/tws-headless/wiki/Theory-of-Operation) · [CLI Task Guide](https://github.com/commuted/tws-headless/wiki/CLI) · [Plugin Design](https://github.com/commuted/tws-headless/wiki/Plugin-Design) · [Plugin Manual](https://github.com/commuted/tws-headless/wiki/Plugin-Manual) · [Bar Store](https://github.com/commuted/tws-headless/wiki/Bar-Store)

## Requirements

- Python 3.10+
- Interactive Brokers TWS or IB Gateway (running and accepting API connections)
- `ibapi` — IB Python client (`pip install ibapi` or install from IB's website)

## Quick Start

```bash
# Install dependencies
pip install -e ".[dev]"

# Start the engine in dry-run mode (no real orders)
python3 -m ib.run_engine --port 7497 --mode dry_run

# In another terminal, check status
./ibctl.py status
./ibctl.py positions
```

## Engine

```bash
python3 -m ib.run_engine [options]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--port` | `7497` | TWS/Gateway port (`7496` live TWS, `7497` paper TWS, `4001`/`4002` Gateway) |
| `--mode` | `dry_run` | `dry_run` — log signals only; `immediate` — send orders; `queued` — batch |
| `--host` | `127.0.0.1` | IB host |
| `--client-id` | `1` | IB client ID (must be unique per session) |
| `--market-data-type` | auto | `1`=live, `2`=frozen, `3`=delayed, `4`=delayed-frozen. Auto-detected if omitted. |
| `--env` | auto (from port) | Trading environment `paper`/`live`. Namespaces the socket, execution DB, and logs. |
| `--allow-env-mismatch` | — | Override the guardrail that aborts on a paper/live account vs. environment mismatch |
| `--live-confirmed` | — | Required to run `immediate`/`queued` (real) orders against a **live** account |
| `--socket` | `~/.tws_headless_{env}.sock` | Unix socket path for `ibctl.py` commands (env-keyed by default) |
| `--no-server` | — | Disable socket command server |
| `--plugin-dir` | `plugins/` | Plugin search directory |
| `--verbose` / `--quiet` | — | Logging verbosity |

Environment variables mirror all options: `PORT`, `MODE`, `MARKET_DATA_TYPE`, `IB_PLUGIN_DIR`.

### Paper/live separation

Paper and live operation never share state. The environment is derived from the port
(`7497`/`4002` → paper, `7496`/`4001` → live; override with `--env`) and namespaces every
per-session resource, so a paper and a live engine can run side-by-side safely:

| Resource | Path | Keyed by |
|----------|------|----------|
| Command socket | `~/.tws_headless_{paper,live}.sock` | environment |
| Log file | `logs/{paper,live}/engine.log` | environment |
| Execution/fills DB | `~/.ib_executions_{account_id}.db` | account |
| Plugin registry DB | `~/.ib_plugin_store_{account_id}.db` | account |
| Plugin state/holdings | `plugins/{slot}/{account_id}/` | account |

On connect, the engine verifies the account's paper/live nature matches the declared
environment and **aborts on mismatch** (override: `--allow-env-mismatch`). Real orders
against a live account additionally require `--live-confirmed`. If IB reports **no managed
account**, the engine aborts rather than fall back to shared/default state. Point
`ibctl.py` at a specific engine with `--env paper|live` (or `--port`).

All plugin state is scoped to the account, including the system `_unassigned` plugin and
any system/example plugins registered at engine startup: once the account is known, every
registered plugin is re-rooted under `plugins/{slot}/{account_id}/`. Legacy accountless
`plugins/{slot}/` directories from before this change are **left untouched and no longer
read** (per-account state starts fresh; reconciliation repopulates `_unassigned` from the
live account).

#### Migrating from the old single-file DB

Before this change all fills were logged to a single `~/.ib_executions.db` (and the
socket was `~/.tws_headless.sock`). Those legacy files are **left untouched and are no
longer read** — the engine now writes to per-account databases, which start empty. No
automatic copy is performed, because blindly merging the old file back in would
re-introduce the very paper/live comingling this change removes.

If you want to carry historical fills forward, split the old DB by its `account` column
into the per-account files. Each row is already tagged with its account, so the split is
lossless:

```bash
# For each account that appears in the old DB (find them with:
#   sqlite3 ~/.ib_executions.db 'SELECT DISTINCT account FROM executions;')
ACCT=DU1234567
sqlite3 ~/.ib_executions.db <<SQL
ATTACH DATABASE '$HOME/.ib_executions_${ACCT}.db' AS dst;
CREATE TABLE IF NOT EXISTS dst.executions AS SELECT * FROM executions WHERE 0;
CREATE TABLE IF NOT EXISTS dst.commissions AS SELECT * FROM commissions WHERE 0;
INSERT INTO dst.executions  SELECT * FROM executions  WHERE account = '$ACCT';
INSERT INTO dst.commissions SELECT * FROM commissions
  WHERE exec_id IN (SELECT exec_id FROM executions WHERE account = '$ACCT');
SQL
```

Start the engine once against that account first so it creates the schema, or let the
`CREATE TABLE ... AS SELECT ... WHERE 0` statements above seed empty tables. Once you've
confirmed the new per-account DBs look right, the legacy `~/.ib_executions.db` and
`~/.tws_headless.sock` can be deleted.

### Relocating the platform — STATE snapshots

Runtime state is split across four stores, and three of them live in `$HOME`, not the
repo: the per-plugin `state.json` / `holdings.json` / `instruments.json` files (in the
repo but **gitignored**), `~/.ib_plugin_store_{account}.db` (which plugins reload and in
what status), `~/.ib_executions_{account}.db`, and `~/.ib_forex_cost_basis.json`. A repo
image alone therefore cannot relocate a running platform — a `git clone` carries none of
it.

A **STATE file** closes that gap. The engine writes `./STATE.json` on every clean stop
(disable with `--no-state-on-stop`), after plugin state has been flushed to disk. STATE
file + repo image is sufficient to restart elsewhere.

```bash
# The engine writes STATE.json itself on a clean stop. To collect the same
# snapshot from a system that is already stopped (or whose engine died):
./ibctl.py state collect --account DUP278735          # → ./STATE.json
./ibctl.py state show                                 # summarize, don't apply

# On the target machine, restore before starting:
RESTORE_STATE=1 ./start_trading.sh 4002 immediate     # first run only
python3 -m ib.run_engine --port 4002 --restore-state  # or directly

# Or apply it offline (dry run unless --confirm):
./ibctl.py state restore --account DUP278735 --confirm
```

`state collect`, `show`, and `restore` read the on-disk stores directly and never touch
the command socket — that is what lets them work with no engine running.

Restore is **not** a merge. It writes back what the snapshot says is true, then the
engine's normal startup reconciliation settles the remainder against live IB positions.
Two safety properties are worth knowing:

- **Restore is opt-in.** Without `--restore-state` the engine starts from whatever state
  is already on the machine. Under `start_trading.sh` the flag applies to the *first* run
  only — a supervised restart must not wind the engine back to the snapshot.
- **It refuses to cross accounts.** A snapshot collected from one account will not be
  restored over another's stores, for the same reason paper and live never share a
  socket, an execution DB, or a log.

The executions DB and `historical/bars.db` are deliberately *not* carried — one is an
audit log of past fills, the other a refetchable market-data cache. Both are listed in
the snapshot's `not_carried` manifest so a restore tells you what was left behind.
`STATE.json` holds live positions and is gitignored: copy it alongside a repo image,
never commit it.

## CLI — `ibctl.py`

Sends commands to a running engine over the Unix socket.

```bash
# Portfolio
./ibctl.py status
./ibctl.py positions
./ibctl.py summary [--json]

# Orders (add --confirm to execute; default is preview)
./ibctl.py order buy  SPY 100                        # market
./ibctl.py order buy  SPY 100 limit 450.00           # limit
./ibctl.py order sell QQQ  50 stop 380               # stop
./ibctl.py order buy  AAPL 25 stop-limit 175 170     # stop-limit
./ibctl.py order sell MSFT 30 trail 2.00             # trailing stop ($)
./ibctl.py order sell MSFT 30 trail 1%               # trailing stop (%)
./ibctl.py order buy  SPY 100 moc                    # market-on-close
./ibctl.py sell SPY all --confirm                    # sell entire position
./ibctl.py liquidate  --confirm                      # close all positions

# Plugin-attributed trade
./ibctl.py trade my_plugin BUY SPY 100 --confirm

# Plugin management
./ibctl.py plugin list
./ibctl.py plugin load  plugins.my_package.my_plugin
./ibctl.py plugin load  plugins.my_package.my_plugin=spy_leg  # named slot
./ibctl.py plugin start my_plugin
./ibctl.py plugin stop  my_plugin
./ibctl.py plugin freeze   my_plugin
./ibctl.py plugin resume   my_plugin
./ibctl.py plugin dump     my_plugin    # positions + open orders
./ibctl.py plugin request  my_plugin get_status
./ibctl.py plugin message  my_plugin '{"action": "reset"}'  # arbitrary message
./ibctl.py plugin help     my_plugin   # show plugin CLI help
./ibctl.py plugin unload   my_plugin

# Internal bookkeeping transfers (no IB orders placed)
./ibctl.py transfer list     _unassigned
./ibctl.py transfer cash     _unassigned my_plugin 10000 --confirm
./ibctl.py transfer position _unassigned my_plugin SPY 50 --confirm

# Historical bar data — always saved to historical/bars.db (default)
./ibctl.py historical fetch GLD                                        # 1 W daily bars
./ibctl.py historical fetch GLD --bar-size "5 mins" --duration "2 D"  # 5-min bars
./ibctl.py historical fetch EUR --type forex --what MIDPOINT --no-rth
./ibctl.py historical coverage                  # what is cached
./ibctl.py historical coverage --symbol GLD
./ibctl.py historical purge --symbol GLD --bar-size "5 mins"
./ibctl.py historical get-db                    # show current DB path
./ibctl.py historical set-db /data/bars.db      # change DB path (persisted)

# Engine control
./ibctl.py pause
./ibctl.py resume
./ibctl.py stop
```

## Plugins

Plugins are Python classes that subclass `PluginBase`. They receive market data, publish signals, and interact with the MessageBus. See the **[Plugin Manual](https://github.com/commuted/tws-headless/wiki/Plugin-Manual)** for the full authoring reference.

Plugins that were running when the engine last stopped are **automatically reloaded on the next start** — no manual `plugin load` / `plugin start` needed. The engine records each instance in `~/.ib_plugin_store.db` and replays their last lifecycle status (`running` → auto-start; `frozen` → load only).

The auto-reload happens in **two phases with reconciliation between them** — plugins are
loaded, holdings are reconciled against the account, and only then are plugins started
(`PluginExecutive.load_registered_plugins` → `reconcile_with_account` →
`start_loaded_plugins`, sequenced by `run_engine.startup_plugin_sequence`). Both
boundaries matter:

- **Reconcile after the load**, because `reconcile_with_account()` derives what plugins
  claim from the plugins loaded in memory and *sets* `_unassigned` to (account − claimed).
  Reconciling with nothing loaded hands `_unassigned` the whole account, and every
  position is double-counted once the real plugins load on top.
- **Reconcile before the start**, because starting a plugin opens the door to trading — a
  plugin's `start()` typically self-reconciles its own holding flags and subscribes to
  live bars, after which a bar can place an order. It should act on a ledger already
  squared against the account.
- **Reconcile only after IB's position snapshot has arrived** (`Portfolio.wait_for_positions`).
  `on_started` fires before `Portfolio.load()` has even called `reqPositions`, and an
  empty `positions` list means "not downloaded yet" just as often as "account holds
  nothing". Reconciling against the former reads every plugin holding as a phantom and
  deletes it. If the snapshot never arrives, startup reconciliation is **skipped** rather
  than run on incomplete data — a stale ledger is recoverable, deleted holdings are not.

`reload_registered_plugins()` still does both phases in one pass for callers with nothing
to do in between; the engine's own startup does not use it.

### File layout

```
plugins/
  my_strategy/
    __init__.py      # re-exports the class
    plugin.py        # PluginBase subclass
    state.json       # written/read by save_state / load_state
    instruments.json # auto-managed instrument list
    holdings.json    # auto-managed holdings tracking
```

Each plugin owns its directory. To move or backup a plugin instance, tar its directory — everything it owns is there.

### Minimal plugin

```python
# plugins/my_strategy/plugin.py
from plugins.base import PluginBase, TradeSignal

class MyStrategyPlugin(PluginBase):
    VERSION = "1.0.0"
    INSTRUMENT_COMPLIANCE = False  # True → signals for unlisted symbols are blocked

    def __init__(self, base_path=None, portfolio=None,
                 shared_holdings=None, message_bus=None):
        super().__init__("my_strategy", base_path, portfolio,
                         shared_holdings, message_bus)

    @property
    def description(self): return "My strategy."

    def start(self):   return True
    def stop(self):    return True
    def freeze(self):  return True
    def resume(self):  return True
    def calculate_signals(self): return []
    def handle_request(self, request_type, payload):
        return {"success": False, "message": f"Unknown: {request_type}"}
    def cli_help(self) -> str:
        return "my_strategy: no custom commands."
```

```bash
./ibctl.py plugin load  plugins.my_strategy
./ibctl.py plugin start my_strategy
```

## Project Layout

```
ib/                     Core engine package
  trading_engine.py     Top-level engine (connects portfolio, data, plugins)
  plugin_executive.py   Plugin lifecycle, signal routing, order execution
  plugin_store.py       SQLite registry (which plugins to auto-reload on engine restart)
  portfolio.py          IB connection, positions, account data
  data_feed.py          Real-time tick/bar streaming and aggregation
  command_server.py     Unix socket command server
  order_reconciler.py   Nets signals from multiple plugins before placing orders
  message_bus.py        Pub/sub broker for inter-plugin communication
  execution_db.py       SQLite trade/execution log
  bar_store.py          SQLite historical bar cache with coverage tracking and gap detection
  rate_limiter.py       Token-bucket rate limiting (10 orders/sec default)
  contract_builder.py   Contract construction (stocks, options, futures, forex…)
  order_builder.py      Order type construction
  algo_params.py        IB algo parameters (Adaptive, TWAP, VWAP, PctVol…)
  client.py             Async IBClient (asyncio transport over EClient/EWrapper)
  models.py             Data classes (Bar, Position, OrderRecord, PnLData…)
  connection_manager.py Auto-reconnect connection management
  rebalancer.py         Portfolio rebalancing strategies
  enter_exit.py         Entry/exit and bracket order builders

plugins/                Plugin implementations
  base.py               PluginBase class and TradeSignal
  unassigned/           System plugin for unattributed cash and positions
  demo/                 Demo plugins (SMA publisher/subscriber via MessageBus)
  momentum_5day/        5-day momentum strategy
  paper_tests/          Paper trading integration test suite

tests/                  Unit and integration tests (pytest)
ibctl.py                CLI client (talks to running engine via Unix socket)
run_paper_tests.sh      Cron-schedulable script to run paper tests at market open
PLUGIN_MANUAL.md        Complete plugin authoring reference
```

## Market Data

The engine auto-detects the appropriate market data type at connect time. Paper accounts with a live data subscription shared from a funded account receive live data (type 1). Without a live subscription, delayed data (type 3) is used — note that real-time bar streams (`BAR_5SEC` and aggregated timeframes) are silent in delayed mode.

Override with `--market-data-type 3` to force delayed data.

## Tests

```bash
pytest tests/
pytest tests/test_rate_limiter.py   # specific file
pytest -x                           # stop on first failure
```

The test suite mocks the `ibapi` package so no IB connection is required.

## Paper Trading Tests

End-to-end integration tests run against a live paper account. Each test plugin is loaded, started, and asked to `run_tests`; results are saved and reported. Use `run_paper_tests.py` to drive them:

```bash
python run_paper_tests.py              # order tests 1–5
python run_paper_tests.py --historical # historical data API tests
python run_paper_tests.py --bar-store  # BarStore cache end-to-end tests
python run_paper_tests.py --all        # everything
```

`--bar-store` runs `plugins/paper_tests/paper_test_bar_store/` against a live paper account, verifying cold fetch, cache hits, gap fill, force refetch, coverage tracking, purge, multi-symbol isolation, and OHLC validity (9 tests).

Or schedule at market open via `run_paper_tests.sh` (requires TWS/Gateway to be running).
