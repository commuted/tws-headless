#!/usr/bin/env python3
"""
ibctl.py - Command-line client for TWS Headless

Send commands to a running main.py or run_engine.py instance via Unix socket.

This is a quick-start reference, not the full command list — it drifts
(see git blame) precisely because it's the *second* place commands get
documented and nobody remembers to update it when the first place, the
argparse epilog in main() below, gains a new one. Run `./ibctl.py --help`
for the authoritative, complete list — in particular the full `plugin`
lifecycle set (freeze, resume, enable, disable, trigger, request, all six
`instruments` subcommands, export, import) and `reconcile` aren't shown
below at all.

Usage:
    ./ibctl.py status                          # Get portfolio status
    ./ibctl.py positions                       # List all positions
    ./ibctl.py summary                         # Account summary with plugin breakdown
    ./ibctl.py summary --json                  # Account summary as JSON
    ./ibctl.py account                         # IB account values (all raw tags)
    ./ibctl.py account --json                  # ...as JSON
    ./ibctl.py commissions                     # Commission and fee report
    ./ibctl.py commissions --symbol GLD --days 30 --json
    ./ibctl.py reconcile                       # Sync plugin holdings with IB account

    # Simple orders (market only)
    ./ibctl.py sell SPY 10                     # Preview selling 10 shares of SPY
    ./ibctl.py sell SPY all --confirm          # Sell entire SPY position
    ./ibctl.py buy SPY 10 --confirm            # Buy 10 shares of SPY
    ./ibctl.py liquidate --confirm             # Liquidate all positions

    # Advanced orders (all IB order types)
    ./ibctl.py order buy SPY 100               # Market order (dry run)
    ./ibctl.py order buy SPY 100 --confirm     # Market order (execute)
    ./ibctl.py order buy SPY 100 limit 450     # Limit order at $450
    ./ibctl.py order sell QQQ 50 stop 380      # Stop order at $380
    ./ibctl.py order buy AAPL 25 stop-limit 175 170   # Stop-limit order
    ./ibctl.py order sell MSFT 30 trail 2.00   # Trailing stop $2
    ./ibctl.py order sell MSFT 30 trail 1%     # Trailing stop 1%
    ./ibctl.py order buy SPY 100 moc           # Market on Close
    ./ibctl.py order sell QQQ 50 loc 380       # Limit on Close

    # Plugin-attributed trades
    ./ibctl.py trade PLUGIN BUY SPY 100        # Preview plugin-attributed trade
    ./ibctl.py trade PLUGIN BUY SPY 100 --confirm  # Execute trade

    # Internal transfers (bookkeeping only, no actual trades)
    ./ibctl.py transfer list _unassigned       # Show transferable assets
    ./ibctl.py transfer cash _unassigned momentum_5day 10000 --confirm
    ./ibctl.py transfer position _unassigned momentum_5day SPY 50 --confirm

    # Historical bar data — always saved to the configured BarStore DB
    ./ibctl.py historical fetch GLD            # Fetch 1 week of daily bars
    ./ibctl.py historical fetch GLD --bar-size "5 mins" --duration "2 D"
    ./ibctl.py historical fetch EUR --type forex --what MIDPOINT --no-rth
    ./ibctl.py historical coverage             # what is cached
    ./ibctl.py historical coverage --symbol GLD
    ./ibctl.py historical purge --symbol GLD --bar-size "5 mins"
    ./ibctl.py historical set-db /data/bars.db # change the DB path (persisted)
    ./ibctl.py historical get-db               # show current DB path

    # Plugin/algorithm management
    ./ibctl.py plugin list                     # List all plugins
    ./ibctl.py plugin load PATH                # Load plugin from file
    ./ibctl.py plugin load PATH=SLOT           # Load with named instance slot
    ./ibctl.py plugin load PATH DESCRIPTOR     # Load with descriptor
    ./ibctl.py plugin unload NAME_OR_ID        # Unload a plugin
    ./ibctl.py plugin start NAME_OR_ID         # Start an idle plugin
    ./ibctl.py plugin stop NAME_OR_ID          # Stop a running plugin
    ./ibctl.py plugin status NAME_OR_ID        # Get plugin status
    ./ibctl.py plugin dump NAME_OR_ID          # Dump positions & open orders
    ./ibctl.py plugin help NAME_OR_ID          # Show plugin CLI help
    ./ibctl.py plugin message NAME_OR_ID JSON  # Send arbitrary message to plugin
    #  ...also: freeze/resume/enable/disable/trigger/request/instruments/
    #  export/import — see --help, not shown here
    ./ibctl.py pause                           # Pause execution
    ./ibctl.py resume                          # Resume execution
    ./ibctl.py ping                            # Test server connectivity
    ./ibctl.py stop                            # Shutdown the server gracefully
    ./ibctl.py shutdown                        # Shutdown the server (alias for stop)
"""

import argparse
import json
import os
import socket
import sys
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Any, Optional


# Legacy single socket path (pre paper/live separation). Used as a fallback only.
DEFAULT_SOCKET_PATH = os.path.expanduser("~/.tws_headless.sock")

# Paper/live separation: env-keyed sockets so ibctl targets the right engine.
# Kept in sync with ib/environment.py (ibctl stays standalone — no package imports).
_PAPER_PORTS = {7497, 4002}
_LIVE_PORTS = {7496, 4001}


def _socket_for_env(env):
    return os.path.expanduser(f"~/.tws_headless_{env}.sock")


def _env_from_port(port):
    if port in _PAPER_PORTS:
        return "paper"
    if port in _LIVE_PORTS:
        return "live"
    return None


def resolve_socket_path(explicit_socket, env, port, exists=os.path.exists):
    """Resolve which engine socket to talk to.

    Priority: explicit --socket > --env > --port-derived env > auto-discovery
    of a running engine's socket > legacy fallback.

    An unrecognized --port (not in _PAPER_PORTS/_LIVE_PORTS) is a hard error,
    not silently ignored: falling through to auto-discovery on a typo'd port
    would let a command land on whichever engine happens to be running,
    without so much as a warning that the port was never used.

    Auto-discovery (nothing specified): if the
    legacy socket exists, use it (a pre-separation engine is running); else
    if exactly the *paper* socket exists, use it — the common single-engine
    case should just work. A live engine is never targeted implicitly: if
    the live socket exists (alone or alongside paper), require --env so
    commands cannot land on a live account by default.

    Returns (socket_path, error_message); error_message is set when the
    port is unrecognized, or the target is ambiguous or live-only and must
    be chosen explicitly.
    """
    if explicit_socket:
        return explicit_socket, None
    if env:
        return _socket_for_env(env), None
    if port is not None:
        derived = _env_from_port(port)
        if derived:
            return _socket_for_env(derived), None
        return None, (
            f"Port {port} is not a recognized paper port ({sorted(_PAPER_PORTS)}) "
            f"or live port ({sorted(_LIVE_PORTS)}). Pass --env paper or --env live, "
            f"or --socket to target a specific socket path directly."
        )

    if exists(DEFAULT_SOCKET_PATH):
        return DEFAULT_SOCKET_PATH, None

    paper = _socket_for_env("paper")
    live = _socket_for_env("live")
    paper_up, live_up = exists(paper), exists(live)
    if paper_up and not live_up:
        return paper, None
    if live_up:
        which = ("both paper and live engine sockets"
                 if paper_up else "a live engine socket")
        return None, (
            f"Found {which}; refusing to guess. "
            f"Pass --env paper or --env live to target the right engine."
        )
    # Nothing running — return the legacy path so the caller produces the
    # standard "server not running" error (with env-socket hints).
    return DEFAULT_SOCKET_PATH, None


class CommandStatus(Enum):
    """Command execution status"""
    SUCCESS = "success"
    ERROR = "error"
    PENDING = "pending"
    UNAUTHORIZED = "unauthorized"


@dataclass
class CommandResult:
    """Result of a command execution"""
    status: CommandStatus
    message: str = ""
    data: Dict[str, Any] = field(default_factory=dict)
    request_token: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "status": self.status.value,
            "message": self.message,
            "data": self.data,
        }
        if self.request_token is not None:
            result["request_token"] = self.request_token
        return result


def send_command(
    command: str,
    socket_path: str = DEFAULT_SOCKET_PATH,
    timeout: float = 10.0,
    token: Optional[str] = None,
    request_token: Optional[str] = None,
) -> CommandResult:
    """
    Send a command to the running server.

    Args:
        command: Command string to send
        socket_path: Path to Unix socket
        timeout: Connection timeout in seconds
        token: Authentication token (if server requires auth)
        request_token: Optional request token for tracking/dedup

    Returns:
        CommandResult from server
    """
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(socket_path)
        sock.settimeout(timeout)

        # Build wire command: REQ first, then AUTH, then command
        full_command = command
        if token:
            full_command = f"AUTH {token} {full_command}"
        if request_token:
            full_command = f"REQ {request_token} {full_command}"

        # Send command
        sock.sendall((full_command + "\n").encode("utf-8"))

        # Receive response
        data = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
            if b"\n" in data:
                break

        sock.close()

        # Parse response
        response = json.loads(data.decode("utf-8").strip())
        return CommandResult(
            status=CommandStatus(response.get("status", "error")),
            message=response.get("message", ""),
            data=response.get("data", {}),
            request_token=response.get("request_token"),
        )

    except FileNotFoundError:
        running = [p for e in ("paper", "live")
                   if os.path.exists(p := _socket_for_env(e))]
        hint = (f" Found running engine socket(s): {', '.join(running)} — "
                f"target one with --env paper or --env live."
                if running else "")
        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Server not running (socket not found: {socket_path}).{hint}",
        )
    except ConnectionRefusedError:
        return CommandResult(
            status=CommandStatus.ERROR,
            message="Connection refused - server may not be running",
        )
    except Exception as e:
        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Failed to send command: {e}",
        )


def format_result(result: CommandResult, verbose: bool = False):
    """Format the command result for display"""
    # Status indicator
    if result.status == CommandStatus.SUCCESS:
        status_str = "[OK]"
    elif result.status == CommandStatus.ERROR:
        status_str = "[ERROR]"
    elif result.status == CommandStatus.UNAUTHORIZED:
        status_str = "[UNAUTHORIZED]"
    else:
        status_str = "[PENDING]"

    print(f"{status_str} {result.message}")

    if result.request_token is not None:
        print(f"Request-Token: {result.request_token}")

    # Print data in verbose mode or for specific commands
    if verbose and result.data:
        print("\nData:")
        print(json.dumps(result.data, indent=2))
    elif result.data:
        # Special formatting for certain data types
        if "instance_id" in result.data and "plugin_name" in result.data:
            # Plugin load response
            print(f"\n  Plugin:      {result.data['plugin_name']}")
            slot = result.data.get("slot")
            if slot and slot != result.data["plugin_name"]:
                print(f"  Slot:        {slot}")
            print(f"  Instance ID: {result.data['instance_id']}")
            if result.data.get("descriptor") is not None:
                print(f"  Descriptor:  {result.data['descriptor']}")
            if result.data.get("path"):
                print(f"  Path:        {result.data['path']}")

        elif "open_orders" in result.data:
            # Plugin dump output
            cash = result.data.get("cash", 0.0)
            positions = result.data.get("positions", [])
            open_orders = result.data["open_orders"]

            print(f"\n  Cash: ${cash:,.2f}")

            if positions:
                print(f"\n  {'Symbol':<8} {'Qty':>10} {'Cost Basis':>12} {'Price':>12} {'Value':>14}")
                print(f"  {'-' * 60}")
                for p in positions:
                    print(f"  {p['symbol']:<8} {p['quantity']:>10,.0f} "
                          f"${p.get('cost_basis', 0):>10,.2f} "
                          f"${p.get('current_price', 0):>10,.2f} "
                          f"${p.get('market_value', 0):>12,.2f}")
            else:
                print("\n  Positions: (none)")

            if open_orders:
                print(f"\n  {'Order ID':>10} {'Symbol':<8} {'Action':<6} {'Qty':>8} {'Status':<10} {'Created'}")
                print(f"  {'-' * 70}")
                for o in open_orders:
                    created = o.get("created_at", "")[:19]
                    print(f"  {o['order_id']:>10} {o['symbol']:<8} {o['action']:<6} "
                          f"{o['quantity']:>8} {o['status']:<10} {created}")
            else:
                print("\n  Open orders: (none)")

        elif "instruments" in result.data and "compliance" in result.data:
            # Plugin instruments list response
            instruments = result.data["instruments"]
            compliance = result.data["compliance"]
            print(f"\n  Compliance enforcement: {'ON' if compliance else 'off'}")
            if instruments:
                print(f"\n  {'Symbol':<8} {'Name':<20} {'Weight':>8} {'Min':>6} {'Max':>6} "
                      f"{'Exch':<6} {'Ccy':<4} {'Type':<4} {'En'}")
                print(f"  {'-' * 72}")
                for i in instruments:
                    enabled = "yes" if i.get("enabled", True) else "no"
                    print(f"  {i['symbol']:<8} {i.get('name',''):<20} "
                          f"{i.get('weight', 0.0):>8.2f} "
                          f"{i.get('min_weight', 0.0):>6.2f} "
                          f"{i.get('max_weight', 100.0):>6.2f} "
                          f"{i.get('exchange','SMART'):<6} "
                          f"{i.get('currency','USD'):<4} "
                          f"{i.get('sec_type','STK'):<4} "
                          f"{enabled}")
            else:
                print("\n  Instruments: (none)")

        elif "positions" in result.data:
            positions = result.data["positions"]
            # Only render the portfolio-style table when rows actually carry
            # that schema. Other commands (e.g. transfer list) reuse the
            # "positions" key with different row shapes and already list
            # their positions in the message body — indexing portfolio keys
            # on those rows crashed with KeyError.
            _table_keys = ("symbol", "quantity", "price", "value", "pnl", "allocation")
            if positions and all(k in positions[0] for k in _table_keys):
                print(f"\n{'Symbol':<8} {'Qty':>10} {'Price':>12} {'Value':>14} {'P&L':>12} {'Alloc':>8}")
                print("-" * 70)
                for p in positions:
                    print(f"{p['symbol']:<8} {p['quantity']:>10,.0f} "
                          f"${p['price']:>10,.2f} ${p['value']:>12,.2f} "
                          f"${p['pnl']:>10,.2f} {p['allocation']:>7.1f}%")


# =============================================================================
# Historical data helpers
# =============================================================================

_IBCTL_DIR     = os.path.dirname(os.path.abspath(__file__))
_HIST_CONFIG   = os.path.join(_IBCTL_DIR, "historical", "config.json")
_HIST_DB_DEFAULT = os.path.join(_IBCTL_DIR, "historical", "bars.db")


def _historical_db_path() -> str:
    """Return the configured BarStore DB path, falling back to the default."""
    if os.path.exists(_HIST_CONFIG):
        try:
            with open(_HIST_CONFIG) as f:
                return json.load(f).get("db_path", _HIST_DB_DEFAULT)
        except Exception:
            pass
    return _HIST_DB_DEFAULT


def _historical_set_db(subargs: list) -> None:
    """Persist a new DB path to historical/config.json."""
    if not subargs:
        print("Usage: historical set-db PATH")
        sys.exit(1)
    path = os.path.abspath(subargs[0])
    os.makedirs(os.path.dirname(_HIST_CONFIG), exist_ok=True)
    with open(_HIST_CONFIG, "w") as f:
        json.dump({"db_path": path}, f, indent=2)
    print(f"[OK] Historical DB path set to: {path}")


def _historical_fetch(subargs: list, socket_path: str, timeout: float) -> None:
    """Fetch bars from IB, print them, and save to the configured BarStore DB."""
    symbol        = None
    bar_size      = "1 day"
    duration      = "1 W"
    end           = ""
    what          = "TRADES"
    use_rth       = True
    contract_type = "etf"

    i = 0
    while i < len(subargs):
        a = subargs[i]
        if a == "--bar-size" and i + 1 < len(subargs):
            bar_size = subargs[i + 1];              i += 2
        elif a == "--duration" and i + 1 < len(subargs):
            duration = subargs[i + 1];              i += 2
        elif a == "--end" and i + 1 < len(subargs):
            end = subargs[i + 1];                   i += 2
        elif a == "--what" and i + 1 < len(subargs):
            what = subargs[i + 1];                  i += 2
        elif a == "--type" and i + 1 < len(subargs):
            contract_type = subargs[i + 1].lower(); i += 2
        elif a == "--no-rth":
            use_rth = False;                        i += 1
        elif not a.startswith("-") and symbol is None:
            symbol = a.upper();                     i += 1
        else:
            i += 1

    if symbol is None:
        print("Usage: historical fetch SYMBOL [--bar-size X] [--duration X]")
        print("       [--end YYYYMMDD-HH:MM:SS] [--what TRADES|MIDPOINT|BID|ASK]")
        print("       [--type etf|stock|forex] [--no-rth]")
        sys.exit(1)

    db_path = _historical_db_path()

    # Build engine command — spaces in bar_size/duration become _ on the wire
    engine_parts = [
        "historical", "fetch", symbol,
        "--bar-size", bar_size.replace(" ", "_"),
        "--duration", duration.replace(" ", "_"),
        "--what",     what,
        "--type",     contract_type,
    ]
    if end:
        engine_parts += ["--end", end]
    if not use_rth:
        engine_parts.append("--no-rth")

    result = send_command(
        " ".join(engine_parts),
        socket_path=socket_path,
        timeout=max(timeout, 120.0),
    )

    if result.status != CommandStatus.SUCCESS:
        print(f"[ERROR] {result.message}")
        sys.exit(1)

    bars = result.data.get("bars", [])
    print(f"[OK] {result.message}")

    if bars:
        col = "  {:<22} {:>9} {:>9} {:>9} {:>9} {:>11}"
        print()
        print(col.format("Date", "Open", "High", "Low", "Close", "Volume"))
        print("  " + "-" * 72)
        show = bars if len(bars) <= 12 else bars[:6] + [None] + bars[-6:]
        for b in show:
            if b is None:
                print("  ...")
                continue
            print(col.format(
                b["date"][:22],
                f"{b['open']:.2f}",
                f"{b['high']:.2f}",
                f"{b['low']:.2f}",
                f"{b['close']:.2f}",
                f"{b['volume']:,}",
            ))
        print()

    # ── Persist to BarStore ──────────────────────────────────────────────────
    sys.path.insert(0, _IBCTL_DIR)
    from ib.bar_store import BarStore, SeriesKey
    from datetime import datetime, timezone
    UTC = timezone.utc

    class _Bar:
        __slots__ = ("date", "open", "high", "low", "close", "volume", "wap", "barCount")
        def __init__(self, d: dict):
            self.date     = d["date"]
            self.open     = d["open"]
            self.high     = d["high"]
            self.low      = d["low"]
            self.close    = d["close"]
            self.volume   = d["volume"]
            self.wap      = d.get("wap", 0.0)
            self.barCount = d.get("bar_count", 0)

    start_iso = result.data.get("fetch_start_utc", "")
    end_iso   = result.data.get("fetch_end_utc",   "")
    start_dt  = (
        datetime.strptime(start_iso, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)
        if start_iso else datetime.now(UTC)
    )
    end_dt = (
        datetime.strptime(end_iso, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)
        if end_iso else datetime.now(UTC)
    )

    store = BarStore(db_path)
    key   = SeriesKey(symbol, bar_size, what, int(use_rth))
    proxy = [_Bar(d) for d in bars]

    with store._write_lock:
        n = store._store_bars(key, proxy, start_dt, end_dt)
        store._update_coverage(key, start_dt, end_dt)

    print(f"[DB] Saved {n} bar(s) to {db_path}  [{symbol} / {bar_size} / {what}]")


def _historical_coverage(subargs: list) -> None:
    """Print coverage summary for the configured BarStore DB."""
    symbol = None
    i = 0
    while i < len(subargs):
        if subargs[i] == "--symbol" and i + 1 < len(subargs):
            symbol = subargs[i + 1].upper(); i += 2
        else:
            i += 1

    db_path = _historical_db_path()
    sys.path.insert(0, _IBCTL_DIR)
    from ib.bar_store import BarStore

    if not os.path.exists(db_path):
        print(f"No DB yet at {db_path}")
        return

    entries = BarStore(db_path).coverage_summary(symbol=symbol)
    print(f"DB: {db_path}")
    if not entries:
        print("No coverage found.")
        return

    col = "{:<8} {:<10} {:<10} {:<5} {:>6}  {}"
    print(col.format("Symbol", "BarSize", "What", "RTH", "Bars", "Intervals"))
    print("-" * 80)
    for e in entries:
        ivs = ", ".join(f"{s}..{en}" for s, en in e["intervals"])
        print(col.format(
            e["symbol"][:8],
            e["bar_size"][:10],
            e["what_to_show"][:10],
            "Y" if e["use_rth"] else "N",
            e["total_bars"],
            ivs[:60],
        ))


def _historical_purge(subargs: list) -> None:
    """Purge a series from the configured BarStore DB."""
    symbol   = None
    bar_size = "1 day"
    what     = "TRADES"
    use_rth  = True
    i = 0
    while i < len(subargs):
        a = subargs[i]
        if a == "--symbol" and i + 1 < len(subargs):
            symbol = subargs[i + 1].upper(); i += 2
        elif a == "--bar-size" and i + 1 < len(subargs):
            bar_size = subargs[i + 1];       i += 2
        elif a == "--what" and i + 1 < len(subargs):
            what = subargs[i + 1];           i += 2
        elif a == "--no-rth":
            use_rth = False;                 i += 1
        else:
            i += 1

    if not symbol:
        print("Usage: historical purge --symbol SYMBOL [--bar-size X] [--what X] [--no-rth]")
        sys.exit(1)

    db_path = _historical_db_path()
    if not os.path.exists(db_path):
        print(f"[ERROR] DB not found: {db_path}")
        sys.exit(1)

    sys.path.insert(0, _IBCTL_DIR)
    from ib.bar_store import BarStore

    deleted = BarStore(db_path).purge(
        symbol=symbol, bar_size=bar_size, what_to_show=what, use_rth=use_rth,
    )
    print(f"[OK] Purged {deleted} bar(s) for {symbol} / {bar_size} / {what} from {db_path}")


# =============================================================================
# STATE snapshots — handled entirely locally
#
# These read on-disk stores directly and never touch the command socket, which
# is the point: the engine writes a STATE file when it stops, and this is the
# redundant path for collecting the same snapshot from a system that is already
# stopped (or whose engine died without writing one).
# =============================================================================

def _discover_account() -> str:
    """Infer the account from the per-account plugin store DBs in $HOME.

    Exits with guidance rather than guessing when there is more than one, since
    picking the wrong account here would write one account's positions into
    another's state files.
    """
    import glob
    prefix = os.path.join(os.path.expanduser("~"), ".ib_plugin_store_")
    accounts = sorted(
        os.path.basename(p)[len(".ib_plugin_store_"):-len(".db")]
        for p in glob.glob(prefix + "*.db")
        if not p.endswith(("-wal", "-shm"))
    )
    if len(accounts) == 1:
        return accounts[0]
    if not accounts:
        print("[ERROR] No per-account plugin store found in ~. Pass --account ACCOUNT.")
    else:
        print(f"[ERROR] Multiple accounts found ({', '.join(accounts)}). "
              f"Pass --account ACCOUNT to choose one.")
    sys.exit(1)


def _state_args(subargs: list) -> dict:
    """Parse the flags shared by the state subcommands.

    Unknown flags are a hard error, exactly as resolve_socket_path treats an
    unrecognized --port: a silently skipped flag is bad enough on its own, but
    here a typo'd `--acount U123` would also promote `U123` into the
    positional PATH — restoring the wrong file for the wrong reason with no
    warning at all.
    """
    opts = {"account": None, "output": None, "path": None,
            "confirm": False, "json": False}
    i = 0
    while i < len(subargs):
        a = subargs[i]
        if a == "--account" and i + 1 < len(subargs):
            opts["account"] = subargs[i + 1]; i += 2
        elif a in ("-o", "--output") and i + 1 < len(subargs):
            opts["output"] = subargs[i + 1];  i += 2
        elif a == "--confirm":
            opts["confirm"] = True;           i += 1
        elif a == "--json":
            opts["json"] = True;              i += 1
        elif not a.startswith("-"):
            if opts["path"] is not None:
                print(f"[ERROR] Unexpected extra argument '{a}' "
                      f"(PATH already given: {opts['path']}).")
                sys.exit(1)
            opts["path"] = a;                 i += 1
        else:
            print(f"[ERROR] Unknown or incomplete option '{a}' for the state "
                  "subcommand. Valid: --account ACCT, -o/--output PATH, "
                  "--confirm, --json.")
            sys.exit(1)
    return opts


def _state_module():
    """Import the state_file module from the repo next to this script."""
    sys.path.insert(0, _IBCTL_DIR)
    from ib import state_file
    return state_file


def _state_collect(subargs: list) -> None:
    """Collect a STATE snapshot from a stopped system and write it to a file."""
    opts = _state_args(subargs)
    state_mod = _state_module()
    account = opts["account"] or _discover_account()

    # Collection reads plugin state.json files that a RUNNING engine only
    # flushes at stop/save points, so a snapshot taken mid-session is stale by
    # however long the session has run. Warn — don't refuse: a leftover socket
    # from a crashed engine shouldn't block collecting, and the engine writes
    # its own STATE.json on any clean stop anyway.
    live_sockets = [p for p in (DEFAULT_SOCKET_PATH,
                                _socket_for_env("paper"),
                                _socket_for_env("live"))
                    if os.path.exists(p)]
    if live_sockets:
        print(f"[WARN] An engine may be running (socket present: "
              f"{', '.join(live_sockets)}). A snapshot from a running system "
              "reads each plugin's last-flushed state, not its live state — "
              "prefer stopping the engine first (it writes STATE.json itself "
              "on a clean stop).")

    snapshot = state_mod.collect_state(account)
    path = state_mod.write_state(
        snapshot, opts["output"] or opts["path"] or state_mod.default_state_path()
    )
    print(f"[OK] STATE collected for {account} → {path}")
    print(f"     {len(snapshot['plugin_registry'])} registry entries, "
          f"{len(snapshot['plugin_files'])} plugin files")
    for item in snapshot.get("not_carried", []):
        if item.get("exists"):
            print(f"     not carried: {item['path']} ({item['reason']})")


def _state_show(subargs: list) -> None:
    """Summarize a STATE file without applying it."""
    opts = _state_args(subargs)
    state_mod = _state_module()
    path = opts["path"] or state_mod.default_state_path()
    try:
        snapshot = state_mod.load_state(path)
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

    if opts["json"]:
        print(json.dumps(snapshot, indent=2, default=str))
        return

    src = snapshot.get("source", {})
    print(f"STATE file : {path}")
    print(f"  created  : {snapshot.get('created_at')}")
    print(f"  account  : {src.get('account_id')} ({src.get('env')})")
    print(f"  host     : {src.get('host')}")
    print(f"  commit   : {src.get('repo_commit')}")
    print(f"  plugins  : {len(snapshot.get('plugin_registry', []))} registry entries")
    for entry in snapshot.get("plugin_registry", []):
        print(f"    {entry['slot']:<24} {entry.get('status', '?'):<10} {entry.get('class_path', '')}")
    print(f"  files    : {len(snapshot.get('plugin_files', []))}")
    for entry in snapshot.get("plugin_files", []):
        print(f"    {entry['path']}")
    for item in snapshot.get("not_carried", []):
        if item.get("exists"):
            print(f"  not carried: {item['path']} ({item['reason']})")


def _state_restore(subargs: list) -> None:
    """Apply a STATE file to this machine's stores (offline).

    Defaults to a dry run — writing another machine's positions and plugin
    registry over this one's is not something to do on a typo.
    """
    opts = _state_args(subargs)
    state_mod = _state_module()
    path = opts["path"] or state_mod.default_state_path()
    try:
        snapshot = state_mod.load_state(path)
        report = state_mod.restore_state(
            snapshot,
            expected_account=opts["account"],
            dry_run=not opts["confirm"],
        )
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

    print(state_mod.format_restore_report(report))
    if not opts["confirm"]:
        print("\nDry run — nothing was written. Re-run with --confirm to apply.")


def _state_dispatch(subargs: list) -> None:
    """Route `ibctl.py state <sub>`; never touches the command socket."""
    if not subargs:
        print("Usage: state <collect|show|restore> [PATH] [--account ACCOUNT] "
              "[-o PATH] [--json] [--confirm]")
        sys.exit(1)
    sub = subargs[0].lower()
    if sub == "collect":
        _state_collect(subargs[1:])
    elif sub == "show":
        _state_show(subargs[1:])
    elif sub == "restore":
        _state_restore(subargs[1:])
    else:
        print(f"Unknown state subcommand '{sub}'. Use collect, show, or restore.")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Command-line client for TWS Headless",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  status               Get portfolio status
  positions            List all positions with details
  summary [--json]     Account summary with plugin breakdown

  Simple orders (market only, requires existing position):
  sell SYMBOL QTY      Sell shares (use 'all' for entire position, --confirm to execute)
  buy SYMBOL QTY       Buy shares (--confirm to execute)
  liquidate [SYMBOL]   Liquidate positions (add --confirm to execute)

  Advanced orders (all IB order types):
  order ACTION SYMBOL QTY [TYPE] [options] [--confirm]
                       ACTION: buy or sell
                       TYPE: market (default), limit PRICE, stop PRICE,
                             stop-limit STOP LIMIT, trail AMOUNT|PERCENT,
                             moc, loc PRICE, moo, loo PRICE
                       --tif TIF: day (default), gtc, ioc, fok

  Plugin-attributed trades:
  trade PLUGIN ACTION SYMBOL QTY [--confirm] [--reason "text"]
                       Execute trade with plugin attribution

  Internal transfers (bookkeeping only, no actual trades):
  transfer cash FROM TO AMOUNT [--confirm]
                       Transfer cash between plugins
  transfer position FROM TO SYMBOL QTY [--confirm]
                       Transfer position between plugins
  transfer list PLUGIN Show transferable assets in a plugin

  Plugin commands:
  All NAME_OR_ID args accept a plugin name or instance_id (UUID).
  Use instance_id to target a specific instance when multiple
  instances of the same plugin are loaded.

  plugin list                       List all plugins
  plugin load PATH[=SLOT] [DESC]    Load plugin; optional =SLOT for instance key
  plugin unload NAME_OR_ID          Unload a plugin
  plugin status NAME_OR_ID          Get plugin status
  plugin start NAME_OR_ID           Start a plugin
  plugin stop NAME_OR_ID            Stop a plugin
  plugin freeze NAME_OR_ID          Freeze a plugin (pause with state save)
  plugin resume NAME_OR_ID          Resume a frozen plugin
  plugin enable NAME_OR_ID          Enable plugin for execution
  plugin disable NAME_OR_ID         Disable plugin
  plugin trigger NAME_OR_ID         Manually trigger plugin run
  plugin dump NAME_OR_ID            Dump plugin positions and open orders
  plugin request NAME TYPE [JSON]   Send a typed request to handle_request()
  plugin message NAME [JSON]        Send arbitrary JSON message to plugin
  plugin help NAME                  Show plugin CLI help (cli_help())
  plugin instruments list NAME      List instruments for a plugin instance
  plugin instruments add NAME SYM [--weight W] [--exchange X] [--currency C] [--sec-type T] [--disabled]
  plugin instruments remove NAME SYM   Remove an instrument
  plugin instruments enable NAME SYM   Enable an instrument
  plugin instruments disable NAME SYM  Disable an instrument
  plugin instruments clear NAME        Remove all instruments from a plugin
  plugin instruments reload NAME       Re-read instruments from SQLite into memory
  plugin export SLOT [FILE]            Export instance to portable JSON
  plugin import FILE                   Import instance from JSON

  historical fetch SYMBOL [--bar-size X] [--duration X] [--end DATETIME]
                       [--what TRADES|MIDPOINT|BID|ASK] [--type etf|stock|forex]
                       [--no-rth]
                       Fetch bars from IB, print a bar table, and save to the
                       configured BarStore DB (default: historical/bars.db).
                       Shell-quote multi-word values: --bar-size "5 mins"
  historical coverage [--symbol SYMBOL]
                       Show coverage intervals and bar counts in the DB.
  historical purge --symbol SYMBOL [--bar-size X] [--what X] [--no-rth]
                       Delete a series from the DB.
  historical set-db PATH
                       Persist a new DB path (stored in historical/config.json).
  historical get-db    Show the current DB path.

  reconcile            Sync plugin holdings with IB account

  Account-level (the IB account itself, not plugin ledgers):
  account [--json]     IB account values, including every raw tag IB sent
  commissions [--symbol SYM] [--days N] [--json]
                       Commission and fee report, with a per-symbol breakdown

  STATE snapshots (local — no running engine required):
  state collect [--account ACCT] [-o PATH]
                       Collect a relocation snapshot from a stopped system
  state show [PATH] [--json]
                       Summarize a STATE file without applying it
  state restore [PATH] [--account ACCT] [--confirm]
                       Apply a STATE file to this machine (dry run without --confirm)
  pause                Pause algorithm/plugin execution
  resume               Resume algorithm/plugin execution
  ping                 Test server connectivity
  stop                 Shutdown the server gracefully
  shutdown             Shutdown the server gracefully (alias for stop)

Examples:
  ./ibctl.py status
  ./ibctl.py positions
  ./ibctl.py summary --json
  ./ibctl.py order buy SPY 100                      # Market order (dry run)
  ./ibctl.py order buy SPY 100 --confirm            # Market order (execute)
  ./ibctl.py order buy SPY 100 limit 450.00         # Limit order
  ./ibctl.py order sell QQQ 50 stop 380             # Stop order
  ./ibctl.py order buy AAPL 25 stop-limit 175 170   # Stop-limit order
  ./ibctl.py order sell MSFT 30 trail 2.00          # Trailing stop $2
  ./ibctl.py order buy SPY 100 moc --confirm        # Market on Close
  ./ibctl.py order buy SPY 100 limit 450 --tif gtc  # Good till cancelled
  ./ibctl.py trade momentum_5day BUY SPY 100 --confirm
  ./ibctl.py transfer list _unassigned              # Show transferable assets
  ./ibctl.py transfer cash _unassigned momentum_5day 10000 --confirm
  ./ibctl.py transfer position _unassigned momentum_5day SPY 50 --confirm
  ./ibctl.py plugin list
  ./ibctl.py plugin load /path/to/plugin.py
  ./ibctl.py plugin load /path/to/plugin.py=spy_momentum
  ./ibctl.py plugin load /path/to/plugin.py '{"symbol": "AAPL"}'
  ./ibctl.py plugin help momentum_5day
  ./ibctl.py plugin message momentum_5day '{"action": "set_threshold", "value": 0.5}'
  ./ibctl.py plugin status momentum_5day
  ./ibctl.py plugin start momentum_5day
  ./ibctl.py plugin stop 78b052d2-17e7-4ee5-ac03-282e0cd05c2b
  ./ibctl.py plugin instruments list spy_momentum
  ./ibctl.py plugin instruments add spy_momentum SPY --weight 1.0
  ./ibctl.py plugin instruments remove spy_momentum AAPL
  ./ibctl.py plugin instruments enable spy_momentum QQQ
        """,
    )

    parser.add_argument(
        "command",
        nargs="*",
        help="Command and arguments to send",
    )
    parser.add_argument(
        "--socket", "-s",
        default=None,
        help="Socket path — overrides --env/--port. Default: auto-detect the "
             "one running engine (legacy pre-separation socket, else a lone "
             "paper socket); refuses to guess and requires --env if a live "
             "engine is running, alone or alongside paper",
    )
    parser.add_argument(
        "--env",
        choices=["paper", "live"],
        default=None,
        help="Target the paper or live engine explicitly (selects "
             "~/.tws_headless_{env}.sock) — required whenever a live engine "
             "might be running, since auto-detection never targets live",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Derive --env from a TWS/Gateway port (paper: 7497/4002, "
             "live: 7496/4001); an unrecognized port is a hard error, not ignored",
    )
    parser.add_argument(
        "--timeout", "-t",
        type=float,
        default=10.0,
        help="Timeout in seconds (default: 10)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed output",
    )
    parser.add_argument(
        "--json", "-j",
        action="store_true",
        help="Output raw JSON response",
    )
    parser.add_argument(
        "--token",
        help="Authentication token (if server requires auth)",
    )
    parser.add_argument(
        "--request-token",
        help="Request token for tracking/deduplication",
    )

    args, extra = parser.parse_known_args()
    # Flags not recognised by the top-level parser (e.g. --bar-size, --db)
    # are treated as part of the subcommand's argument list.
    args.command = list(args.command) + extra

    if not args.command:
        parser.print_help()
        sys.exit(0)
        return

    # STATE subcommand — entirely local, and dispatched before socket resolution
    # because its whole purpose is to work on a system with no engine running.
    if args.command[0].lower() == "state":
        _state_dispatch(args.command[1:])
        return

    # Resolve which engine socket to talk to (paper vs live separation).
    args.socket, socket_err = resolve_socket_path(args.socket, args.env, args.port)
    if socket_err:
        print(f"Error: {socket_err}", file=sys.stderr)
        sys.exit(2)

    # Historical subcommand — handled locally (coverage/purge) or via engine (fetch)
    if args.command[0].lower() == "historical":
        subargs = args.command[1:]
        if not subargs:
            print("Usage: historical <fetch|coverage|purge> ...")
            sys.exit(1)
        sub = subargs[0].lower()
        if sub == "fetch":
            _historical_fetch(subargs[1:], args.socket, args.timeout)
        elif sub == "coverage":
            _historical_coverage(subargs[1:])
        elif sub == "purge":
            _historical_purge(subargs[1:])
        elif sub == "set-db":
            _historical_set_db(subargs[1:])
        elif sub == "get-db":
            print(_historical_db_path())
        else:
            print(f"Unknown historical subcommand '{sub}'. Use fetch, coverage, purge, set-db, or get-db.")
            sys.exit(1)
        return

    # --json is NOT forwarded to the server. It used to be, for the handlers
    # that re-rendered their `message` as a JSON dump when they saw the flag,
    # and the result was output carrying the same content twice: `message` held
    # an escaped JSON string of exactly what `data` already held as structure.
    # Raw output became unreadable, and `message` — the obvious field to reach
    # for — was the wrong one to parse.
    #
    # Each field now has one job. `message` is always the human-readable text,
    # `data` is always the machine-readable structure, and --json prints the
    # whole envelope so scripts can branch on `status` and read `.data`:
    #
    #     ./ibctl.py account --json | jq .data
    #     ./ibctl.py commissions --json | jq '.data.by_symbol'

    # Build command string
    command_str = " ".join(args.command)

    # Send command
    result = send_command(
        command=command_str,
        socket_path=args.socket,
        timeout=args.timeout,
        token=args.token,
        request_token=args.request_token,
    )

    # Output result
    if args.json:
        print(json.dumps(result.to_dict(), indent=2))
    else:
        format_result(result, verbose=args.verbose)

    # Exit with error code if command failed
    if result.status in (CommandStatus.ERROR, CommandStatus.UNAUTHORIZED):
        sys.exit(1)


if __name__ == "__main__":
    main()
