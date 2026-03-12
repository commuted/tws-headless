#!/usr/bin/env python3
"""
run_paper_tests.py — Sequential runner for IB paper trading test plugins.

Loads each plugin, triggers its tests, waits for results, and prints a
formatted summary.  The engine must already be running and connected to a
paper-trading account.

Usage:
    ./run_paper_tests.py                      # all order-type plugins (1-5)
    ./run_paper_tests.py --feeds              # feed tests only
    ./run_paper_tests.py --historical         # historical data tests only
    ./run_paper_tests.py --all                # feeds + historical + all order plugins + orders6
    ./run_paper_tests.py --only 1 3 5         # specific order plugins
    ./run_paper_tests.py --orders6            # round-trip lifecycle tests (orders 6)
    ./run_paper_tests.py --open               # market-open order types (MOO, LOO, Auction)
    ./run_paper_tests.py --close              # market-close order types (MOC, LOC)
    ./run_paper_tests.py --interface          # interface validation tests only
    ./run_paper_tests.py --socket /tmp/x.sock # custom socket
    ./run_paper_tests.py --timeout 3600       # per-plugin timeout (seconds)
    ./run_paper_tests.py --dry-run            # show plan, don't execute
    ./run_paper_tests.py --restart-engine     # graceful shutdown + fresh start first
    ./run_paper_tests.py --restart-engine --engine-port 4002  # use gateway port

Session-specific plugins (run at the appropriate market time):
    --open   Submit before 9:25 AM ET  — MOO, LOO, At-Auction
    --close  Submit before 3:50 PM ET  — MOC, LOC
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Reuse ibctl's socket transport
# ---------------------------------------------------------------------------
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
from ibctl import DEFAULT_SOCKET_PATH, CommandStatus, send_command  # noqa: E402

# ---------------------------------------------------------------------------
# Plugin registry
# ---------------------------------------------------------------------------

INTERFACE_PLUGIN = {
    "name": "paper_test_interface",
    "module": "plugins.paper_tests.paper_test_interface",
    "label": "Interface Tests",
    "timeout": 120.0,
}

FEED_PLUGIN = {
    "name": "paper_test_feeds",
    "module": "plugins.paper_tests.paper_test_feeds",
    "label": "Feed Tests",
    "timeout": 120.0,
}

HISTORICAL_PLUGIN = {
    "name": "paper_test_historical",
    "module": "plugins.paper_tests.paper_test_historical",
    "label": "Historical Data Tests",
    "timeout": 300.0,
}

# Pair run together to test StreamManager shared-subscription paths
DUAL_FEED_PLUGINS = [
    FEED_PLUGIN,
    {
        "name": "paper_test_feeds_2",
        "module": "plugins.paper_tests.paper_test_feeds_2",
        "label": "Feed Tests 2 (concurrent)",
        "timeout": 120.0,
    },
]

ORDER_PLUGINS = [
    {
        "name": "paper_test_orders_1",
        "module": "plugins.paper_tests.paper_test_orders_1",
        "label": "Orders 1 — Market, Limit, Stop, STP LMT, MOC, MOO, MTL",
        "timeout": 2700.0,
    },
    {
        "name": "paper_test_orders_2",
        "module": "plugins.paper_tests.paper_test_orders_2",
        "label": "Orders 2 — LOC, LOO, MIT, LIT, Midprice, Discretionary, Trail",
        "timeout": 2700.0,
    },
    {
        "name": "paper_test_orders_3",
        "module": "plugins.paper_tests.paper_test_orders_3",
        "label": "Orders 3 — Trail-Limit, PEG MKT, REL, PASSV REL, PEG MID, Auction, Adjusted",
        "timeout": 2700.0,
    },
    {
        "name": "paper_test_orders_4",
        "module": "plugins.paper_tests.paper_test_orders_4",
        "label": "Orders 4 — Bracket, OCA, Scale, Price/Time/Volume/Execution Conditions",
        "timeout": 2700.0,
    },
    {
        "name": "paper_test_orders_5",
        "module": "plugins.paper_tests.paper_test_orders_5",
        "label": "Orders 5 — Margin/PriceChange/PctChange Conditions; BAG/hedge stubs",
        "timeout": 2700.0,
    },
]

ORDERS_6_PLUGIN = {
    "name": "paper_test_orders_6",
    "module": "plugins.paper_tests.paper_test_orders_6",
    "label": "Orders 6 — Round-trip lifecycle: IOC/GTC/FOK, Adaptive algo, Iceberg",
    "timeout": 2700.0,
}

# Session-specific plugins — must run at the right time of day
OPEN_PLUGIN = {
    "name": "paper_test_orders_open",
    "module": "plugins.paper_tests.paper_test_orders_open",
    "label": "Orders Open — MOO (MKT+OPG), LOO (LMT+OPG), At-Auction (MTL+AUC)  [submit before 9:25 AM ET]",
    "timeout": 300.0,
}

CLOSE_PLUGIN = {
    "name": "paper_test_orders_close",
    "module": "plugins.paper_tests.paper_test_orders_close",
    "label": "Orders Close — MOC, LOC  [submit before 3:50 PM ET]",
    "timeout": 300.0,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cmd(command: str, socket_path: str, timeout: float) -> Tuple[bool, str, Dict]:
    """Send one command; return (ok, message, data)."""
    r = send_command(command, socket_path=socket_path, timeout=timeout)
    ok = r.status == CommandStatus.SUCCESS
    return ok, r.message, r.data


def _restart_engine(
    socket_path: str,
    port: int = 7497,
    mode: str = "immediate",
    client_id: int = 4,
    startup_wait: float = 12.0,
) -> bool:
    """Gracefully shut down a running engine, then start a fresh one.

    Shutdown strategy:
      1. Send the 'shutdown' command via the socket (clean stop: all plugins
         get stop() called, test orders are cancelled).
      2. If the socket is unreachable, fall back to SIGINT on any running
         ib.run_engine process.
      3. Wait up to 15 s for the process to exit.
      4. Start a new engine and wait *startup_wait* seconds for it to connect.

    Returns True if the new engine is reachable and connected, False otherwise.
    """
    # ── 1. Attempt graceful shutdown via socket ───────────────────────────
    pids = [int(p) for p in subprocess.run(
        ["pgrep", "-f", "ib.run_engine"], capture_output=True, text=True
    ).stdout.split() if p.strip()]

    if pids:
        ok, _, _ = _cmd("shutdown", socket_path, timeout=5.0)
        if ok:
            print("  Sent shutdown command — waiting for engine to exit...")
        else:
            print("  Socket unreachable — sending SIGINT to engine process(es)...")
            for pid in pids:
                try:
                    os.kill(pid, signal.SIGINT)
                except ProcessLookupError:
                    pass

        # ── 2. Wait for process(es) to exit ──────────────────────────────
        deadline = time.time() + 15.0
        while time.time() < deadline:
            alive = [p for p in pids if _pid_alive(p)]
            if not alive:
                break
            time.sleep(0.5)
        else:
            # Last resort: SIGINT again (engine requires 3 to force-quit)
            for pid in [p for p in pids if _pid_alive(p)]:
                for _ in range(3):
                    try:
                        os.kill(pid, signal.SIGINT)
                    except ProcessLookupError:
                        break
                    time.sleep(0.3)
            time.sleep(2.0)

        print("  Engine stopped.")
    else:
        print("  No running engine found — starting fresh.")

    # ── 3. Start new engine ───────────────────────────────────────────────
    log_path = "/tmp/engine.log"
    with open(log_path, "a") as log_fh:
        subprocess.Popen(
            [
                sys.executable, "-m", "ib.run_engine",
                "--port", str(port),
                "--mode", mode,
                "--client-id", str(client_id),
                "--verbose",
            ],
            stdout=log_fh,
            stderr=log_fh,
            start_new_session=True,
        )
    print(f"  Engine started (log: {log_path}). Waiting {startup_wait:.0f}s for connect...")
    time.sleep(startup_wait)

    # ── 4. Verify reachable and connected ────────────────────────────────
    ok, msg, _ = _cmd("status", socket_path, timeout=10.0)
    if not ok:
        print(f"  [ERROR] Engine not responding after restart: {msg}")
        return False
    if "Connected: True" not in msg:
        print(f"  [ERROR] Engine started but not connected to IB: {msg.splitlines()[0]}")
        return False
    print(f"  Engine ready: {msg.splitlines()[0]}")
    return True


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def _is_loaded(plugin_name: str, socket_path: str) -> bool:
    """Return True if the plugin is already registered with the engine."""
    ok, _, data = _cmd("plugin list", socket_path, timeout=10.0)
    if not ok:
        return False
    return plugin_name in data.get("plugins", {})


def _poll_status(plugin_name: str, socket_path: str, stop_event: threading.Event):
    """Background thread: print progress dots while run_tests is in flight."""
    last_count = 0
    while not stop_event.is_set():
        time.sleep(5)
        if stop_event.is_set():
            break
        ok, _, data = _cmd(
            f"plugin request {plugin_name} get_status",
            socket_path,
            timeout=10.0,
        )
        if ok:
            count = data.get("result_count", 0)
            if count != last_count:
                print(f"    ... {count} test(s) complete", flush=True)
                last_count = count
        else:
            # Plugin may have unloaded (auto-unload on completion)
            break


# ---------------------------------------------------------------------------
# Result formatting
# ---------------------------------------------------------------------------

def _fmt_interface_results(results: List[Dict]) -> str:
    lines = []
    col = "{:<40} {:<16} {:<6} {}"
    lines.append(col.format("Test", "Category", "Pass?", "Detail / Error"))
    lines.append("-" * 90)
    for r in results:
        passed = "PASS" if r.get("passed") else "FAIL"
        detail = r.get("detail") or r.get("error_message", "")
        lines.append(
            col.format(
                r.get("test_name", "?")[:40],
                r.get("category", "?")[:16],
                passed,
                detail[:45],
            )
        )
    return "\n".join(lines)


def _fmt_order_results(results: List[Dict]) -> str:
    lines = []
    col = "{:<30} {:<14} {:<10} {:<14} {}"
    lines.append(col.format("Test", "Order Type", "Status", "Fill", "Notes"))
    lines.append("-" * 90)
    for r in results:
        if r.get("is_stub"):
            status = "STUB"
            fill = "-"
        elif r.get("error_message"):
            status = "ERROR"
            fill = "-"
        elif r.get("fill_side"):
            status = "FILLED"
            fill = f"{r['fill_side']}@${r.get('fill_price', 0):.2f}"
        elif r.get("submitted"):
            status = "SUBMITTED"
            fill = "pending"
        else:
            status = "FAILED"
            fill = "-"
        note = r.get("error_message") or r.get("stub_reason", "")[:40]
        lines.append(
            col.format(
                r.get("test_name", "?")[:30],
                r.get("order_type", "?")[:14],
                status,
                fill,
                note,
            )
        )
    return "\n".join(lines)


def _fmt_roundtrip_results(results: List[Dict]) -> str:
    lines = []
    col = "{:<22} {:<16} {:<6} {:<10} {:<10} {:<9} {}"
    lines.append(col.format("Test", "Type", "Pass?", "Entry@", "Exit@", "PnL", "Notes"))
    lines.append("-" * 100)
    for r in results:
        passed = "PASS" if r.get("passed") else "FAIL"
        if r.get("expected_no_fill"):
            entry_s = "—"
            exit_s = "—"
            pnl_s = "—"
            status = "CONFIRMED" if r.get("no_fill_confirmed") else "MISS"
            notes = r.get("notes") or status
        elif r.get("error_message"):
            entry_s = "—"
            exit_s = "—"
            pnl_s = "—"
            notes = r.get("error_message", "")[:45]
        else:
            ep = r.get("entry_fill_price", 0)
            xp = r.get("exit_fill_price", 0)
            entry_s = f"${ep:.3f}" if ep else "pending"
            exit_s = f"${xp:.3f}" if xp else "pending"
            pnl = r.get("net_pnl", 0)
            pnl_s = f"${pnl:+.3f}" if r.get("round_trip_complete") else "—"
            notes = r.get("notes") or ("roundtrip" if r.get("round_trip_complete") else "open")
        lines.append(
            col.format(
                r.get("test_name", "?")[:22],
                r.get("order_type", "?")[:16],
                passed,
                entry_s[:10],
                exit_s[:10],
                pnl_s[:9],
                str(notes)[:45],
            )
        )
    return "\n".join(lines)


def _fmt_historical_results(results: List[Dict]) -> str:
    lines = []
    col = "{:<25} {:<8} {:<10} {:<6} {:<6} {}"
    lines.append(col.format("Test", "BarSize", "Duration", "Bars", "Pass?", "Detail"))
    lines.append("-" * 84)
    for r in results:
        passed = "PASS" if r.get("passed") else "FAIL"
        bars = f"{r.get('bars_received', 0)}/{r.get('min_bars_required', 0)}"
        detail = r.get("error_message") or f"{r.get('first_date', '')}..{r.get('last_date', '')}"
        lines.append(
            col.format(
                r.get("test_name", "?")[:25],
                r.get("bar_size", "?")[:8],
                r.get("duration_str", "?")[:10],
                bars,
                passed,
                detail[:50],
            )
        )
    return "\n".join(lines)


def _fmt_feed_results(results: List[Dict]) -> str:
    lines = []
    col = "{:<30} {:<8} {:<8} {:<8} {}"
    lines.append(col.format("Test", "Type", "Data", "Pass?", "Detail"))
    lines.append("-" * 80)
    for r in results:
        passed = "PASS" if r.get("passed") else "FAIL"
        detail = r.get("error_message") or str(r.get("details", {}))[:40]
        lines.append(
            col.format(
                r.get("test_name", "?")[:30],
                r.get("feed_type", "?")[:8],
                r.get("data_type", "?")[:8],
                passed,
                detail,
            )
        )
    return "\n".join(lines)


def _print_summary(label: str, summary: Dict):
    total = summary.get("total", 0)
    errors = summary.get("errors", [])

    # Round-trip summary (orders 6)
    if "round_trips" in summary:
        passed = summary.get("passed", 0)
        round_trips = summary.get("round_trips", 0)
        no_fill_ok = summary.get("no_fill_confirmed", 0)
        total_pnl = summary.get("total_pnl", 0.0)
        parts = [
            f"passed={passed}/{total}",
            f"round_trips={round_trips}",
            f"no_fill_confirmed={no_fill_ok}",
            f"pnl=${total_pnl:+.3f}",
        ]
        if errors:
            parts.append(f"errors={len(errors)} ({', '.join(errors[:3])})")
        ok = not errors and passed == total
        marker = "[PASS]" if ok else "[WARN]"
        print(f"  {marker} {label}: {', '.join(parts)}")
        return

    filled = summary.get("filled", summary.get("passed", 0))
    stubs = summary.get("stubs", 0)
    submitted = summary.get("submitted", 0)

    parts = []
    if "submitted" in summary:
        parts.append(f"submitted={submitted}/{total}")
    parts.append(f"filled={filled}/{total}")
    if stubs:
        parts.append(f"stubs={stubs}")
    if errors:
        parts.append(f"errors={len(errors)} ({', '.join(errors[:3])})")

    ok = not errors and (filled > 0 or stubs > 0)
    marker = "[PASS]" if ok else "[WARN]"
    print(f"  {marker} {label}: {', '.join(parts)}")


# ---------------------------------------------------------------------------
# Per-plugin runner
# ---------------------------------------------------------------------------

def _load_and_start(plugin_info: Dict, socket_path: str) -> bool:
    """Load and start one plugin. Returns True on success."""
    name = plugin_info["name"]
    module = plugin_info["module"]
    if _is_loaded(name, socket_path):
        print(f"  Plugin '{name}' already loaded — skipping load step")
    else:
        print(f"  Loading {module} ...", end=" ", flush=True)
        ok, msg, _ = _cmd(f"plugin load {module}", socket_path, timeout=15.0)
        if not ok:
            print(f"FAILED\n  {msg}")
            return False
        print("OK")
    print(f"  Starting '{name}' ...", end=" ", flush=True)
    ok, msg, _ = _cmd(f"plugin start {name}", socket_path, timeout=15.0)
    if not ok:
        print(f"FAILED\n  {msg}")
        return False
    print("OK")
    return True


def run_plugins_parallel(
    plugin_infos: List[Dict],
    socket_path: str,
    timeout: float,
    dry_run: bool,
) -> bool:
    """Load all plugins sequentially, then run their tests concurrently."""
    if dry_run:
        for info in plugin_infos:
            print(f"  [DRY-RUN] Would load {info['module']} and run concurrently")
        return True

    # Load and start all plugins before firing off concurrent run_tests calls.
    # Sequential here avoids racing on the socket and makes errors easy to read.
    for info in plugin_infos:
        if not _load_and_start(info, socket_path):
            return False

    print(f"\n  Launching {len(plugin_infos)} plugins concurrently ...\n")

    results_map: Dict[str, Tuple[bool, str, Dict, float]] = {}
    lock = threading.Lock()

    def _run_one(info: Dict):
        name = info["name"]
        t0 = time.time()
        ok, msg, data = _cmd(
            f"plugin request {name} run_tests", socket_path, timeout=timeout
        )
        elapsed = time.time() - t0
        with lock:
            results_map[name] = (ok, msg, data, elapsed)

    threads = [
        threading.Thread(target=_run_one, args=(info,), daemon=True)
        for info in plugin_infos
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    overall_ok = True
    for info in plugin_infos:
        name = info["name"]
        label = info["label"]
        ok, msg, data, elapsed = results_map.get(name, (False, "no result", {}, 0.0))
        print(f"\n  --- {label} ({elapsed:.1f}s) ---")
        if not ok:
            print(f"  [ERROR] {msg}")
            overall_ok = False
            continue
        results = data.get("results", [])
        summary = data.get("summary", {})
        if results:
            print(_fmt_feed_results(results))
        if summary:
            _print_summary(label, summary)

    return overall_ok


def run_plugin(plugin_info: Dict, socket_path: str, timeout: float, dry_run: bool) -> bool:
    """Load, start, run tests, and report for one plugin. Returns True on success."""
    name = plugin_info["name"]
    module = plugin_info["module"]
    label = plugin_info["label"]

    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"{'='*70}")

    if dry_run:
        print(f"  [DRY-RUN] Would load {module}, start {name}, request run_tests")
        return True

    # 1. Load (unless already present — e.g. paper_test_feeds at engine startup)
    if _is_loaded(name, socket_path):
        print(f"  Plugin '{name}' already loaded — skipping load step")
    else:
        print(f"  Loading {module} ...", end=" ", flush=True)
        ok, msg, _ = _cmd(f"plugin load {module}", socket_path, timeout=15.0)
        if not ok:
            print(f"FAILED\n  {msg}")
            return False
        print("OK")

    # 2. Start
    print(f"  Starting '{name}' ...", end=" ", flush=True)
    ok, msg, _ = _cmd(f"plugin start {name}", socket_path, timeout=15.0)
    if not ok:
        print(f"FAILED\n  {msg}")
        return False
    print("OK")

    # 3. Run tests (blocks until complete — can take tens of minutes)
    print(f"  Running tests (timeout={timeout:.0f}s) ...")
    stop_poll = threading.Event()
    poll_thread = threading.Thread(
        target=_poll_status,
        args=(name, socket_path, stop_poll),
        daemon=True,
    )
    poll_thread.start()

    t0 = time.time()
    ok, msg, data = _cmd(f"plugin request {name} run_tests", socket_path, timeout=timeout)
    elapsed = time.time() - t0
    stop_poll.set()
    poll_thread.join(timeout=2)

    print(f"  Completed in {elapsed:.1f}s")

    if not ok:
        print(f"  [ERROR] {msg}")
        return False

    # 4. Display results
    results = data.get("results", [])
    summary = data.get("summary", {})

    if results:
        print()
        first = results[0] if results else {}
        if "round_trip_complete" in first:
            print(_fmt_roundtrip_results(results))
        elif "order_type" in first:
            print(_fmt_order_results(results))
        elif "bar_size" in first:
            print(_fmt_historical_results(results))
        elif "category" in first:
            print(_fmt_interface_results(results))
        else:
            print(_fmt_feed_results(results))
        print()

    if summary:
        _print_summary(label, summary)

    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Run IB paper trading test plugins sequentially",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--socket",
        default=DEFAULT_SOCKET_PATH,
        help=f"Unix socket path (default: {DEFAULT_SOCKET_PATH})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Per-plugin run_tests timeout in seconds (default: per-plugin)",
    )
    parser.add_argument(
        "--feeds",
        action="store_true",
        help="Run the paper_test_feeds plugin",
    )
    parser.add_argument(
        "--feeds-dual",
        action="store_true",
        dest="feeds_dual",
        help="Run two feed test instances concurrently (tests StreamManager sharing)",
    )
    parser.add_argument(
        "--historical",
        action="store_true",
        help="Run the paper_test_historical plugin",
    )
    parser.add_argument(
        "--interface",
        action="store_true",
        help="Run the paper_test_interface plugin (PluginBase interface validation)",
    )
    parser.add_argument(
        "--orders6",
        action="store_true",
        help="Run the paper_test_orders_6 plugin (round-trip lifecycle tests)",
    )
    parser.add_argument(
        "--open",
        action="store_true",
        help=(
            "Run market-open order types (MOO, LOO, At-Auction); "
            "submit before 9:25 AM ET"
        ),
    )
    parser.add_argument(
        "--close",
        action="store_true",
        help=(
            "Run market-close order types (MOC, LOC); "
            "submit before 3:50 PM ET"
        ),
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="run_all",
        help="Run feeds + historical + all order plugins (1-5) + orders6",
    )
    parser.add_argument(
        "--only",
        nargs="+",
        type=int,
        metavar="N",
        help="Run only the specified order plugin numbers (1-5)",
    )
    parser.add_argument(
        "--restart-engine",
        action="store_true",
        dest="restart_engine",
        help=(
            "Gracefully shut down the running engine before starting tests "
            "(sends 'shutdown' command so plugins cancel open orders), "
            "then start a fresh engine. Picks up code changes."
        ),
    )
    parser.add_argument(
        "--engine-port",
        type=int,
        default=7497,
        dest="engine_port",
        help="Port for --restart-engine to use when starting the new engine (default: 7497)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show which plugins would run without executing",
    )
    args = parser.parse_args()

    # ── Optional engine restart (graceful shutdown + fresh start) ────────────
    if args.restart_engine and not args.dry_run:
        print("Restarting engine (graceful shutdown → fresh start)...")
        if not _restart_engine(args.socket, port=args.engine_port):
            sys.exit(1)

    # Build plugin list
    plugins_to_run: List[Dict] = []

    if args.feeds_dual:
        if not args.dry_run:
            ok, msg, _ = _cmd("status", args.socket, timeout=10.0)
            if not ok:
                print(f"[ERROR] Cannot reach engine at {args.socket}: {msg}")
                sys.exit(1)
            print(f"Engine is up: {msg.splitlines()[0]}")
        timeout = args.timeout or DUAL_FEED_PLUGINS[0]["timeout"]
        print(f"\nRunning {len(DUAL_FEED_PLUGINS)} feed plugins concurrently (timeout={timeout:.0f}s):")
        for p in DUAL_FEED_PLUGINS:
            print(f"  {p['label']}")
        success = run_plugins_parallel(DUAL_FEED_PLUGINS, args.socket, timeout, args.dry_run)
        sys.exit(0 if success else 1)

    if args.run_all:
        plugins_to_run = (
            [INTERFACE_PLUGIN, FEED_PLUGIN, HISTORICAL_PLUGIN]
            + ORDER_PLUGINS
            + [ORDERS_6_PLUGIN]
        )
    elif args.interface:
        plugins_to_run = [INTERFACE_PLUGIN]
    elif args.orders6:
        plugins_to_run = [ORDERS_6_PLUGIN]
    elif args.open:
        plugins_to_run = [OPEN_PLUGIN]
    elif args.close:
        plugins_to_run = [CLOSE_PLUGIN]
    elif args.feeds:
        plugins_to_run = [FEED_PLUGIN]
    elif args.historical:
        plugins_to_run = [HISTORICAL_PLUGIN]
    elif args.only:
        for n in args.only:
            if 1 <= n <= len(ORDER_PLUGINS):
                plugins_to_run.append(ORDER_PLUGINS[n - 1])
            else:
                print(f"Warning: no order plugin #{n} (valid: 1-{len(ORDER_PLUGINS)})")
    else:
        # Default: all order plugins (1-5, not 6 — 6 must be opted in explicitly)
        plugins_to_run = ORDER_PLUGINS

    if not plugins_to_run:
        print("Nothing to run.  Use --help for usage.")
        sys.exit(1)

    # Verify engine is reachable
    if not args.dry_run:
        ok, msg, _ = _cmd("status", args.socket, timeout=10.0)
        if not ok:
            print(f"[ERROR] Cannot reach engine at {args.socket}: {msg}")
            sys.exit(1)
        print(f"Engine is up: {msg.splitlines()[0]}")

    print(f"\nRunning {len(plugins_to_run)} plugin(s):")
    for p in plugins_to_run:
        to = args.timeout or p["timeout"]
        print(f"  {p['label']}  (timeout={to:.0f}s)")

    overall_ok = True
    t_start = time.time()

    for plugin_info in plugins_to_run:
        timeout = args.timeout or plugin_info["timeout"]
        success = run_plugin(plugin_info, args.socket, timeout, args.dry_run)
        if not success:
            overall_ok = False

    elapsed_total = time.time() - t_start
    print(f"\n{'='*70}")
    print(f"  All plugins complete in {elapsed_total:.1f}s")
    print(f"  Overall: {'PASS' if overall_ok else 'WARN/ERROR'}")
    print(f"{'='*70}")

    sys.exit(0 if overall_ok else 1)


if __name__ == "__main__":
    main()
