#!/usr/bin/env python3
"""
historical/recorder.py — keep series alive past IB's retention window.

WHY THIS EXISTS. IB serves historical bars only for a rolling window whose
length depends on the bar size, and the finer the bar the shorter the window.
Measured against this account in September 2026:

    1 min      ~30 days      (a "6 M" request returns EMPTY, not an error)
    5 mins     ~6 months
    1 hour     ~2 years
    1 day      years

Data that ages out is not "slow to fetch", it is GONE — no amount of patience
or rate-limit courtesy brings it back. The only way to hold a five-year
history of 1-minute bid/ask bars is to have been recording it for five years.
This is the thing that does the recording.

It is a scheduler and a gap-filler, not a store. `ib/bar_store.py` already
does the storage properly: series keyed on (symbol, bar_size, what_to_show,
use_rth), coverage tracked as merged intervals, gaps re-fetched and nothing
else. And `ibctl.py historical fetch` already drives a fetch through the
RUNNING ENGINE's IB connection, which is the important part — it means this
never opens a second connection, never competes for a clientId, and cannot
disconnect the trading session by taking an id that was already in use.

What it adds on top is the part neither of those has: knowing WHICH series
must be extended, HOW OFTEN, and — the one that matters — noticing when a
series has fallen so far behind that a permanent hole now exists. A recorder
that silently stops is worse than no recorder, because it leaves you
believing you have history you do not have. --status is the check, and a
non-zero exit is the alarm for cron.

WHAT IT WILL NOT DO. It places no orders and modifies no positions; the only
engine command it issues is `historical fetch`. It does not fetch futures,
because the engine's historical command builds only etf/stock/forex contracts
(ib/run_engine.py) — recording a specific gold contract needs a change there
first, and that is a change to a live trading system, not something to slip
into a data script.

Run it with an interpreter that has ibapi — ibctl imports ib/ to store what
it fetches, so a bare `python3` without ibapi will pull the bars and then
fail to save them. The recorder passes its own sys.executable down to ibctl
so the two never disagree about that.

Usage:
    python3 recorder.py --status              # coverage + how close to permanent loss
    ./recorder.py --dry-run             # what it would fetch, and why
    ./recorder.py                       # one pass; this is the cron entry point
    ./recorder.py --watchlist my.json

Cron, once a day, comfortably inside even the 1-minute window:
    17 20 * * 1-5  cd /home/ron/tws-headless && ./historical/recorder.py \
                       >> historical/recorder.log 2>&1
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

WATCHLIST = os.path.join(HERE, "watchlist.json")
CONFIG = os.path.join(HERE, "config.json")
DEFAULT_DB = os.path.join(HERE, "bars.db")

# Two different numbers that look like one, and must not be conflated.
#
# RETENTION_DAYS is the ALARM threshold: how long a series can go unrecorded
# before data starts aging out of IB's window for good. It wants to be
# pessimistic — an early warning costs one wasted request, a late one costs
# bars nobody can ever get back.
#
# BACKFILL_DAYS is how deep to reach on a first fill. It wants to be
# optimistic, because anything not taken on the initial pass is only
# available until it ages out. The first backfill against this account
# returned 32 days of 1-minute, 214 of 5-minute and 854 of hourly bars —
# comfortably more than the conservative figures below, which is exactly why
# the same constant cannot serve both purposes. Asking for more than IB has
# is free; it returns what it has.
RETENTION_DAYS = {
    "1 min": 25, "2 mins": 50, "5 mins": 150, "15 mins": 300,
    "30 mins": 300, "1 hour": 700, "1 day": 3000,
}

BACKFILL_DAYS = {
    "1 min": 40, "2 mins": 60, "5 mins": 240, "15 mins": 360,
    "30 mins": 360, "1 hour": 900, "1 day": 3650,
}

# Largest span IB will return in a single request, per bar size.
MAX_REQUEST_DAYS = {
    "1 min": 25, "2 mins": 50, "5 mins": 150, "15 mins": 300,
    "30 mins": 300, "1 hour": 365, "1 day": 3000,
}

DEFAULT_WATCHLIST = [
    # The three series any fill question needs: what traded, and the quotes
    # either side of it. Extended hours throughout (use_rth false), because
    # the pre-market is the whole point and RTH-only would silently drop it.
    {"symbol": s, "type": "etf", "bar_size": b, "what": w, "use_rth": False}
    for s in ("GLD",)
    for b in ("1 min", "5 mins", "1 hour")
    for w in ("TRADES", "BID", "ASK")
]


def engine_socket():
    """
    Find the RUNNING engine's control socket rather than assuming ibctl's
    default. The engine separates sockets per account, so a paper session
    listens on ~/.tws_headless_paper.sock while ibctl defaults to
    ~/.tws_headless.sock — and the failure mode is a flat "Server not
    running" against an engine that is plainly running. Order: an explicit
    override, then whatever is actually listening, then the default.
    """
    env = os.environ.get("IBCTL_SOCKET")
    if env and os.path.exists(env):
        return env
    try:
        out = subprocess.run(["ss", "-xl"], capture_output=True, text=True,
                             timeout=10).stdout
        found = [tok for line in out.splitlines() for tok in line.split()
                 if "tws_headless" in tok and tok.endswith(".sock")]
        # Prefer a socket that exists on disk; ss lists the bound name either way.
        for cand in found:
            if os.path.exists(cand):
                return cand
        if found:
            return found[0]
    except (OSError, subprocess.SubprocessError):
        pass
    return os.path.expanduser("~/.tws_headless.sock")


def load_watchlist(path):
    if not os.path.exists(path):
        with open(path, "w") as fh:
            json.dump(DEFAULT_WATCHLIST, fh, indent=2)
        print(f"  wrote a default watchlist to {path}")
        return DEFAULT_WATCHLIST
    with open(path) as fh:
        return json.load(fh)


def db_path():
    """Honour the same config.json ibctl uses, so both see one database."""
    try:
        with open(CONFIG) as fh:
            p = json.load(fh).get("db_path")
            if p:
                return p
    except (OSError, ValueError):
        pass
    return DEFAULT_DB


def coverage_by_series(path):
    """
    {(symbol, bar_size, what, use_rth): (first_bar, last_bar, n)} taken from
    the BARS themselves, by SQL, not from BarStore.coverage_summary().

    That is deliberate. The coverage table records the wall-clock window of
    the FETCH, not the span of the data: a request returning 28,735 bars over
    150 days logs an interval 75 seconds wide, because the engine stamps it
    with datetime.now() either side of the call (ib/run_engine.py). That is
    fine for BarStore's own purpose — not re-requesting a range it just
    requested — but it answers a different question from the one a recorder
    asks, which is "how recent is the newest bar I actually hold". Reading
    MAX(bar_dt_utc) cannot disagree with the data, because it IS the data.
    """
    if not os.path.exists(path):
        return {}
    import sqlite3
    out = {}
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        rows = con.execute(
            """SELECT symbol, bar_size, what_to_show, use_rth,
                      MIN(bar_dt_utc), MAX(bar_dt_utc), COUNT(*)
                 FROM bars
                GROUP BY symbol, bar_size, what_to_show, use_rth""").fetchall()
        con.close()
    except sqlite3.Error as exc:
        print(f"  could not read {path}: {exc}")
        return {}
    for sym, bar, what, rth, lo, hi, n in rows:
        out[(sym, bar, what, bool(rth))] = (lo, hi, n)
    return out


def as_dt(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    try:
        d = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def plan(entry, cov, now):
    """
    What to do about one series: how big a request, and whether a hole has
    already opened. `lost_days` is the span that has aged out of IB's window
    since the last recorded bar — data no request will ever return again.
    """
    bar = entry["bar_size"]
    key = (entry["symbol"].upper(), bar, entry["what"], bool(entry.get("use_rth", False)))
    retention = RETENTION_DAYS.get(bar, 30)   # alarm threshold, pessimistic
    cap = MAX_REQUEST_DAYS.get(bar, 30)
    have = cov.get(key)
    last = as_dt(have[1]) if have else None

    if last is None:
        # Nothing recorded: take the whole window IB will still serve, in as
        # many requests as that takes. Retention exceeds the per-request cap
        # for the coarser bar sizes (2 years of hourly against a 365-day
        # request limit), and a single capped request would fill half the
        # window and then look fully caught up forever after — the backfill
        # would never resume, and nobody would be told.
        chunks, remaining, end = [], BACKFILL_DAYS.get(bar, retention), now
        while remaining > 0:
            span = min(cap, remaining)
            chunks.append((span, end))
            remaining -= span
            end = end - timedelta(days=span)
        return dict(key=key, chunks=chunks, reason=f"no coverage, {len(chunks)} chunk(s)",
                    lost_days=0.0, last=None)

    behind = (now - last).total_seconds() / 86400.0
    lost = max(0.0, behind - retention)
    # Re-request a day more than the gap: bar boundaries and holidays make
    # exact-width requests fragile, and overlap is free — BarStore dedupes on
    # the primary key.
    want = min(max(behind + 1.0, 1.0), cap)
    return dict(key=key, chunks=[(want, now)],
                reason=f"{behind:.1f}d behind", lost_days=lost, last=last)


def fetch(entry, days, pace, dry, sock, end=None):
    dur = f"{max(1, int(round(days)))} D"
    # sys.executable, not the shebang. ibctl imports ib/ to write the bars it
    # gets back, and ib/models.py imports ibapi — so a shebang resolving to a
    # system python without ibapi fetches the data successfully and then dies
    # saving it, which looks like a fetch failure and is not one.
    cmd = [sys.executable, "ibctl.py", "--socket", sock,
           "historical", "fetch", entry["symbol"].upper(),
           "--bar-size", entry["bar_size"], "--duration", dur,
           "--what", entry["what"], "--type", entry.get("type", "etf")]
    if end is not None:
        cmd += ["--end", end.strftime("%Y%m%d-%H:%M:%S")]
    if not entry.get("use_rth", False):
        cmd.append("--no-rth")
    if dry:
        print(f"    would run: {' '.join(cmd)}")
        return True
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True,
                          timeout=300)
    ok = proc.returncode == 0
    tail = (proc.stdout or proc.stderr or "").strip().splitlines()
    print(f"    {'ok ' if ok else 'FAIL'} {dur:>7}  {tail[-1][:80] if tail else ''}")
    time.sleep(pace)
    return ok


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--watchlist", default=WATCHLIST)
    ap.add_argument("--status", action="store_true", help="report only, fetch nothing")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--pace", type=float, default=11.0,
                    help="seconds between requests (IB rejects repeats inside ~15s)")
    ap.add_argument("--socket", default=None,
                    help="engine control socket (default: auto-detect)")
    a = ap.parse_args()

    now = datetime.now(timezone.utc)
    sock = a.socket or engine_socket()
    path = db_path()
    entries = load_watchlist(a.watchlist)
    cov = coverage_by_series(path)

    print(f"  recorder  {now:%Y-%m-%d %H:%M} UTC   db={path}")
    print(f"  engine socket: {sock}"
          + ("" if os.path.exists(sock) else "   <-- NOT PRESENT, fetches will fail"))
    print(f"  {len(entries)} series")
    print(f"  {'symbol':<7} {'bar':<8} {'what':<7} {'rth':<4} {'last bar (UTC)':<20} "
          f"{'state':<22}")
    print("  " + "-" * 76)

    todo, at_risk = [], []
    for e in entries:
        p = plan(e, cov, now)
        last = f"{p['last']:%Y-%m-%d %H:%M}" if p["last"] else "-- none --"
        if p["lost_days"] > 0:
            state = f"LOST {p['lost_days']:.0f}d PERMANENTLY"
            at_risk.append((e, p))
        else:
            state = p["reason"]
        print(f"  {e['symbol']:<7} {e['bar_size']:<8} {e['what']:<7} "
              f"{str(bool(e.get('use_rth', False))):<4} {last:<20} {state:<22}")
        todo.append((e, p))

    if at_risk:
        print()
        print(f"  *** {len(at_risk)} series have permanent gaps. Those bars are not")
        print("  recoverable from IB at any price; the recorder was not running, or")
        print("  not running often enough, while they aged out. Everything from here")
        print("  forward is still savable, which is the only reason to keep going.")

    if a.status:
        return 1 if at_risk else 0

    print()
    failures = 0
    for e, p in todo:
        print(f"  {e['symbol']} {e['bar_size']} {e['what']}  ({p['reason']})")
        for span, end in p["chunks"]:
            # The most recent chunk needs no --end; older ones step back.
            if not fetch(e, span, a.pace, a.dry_run, sock,
                         None if end >= now - timedelta(minutes=1) else end):
                failures += 1

    print()
    print(f"  done, {failures} failed request(s). "
          f"Re-run with --status to confirm coverage advanced.")
    return 1 if (at_risk or failures) else 0


if __name__ == "__main__":
    sys.exit(main())
