#!/usr/bin/env python3
"""
functional_historical.py — exercise the historical-bar path against a REAL
IB connection: engine `historical fetch` -> ib/historical.py -> BarStore,
plus ibctl as a client. Places no orders.

The engine must be running with IB_HIST_DB pointing at a SCRATCH database;
this refuses to send anything otherwise, so a test run can never write the
research store:

    IB_HIST_DB=/tmp/fh/bars.db python3 -m ib.run_engine --port 4002 --mode dry_run
    python3 tools/functional_historical.py --env paper --db /tmp/fh/bars.db

Each check prints PASS / FAIL / INCONCLUSIVE with what was seen:

  cold_fetch      bars fetched from IB, stored, coverage recorded
  cache_hit       the same range again: zero IB requests, same bars
  stored_dates    rows written by the engine carry IB's epoch dates and
                  parse to the stored UTC time
  forex_key       USD.JPY is stored under "USD.JPY", never "USD"
  refusal         an unknown contract (error 200) returns ERROR promptly —
                  not after the 120 s timeout — and records no coverage
  no_data         a weekend range: IB's "no data" is an ANSWER, so the
                  range is covered with zero bars and not asked again
  pacing          two identical --force requests back to back; if IB
                  refuses the second (162 pacing), coverage must not grow
                  and the first request's bars must survive. If IB serves
                  both, the check is INCONCLUSIVE, not a pass
  ibctl_cli       ./ibctl.py historical fetch prints "[OK] ... from cache"
                  and exits 0, and writes nothing itself
"""

import argparse
import importlib.util
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from ib.bar_store import parse_ib_bar_dt  # noqa: E402

_spec = importlib.util.spec_from_file_location("ibctl", ROOT / "ibctl.py")
ibctl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ibctl)

RESULTS = []


def report(name, status, detail):
    RESULTS.append((name, status))
    print(f"{status:12s} {name:13s} {detail}", flush=True)


def engine_db(env):
    """IB_HIST_DB of the running engine process, read from /proc."""
    out = subprocess.run(["pgrep", "-f", "ib.run_engine"], capture_output=True, text=True).stdout
    found = []
    for pid in out.split():
        try:
            envs = Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")
            argv = [t.decode() for t in Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0") if t]
        except OSError:
            continue
        # the engine itself: a python whose argv is `-m ib.run_engine` — not a
        # shell or wrapper whose command line merely mentions it
        if not argv or not Path(argv[0]).name.startswith("python") or "ib.run_engine" not in argv:
            continue
        cmd = " ".join(argv)
        val = next((e.split(b"=", 1)[1].decode() for e in envs if e.startswith(b"IB_HIST_DB=")), None)
        found.append((pid, val, cmd))
    return found


def send(sock, cmd, timeout=300.0):
    t0 = time.time()
    r = ibctl.send_command(cmd, socket_path=sock, timeout=timeout)
    return r, time.time() - t0, r.data or {}


def rows(db, symbol):
    con = sqlite3.connect(db)
    try:
        b = con.execute("select count(*) from bars where symbol=?", (symbol,)).fetchone()[0]
        c = con.execute("select start_utc, end_utc from coverage where symbol=? order by start_utc",
                        (symbol,)).fetchall()
        sample = con.execute("select bar_dt_utc, bar_dt_orig from bars where symbol=? limit 3",
                             (symbol,)).fetchall()
        return b, c, sample
    finally:
        con.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--env", default="paper", choices=["paper", "live"])
    ap.add_argument("--socket", default=None)
    ap.add_argument("--db", required=True, help="the scratch DB the engine was started with")
    ap.add_argument("--end", default="20260925-21:00:00",
                    help="end of the cold-fetch window (UTC, a completed trading day)")
    ap.add_argument("--weekend-end", default="20260927-00:00:00",
                    help="end of a 1-day window that is a Saturday (UTC)")
    a = ap.parse_args()
    db = str(Path(a.db).resolve())
    sock = a.socket or ibctl._socket_for_env(a.env)

    engines = engine_db(a.env)
    if not engines:
        sys.exit("no running ib.run_engine process found")
    mism = [(pid, v) for pid, v, _ in engines if v is None or str(Path(v).resolve()) != db]
    if mism:
        sys.exit(f"refusing: engine IB_HIST_DB is {mism[0][1]!r}, expected {db!r}. "
                 f"Start the engine with IB_HIST_DB={db}")
    if a.env == "live":
        print("WARNING: running against the LIVE engine's connection (read-only requests)")

    base = f"--bar-size 5_mins --what MIDPOINT --type etf --end {a.end}"

    # cold fetch
    r, dt, d = send(sock, f"historical fetch GLD --duration 1_D {base}")
    n, cov, sample = rows(db, "GLD")
    ok = r.status == ibctl.CommandStatus.SUCCESS and d.get("stats", {}).get("requests", 0) >= 1 \
        and n > 0 and cov and d.get("db_path") == db
    report("cold_fetch", "PASS" if ok else "FAIL",
           f"{r.status.value} in {dt:.1f}s; stats {d.get('stats')}; rows {n}; coverage {cov}; db {d.get('db_path')}")

    # cache hit
    r, dt, d2 = send(sock, f"historical fetch GLD --duration 1_D {base}")
    ok = r.status == ibctl.CommandStatus.SUCCESS and d2.get("stats", {}).get("requests") == 0 \
        and d2.get("count") == d.get("count")
    report("cache_hit", "PASS" if ok else "FAIL",
           f"{r.status.value} in {dt:.1f}s; stats {d2.get('stats')}; count {d2.get('count')} vs {d.get('count')}")

    # stored dates
    bad = []
    for utc, orig in sample:
        got = parse_ib_bar_dt(orig).strftime("%Y-%m-%dT%H:%M:%S")
        if got != utc:
            bad.append((utc, orig, got))
    report("stored_dates", "PASS" if sample and not bad else "FAIL",
           f"samples {sample}; mismatches {bad}")

    # forex key
    r, dt, d = send(sock, f"historical fetch USD.JPY --duration 1_D --bar-size 5_mins --what MIDPOINT "
                          f"--type forex --no-rth --end {a.end}")
    n_pair, cov_pair, _ = rows(db, "USD.JPY")
    n_base, cov_base, _ = rows(db, "USD")
    ok = r.status == ibctl.CommandStatus.SUCCESS and d.get("symbol") == "USD.JPY" and n_pair > 0 \
        and n_base == 0 and not cov_base
    report("forex_key", "PASS" if ok else "FAIL",
           f"{r.status.value}; symbol {d.get('symbol')}; USD.JPY rows {n_pair}; 'USD' rows {n_base}")

    # refusal: unknown contract
    r, dt, d = send(sock, f"historical fetch ZZQXJ --duration 1_D {base}")
    n, cov, _ = rows(db, "ZZQXJ")
    ok = r.status == ibctl.CommandStatus.ERROR and dt < 60 and not cov and n == 0
    report("refusal", "PASS" if ok else "FAIL",
           f"{r.status.value} in {dt:.1f}s (old code waited 120s); {r.message[:90]}; coverage {cov}")

    # no data: a Saturday
    r, dt, d = send(sock, f"historical fetch GLD --duration 1_D --bar-size 5_mins --what MIDPOINT "
                          f"--type etf --end {a.weekend_end}")
    _, cov_after, _ = rows(db, "GLD")
    sat = datetime.strptime(a.weekend_end, "%Y%m%d-%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%S")
    covered = any(s <= sat and e >= sat for s, e in cov_after)
    r2, _, d2 = send(sock, f"historical fetch GLD --duration 1_D --bar-size 5_mins --what MIDPOINT "
                           f"--type etf --end {a.weekend_end}")
    ok = r.status == ibctl.CommandStatus.SUCCESS and d.get("count") == 0 and covered \
        and d2.get("stats", {}).get("requests") == 0
    report("no_data", "PASS" if ok else "FAIL",
           f"{r.status.value}; count {d.get('count')}; stats {d.get('stats')}; covered {covered}; "
           f"repeat requests {d2.get('stats', {}).get('requests')}")

    # pacing: identical forced requests back to back
    _, cov_before, _ = rows(db, "GLD")
    n_before = rows(db, "GLD")[0]
    ra, _, da = send(sock, f"historical fetch GLD --duration 1_D {base} --force")
    rb, dtb, db_ = send(sock, f"historical fetch GLD --duration 1_D {base} --force")
    n_after, cov_after, _ = rows(db, "GLD")
    if rb.status == ibctl.CommandStatus.ERROR:
        ok = cov_after == cov_before and n_after >= n_before
        report("pacing", "PASS" if ok else "FAIL",
               f"second refused ({rb.message[:80]}); coverage unchanged {cov_after == cov_before}; "
               f"rows {n_before} -> {n_after}")
    else:
        report("pacing", "INCONCLUSIVE",
               f"IB served both identical requests ({da.get('stats')}, {db_.get('stats')}); "
               f"no pacing refusal was provoked")

    # ibctl CLI
    before = Path(db).stat().st_mtime
    p = subprocess.run([sys.executable, str(ROOT / "ibctl.py"), "--socket", sock, "historical", "fetch",
                        "GLD", "--bar-size", "5 mins", "--duration", "1 D", "--what", "MIDPOINT",
                        "--type", "etf", "--end", a.end], capture_output=True, text=True, cwd=ROOT)
    first = (p.stdout.strip().splitlines() or [""])[0]
    ok = p.returncode == 0 and first.startswith("[OK]") and "from cache" in first
    report("ibctl_cli", "PASS" if ok else "FAIL", f"exit {p.returncode}; {first[:100]}")

    fails = [n for n, s in RESULTS if s == "FAIL"]
    print(f"\n{len(RESULTS)} checks: {sum(s == 'PASS' for _, s in RESULTS)} pass, {len(fails)} fail, "
          f"{sum(s == 'INCONCLUSIVE' for _, s in RESULTS)} inconclusive")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
