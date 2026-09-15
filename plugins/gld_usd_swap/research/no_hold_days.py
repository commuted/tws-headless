#!/usr/bin/env python3
"""
no_hold_days.py — list the sessions gld_usd_swap sits out intraday.

The plugin is long GLD overnight every night. On a session whose PRIOR close
carried a CASH composite regime it sells at the open, sits in cash through the
day, and re-enters via MOC at 15:50. Those sessions are the "no-hold days":
the window where the strategy's capital is idle and an overlay could run.

This replays cached 5-min RTH bars through the plugin's OWN _InstrumentState
and _recompute_regime() rather than reimplementing the signal, so the list
cannot drift from live behaviour. It reads the BarStore directly — no engine,
no IB traffic.

HORIZON. The signal is built from 5-minute SMAs, so the list can only reach as
far back as 5-min bars are cached (IB does not serve them indefinitely). Daily
bars go back decades but cannot reproduce an intraday crossover. Whatever the
cache holds IS the longest achievable history for this question.

Requires in the BarStore: UUP, TLT, RINF, GLD @ "5 mins", use_rth=1.

    python3 no_hold_days.py --db ../../../historical/bars.db --out no_hold_days.json
"""
import argparse, json, os, sqlite3, sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
CLOSE_HH, CLOSE_MM = 15, 45      # the bar whose regime decides the next open


def _plugin():
    """Import the live plugin. It imports as `ib.*`, but the project directory
    is named tws-headless, so register the alias the same way run_tests.sh does."""
    root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(root))
    import importlib.util
    if "ib" not in sys.modules:
        spec = importlib.util.spec_from_file_location(
            "ib", root / "__init__.py",
            submodule_search_locations=[str(root)])
        mod = importlib.util.module_from_spec(spec)
        sys.modules["ib"] = mod
        spec.loader.exec_module(mod)
    from plugins.gld_usd_swap.plugin import GldUsdSwapPlugin, REGIME_CASH, REGIME_GOLD
    return GldUsdSwapPlugin, REGIME_CASH, REGIME_GOLD


def load_bars(db):
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    rows = con.execute("""
        select symbol, bar_dt_utc, close from bars
        where bar_size='5 mins' and use_rth=1
          and symbol in ('UUP','TLT','RINF','GLD')
        order by bar_dt_utc""").fetchall()
    by_ts = defaultdict(dict)
    for sym, dt_utc, close in rows:
        t = datetime.fromisoformat(dt_utc.replace("Z", "+00:00"))
        t = (t if t.tzinfo else t.replace(tzinfo=timezone.utc)).astimezone(ET)
        by_ts[t][sym] = close
    if not by_ts:
        raise SystemExit(f"no 5-min RTH bars for UUP/TLT/RINF/GLD in {db}")
    return by_ts


def classify(by_ts, tmp_state):
    GldUsdSwapPlugin, REGIME_CASH, REGIME_GOLD = _plugin()
    p = GldUsdSwapPlugin(base_path=tmp_state)
    close_regime = {}
    for t in sorted(by_ts):
        px = by_ts[t]
        for sym, state in (("UUP", p._uup), ("TLT", p._tlt), ("RINF", p._rinf)):
            if sym in px:
                state.push(px[sym], p.vol_window, p.derivative_percentile,
                           p.fast_bars, p.slow_bars)
        if "GLD" in px:
            p._gld_price = px["GLD"]
            p._push_gld_meta(px["GLD"])
        p._recompute_regime()
        if (t.hour, t.minute) == (CLOSE_HH, CLOSE_MM):
            close_regime[t.date()] = p._regime

    decided = sorted(close_regime)
    no_hold, hold = [], []
    for prior, nxt in zip(decided, decided[1:]):
        if close_regime[prior] == REGIME_CASH:
            no_hold.append(nxt)
        elif close_regime[prior] == REGIME_GOLD:
            hold.append(nxt)
    return decided, no_hold, hold


def main():
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(here.parents[2] / "historical" / "bars.db"))
    ap.add_argument("--out", default=str(here / "no_hold_days.json"))
    ap.add_argument("--state-dir", default="/tmp/_no_hold_replay",
                    help="scratch dir for the replay plugin's state (never live state)")
    a = ap.parse_args()

    by_ts = load_bars(a.db)
    decided, no_hold, hold = classify(by_ts, a.state_dir)
    n = len(no_hold) + len(hold)
    print(f"sessions with a {CLOSE_HH}:{CLOSE_MM} close bar : {len(decided)}  "
          f"({decided[0]} .. {decided[-1]})")
    print(f"  NO-HOLD (sits out intraday)   : {len(no_hold)}  ({100*len(no_hold)/n:.1f}%)")
    print(f"  HOLD    (stays long all day)  : {len(hold)}")

    json.dump({"source": os.path.abspath(a.db),
               "span": [str(decided[0]), str(decided[-1])],
               "no_hold_days": [str(d) for d in no_hold],
               "hold_days": [str(d) for d in hold]},
              open(a.out, "w"), indent=1)
    print(f"written: {a.out}")


if __name__ == "__main__":
    main()
