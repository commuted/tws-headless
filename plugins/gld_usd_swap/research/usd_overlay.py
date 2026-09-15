#!/usr/bin/env python3
"""
usd_overlay.py — does a long-USD overlay on no-hold days add risk or diversify?

The standalone volatility of an overlay is not the question; what it does to
the combined stream is. This builds both series and sizes the overlay against
the strategy.

  strategy  intraday held only on HOLD days, overnight held every night
            (reconstructed from cached 5-min GLD bars)
  overlay   long USD (UUP) intraday on NO-HOLD days only — the idle window

First run (506 sessions, 2024-08-23 .. 2026-09-11): strategy Sharpe 1.45 —
which matches the plugin's own documented 1.42-1.44 backtest, so the
reconstruction is sound — overlay Sharpe 0.73 net, and correlation +0.018.
Near-zero correlation is the whole finding: the strategy's no-hold-day return
is the OVERNIGHT gold move while the overlay is the INTRADAY dollar move, so
despite both sitting on a USD axis they barely overlap. Combined Sharpe peaks
around 3x overlay notional at 1.61, then decays as cost and vol take over.

It does not lower absolute risk (sd rises with size); it lowers risk per unit
of return, which is a different claim and the one the numbers support.

    python3 usd_overlay.py [--db PATH] [--days no_hold_days.json] [--overlay UUP]
"""
import argparse
import math
import sqlite3
import statistics as st
from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import _data

ET = ZoneInfo("America/New_York")


def gld_sessions(db):
    """{date: (open, close)} for GLD's RTH session, from 5-min bars."""
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    sess = defaultdict(dict)
    for dt_utc, o, c in con.execute(
            """select bar_dt_utc, open, close from bars
               where symbol='GLD' and bar_size='5 mins' and use_rth=1
               order by bar_dt_utc"""):
        t = datetime.fromisoformat(dt_utc.replace("Z", "+00:00"))
        t = (t if t.tzinfo else t.replace(tzinfo=timezone.utc)).astimezone(ET)
        d = str(t.date())
        if (t.hour, t.minute) == (9, 30):
            sess[d]["open"] = o
        sess[d]["close"] = c            # last bar of the session wins
    return {d: (v["open"], v["close"]) for d, v in sess.items() if "open" in v}


def sharpe(xs):
    return st.mean(xs) / st.stdev(xs) * math.sqrt(252)


def corr(a, b):
    ma, mb = st.mean(a), st.mean(b)
    da = math.sqrt(sum((x - ma) ** 2 for x in a))
    db = math.sqrt(sum((y - mb) ** 2 for y in b))
    if not da or not db:
        return float("nan")
    return sum((x - ma) * (y - mb) for x, y in zip(a, b)) / (da * db)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(_data.DEFAULT_DB))
    ap.add_argument("--days", default=str(_data.DEFAULT_DAYS))
    ap.add_argument("--overlay", default="UUP")
    ap.add_argument("--commission", type=float, default=0.72, help="bp per side")
    a = ap.parse_args()

    no_hold, hold, span = _data.sessions(a.days)
    no_hold, hold = set(no_hold), set(hold)
    gld = gld_sessions(a.db)
    fx = _data.daily(a.db, [a.overlay])
    if a.overlay not in fx:
        raise SystemExit(f"no daily bars for {a.overlay} — see _data.py")

    days = sorted(gld)
    intraday = {d: (gld[d][1] - gld[d][0]) / gld[d][0] * 1e4 for d in days}
    overnight = {d: (gld[days[i + 1]][0] - gld[d][1]) / gld[d][1] * 1e4
                 for i, d in enumerate(days) if i + 1 < len(days)}

    cost = 2 * a.commission
    strat, ovl = [], []
    for d in days:
        if (d not in no_hold and d not in hold) or d not in overnight:
            continue
        b = fx[a.overlay].get(d)
        if not b or not b[0]:
            continue
        strat.append(overnight[d] + (intraday[d] if d in hold else 0.0))
        ovl.append(((b[1] - b[0]) / b[0] * 1e4 - cost) if d in no_hold else 0.0)

    print(f"window {span[0]} .. {span[1]}   sessions {len(strat)}   "
          f"overlay active on {sum(1 for o in ovl if o)}\n")
    print(f"{'strategy alone (GLD)':38} mean {st.mean(strat):>7.2f} bp  "
          f"sd {st.stdev(strat):>7.2f} bp  Sharpe {sharpe(strat):>5.2f}")
    print(f"{'overlay net of ' + f'{cost:.2f}' + ' bp':38} mean {st.mean(ovl):>7.2f} bp  "
          f"sd {st.stdev(ovl):>7.2f} bp  Sharpe {sharpe(ovl):>5.2f}")
    print(f"\ncorrelation(strategy, overlay) = {corr(strat, ovl):+.3f}\n")

    base = sharpe(strat)
    print(f"{'overlay size':>14} {'comb mean':>10} {'comb sd':>9} {'Sharpe':>8} {'vs base':>9}")
    print("-" * 56)
    for k in (0, 0.5, 1, 2, 3, 5, 8):
        comb = [s + k * o for s, o in zip(strat, ovl)]
        print(f"{k:>13.1f}x {st.mean(comb):>10.2f} {st.stdev(comb):>9.2f} "
              f"{sharpe(comb):>8.2f} {sharpe(comb) - base:>+9.2f}")
    print(f"\n(x = {a.overlay} notional per 1.0 of GLD notional)")


if __name__ == "__main__":
    main()
