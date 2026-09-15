#!/usr/bin/env python3
"""
spot_scan.py — the USD-side test on real spot FX, with measured spot costs.

fx_scan.py answered the direction question with currency ETFs, whose daily
open/close conveniently ARE the 09:30-16:00 window. It could not answer the
economics: an ETF wrapper quotes several bp wide, the same order as the edge,
so the sign of the net result hung entirely on which execution model applied.

Spot quotes an order of magnitude tighter. This re-runs the identical test on
EUR.USD and USD.JPY, measuring the spread at the two instants that matter
rather than estimating it, and charges IB's published forex commission.

FINDINGS (2024-09-03 .. 2026-09-15, 338-341 no-hold sessions)

    09:30 -> 16:00 ET      gross     t      p   hit%   spread    net
    EUR.USD                 1.99  1.32  0.188   56.9     0.18   1.81
    USD.JPY                 4.14  2.38  0.017   59.8     0.30   3.84

USD.JPY survives paying the FULL spread both ways — no auction argument
needed, which is just as well since spot has no closing auction. The ETF
proxy said -2.06 bp for the same trade; the instrument was the problem, not
the signal. EUR.USD stays weak on both.

The longer window is better for both pairs: holding to 16:00 rather than
15:50 adds 0.3-0.4 bp, so the last ten minutes contribute rather than give
back. 16:00 ET also clears the 17:00 New York rollover, when spreads widen
3-5x — the measurements confirm no widening signature at the 16:00 mark.

COST. IB forex is 0.20 bp of trade value with a $2.00 per-order minimum, and
the MINIMUM is what decides viability, not the rate: the trade is uneconomic
below ~$10.4k notional, break-even there, and only reaches the flat 0.40 bp
round-trip floor at $100k. That makes the edge an increasing function of
size — the opposite of the ETF, where cost was size-independent and the
trade never cleared its spread.

CAVEATS. t=2.38 does not survive Bonferroni across the six currency legs
already searched. Two years, one pair, one regime — USDJPY spent much of
this sample in a persistent carry environment. Cost precision does not fix
either of those.

Spot needs intraday bars: a spot FX daily bar runs 17:00->17:00 ET, so its
open and close are the wrong two prices. Mid is derived from BID and ASK
rather than fetching MIDPOINT separately, so the spread comes free with the
prices and is measured at the trade instant rather than as a daily average.

Populate the series first (5-min bars chunk at 30 days, so this is ~25 IB
requests per series; run it outside market hours):

    ./ibctl.py --env live historical fetch USD.JPY --type forex --what BID \
        --no-rth --bar-size "5 mins" --duration "2 Y"
    (repeat for --what ASK, and for EUR.USD)

    python3 spot_scan.py --db PATH --days no_hold_days.json
"""
import argparse
import json
import math
import sqlite3
import statistics as st
from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
PAIRS = [("EUR.USD", -1), ("USD.JPY", +1)]   # sign: + means the USD side won
MARKS = {(9, 30), (15, 50), (16, 0)}
RATE_BP, MIN_USD = 0.20, 2.00                # IB forex commission, per order


def load_marks(db, symbol):
    """{date: {'0930BID': px, ...}} — the open of each bar we care about.

    The bar's OPEN is the price at the labelled instant; its close belongs to
    the next mark. Using open at both ends keeps entry and exit symmetric.
    """
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    out = defaultdict(dict)
    for what in ("BID", "ASK"):
        for dt_utc, o in con.execute(
                "select bar_dt_utc, open from bars where symbol=? and"
                " bar_size='5 mins' and what_to_show=? order by bar_dt_utc",
                (symbol, what)):
            t = datetime.fromisoformat(dt_utc.replace("Z", "+00:00"))
            t = (t if t.tzinfo else t.replace(tzinfo=timezone.utc)).astimezone(ET)
            if (t.hour, t.minute) in MARKS:
                out[str(t.date())][f"{t.hour:02d}{t.minute:02d}{what}"] = o
    return out


def mid_spread(day, hhmm):
    b, a = day.get(f"{hhmm}BID"), day.get(f"{hhmm}ASK")
    if not b or not a:
        return None, None
    m = (a + b) / 2
    return m, (a - b) / m * 1e4


def stats(xs):
    n = len(xs)
    if n < 3:
        return n, float("nan"), float("nan"), float("nan"), float("nan")
    m, sd = st.mean(xs), st.stdev(xs)
    t = m / (sd / math.sqrt(n))
    return n, m, t, math.erfc(abs(t) / math.sqrt(2)), 100 * sum(1 for x in xs if x > 0) / n


def commission_bp(notional):
    """Per side, in bp of notional. The minimum dominates at small size."""
    return max(MIN_USD, notional * RATE_BP / 1e4) / notional * 1e4


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--days", required=True)
    ap.add_argument("--notional", type=float, default=100_000.0)
    a = ap.parse_args()

    d = json.load(open(a.days))
    no_hold = d["no_hold_days"]
    bars = {sym: load_marks(a.db, sym) for sym, _ in PAIRS}

    print(f"window {d['span'][0]} .. {d['span'][1]}   no-hold sessions {len(no_hold)}")
    print("positive = the USD side won\n")

    results = {}
    for exit_t, label in (("1550", "09:30 -> 15:50 ET"), ("1600", "09:30 -> 16:00 ET")):
        print(f"=== {label} ===")
        print(f"{'pair':9} {'n':>4} {'gross':>7} {'t':>6} {'p':>7} {'hit%':>6} "
              f"| {'spread: mean':>12} {'med':>5} {'%zero':>6} | {'net of spread':>13}")
        print("-" * 86)
        for sym, sign in PAIRS:
            rs, sp = [], []
            for day_s in sorted(no_hold):
                day = bars[sym].get(day_s)
                if not day:
                    continue
                m0, s0 = mid_spread(day, "0930")
                m1, s1 = mid_spread(day, exit_t)
                if m0 is None or m1 is None:
                    continue
                rs.append(sign * (m1 - m0) / m0 * 1e4)
                sp += [s0, s1]
            if not rs:
                print(f"{sym:9}   no cached BID/ASK bars — see the docstring")
                continue
            n, m, t, p, hit = stats(rs)
            # The MEAN spread, not the median: EUR.USD's tick is ~0.43 bp and
            # bid often equals ask at the mark, so its median reads 0.00 while
            # real cost is paid on the rest.
            cross = st.mean(sp)
            zero = 100 * sum(1 for x in sp if x == 0) / len(sp)
            results[(sym, exit_t)] = (m, cross)
            print(f"{sym:9} {n:>4} {m:>7.2f} {t:>6.2f} {p:>7.3f} {hit:>6.1f} "
                  f"| {cross:>12.2f} {st.median(sp):>5.2f} {zero:>5.1f}% "
                  f"| {m - cross:>13.2f}")
        print()

    key = ("USD.JPY", "1600")
    if key not in results:
        return
    gross, spread = results[key]
    print(f"=== USD.JPY 09:30->16:00 net of IB forex commission "
          f"({RATE_BP} bp of value, ${MIN_USD:.2f} minimum) ===")
    print(f"{'notional':>12} {'comm/side':>10} {'comm RT':>8} {'net bp':>8}  {'min binds':>9}")
    print("-" * 54)
    for n in (10_000, 20_000, 40_000, 60_000, 100_000, 250_000, 1_000_000):
        c = commission_bp(n)
        binds = "yes" if n * RATE_BP / 1e4 < MIN_USD else "no"
        print(f"${n:>11,} {c:>10.2f} {2*c:>8.2f} {gross - spread - 2*c:>8.2f}  {binds:>9}")
    be = 2 * MIN_USD * 1e4 / (gross - spread)
    print(f"\nbreak-even notional      : ${be:,.0f}")
    print(f"minimum stops binding at : ${MIN_USD * 1e4 / RATE_BP:,.0f}")
    print(f"floor cost above that    : {2*RATE_BP:.2f} bp round trip")


if __name__ == "__main__":
    main()
