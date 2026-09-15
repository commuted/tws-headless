#!/usr/bin/env python3
"""
long_history.py — does the USD effect survive on a much longer sample?

The live regime is a 5-minute SMA crossover, so no_hold_days.py can only
reach as far back as 5-minute bars are cached — about two years, 347 no-hold
sessions. On that sample the USD effect looked real in direction (8 of 8 legs
positive) but a Westfall-Young permutation test over the six currency legs
put the best leg at adjusted p = 0.28. The honest reading was that the sample
was too small to separate a 4 bp effect from noise.

Daily bars reach much further, so this rebuilds an ANALOGOUS regime from
daily closes and re-runs the test with several times the data.

WHAT THIS IS AND IS NOT. A daily crossover is not the plugin's signal. A
result here says "USD momentum predicts next-session USD direction", which is
the underlying claim; it does not validate the live 5-minute regime. Treat it
as evidence about the idea, not about the plugin.

HORIZON is set by the shortest input, and on IB that is TLT, which stops at
2016-02-03 — verified by purging its cache and re-fetching with zero
failures, so it is an IB limit rather than a gap in the store. UUP reaches
its 2007 inception and so do the currency ETFs, hence two regimes worth
running:

    UUP + TLT   the live default          from 2016-02   ~2,400 sessions
    UUP only    the plugin's fallback     from 2007-02   ~4,700 sessions

The overlay instruments are currency ETFs, not spot: they trade the US equity
session, so a daily open->close IS the 09:30-16:00 window the strategy sits
out. Spot cannot be used here — its daily bar runs 17:00->17:00 ET. Costs
were already measured on spot in spot_scan.py and are not what is in doubt.

    python3 long_history.py --db PATH [--regime uup_tlt|uup] [--fast 5 --slow 20]
"""
import argparse
import math
import random
import sqlite3
import statistics as st
from collections import defaultdict

LEGS = [("EUR", "FXE", -1), ("JPY", "FXY", -1), ("GBP", "FXB", -1),
        ("CHF", "FXF", -1), ("AUD", "FXA", -1), ("CAD", "FXC", -1),
        ("USD basket", "UUP", +1)]


def daily(db, symbols):
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    marks = ",".join("?" * len(symbols))
    out = defaultdict(dict)
    for sym, d, o, c in con.execute(
            f"""select symbol, date(bar_dt_utc), open, close from bars
                where bar_size='1 day' and what_to_show='TRADES'
                  and symbol in ({marks}) order by date(bar_dt_utc)""", symbols):
        out[sym][d] = (o, c)
    return out


def sma_regime(px, regime, fast, slow):
    """Daily analogue of _recompute_regime: gold when the dollar is weakening
    and (for uup_tlt) rates are falling. Returns {date: 'gold'|'cash'}."""
    need = ["UUP"] + (["TLT"] if regime == "uup_tlt" else [])
    days = sorted(set.intersection(*(set(px[s]) for s in need)))
    closes = {s: [px[s][d][1] for d in days] for s in need}
    out = {}
    for i in range(slow - 1, len(days)):
        votes = []
        for s in need:
            f = sum(closes[s][i - fast + 1:i + 1]) / fast
            sl = sum(closes[s][i - slow + 1:i + 1]) / slow
            # UUP falling => dollar weakening => gold. TLT rising => rates
            # falling => gold. Same senses as the live plugin.
            votes.append(f < sl if s == "UUP" else f > sl)
        out[days[i]] = "gold" if all(votes) else "cash"
    return out, days


def stats(xs):
    n = len(xs)
    m, sd = st.mean(xs), st.stdev(xs)
    t = m / (sd / math.sqrt(n))
    return n, m, t, math.erfc(abs(t) / math.sqrt(2)), 100 * sum(1 for x in xs if x > 0) / n


def westfall_young(series, idx_sel, n_perm, seed=20260915):
    """Adjusted p for the best leg, permuting the no-hold label. Uses the
    actual dependence between legs rather than assuming independence, which
    matters: these legs correlate ~0.57 by construction."""
    rnd = random.Random(seed)
    names = list(series)
    N = len(next(iter(series.values())))
    obs = {n: stats([series[n][i] for i in idx_sel])[2] for n in names}
    obs_max = max(obs.values())
    k, pool, ge = len(idx_sel), list(range(N)), 0
    for _ in range(n_perm):
        sel = rnd.sample(pool, k)
        if max(stats([series[n][i] for i in sel])[2] for n in names) >= obs_max:
            ge += 1
    return obs, obs_max, (ge + 1) / (n_perm + 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--regime", choices=["uup_tlt", "uup"], default="uup_tlt")
    ap.add_argument("--fast", type=int, default=5)
    ap.add_argument("--slow", type=int, default=20)
    ap.add_argument("--perm", type=int, default=20000)
    a = ap.parse_args()

    px = daily(a.db, [s for _, s, _ in LEGS] + ["TLT", "GLD"])
    reg, _ = sma_regime(px, a.regime, a.fast, a.slow)
    decided = sorted(reg)

    # regime at D's close decides D+1; D+1 is a no-hold session when it is cash
    no_hold, hold = [], []
    for prior, nxt in zip(decided, decided[1:]):
        (no_hold if reg[prior] == "cash" else hold).append(nxt)

    print(f"regime {a.regime} ({a.fast}/{a.slow} daily SMA)   "
          f"{decided[0]} .. {decided[-1]}")
    print(f"  no-hold {len(no_hold)}  hold {len(hold)}  "
          f"({100*len(no_hold)/(len(no_hold)+len(hold)):.1f}% no-hold)\n")

    avail = [(n, s, sg) for n, s, sg in LEGS if s in px]
    sessions = [d for d in no_hold if all(d in px[s] for _, s, _ in avail)]
    allday = [d for d in no_hold + hold if all(d in px[s] for _, s, _ in avail)]
    allday.sort()
    if len(sessions) < 30:
        raise SystemExit("too few overlapping sessions — extend the daily fetches")

    print(f"{'leg':12} {'n':>6} {'mean bp':>8} {'t':>6} {'p':>7} {'hit%':>6}")
    print("-" * 50)
    series = {}
    for name, sym, sign in avail:
        series[name] = [sign * (px[sym][d][1] - px[sym][d][0]) / px[sym][d][0] * 1e4
                        for d in allday]
        xs = [sign * (px[sym][d][1] - px[sym][d][0]) / px[sym][d][0] * 1e4
              for d in sessions]
        n, m, t, p, hit = stats(xs)
        print(f"{name:12} {n:>6} {m:>8.2f} {t:>6.2f} {p:>7.3f} {hit:>6.1f}")

    nh = set(no_hold)
    idx = [i for i, d in enumerate(allday) if d in nh]
    obs, obs_max, p_wy = westfall_young(series, idx, a.perm)
    print(f"\nWestfall-Young over {len(series)} legs, {a.perm:,} permutations")
    print(f"  best observed t = {obs_max:.2f} ({max(obs, key=obs.get)})")
    print(f"  adjusted p      = {p_wy:.4f}  -> "
          f"{'SIGNIFICANT' if p_wy < 0.05 else 'not significant'} at 0.05")

    # era stability: a real effect should not live in one regime
    print("\nby era (best leg):")
    best = max(obs, key=obs.get)
    sym, sign = next((s, sg) for n, s, sg in avail if n == best)
    by = defaultdict(list)
    for d in sessions:
        by[d[:4]].append(sign * (px[sym][d][1] - px[sym][d][0]) / px[sym][d][0] * 1e4)
    for y in sorted(by):
        if len(by[y]) >= 20:
            n, m, t, p, hit = stats(by[y])
            print(f"  {y}  n={n:>3}  mean {m:>6.2f} bp  t {t:>5.2f}  hit {hit:>5.1f}%")


if __name__ == "__main__":
    main()
