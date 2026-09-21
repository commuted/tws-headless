#!/usr/bin/env python3
"""
opex_weekend_long.py — is the third-Friday weekend hold real, over 22 years?

overnight_by_weekday.py found the third Friday of the month (monthly option
expiration) worth +71.74 bp on 22 nights with an 81.8% hit rate, against
+3.42 bp on other Fridays. Two years is 22 expirations, and a Westfall-Young
permutation over the five position buckets put the adjusted p at 0.12 — so
that result was suggestive and nothing more.

This asks the same question of GLD's whole history. The question needs only a
daily open and close, so it is not limited by IB's 5-min horizon: the volomom
Yahoo cache holds GLD daily from its 2004 launch, about 260 expirations.

    python3 opex_weekend_long.py

ADJUSTED PRICES. The cache is auto_adjust=true. GLD makes no distributions
and has never split, so adjustment should be a no-op, but an overnight return
spans two rows and a factor change between them would bias it. --raw reruns
against the unadjusted file; the two should agree, and the script says so.

WHAT WOULD MAKE THIS ACTIONABLE. An edge has to clear the round trip it
implies: skipping a night means selling into that close and buying back at
the next open, roughly 2 bp on a $20k slice once commission and spread are
paid. An edge also has to be stable, which is why the per-era table matters
more than the headline.
"""
import argparse
import csv
import math
import random
import statistics as st
from collections import defaultdict
from datetime import date
from pathlib import Path

CACHE = Path("/home/ron/claude/volomom/.pricecache")


def load(path):
    """{date: (open, close)} from a volomom price-cache CSV."""
    out = {}
    with open(path) as fh:
        for row in csv.DictReader(fh):
            try:
                d = date.fromisoformat(row["Date"][:10])
                o, c = float(row["Open"]), float(row["Close"])
            except (ValueError, KeyError, TypeError):
                continue
            if o > 0 and c > 0:
                out[d] = (o, c)
    return dict(sorted(out.items()))


def overnights(sess):
    days = sorted(sess)
    return [(a, a.weekday(), (b - a).days,
             (sess[b][0] - sess[a][1]) / sess[a][1] * 10_000)
            for a, b in zip(days, days[1:])]


def nth_friday(d):
    """Which Friday of its month `d` is (1-5)."""
    return sum(1 for x in range(1, d.day + 1)
               if date(d.year, d.month, x).weekday() == 4)


def stats(xs):
    n = len(xs)
    if n < 3:
        return n, float("nan"), float("nan"), float("nan"), float("nan")
    m, sd = st.mean(xs), st.stdev(xs)
    if sd == 0:
        return n, m, float("nan"), float("nan"), float("nan")
    t = m / (sd / math.sqrt(n))
    return n, m, t, math.erfc(abs(t) / math.sqrt(2)), \
        100 * sum(1 for x in xs if x > 0) / n


def report(sess, label):
    nights = overnights(sess)
    fri = [(d, r) for d, w, _, r in nights if w == 4]
    allr = [r for *_, r in nights]
    n, m, t, p, hit = stats(allr)
    print(f"\n{label}: {min(sess)} .. {max(sess)}  "
          f"({len(sess)} sessions, {len(nights)} nights)")
    print(f"  ALL NIGHTS  n={n:5} mean {m:+7.2f} bp  t={t:+5.2f} p={p:.4f} "
          f"hit {hit:.1f}%")

    bypos = defaultdict(list)
    for d, r in fri:
        bypos[nth_friday(d)].append(r)
    print(f"\n  {'which':8} {'n':>5} {'mean bp':>9} {'median':>8} {'hit%':>6} "
          f"{'t':>6} {'p':>8}")
    print("  " + "-" * 54)
    for pos in sorted(bypos):
        nn, mm, tt, pp, hh = stats(bypos[pos])
        suf = {1: "st", 2: "nd", 3: "rd"}.get(pos, "th")
        print(f"  {str(pos)+suf:8} {nn:5} {mm:+9.2f} "
              f"{st.median(bypos[pos]):+8.2f} {hh:6.1f} {tt:+6.2f} {pp:8.4f}")

    third = bypos.get(3, [])
    other = [r for pos, xs in bypos.items() if pos != 3 for r in xs]
    if len(third) < 3 or len(other) < 3:
        return
    n3, m3, _, p3, h3 = stats(third)
    no, mo, _, po, ho = stats(other)
    tw = (m3 - mo) / math.sqrt(st.variance(third) / n3 + st.variance(other) / no)
    pw = math.erfc(abs(tw) / math.sqrt(2))
    print(f"\n  3rd Friday    n={n3:4} mean {m3:+7.2f} bp  hit {h3:5.1f}%")
    print(f"  other Fridays n={no:4} mean {mo:+7.2f} bp  hit {ho:5.1f}%")
    print(f"  difference         {m3 - mo:+7.2f} bp  Welch t={tw:+.2f} p={pw:.4f}")

    # Westfall-Young over the five buckets: could the best of five be this big?
    random.seed(11)
    labels = [nth_friday(d) for d, _ in fri]
    vals = [r for _, r in fri]
    obs, N, hits = m3, 20000, 0
    for _ in range(N):
        random.shuffle(labels)
        b = defaultdict(list)
        for L, v in zip(labels, vals):
            b[L].append(v)
        if max(st.mean(v) for v in b.values() if len(v) >= 5) >= obs:
            hits += 1
    print(f"  Westfall-Young (max of 5 buckets, {N} perms): adjusted p={hits/N:.4f}")

    # Witching: the quarterly third Fridays (Mar/Jun/Sep/Dec), when index
    # futures and index options expire alongside stock options. "Triple" and
    # "quadruple" witching name the SAME four dates — the fourth leg was
    # single-stock futures, delisted in the US around 2020, so the label
    # changed and the calendar did not.
    #
    # CAUTION: this is a subgroup of a parent effect that did not survive its
    # own correction. Splitting a null result until something looks big is how
    # false positives are manufactured, so the permutation below shuffles
    # WHICH third Fridays are called witching and re-takes the difference.
    WITCH = {3, 6, 9, 12}
    witch = [r for d, r in fri if nth_friday(d) == 3 and d.month in WITCH]
    plain = [r for d, r in fri if nth_friday(d) == 3 and d.month not in WITCH]
    if len(witch) >= 5 and len(plain) >= 5:
        nw, mw, tw_, pw_, hw = stats(witch)
        np_, mp, tp_, pp_, hp = stats(plain)
        print(f"\n  WITCHING (quarterly 3rd Fridays, Mar/Jun/Sep/Dec)")
        print(f"  {'bucket':16} {'n':>4} {'mean bp':>9} {'median':>8} {'hit%':>6} {'p':>8}")
        print("  " + "-" * 56)
        print(f"  {'witching':16} {nw:4} {mw:+9.2f} {st.median(witch):+8.2f} {hw:6.1f} {pw_:8.4f}")
        print(f"  {'other 3rd Fri':16} {np_:4} {mp:+9.2f} {st.median(plain):+8.2f} {hp:6.1f} {pp_:8.4f}")
        diff = mw - mp
        tt = diff / math.sqrt(st.variance(witch)/nw + st.variance(plain)/np_)
        print(f"  {'difference':16} {'':4} {diff:+9.2f} {'':8} {'':6} "
              f"{math.erfc(abs(tt)/math.sqrt(2)):8.4f}")
        random.seed(13)
        pool = witch + plain
        hits, N = 0, 20000
        for _ in range(N):
            random.shuffle(pool)
            if st.mean(pool[:nw]) - st.mean(pool[nw:]) >= diff:
                hits += 1
        print(f"  permutation (is 'witching' a special label?): p={hits/N:.4f}")

    # Stability: the headline is worthless if it lives in one era.
    print(f"\n  {'era':11} {'n':>4} {'mean bp':>9} {'hit%':>6} {'total bp':>10}")
    print("  " + "-" * 44)
    for lo in range(2004, 2027, 4):
        hi = lo + 4
        xs = [r for d, r in fri if nth_friday(d) == 3 and lo <= d.year < hi]
        if len(xs) >= 3:
            nn, mm, _, _, hh = stats(xs)
            print(f"  {lo}-{hi-1:<6} {nn:4} {mm:+9.2f} {hh:6.1f} {sum(xs):+10.0f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="GLD")
    ap.add_argument("--raw", action="store_true",
                    help="use the unadjusted cache file")
    args = ap.parse_args()
    name = f"{args.symbol}@1d{'.raw' if args.raw else ''}.csv"
    path = CACHE / name
    if not path.exists():
        print(f"no cache at {path}")
        return
    report(load(path), f"{args.symbol} daily ({'raw' if args.raw else 'adjusted'})")


if __name__ == "__main__":
    main()
