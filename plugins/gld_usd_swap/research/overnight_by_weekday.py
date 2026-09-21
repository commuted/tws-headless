#!/usr/bin/env python3
"""
overnight_by_weekday.py — is any weekday's overnight hold worth skipping?

The plugin is long GLD overnight EVERY night (see no_hold_days.py): the
composite regime decides whether it sits out the following intraday session,
never whether it holds overnight. So the overnight leg is unconditional, and
asking "what would omitting night X cost?" needs no regime replay at all —
only the close-to-next-open return, bucketed by the weekday the night STARTS.

    Friday  = the weekend hold (Fri close -> Mon open)
    Monday  = Mon close -> Tue open
    ...

WHAT THIS MEASURES, AND WHAT IT DOES NOT

It measures the gross overnight return of each bucket. It does NOT model the
cost of acting on the result. Skipping a night is not free: the strategy would
have to sell into that close and buy back at the next open, a round trip it
would otherwise not make. On a ~$20k slice that is ~$2 of commission (~1 bp)
plus whatever the spread costs, so an edge smaller than about 2 bp is not
actionable even if it is real.

PRICE PROXY. Only 5-min RTH bars are cached, so the session open is the first
5-min bar's open (09:30) and the session close is the last bar's close
(15:55). The strategy actually trades MOO and MOC, which execute at the
auction prints. Those are close to, but not identical to, these proxies. The
bias should be small and roughly symmetric across weekdays, so it distorts
levels more than it distorts the comparison between buckets.

MULTIPLE COMPARISONS. Five weekdays is five tests. Raw p-values are reported
beside Holm-Bonferroni adjusted ones; read the adjusted column.

    python3 overnight_by_weekday.py --db ../../../historical/bars.db
"""
import argparse
import math
import sqlite3
import statistics as st
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri"]


def sessions(db, symbol="GLD"):
    """{date: (session_open, session_close)} from cached 5-min RTH bars.

    bar_dt_utc is UTC; the ET session date is what matters for a weekday
    bucket, so convert before grouping or a 5-min bar after 20:00 UTC lands
    on the wrong day.
    """
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    rows = con.execute(
        """select bar_dt_utc, open, close from bars
           where symbol=? and bar_size='5 mins' and use_rth=1
           order by bar_dt_utc""", (symbol,)).fetchall()
    con.close()
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
    out = {}
    for ts, o, c in rows:
        d = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc).astimezone(ET).date()
        if d not in out:
            out[d] = [o, c]
        else:
            out[d][1] = c
    return {d: tuple(v) for d, v in sorted(out.items())}


def overnights(sess):
    """[(close_date, weekday, gap_days, return_bp)] for consecutive sessions."""
    days = sorted(sess)
    out = []
    for a, b in zip(days, days[1:]):
        close = sess[a][1]
        nxt_open = sess[b][0]
        if not close or not nxt_open:
            continue
        out.append((a, a.weekday(), (b - a).days,
                    (nxt_open - close) / close * 10_000))
    return out


def stats(xs):
    n = len(xs)
    if n < 3:
        return n, float("nan"), float("nan"), float("nan"), float("nan")
    m, sd = st.mean(xs), st.stdev(xs)
    if sd == 0:
        return n, m, float("nan"), float("nan"), float("nan")
    t = m / (sd / math.sqrt(n))
    p = math.erfc(abs(t) / math.sqrt(2))
    return n, m, t, p, 100 * sum(1 for x in xs if x > 0) / n


def holm(pairs):
    """Holm-Bonferroni: [(label, p)] -> {label: adjusted p}.

    Uniformly more powerful than plain Bonferroni at the same family-wise
    error rate, and the adjusted values are kept monotone so a later test
    cannot end up looking stronger than an earlier, smaller one.
    """
    ordered = sorted((p, lab) for lab, p in pairs if p == p)
    m, adj, running = len(ordered), {}, 0.0
    for i, (p, lab) in enumerate(ordered):
        running = max(running, min(1.0, (m - i) * p))
        adj[lab] = running
    return adj


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(
        Path(__file__).resolve().parents[3] / "historical" / "bars.db"))
    ap.add_argument("--symbol", default="GLD")
    args = ap.parse_args()

    sess = sessions(args.db, args.symbol)
    nights = overnights(sess)
    if not nights:
        print("no overnight pairs — is the 5-min RTH series cached?")
        return

    lo, hi = min(sess), max(sess)
    allr = [r for *_, r in nights]
    n_all, m_all, t_all, p_all, hit_all = stats(allr)
    print(f"\n{args.symbol} overnight holds (close -> next open), "
          f"{lo} .. {hi}")
    print(f"sessions {len(sess)}, nights {len(nights)}\n")
    print(f"ALL NIGHTS       n={n_all:4}  mean {m_all:+7.2f} bp  "
          f"t={t_all:+5.2f}  p={p_all:.3f}  hit {hit_all:.1f}%  "
          f"total {sum(allr):+8.0f} bp")

    by = defaultdict(list)
    for _, wd, _, r in nights:
        by[wd].append(r)

    rows, praw = [], []
    for wd in range(5):
        xs = by.get(wd, [])
        n, m, t, p, hit = stats(xs)
        rows.append((WEEKDAYS[wd], n, m, t, p, hit, sum(xs)))
        praw.append((WEEKDAYS[wd], p))
    adj = holm(praw)

    print("\nBY THE WEEKDAY THE NIGHT STARTS  (Fri = the weekend hold)")
    print(f"  {'day':4} {'n':>4} {'mean bp':>9} {'total bp':>9} {'hit%':>6} "
          f"{'t':>6} {'p':>7} {'p(Holm)':>8}")
    print("  " + "-" * 62)
    for lab, n, m, t, p, hit, tot in rows:
        print(f"  {lab:4} {n:4} {m:+9.2f} {tot:+9.0f} {hit:6.1f} "
              f"{t:+6.2f} {p:7.3f} {adj.get(lab, float('nan')):8.3f}")

    print("\nOMISSION TEST — total overnight bp if that weekday's nights are skipped")
    print(f"  {'skip':6} {'kept n':>7} {'total bp':>10} {'vs all':>9} {'per night':>10}")
    print("  " + "-" * 48)
    base = sum(allr)
    for wd in range(5):
        kept = [r for _, w, _, r in nights if w != wd]
        tot = sum(kept)
        print(f"  {WEEKDAYS[wd]:6} {len(kept):7} {tot:+10.0f} "
              f"{tot - base:+9.0f} {tot / len(kept):+10.2f}")

    # Fridays are not all weekends: a Monday holiday makes it a 3-night hold.
    fri = [(g, r) for _, w, g, r in nights if w == 4]
    if fri:
        print("\nFRIDAY NIGHTS BY GAP LENGTH")
        bygap = defaultdict(list)
        for g, r in fri:
            bygap[g].append(r)
        for g in sorted(bygap):
            n, m, t, p, hit = stats(bygap[g])
            span = "Fri->Mon" if g == 3 else f"Fri->+{g}d"
            print(f"  {span:10} n={n:3}  mean {m:+7.2f} bp  hit {hit:5.1f}%  "
                  f"p={p:.3f}  total {sum(bygap[g]):+7.0f} bp")

    # The third Friday is monthly option expiration for US equities and for
    # GLD itself, so the weekend hold that follows it is the one with a
    # plausible mechanism for behaving differently. Ordinal position is
    # counted within the calendar month.
    print("\nFRIDAY NIGHTS BY POSITION IN THE MONTH  (3rd Friday = monthly OpEx)")
    bypos = defaultdict(list)
    for d, w, g, r in nights:
        if w != 4:
            continue
        bypos[sum(1 for day in range(1, d.day + 1)
                  if date(d.year, d.month, day).weekday() == 4)].append(r)
    print(f"  {'which':8} {'n':>4} {'mean bp':>9} {'total bp':>9} {'hit%':>6} "
          f"{'t':>6} {'p':>7}")
    print("  " + "-" * 52)
    for pos in sorted(bypos):
        n, m, t_, pv, hit = stats(bypos[pos])
        lab = f"{pos}{'st' if pos == 1 else 'nd' if pos == 2 else 'rd' if pos == 3 else 'th'}"
        print(f"  {lab:8} {n:4} {m:+9.2f} {sum(bypos[pos]):+9.0f} {hit:6.1f} "
              f"{t_:+6.2f} {pv:7.3f}")

    third = bypos.get(3, [])
    other = [r for pos, xs in bypos.items() if pos != 3 for r in xs]
    if len(third) >= 3 and len(other) >= 3:
        n3, m3, _, p3, h3 = stats(third)
        no, mo, _, po, ho = stats(other)
        # Welch two-sample t on the difference of means.
        v3, vo = st.variance(third) / n3, st.variance(other) / no
        tw = (m3 - mo) / math.sqrt(v3 + vo)
        pw = math.erfc(abs(tw) / math.sqrt(2))
        print(f"\n  3rd Friday   n={n3:3}  mean {m3:+7.2f} bp  hit {h3:5.1f}%")
        print(f"  other Fridays n={no:3}  mean {mo:+7.2f} bp  hit {ho:5.1f}%")
        print(f"  difference        {m3 - mo:+7.2f} bp   Welch t={tw:+.2f}  p={pw:.3f}")
        print(f"  NOTE: n={n3} is ~2 years of monthly expirations. Power is very")
        print(f"  low; only an enormous effect could reach significance here.")

    print("\nA bucket needs roughly 2 bp of edge to survive the round trip "
          "(~$2 commission on a $20k slice, plus spread).")


if __name__ == "__main__":
    main()
