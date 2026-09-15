#!/usr/bin/env python3
"""
fx_scan.py — does the USD side win over the idle window on no-hold days?

Uses currency ETFs rather than spot pairs, deliberately: they trade the US
equity session, so their daily open->close IS the 09:30-16:00 window the
strategy sits out. A spot FX daily bar's "day" runs 17:00->17:00 ET and mostly
covers hours the plugin is not idle, so its open and close are the wrong two
prices. Returns are signed so POSITIVE always means the USD side won.

Costs are measured, not assumed:
  * spreads come from IB BID/ASK daily bars (fetch them; see _data.py)
  * commission defaults to 0.72 bp/side, the realised rate on a 50-share GLD
    order at $19,592 notional in the live account — override with --commission

The two cost models differ by execution, and the answer flips between them:
  CROSS   buy the ask at the open, sell the bid at the close
  AUCTION MOO in / MOC out, receiving the auction prints that the gross column
          already measures, so only commission is deducted. This is the right
          model for a system that already trades MOC. It does NOT include
          auction imbalance slippage, which is not measured here.

First run (347 no-hold days, 2024-08-23 .. 2026-09-11): all eight legs positive,
UUP t=2.01 and FXY t=2.12, neither surviving Bonferroni across six currency
legs. Negative under CROSS, positive under AUCTION.

    python3 fx_scan.py [--db PATH] [--days no_hold_days.json] [--commission BP]
"""
import argparse
import statistics as st

import _data

LEGS = [
    ("USD basket (UUP long)", "UUP", +1), ("USD basket (UDN short)", "UDN", -1),
    ("USD vs EUR  (FXE short)", "FXE", -1), ("USD vs JPY  (FXY short)", "FXY", -1),
    ("USD vs GBP  (FXB short)", "FXB", -1), ("USD vs CHF  (FXF short)", "FXF", -1),
    ("USD vs AUD  (FXA short)", "FXA", -1), ("USD vs CAD  (FXC short)", "FXC", -1),
]
CURRENCY_LEGS = LEGS[2:]          # the six crosses, excluding the two baskets


def spread_bp(bid, ask, sym, days):
    """Median quoted spread in bp at the open and at the close."""
    so, sc = [], []
    for d in days:
        b, a = bid.get(sym, {}).get(d), ask.get(sym, {}).get(d)
        if not b or not a:
            continue
        if b[0] and a[0]:
            so.append((a[0] - b[0]) / ((a[0] + b[0]) / 2) * 1e4)
        if b[1] and a[1]:
            sc.append((a[1] - b[1]) / ((a[1] + b[1]) / 2) * 1e4)
    return (st.median(so) if so else float("nan"),
            st.median(sc) if sc else float("nan"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(_data.DEFAULT_DB))
    ap.add_argument("--days", default=str(_data.DEFAULT_DAYS))
    ap.add_argument("--commission", type=float, default=0.72,
                    help="bp per side (default: measured live rate)")
    a = ap.parse_args()

    no_hold, hold, span = _data.sessions(a.days)
    syms = [s for _, s, _ in LEGS]
    px = _data.daily(a.db, syms)
    bid, ask = _data.daily(a.db, syms, "BID"), _data.daily(a.db, syms, "ASK")

    print(f"window {span[0]} .. {span[1]}   no-hold {len(no_hold)}   hold {len(hold)}")
    print("positive = the USD side won over 09:30->16:00 ET\n")
    print(f"{'leg':26} {'n':>4} {'mean bp':>8} {'t':>6} {'p':>7} {'USD hit%':>9} "
          f"| {'hold mean':>9}")
    print("-" * 86)
    gross, ps = {}, []
    for label, sym, sign in LEGS:
        xs = _data.intraday_bp(px, sym, no_hold, sign)
        n, m, t, p, hit = _data.stats(xs)
        gross[sym] = m
        if (label, sym, sign) in CURRENCY_LEGS:
            ps.append((sym, p))
        mb = st.mean(_data.intraday_bp(px, sym, hold, sign))
        print(f"{label:26} {n:>4} {m:>8.2f} {t:>6.2f} {p:>7.3f} {hit:>9.1f} "
              f"| {mb:>9.2f}")

    thresh = 0.05 / len(ps)
    print(f"\nsignificant at p<0.05 (uncorrected): "
          f"{[s for s, p in ps if p == p and p < 0.05] or 'none'}")
    print(f"surviving Bonferroni (p<{thresh:.4f}): "
          f"{[s for s, p in ps if p == p and p < thresh] or 'none'}")

    comm = 2 * a.commission
    have = [s for s in syms if s in bid and s in ask]
    if not have:
        print("\n(no BID/ASK series cached — skipping the cost table; see _data.py)")
        return
    print(f"\ncommission {a.commission:.2f} bp/side => {comm:.2f} bp round trip\n")
    print(f"{'leg':6} {'gross':>7} | {'spr@open':>9} {'spr@close':>10} {'cross':>7} "
          f"| {'net CROSS':>10} {'net AUCTION':>12}")
    print("-" * 74)
    for sym in have:
        so, sc = spread_bp(bid, ask, sym, no_hold)
        cross = (so + sc) / 2
        print(f"{sym:6} {gross[sym]:>7.2f} | {so:>9.2f} {sc:>10.2f} {cross:>7.2f} "
              f"| {gross[sym]-cross-comm:>10.2f} {gross[sym]-comm:>12.2f}")


if __name__ == "__main__":
    main()
