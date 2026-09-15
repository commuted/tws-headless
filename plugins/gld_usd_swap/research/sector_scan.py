#!/usr/bin/env python3
"""
sector_scan.py — is there an intraday edge in any equity sector on no-hold days?

The strategy sits in cash 09:30-15:50 on no-hold days. This asks whether the
S&P Select Sector indices move systematically over that window, via the SPDR
ETFs that track them (an ETF's daily open/close ARE the auction prints, so a
daily bar measures exactly the idle window — no intraday bars needed).

Result when first run (2024-08-23 .. 2026-09-11, 347 no-hold days): nothing.
No sector reached p<0.05 even before multiple-testing correction, |t| never
exceeded 0.7, and the equal-weight basket averaged 0.27 bp against an 81.6 bp
standard deviation. Unsurprising on reflection — the regime is built from USD,
rate and inflation proxies to time gold, and carries no information about
equity sector direction.

    python3 sector_scan.py [--db PATH] [--days no_hold_days.json]
"""
import argparse
import statistics as st

import _data

INDEX_ETF = [
    ("SIXB  Materials", "XLB"), ("SIXC  Communications", "XLC"),
    ("SIXE  Energy", "XLE"),    ("SIXI  Industrials", "XLI"),
    ("SIXM  Financials", "XLF"), ("SIXR  Staples", "XLP"),
    ("SIXRE Real estate", "XLRE"), ("SIXT  Technology", "XLK"),
    ("SIXU  Utilities", "XLU"), ("SIXV  Health care", "XLV"),
    ("SIXY  Discretionary", "XLY"), ("---   S&P 500 (ref)", "SPY"),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(_data.DEFAULT_DB))
    ap.add_argument("--days", default=str(_data.DEFAULT_DAYS))
    a = ap.parse_args()

    no_hold, hold, span = _data.sessions(a.days)
    px = _data.daily(a.db, [s for _, s in INDEX_ETF])
    missing = [s for _, s in INDEX_ETF if s not in px]
    if missing:
        raise SystemExit(f"no daily bars for {missing} — see _data.py for the fetch")

    print(f"window {span[0]} .. {span[1]}   no-hold {len(no_hold)}   hold {len(hold)}\n")
    print(f"{'index / ETF':26} {'n':>4} {'mean bp':>8} {'t':>6} {'p':>7} {'hit%':>6} "
          f"| {'hold mean':>9} {'diff bp':>8}")
    print("-" * 92)
    ps = []
    for label, sym in INDEX_ETF:
        a_ = _data.intraday_bp(px, sym, no_hold)
        b_ = _data.intraday_bp(px, sym, hold)
        n, m, t, p, hit = _data.stats(a_)
        mb = st.mean(b_) if b_ else float("nan")
        if sym != "SPY":
            ps.append((sym, p))
        print(f"{label:26} {n:>4} {m:>8.2f} {t:>6.2f} {p:>7.3f} {hit:>6.1f} "
              f"| {mb:>9.2f} {m - mb:>8.2f}")

    thresh = 0.05 / len(ps)
    print(f"\nsignificant at p<0.05 (uncorrected): "
          f"{[s for s, p in ps if p == p and p < 0.05] or 'none'}")
    print(f"surviving Bonferroni (p<{thresh:.4f}): "
          f"{[s for s, p in ps if p == p and p < thresh] or 'none'}")

    basket = []
    for d in no_hold:
        r = [(px[s][d][1] - px[s][d][0]) / px[s][d][0] * 1e4
             for _, s in INDEX_ETF[:11] if d in px[s] and px[s][d][0]]
        if r:
            basket.append(st.mean(r))
    n, m, t, p, _ = _data.stats(basket)
    print(f"\nequal-weight basket of all 11 sectors: n={n} mean={m:.2f} bp "
          f"t={t:.2f} p={p:.3f} sd={st.stdev(basket):.1f} bp")


if __name__ == "__main__":
    main()
