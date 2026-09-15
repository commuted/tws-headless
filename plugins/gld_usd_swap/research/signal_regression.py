#!/usr/bin/env python3
"""
signal_regression.py — do the plugin's own signals predict what it bets on?

Everything else here tests the BINARY regime. That discards most of the
information: a crossover barely apart and one wide apart both collapse to
"cash". This regresses the continuous signals against the returns the plugin
is actually exposed to, then asks whether the intraday exit pays for itself.

    signal at session D's close  ->  decides D+1's open action
    D+1 INTRADAY (09:30->16:00) is what it SITS OUT when the regime is cash
    D -> D+1 OVERNIGHT is held every night regardless

FINDINGS

(1) The signals do not predict intraday DIRECTION. Across 4,610 sessions and
    nineteen years, R2 is 0.03-0.05% and no regressor approaches
    significance; the sign of uup is not even stable between samples
    (-1.16 bp in 2007+, +0.23 bp in 2016+). The intraday exit rests on these
    signals forecasting GLD's intraday move, and on this evidence they do not.

(2) The only significant term is the one the plugin does not use. The
    INTERCEPT on the overnight return is 4.20 / 3.45 / 5.40 bp (t = 3.48,
    2.35, 3.46) with no slope anywhere near significance. GLD earns 4-5 bp
    per night unconditionally and nothing in the signal set predicts
    variation around it. The strategy's return is the overnight hold.

(3) The signals DO predict MAGNITUDE. Regressing |intraday move| gives
    R2 = 0.80% (2007+) and 2.70% (2016+) with the GLD meta-signal strongest
    (t = 5.50, 6.83). Cash-regime sessions are genuinely more volatile —
    intraday sd 75.3 vs 73.5 bp (2007+) and 57.9 vs 52.5 bp (2016+). So the
    exit does avoid variance, which is the strongest defence of the design.

(4) But net of cost the three variants are indistinguishable. Charging the
    0.72 bp/side measured on this account's own GLD fills:

        2016+   current 0.87   always-hold 0.89   overnight-only 0.83
        2007+   current 0.64   always-hold 0.63   overnight-only 0.53

    The variance reduction is real and is almost exactly cancelled by the
    trades that achieve it. Note the 2016 regime is cash on 89% of sessions,
    so it is nearly always-on rather than a selective filter.

CAVEAT THAT MATTERS. This is a daily-SMA analogue, not the live 5-minute
signal. A 25-minute/100-minute crossover could carry intraday information a
5/20-day one cannot — volatility clusters on very different timescales. That
is the real remaining defence and this data cannot test it. The cost model is
also crude: a flat round trip on every variant, ignoring MOC/MKT execution
differences and per-order minimums at real size.

Horizons are set by the shortest input; on IB that is TLT at 2016-02-03,
verified by purge and re-fetch. UUP reaches its 2007 inception.

OLS by Gaussian elimination on the normal equations — no numpy on the
trading host, and fine at this width.

    python3 signal_regression.py --db PATH [--fast 5 --slow 20]
"""
import argparse
import math
import sqlite3
import statistics as st
from collections import defaultdict

RT_COST_BP = 1.44        # 2 x 0.72 bp/side, measured from live GLD fills
SPECS = [("UUP only            (2007+)", ["UUP"]),
         ("UUP + RINF          (2012+)", ["UUP", "RINF"]),
         ("UUP + TLT + RINF    (2016+)", ["UUP", "TLT", "RINF"])]


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


def sma_gap(closes, i, fast, slow):
    if i < slow - 1:
        return None
    s = sum(closes[i - slow + 1:i + 1]) / slow
    return (sum(closes[i - fast + 1:i + 1]) / fast - s) / s if s else None


def ols(y, X, names):
    n, k = len(y), len(X[0]) + 1
    A = [[1.0] + list(r) for r in X]
    M = [[sum(A[r][i] * A[r][j] for r in range(n)) for j in range(k)]
         + [1.0 if i == j else 0.0 for j in range(k)]
         + [sum(A[r][i] * y[r] for r in range(n))] for i in range(k)]
    for c in range(k):
        p = max(range(c, k), key=lambda r: abs(M[r][c]))
        if abs(M[p][c]) < 1e-14:
            return None
        M[c], M[p] = M[p], M[c]
        pv = M[c][c]
        M[c] = [v / pv for v in M[c]]
        for r in range(k):
            if r != c and M[r][c]:
                f = M[r][c]
                M[r] = [v - f * w for v, w in zip(M[r], M[c])]
    b = [M[i][-1] for i in range(k)]
    inv = [[M[i][k + j] for j in range(k)] for i in range(k)]
    res = [y[r] - sum(b[i] * A[r][i] for i in range(k)) for r in range(n)]
    s2 = sum(e * e for e in res) / (n - k)
    ybar = st.mean(y)
    r2 = 1 - sum(e * e for e in res) / sum((v - ybar) ** 2 for v in y)
    rows = []
    for i, nm in enumerate(["intercept"] + names):
        se = math.sqrt(max(s2 * inv[i][i], 0.0))
        rows.append((nm, b[i], se, b[i] / se if se else float("nan")))
    return rows, r2


def zscore(xs):
    m, sd = st.mean(xs), st.stdev(xs)
    return [(x - m) / sd for x in xs] if sd else list(xs)


def show(title, note, coefs, r2):
    print(f"  {title:26} ({note})   R2={r2*100:.2f}%")
    print(f"    {'term':10} {'beta bp':>9} {'se':>7} {'t':>7} {'p':>7}")
    for nm, b, se, t in coefs:
        p = math.erfc(abs(t) / math.sqrt(2))
        print(f"    {nm:10} {b:>9.2f} {se:>7.2f} {t:>7.2f} {p:>7.3f}"
              f"{'  *' if p < 0.05 else ''}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--fast", type=int, default=5)
    ap.add_argument("--slow", type=int, default=20)
    ap.add_argument("--meta-fast", type=int, default=20)
    ap.add_argument("--meta-slow", type=int, default=60)
    a = ap.parse_args()

    px = daily(a.db, ["GLD", "UUP", "TLT", "RINF"])
    gld = px["GLD"]

    for label, regs in SPECS:
        days = sorted(set(gld) & set.intersection(*(set(px[s]) for s in regs)))
        closes = {s: [px[s][d][1] for d in days] for s in regs}
        gclose = [gld[d][1] for d in days]

        X, y_intra, y_over, regime = [], [], [], []
        for i in range(len(days) - 1):
            gaps = [sma_gap(closes[s], i, a.fast, a.slow) for s in regs]
            meta = sma_gap(gclose, i, a.meta_fast, a.meta_slow)
            if any(g is None for g in gaps) or meta is None:
                continue
            o1, c1 = gld[days[i + 1]]
            X.append(gaps + [meta])
            y_intra.append((c1 - o1) / o1 * 1e4)
            y_over.append((o1 - gclose[i]) / gclose[i] * 1e4)
            # UUP falling and TLT/RINF rising are the plugin's gold votes
            votes = [(gaps[j] < 0) if s == "UUP" else (gaps[j] > 0)
                     for j, s in enumerate(regs)]
            regime.append("gold" if all(votes) else "cash")
        if len(X) < 100:
            print(f"\n{label}: too few observations")
            continue

        names = [s.lower() for s in regs] + ["meta"]
        Xz = list(zip(*[zscore(list(c)) for c in zip(*X)]))
        print(f"\n=== {label}   n={len(X)}   {days[0]} .. {days[-1]} ===")

        for tgt, yy, note in (("GLD next-session INTRADAY", y_intra, "what it sits out"),
                              ("GLD next OVERNIGHT", y_over, "what it always holds"),
                              ("|GLD next INTRADAY move|", [abs(v) for v in y_intra],
                               "magnitude, not direction")):
            res = ols(yy, Xz, names)
            if res:
                show(tgt, note, *res)

        cash = [y_intra[i] for i in range(len(y_intra)) if regime[i] == "cash"]
        gold = [y_intra[i] for i in range(len(y_intra)) if regime[i] == "gold"]
        sc, sg = st.stdev(cash), st.stdev(gold)
        print(f"  intraday sd  CASH {sc:.1f} bp (n={len(cash)})   "
              f"GOLD {sg:.1f} bp (n={len(gold)})   ratio {sc*sc/(sg*sg):.3f}")

        frac_cash = len(cash) / len(y_intra)
        variants = {
            "current (exit on cash)": ([y_over[i] + (y_intra[i] if regime[i] == "gold" else 0.0)
                                        for i in range(len(y_intra))], frac_cash),
            "always hold intraday":   ([y_over[i] + y_intra[i] for i in range(len(y_intra))], 0.0),
            "overnight only":         (list(y_over), 1.0),
        }
        print(f"\n  {'variant':24} {'gross':>7} {'cost':>6} {'net':>7} {'sd':>7} {'Sharpe':>7}")
        for nm, (s_, trades) in variants.items():
            m, sd = st.mean(s_), st.stdev(s_)
            cost = trades * RT_COST_BP
            print(f"  {nm:24} {m:>7.2f} {cost:>6.2f} {m-cost:>7.2f} {sd:>7.1f} "
                  f"{(m-cost)/sd*math.sqrt(252):>7.2f}")

    print("\nbeta is bp of GLD move per 1 sd of signal. * marks p<0.05 uncorrected;")
    print("many regressions are reported, so read the stars accordingly.")


if __name__ == "__main__":
    main()
