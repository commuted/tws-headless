"""Shared BarStore readers for the research scripts in this directory.

The daily series these scripts need are not fetched automatically. Populate
them through a running engine, which routes via the BarStore cache:

    ./ibctl.py --env live historical fetch XLB --bar-size "1 day" --duration "2 Y"
    ./ibctl.py --env live historical fetch UUP --bar-size "1 day" --duration "2 Y" --what BID
    ./ibctl.py --env live historical fetch UUP --bar-size "1 day" --duration "2 Y" --what ASK

Daily bars cap at a 2-year chunk (bar_store._MAX_FETCH_SECONDS), so each
symbol-series is a single IB request — cheap enough to run against a live
connection outside market hours.
"""
import json
import sqlite3
from pathlib import Path

DEFAULT_DB = Path(__file__).resolve().parents[3] / "historical" / "bars.db"
DEFAULT_DAYS = Path(__file__).resolve().parent / "no_hold_days.json"


def daily(db, symbols, what="TRADES"):
    """{symbol: {'YYYY-MM-DD': (open, close)}} for one what_to_show series."""
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    marks = ",".join("?" * len(symbols))
    rows = con.execute(
        f"""select symbol, date(bar_dt_utc), open, close from bars
            where bar_size='1 day' and what_to_show=? and symbol in ({marks})
            order by symbol, bar_dt_utc""",
        [what, *symbols]).fetchall()
    out = {}
    for sym, d, o, c in rows:
        out.setdefault(sym, {})[d] = (o, c)
    return out


def sessions(path):
    """The no_hold_days.json written by no_hold_days.py."""
    d = json.loads(Path(path).read_text())
    return d["no_hold_days"], d["hold_days"], d["span"]


def intraday_bp(series, sym, days, sign=1):
    """Signed open->close return in basis points, skipping days with no bar.

    For an equity-session ETF the daily open and close are the opening and
    closing auction prints, so this is also the return an MOO-in / MOC-out
    execution would have received — which is why the cost model in fx_scan.py
    deducts commission only, not a crossed spread.
    """
    out = []
    for d in sorted(days):
        b = series.get(sym, {}).get(d)
        if b and b[0]:
            out.append(sign * (b[1] - b[0]) / b[0] * 10_000)
    return out


def stats(xs):
    """(n, mean, t, two-sided p, hit%) — normal approximation, large n."""
    import math, statistics as st
    n = len(xs)
    if n < 3:
        return n, float("nan"), float("nan"), float("nan"), float("nan")
    m, sd = st.mean(xs), st.stdev(xs)
    if sd == 0:
        return n, m, float("nan"), float("nan"), float("nan")
    t = m / (sd / math.sqrt(n))
    p = math.erfc(abs(t) / math.sqrt(2))
    return n, m, t, p, 100 * sum(1 for x in xs if x > 0) / n
