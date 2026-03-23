"""
backtest_5min_filter_comparison.py — Intraday filter comparison via TWS

Fetches up to 2 years of 5-min RTH bars for GLD and UUP directly from TWS
and runs TriangleTooth vs Hampel pre-smoother comparison on actual intraday
data, reporting the same strategy-D metrics as backtest_filter_comparison.py.

Strategy D: overnight always + intraday in UUP-gold regime.
Signal:     UUP fast(5) / slow(20) SMA crossover on 5-min bars.
            fast_bars=5 (25 min), slow_bars=20 (100 min) — plugin defaults.

Data is fetched in monthly chunks with pacing delays (IB limit: 1 M per
request for 5-min bars; up to 2 years of history available).

Placed in plugins/gld_usd_swap/ as a backtesting reference and as a template
for adapting the filter comparison to live intraday bar data.

Usage:
    python backtest_5min_filter_comparison.py
    python backtest_5min_filter_comparison.py --port 7496
    python backtest_5min_filter_comparison.py --port 7497 --years 1 --clientid 78
"""

import argparse
import bisect
import sys
import threading
import time
from collections import deque, defaultdict
from datetime import datetime

sys.path.insert(0, "/home/ron/claude/pythonclient")
sys.path.insert(0, "/home/ron/claude/volomom")

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper

from volmon import StreamingTriangleTooth
from hampel import StreamingHampel

# ---------------------------------------------------------------------------
# Strategy parameters — match plugin defaults
# ---------------------------------------------------------------------------
FAST  = 5      # UUP fast SMA bars (25 min at 5-min bars)
SLOW  = 20     # UUP slow SMA bars (100 min)
ALLOC = 10_000

OPEN_TIME  = "09:30:00"   # bar that carries the session open price
CLOSE_TIME = "15:45:00"   # decision bar; close ≈ MOC fill proxy


# ---------------------------------------------------------------------------
# IB connection and historical data fetcher
# ---------------------------------------------------------------------------

class HistoricalFetcher(EWrapper, EClient):

    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self._lock       = threading.Lock()
        self._requests   = {}          # req_id → {"bars": list, "event": Event}
        self._next_req_id = 1000
        self._connected  = threading.Event()

    # -- connection ---------------------------------------------------------

    def nextValidId(self, orderId: int) -> None:
        self._connected.set()

    def connectionClosed(self) -> None:
        print("[IB] Connection closed", flush=True)

    def error(self, reqId: int, errorTime: int, errorCode: int,
              errorString: str, advancedOrderRejectJson: str = "") -> None:
        # Release any waiting request on fatal data errors
        if errorCode in (162, 321, 354):
            with self._lock:
                if reqId in self._requests:
                    self._requests[reqId]["event"].set()
        elif errorCode not in (2104, 2106, 2107, 2108, 2119, 2158):
            print(f"  [IB {errorCode}] req={reqId}: {errorString}", flush=True)

    # -- historical data callbacks ------------------------------------------

    def historicalData(self, reqId: int, bar) -> None:
        with self._lock:
            if reqId in self._requests:
                self._requests[reqId]["bars"].append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
        with self._lock:
            if reqId in self._requests:
                self._requests[reqId]["event"].set()

    # -- public fetch API ---------------------------------------------------

    def fetch_bars(self, symbol: str, years: int = 2,
                   pacing_secs: float = 3.0) -> list:
        """
        Fetch up to `years` years of 5-min RTH bars in 1-month chunks,
        walking backwards from today.  Returns bars sorted oldest→newest.

        IB pacing: max ~60 requests / 10 min; 3-second default is safe.
        """
        contract          = Contract()
        contract.symbol   = symbol
        contract.secType  = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        all_bars: dict = {}   # date_str → bar  (deduplicates overlapping chunks)
        end_dt   = ""          # first request: "" means now

        total_chunks = years * 12
        print(f"  Fetching {symbol}: {total_chunks} monthly chunks …", flush=True)

        for chunk_idx in range(total_chunks):
            req_id = self._next_req_id
            self._next_req_id += 1

            event = threading.Event()
            with self._lock:
                self._requests[req_id] = {"bars": [], "event": event}

            self.reqHistoricalData(
                req_id, contract, end_dt,
                "1 M",      # duration
                "5 mins",   # bar size
                "TRADES",
                1,          # useRTH
                1,          # formatDate=1 (human-readable local time)
                False,      # keepUpToDate
                [],
            )

            if not event.wait(timeout=60):
                print(f"    Timeout on chunk {chunk_idx + 1}", flush=True)
                break

            with self._lock:
                chunk_bars = list(self._requests.pop(req_id)["bars"])

            if not chunk_bars:
                print(f"    No data at chunk {chunk_idx + 1} — reached history limit",
                      flush=True)
                break

            for b in chunk_bars:
                all_bars[b.date] = b

            earliest  = min(chunk_bars, key=lambda b: b.date)
            date_part = earliest.date[:8]          # "YYYYMMDD" — robust to space or hyphen separator
            end_dt    = date_part + "-00:00:00"    # midnight UTC — after US market close (4 PM ET)

            print(f"    chunk {chunk_idx + 1:>2}/{total_chunks}  "
                  f"{len(chunk_bars):>5} bars  earliest {date_part}", flush=True)

            if chunk_idx < total_chunks - 1:
                time.sleep(pacing_secs)

        bars = sorted(all_bars.values(), key=lambda b: b.date)
        print(f"  {symbol}: {len(bars)} total bars", flush=True)
        return bars


# ---------------------------------------------------------------------------
# Filter wrappers — unified interface: push(float) → float
# ---------------------------------------------------------------------------

class _RawFilter:
    def push(self, x: float) -> float:
        return x


class _RampAdaptiveFilter:
    """Walk-forward TriangleTooth: p50 of trailing 20-bar moves."""

    def __init__(self, vol_window: int = 20, percentile: int = 50,
                 change_threshold: float = 0.05):
        self._vol_window   = vol_window
        self._percentile   = percentile
        self._change_thresh = change_threshold
        self._moves_dq     = deque()
        self._sorted_buf   = []
        self._smoother     = None
        self._cur_deriv    = None
        self._prev         = None

    def push(self, p: float) -> float:
        if self._prev is not None:
            move = abs(p - self._prev)
            self._moves_dq.append(move)
            bisect.insort(self._sorted_buf, move)
            if len(self._moves_dq) > self._vol_window:
                old = self._moves_dq.popleft()
                del self._sorted_buf[bisect.bisect_left(self._sorted_buf, old)]

        new_deriv = None
        if len(self._sorted_buf) >= 2:
            idx       = min(int(len(self._sorted_buf) * self._percentile / 100),
                            len(self._sorted_buf) - 1)
            new_deriv = self._sorted_buf[idx]

        if new_deriv is not None and new_deriv > 0:
            if self._smoother is None:
                self._smoother  = StreamingTriangleTooth(new_deriv, 1.0)
                self._cur_deriv = new_deriv
            elif (self._cur_deriv > 0 and
                  abs(new_deriv - self._cur_deriv) / self._cur_deriv
                  > self._change_thresh):
                s        = StreamingTriangleTooth(new_deriv, 1.0)
                s._prev  = self._prev
                self._smoother  = s
                self._cur_deriv = new_deriv

        self._prev = p

        if self._smoother is not None:
            pts = self._smoother.push(p)
            return sum(pts) / len(pts)
        return p


# Filter variant registry: (label, factory_fn)
FILTER_VARIANTS = [
    ("RAW",            _RawFilter),
    ("RAMP adaptive",  _RampAdaptiveFilter),
    ("HAMPEL w5  k3",  lambda: StreamingHampel(window=5,  k=3.0)),
    ("HAMPEL w10 k3",  lambda: StreamingHampel(window=10, k=3.0)),
    ("HAMPEL w20 k3",  lambda: StreamingHampel(window=20, k=3.0)),
    ("HAMPEL w10 k2",  lambda: StreamingHampel(window=10, k=2.0)),
]


# ---------------------------------------------------------------------------
# Bar helpers
# ---------------------------------------------------------------------------

def _bar_date(b) -> str:
    return b.date[:8]     # chars 0-7 always "YYYYMMDD" regardless of separator

def _bar_time(b) -> str:
    return b.date[9:17]   # chars 9-16 always "HH:MM:SS" regardless of space/hyphen separator


# ---------------------------------------------------------------------------
# Build daily session records
# ---------------------------------------------------------------------------

def build_sessions(gld_bars: list, uup_bars: list) -> list:
    """
    Walk 5-min bars day by day.  For each trading day produce a dict:
        date        : "YYYYMMDD"
        gld_open    : float | None   (09:30 bar open — session open price)
        gld_close   : float | None   (15:45 bar close — MOC price proxy)
        regimes     : {filter_label: "gold"|"cash"|None}

    UUP bars are fed through every filter variant in parallel, maintaining
    independent SMA state per variant.  Regime is read at the 15:45 bar.
    """
    # Group by date
    gld_by_date: dict = defaultdict(list)
    for b in gld_bars:
        gld_by_date[_bar_date(b)].append(b)

    uup_by_date: dict = defaultdict(list)
    for b in uup_bars:
        uup_by_date[_bar_date(b)].append(b)

    trading_dates = sorted(set(gld_by_date) & set(uup_by_date))

    # Per-variant persistent state (survives across days, as in the live plugin)
    filter_states = {lbl: fn() for lbl, fn in FILTER_VARIANTS}
    sma_bufs      = {lbl: deque() for lbl, _ in FILTER_VARIANTS}

    sessions = []

    for date_str in trading_dates:
        gld_day = sorted(gld_by_date[date_str], key=_bar_time)
        uup_day = sorted(uup_by_date[date_str], key=_bar_time)

        # GLD open price: 09:30 bar open
        gld_open = next(
            (float(b.open) for b in gld_day if _bar_time(b) == OPEN_TIME),
            None,
        )
        # GLD decision price: 15:45 bar close
        gld_close = next(
            (float(b.close) for b in gld_day if _bar_time(b) == CLOSE_TIME),
            None,
        )

        # Feed all UUP bars through each filter; read regime at 15:45
        day_regimes = {lbl: None for lbl, _ in FILTER_VARIANTS}

        for uup_bar in uup_day:
            t     = _bar_time(uup_bar)
            close = float(uup_bar.close)

            for lbl, _ in FILTER_VARIANTS:
                filtered = filter_states[lbl].push(close)

                buf = sma_bufs[lbl]
                buf.append(filtered)
                if len(buf) > SLOW:
                    buf.popleft()

                if t == CLOSE_TIME:
                    if len(buf) >= SLOW:
                        fast = sum(list(buf)[-FAST:]) / FAST
                        slow = sum(buf) / len(buf)
                        day_regimes[lbl] = "gold" if fast <= slow else "cash"

        sessions.append({
            "date":      date_str,
            "gld_open":  gld_open,
            "gld_close": gld_close,
            "regimes":   day_regimes,
        })

    return sessions


# ---------------------------------------------------------------------------
# Strategy runner
# ---------------------------------------------------------------------------

def run_strategy(sessions: list, filter_label: str) -> list:
    """
    Strategy D on 5-min bar sessions:
      - Overnight (always): gld_open[N] / gld_close[N-1] - 1
      - Intraday (if prior-day regime = gold): gld_close[N] / gld_open[N] - 1
    """
    cash         = float(ALLOC)
    equity       = [cash]
    prior_regime = sessions[0]["regimes"][filter_label] if sessions else None

    for i in range(1, len(sessions)):
        day      = sessions[i]
        prev_day = sessions[i - 1]

        gld_open  = day["gld_open"]
        gld_close = day["gld_close"]
        prev_close = prev_day["gld_close"]

        # Overnight leg — always held
        if gld_open and prev_close:
            cash *= gld_open / prev_close

        # Intraday leg — only in gold regime
        if prior_regime == "gold" and gld_open and gld_close:
            cash *= gld_close / gld_open

        prior_regime = day["regimes"][filter_label]
        equity.append(cash)

    return equity


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def stats(equity: list):
    """Return (total_ret%, ann%, vol%, sharpe, max_dd%)."""
    n         = len(equity)
    total_ret = (equity[-1] - ALLOC) / ALLOC * 100
    ann       = total_ret / (n / 252)
    rets      = [(equity[i] - equity[i-1]) / equity[i-1]
                 for i in range(1, n) if equity[i-1] > 0]
    mean_r    = sum(rets) / len(rets) if rets else 0
    var       = sum((r - mean_r)**2 for r in rets) / len(rets) if rets else 0
    vol       = var**0.5 * 252**0.5 * 100
    sharpe    = ann / vol if vol > 0 else 0.0
    peak, max_dd = float(ALLOC), 0.0
    for v in equity:
        peak   = max(peak, v)
        max_dd = max(max_dd, (peak - v) / peak * 100)
    return total_ret, ann, vol, sharpe, max_dd


def regime_flips(sessions: list, label: str) -> int:
    regs  = [s["regimes"][label] for s in sessions if s["regimes"][label]]
    return sum(1 for a, b in zip(regs, regs[1:]) if a != b)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="5-min filter comparison backtest via TWS"
    )
    parser.add_argument("--port",     type=int, default=7497)
    parser.add_argument("--clientid", type=int, default=78)
    parser.add_argument("--years",    type=int, default=2,
                        help="Years of history to fetch (max ~2)")
    args = parser.parse_args()

    print(f"\n5-min filter comparison  port={args.port}  "
          f"clientid={args.clientid}  years={args.years}\n")

    # -- Connect -------------------------------------------------------------
    app = HistoricalFetcher()
    app.connect("127.0.0.1", args.port, clientId=args.clientid)

    reader = threading.Thread(target=app.run, daemon=True)
    reader.start()

    if not app._connected.wait(timeout=10):
        print("ERROR: could not connect to TWS within 10 seconds")
        sys.exit(1)

    time.sleep(0.5)
    print("Connected.\n")

    # -- Fetch ---------------------------------------------------------------
    gld_bars = app.fetch_bars("GLD", years=args.years)
    print()
    uup_bars = app.fetch_bars("UUP", years=args.years)
    print()

    app.disconnect()

    if not gld_bars or not uup_bars:
        print("ERROR: no bars received")
        sys.exit(1)

    # -- Build sessions ------------------------------------------------------
    print("Building daily sessions …", flush=True)
    sessions = build_sessions(gld_bars, uup_bars)
    print(f"  {len(sessions)} trading days\n")

    if len(sessions) < SLOW + 1:
        print("ERROR: not enough data to warm up the SMA")
        sys.exit(1)

    # -- GLD buy-and-hold reference -----------------------------------------
    first_close = next(s["gld_close"] for s in sessions if s["gld_close"])
    last_close  = next(s["gld_close"] for s in reversed(sessions) if s["gld_close"])
    bah_ret     = (last_close / first_close - 1) * 100

    # -- Results -------------------------------------------------------------
    SEP = "  " + "-" * 96

    print(f"{'='*98}")
    print(f"  Strategy D — overnight always + intraday in UUP-gold regime")
    print(f"  Signal: UUP fast={FAST} / slow={SLOW} bars on 5-min RTH data")
    print(f"  Period: {sessions[0]['date']} → {sessions[-1]['date']}  "
          f"({len(sessions)} trading days)")
    print(f"{'='*98}")
    print(f"\n  {'Filter':<18} {'Return':>8} {'Ann%':>7} {'Vol%':>6} "
          f"{'Sharpe':>7} {'MaxDD%':>7} {'Flips':>6} {'Final$':>10}")
    print(SEP)

    for lbl, _ in FILTER_VARIANTS:
        eq = run_strategy(sessions, lbl)
        tr, ann, vol, sh, dd = stats(eq)
        flips = regime_flips(sessions, lbl)
        print(f"  {lbl:<18} {tr:>+8.1f}% {ann:>+7.1f}% {vol:>6.1f}% "
              f"{sh:>7.2f} {dd:>7.1f}% {flips:>6} ${eq[-1]:>9,.0f}")

    print(SEP)
    print(f"  {'GLD buy-and-hold':<18} {bah_ret:>+8.1f}%   {'—':>6}   {'—':>5}   "
          f"{'—':>6}   {'—':>6}   {'—':>5}")

    # -- Spike attenuation table on biggest UUP daily moves -----------------
    print(f"\n{'='*98}")
    print(f"  10 largest single 5-min UUP moves — filter attenuation")
    print(f"{'='*98}")

    # Rebuild filter outputs for the full UUP series
    uup_filtered: dict = {lbl: [] for lbl, _ in FILTER_VARIANTS}
    filter_states2 = {lbl: fn() for lbl, fn in FILTER_VARIANTS}
    for b in uup_bars:
        c = float(b.close)
        for lbl, _ in FILTER_VARIANTS:
            uup_filtered[lbl].append(filter_states2[lbl].push(c))

    raw_vals = uup_filtered["RAW"]
    moves    = [abs(raw_vals[i] - raw_vals[i-1]) for i in range(1, len(raw_vals))]
    top10_idx = sorted(range(len(moves)), key=lambda i: moves[i], reverse=True)[:10]

    labels_filt = [lbl for lbl, _ in FILTER_VARIANTS if lbl != "RAW"]
    print(f"\n  {'Date/Time':<20} {'Raw Δ':>8}", end="")
    for lbl in labels_filt:
        print(f"  {lbl[:13]:>13}", end="")
    print()
    print(f"  {'-'*90}")

    for idx in sorted(top10_idx):
        i   = idx + 1   # offset because moves[0] = bars[1]-bars[0]
        raw_d = raw_vals[i] - raw_vals[i-1]
        dt    = uup_bars[i].date[:16]
        print(f"  {dt:<20} {raw_d:>+8.4f}", end="")
        for lbl in labels_filt:
            fv = uup_filtered[lbl]
            d  = fv[i] - fv[i-1]
            print(f"  {d:>+13.4f}", end="")
        print()

    print(f"\nDone.")


if __name__ == "__main__":
    main()
