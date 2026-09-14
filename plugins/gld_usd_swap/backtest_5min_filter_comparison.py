"""
backtest_5min_filter_comparison.py — Intraday backtest of the live
gld_usd_swap composite-regime strategy, via BarStore.

Reproduces exactly what plugins/gld_usd_swap/plugin.py does live:

    - 5-min RTH bars for GLD, UUP, TLT, RINF (fetched through the trading
      engine's `historical fetch` command, backed by the shared BarStore
      cache at historical/bars.db — cached ranges never touch IB again).

    - Each signal ETF (UUP, TLT, RINF) is pre-smoothed with an adaptive
      StreamingTriangleTooth whose slope cap is the p50 of the trailing
      200-bar |Δclose| moves (5% change threshold to re-seat). This is the
      identical machinery the plugin uses at runtime (see
      _InstrumentState.push in plugin.py) — inlined here so a backtest run
      matches the live plugin without importing PluginBase.

    - Fast/slow SMAs (5 / 20 bars) over the *filtered* closes, evaluated
      once per day on the 15:45 bar.

    - GLD meta-signal on RAW closes: 20-bar fast > 60-bar slow ⇒ GLD in
      structural uptrend (no smoother — matches plugin._push_gld_meta).

    - Composite regime cascade (matches plugin._recompute_regime):
        * UUP+TLT|RINF(meta): if GLD in uptrend AND RINF warm ⇒
                              gold = uup_gold AND (tlt_gold OR rinf_gold)
        * UUP+TLT          : else if TLT warm  ⇒ gold = uup_gold AND tlt_gold
        * UUP(fallback)    : else              ⇒ gold = uup_gold
      where uup_gold = uup_fast < uup_slow (USD weakening),
            tlt_gold = tlt_fast > tlt_slow (rates falling),
            rinf_gold = rinf_fast > rinf_slow (inflation exp rising).

    - Strategy execution (matches _on_market_open / _on_market_close):
        * Overnight (always): 15:45 close → 09:30 open on GLD.
        * Intraday (conditional): if regime saved at PRIOR 15:45 == gold,
          hold GLD 09:30 → 15:45; else sit in cash intraday.

Filter comparison: the same composite pipeline is run under several
pre-smoothers. RAW = no smoothing. PLUGIN adaptive = live plugin's exact
TriangleTooth. HAMPEL variants = MAD-based outlier suppression at various
windows.

Requires a running engine (paper or live) with its command socket reachable.

Usage:
    python backtest_5min_filter_comparison.py
    python backtest_5min_filter_comparison.py --env paper
    python backtest_5min_filter_comparison.py --socket ~/.tws_headless_paper.sock
    python backtest_5min_filter_comparison.py --years 1 --force
"""

import argparse
import bisect
import json
import os
import socket
import sys
import time
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from typing import List

# Allow overriding via environment variables; fall back to sibling directories
# relative to this file so the script works without any configuration when the
# repos are checked out next to each other.  Also add the tws-headless project
# root so `from ib.bar_store import BarStore` resolves.
_HERE         = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
sys.path.insert(0, _PROJECT_ROOT)
sys.path.insert(0, os.environ.get("VOLOMOM_PATH", os.path.join(_HERE, "..", "..", "..", "volomom")))

from ib.bar_store import BarStore, duration_str as _duration_str

from hampel import StreamingHampel

UTC = timezone.utc

# ---------------------------------------------------------------------------
# Strategy parameters — MUST match plugins/gld_usd_swap/plugin.py defaults
# ---------------------------------------------------------------------------
FAST_BARS       = 5      # UUP/TLT/RINF fast SMA (25 min)
SLOW_BARS       = 20     # UUP/TLT/RINF slow SMA (100 min)
META_FAST_BARS  = 20     # GLD structural trend fast SMA
META_SLOW_BARS  = 60     # GLD structural trend slow SMA
VOL_WINDOW      = 20     # (matches plugin default; unused inside push — see below)
DERIV_PCTILE    = 50     # p50 of recent |Δclose| sets smoother slope limit
CHANGE_THRESH   = 0.05   # 5% derivative change re-seats the smoother
ADAPT_MAXLEN    = 200    # matches _InstrumentState.abs_moves maxlen in plugin
CLOSES_MAXLEN   = 80     # matches _InstrumentState.closes  maxlen in plugin

ALLOC           = 10_000

# Per-ETF initial derivative — matches _INIT_DERIV_* in plugin.py
_INIT_DERIV = {"UUP": 0.074, "TLT": 0.500, "RINF": 0.200}

SIGNAL_SYMBOLS = ["UUP", "TLT", "RINF"]

OPEN_TIME  = "09:30:00"   # bar that carries the session open price
CLOSE_TIME = "15:45:00"   # decision bar; close ≈ MOC fill proxy

# BarStore series parameters
BAR_SIZE     = "5 mins"
WHAT_TO_SHOW = "TRADES"
USE_RTH      = True

# Regime labels — match plugin.py
REGIME_GOLD    = "gold"
REGIME_CASH    = "cash"
REGIME_UNKNOWN = "unknown"

# ---------------------------------------------------------------------------
# Socket + DB resolution — mirrors ibctl.py's helpers so this script stays
# runnable without importing ibctl as a module.
# ---------------------------------------------------------------------------

_PAPER_PORTS = {7497, 4002}
_LIVE_PORTS  = {7496, 4001}
_LEGACY_SOCK = os.path.expanduser("~/.tws_headless.sock")
_HIST_CONFIG = os.path.join(_PROJECT_ROOT, "historical", "config.json")
_HIST_DB_DEF = os.path.join(_PROJECT_ROOT, "historical", "bars.db")


def _socket_for_env(env: str) -> str:
    return os.path.expanduser(f"~/.tws_headless_{env}.sock")


def _resolve_socket(explicit: str | None, env: str | None) -> str:
    if explicit:
        return os.path.expanduser(explicit)
    if env:
        return _socket_for_env(env)
    if os.path.exists(_LEGACY_SOCK):
        return _LEGACY_SOCK
    paper = _socket_for_env("paper")
    live  = _socket_for_env("live")
    if os.path.exists(paper) and not os.path.exists(live):
        return paper
    if os.path.exists(live):
        raise SystemExit(
            "Live engine socket exists; refusing to guess. "
            "Pass --env paper|live or --socket."
        )
    raise SystemExit(
        f"No engine socket found at {paper}. Start the trading engine first."
    )


def _resolve_db(explicit: str | None) -> str:
    if explicit:
        return os.path.abspath(os.path.expanduser(explicit))
    if os.path.exists(_HIST_CONFIG):
        try:
            with open(_HIST_CONFIG) as f:
                return json.load(f).get("db_path", _HIST_DB_DEF)
        except Exception:
            pass
    return _HIST_DB_DEF


# ---------------------------------------------------------------------------
# Engine socket client — sends one `historical fetch ...` command per chunk
# ---------------------------------------------------------------------------

def _send_command(cmd: str, socket_path: str, timeout: float) -> dict:
    """Send a newline-terminated command to the engine and return the JSON dict."""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(socket_path)
    except FileNotFoundError:
        raise SystemExit(f"Engine socket not found: {socket_path}")
    except ConnectionRefusedError:
        raise SystemExit(f"Engine not accepting connections at {socket_path}")

    try:
        sock.sendall((cmd + "\n").encode("utf-8"))
        buf = b""
        while b"\n" not in buf:
            chunk = sock.recv(65536)
            if not chunk:
                break
            buf += chunk
    finally:
        sock.close()

    if not buf:
        raise RuntimeError(f"Empty response from engine for: {cmd}")
    return json.loads(buf.decode("utf-8").strip())


class _EngineBar:
    """Bar-like object matching what BarStore._store_bars expects."""
    __slots__ = ("date", "open", "high", "low", "close", "volume", "wap", "barCount")

    def __init__(self, d: dict):
        self.date     = d["date"]
        self.open     = float(d["open"])
        self.high     = float(d["high"])
        self.low      = float(d["low"])
        self.close    = float(d["close"])
        self.volume   = int(d["volume"])
        self.wap      = float(d.get("wap", 0.0))
        self.barCount = int(d.get("bar_count", 0))


def _make_fetch_fn(symbol: str, socket_path: str, timeout: float, pacing_secs: float):
    """Build a fetch_fn(start_dt, end_dt) → list of _EngineBar for BarStore."""

    def fetch(chunk_start: datetime, chunk_end: datetime) -> list:
        end_str  = chunk_end.astimezone(UTC).strftime("%Y%m%d-%H:%M:%S")
        duration = _duration_str(chunk_start, chunk_end).replace(" ", "_")
        cmd = (
            f"historical fetch {symbol} "
            f"--bar-size 5_mins --duration {duration} "
            f"--what TRADES --type etf --end {end_str}"
        )
        print(f"    engine → {cmd}", flush=True)
        resp = _send_command(cmd, socket_path, timeout=timeout)

        status = resp.get("status")
        if status != "success":
            print(f"    [engine {status}] {resp.get('message', '')}", flush=True)
            return []

        bars = resp.get("data", {}).get("bars", []) or []
        if pacing_secs > 0:
            time.sleep(pacing_secs)
        return [_EngineBar(b) for b in bars]

    return fetch


def fetch_bars_cached(
    symbol: str,
    start_dt: datetime,
    end_dt: datetime,
    store: BarStore,
    socket_path: str,
    timeout: float,
    pacing_secs: float,
    force: bool,
) -> list:
    """Return all bars for [start_dt, end_dt], pulling gaps through the engine."""
    print(f"  Fetching {symbol}: {start_dt.date()} → {end_dt.date()} …", flush=True)
    bars = store.get_bars(
        symbol=symbol,
        bar_size=BAR_SIZE,
        what_to_show=WHAT_TO_SHOW,
        use_rth=USE_RTH,
        start_dt=start_dt,
        end_dt=end_dt,
        fetch_fn=_make_fetch_fn(symbol, socket_path, timeout, pacing_secs),
        force=force,
    )
    print(f"  {symbol}: {len(bars)} bars in [{start_dt.date()}, {end_dt.date()}]",
          flush=True)
    return bars


# ---------------------------------------------------------------------------
# StreamingTriangleTooth + adaptive driver — INLINED from plugin.py so a
# backtest under the "PLUGIN adaptive" filter matches live behavior byte for
# byte.  Do NOT edit these in isolation; keep them in sync with
# plugins/gld_usd_swap/plugin.py._StreamingTriangleTooth and _InstrumentState.
# ---------------------------------------------------------------------------

class _StreamingTriangleTooth:
    """Slope-limited streaming interpolator (inlined from plugin.py)."""

    def __init__(self, target_derivative: float, step: float = 1.0):
        if step <= 0:
            raise ValueError("step must be positive")
        if target_derivative == 0:
            raise ValueError("target_derivative cannot be zero")
        self.max_slope = abs(target_derivative)
        self.step      = step
        self._prev: float | None = None

    def push(self, value: float) -> List[float]:
        if self._prev is None:
            self._prev = value
            return [value]
        ramp       = self._ramp_between(self._prev, value)
        self._prev = value
        return ramp + [value]

    def _ramp_between(self, start: float, end: float) -> List[float]:
        distance    = end - start
        direction   = 1 if distance > 0 else -1
        dy_per_step = direction * self.max_slope * self.step
        if (direction > 0 and start + dy_per_step >= end) or \
           (direction < 0 and start + dy_per_step <= end):
            return []
        ramp, cur = [], start
        while (direction > 0 and cur + dy_per_step < end) or \
              (direction < 0 and cur + dy_per_step > end):
            cur += dy_per_step
            ramp.append(cur)
        return ramp

    def seed(self, value: float) -> None:
        self._prev = value


class _PluginAdaptive:
    """Wraps plugin's adaptive TriangleTooth pattern behind the push(x)→float
    filter interface used by this backtest.  Matches _InstrumentState in
    plugin.py: same maxlen (200 moves), same p50 percentile, same 5%
    re-seat threshold, per-ETF initial derivative.
    """

    def __init__(self, init_derivative: float):
        self.derivative:    float                    = init_derivative
        self.smoother:      _StreamingTriangleTooth  = _StreamingTriangleTooth(init_derivative)
        self.abs_moves:     deque                    = deque(maxlen=ADAPT_MAXLEN)
        self._sorted_moves: list                     = []
        self.prev_close:    float                    = 0.0

    def push(self, close: float) -> float:
        if self.prev_close > 0:
            move = abs(close - self.prev_close)
            if len(self.abs_moves) == self.abs_moves.maxlen:
                evicted = self.abs_moves[0]
                del self._sorted_moves[bisect.bisect_left(self._sorted_moves, evicted)]
            self.abs_moves.append(move)
            bisect.insort(self._sorted_moves, move)
            self._adapt()
        self.prev_close = close
        out = self.smoother.push(close)
        return sum(out) / len(out)

    def _adapt(self) -> None:
        n = len(self._sorted_moves)
        if n < 5:
            return
        idx       = max(0, int(n * DERIV_PCTILE / 100) - 1)
        new_deriv = self._sorted_moves[idx]
        if new_deriv <= 0:
            return
        if abs(new_deriv - self.derivative) / self.derivative > CHANGE_THRESH:
            self.derivative = new_deriv
            s = _StreamingTriangleTooth(new_deriv)
            if self.prev_close > 0:
                s.seed(self.prev_close)
            self.smoother = s


# ---------------------------------------------------------------------------
# Alternative filter wrappers — same push(x)→float interface
# ---------------------------------------------------------------------------

class _RawFilter:
    def push(self, x: float) -> float:
        return x


# Filter variant registry: (label, factory_fn(symbol) → filter)
# The factory receives the symbol so per-ETF initial derivatives can be
# injected into adaptive filters that need one.
FILTER_VARIANTS = [
    ("RAW",             lambda sym: _RawFilter()),
    ("PLUGIN adaptive", lambda sym: _PluginAdaptive(_INIT_DERIV[sym])),
    ("HAMPEL w5  k3",   lambda sym: StreamingHampel(window=5,  k=3.0)),
    ("HAMPEL w10 k3",   lambda sym: StreamingHampel(window=10, k=3.0)),
    ("HAMPEL w20 k3",   lambda sym: StreamingHampel(window=20, k=3.0)),
    ("HAMPEL w10 k2",   lambda sym: StreamingHampel(window=10, k=2.0)),
]


# ---------------------------------------------------------------------------
# Bar helpers
# ---------------------------------------------------------------------------

def _bar_date(b) -> str:
    return b.date[:8]     # chars 0-7 always "YYYYMMDD" regardless of separator

def _bar_time(b) -> str:
    return b.date[9:17]   # chars 9-16 always "HH:MM:SS" regardless of space/hyphen separator


# ---------------------------------------------------------------------------
# Composite regime — matches plugin._recompute_regime cascade
# ---------------------------------------------------------------------------

def _sma_pair(buf: deque):
    """Return (fast_sma, slow_sma) or (None, None) if not warmed up."""
    if len(buf) < SLOW_BARS:
        return None, None
    cl = list(buf)
    return sum(cl[-FAST_BARS:]) / FAST_BARS, sum(cl[-SLOW_BARS:]) / SLOW_BARS


def _composite_regime(smas: dict, gld_in_uptrend: bool) -> str | None:
    """Replicates plugin._recompute_regime: 3-tier fallback cascade.

    smas is {sym: deque of filtered closes}.  Returns "gold" | "cash" | None
    (None while primary UUP signal hasn't warmed up yet).
    """
    uup_fast, uup_slow = _sma_pair(smas["UUP"])
    if uup_fast is None:
        return None
    uup_gold = uup_fast < uup_slow

    tlt_fast, tlt_slow = _sma_pair(smas["TLT"])
    if tlt_fast is None:
        return REGIME_GOLD if uup_gold else REGIME_CASH   # UUP(fallback)

    tlt_gold = tlt_fast > tlt_slow
    rinf_fast, rinf_slow = _sma_pair(smas["RINF"])

    if gld_in_uptrend and rinf_fast is not None:
        rinf_gold = rinf_fast > rinf_slow
        gold = uup_gold and (tlt_gold or rinf_gold)
    else:
        gold = uup_gold and tlt_gold

    return REGIME_GOLD if gold else REGIME_CASH


# ---------------------------------------------------------------------------
# Build daily session records with composite regime
# ---------------------------------------------------------------------------

def build_sessions(gld_bars, uup_bars, tlt_bars, rinf_bars) -> list:
    """
    Walk 5-min bars day by day.  For each trading day produce:
        date        : "YYYYMMDD"
        gld_open    : float | None   (09:30 bar open — session open price)
        gld_close   : float | None   (15:45 bar close — MOC price proxy)
        regimes     : {filter_label: "gold"|"cash"|None (from 15:45 bar)}

    Per filter variant, three signal ETFs (UUP/TLT/RINF) each have their own
    filter instance and independent SMA buffer; the composite regime is
    evaluated at 15:45 using plugin._recompute_regime's exact cascade.

    GLD meta-signal (structural uptrend) uses RAW closes (no smoother),
    shared across variants — matches plugin._push_gld_meta.
    """
    def _by_date(bars):
        d = defaultdict(list)
        for b in bars:
            d[_bar_date(b)].append(b)
        return d

    by_date = {
        "GLD":  _by_date(gld_bars),
        "UUP":  _by_date(uup_bars),
        "TLT":  _by_date(tlt_bars),
        "RINF": _by_date(rinf_bars),
    }

    trading_dates = sorted(
        set(by_date["GLD"]) & set(by_date["UUP"]) &
        set(by_date["TLT"]) & set(by_date["RINF"])
    )

    # Per-variant persistent state (survives across days, as in the live plugin)
    var_state = {}
    for lbl, factory in FILTER_VARIANTS:
        var_state[lbl] = {
            "filters": {sym: factory(sym)              for sym in SIGNAL_SYMBOLS},
            "smas":    {sym: deque(maxlen=CLOSES_MAXLEN) for sym in SIGNAL_SYMBOLS},
        }

    # GLD meta-signal (raw closes; shared across variants)
    gld_meta_closes = deque(maxlen=CLOSES_MAXLEN)

    sessions = []

    for date_str in trading_dates:
        day_bars = {
            sym: sorted(by_date[sym][date_str], key=_bar_time)
            for sym in ("GLD", "UUP", "TLT", "RINF")
        }

        gld_open = next(
            (float(b.open)  for b in day_bars["GLD"] if _bar_time(b) == OPEN_TIME),
            None,
        )
        gld_close = next(
            (float(b.close) for b in day_bars["GLD"] if _bar_time(b) == CLOSE_TIME),
            None,
        )

        # Index bars by (time, symbol) for chronological interleaved processing
        bars_at_time: dict = defaultdict(dict)
        for sym, bars in day_bars.items():
            for b in bars:
                bars_at_time[_bar_time(b)][sym] = b

        day_regimes = {lbl: None for lbl, _ in FILTER_VARIANTS}

        for t in sorted(bars_at_time.keys()):
            b_by_sym = bars_at_time[t]

            # Feed each signal ETF's close into every filter variant
            for lbl, _ in FILTER_VARIANTS:
                st = var_state[lbl]
                for sym in SIGNAL_SYMBOLS:
                    if sym in b_by_sym:
                        filtered = st["filters"][sym].push(float(b_by_sym[sym].close))
                        st["smas"][sym].append(filtered)

            # Feed GLD close into shared meta-signal buffer (raw, no smoother)
            if "GLD" in b_by_sym:
                gld_meta_closes.append(float(b_by_sym["GLD"].close))

            # Evaluate composite regime at 15:45
            if t == CLOSE_TIME:
                if len(gld_meta_closes) >= META_SLOW_BARS:
                    cl = list(gld_meta_closes)
                    meta_fast = sum(cl[-META_FAST_BARS:]) / META_FAST_BARS
                    meta_slow = sum(cl[-META_SLOW_BARS:]) / META_SLOW_BARS
                    gld_in_uptrend = meta_fast > meta_slow
                else:
                    gld_in_uptrend = False

                for lbl, _ in FILTER_VARIANTS:
                    day_regimes[lbl] = _composite_regime(
                        var_state[lbl]["smas"], gld_in_uptrend
                    )

        sessions.append({
            "date":      date_str,
            "gld_open":  gld_open,
            "gld_close": gld_close,
            "regimes":   day_regimes,
        })

    return sessions


# ---------------------------------------------------------------------------
# Strategy runner — matches plugin _on_market_open / _on_market_close
# ---------------------------------------------------------------------------

def run_strategy(sessions: list, filter_label: str) -> list:
    """
    Live plugin's execution model on 5-min-bar sessions:
      - Overnight (always):     15:45 MOC buy → next 09:30 open on GLD.
      - Intraday (conditional): hold GLD 09:30→15:45 iff regime at PRIOR
                                15:45 == gold; else sit in cash intraday.
    """
    cash         = float(ALLOC)
    equity       = [cash]
    prior_regime = sessions[0]["regimes"][filter_label] if sessions else None

    for i in range(1, len(sessions)):
        day       = sessions[i]
        prev_day  = sessions[i - 1]
        gld_open  = day["gld_open"]
        gld_close = day["gld_close"]
        prev_close = prev_day["gld_close"]

        # Overnight leg — always held (unconditional)
        if gld_open and prev_close:
            cash *= gld_open / prev_close

        # Intraday leg — only when prior-close regime == gold
        if prior_regime == REGIME_GOLD and gld_open and gld_close:
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


def regime_split(sessions: list, label: str) -> tuple:
    """(gold_days, cash_days, unknown_days) for a given filter variant."""
    g = c = u = 0
    for s in sessions:
        r = s["regimes"][label]
        if   r == REGIME_GOLD: g += 1
        elif r == REGIME_CASH: c += 1
        else:                  u += 1
    return g, c, u


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="5-min composite-regime backtest of gld_usd_swap "
                    "(via engine + BarStore cache)"
    )
    parser.add_argument("--socket",  default=None,
                        help="Explicit engine socket path (overrides --env)")
    parser.add_argument("--env",     default=None, choices=["paper", "live"],
                        help="Which engine to target (defaults: legacy → paper)")
    parser.add_argument("--db",      default=None,
                        help="BarStore DB path (default: historical/config.json → historical/bars.db)")
    parser.add_argument("--years",   type=int, default=2,
                        help="Years of history to fetch (max ~2 for 5-min bars)")
    parser.add_argument("--timeout", type=float, default=180.0,
                        help="Per-fetch engine command timeout, seconds")
    parser.add_argument("--pacing",  type=float, default=3.0,
                        help="Seconds to sleep between engine fetches (IB pacing)")
    parser.add_argument("--force",   action="store_true",
                        help="Bypass BarStore cache and refetch the full range")
    args = parser.parse_args()

    socket_path = _resolve_socket(args.socket, args.env)
    db_path     = _resolve_db(args.db)

    end_dt   = datetime.now(UTC).replace(microsecond=0)
    start_dt = end_dt - timedelta(days=args.years * 365)

    print(f"\ngld_usd_swap composite-regime backtest (live-plugin fidelity)")
    print(f"  socket : {socket_path}")
    print(f"  db     : {db_path}")
    print(f"  range  : {start_dt.date()} → {end_dt.date()}  ({args.years}y)")
    print(f"  force  : {args.force}\n")

    store = BarStore(db_path)

    # -- Fetch all 4 series (cache-first; gaps go through the engine) --------
    bars = {}
    for sym in ("GLD", "UUP", "TLT", "RINF"):
        bars[sym] = fetch_bars_cached(
            sym, start_dt, end_dt, store,
            socket_path=socket_path,
            timeout=args.timeout,
            pacing_secs=args.pacing,
            force=args.force,
        )
        print()

    if any(not v for v in bars.values()):
        missing = [k for k, v in bars.items() if not v]
        print(f"ERROR: no bars received for {missing}")
        sys.exit(1)

    # -- Build sessions with composite regime -------------------------------
    print("Building daily sessions with composite UUP+TLT+RINF regime …", flush=True)
    sessions = build_sessions(bars["GLD"], bars["UUP"], bars["TLT"], bars["RINF"])
    print(f"  {len(sessions)} trading days\n")

    if len(sessions) < META_SLOW_BARS // 78 + 2:
        print("ERROR: not enough data to warm up the meta signal")
        sys.exit(1)

    # -- GLD buy-and-hold reference -----------------------------------------
    first_close = next(s["gld_close"] for s in sessions if s["gld_close"])
    last_close  = next(s["gld_close"] for s in reversed(sessions) if s["gld_close"])
    bah_ret     = (last_close / first_close - 1) * 100

    # -- Results -------------------------------------------------------------
    SEP = "  " + "-" * 110

    print(f"{'='*112}")
    print(f"  Composite gold/cash regime — overnight always + intraday when prior-close regime=gold")
    print(f"  Signals: UUP(fast<slow)  ∧  TLT(fast>slow)  [∨ RINF(fast>slow) when GLD meta uptrend]")
    print(f"  Bars: fast={FAST_BARS} / slow={SLOW_BARS} on 5-min RTH; "
          f"meta {META_FAST_BARS}/{META_SLOW_BARS} on raw GLD closes")
    print(f"  Period: {sessions[0]['date']} → {sessions[-1]['date']}  "
          f"({len(sessions)} trading days)")
    print(f"{'='*112}")
    print(f"\n  {'Filter':<18} {'Return':>8} {'Ann%':>7} {'Vol%':>6} "
          f"{'Sharpe':>7} {'MaxDD%':>7} {'Flips':>6} {'Gold':>5} {'Cash':>5} {'?':>4} {'Final$':>10}")
    print(SEP)

    for lbl, _ in FILTER_VARIANTS:
        eq = run_strategy(sessions, lbl)
        tr, ann, vol, sh, dd = stats(eq)
        flips = regime_flips(sessions, lbl)
        g, c, u = regime_split(sessions, lbl)
        print(f"  {lbl:<18} {tr:>+8.1f}% {ann:>+7.1f}% {vol:>6.1f}% "
              f"{sh:>7.2f} {dd:>7.1f}% {flips:>6} {g:>5} {c:>5} {u:>4} "
              f"${eq[-1]:>9,.0f}")

    print(SEP)
    print(f"  {'GLD buy-and-hold':<18} {bah_ret:>+8.1f}%   {'—':>6}   {'—':>5}   "
          f"{'—':>6}   {'—':>6}   {'—':>5}   {'—':>4}  {'—':>4}   {'—':>4}")

    # -- Spike attenuation table on biggest UUP daily moves -----------------
    print(f"\n{'='*112}")
    print(f"  10 largest single 5-min UUP moves — filter attenuation")
    print(f"{'='*112}")

    # Rebuild filter outputs for the full UUP series (independent state)
    uup_filtered: dict = {lbl: [] for lbl, _ in FILTER_VARIANTS}
    filter_states2 = {lbl: fn("UUP") for lbl, fn in FILTER_VARIANTS}
    for b in bars["UUP"]:
        c = float(b.close)
        for lbl, _ in FILTER_VARIANTS:
            uup_filtered[lbl].append(filter_states2[lbl].push(c))

    raw_vals  = uup_filtered["RAW"]
    moves     = [abs(raw_vals[i] - raw_vals[i-1]) for i in range(1, len(raw_vals))]
    top10_idx = sorted(range(len(moves)), key=lambda i: moves[i], reverse=True)[:10]

    labels_filt = [lbl for lbl, _ in FILTER_VARIANTS if lbl != "RAW"]
    print(f"\n  {'Date/Time':<20} {'Raw Δ':>8}", end="")
    for lbl in labels_filt:
        print(f"  {lbl[:16]:>16}", end="")
    print()
    print(f"  {'-'*110}")

    for idx in sorted(top10_idx):
        i     = idx + 1   # offset because moves[0] = bars[1]-bars[0]
        raw_d = raw_vals[i] - raw_vals[i-1]
        dt    = bars["UUP"][i].date[:16]
        print(f"  {dt:<20} {raw_d:>+8.4f}", end="")
        for lbl in labels_filt:
            fv = uup_filtered[lbl]
            d  = fv[i] - fv[i-1]
            print(f"  {d:>+16.4f}", end="")
        print()

    print(f"\nDone.")


if __name__ == "__main__":
    main()
