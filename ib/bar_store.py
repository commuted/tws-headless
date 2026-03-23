"""
ib/bar_store.py — SQLite-backed historical bar cache

Persistent local cache for IB historical bar data.  Tracks coverage
intervals per series so repeated requests for the same range never hit
IB rate limits.

Concepts
--------
Series
    A unique combination of (symbol, bar_size, what_to_show, use_rth).
    Example: ("GLD", "5 mins", "TRADES", True)

Coverage
    A set of non-overlapping [start, end] UTC datetime intervals recording
    which date ranges have already been fetched from IB.  Stored in the
    ``coverage`` table and automatically merged on every update.

Gap
    A sub-interval of the requested range that has no coverage.  The
    caller-supplied ``fetch_fn`` is called for each gap; returned bars are
    stored and coverage is extended.

Usage
-----
    from zoneinfo import ZoneInfo
    from datetime import datetime
    from ib.bar_store import BarStore

    store = BarStore("historical/bars.db")

    # Build a fetch_fn that wraps plugin.get_historical_data
    def fetch_gld(start_dt, end_dt):
        end_str = end_dt.strftime("%Y%m%d-%H:%M:%S")   # UTC, new API format
        return plugin.get_historical_data(
            contract=ContractBuilder.etf("GLD"),
            end_date_time=end_str,
            duration_str=bar_store.duration_str(start_dt, end_dt),
            bar_size_setting="5 mins",
            what_to_show="TRADES",
            use_rth=True,
        )

    UTC = ZoneInfo("UTC")
    bars = store.get_bars(
        symbol="GLD", bar_size="5 mins", what_to_show="TRADES", use_rth=True,
        start_dt=datetime(2024, 1, 1, tzinfo=UTC),
        end_dt=datetime(2025, 1, 1, tzinfo=UTC),
        fetch_fn=fetch_gld,
    )

    # Force re-fetch from IB (overwrites cache)
    bars = store.get_bars(..., fetch_fn=fetch_gld, force=True)

    # Inspect what is cached
    store.coverage_summary()

Schema
------
    bars     : one row per bar, keyed on (symbol, bar_size, what_to_show,
               use_rth, bar_dt_utc).  bar_dt_orig preserves the original IB
               date string so callers' existing parsing code is unchanged.

    coverage : one row per non-overlapping interval per series, stored as
               ISO-8601 UTC strings.  Automatically merged on every insert.
"""

import logging
import sqlite3
import threading
from collections import namedtuple
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, List, Optional, Tuple

logger = logging.getLogger(__name__)

UTC = timezone.utc

# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

SeriesKey = namedtuple("SeriesKey", ["symbol", "bar_size", "what_to_show", "use_rth"])

BarRecord = namedtuple(
    "BarRecord",
    ["date", "open", "high", "low", "close", "volume", "wap", "bar_count"],
)

# ---------------------------------------------------------------------------
# IB bar size → maximum safe fetch duration in seconds
# (Conservative; IB may allow slightly more per call)
# ---------------------------------------------------------------------------
_MAX_FETCH_SECONDS = {
    "1 secs":   86_400,       # 1 day
    "5 secs":   86_400,
    "10 secs":  86_400,
    "15 secs":  86_400,
    "30 secs":  86_400,
    "1 min":    604_800,      # 1 week
    "2 mins":   604_800,
    "3 mins":   604_800,
    "5 mins":   2_592_000,    # 30 days (1 M)
    "10 mins":  2_592_000,
    "15 mins":  2_592_000,
    "20 mins":  2_592_000,
    "30 mins":  2_592_000,
    "1 hour":   2_592_000,
    "2 hours":  2_592_000,
    "3 hours":  2_592_000,
    "4 hours":  2_592_000,
    "8 hours":  2_592_000,
    "1 day":    63_072_000,   # 2 years (730 days)
    "1 week":   63_072_000,
    "1 month":  63_072_000,
}
_DEFAULT_MAX_FETCH_SECONDS = 2_592_000   # 30 days fallback

# Bar size → duration of one bar in seconds (for insert_bar coverage endpoint)
_BAR_SECONDS = {
    "1 secs": 1,    "5 secs": 5,    "10 secs": 10,  "15 secs": 15,  "30 secs": 30,
    "1 min":  60,   "2 mins": 120,  "3 mins":  180,  "5 mins":  300,
    "10 mins": 600, "15 mins": 900, "20 mins": 1200, "30 mins": 1800,
    "1 hour": 3600, "2 hours": 7200, "3 hours": 10800,
    "4 hours": 14400, "8 hours": 28800,
    "1 day": 86400, "1 week": 604800, "1 month": 2592000,
}


# ---------------------------------------------------------------------------
# Helpers: bar date parsing
# ---------------------------------------------------------------------------

def _parse_bar_dt(date_str: str) -> datetime:
    """
    Parse an IB bar date string to a UTC-aware datetime.

    Handles:
      "20240115"              daily bar   → midnight UTC that day
      "20240115 09:30:00"     intraday, ET (legacy space separator)
      "20240115-09:30:00"     intraday, ET (new hyphen separator)
      "20240115 09:30:00 US/Eastern"  (suffix stripped)

    Intraday bars from IB carry exchange-local time (US/Eastern).  The
    conversion uses zoneinfo when available; falls back to a fixed UTC-5
    offset when zoneinfo is absent (acceptable for backtesting, wrong for
    DST boundary bars — install tzdata for full accuracy).
    """
    s = date_str.strip()

    # Daily bar: date only
    if len(s) == 8 and s.isdigit():
        return datetime(int(s[:4]), int(s[4:6]), int(s[6:8]), tzinfo=UTC)

    # Intraday: extract "YYYYMMDD" and "HH:MM:SS" regardless of separator
    date_part = s[:8]
    time_part = s[9:17]
    naive = datetime(
        int(date_part[:4]), int(date_part[4:6]), int(date_part[6:8]),
        int(time_part[:2]), int(time_part[3:5]), int(time_part[6:8]),
    )

    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
        return naive.replace(tzinfo=et).astimezone(UTC)
    except Exception:
        # Fallback: treat as EST (UTC-5); off by 1 h during EDT
        return naive.replace(tzinfo=timezone(timedelta(hours=-5))).astimezone(UTC)


def _dt_to_iso(dt: datetime) -> str:
    """Format a UTC datetime as ISO-8601 string for SQLite storage."""
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")


def _iso_to_dt(s: str) -> datetime:
    """Parse an ISO-8601 UTC string from SQLite back to a datetime."""
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)


# ---------------------------------------------------------------------------
# Helpers: interval arithmetic
# ---------------------------------------------------------------------------

def _merge_intervals(intervals: List[Tuple[datetime, datetime]]) \
        -> List[Tuple[datetime, datetime]]:
    """Merge a list of possibly-overlapping [start, end] intervals."""
    if not intervals:
        return []
    srt = sorted(intervals, key=lambda x: x[0])
    merged = [srt[0]]
    for start, end in srt[1:]:
        if start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def _compute_gaps(
    coverage: List[Tuple[datetime, datetime]],
    start_dt: datetime,
    end_dt: datetime,
) -> List[Tuple[datetime, datetime]]:
    """
    Return sub-intervals of [start_dt, end_dt] not covered by any
    interval in ``coverage``.
    """
    gaps = []
    cursor = start_dt
    for cov_start, cov_end in sorted(coverage, key=lambda x: x[0]):
        if cov_end <= cursor:
            continue
        if cov_start > cursor:
            gaps.append((cursor, cov_start))
        cursor = max(cursor, cov_end)
        if cursor >= end_dt:
            break
    if cursor < end_dt:
        gaps.append((cursor, end_dt))
    return gaps


def _chunk_gap(
    start_dt: datetime,
    end_dt: datetime,
    max_seconds: int,
) -> List[Tuple[datetime, datetime]]:
    """Split [start_dt, end_dt] into chunks no larger than max_seconds."""
    chunks = []
    chunk_start = start_dt
    delta = timedelta(seconds=max_seconds)
    while chunk_start < end_dt:
        chunk_end = min(chunk_start + delta, end_dt)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end
    return chunks


def duration_str(start_dt: datetime, end_dt: datetime) -> str:
    """
    Convert a (start, end) datetime pair to an IB durationStr.

    Exported as a module-level helper so callers can build the correct
    durationStr when constructing a fetch_fn.
    """
    seconds = int((end_dt - start_dt).total_seconds())
    if seconds <= 86_400:
        return f"{seconds} S"
    days = (seconds + 86_399) // 86_400    # ceiling
    if days <= 30:
        return f"{days} D"
    weeks = (days + 6) // 7
    if weeks <= 52:
        return f"{weeks} W"
    months = (days + 29) // 30
    if months <= 24:
        return f"{months} M"
    years = (months + 11) // 12
    return f"{years} Y"


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_DDL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS bars (
    symbol       TEXT    NOT NULL,
    bar_size     TEXT    NOT NULL,
    what_to_show TEXT    NOT NULL,
    use_rth      INTEGER NOT NULL,
    bar_dt_utc   TEXT    NOT NULL,
    bar_dt_orig  TEXT    NOT NULL,
    open         REAL    NOT NULL,
    high         REAL    NOT NULL,
    low          REAL    NOT NULL,
    close        REAL    NOT NULL,
    volume       INTEGER NOT NULL,
    wap          REAL    NOT NULL,
    bar_count    INTEGER NOT NULL,
    PRIMARY KEY (symbol, bar_size, what_to_show, use_rth, bar_dt_utc)
);

CREATE TABLE IF NOT EXISTS coverage (
    symbol       TEXT NOT NULL,
    bar_size     TEXT NOT NULL,
    what_to_show TEXT NOT NULL,
    use_rth      INTEGER NOT NULL,
    start_utc    TEXT NOT NULL,
    end_utc      TEXT NOT NULL,
    fetched_at   TEXT NOT NULL,
    PRIMARY KEY (symbol, bar_size, what_to_show, use_rth, start_utc)
);

CREATE INDEX IF NOT EXISTS idx_bars_lookup
    ON bars (symbol, bar_size, what_to_show, use_rth, bar_dt_utc);
"""


# ---------------------------------------------------------------------------
# BarStore
# ---------------------------------------------------------------------------

class BarStore:
    """
    SQLite-backed historical bar cache.

    Thread-safe: each thread gets its own SQLite connection via
    threading.local().  Writes are serialised with a mutex so coverage
    merges are atomic.
    """

    def __init__(self, db_path: str | Path):
        self._db_path  = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local    = threading.local()
        self._write_lock = threading.Lock()
        # Initialise schema on the calling thread
        with self._conn() as conn:
            conn.executescript(_DDL)
        logger.info(f"BarStore initialised: {self._db_path}")

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def get_bars(
        self,
        symbol: str,
        bar_size: str,
        what_to_show: str,
        use_rth: bool,
        start_dt: datetime,
        end_dt: datetime,
        fetch_fn: Callable[[datetime, datetime], List],
        force: bool = False,
    ) -> List[BarRecord]:
        """
        Return all bars for the requested series and date range.

        Parameters
        ----------
        symbol, bar_size, what_to_show, use_rth
            Series identifier.
        start_dt, end_dt
            Requested range (UTC-aware datetimes).
        fetch_fn
            Callable(start_dt, end_dt) → list of bar objects.  Called for
            each gap (or for the full range when force=True).  The returned
            objects must expose .date, .open, .high, .low, .close, .volume,
            and optionally .wap and .barCount.
        force
            If True, bypass the cache, fetch the full range from IB, and
            overwrite any cached bars in [start_dt, end_dt].
        """
        key = SeriesKey(symbol, bar_size, what_to_show, int(use_rth))

        if force:
            gaps = [(start_dt, end_dt)]
            logger.info(f"BarStore force-fetch {key} {start_dt} → {end_dt}")
        else:
            coverage = self._get_coverage(key)
            gaps     = _compute_gaps(coverage, start_dt, end_dt)
            if gaps:
                logger.info(
                    f"BarStore {key.symbol}/{key.bar_size}: "
                    f"{len(gaps)} gap(s) to fetch"
                )
            else:
                logger.debug(f"BarStore {key.symbol}/{key.bar_size}: fully cached")

        max_secs = _MAX_FETCH_SECONDS.get(bar_size, _DEFAULT_MAX_FETCH_SECONDS)

        for gap_start, gap_end in gaps:
            for chunk_start, chunk_end in _chunk_gap(gap_start, gap_end, max_secs):
                logger.debug(
                    f"  fetch {key.symbol} {chunk_start} → {chunk_end}"
                )
                try:
                    bars = fetch_fn(chunk_start, chunk_end) or []
                except Exception as exc:
                    logger.error(
                        f"  fetch_fn failed for {key.symbol} "
                        f"{chunk_start} → {chunk_end}: {exc}"
                    )
                    bars = []

                with self._write_lock:
                    n = self._store_bars(key, bars, chunk_start, chunk_end)
                    self._update_coverage(key, chunk_start, chunk_end)

                logger.info(
                    f"  stored {n} bars, coverage extended to "
                    f"{chunk_start} → {chunk_end}"
                )

        return self._query_bars(key, start_dt, end_dt)

    def coverage_summary(
        self, symbol: Optional[str] = None
    ) -> List[dict]:
        """
        Return a list of coverage descriptors, optionally filtered by symbol.

        Each dict has keys: symbol, bar_size, what_to_show, use_rth,
        intervals (list of (start_utc, end_utc) strings), total_bars.
        """
        conn = self._conn()
        where = "WHERE symbol = ?" if symbol else ""
        params = (symbol,) if symbol else ()

        rows = conn.execute(
            f"""
            SELECT symbol, bar_size, what_to_show, use_rth,
                   start_utc, end_utc
            FROM   coverage
            {where}
            ORDER  BY symbol, bar_size, what_to_show, use_rth, start_utc
            """,
            params,
        ).fetchall()

        # Group by series key
        from itertools import groupby
        result = []
        keyfn  = lambda r: (r[0], r[1], r[2], r[3])
        for (sym, bs, wts, rth), group in groupby(rows, keyfn):
            intervals = [(r[4], r[5]) for r in group]
            n = conn.execute(
                """SELECT COUNT(*) FROM bars
                   WHERE symbol=? AND bar_size=? AND what_to_show=? AND use_rth=?""",
                (sym, bs, wts, rth),
            ).fetchone()[0]
            result.append({
                "symbol":       sym,
                "bar_size":     bs,
                "what_to_show": wts,
                "use_rth":      bool(rth),
                "intervals":    intervals,
                "total_bars":   n,
            })
        return result

    def purge(
        self,
        symbol: str,
        bar_size: str,
        what_to_show: str,
        use_rth: bool,
    ) -> int:
        """Delete all bars and coverage for a series. Returns bars deleted."""
        key = SeriesKey(symbol, bar_size, what_to_show, int(use_rth))
        with self._write_lock, self._conn() as conn:
            n = conn.execute(
                "DELETE FROM bars WHERE symbol=? AND bar_size=? "
                "AND what_to_show=? AND use_rth=?",
                key,
            ).rowcount
            conn.execute(
                "DELETE FROM coverage WHERE symbol=? AND bar_size=? "
                "AND what_to_show=? AND use_rth=?",
                key,
            )
        logger.info(f"Purged {n} bars for {key}")
        return n

    def insert_bar(
        self,
        symbol: str,
        bar_size: str,
        what_to_show: str,
        use_rth: bool,
        bar,  # ibapi BarData-like: .date, .open, .high, .low, .close, .volume
    ) -> None:
        """Insert a single bar into the cache. Silently no-ops on any failure."""
        try:
            bar_dt = _parse_bar_dt(str(bar.date))
            if bar_dt is None:
                return
            bar_end = bar_dt + timedelta(seconds=_BAR_SECONDS.get(bar_size, 300))
            key = SeriesKey(symbol, bar_size, what_to_show, int(use_rth))
            with self._write_lock:
                self._store_bars(key, [bar], bar_dt, bar_end)
                self._update_coverage(key, bar_dt, bar_end)
        except Exception:
            pass

    # -------------------------------------------------------------------------
    # Internal
    # -------------------------------------------------------------------------

    def _conn(self) -> sqlite3.Connection:
        if not getattr(self._local, "conn", None):
            self._local.conn = sqlite3.connect(
                self._db_path,
                check_same_thread=False,
                isolation_level=None,   # autocommit; we manage transactions
            )
            self._local.conn.execute("PRAGMA journal_mode=WAL")
        return self._local.conn

    def _get_coverage(
        self, key: SeriesKey
    ) -> List[Tuple[datetime, datetime]]:
        rows = self._conn().execute(
            "SELECT start_utc, end_utc FROM coverage "
            "WHERE symbol=? AND bar_size=? AND what_to_show=? AND use_rth=? "
            "ORDER BY start_utc",
            key,
        ).fetchall()
        return [(_iso_to_dt(r[0]), _iso_to_dt(r[1])) for r in rows]

    def _store_bars(
        self,
        key: SeriesKey,
        bars: List,
        chunk_start: datetime,
        chunk_end: datetime,
    ) -> int:
        """
        Upsert bars into the cache.  If force was used the existing rows for
        [chunk_start, chunk_end] are deleted first to ensure freshness.
        """
        conn   = self._conn()
        stored = 0
        with conn:
            conn.execute("BEGIN")
            for b in bars:
                try:
                    bar_dt_utc = _dt_to_iso(_parse_bar_dt(str(b.date)))
                except Exception:
                    logger.warning(f"Could not parse bar date: {b.date!r}")
                    continue

                wap       = float(getattr(b, "wap",      0.0))
                bar_count = int(getattr(b, "barCount",   0))

                conn.execute(
                    """
                    INSERT OR REPLACE INTO bars
                        (symbol, bar_size, what_to_show, use_rth,
                         bar_dt_utc, bar_dt_orig,
                         open, high, low, close, volume, wap, bar_count)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        key.symbol, key.bar_size, key.what_to_show, key.use_rth,
                        bar_dt_utc, str(b.date),
                        float(b.open), float(b.high),
                        float(b.low),  float(b.close),
                        int(b.volume), wap, bar_count,
                    ),
                )
                stored += 1
        return stored

    def _update_coverage(
        self,
        key: SeriesKey,
        start_dt: datetime,
        end_dt: datetime,
    ) -> None:
        """Add a new coverage interval and merge all intervals for the series."""
        existing  = self._get_coverage(key)
        new_start = start_dt.astimezone(UTC)
        new_end   = end_dt.astimezone(UTC)
        merged    = _merge_intervals(existing + [(new_start, new_end)])
        now       = _dt_to_iso(datetime.now(UTC))

        conn = self._conn()
        with conn:
            conn.execute("BEGIN")
            conn.execute(
                "DELETE FROM coverage "
                "WHERE symbol=? AND bar_size=? AND what_to_show=? AND use_rth=?",
                key,
            )
            conn.executemany(
                "INSERT INTO coverage "
                "(symbol, bar_size, what_to_show, use_rth, "
                " start_utc, end_utc, fetched_at) "
                "VALUES (?,?,?,?,?,?,?)",
                [
                    (key.symbol, key.bar_size, key.what_to_show, key.use_rth,
                     _dt_to_iso(s), _dt_to_iso(e), now)
                    for s, e in merged
                ],
            )

    def _query_bars(
        self,
        key: SeriesKey,
        start_dt: datetime,
        end_dt: datetime,
    ) -> List[BarRecord]:
        start_iso = _dt_to_iso(start_dt)
        end_iso   = _dt_to_iso(end_dt)
        rows = self._conn().execute(
            """
            SELECT bar_dt_orig, open, high, low, close, volume, wap, bar_count
            FROM   bars
            WHERE  symbol=? AND bar_size=? AND what_to_show=? AND use_rth=?
              AND  bar_dt_utc >= ? AND bar_dt_utc <= ?
            ORDER  BY bar_dt_utc
            """,
            (*key, start_iso, end_iso),
        ).fetchall()
        return [BarRecord(*r) for r in rows]
