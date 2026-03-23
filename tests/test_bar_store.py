"""
Unit tests for ib/bar_store.py

Tests BarStore, coverage tracking, gap detection, chunking, and bar parsing.
"""

import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ib.bar_store import (
    BarRecord,
    BarStore,
    SeriesKey,
    _chunk_gap,
    _compute_gaps,
    _merge_intervals,
    _parse_bar_dt,
    duration_str,
)

UTC = timezone.utc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dt(year, month, day, hour=0, minute=0, second=0):
    return datetime(year, month, day, hour, minute, second, tzinfo=UTC)


def _make_bar(date_str, close=100.0, open_=100.0, high=101.0, low=99.0,
              volume=1000, wap=100.0, bar_count=100):
    return SimpleNamespace(
        date=date_str,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        wap=wap,
        barCount=bar_count,
    )


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "test_bars.db"


@pytest.fixture
def store(db_path):
    return BarStore(db_path)


def _key():
    return SeriesKey("GLD", "5 mins", "TRADES", 1)


# ---------------------------------------------------------------------------
# _parse_bar_dt
# ---------------------------------------------------------------------------

class TestParseBarDt:
    def test_daily_bar(self):
        dt = _parse_bar_dt("20240115")
        assert dt == _dt(2024, 1, 15)
        assert dt.tzinfo is not None

    def test_intraday_space_separator(self):
        # ET 09:30 → UTC 14:30 (EST, no DST)
        dt = _parse_bar_dt("20240115 09:30:00")
        assert dt.hour == 14
        assert dt.minute == 30

    def test_intraday_hyphen_separator(self):
        dt = _parse_bar_dt("20240115-09:30:00")
        assert dt.hour == 14
        assert dt.minute == 30

    def test_intraday_with_timezone_suffix(self):
        # Old format with trailing timezone — suffix stripped by positional slice
        dt = _parse_bar_dt("20240115 09:30:00 US/Eastern")
        assert dt.hour == 14

    def test_both_separators_same_result(self):
        a = _parse_bar_dt("20240115 09:30:00")
        b = _parse_bar_dt("20240115-09:30:00")
        assert a == b


# ---------------------------------------------------------------------------
# _merge_intervals
# ---------------------------------------------------------------------------

class TestMergeIntervals:
    def test_empty(self):
        assert _merge_intervals([]) == []

    def test_single(self):
        iv = (_dt(2024, 1, 1), _dt(2024, 1, 2))
        assert _merge_intervals([iv]) == [iv]

    def test_non_overlapping(self):
        a = (_dt(2024, 1, 1), _dt(2024, 1, 2))
        b = (_dt(2024, 1, 3), _dt(2024, 1, 4))
        assert _merge_intervals([a, b]) == [a, b]

    def test_overlapping(self):
        a = (_dt(2024, 1, 1), _dt(2024, 1, 3))
        b = (_dt(2024, 1, 2), _dt(2024, 1, 5))
        result = _merge_intervals([a, b])
        assert result == [(_dt(2024, 1, 1), _dt(2024, 1, 5))]

    def test_adjacent(self):
        a = (_dt(2024, 1, 1), _dt(2024, 1, 2))
        b = (_dt(2024, 1, 2), _dt(2024, 1, 3))
        result = _merge_intervals([a, b])
        assert result == [(_dt(2024, 1, 1), _dt(2024, 1, 3))]

    def test_three_merge_to_one(self):
        intervals = [
            (_dt(2024, 1, 1), _dt(2024, 1, 10)),
            (_dt(2024, 1, 3), _dt(2024, 1, 7)),
            (_dt(2024, 1, 8), _dt(2024, 1, 15)),
        ]
        result = _merge_intervals(intervals)
        assert result == [(_dt(2024, 1, 1), _dt(2024, 1, 15))]

    def test_unsorted_input(self):
        b = (_dt(2024, 1, 5), _dt(2024, 1, 8))
        a = (_dt(2024, 1, 1), _dt(2024, 1, 4))
        result = _merge_intervals([b, a])
        assert result == [a, b]


# ---------------------------------------------------------------------------
# _compute_gaps
# ---------------------------------------------------------------------------

class TestComputeGaps:
    def test_fully_covered(self):
        coverage = [(_dt(2024, 1, 1), _dt(2024, 2, 1))]
        gaps = _compute_gaps(coverage, _dt(2024, 1, 5), _dt(2024, 1, 20))
        assert gaps == []

    def test_no_coverage(self):
        gaps = _compute_gaps([], _dt(2024, 1, 1), _dt(2024, 1, 31))
        assert gaps == [(_dt(2024, 1, 1), _dt(2024, 1, 31))]

    def test_gap_before(self):
        coverage = [(_dt(2024, 1, 15), _dt(2024, 2, 1))]
        gaps = _compute_gaps(coverage, _dt(2024, 1, 1), _dt(2024, 1, 31))
        assert gaps == [(_dt(2024, 1, 1), _dt(2024, 1, 15))]

    def test_gap_after(self):
        coverage = [(_dt(2024, 1, 1), _dt(2024, 1, 15))]
        gaps = _compute_gaps(coverage, _dt(2024, 1, 1), _dt(2024, 1, 31))
        assert gaps == [(_dt(2024, 1, 15), _dt(2024, 1, 31))]

    def test_gap_in_middle(self):
        coverage = [
            (_dt(2024, 1, 1), _dt(2024, 1, 10)),
            (_dt(2024, 1, 20), _dt(2024, 2, 1)),
        ]
        gaps = _compute_gaps(coverage, _dt(2024, 1, 1), _dt(2024, 1, 31))
        assert gaps == [(_dt(2024, 1, 10), _dt(2024, 1, 20))]

    def test_multiple_gaps(self):
        coverage = [
            (_dt(2024, 1, 5), _dt(2024, 1, 10)),
            (_dt(2024, 1, 20), _dt(2024, 1, 25)),
        ]
        gaps = _compute_gaps(coverage, _dt(2024, 1, 1), _dt(2024, 1, 31))
        assert len(gaps) == 3
        assert gaps[0] == (_dt(2024, 1, 1), _dt(2024, 1, 5))
        assert gaps[1] == (_dt(2024, 1, 10), _dt(2024, 1, 20))
        assert gaps[2] == (_dt(2024, 1, 25), _dt(2024, 1, 31))


# ---------------------------------------------------------------------------
# _chunk_gap
# ---------------------------------------------------------------------------

class TestChunkGap:
    def test_fits_in_one(self):
        start = _dt(2024, 1, 1)
        end   = _dt(2024, 1, 5)
        chunks = _chunk_gap(start, end, max_seconds=30 * 86_400)
        assert chunks == [(start, end)]

    def test_splits_evenly(self):
        start = _dt(2024, 1, 1)
        end   = _dt(2024, 1, 1) + timedelta(days=60)
        chunks = _chunk_gap(start, end, max_seconds=30 * 86_400)
        assert len(chunks) == 2
        assert chunks[0][0] == start
        assert chunks[-1][1] == end
        # No gaps between chunks
        for i in range(len(chunks) - 1):
            assert chunks[i][1] == chunks[i + 1][0]

    def test_splits_remainder(self):
        start = _dt(2024, 1, 1)
        end   = start + timedelta(days=35)
        chunks = _chunk_gap(start, end, max_seconds=30 * 86_400)
        assert len(chunks) == 2
        assert chunks[-1][1] == end


# ---------------------------------------------------------------------------
# duration_str
# ---------------------------------------------------------------------------

class TestDurationStr:
    def test_seconds(self):
        s = _dt(2024, 1, 1, 9, 30)
        e = _dt(2024, 1, 1, 16, 0)
        assert duration_str(s, e).endswith(" S")

    def test_days(self):
        s = _dt(2024, 1, 1)
        e = _dt(2024, 1, 15)
        assert duration_str(s, e).endswith(" D")

    def test_weeks(self):
        s = _dt(2024, 1, 1)
        e = _dt(2024, 3, 1)
        assert duration_str(s, e).endswith(" W")

    def test_months(self):
        s = _dt(2023, 1, 1)
        e = _dt(2024, 6, 1)
        assert duration_str(s, e).endswith(" M")

    def test_years(self):
        s = _dt(2022, 1, 1)
        e = _dt(2024, 6, 1)
        assert duration_str(s, e).endswith(" Y")


# ---------------------------------------------------------------------------
# BarStore — basic lifecycle
# ---------------------------------------------------------------------------

class TestBarStoreLifecycle:
    def test_empty_cache_calls_fetch_fn(self, store):
        bars_fetched = []

        def fetch_fn(start, end):
            bars_fetched.append((start, end))
            return [_make_bar("20240115-14:30:00", close=180.0)]

        result = store.get_bars(
            "GLD", "5 mins", "TRADES", True,
            start_dt=_dt(2024, 1, 15),
            end_dt=_dt(2024, 1, 16),
            fetch_fn=fetch_fn,
        )
        assert len(bars_fetched) >= 1
        assert len(result) == 1
        assert result[0].close == pytest.approx(180.0)

    def test_second_call_hits_cache(self, store):
        call_count = [0]

        def fetch_fn(start, end):
            call_count[0] += 1
            return [_make_bar("20240115-14:30:00")]

        kwargs = dict(
            symbol="GLD", bar_size="5 mins", what_to_show="TRADES", use_rth=True,
            start_dt=_dt(2024, 1, 15), end_dt=_dt(2024, 1, 16),
            fetch_fn=fetch_fn,
        )
        store.get_bars(**kwargs)
        store.get_bars(**kwargs)
        assert call_count[0] == 1   # second call served from cache

    def test_force_refetches(self, store):
        call_count = [0]

        def fetch_fn(start, end):
            call_count[0] += 1
            return [_make_bar("20240115-14:30:00")]

        kwargs = dict(
            symbol="GLD", bar_size="5 mins", what_to_show="TRADES", use_rth=True,
            start_dt=_dt(2024, 1, 15), end_dt=_dt(2024, 1, 16),
            fetch_fn=fetch_fn,
        )
        store.get_bars(**kwargs)
        store.get_bars(force=True, **kwargs)
        assert call_count[0] == 2

    def test_returns_bar_records(self, store):
        def fetch_fn(start, end):
            return [_make_bar("20240115-14:30:00", close=181.5)]

        result = store.get_bars(
            "GLD", "5 mins", "TRADES", True,
            start_dt=_dt(2024, 1, 15), end_dt=_dt(2024, 1, 16),
            fetch_fn=fetch_fn,
        )
        assert isinstance(result[0], BarRecord)
        assert result[0].close == pytest.approx(181.5)


# ---------------------------------------------------------------------------
# BarStore — coverage
# ---------------------------------------------------------------------------

class TestBarStoreCoverage:
    def test_coverage_summary_empty(self, store):
        assert store.coverage_summary() == []

    def test_coverage_summary_after_fetch(self, store):
        store.get_bars(
            "GLD", "5 mins", "TRADES", True,
            start_dt=_dt(2024, 1, 1), end_dt=_dt(2024, 1, 31),
            fetch_fn=lambda s, e: [_make_bar("20240102-14:30:00")],
        )
        summary = store.coverage_summary()
        assert len(summary) == 1
        assert summary[0]["symbol"] == "GLD"
        assert len(summary[0]["intervals"]) >= 1

    def test_coverage_merges_adjacent(self, store):
        """Two back-to-back fetches should merge into one coverage interval."""
        def fetch_fn(s, e):
            return []

        store.get_bars(
            "GLD", "5 mins", "TRADES", True,
            start_dt=_dt(2024, 1, 1), end_dt=_dt(2024, 1, 15),
            fetch_fn=fetch_fn,
        )
        store.get_bars(
            "GLD", "5 mins", "TRADES", True,
            start_dt=_dt(2024, 1, 15), end_dt=_dt(2024, 1, 31),
            fetch_fn=fetch_fn,
        )
        summary = store.coverage_summary(symbol="GLD")
        assert len(summary[0]["intervals"]) == 1

    def test_coverage_filter_by_symbol(self, store):
        for sym in ("GLD", "UUP"):
            store.get_bars(
                sym, "5 mins", "TRADES", True,
                start_dt=_dt(2024, 1, 1), end_dt=_dt(2024, 1, 5),
                fetch_fn=lambda s, e: [],
            )
        gld_summary = store.coverage_summary(symbol="GLD")
        assert all(r["symbol"] == "GLD" for r in gld_summary)

    def test_gap_fetch_only_missing(self, store):
        """Pre-cache first half; second call should only fetch the gap."""
        calls = []

        def fetch_fn(start, end):
            calls.append((start, end))
            return []

        # Pre-populate first half
        store.get_bars(
            "GLD", "5 mins", "TRADES", True,
            start_dt=_dt(2024, 1, 1), end_dt=_dt(2024, 1, 16),
            fetch_fn=fetch_fn,
        )
        calls.clear()

        # Request full month — only second half should be fetched
        store.get_bars(
            "GLD", "5 mins", "TRADES", True,
            start_dt=_dt(2024, 1, 1), end_dt=_dt(2024, 1, 31),
            fetch_fn=fetch_fn,
        )
        assert len(calls) == 1
        # The fetched gap should start at or around Jan 16, not Jan 1
        assert calls[0][0] >= _dt(2024, 1, 15)


# ---------------------------------------------------------------------------
# BarStore — purge
# ---------------------------------------------------------------------------

class TestBarStorePurge:
    def test_purge_removes_bars_and_coverage(self, store):
        store.get_bars(
            "GLD", "5 mins", "TRADES", True,
            start_dt=_dt(2024, 1, 1), end_dt=_dt(2024, 1, 31),
            fetch_fn=lambda s, e: [_make_bar("20240102-14:30:00")],
        )
        n = store.purge("GLD", "5 mins", "TRADES", True)
        assert n == 1
        assert store.coverage_summary(symbol="GLD") == []

    def test_purge_does_not_affect_other_series(self, store):
        for sym in ("GLD", "UUP"):
            store.get_bars(
                sym, "5 mins", "TRADES", True,
                start_dt=_dt(2024, 1, 1), end_dt=_dt(2024, 1, 5),
                fetch_fn=lambda s, e: [_make_bar("20240102-14:30:00")],
            )
        store.purge("GLD", "5 mins", "TRADES", True)
        summary = store.coverage_summary(symbol="UUP")
        assert len(summary) == 1

    def test_purge_allows_refetch(self, store):
        call_count = [0]

        def fetch_fn(s, e):
            call_count[0] += 1
            return []

        kwargs = dict(
            symbol="GLD", bar_size="5 mins", what_to_show="TRADES", use_rth=True,
            start_dt=_dt(2024, 1, 1), end_dt=_dt(2024, 1, 5),
            fetch_fn=fetch_fn,
        )
        store.get_bars(**kwargs)
        store.purge("GLD", "5 mins", "TRADES", True)
        store.get_bars(**kwargs)
        assert call_count[0] == 2   # re-fetched after purge


# ---------------------------------------------------------------------------
# BarStore — bar date formats
# ---------------------------------------------------------------------------

class TestBarStoreDateFormats:
    def test_daily_bar_stored_and_retrieved(self, store):
        result = store.get_bars(
            "GLD", "1 day", "TRADES", True,
            start_dt=_dt(2024, 1, 1), end_dt=_dt(2024, 12, 31),
            fetch_fn=lambda s, e: [_make_bar("20240115")],
        )
        assert len(result) == 1

    def test_space_separator_bar(self, store):
        result = store.get_bars(
            "GLD", "5 mins", "TRADES", True,
            start_dt=_dt(2024, 1, 15), end_dt=_dt(2024, 1, 16),
            fetch_fn=lambda s, e: [_make_bar("20240115 09:30:00")],
        )
        assert len(result) == 1

    def test_hyphen_separator_bar(self, store):
        result = store.get_bars(
            "GLD", "5 mins", "TRADES", True,
            start_dt=_dt(2024, 1, 15), end_dt=_dt(2024, 1, 16),
            fetch_fn=lambda s, e: [_make_bar("20240115-09:30:00")],
        )
        assert len(result) == 1

    def test_bar_dt_orig_preserved(self, store):
        original = "20240115-14:30:00"
        result = store.get_bars(
            "GLD", "5 mins", "TRADES", True,
            start_dt=_dt(2024, 1, 15), end_dt=_dt(2024, 1, 16),
            fetch_fn=lambda s, e: [_make_bar(original)],
        )
        assert result[0].date == original


# ---------------------------------------------------------------------------
# BarStore — fetch_fn error handling
# ---------------------------------------------------------------------------

class TestFetchFnErrors:
    def test_fetch_fn_exception_returns_empty(self, store):
        def bad_fetch(s, e):
            raise RuntimeError("IB disconnected")

        result = store.get_bars(
            "GLD", "5 mins", "TRADES", True,
            start_dt=_dt(2024, 1, 1), end_dt=_dt(2024, 1, 5),
            fetch_fn=bad_fetch,
        )
        # Should return empty list, not raise
        assert result == []

    def test_fetch_fn_returns_none(self, store):
        result = store.get_bars(
            "GLD", "5 mins", "TRADES", True,
            start_dt=_dt(2024, 1, 1), end_dt=_dt(2024, 1, 5),
            fetch_fn=lambda s, e: None,
        )
        assert result == []


# ---------------------------------------------------------------------------
# _BAR_SECONDS lookup table
# ---------------------------------------------------------------------------

class TestBarSeconds:
    def test_5_mins(self):
        from ib.bar_store import _BAR_SECONDS
        assert _BAR_SECONDS["5 mins"] == 300

    def test_1_day(self):
        from ib.bar_store import _BAR_SECONDS
        assert _BAR_SECONDS["1 day"] == 86_400

    def test_1_hour(self):
        from ib.bar_store import _BAR_SECONDS
        assert _BAR_SECONDS["1 hour"] == 3_600

    def test_1_min(self):
        from ib.bar_store import _BAR_SECONDS
        assert _BAR_SECONDS["1 min"] == 60

    def test_all_values_positive(self):
        from ib.bar_store import _BAR_SECONDS
        for k, v in _BAR_SECONDS.items():
            assert v > 0, f"_BAR_SECONDS[{k!r}] should be positive"

    def test_keys_match_max_fetch_seconds_keys(self):
        """Every bar size in _MAX_FETCH_SECONDS should also appear in _BAR_SECONDS."""
        from ib.bar_store import _BAR_SECONDS, _MAX_FETCH_SECONDS
        missing = set(_MAX_FETCH_SECONDS) - set(_BAR_SECONDS)
        assert not missing, f"_BAR_SECONDS missing keys: {missing}"


# ---------------------------------------------------------------------------
# BarStore.insert_bar
# ---------------------------------------------------------------------------

class TestInsertBar:
    def test_bar_is_retrievable_after_insert(self, store):
        bar = _make_bar("20250101 10:00:00", close=185.0)
        store.insert_bar("GLD", "5 mins", "TRADES", True, bar)

        rows = store._query_bars(
            SeriesKey("GLD", "5 mins", "TRADES", 1),
            _dt(2025, 1, 1), _dt(2025, 1, 2),
        )
        assert len(rows) == 1
        assert rows[0].close == pytest.approx(185.0)

    def test_coverage_created_after_insert(self, store):
        bar = _make_bar("20250101 10:00:00", close=185.0)
        store.insert_bar("GLD", "5 mins", "TRADES", True, bar)

        summary = store.coverage_summary(symbol="GLD")
        assert len(summary) == 1
        assert len(summary[0]["intervals"]) >= 1

    def test_two_inserts_merge_coverage(self, store):
        store.insert_bar("GLD", "5 mins", "TRADES", True,
                         _make_bar("20250101 10:00:00", close=185.0))
        store.insert_bar("GLD", "5 mins", "TRADES", True,
                         _make_bar("20250101 10:05:00", close=186.0))

        cov = store._get_coverage(SeriesKey("GLD", "5 mins", "TRADES", 1))
        # Two adjacent 5-min bars → coverage windows overlap → merged to 1
        assert len(cov) == 1

    def test_different_symbols_kept_separate(self, store):
        store.insert_bar("GLD", "5 mins", "TRADES", True,
                         _make_bar("20250101 10:00:00", close=185.0))
        store.insert_bar("UUP", "5 mins", "TRADES", True,
                         _make_bar("20250101 10:00:00", close=28.5))

        assert len(store.coverage_summary(symbol="GLD")) == 1
        assert len(store.coverage_summary(symbol="UUP")) == 1

    def test_use_rth_false_stored_separately(self, store):
        store.insert_bar("GLD", "5 mins", "TRADES", True,
                         _make_bar("20250101 10:00:00", close=185.0))
        store.insert_bar("GLD", "5 mins", "TRADES", False,
                         _make_bar("20250101 10:00:00", close=186.0))

        rth  = store._get_coverage(SeriesKey("GLD", "5 mins", "TRADES", 1))
        full = store._get_coverage(SeriesKey("GLD", "5 mins", "TRADES", 0))
        assert len(rth)  == 1
        assert len(full) == 1

    def test_bad_date_is_silent_noop(self, store):
        bad = _make_bar("not-a-date", close=100.0)
        store.insert_bar("GLD", "5 mins", "TRADES", True, bad)  # must not raise
        assert store.coverage_summary(symbol="GLD") == []

    def test_unknown_bar_size_uses_default_duration(self, store):
        bar = _make_bar("20250101 10:00:00", close=100.0)
        store.insert_bar("GLD", "weird size", "TRADES", True, bar)  # must not raise
        assert len(store.coverage_summary(symbol="GLD")) == 1

    def test_exception_in_store_bars_is_silent(self, store, monkeypatch):
        def boom(*a, **kw):
            raise RuntimeError("storage failure")
        monkeypatch.setattr(store, "_store_bars", boom)
        bar = _make_bar("20250101 10:00:00", close=100.0)
        store.insert_bar("GLD", "5 mins", "TRADES", True, bar)  # must not raise
