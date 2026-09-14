"""
Unit tests for ib/market_calendar.py

Covers hours-string parsing (both IB formats), state/holiday/half-day
verdicts, the past-state gap classifier, JSON round-tripping, and the two
IB fetch helpers against a stand-in portfolio.
"""

from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from ib.market_calendar import (
    MarketCalendar,
    MarketState,
    Session,
    SessionSet,
    fetch_historical_schedule,
    fetch_market_calendar,
    load_or_fetch,
    resolve_tz,
)

NY = ZoneInfo("America/New_York")
UTC = timezone.utc
TZ = "America/New_York"

# A July-4th week: normal Thursday, 1 PM close on the 3rd, holiday, weekend.
LIQUID = (
    "20260702:0930-20260702:1600;"
    "20260703:0930-20260703:1300;"
    "20260704:CLOSED;"
    "20260705:CLOSED;"
    "20260706:CLOSED;"
    "20260707:0930-20260707:1600"
)
TRADING = (
    "20260702:0400-20260702:2000;"
    "20260703:0400-20260703:1700;"
    "20260704:CLOSED;"
    "20260705:CLOSED;"
    "20260706:CLOSED;"
    "20260707:0400-20260707:2000"
)


def _ny(y, m, d, hh=0, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=NY)


def _hist_session(ref, start, end):
    return SimpleNamespace(refDate=ref, startDateTime=start, endDateTime=end)


def _calendar():
    return MarketCalendar(
        symbol="SPY",
        exchange="SMART",
        sec_type="STK",
        time_zone_id=TZ,
        trading=SessionSet.from_ib_string(TRADING, TZ),
        liquid=SessionSet.from_ib_string(LIQUID, TZ),
        fetched_at=datetime.now(UTC).isoformat(),
    )


class _FakeContractDetails:
    def __init__(self, trading=TRADING, liquid=LIQUID, tz=TZ):
        self.tradingHours = trading
        self.liquidHours = liquid
        self.timeZoneId = tz
        self.contract = SimpleNamespace(
            symbol="SPY", exchange="SMART", secType="STK", conId=756733
        )


class _FakePortfolio:
    """Minimal stand-in: the two request methods market_calendar uses."""

    def __init__(self, details=None, respond=True):
        self.details = details if details is not None else [_FakeContractDetails()]
        self.respond = respond
        self.historical_calls = []
        self.schedule_payload = None
        self._next_req_id = 100

    def request_contract_details(self, contract, on_details=None, on_end=None):
        self._next_req_id += 1
        if self.respond and on_end:
            on_end(self.details)
        return self._next_req_id

    def request_historical_data(self, **kwargs):
        self._next_req_id += 1
        req_id = self._next_req_id
        self.historical_calls.append(kwargs)
        if self.respond and self.schedule_payload is not None:
            start, end, tz, sessions = self.schedule_payload
            self.historicalSchedule(req_id, start, end, tz, sessions)
        return req_id

    def historicalSchedule(self, reqId, startDateTime, endDateTime, timeZone, sessions):
        """Class-level default, as EWrapper has — must survive the fetch."""
        return None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

class TestParsing:
    def test_modern_format(self):
        s = SessionSet.from_ib_string(LIQUID, TZ)
        assert s.sessions_on(date(2026, 7, 2)) == [
            Session(_ny(2026, 7, 2, 9, 30), _ny(2026, 7, 2, 16, 0))
        ]
        assert date(2026, 7, 4) in s.closed_days

    def test_legacy_format_time_only(self):
        s = SessionSet.from_ib_string("20090507:0700-1830;20090508:CLOSED", TZ)
        assert s.sessions_on(date(2009, 5, 7)) == [
            Session(_ny(2009, 5, 7, 7, 0), _ny(2009, 5, 7, 18, 30))
        ]
        assert s.closed_days == [date(2009, 5, 8)]

    def test_legacy_multiple_sessions_in_one_day(self):
        s = SessionSet.from_ib_string("20260702:0700-1130,1300-1830", TZ)
        assert len(s.sessions_on(date(2026, 7, 2))) == 2
        assert s.open_seconds_on(date(2026, 7, 2)) == (4.5 + 5.5) * 3600

    def test_session_crossing_midnight_without_explicit_date(self):
        s = SessionSet.from_ib_string("20260702:1800-1700", TZ)
        sess = s.sessions_on(date(2026, 7, 2))[0]
        assert sess.end == _ny(2026, 7, 3, 17, 0)
        # And it is genuinely open at 02:00 the next calendar morning.
        assert s.is_open(_ny(2026, 7, 3, 2, 0))

    def test_explicit_next_day_end(self):
        s = SessionSet.from_ib_string("20260702:1800-20260703:1700", TZ)
        assert s.sessions_on(date(2026, 7, 2))[0].end == _ny(2026, 7, 3, 17, 0)

    def test_2400_is_end_of_day(self):
        s = SessionSet.from_ib_string("20260702:1800-2400", TZ)
        assert s.sessions_on(date(2026, 7, 2))[0].end == _ny(2026, 7, 3, 0, 0)

    def test_malformed_chunk_is_skipped_not_fatal(self):
        s = SessionSet.from_ib_string("garbage;20260702:0930-1600;20260703:zzzz-1600", TZ)
        assert s.sessions_on(date(2026, 7, 2))
        assert not s.sessions_on(date(2026, 7, 3))

    def test_empty_string_is_falsy(self):
        assert not SessionSet.from_ib_string("", TZ)

    def test_long_overnight_session_wins_over_nearer_starts(self):
        # Futures-shaped: a nearly-24h span dated Monday, with later-starting
        # sessions in between. A lookup at 03:00 Tuesday must still find it,
        # not stop at the nearest preceding start.
        s = SessionSet.from_ib_string(
            "20260706:1800-20260707:1700;"
            "20260707:0930-20260707:1600;"
            "20260707:0400-20260707:0930",
            TZ,
        )
        sess = s.session_at(_ny(2026, 7, 7, 3, 0))
        assert sess is not None
        assert sess.start == _ny(2026, 7, 6, 18, 0)


class TestTimezone:
    def test_iana_id(self):
        tz, fell_back = resolve_tz("America/New_York")
        assert fell_back is False
        assert tz.key == "America/New_York"

    def test_ib_abbreviation_is_mapped_to_a_dst_aware_zone(self):
        # tzdata's own "EST" is fixed UTC-5, which would be an hour wrong all
        # summer; the alias table has to win here.
        tz, fell_back = resolve_tz("EST")
        assert fell_back is False
        assert datetime(2026, 7, 2, 12, tzinfo=tz).utcoffset() == timedelta(hours=-4)

    def test_legacy_zone_name_resolves_without_tzdata_backward_links(self):
        # This is what a live gateway actually sends for US equities, and
        # plenty of installs (this one included) have no US/ directory in
        # /usr/share/zoneinfo — a direct ZoneInfo() would raise and drop
        # every session time into UTC.
        tz, fell_back = resolve_tz("US/Eastern")
        assert fell_back is False
        assert datetime(2026, 7, 2, 12, tzinfo=tz).utcoffset() == timedelta(hours=-4)
        assert datetime(2026, 1, 2, 12, tzinfo=tz).utcoffset() == timedelta(hours=-5)

    def test_other_legacy_names(self):
        for name, july_offset in (
            ("Japan", 9), ("Hongkong", 8), ("US/Pacific", -7), ("GMT", 0),
        ):
            tz, fell_back = resolve_tz(name)
            assert fell_back is False, name
            assert datetime(2026, 7, 2, 12, tzinfo=tz).utcoffset() == timedelta(
                hours=july_offset
            ), name

    def test_unknown_id_falls_back_and_says_so(self):
        tz, fell_back = resolve_tz("Mars/Olympus")
        assert fell_back is True
        assert tz is UTC


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class TestState:
    def test_open_during_session(self):
        cal = _calendar()
        assert cal.is_open(_ny(2026, 7, 2, 11, 0)) is True
        assert cal.state_at(_ny(2026, 7, 2, 11, 0)) is MarketState.OPEN

    def test_closing_instant_is_not_open(self):
        assert _calendar().is_open(_ny(2026, 7, 2, 16, 0)) is False

    def test_closed_outside_session_on_a_trading_day(self):
        assert _calendar().state_at(_ny(2026, 7, 2, 8, 0)) is MarketState.CLOSED

    def test_holiday(self):
        assert _calendar().state_at(_ny(2026, 7, 4, 11, 0)) is MarketState.HOLIDAY

    def test_early_close_is_closed_not_open(self):
        cal = _calendar()
        assert cal.is_open(_ny(2026, 7, 3, 14, 0)) is False
        assert cal.state_at(_ny(2026, 7, 3, 14, 0)) is MarketState.CLOSED

    def test_extended_hours_still_open_after_the_rth_close(self):
        cal = _calendar()
        at = _ny(2026, 7, 2, 18, 0)
        assert cal.is_open(at, rth=True) is False
        assert cal.is_open(at, rth=False) is True

    def test_outside_coverage_is_unknown_not_closed(self):
        cal = _calendar()
        assert cal.state_at(_ny(2026, 8, 12, 11, 0)) is MarketState.UNKNOWN
        assert cal.was_open(_ny(2026, 8, 12, 11, 0)) is None

    def test_was_open_is_tri_valued(self):
        cal = _calendar()
        assert cal.was_open(_ny(2026, 7, 2, 11, 0)) is True
        assert cal.was_open(_ny(2026, 7, 4, 11, 0)) is False
        assert cal.was_open(_ny(2025, 1, 2, 11, 0)) is None

    def test_utc_input_is_converted(self):
        # 15:00 UTC is 11:00 ET in July.
        assert _calendar().is_open(datetime(2026, 7, 2, 15, 0, tzinfo=UTC)) is True

    def test_naive_input_is_exchange_local(self):
        assert _calendar().is_open(datetime(2026, 7, 2, 11, 0)) is True

    def test_liquid_falls_back_to_trading_when_absent(self):
        cd = _FakeContractDetails(liquid="")
        cal = MarketCalendar.from_contract_details(cd)
        assert cal.is_open(_ny(2026, 7, 2, 18, 0), rth=True) is True


class TestDayQueries:
    def test_half_day_detected(self):
        s = SessionSet.from_ib_string(LIQUID, TZ)
        assert s.is_half_day(date(2026, 7, 3)) is True
        assert s.is_half_day(date(2026, 7, 2)) is False

    def test_half_day_unknown_for_a_closed_day(self):
        assert SessionSet.from_ib_string(LIQUID, TZ).is_half_day(date(2026, 7, 4)) is None

    def test_next_open_skips_the_long_weekend(self):
        s = SessionSet.from_ib_string(LIQUID, TZ)
        assert s.next_open(_ny(2026, 7, 3, 13, 30)) == _ny(2026, 7, 7, 9, 30)

    def test_next_close_inside_a_session(self):
        s = SessionSet.from_ib_string(LIQUID, TZ)
        assert s.next_close(_ny(2026, 7, 3, 10, 0)) == _ny(2026, 7, 3, 13, 0)

    def test_previous_close(self):
        s = SessionSet.from_ib_string(LIQUID, TZ)
        assert s.previous_close(_ny(2026, 7, 6, 9, 0)) == _ny(2026, 7, 3, 13, 0)

    def test_trading_days_between_excludes_holidays(self):
        cal = _calendar()
        days = cal.trading_days_between(date(2026, 7, 2), date(2026, 7, 7))
        assert days == [date(2026, 7, 2), date(2026, 7, 3), date(2026, 7, 7)]

    def test_covered_range(self):
        assert SessionSet.from_ib_string(LIQUID, TZ).covered_range() == (
            date(2026, 7, 2), date(2026, 7, 7)
        )

    def test_describe_reports_session_and_countdown(self):
        d = _calendar().describe(_ny(2026, 7, 3, 12, 30))
        assert d["state"] == "open"
        assert d["is_half_day"] is True
        assert d["seconds_to_close"] == 1800.0
        assert d["session_end"].startswith("2026-07-03T13:00")
        assert d["tz_fallback"] is False


# ---------------------------------------------------------------------------
# Past state — the gap classifier
# ---------------------------------------------------------------------------

class TestClassifyGap:
    def test_silence_over_a_holiday_weekend_is_explained(self):
        r = _calendar().classify_gap(_ny(2026, 7, 3, 13, 0), _ny(2026, 7, 7, 9, 30))
        assert r["verdict"] == "closed"
        assert r["open_seconds"] == 0.0

    def test_silence_inside_a_session_is_a_real_outage(self):
        r = _calendar().classify_gap(_ny(2026, 7, 2, 11, 0), _ny(2026, 7, 2, 12, 30))
        assert r["verdict"] == "open"
        assert r["open_seconds"] == 5400.0
        assert r["open_fraction"] == 1.0

    def test_partial_overlap_reports_only_the_open_part(self):
        # 90 minutes of silence around the early close: only 30 min tradeable.
        r = _calendar().classify_gap(_ny(2026, 7, 3, 12, 30), _ny(2026, 7, 3, 14, 0))
        assert r["verdict"] == "partial"
        assert r["open_seconds"] == 1800.0
        assert r["open_windows"] == [
            {"start": _ny(2026, 7, 3, 12, 30).isoformat(),
             "end": _ny(2026, 7, 3, 13, 0).isoformat()}
        ]

    def test_gap_outside_coverage_is_unknown(self):
        r = _calendar().classify_gap(_ny(2026, 8, 3, 11, 0), _ny(2026, 8, 3, 12, 0))
        assert r["verdict"] == "unknown"
        assert "does not cover" in r["explanation"]

    def test_extended_hours_view_of_the_same_gap(self):
        # 16:30-17:30 on the 2nd is shut for RTH, open in extended hours.
        cal = _calendar()
        window = (_ny(2026, 7, 2, 16, 30), _ny(2026, 7, 2, 17, 30))
        assert cal.classify_gap(*window, rth=True)["verdict"] == "closed"
        assert cal.classify_gap(*window, rth=False)["verdict"] == "open"

    def test_open_seconds_between_spans_multiple_sessions(self):
        s = SessionSet.from_ib_string(LIQUID, TZ)
        total = s.open_seconds_between(_ny(2026, 7, 2, 0, 0), _ny(2026, 7, 8, 0, 0))
        assert total == (6.5 + 3.5 + 6.5) * 3600


# ---------------------------------------------------------------------------
# Merging the two IB sources
# ---------------------------------------------------------------------------

class TestMerge:
    """Contract details run forward from today; SCHEDULE runs backward and
    stops at the last day that traded. Only merged do they span 'now'."""

    def _past(self):
        return SessionSet.from_historical_sessions(
            [
                _hist_session("20260901", "20260901-09:30:00", "20260901-16:00:00"),
                _hist_session("20260902", "20260902-09:30:00", "20260902-16:00:00"),
            ],
            TZ,
            start_date=date(2026, 8, 31),
            end_date=date(2026, 9, 2),
        )

    def _future(self):
        return SessionSet.from_ib_string(
            "20260903:0930-20260903:1600;20260904:CLOSED", TZ
        )

    def test_union_spans_both_sides(self):
        merged = self._future().merged_with(self._past())
        assert merged.covered_range() == (date(2026, 8, 31), date(2026, 9, 4))
        assert merged.is_open(_ny(2026, 9, 1, 11, 0)) is True     # from history
        assert merged.is_open(_ny(2026, 9, 3, 11, 0)) is True     # from details
        assert merged.state_at(_ny(2026, 8, 31, 11, 0)) is MarketState.HOLIDAY
        assert merged.state_at(_ny(2026, 9, 4, 11, 0)) is MarketState.HOLIDAY

    def test_other_wins_on_a_shared_day(self):
        # An early close learned from history replaces the details' full day.
        details = SessionSet.from_ib_string("20260901:0930-20260901:1600", TZ)
        history = SessionSet.from_historical_sessions(
            [_hist_session("20260901", "20260901-09:30:00", "20260901-13:00:00")], TZ
        )
        merged = details.merged_with(history)
        assert merged.is_open(_ny(2026, 9, 1, 14, 0)) is False

    def test_history_closed_day_overrides_a_session(self):
        details = SessionSet.from_ib_string("20260901:0930-20260901:1600", TZ)
        history = SessionSet.from_historical_sessions(
            [], TZ, start_date=date(2026, 9, 1), end_date=date(2026, 9, 1)
        )
        merged = details.merged_with(history)
        assert merged.state_at(_ny(2026, 9, 1, 11, 0)) is MarketState.HOLIDAY

    def test_with_history_makes_a_cross_weekend_gap_answerable(self):
        cal = MarketCalendar(
            symbol="SPY", time_zone_id=TZ,
            trading=self._future(), liquid=self._future(),
        )
        window = (_ny(2026, 9, 1, 15, 55), _ny(2026, 9, 3, 9, 35))
        assert cal.classify_gap(*window)["verdict"] == "unknown"

        extended = cal.with_history(self._past(), rth=True)
        report = extended.classify_gap(*window)
        # 5 min before Tuesday's close, 6.5h Wednesday, 5 min Thursday.
        assert report["verdict"] == "partial"
        assert report["open_seconds"] == 300 + 6.5 * 3600 + 300
        assert extended.source.endswith("+schedule")

    def test_with_history_leaves_the_other_hours_set_alone(self):
        cal = MarketCalendar(
            symbol="SPY", time_zone_id=TZ,
            trading=SessionSet.from_ib_string(TRADING, TZ),
            liquid=SessionSet.from_ib_string(LIQUID, TZ),
        )
        extended = cal.with_history(self._past(), rth=True)
        assert extended.trading.covered_range() == (date(2026, 7, 2), date(2026, 7, 7))
        assert extended.liquid.covers(_ny(2026, 9, 1, 11, 0))


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

class TestPersistence:
    def test_round_trip_preserves_verdicts(self):
        cal = _calendar()
        restored = MarketCalendar.from_dict(cal.to_dict())
        assert restored.symbol == "SPY"
        assert restored.is_open(_ny(2026, 7, 2, 11, 0)) is True
        assert restored.state_at(_ny(2026, 7, 4, 11, 0)) is MarketState.HOLIDAY
        assert restored.liquid.is_half_day(date(2026, 7, 3)) is True

    def test_save_and_load(self, tmp_path):
        path = tmp_path / "cache" / "spy.json"
        _calendar().save(path)
        loaded = MarketCalendar.load(path)
        assert loaded is not None
        assert loaded.is_open(_ny(2026, 7, 2, 11, 0)) is True

    def test_load_missing_file_returns_none(self, tmp_path):
        assert MarketCalendar.load(tmp_path / "nope.json") is None

    def test_load_corrupt_file_returns_none(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("{not json")
        assert MarketCalendar.load(path) is None

    def test_age_seconds(self):
        cal = _calendar()
        cal.fetched_at = (datetime.now(UTC) - timedelta(hours=3)).isoformat()
        assert 10700 < cal.age_seconds() < 10900


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

class TestFetchMarketCalendar:
    def test_builds_a_calendar_from_contract_details(self):
        cal = fetch_market_calendar(_FakePortfolio(), SimpleNamespace(symbol="SPY"))
        assert cal is not None
        assert cal.symbol == "SPY"
        assert cal.con_id == 756733
        assert cal.is_open(_ny(2026, 7, 2, 11, 0)) is True

    def test_timeout_returns_none(self):
        pf = _FakePortfolio(respond=False)
        assert fetch_market_calendar(pf, SimpleNamespace(symbol="SPY"), timeout=0.05) is None

    def test_no_details_returns_none(self):
        assert fetch_market_calendar(_FakePortfolio(details=[]), SimpleNamespace()) is None

    def test_request_error_returns_none(self):
        class Boom(_FakePortfolio):
            def request_contract_details(self, **kwargs):
                raise RuntimeError("not connected")

        assert fetch_market_calendar(Boom(), SimpleNamespace()) is None


class TestLoadOrFetch:
    def test_fresh_cache_is_used_without_a_request(self, tmp_path):
        path = tmp_path / "spy.json"
        _calendar().save(path)
        pf = _FakePortfolio(respond=False)          # would time out if called
        cal = load_or_fetch(pf, SimpleNamespace(), path, max_age_hours=24)
        assert cal is not None and cal.symbol == "SPY"

    def test_stale_cache_triggers_a_refetch_and_rewrite(self, tmp_path):
        path = tmp_path / "spy.json"
        stale = _calendar()
        stale.fetched_at = (datetime.now(UTC) - timedelta(days=3)).isoformat()
        stale.save(path)
        cal = load_or_fetch(_FakePortfolio(), SimpleNamespace(), path, max_age_hours=12)
        assert cal is not None
        assert cal.age_seconds() < 60
        assert MarketCalendar.load(path).age_seconds() < 60

    def test_failed_refetch_falls_back_to_the_stale_cache(self, tmp_path):
        path = tmp_path / "spy.json"
        stale = _calendar()
        stale.fetched_at = (datetime.now(UTC) - timedelta(days=3)).isoformat()
        stale.save(path)
        cal = load_or_fetch(
            _FakePortfolio(respond=False), SimpleNamespace(), path,
            max_age_hours=12, timeout=0.05,
        )
        assert cal is not None and cal.symbol == "SPY"

    def test_no_cache_and_failed_fetch_returns_none(self, tmp_path):
        cal = load_or_fetch(
            _FakePortfolio(respond=False), SimpleNamespace(), tmp_path / "x.json",
            timeout=0.05,
        )
        assert cal is None


class TestFetchHistoricalSchedule:
    def _portfolio(self):
        pf = _FakePortfolio()
        pf.schedule_payload = (
            "20260702-00:00:00",
            "20260707-00:00:00",
            TZ,
            [
                _hist_session("20260702", "20260702-09:30:00", "20260702-16:00:00"),
                _hist_session("20260703", "20260703-09:30:00", "20260703-13:00:00"),
                # 4th-6th absent: that absence IS the holiday signal.
                _hist_session("20260707", "20260707-09:30:00", "20260707-16:00:00"),
            ],
        )
        return pf

    def test_parses_sessions_and_infers_the_closed_days(self):
        s = fetch_historical_schedule(self._portfolio(), SimpleNamespace(symbol="SPY"))
        assert s is not None
        assert s.is_open(_ny(2026, 7, 2, 11, 0)) is True
        assert s.state_at(_ny(2026, 7, 4, 11, 0)) is MarketState.HOLIDAY
        assert s.is_half_day(date(2026, 7, 3)) is True

    def test_requests_schedule_not_bars(self):
        pf = self._portfolio()
        fetch_historical_schedule(pf, SimpleNamespace(symbol="SPY"), num_days=45)
        call = pf.historical_calls[0]
        assert call["what_to_show"] == "SCHEDULE"
        assert call["bar_size_setting"] == "1 day"
        assert call["duration_str"] == "45 D"

    def test_restores_the_wrapper_callback_afterwards(self):
        pf = self._portfolio()
        fetch_historical_schedule(pf, SimpleNamespace(symbol="SPY"))
        assert "historicalSchedule" not in pf.__dict__
        assert pf.historicalSchedule(1, "", "", TZ, []) is None

    def test_restores_a_pre_existing_instance_handler(self):
        pf = self._portfolio()
        sentinel = lambda *a: "mine"
        pf.historicalSchedule = sentinel
        fetch_historical_schedule(pf, SimpleNamespace(symbol="SPY"))
        assert pf.historicalSchedule is sentinel

    def test_timeout_returns_none_and_still_restores(self):
        pf = _FakePortfolio(respond=False)
        assert fetch_historical_schedule(pf, SimpleNamespace(), timeout=0.05) is None
        assert "historicalSchedule" not in pf.__dict__

    def test_request_error_returns_none(self):
        class Boom(_FakePortfolio):
            def request_historical_data(self, **kwargs):
                raise RuntimeError("not connected")

        pf = Boom()
        assert fetch_historical_schedule(pf, SimpleNamespace(), timeout=0.05) is None
        assert "historicalSchedule" not in pf.__dict__
