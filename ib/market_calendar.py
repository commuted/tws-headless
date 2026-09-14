"""
ib/market_calendar.py — "is this market open?", answered out of IB's own data.

WHY THIS EXISTS. There is no reqMarketStatus() in the TWS API and no holiday
endpoint; reqMarketHours() and reqHolidayList() do not exist despite what
third-party articles claim. What IB does give is the schedule attached to a
CONTRACT, and everything here is built on the two ways of getting it:

  1. reqContractDetails  -> ContractDetails.tradingHours / .liquidHours /
     .timeZoneId. Covers roughly the next month (TWS API setting), including
     the days IB marks CLOSED, which is how holidays and weekends show up.

  2. reqHistoricalData(whatToShow="SCHEDULE", barSize="1 day") -> the
     historicalSchedule() callback, one session per trading day over a date
     RANGE. This is the one that answers questions about the PAST.

The parsing half of this module is pure: give it an hours string and a
timezone id and it will tell you the state at any instant, with no IB
connection anywhere near it. That is deliberate — the schedule is worth
caching to JSON once a day and answering from memory thereafter, which is
also how the polling pattern IB recommends works.

WHAT THE PAST-STATE PART IS FOR. Knowing the market was shut is the
difference between two identical-looking observations:

    "no bars for 90 minutes"  on a half-day session that closed at 13:00 ET
    "no bars for 90 minutes"  at 11:00 ET on an ordinary Tuesday

The first is normal and the second is a dead feed. Today the watchdog's
_in_rth() and every other 09:30-16:00 weekday test in this repo cannot tell
them apart: they hard-code the hours and explicitly do not model holidays or
early closes. classify_gap() below answers the question properly, from the
exchange's own calendar, for any window the cached schedule covers — and
says UNKNOWN rather than guessing when it does not cover it.

NOT INTEGRATED. Nothing imports this module. It places no orders, opens no
connection of its own, and changes no existing behaviour. The fetch helpers
take an already-connected Portfolio (or anything with the same two request
methods) so they can be driven from a plugin, a REPL, or a test.

Offline use:

    python3 -m ib.market_calendar --demo
    python3 -m ib.market_calendar --tz America/New_York \\
        --hours "20260703:0930-20260703:1300;20260704:CLOSED" --at 2026-07-03T12:00

From a plugin that already has a portfolio:

    from ib.contract_builder import ContractBuilder
    from ib.market_calendar import fetch_market_calendar

    cal = fetch_market_calendar(self.portfolio, ContractBuilder.etf("SPY"))
    if cal and cal.is_open():        # rth=True by default -> liquidHours
        ...
"""

from __future__ import annotations

import argparse
import json
import logging
import threading
import time as _time
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

try:
    from zoneinfo import ZoneInfo
except ImportError:                                    # pragma: no cover
    ZoneInfo = None                                    # type: ignore[assignment]

UTC = timezone.utc

# IB does not send canonical IANA names. Two distinct problems, both seen
# against a live gateway rather than imagined:
#
#   1. Legacy zone names. IB sends "US/Eastern" for every US equity. Those
#      names live in tzdata's "backward" file, and plenty of installs ship
#      without it — this machine's /usr/share/zoneinfo has no US/ directory
#      at all, so ZoneInfo("US/Eastern") raises and every session time would
#      silently land in UTC, four hours out.
#   2. Abbreviations that resolve to the WRONG thing. tzdata's "EST" is a
#      fixed UTC-5 with no DST; taking it literally puts every summer session
#      an hour off. These have to be overridden BEFORE the direct lookup.
_TZ_OVERRIDES = {
    "EST": "America/New_York",
    "CST": "America/Chicago",
    "MST": "America/Denver",
    "PST": "America/Los_Angeles",
}

_TZ_LEGACY = {
    "US/EASTERN": "America/New_York",
    "US/CENTRAL": "America/Chicago",
    "US/MOUNTAIN": "America/Denver",
    "US/PACIFIC": "America/Los_Angeles",
    "US/ARIZONA": "America/Phoenix",
    "US/ALASKA": "America/Anchorage",
    "US/HAWAII": "Pacific/Honolulu",
    "CANADA/EASTERN": "America/Toronto",
    "CANADA/CENTRAL": "America/Winnipeg",
    "CANADA/MOUNTAIN": "America/Edmonton",
    "CANADA/PACIFIC": "America/Vancouver",
    "MEXICO/GENERAL": "America/Mexico_City",
    "BRAZIL/EAST": "America/Sao_Paulo",
    "JAPAN": "Asia/Tokyo",
    "JST": "Asia/Tokyo",
    "HONGKONG": "Asia/Hong_Kong",
    "HKT": "Asia/Hong_Kong",
    "SINGAPORE": "Asia/Singapore",
    "PRC": "Asia/Shanghai",
    "ROK": "Asia/Seoul",
    "ROC": "Asia/Taipei",
    "ISRAEL": "Asia/Jerusalem",
    "ASIA/CALCUTTA": "Asia/Kolkata",
    "AUSTRALIA/NSW": "Australia/Sydney",
    "AUSTRALIA/QUEENSLAND": "Australia/Brisbane",
    "AUSTRALIA/VICTORIA": "Australia/Melbourne",
    "AUSTRALIA/WEST": "Australia/Perth",
    "AEST": "Australia/Sydney",
    "NZ": "Pacific/Auckland",
    "GB": "Europe/London",
    "GB-EIRE": "Europe/London",
    "EIRE": "Europe/Dublin",
    "POLAND": "Europe/Warsaw",
    "TURKEY": "Europe/Istanbul",
    "EGYPT": "Africa/Cairo",
    "GMT": "UTC",
    "GMT0": "UTC",
    "GREENWICH": "UTC",
    "UCT": "UTC",
    "UNIVERSAL": "UTC",
    "ZULU": "UTC",
}

# A day whose sessions total less than this fraction of the typical day is
# an early close (US half-days run 3.5h against 6.5h RTH, so 0.54).
_HALF_DAY_RATIO = 0.75


class MarketState(str, Enum):
    """What the calendar says about one instant.

    HOLIDAY is IB's own verdict — the day appears in the hours string marked
    CLOSED — and covers weekends as well as holidays; IB does not distinguish
    them and neither does this. CLOSED means the day trades but the instant
    falls outside its sessions (overnight, lunch break, after an early
    close). UNKNOWN means the cached schedule does not reach that date, which
    is a different claim from "shut" and must not be collapsed into one.
    """

    OPEN = "open"
    CLOSED = "closed"
    HOLIDAY = "holiday"
    UNKNOWN = "unknown"


# ---------------------------------------------------------------------------
# Timezone + string parsing (pure — no IB)
# ---------------------------------------------------------------------------

def resolve_tz(time_zone_id: str) -> Tuple[Any, bool]:
    """Resolve an IB timeZoneId to a tzinfo.

    Returns (tzinfo, fell_back). fell_back is True when the id could not be
    resolved and UTC was substituted — callers must surface that rather than
    quietly reporting session times that are hours wrong. describe() puts it
    in its report for exactly that reason.
    """
    raw = (time_zone_id or "").strip()
    if not raw:
        return UTC, True
    if ZoneInfo is None:                               # pragma: no cover
        logger.warning("zoneinfo unavailable; market calendar times fall back to UTC")
        return UTC, True

    key = raw.upper()
    for candidate in (_TZ_OVERRIDES.get(key), raw, _TZ_LEGACY.get(key)):
        if not candidate:
            continue
        try:
            return ZoneInfo(candidate), False
        except Exception:
            continue

    logger.warning(
        f"Unknown IB timeZoneId '{time_zone_id}'; falling back to UTC — session "
        f"times will be wrong by that zone's offset"
    )
    return UTC, True


def _parse_day_token(token: str) -> date:
    return datetime.strptime(token.strip(), "%Y%m%d").date()


def _parse_endpoint(token: str, fallback_day: date, tz) -> datetime:
    """Parse one side of a session range.

    Both IB formats turn up in the wild and a single string can mix them:

        legacy  20090507:0700-1830        (time only; day implied)
        modern  20250313:0930-20250313:1600

    A leading 8-digit group before a colon is a date; anything else is a
    time, with or without its colon separator. "2400"/"24:00" is a real IB
    value meaning midnight at the END of the day.
    """
    token = token.strip()
    day = fallback_day
    head, sep, tail = token.partition(":")
    if sep and len(head) == 8 and head.isdigit():
        day = _parse_day_token(head)
        token = tail.strip()
    digits = token.replace(":", "").strip()
    if len(digits) < 3 or not digits.isdigit():
        raise ValueError(f"unparseable time token: {token!r}")
    hour = int(digits[:-2])
    minute = int(digits[-2:])
    base = datetime(day.year, day.month, day.day, tzinfo=tz)
    return base + timedelta(hours=hour, minutes=minute)


@dataclass(frozen=True)
class Session:
    """One continuous stretch of trading, in exchange-local time."""

    start: datetime
    end: datetime

    @property
    def duration_seconds(self) -> float:
        return (self.end - self.start).total_seconds()

    def contains(self, dt: datetime) -> bool:
        """Half-open: the closing instant itself is not open (16:00 is shut)."""
        return self.start <= dt < self.end

    def to_dict(self) -> Dict[str, str]:
        return {"start": self.start.isoformat(), "end": self.end.isoformat()}


@dataclass
class SessionSet:
    """A parsed schedule: sessions by trading date, plus the days IB called CLOSED.

    Sessions are keyed by IB's trading DATE, which is not always the calendar
    date of the wall-clock times inside it — a futures session dated Monday
    can start 18:00 Sunday and run to 17:00 Monday. Point lookups therefore
    go through a flat time-ordered list, and only the per-day questions
    (is_half_day, sessions_on) use the date keys.
    """

    time_zone_id: str = ""
    tz: Any = UTC
    tz_fallback: bool = False
    days: Dict[date, List[Session]] = field(default_factory=dict)
    closed_days: List[date] = field(default_factory=list)

    # -- construction ------------------------------------------------------

    @classmethod
    def from_ib_string(cls, hours: str, time_zone_id: str) -> "SessionSet":
        """Parse a tradingHours/liquidHours string.

        Unparseable day entries are logged and skipped rather than aborting
        the whole schedule: one malformed day should cost you that day, not
        the month.
        """
        tz, fell_back = resolve_tz(time_zone_id)
        out = cls(time_zone_id=time_zone_id, tz=tz, tz_fallback=fell_back)
        for chunk in (hours or "").split(";"):
            chunk = chunk.strip()
            if not chunk:
                continue
            head, sep, spec = chunk.partition(":")
            if not sep or len(head) != 8 or not head.isdigit():
                logger.debug(f"market_calendar: skipping hours chunk {chunk!r}")
                continue
            try:
                day = _parse_day_token(head)
            except ValueError:
                logger.debug(f"market_calendar: bad date in chunk {chunk!r}")
                continue
            if spec.strip().upper() == "CLOSED":
                out.mark_closed(day)
                continue
            for part in spec.split(","):
                part = part.strip()
                if not part:
                    continue
                start_tok, dash, end_tok = part.partition("-")
                if not dash:
                    logger.debug(f"market_calendar: skipping session {part!r}")
                    continue
                try:
                    start = _parse_endpoint(start_tok, day, tz)
                    end = _parse_endpoint(end_tok, day, tz)
                except ValueError as exc:
                    logger.debug(f"market_calendar: {exc} in chunk {chunk!r}")
                    continue
                if end <= start:
                    # Legacy form with no date on the far side: the session
                    # runs through midnight into the next calendar day.
                    end += timedelta(days=1)
                out.add_session(day, Session(start, end))
        out._reindex()
        return out

    @classmethod
    def from_historical_sessions(
        cls,
        sessions: Sequence[Any],
        time_zone_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> "SessionSet":
        """Build from the historicalSchedule() callback's HistoricalSession list.

        SCHEDULE responses list only the days that traded — a holiday is
        simply absent. Every date in [start_date, end_date] with no session
        is therefore inferred CLOSED, which is the whole reason to ask for a
        range: absence is the holiday signal.
        """
        tz, fell_back = resolve_tz(time_zone_id)
        out = cls(time_zone_id=time_zone_id, tz=tz, tz_fallback=fell_back)
        for s in sessions or []:
            start_s = getattr(s, "startDateTime", None) or getattr(s, "start", "")
            end_s = getattr(s, "endDateTime", None) or getattr(s, "end", "")
            ref_s = getattr(s, "refDate", None) or getattr(s, "ref_date", "")
            start = _parse_schedule_dt(start_s, tz)
            end = _parse_schedule_dt(end_s, tz)
            if start is None or end is None:
                logger.debug(f"market_calendar: unparseable schedule session {s!r}")
                continue
            ref = _parse_schedule_dt(ref_s, tz)
            day = ref.date() if ref else start.date()
            out.add_session(day, Session(start, end))
        if start_date and end_date:
            cursor = start_date
            while cursor <= end_date:
                if cursor not in out.days:
                    out.mark_closed(cursor)
                cursor += timedelta(days=1)
        out._reindex()
        return out

    def add_session(self, day: date, session: Session) -> None:
        self.days.setdefault(day, []).append(session)

    def mark_closed(self, day: date) -> None:
        if day not in self.closed_days:
            self.closed_days.append(day)

    def _reindex(self) -> None:
        for day in self.days:
            self.days[day].sort(key=lambda s: s.start)
        self._flat: List[Session] = sorted(
            (s for sessions in self.days.values() for s in sessions),
            key=lambda s: s.start,
        )
        self._starts: List[datetime] = [s.start for s in self._flat]
        # Longest session on record: how far back a point lookup has to scan
        # before it can be sure no earlier session still contains the instant.
        self._max_span: timedelta = timedelta(
            seconds=max((s.duration_seconds for s in self._flat), default=0.0)
        )

    def __post_init__(self) -> None:
        self._reindex()

    # -- coverage ----------------------------------------------------------

    def __bool__(self) -> bool:
        return bool(self.days or self.closed_days)

    def covered_range(self) -> Optional[Tuple[date, date]]:
        """First and last date the schedule says anything about."""
        known = list(self.days.keys()) + list(self.closed_days)
        if not known:
            return None
        return min(known), max(known)

    def localize(self, dt: datetime) -> datetime:
        """Naive input is taken as exchange-local; aware input is converted."""
        if dt.tzinfo is None:
            return dt.replace(tzinfo=self.tz)
        return dt.astimezone(self.tz)

    def covers(self, dt: datetime) -> bool:
        rng = self.covered_range()
        if rng is None:
            return False
        return rng[0] <= self.localize(dt).date() <= rng[1]

    # -- point queries -----------------------------------------------------

    def session_at(self, dt: datetime) -> Optional[Session]:
        """The session containing dt, or None."""
        local = self.localize(dt)
        idx = bisect_right(self._starts, local)
        # Walk back from the nearest start, not just one entry: an overnight
        # or overlapping session that began well before `local` can still be
        # the one containing it. The longest known session bounds the walk.
        floor = local - self._max_span
        for i in range(idx - 1, -1, -1):
            candidate = self._flat[i]
            if candidate.start < floor:
                break
            if candidate.contains(local):
                return candidate
        return None

    def state_at(self, dt: datetime) -> MarketState:
        local = self.localize(dt)
        if self.session_at(local) is not None:
            return MarketState.OPEN
        if not self.covers(local):
            return MarketState.UNKNOWN
        day = local.date()
        if day in self.days:
            return MarketState.CLOSED
        if day in self.closed_days:
            return MarketState.HOLIDAY
        # Inside the covered range but the day is missing entirely: the
        # schedule has a hole, and saying so beats inventing a verdict.
        return MarketState.UNKNOWN

    def is_open(self, dt: datetime) -> bool:
        return self.state_at(dt) is MarketState.OPEN

    def next_open(self, after: datetime) -> Optional[datetime]:
        """Start of the first session beginning strictly after `after`."""
        local = self.localize(after)
        idx = bisect_right(self._starts, local)
        return self._flat[idx].start if idx < len(self._flat) else None

    def next_close(self, after: datetime) -> Optional[datetime]:
        """End of the session containing `after`, else the next session's end."""
        local = self.localize(after)
        current = self.session_at(local)
        if current is not None:
            return current.end
        for s in self._flat:
            if s.end > local:
                return s.end
        return None

    def previous_close(self, before: datetime) -> Optional[datetime]:
        local = self.localize(before)
        ends = [s.end for s in self._flat if s.end <= local]
        return max(ends) if ends else None

    # -- day queries -------------------------------------------------------

    def sessions_on(self, day: date) -> List[Session]:
        return list(self.days.get(day, []))

    def open_seconds_on(self, day: date) -> float:
        return sum(s.duration_seconds for s in self.days.get(day, []))

    def typical_open_seconds(self) -> Optional[float]:
        totals = [self.open_seconds_on(d) for d in self.days]
        totals = [t for t in totals if t > 0]
        return median(totals) if totals else None

    def is_half_day(self, day: date) -> Optional[bool]:
        """True if `day` trades noticeably shorter than the typical day.

        None when the day is unknown or there is nothing to compare against.
        Early closes are the case that quietly breaks a hard-coded 16:00
        assumption, so it is worth naming separately from HOLIDAY.
        """
        if day not in self.days:
            return None
        typical = self.typical_open_seconds()
        if not typical:
            return None
        return self.open_seconds_on(day) < _HALF_DAY_RATIO * typical

    # -- range queries (the past-state half) -------------------------------

    def sessions_between(self, start: datetime, end: datetime) -> List[Session]:
        """Sessions overlapping [start, end), clipped to that window."""
        lo, hi = self.localize(start), self.localize(end)
        if hi <= lo:
            return []
        out = []
        for s in self._flat:
            if s.end <= lo:
                continue
            if s.start >= hi:
                break
            out.append(Session(max(s.start, lo), min(s.end, hi)))
        return out

    def open_seconds_between(self, start: datetime, end: datetime) -> float:
        return sum(s.duration_seconds for s in self.sessions_between(start, end))

    # -- combining ---------------------------------------------------------

    def merged_with(self, other: "SessionSet") -> "SessionSet":
        """Union of two schedules; `other` wins on any date both describe.

        The two IB sources cover disjoint spans — contract details run from
        today forward, a SCHEDULE request runs backward and stops at the last
        day that traded. Neither alone can answer "was the feed dead from
        Friday afternoon until now"; merged, they can. Days are the unit of
        resolution because that is the unit IB reports.
        """
        merged = SessionSet(
            time_zone_id=self.time_zone_id or other.time_zone_id,
            tz=self.tz if self.days or self.closed_days else other.tz,
            tz_fallback=self.tz_fallback or other.tz_fallback,
        )
        for day, sessions in self.days.items():
            for sess in sessions:
                merged.add_session(day, sess)
        for day in self.closed_days:
            merged.mark_closed(day)
        for day, sessions in other.days.items():
            merged.days.pop(day, None)
            if day in merged.closed_days:
                merged.closed_days.remove(day)
            for sess in sessions:
                merged.add_session(
                    day,
                    Session(
                        sess.start.astimezone(merged.tz),
                        sess.end.astimezone(merged.tz),
                    ),
                )
        for day in other.closed_days:
            if day not in other.days:
                merged.days.pop(day, None)
                merged.mark_closed(day)
        merged._reindex()
        return merged

    # -- persistence -------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "time_zone_id": self.time_zone_id,
            "tz_fallback": self.tz_fallback,
            "days": {
                d.isoformat(): [s.to_dict() for s in sessions]
                for d, sessions in sorted(self.days.items())
            },
            "closed_days": [d.isoformat() for d in sorted(self.closed_days)],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SessionSet":
        tz, fell_back = resolve_tz(data.get("time_zone_id", ""))
        out = cls(
            time_zone_id=data.get("time_zone_id", ""),
            tz=tz,
            tz_fallback=bool(data.get("tz_fallback", fell_back)),
        )
        for day_s, sessions in (data.get("days") or {}).items():
            day = date.fromisoformat(day_s)
            for s in sessions:
                out.add_session(
                    day,
                    Session(
                        datetime.fromisoformat(s["start"]).astimezone(tz),
                        datetime.fromisoformat(s["end"]).astimezone(tz),
                    ),
                )
        for day_s in data.get("closed_days") or []:
            out.mark_closed(date.fromisoformat(day_s))
        out._reindex()
        return out


def _parse_schedule_dt(text: str, tz) -> Optional[datetime]:
    """Parse a historicalSchedule timestamp ('20250313-09:30:00' and friends)."""
    text = (text or "").strip()
    if not text:
        return None
    for fmt in ("%Y%m%d-%H:%M:%S", "%Y%m%d %H:%M:%S", "%Y%m%d-%H%M%S", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=tz)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# The calendar for one instrument
# ---------------------------------------------------------------------------

@dataclass
class MarketCalendar:
    """Trading and liquid hours for one contract, with the questions worth asking.

    `rth=True` (the default everywhere below) reads liquidHours — regular
    hours, the session an equity strategy actually trades. `rth=False` reads
    tradingHours, which includes pre/post. When liquidHours is absent (forex,
    some futures) the liquid set falls back to the trading set.
    """

    symbol: str = ""
    exchange: str = ""
    sec_type: str = ""
    con_id: int = 0
    time_zone_id: str = ""
    trading: SessionSet = field(default_factory=SessionSet)
    liquid: SessionSet = field(default_factory=SessionSet)
    source: str = "contract_details"
    fetched_at: str = ""

    # -- construction ------------------------------------------------------

    @classmethod
    def from_contract_details(cls, cd: Any) -> "MarketCalendar":
        contract = getattr(cd, "contract", None)
        tz_id = getattr(cd, "timeZoneId", "") or ""
        trading = SessionSet.from_ib_string(getattr(cd, "tradingHours", "") or "", tz_id)
        liquid_raw = getattr(cd, "liquidHours", "") or ""
        liquid = SessionSet.from_ib_string(liquid_raw, tz_id) if liquid_raw else trading
        return cls(
            symbol=getattr(contract, "symbol", "") or "",
            exchange=getattr(contract, "exchange", "") or "",
            sec_type=getattr(contract, "secType", "") or "",
            con_id=int(getattr(contract, "conId", 0) or 0),
            time_zone_id=tz_id,
            trading=trading,
            liquid=liquid,
            source="contract_details",
            fetched_at=datetime.now(UTC).isoformat(),
        )

    def sessions(self, rth: bool = True) -> SessionSet:
        target = self.liquid if rth else self.trading
        return target if target else self.trading

    # -- current + past state ---------------------------------------------

    def state_at(self, at: Optional[datetime] = None, rth: bool = True) -> MarketState:
        return self.sessions(rth).state_at(at or datetime.now(UTC))

    def is_open(self, at: Optional[datetime] = None, rth: bool = True) -> bool:
        return self.state_at(at, rth) is MarketState.OPEN

    def was_open(self, at: datetime, rth: bool = True) -> Optional[bool]:
        """Past-tense is_open(). None when the schedule does not cover `at`.

        Three-valued on purpose: "I have no data for that date" is not the
        same answer as "shut", and a caller deciding whether to raise a
        dead-feed alarm needs to be able to tell.
        """
        state = self.state_at(at, rth)
        if state is MarketState.UNKNOWN:
            return None
        return state is MarketState.OPEN

    def describe(self, at: Optional[datetime] = None, rth: bool = True) -> Dict[str, Any]:
        """A status-line-shaped summary — what a `market` command would print."""
        sessions = self.sessions(rth)
        now = sessions.localize(at or datetime.now(UTC))
        state = sessions.state_at(now)
        current = sessions.session_at(now)
        nxt_open = sessions.next_open(now)
        rng = sessions.covered_range()
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "rth": rth,
            "state": state.value,
            "as_of": now.isoformat(),
            "time_zone_id": self.time_zone_id,
            "tz_fallback": sessions.tz_fallback,
            "session_start": current.start.isoformat() if current else None,
            "session_end": current.end.isoformat() if current else None,
            "seconds_to_close": (
                (current.end - now).total_seconds() if current else None
            ),
            "next_open": nxt_open.isoformat() if nxt_open else None,
            "seconds_to_open": (
                (nxt_open - now).total_seconds() if nxt_open and not current else None
            ),
            "is_half_day": sessions.is_half_day(now.date()),
            "covered_from": rng[0].isoformat() if rng else None,
            "covered_to": rng[1].isoformat() if rng else None,
            "source": self.source,
            "fetched_at": self.fetched_at,
        }

    def classify_gap(
        self,
        start: datetime,
        end: datetime,
        rth: bool = True,
    ) -> Dict[str, Any]:
        """Was the market open during [start, end)? — the dead-feed question.

        Given the window in which data stopped arriving, this reports how
        much of it the exchange was actually open for:

            verdict "closed"   no open time in the window; silence explained
            verdict "partial"  some open time; a real gap, smaller than it looks
            verdict "open"     the whole window was tradeable; nothing excuses it
            verdict "unknown"  the schedule does not cover the window

        `open_seconds` is the number that belongs in the alert: a 90-minute
        silence containing 4 minutes of open market is a very different page
        at 03:00 than one containing 90.
        """
        sessions = self.sessions(rth)
        lo, hi = sessions.localize(start), sessions.localize(end)
        total = max(0.0, (hi - lo).total_seconds())
        covered = sessions.covers(lo) and sessions.covers(hi)
        open_seconds = sessions.open_seconds_between(lo, hi) if covered else 0.0
        windows = sessions.sessions_between(lo, hi) if covered else []

        if not covered:
            verdict = "unknown"
            explanation = (
                "schedule does not cover this window — refetch contract "
                "details or request a SCHEDULE range that spans it"
            )
        elif open_seconds <= 0:
            verdict = "closed"
            explanation = "market was shut for the whole window"
        elif total - open_seconds > 1.0:
            verdict = "partial"
            explanation = (
                f"market was open for {open_seconds:.0f}s of a {total:.0f}s window"
            )
        else:
            verdict = "open"
            explanation = "market was open throughout the window"

        return {
            "symbol": self.symbol,
            "rth": rth,
            "start": lo.isoformat(),
            "end": hi.isoformat(),
            "verdict": verdict,
            "gap_seconds": total,
            "open_seconds": open_seconds,
            "open_fraction": (open_seconds / total) if total else 0.0,
            "open_windows": [s.to_dict() for s in windows],
            "explanation": explanation,
        }

    def with_history(self, past: SessionSet, rth: bool = True) -> "MarketCalendar":
        """A copy extended backwards with a SCHEDULE-derived session set.

        Contract details cover today forward; fetch_historical_schedule()
        covers the recent past. Fold the second into the first and the
        calendar can answer questions on both sides of now:

            cal  = fetch_market_calendar(pf, contract)
            hist = fetch_historical_schedule(pf, contract, num_days=30)
            cal  = cal.with_history(hist)

        `rth` must match the use_rth the schedule was fetched with — a
        useRTH=1 SCHEDULE describes liquid hours, useRTH=0 the full session.
        """
        merged = self.sessions(rth).merged_with(past)
        return MarketCalendar(
            symbol=self.symbol,
            exchange=self.exchange,
            sec_type=self.sec_type,
            con_id=self.con_id,
            time_zone_id=self.time_zone_id,
            trading=merged if not rth else self.trading,
            liquid=merged if rth else self.liquid,
            source=f"{self.source}+schedule",
            fetched_at=self.fetched_at,
        )

    def trading_days_between(self, start: date, end: date) -> List[date]:
        """Dates in [start, end] with at least one session, clipped to coverage."""
        return sorted(d for d in self.trading.days if start <= d <= end)

    # -- persistence -------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "sec_type": self.sec_type,
            "con_id": self.con_id,
            "time_zone_id": self.time_zone_id,
            "source": self.source,
            "fetched_at": self.fetched_at,
            "trading": self.trading.to_dict(),
            "liquid": self.liquid.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MarketCalendar":
        return cls(
            symbol=data.get("symbol", ""),
            exchange=data.get("exchange", ""),
            sec_type=data.get("sec_type", ""),
            con_id=int(data.get("con_id", 0) or 0),
            time_zone_id=data.get("time_zone_id", ""),
            trading=SessionSet.from_dict(data.get("trading") or {}),
            liquid=SessionSet.from_dict(data.get("liquid") or {}),
            source=data.get("source", "contract_details"),
            fetched_at=data.get("fetched_at", ""),
        )

    def save(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2))

    @classmethod
    def load(cls, path: str | Path) -> Optional["MarketCalendar"]:
        path = Path(path)
        if not path.exists():
            return None
        try:
            return cls.from_dict(json.loads(path.read_text()))
        except (OSError, ValueError, KeyError) as exc:
            logger.warning(f"market_calendar: unreadable cache {path}: {exc}")
            return None

    def age_seconds(self) -> Optional[float]:
        if not self.fetched_at:
            return None
        try:
            fetched = datetime.fromisoformat(self.fetched_at)
        except ValueError:
            return None
        if fetched.tzinfo is None:
            fetched = fetched.replace(tzinfo=UTC)
        return (datetime.now(UTC) - fetched).total_seconds()


# ---------------------------------------------------------------------------
# Fetching from IB
# ---------------------------------------------------------------------------

def fetch_market_calendar(
    portfolio: Any,
    contract: Any,
    timeout: float = 15.0,
) -> Optional[MarketCalendar]:
    """Fetch hours for `contract` via reqContractDetails. Blocks up to `timeout`.

    `portfolio` is anything exposing request_contract_details() — the live
    Portfolio, or a stand-in in tests.

    Hours are per-VENUE: the same symbol on SMART and on ARCA can differ, and
    an ambiguous contract (a whole option chain, a symbol with no exchange)
    returns many ContractDetails. This takes the first and says so in the
    log; qualify the contract if that matters to you.
    """
    done = threading.Event()
    box: Dict[str, Any] = {}

    def on_end(details: list) -> None:
        box["details"] = details
        done.set()

    try:
        portfolio.request_contract_details(contract=contract, on_end=on_end)
    except Exception as exc:
        logger.error(f"market_calendar: reqContractDetails failed: {exc}")
        return None

    if not done.wait(timeout=timeout):
        logger.warning(f"market_calendar: contract details timeout after {timeout}s")
        return None

    details = box.get("details") or []
    if not details:
        logger.warning(
            f"market_calendar: no contract details for "
            f"{getattr(contract, 'symbol', '?')}"
        )
        return None
    if len(details) > 1:
        logger.info(
            f"market_calendar: {len(details)} contracts matched "
            f"{getattr(contract, 'symbol', '?')}; using the first "
            f"(hours are per-exchange)"
        )
    return MarketCalendar.from_contract_details(details[0])


def fetch_historical_schedule(
    portfolio: Any,
    contract: Any,
    num_days: int = 30,
    end_date_time: str = "",
    use_rth: bool = True,
    timeout: float = 30.0,
) -> Optional[SessionSet]:
    """Fetch a session calendar over a date RANGE, for questions about the past.

    reqHistoricalData(whatToShow="SCHEDULE", barSize="1 day") answers on the
    historicalSchedule() callback with one session per trading day instead of
    OHLC bars. Contract details only reach about a month ahead and nothing
    behind; this is how you get "was 2026-07-03 a half day?" for a date
    already gone.

    IMPLEMENTATION NOTE — read before integrating. Portfolio does not
    implement historicalSchedule(), so this installs a handler on the
    INSTANCE for the duration of the call and removes it afterwards. That is
    deliberate for an unintegrated module (it touches no existing file), and
    it is also the first thing to undo when this lands for real: give
    Portfolio a request_historical_schedule() next to request_historical_data()
    with the same reqId-keyed callback dict, and delete the shadowing here.
    Because of it, this is not safe to run concurrently with itself on one
    portfolio.
    """
    captured: Dict[int, Tuple[str, str, str, list]] = {}
    arrived = threading.Event()
    had_own = "historicalSchedule" in getattr(portfolio, "__dict__", {})
    previous = portfolio.__dict__.get("historicalSchedule") if had_own else None

    def handler(reqId, startDateTime, endDateTime, timeZone, sessions):
        captured[reqId] = (startDateTime, endDateTime, timeZone, list(sessions or []))
        arrived.set()

    try:
        portfolio.historicalSchedule = handler
    except Exception as exc:
        logger.error(f"market_calendar: cannot install schedule handler: {exc}")
        return None

    try:
        try:
            req_id = portfolio.request_historical_data(
                contract=contract,
                end_date_time=end_date_time,
                duration_str=f"{max(1, int(num_days))} D",
                bar_size_setting="1 day",
                what_to_show="SCHEDULE",
                use_rth=use_rth,
            )
        except Exception as exc:
            logger.error(f"market_calendar: SCHEDULE request failed: {exc}")
            return None

        deadline = _time.monotonic() + timeout
        while req_id not in captured and _time.monotonic() < deadline:
            arrived.wait(timeout=0.25)
            arrived.clear()

        payload = captured.get(req_id)
        if payload is None:
            logger.warning(
                f"market_calendar: no historicalSchedule for req_id={req_id} "
                f"within {timeout}s"
            )
            return None
    finally:
        try:
            if had_own:
                portfolio.historicalSchedule = previous
            else:
                portfolio.__dict__.pop("historicalSchedule", None)
        except Exception:                              # pragma: no cover
            logger.debug("market_calendar: could not restore historicalSchedule")

    start_s, end_s, tz_id, sessions = payload
    tz, _ = resolve_tz(tz_id)
    start_dt = _parse_schedule_dt(start_s, tz)
    end_dt = _parse_schedule_dt(end_s, tz)
    return SessionSet.from_historical_sessions(
        sessions,
        tz_id,
        start_date=start_dt.date() if start_dt else None,
        end_date=end_dt.date() if end_dt else None,
    )


def load_or_fetch(
    portfolio: Any,
    contract: Any,
    cache_path: str | Path,
    max_age_hours: float = 12.0,
    timeout: float = 15.0,
) -> Optional[MarketCalendar]:
    """Cached fetch — the polling pattern IB's own docs recommend.

    Contract details are worth requesting once a session, not once a poll:
    the schedule changes at most daily, and every request spends pacing
    budget the trading path may want. Falls back to the stale cache when the
    refetch fails, because month-old hours still beat no hours.
    """
    cached = MarketCalendar.load(cache_path)
    if cached is not None:
        age = cached.age_seconds()
        if age is not None and age < max_age_hours * 3600:
            return cached

    fresh = fetch_market_calendar(portfolio, contract, timeout=timeout)
    if fresh is None:
        if cached is not None:
            logger.warning("market_calendar: refetch failed; using stale cache")
        return cached
    try:
        fresh.save(cache_path)
    except OSError as exc:
        logger.warning(f"market_calendar: could not write cache: {exc}")
    return fresh


# ---------------------------------------------------------------------------
# CLI — offline, so the parsing can be exercised without TWS
# ---------------------------------------------------------------------------

_DEMO_TZ = "America/New_York"
# Real shape of a US equity week: a normal Thursday, the 1 PM close before
# Independence Day, the holiday itself, then the weekend.
_DEMO_TRADING = (
    "20260702:0400-20260702:2000;"
    "20260703:0400-20260703:1700;"
    "20260704:CLOSED;"
    "20260705:CLOSED;"
    "20260706:CLOSED;"
    "20260707:0400-20260707:2000"
)
_DEMO_LIQUID = (
    "20260702:0930-20260702:1600;"
    "20260703:0930-20260703:1300;"
    "20260704:CLOSED;"
    "20260705:CLOSED;"
    "20260706:CLOSED;"
    "20260707:0930-20260707:1600"
)


def _demo_calendar() -> MarketCalendar:
    return MarketCalendar(
        symbol="SPY",
        exchange="SMART",
        sec_type="STK",
        time_zone_id=_DEMO_TZ,
        trading=SessionSet.from_ib_string(_DEMO_TRADING, _DEMO_TZ),
        liquid=SessionSet.from_ib_string(_DEMO_LIQUID, _DEMO_TZ),
        source="demo",
        fetched_at=datetime.now(UTC).isoformat(),
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Evaluate IB trading-hours strings offline.",
    )
    ap.add_argument("--demo", action="store_true",
                    help="run against a built-in July-4th week")
    ap.add_argument("--hours", help="tradingHours string from ContractDetails")
    ap.add_argument("--liquid", help="liquidHours string (defaults to --hours)")
    ap.add_argument("--tz", default=_DEMO_TZ, help="IB timeZoneId")
    ap.add_argument("--symbol", default="?", help="label only")
    ap.add_argument("--at", help="instant to evaluate (ISO, exchange-local if naive)")
    ap.add_argument("--gap", nargs=2, metavar=("START", "END"),
                    help="classify a data gap between two ISO timestamps")
    ap.add_argument("--extended", action="store_true",
                    help="use tradingHours instead of liquidHours")
    args = ap.parse_args(argv)

    if args.demo:
        cal = _demo_calendar()
    elif args.hours:
        cal = MarketCalendar(
            symbol=args.symbol,
            time_zone_id=args.tz,
            trading=SessionSet.from_ib_string(args.hours, args.tz),
            liquid=SessionSet.from_ib_string(args.liquid or args.hours, args.tz),
            source="cli",
            fetched_at=datetime.now(UTC).isoformat(),
        )
    else:
        ap.error("give --hours or --demo")
        return 2

    rth = not args.extended
    if args.at:
        at = datetime.fromisoformat(args.at)
    elif args.demo:
        # The demo week is fixed, so "now" would almost always fall outside
        # it and report UNKNOWN. Land inside the early-close session instead.
        at = datetime(2026, 7, 3, 12, 30)
    else:
        at = None

    if args.gap:
        report = cal.classify_gap(
            datetime.fromisoformat(args.gap[0]),
            datetime.fromisoformat(args.gap[1]),
            rth=rth,
        )
    else:
        report = cal.describe(at, rth=rth)
    print(json.dumps(report, indent=2))

    if args.demo:
        sessions = cal.sessions(rth)
        print("\nday-by-day:")
        rng = sessions.covered_range()
        cursor, last = rng
        while cursor <= last:
            state = "CLOSED" if cursor in sessions.closed_days else "open"
            half = sessions.is_half_day(cursor)
            marks = ", ".join(
                f"{s.start:%H:%M}-{s.end:%H:%M}" for s in sessions.sessions_on(cursor)
            ) or "—"
            print(f"  {cursor} {state:6} {marks}"
                  f"{'   (early close)' if half else ''}")
            cursor += timedelta(days=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
