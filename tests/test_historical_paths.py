"""
Tests for the single historical-data path (ib/historical.py) and everything
that now goes through it: Portfolio's error classification, PluginBase's
get_historical_data / get_bars_cached / subscribe_live_bars, the engine's
`historical fetch` command, ibctl's thin client, and the one DB resolver.

The two bugs these pin down:

  1. A plugin cache fill that TIMED OUT returned [] (`... or []`), which
     BarStore records as "IB has no bars here" — a permanent hole.
  2. Any path: IB REFUSING a request (162 pacing violation, 321, 354, 200)
     ended it as an empty result. Same hole.

Only IB's own "no data" answer may extend coverage without bars.

The Portfolio used here is real (error(), historicalData(), the request
table); only the socket call reqHistoricalData is replaced by a scripted
responder, so the classification is exercised exactly as IB would drive it.
"""

import json
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from ib import bar_store, historical
from ib.bar_store import BarStore
from ib.client import IBClient
from ib.historical import HistoricalDataError
from ib.portfolio import Portfolio, historical_error_is_no_data

UTC = timezone.utc
START = datetime(2026, 9, 7, tzinfo=UTC)
END = datetime(2026, 9, 8, tzinfo=UTC)
PACING = "Historical Market Data Service error message:Historical data request pacing violation"
NO_DATA = "Historical Market Data Service error message:HMDS query returned no data: GLD@ARCA Trades"


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

def _bar(epoch, close=100.0):
    return SimpleNamespace(date=str(epoch), open=close, high=close + 0.1,
                           low=close - 0.1, close=close, volume=-1, wap=0.0,
                           barCount=0)


def _bars(n=3, start=START):
    t0 = int(start.timestamp())
    return [_bar(t0 + 300 * k, 100.0 + k) for k in range(n)]


def _stk(symbol="GLD"):
    return SimpleNamespace(symbol=symbol, secType="STK", currency="USD")


def _pair(base="USD", quote="JPY"):
    return SimpleNamespace(symbol=base, secType="CASH", currency=quote)


class Scripted:
    """
    What IB does with each reqHistoricalData, in order: a list of bars, the
    string "empty" (IB's 162 no-data), an (code, message) refusal, or None
    (IB never answers). The last entry repeats.
    """

    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls = []

    def __call__(self, p, req_id):
        self.calls.append(req_id)
        reply = self.replies[min(len(self.calls), len(self.replies)) - 1]
        if reply is None:
            return
        if reply == "empty":
            p.error(req_id, 0, 162, NO_DATA)
        elif isinstance(reply, tuple):
            p.error(req_id, 0, *reply)
        else:
            for b in reply:
                p.historicalData(req_id, b)
            p.historicalDataEnd(req_id, "", "")


@pytest.fixture
def portfolio(monkeypatch):
    """A real Portfolio with the socket replaced by a Scripted responder."""
    monkeypatch.setattr(IBClient, "error", lambda self, *a, **k: None)
    p = Portfolio.__new__(Portfolio)
    p._historical_requests = {}
    p._historical_on_error = {}
    p._kutd_symbols = {}
    p._kutd_last_bar = {}
    p._whatif_error = {}
    p._whatif_open_order = {}
    p._orders = {}
    p._pending_orders = {}
    p._market_data_requests = {}
    p._contract_details_requests = {}
    p._connected = threading.Event()
    p._connected.set()
    ids = iter(range(100, 10_000))
    p.get_next_req_id = lambda: next(ids)
    p.cancelHistoricalData = Mock()
    p.script = Scripted([])

    def req(req_id, *args):
        p.script(p, req_id)
    p.reqHistoricalData = req
    return p


@pytest.fixture
def db(tmp_path, monkeypatch):
    """A fresh database that every path resolves to."""
    path = tmp_path / "bars.db"
    monkeypatch.setenv("IB_HIST_DB", str(path))
    monkeypatch.setattr(bar_store, "_shared", {})
    return path


def _coverage(path, symbol):
    import sqlite3
    con = sqlite3.connect(path)
    try:
        return con.execute("select start_utc, end_utc from coverage where symbol=?",
                           (symbol,)).fetchall()
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Portfolio: a refusal is not an empty answer
# ---------------------------------------------------------------------------

class TestPortfolioErrorClassification:
    def _request(self, p, with_on_error=True):
        seen = {}
        p.request_historical_data(
            contract=_stk(),
            on_end=lambda bars, s, e: seen.setdefault("end", list(bars)),
            on_error=(lambda code, msg: seen.setdefault("error", code)) if with_on_error else None,
        )
        return seen

    def test_no_data_ends_as_an_empty_answer(self, portfolio):
        portfolio.script = Scripted("empty")
        seen = self._request(portfolio)
        assert seen == {"end": []}

    @pytest.mark.parametrize("code,msg", [
        (162, PACING),
        (162, "Historical Market Data Service error message:API historical data query cancelled: 101"),
        (321, "Error validating request.-'bW' : cause - Invalid bar size"),
        (354, "Requested market data is not subscribed."),
        (200, "No security definition has been found for the request"),
    ])
    def test_refusal_goes_to_on_error_not_on_end(self, portfolio, code, msg):
        portfolio.script = Scripted((code, msg))
        seen = self._request(portfolio)
        assert seen == {"error": code}
        assert portfolio._historical_requests == {}
        assert portfolio._historical_on_error == {}

    def test_callers_without_on_error_keep_the_old_release(self, portfolio):
        """market_calendar and keepUpToDate subscriptions pass no on_error;
        they are still released rather than left hanging."""
        portfolio.script = Scripted((162, PACING))
        seen = self._request(portfolio, with_on_error=False)
        assert seen == {"end": []}

    def test_classifier(self):
        assert historical_error_is_no_data(162, NO_DATA)
        assert not historical_error_is_no_data(162, PACING)
        assert not historical_error_is_no_data(354, "no data subscription")


# ---------------------------------------------------------------------------
# historical.fetch_bars: raise on failure, [] only for IB's "no data"
# ---------------------------------------------------------------------------

class TestFetchBars:
    def _fetch(self, p, timeout=1.0):
        return historical.fetch_bars(p, _stk(), "", "1 D", "5 mins", "TRADES", True, timeout)

    def test_bars(self, portfolio):
        portfolio.script = Scripted(_bars(3))
        assert len(self._fetch(portfolio)) == 3

    def test_no_data_is_empty(self, portfolio):
        portfolio.script = Scripted("empty")
        assert self._fetch(portfolio) == []

    def test_refusal_raises_with_the_code(self, portfolio):
        portfolio.script = Scripted((162, PACING))
        with pytest.raises(HistoricalDataError) as exc:
            self._fetch(portfolio)
        assert exc.value.code == 162

    def test_timeout_raises_and_cancels(self, portfolio):
        portfolio.script = Scripted(None)
        with pytest.raises(HistoricalDataError) as exc:
            self._fetch(portfolio, timeout=0.05)
        assert exc.value.code is None
        portfolio.cancelHistoricalData.assert_called_once()
        assert portfolio._historical_requests == {}


# ---------------------------------------------------------------------------
# historical.cached_bars: coverage only for what IB answered
# ---------------------------------------------------------------------------

class TestCachedBars:
    def _get(self, p, contract=None, **kw):
        stats = {}
        rows = historical.cached_bars(p, bar_store.shared_store(), contract or _stk(),
                                      START, END, "5 mins", "TRADES", True,
                                      stats=stats, **kw)
        return rows, stats

    def test_bars_are_stored_and_then_served_from_cache(self, portfolio, db):
        portfolio.script = Scripted(_bars(3))
        rows, stats = self._get(portfolio)
        assert len(rows) == 3 and stats["requests"] == 1 and stats["stored"] == 3
        rows, stats = self._get(portfolio)
        assert len(rows) == 3 and stats["requests"] == 0
        assert len(portfolio.script.calls) == 1

    def test_no_data_is_covered_and_not_asked_again(self, portfolio, db):
        portfolio.script = Scripted("empty")
        self._get(portfolio)
        self._get(portfolio)
        assert len(portfolio.script.calls) == 1
        assert _coverage(db, "GLD")

    def test_refusal_leaves_the_range_open_and_retries(self, portfolio, db):
        portfolio.script = Scripted((162, PACING), _bars(2))
        rows, stats = self._get(portfolio)
        assert rows == [] and stats["failed"] == 1
        assert _coverage(db, "GLD") == []
        rows, stats = self._get(portfolio)
        assert len(rows) == 2 and stats["failed"] == 0
        assert len(portfolio.script.calls) == 2

    def test_timeout_leaves_the_range_open(self, portfolio, db, monkeypatch):
        monkeypatch.setattr(historical, "DEFAULT_TIMEOUT", 0.05)
        portfolio.script = Scripted(None)
        rows, stats = self._get(portfolio)
        assert stats["failed"] == 1 and _coverage(db, "GLD") == []

    def test_a_pair_is_keyed_on_the_pair(self, portfolio, db):
        portfolio.script = Scripted(_bars(2))
        self._get(portfolio, contract=_pair("USD", "JPY"))
        assert _coverage(db, "USD.JPY") and _coverage(db, "USD") == []

    def test_no_connection_serves_cache_and_covers_nothing(self, db):
        rows, stats = self._get(None)
        assert rows == [] and stats["failed"] == 1 and _coverage(db, "GLD") == []


# ---------------------------------------------------------------------------
# PluginBase: the same path
# ---------------------------------------------------------------------------

class TestPluginPaths:
    def _plugin(self, portfolio):
        from tests.test_account_separation import SimplePlugin
        plugin = SimplePlugin("hist_test")
        plugin.portfolio = portfolio
        return plugin

    def test_get_bars_cached_timeout_no_longer_covers_the_gap(self, portfolio, db, monkeypatch):
        """The `get_historical_data(...) or []` bug: a timeout became []."""
        monkeypatch.setattr(historical, "DEFAULT_TIMEOUT", 0.05)
        portfolio.script = Scripted(None, _bars(2))
        plugin = self._plugin(portfolio)
        assert plugin.get_bars_cached(_stk(), START, END) == []
        assert _coverage(db, "GLD") == []
        assert len(plugin.get_bars_cached(_stk(), START, END)) == 2

    def test_get_bars_cached_refusal_no_longer_covers_the_gap(self, portfolio, db):
        portfolio.script = Scripted((162, PACING))
        self._plugin(portfolio).get_bars_cached(_stk(), START, END)
        assert _coverage(db, "GLD") == []

    def test_get_historical_data_is_none_on_failure_and_empty_on_no_data(self, portfolio):
        plugin = self._plugin(portfolio)
        portfolio.script = Scripted((354, "Requested market data is not subscribed."))
        assert plugin.get_historical_data(_stk(), duration_str="1 D") is None
        portfolio.script = Scripted("empty")
        assert plugin.get_historical_data(_stk(), duration_str="1 D") == []

    def test_plugin_and_engine_share_one_store(self, portfolio, db):
        assert self._plugin(portfolio)._bar_store is bar_store.shared_store()

    def test_live_bars_are_written_under_the_same_key(self, db):
        from tests.test_account_separation import SimplePlugin
        plugin = SimplePlugin("hist_live")
        plugin.portfolio = Mock()
        plugin.portfolio.request_historical_data.return_value = 7
        plugin.subscribe_live_bars(_pair("USD", "JPY"), on_bar=lambda b: None)
        on_bar = plugin.portfolio.request_historical_data.call_args.kwargs["on_bar"]
        on_bar(_bars(1)[0])
        import sqlite3
        con = sqlite3.connect(db)
        syms = {r[0] for r in con.execute("select symbol from bars")}
        con.close()
        assert syms == {"USD.JPY"}


# ---------------------------------------------------------------------------
# Engine `historical fetch`: the same path, in the engine
# ---------------------------------------------------------------------------

class TestEngineHistoricalCommand:
    def _handler(self, portfolio):
        from ib.run_engine import EngineCommandHandler
        engine = Mock()
        engine.portfolio = portfolio
        return EngineCommandHandler(engine)

    def _fetch(self, portfolio, *extra):
        return self._handler(portfolio).handle_historical(
            ["fetch", "GLD", "--bar-size", "5_mins", "--duration", "1_D",
             "--end", "20260908-00:00:00", *extra])

    def test_fetch_goes_through_the_cache(self, portfolio, db):
        from ib.command_server import CommandStatus
        portfolio.script = Scripted(_bars(3))
        r = self._fetch(portfolio)
        assert r.status == CommandStatus.SUCCESS
        assert r.data["count"] == 3 and r.data["stats"]["stored"] == 3
        assert r.data["db_path"] == str(db)
        assert r.data["bars"][0]["date"].endswith("+00:00")
        r = self._fetch(portfolio)
        assert r.data["stats"]["requests"] == 0 and r.data["count"] == 3

    def test_refusal_is_an_error_and_covers_nothing(self, portfolio, db):
        from ib.command_server import CommandStatus
        portfolio.script = Scripted((162, PACING))
        r = self._fetch(portfolio)
        assert r.status == CommandStatus.ERROR
        assert r.data["stats"]["failed"] == 1
        assert _coverage(db, "GLD") == []

    def test_force_refetches(self, portfolio, db):
        portfolio.script = Scripted(_bars(3))
        self._fetch(portfolio)
        r = self._fetch(portfolio, "--force")
        assert r.data["stats"]["requests"] == 1
        assert len(portfolio.script.calls) == 2

    def test_forex_series_is_the_pair(self, portfolio, db):
        portfolio.script = Scripted(_bars(2))
        r = self._handler(portfolio).handle_historical(
            ["fetch", "USD.JPY", "--type", "forex", "--bar-size", "5_mins",
             "--duration", "1_D", "--end", "20260908-00:00:00", "--no-rth"])
        assert r.data["symbol"] == "USD.JPY"
        assert _coverage(db, "USD.JPY")


# ---------------------------------------------------------------------------
# ibctl: a thin client that writes nothing
# ---------------------------------------------------------------------------

class TestIbctlFetchIsAThinClient:
    def _run(self, monkeypatch, status, data, *args):
        import ibctl
        from ibctl import CommandResult, CommandStatus
        sent = []

        def fake_send(command, socket_path=None, timeout=None, **kw):
            sent.append((command, timeout))
            return CommandResult(status=getattr(CommandStatus, status),
                                 message="msg", data=data)
        monkeypatch.setattr(ibctl, "send_command", fake_send)
        code = 0
        try:
            ibctl._historical_fetch(list(args), "/tmp/nowhere.sock", 10.0)
        except SystemExit as e:
            code = e.code
        return sent, code

    def test_one_engine_call_and_no_local_writes(self, monkeypatch, db):
        sent, code = self._run(
            monkeypatch, "SUCCESS",
            {"bars": [], "stats": {"requests": 1, "stored": 0}, "db_path": str(db)},
            "USD.JPY", "--type", "forex", "--bar-size", "5 mins",
            "--duration", "180 D", "--what", "BID", "--no-rth", "--force")
        assert code == 0 and len(sent) == 1
        cmd, timeout = sent[0]
        assert cmd == ("historical fetch USD.JPY --type forex --bar-size 5_mins "
                       "--duration 180_D --what BID --no-rth --force")
        # 180 days of 5-minute bars is 6 IB chunks; the client waits for them
        assert timeout >= 60 + 130 * 6
        assert not db.exists()

    def test_engine_error_exits_nonzero(self, monkeypatch, db):
        _, code = self._run(monkeypatch, "ERROR",
                            {"bars": [], "stats": {"failed": 1}}, "GLD")
        assert code == 1


# ---------------------------------------------------------------------------
# One database for every path
# ---------------------------------------------------------------------------

class TestOneDatabase:
    def test_precedence_env_then_config_then_default(self, tmp_path, monkeypatch):
        cfg = tmp_path / "config.json"
        monkeypatch.setattr(bar_store, "HIST_CONFIG", cfg)
        monkeypatch.delenv("IB_HIST_DB", raising=False)
        assert bar_store.resolve_db_path() == bar_store.DEFAULT_DB
        cfg.write_text(json.dumps({"db_path": str(tmp_path / "configured.db")}))
        assert bar_store.resolve_db_path() == tmp_path / "configured.db"
        monkeypatch.setenv("IB_HIST_DB", str(tmp_path / "env.db"))
        assert bar_store.resolve_db_path() == tmp_path / "env.db"

    def test_ibctl_and_recorder_agree_with_the_engine(self, tmp_path, monkeypatch):
        import importlib.util
        import ibctl
        monkeypatch.setenv("IB_HIST_DB", str(tmp_path / "one.db"))
        assert ibctl._historical_db_path() == str(tmp_path / "one.db")
        spec = importlib.util.spec_from_file_location(
            "recorder_under_test", bar_store.PROJECT_ROOT / "historical" / "recorder.py")
        rec = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(rec)
        assert rec.db_path() == str(tmp_path / "one.db")

    def test_parse_duration_round_trips(self):
        assert bar_store.parse_duration("6 M") == timedelta(days=180)
        assert bar_store.parse_duration("180_D") == timedelta(days=180)
        with pytest.raises(ValueError):
            bar_store.parse_duration("6 Q")
