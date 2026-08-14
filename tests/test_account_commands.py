"""
Tests for the account-level ibctl pass-throughs: `account` and `commissions`.

Both are deliberately account-oriented rather than plugin-oriented — they
report what IB says and what was actually paid, with no plugin ledger
interpretation layered on top.
"""

import json
from unittest.mock import Mock, patch

import pytest

from ib.run_engine import EngineCommandHandler


def _handler(portfolio=None):
    engine = Mock()
    engine.portfolio = portfolio
    return EngineCommandHandler.__new__(EngineCommandHandler), engine


def _account(**kw):
    acct = Mock()
    acct.account_id = kw.get("account_id", "U21830461")
    acct.currency = kw.get("currency", "USD")
    acct.net_liquidation = kw.get("net_liquidation", 250_000.0)
    acct.total_cash = kw.get("total_cash", 24_811.74)
    acct.buying_power = kw.get("buying_power", 1_000_000.0)
    acct.available_funds = kw.get("available_funds", 24_811.74)
    acct.is_valid = kw.get("is_valid", True)
    acct.values = kw.get("values", {"NetLiquidation": "250000", "Cushion": "0.87"})
    return acct


class TestAccountCommand:
    def _run(self, args, account):
        handler, engine = _handler()
        portfolio = Mock()
        portfolio.get_account_summary.return_value = account
        engine.portfolio = portfolio
        handler.engine = engine
        return handler.handle_account(args)

    def test_reports_the_named_fields(self):
        result = self._run([], _account())
        assert result.data["account_id"] == "U21830461"
        assert result.data["net_liquidation"] == 250_000.0
        assert "U21830461" in result.message

    def test_carries_every_raw_ib_tag(self):
        """The named fields are a convenience view of a few tags, not all of
        them; anything IB sent must survive the round trip."""
        result = self._run([], _account(values={"Cushion": "0.87", "GrossPositionValue": "9"}))
        assert result.data["values"] == {"Cushion": "0.87", "GrossPositionValue": "9"}
        assert "Cushion" in result.message

    def test_message_stays_text_even_with_json(self):
        """`message` is the human view and `data` the machine view — always,
        regardless of --json. Rendering both as JSON duplicated the content and
        made `message`, the obvious field to read, the wrong one to parse."""
        result = self._run(["--json"], _account())
        assert "ACCOUNT" in result.message
        assert not result.message.lstrip().startswith("{")
        assert result.data["account_id"] == "U21830461"
        assert result.data["values"]["Cushion"] == "0.87"

    def test_invalid_account_is_flagged_not_silently_shown(self):
        """net_liquidation 0 means IB has not sent a usable summary. Printing
        zeroes as if they were real is how a disconnected engine looks solvent."""
        result = self._run([], _account(is_valid=False, net_liquidation=0.0))
        assert "WARNING" in result.message
        assert result.data["is_valid"] is False

    def test_no_account_is_an_error(self):
        result = self._run([], None)
        assert result.status.value == "error"

    def test_no_portfolio_is_an_error_not_a_crash(self):
        handler, engine = _handler(portfolio=None)
        handler.engine = engine
        result = handler.handle_account([])
        assert result.status.value == "error"


class TestCommissionsCommand:
    REPORT = {
        "symbol": None, "start_date": None, "end_date": None,
        "total_commission": 4.75, "total_realized_pnl": 7.0,
        "record_count": 3, "currency": "USD",
        "by_symbol": [
            {"symbol": "GLD", "commission": 4.0, "realized_pnl": 6.0, "count": 2},
            {"symbol": "QQQ", "commission": 0.75, "realized_pnl": 1.0, "count": 1},
        ],
    }

    def _run(self, args, report=None):
        handler, engine = _handler()
        handler.engine = engine
        db = Mock()
        db.get_commission_report.return_value = report if report is not None else self.REPORT
        with patch("ib.execution_db.get_execution_db", return_value=db):
            return handler.handle_commissions(args), db

    def test_reports_totals_and_breakdown(self):
        result, _ = self._run([])
        assert result.data["total_commission"] == 4.75
        assert "GLD" in result.message and "QQQ" in result.message

    def test_message_stays_text_even_with_json(self):
        result, _ = self._run(["--json"])
        assert "COMMISSIONS AND FEES" in result.message
        assert not result.message.lstrip().startswith("{")
        assert result.data["record_count"] == 3

    def test_symbol_filter_reaches_the_query(self):
        _, db = self._run(["--symbol", "gld"])
        assert db.get_commission_report.call_args.kwargs["symbol"] == "GLD"

    def test_days_filter_becomes_a_start_date(self):
        _, db = self._run(["--days", "30"])
        assert db.get_commission_report.call_args.kwargs["start_date"] is not None

    def test_no_filters_means_no_start_date(self):
        _, db = self._run([])
        assert db.get_commission_report.call_args.kwargs["start_date"] is None

    def test_bad_days_value_is_rejected_not_ignored(self):
        """Silently dropping an unparseable --days would report all history as
        if it were the requested window."""
        result, db = self._run(["--days", "lots"])
        assert result.status.value == "error"
        db.get_commission_report.assert_not_called()

    def test_empty_report_explains_itself(self):
        empty = dict(self.REPORT, total_commission=0.0, total_realized_pnl=0.0,
                     record_count=0, by_symbol=[], currency=None)
        result, _ = self._run([], report=empty)
        assert "No commission records" in result.message

    def test_unattributed_commission_is_shown(self):
        """A commission whose fill never arrived is still money paid."""
        orphan = dict(self.REPORT, by_symbol=[
            {"symbol": None, "commission": 3.25, "realized_pnl": 0.0, "count": 1},
        ])
        result, _ = self._run([], report=orphan)
        assert "(unattributed)" in result.message
