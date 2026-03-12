"""
Tests for session-specific paper order plugins and order_test_base infrastructure.

Covers:
  - paper_test_orders_open  (MOO, LOO, At-Auction)
  - paper_test_orders_close (MOC, LOC)
  - order_test_base shared types
  - Correct removal of session-specific cases from orders 1, 2, 3
"""

import pytest
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

from plugins.paper_tests.order_test_base import (
    ETF_PAIRS,
    OFFSET_ABOVE,
    OFFSET_BELOW,
    PAPER_PORTS,
    TEST_QTY,
    OrderPairResult,
    OrderTestCase,
    make_stk_contract,
)
from plugins.paper_tests.paper_test_orders_open.plugin import (
    PaperTestOrdersOpenPlugin,
    _CASES as OPEN_CASES,
    _auction,
    _loo,
    _moo,
)
from plugins.paper_tests.paper_test_orders_close.plugin import (
    PaperTestOrdersClosePlugin,
    _CASES as CLOSE_CASES,
    _loc,
    _moc,
)
from plugins.paper_tests.paper_test_orders_1.plugin import _CASES as ORDERS1_CASES
from plugins.paper_tests.paper_test_orders_2.plugin import _CASES as ORDERS2_CASES
from plugins.paper_tests.paper_test_orders_3.plugin import _CASES as ORDERS3_CASES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_portfolio(port=7497, account="DU123456", connected=True):
    p = MagicMock()
    p.connected = connected
    p.port = port
    p.managed_accounts = [account]
    return p


def _case_names(cases):
    return {tc.name for tc in cases}


# ---------------------------------------------------------------------------
# make_stk_contract
# ---------------------------------------------------------------------------

class TestMakeStkContract:
    def test_known_symbol_sets_primary_exchange(self):
        c = make_stk_contract("SPY")
        assert c.primaryExch == "ARCA"

    def test_qqq_primary_exchange(self):
        c = make_stk_contract("QQQ")
        assert c.primaryExch == "NASDAQ"

    def test_unknown_symbol_empty_primary_exchange(self):
        c = make_stk_contract("XYZ")
        assert c.primaryExch == ""

    def test_defaults(self):
        c = make_stk_contract("SPY")
        assert c.symbol == "SPY"
        assert c.secType == "STK"
        assert c.exchange == "SMART"
        assert c.currency == "USD"

    def test_override_exchange(self):
        c = make_stk_contract("SPY", exchange="ARCA")
        assert c.exchange == "ARCA"


# ---------------------------------------------------------------------------
# OrderPairResult
# ---------------------------------------------------------------------------

class TestOrderPairResult:
    def _make(self, **kwargs):
        defaults = dict(test_name="t", order_type="MKT",
                        symbol_long="SPY", symbol_short="QQQ")
        defaults.update(kwargs)
        return OrderPairResult(**defaults)

    def test_stub_always_passes(self):
        r = self._make(is_stub=True, stub_reason="unsupported")
        assert r.passed is True

    def test_submitted_no_error_passes(self):
        r = self._make(submitted=True)
        assert r.passed is True

    def test_not_submitted_fails(self):
        r = self._make(submitted=False)
        assert r.passed is False

    def test_submitted_with_error_fails(self):
        r = self._make(submitted=True, error_message="IB error 201")
        assert r.passed is False

    def test_to_dict_keys(self):
        r = self._make(submitted=True, fill_side="long", fill_price=450.12)
        d = r.to_dict()
        for key in ("test_name", "order_type", "symbol_long", "symbol_short",
                    "submitted", "fill_side", "fill_price", "cancel_ok",
                    "error_message", "duration_seconds", "notes",
                    "is_stub", "stub_reason", "passed"):
            assert key in d, f"Missing key: {key}"

    def test_to_dict_values(self):
        r = self._make(submitted=True, fill_side="short", fill_price=300.0)
        d = r.to_dict()
        assert d["submitted"] is True
        assert d["fill_side"] == "short"
        assert d["fill_price"] == 300.0
        assert d["passed"] is True


# ---------------------------------------------------------------------------
# Open-plugin order builders
# ---------------------------------------------------------------------------

class TestOpenOrderBuilders:
    def test_moo_buy(self):
        o = _moo("BUY")
        assert o.action == "BUY"
        assert o.orderType == "MKT"
        assert o.tif == "OPG"
        assert o.totalQuantity == TEST_QTY

    def test_moo_sell(self):
        o = _moo("SELL")
        assert o.action == "SELL"
        assert o.tif == "OPG"

    def test_loo_buy(self):
        price = 500.0
        o = _loo("BUY", price)
        assert o.action == "BUY"
        assert o.orderType == "LMT"
        assert o.tif == "OPG"
        assert o.totalQuantity == TEST_QTY
        assert o.lmtPrice == round(price, 2)

    def test_loo_price_rounded(self):
        o = _loo("SELL", 499.999)
        assert o.lmtPrice == 500.0

    def test_auction_buy(self):
        price = 480.0
        o = _auction("BUY", price)
        assert o.action == "BUY"
        assert o.orderType == "MTL"
        assert o.tif == "AUC"
        assert o.lmtPrice == round(price, 2)
        assert o.totalQuantity == TEST_QTY

    def test_auction_sell(self):
        o = _auction("SELL", 200.0)
        assert o.action == "SELL"
        assert o.tif == "AUC"


# ---------------------------------------------------------------------------
# Close-plugin order builders
# ---------------------------------------------------------------------------

class TestCloseOrderBuilders:
    def test_moc_buy(self):
        o = _moc("BUY")
        assert o.action == "BUY"
        assert o.orderType == "MOC"
        assert o.totalQuantity == TEST_QTY

    def test_moc_sell(self):
        o = _moc("SELL")
        assert o.action == "SELL"
        assert o.orderType == "MOC"

    def test_loc_buy(self):
        price = 450.0
        o = _loc("BUY", price)
        assert o.action == "BUY"
        assert o.orderType == "LOC"
        assert o.lmtPrice == round(price, 2)
        assert o.totalQuantity == TEST_QTY

    def test_loc_price_rounded(self):
        o = _loc("SELL", 299.999)
        assert o.lmtPrice == 300.0


# ---------------------------------------------------------------------------
# Open plugin: TEST_CASES structure
# ---------------------------------------------------------------------------

class TestOpenCases:
    def test_has_three_cases(self):
        assert len(OPEN_CASES) == 3

    def test_contains_moo(self):
        assert any(tc.name == "market_on_open" for tc in OPEN_CASES)

    def test_contains_loo(self):
        assert any(tc.name == "limit_on_open" for tc in OPEN_CASES)

    def test_contains_auction(self):
        assert any(tc.name == "auction" for tc in OPEN_CASES)

    def test_order_type_labels(self):
        labels = {tc.order_type_label for tc in OPEN_CASES}
        assert "MOO" in labels
        assert "LOO" in labels
        assert "MTL+AUC" in labels

    def test_unique_pair_indices(self):
        indices = [tc.pair_index for tc in OPEN_CASES]
        assert len(indices) == len(set(indices)), "Each test case should use a distinct ETF pair"

    def test_pair_indices_in_bounds(self):
        for tc in OPEN_CASES:
            assert 0 <= tc.pair_index < len(ETF_PAIRS)

    def test_moo_is_immediate(self):
        moo = next(tc for tc in OPEN_CASES if tc.name == "market_on_open")
        assert moo.immediate is True

    def test_loo_not_immediate(self):
        loo = next(tc for tc in OPEN_CASES if tc.name == "limit_on_open")
        assert loo.immediate is False

    def test_builders_called_with_price(self):
        for tc in OPEN_CASES:
            if not tc.is_stub:
                # Builders must accept a price argument without raising
                tc.build_long(500.0)
                tc.build_short(500.0)


# ---------------------------------------------------------------------------
# Close plugin: TEST_CASES structure
# ---------------------------------------------------------------------------

class TestCloseCases:
    def test_has_two_cases(self):
        assert len(CLOSE_CASES) == 2

    def test_contains_moc(self):
        assert any(tc.name == "market_on_close" for tc in CLOSE_CASES)

    def test_contains_loc(self):
        assert any(tc.name == "limit_on_close" for tc in CLOSE_CASES)

    def test_order_type_labels(self):
        labels = {tc.order_type_label for tc in CLOSE_CASES}
        assert "MOC" in labels
        assert "LOC" in labels

    def test_unique_pair_indices(self):
        indices = [tc.pair_index for tc in CLOSE_CASES]
        assert len(indices) == len(set(indices))

    def test_pair_indices_in_bounds(self):
        for tc in CLOSE_CASES:
            assert 0 <= tc.pair_index < len(ETF_PAIRS)

    def test_moc_is_immediate(self):
        moc = next(tc for tc in CLOSE_CASES if tc.name == "market_on_close")
        assert moc.immediate is True

    def test_loc_not_immediate(self):
        loc = next(tc for tc in CLOSE_CASES if tc.name == "limit_on_close")
        assert loc.immediate is False

    def test_builders_called_with_price(self):
        for tc in CLOSE_CASES:
            if not tc.is_stub:
                tc.build_long(450.0)
                tc.build_short(450.0)


# ---------------------------------------------------------------------------
# Verify session-specific tests removed from original plugins
# ---------------------------------------------------------------------------

class TestSessionCasesRemoved:
    def test_moc_not_in_orders_1(self):
        assert "market_on_close" not in _case_names(ORDERS1_CASES)

    def test_moo_not_in_orders_1(self):
        assert "market_on_open" not in _case_names(ORDERS1_CASES)

    def test_orders_1_expected_cases(self):
        names = _case_names(ORDERS1_CASES)
        assert names == {"market", "limit", "stop", "stop_limit", "market_to_limit"}

    def test_loc_not_in_orders_2(self):
        assert "limit_on_close" not in _case_names(ORDERS2_CASES)

    def test_loo_not_in_orders_2(self):
        assert "limit_on_open" not in _case_names(ORDERS2_CASES)

    def test_orders_2_expected_cases(self):
        names = _case_names(ORDERS2_CASES)
        assert names == {
            "market_if_touched", "limit_if_touched", "midprice",
            "discretionary", "trailing_stop",
        }

    def test_auction_not_in_orders_3(self):
        assert "auction" not in _case_names(ORDERS3_CASES)

    def test_orders_3_still_has_other_pegged_types(self):
        names = _case_names(ORDERS3_CASES)
        assert "trailing_stop_limit" in names
        assert "pegged_to_market" in names
        assert "adjusted_stop_to_trail" in names


# ---------------------------------------------------------------------------
# Plugin instantiation and metadata
# ---------------------------------------------------------------------------

class TestOpenPluginMetadata:
    def test_name(self, tmp_path):
        p = PaperTestOrdersOpenPlugin(base_path=tmp_path)
        assert p.name == "paper_test_orders_open"

    def test_description_mentions_moo(self, tmp_path):
        p = PaperTestOrdersOpenPlugin(base_path=tmp_path)
        assert "MOO" in p.description or "Market-on-Open" in p.description

    def test_test_cases_bound(self, tmp_path):
        p = PaperTestOrdersOpenPlugin(base_path=tmp_path)
        assert p.TEST_CASES is OPEN_CASES


class TestClosePluginMetadata:
    def test_name(self, tmp_path):
        p = PaperTestOrdersClosePlugin(base_path=tmp_path)
        assert p.name == "paper_test_orders_close"

    def test_description_mentions_moc(self, tmp_path):
        p = PaperTestOrdersClosePlugin(base_path=tmp_path)
        assert "MOC" in p.description or "Market-on-Close" in p.description

    def test_test_cases_bound(self, tmp_path):
        p = PaperTestOrdersClosePlugin(base_path=tmp_path)
        assert p.TEST_CASES is CLOSE_CASES


# ---------------------------------------------------------------------------
# handle_request routing
# ---------------------------------------------------------------------------

class TestHandleRequest:
    def _plugin(self, tmp_path, cls):
        plugin = cls(base_path=tmp_path)
        plugin.load()
        plugin.start()
        return plugin

    def test_get_status_open(self, tmp_path):
        p = self._plugin(tmp_path, PaperTestOrdersOpenPlugin)
        resp = p.handle_request("get_status", {})
        assert resp["success"] is True
        assert resp["data"]["test_count"] == len(OPEN_CASES)
        assert resp["data"]["result_count"] == 0
        assert resp["data"]["running"] is False

    def test_get_status_close(self, tmp_path):
        p = self._plugin(tmp_path, PaperTestOrdersClosePlugin)
        resp = p.handle_request("get_status", {})
        assert resp["success"] is True
        assert resp["data"]["test_count"] == len(CLOSE_CASES)

    def test_get_results_empty(self, tmp_path):
        p = self._plugin(tmp_path, PaperTestOrdersOpenPlugin)
        resp = p.handle_request("get_results", {})
        assert resp["success"] is True
        assert resp["data"]["results"] == []

    def test_unknown_request_fails(self, tmp_path):
        p = self._plugin(tmp_path, PaperTestOrdersOpenPlugin)
        resp = p.handle_request("nonexistent", {})
        assert resp["success"] is False

    def test_run_tests_blocked_while_running(self, tmp_path):
        p = self._plugin(tmp_path, PaperTestOrdersOpenPlugin)
        p._running = True
        resp = p.handle_request("run_tests", {})
        assert resp["success"] is False
        assert "already running" in resp["message"].lower()


# ---------------------------------------------------------------------------
# _verify_paper_connection
# ---------------------------------------------------------------------------

class TestVerifyPaperConnection:
    def _plugin(self, tmp_path):
        p = PaperTestOrdersOpenPlugin(base_path=tmp_path)
        p.load()
        p.start()
        return p

    def test_no_portfolio(self, tmp_path):
        p = self._plugin(tmp_path)
        # portfolio is None by default
        err = p._verify_paper_connection()
        assert err is not None
        assert "portfolio" in err.lower() or "No portfolio" in err

    def test_not_connected(self, tmp_path):
        p = self._plugin(tmp_path)
        p._portfolio = _make_portfolio(connected=False)
        # inject portfolio via the private attr the base class uses
        p.__dict__["portfolio"] = p._portfolio
        err = p._verify_paper_connection()
        assert err is not None
        assert "connected" in err.lower()

    def test_live_port_rejected(self, tmp_path):
        p = self._plugin(tmp_path)
        portfolio = _make_portfolio(port=7496)  # live TWS port
        p.__dict__["portfolio"] = portfolio
        err = p._verify_paper_connection()
        assert err is not None
        assert "SAFETY" in err or "paper" in err.lower()

    def test_live_account_rejected(self, tmp_path):
        p = self._plugin(tmp_path)
        portfolio = _make_portfolio(account="U1234567")  # live account (no 'D' prefix)
        p.__dict__["portfolio"] = portfolio
        err = p._verify_paper_connection()
        assert err is not None
        assert "SAFETY" in err or "paper" in err.lower()

    def test_valid_paper_setup_passes(self, tmp_path):
        p = self._plugin(tmp_path)
        portfolio = _make_portfolio(port=7497, account="DU123456")
        p.__dict__["portfolio"] = portfolio
        err = p._verify_paper_connection()
        assert err is None

    def test_gateway_paper_port_accepted(self, tmp_path):
        p = self._plugin(tmp_path)
        portfolio = _make_portfolio(port=4002, account="DU999999")
        p.__dict__["portfolio"] = portfolio
        err = p._verify_paper_connection()
        assert err is None

    def test_empty_accounts_rejected(self, tmp_path):
        p = self._plugin(tmp_path)
        portfolio = _make_portfolio()
        portfolio.managed_accounts = []
        p.__dict__["portfolio"] = portfolio
        err = p._verify_paper_connection()
        assert err is not None
        assert "account" in err.lower()


# ---------------------------------------------------------------------------
# _build_summary
# ---------------------------------------------------------------------------

class TestBuildSummary:
    def _plugin(self, tmp_path):
        p = PaperTestOrdersClosePlugin(base_path=tmp_path)
        p.load()
        p.start()
        return p

    def test_empty_summary(self, tmp_path):
        p = self._plugin(tmp_path)
        s = p._build_summary()
        assert s["total"] == 0
        assert s["submitted"] == 0
        assert s["filled"] == 0
        assert s["stubs"] == 0
        assert s["errors"] == []

    def test_summary_counts(self, tmp_path):
        p = self._plugin(tmp_path)
        p._results = [
            OrderPairResult("moc", "MOC", "SPY", "QQQ", submitted=True, fill_side="long"),
            OrderPairResult("loc", "LOC", "IWM", "XLF", submitted=True),
            OrderPairResult("bad", "MKT", "A",   "B",   error_message="err"),
        ]
        s = p._build_summary()
        assert s["total"] == 3
        assert s["submitted"] == 2
        assert s["filled"] == 1
        assert s["errors"] == ["bad"]

    def test_stubs_count_as_submitted(self, tmp_path):
        p = self._plugin(tmp_path)
        p._results = [
            OrderPairResult("s", "X", "A", "B", is_stub=True, stub_reason="unsupported"),
        ]
        s = p._build_summary()
        assert s["total"] == 1
        assert s["submitted"] == 1
        assert s["stubs"] == 1
