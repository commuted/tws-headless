"""
paper_test_orders_4/plugin.py – Complex equity order types

Tests: Bracket, OCA, Scale, Price Condition, Time Condition,
       Volume Condition, Execution Condition

Bracket, OCA, and Scale require multi-leg order placement; the base
_run_test_case() is overridden for those to call _place_raw() with
pre-allocated IDs.

NOTE on conditions: Price/Volume/PercentChange conditions require an IB
conId for the trigger security.  We embed commonly-known conIds for
paper testing (SPY=756733, QQQ=320227571).  In a live system you would
look these up via reqContractDetails.
"""

import time
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ibapi.order import Order
from ibapi.order_condition import (
    ExecutionCondition,
    PriceCondition,
    TimeCondition,
    VolumeCondition,
)

from plugins.paper_tests.order_test_base import (
    ETF_PAIRS,
    OFFSET_ABOVE,
    OFFSET_BELOW,
    TEST_QTY,
    OrderPairResult,
    OrderTestCase,
    OrderTestPluginBase,
    TradeSignal,
    make_stk_contract,
)

# Well-known conIds for condition triggers (paper testing only)
_SPY_CON_ID = 756733
_QQQ_CON_ID = 320227571


# ---------------------------------------------------------------------------
# Order builders
# ---------------------------------------------------------------------------

def _lmt(action: str, price: float) -> Order:
    o = Order()
    o.action = action
    o.orderType = "LMT"
    o.totalQuantity = TEST_QTY
    o.lmtPrice = round(price, 2)
    return o


def _lmt_with_price_cond(action: str, price: float,
                          trigger_price: float, con_id: int,
                          is_more: bool) -> Order:
    """Limit order that only executes when a price condition is met."""
    o = _lmt(action, price)
    cond = PriceCondition()
    cond.isMore = is_more
    cond.price = round(trigger_price, 2)
    cond.conId = con_id
    cond.exchange = "SMART"
    o.conditions = [cond]
    o.conditionIgnoreRth = True
    return o


def _lmt_with_time_cond(action: str, price: float, before_time: str) -> Order:
    """Limit order valid only before a specific time."""
    o = _lmt(action, price)
    cond = TimeCondition()
    cond.isMore = False            # trigger when current time < before_time
    cond.time = before_time
    o.conditions = [cond]
    o.conditionIgnoreRth = True
    return o


def _lmt_with_volume_cond(action: str, price: float,
                            volume: int, con_id: int,
                            is_more: bool) -> Order:
    """Limit order that triggers when volume exceeds/falls below threshold."""
    o = _lmt(action, price)
    cond = VolumeCondition()
    cond.isMore = is_more
    cond.volume = volume
    cond.conId = con_id
    cond.exchange = "SMART"
    o.conditions = [cond]
    o.conditionIgnoreRth = True
    return o


def _lmt_with_exec_cond(action: str, price: float,
                         symbol: str, exchange: str,
                         sec_type: str) -> Order:
    """Limit order that triggers after any execution in the given symbol."""
    o = _lmt(action, price)
    cond = ExecutionCondition()
    cond.symbol = symbol
    cond.exchange = exchange
    cond.secType = sec_type
    o.conditions = [cond]
    o.conditionIgnoreRth = True
    return o


# ---------------------------------------------------------------------------
# Plugin
# ---------------------------------------------------------------------------

class PaperTestOrders4Plugin(OrderTestPluginBase):
    """
    Plugin 4 of 5: Complex equity order types.

    Tests: Bracket, OCA, Scale, Price Condition, Time Condition,
           Volume Condition, Execution Condition.
    Run via: plugin request paper_test_orders_4 run_tests
    """

    VERSION = "1.0.0"
    IS_SYSTEM_PLUGIN = False

    # For simple test cases we still list them here
    TEST_CASES: List[OrderTestCase] = []

    def __init__(
        self,
        base_path: Optional[Path] = None,
        portfolio=None,
        shared_holdings=None,
        message_bus=None,
    ):
        super().__init__(
            "paper_test_orders_4",
            base_path, portfolio, shared_holdings, message_bus,
        )

    @property
    def description(self) -> str:
        return (
            "Paper Test Orders 4 – Complex: "
            "Bracket, OCA, Scale, Price/Time/Volume/Execution Conditions"
        )

    # Override _run_all_tests to handle custom cases
    def _run_all_tests(self) -> Dict:
        if self._running:
            return {"success": False, "message": "Tests already running"}
        if not self._executive:
            return {"success": False, "message": "No executive/stream manager"}

        self._running = True
        self._results = []

        try:
            error = self._verify_paper_connection()
            if error:
                return {"success": False, "message": error}

            self.portfolio.reqMarketDataType(3)

            # Run each custom test method
            tests = [
                self._test_bracket,
                self._test_oca,
                self._test_scale,
                self._test_price_condition,
                self._test_time_condition,
                self._test_volume_condition,
                self._test_execution_condition,
            ]
            for test_fn in tests:
                result = test_fn()
                self._results.append(result)
                self._log_result(result)

            summary = self._build_summary()
            self.request_unload()
            return {
                "success": True,
                "data": {
                    "results": [r.to_dict() for r in self._results],
                    "summary": summary,
                },
            }

        except Exception as e:
            return {"success": False, "message": str(e)}
        finally:
            self._running = False

    # -----------------------------------------------------------------------
    # Individual test methods
    # -----------------------------------------------------------------------

    def _fetch_pair_prices(self, pair_idx: int
                           ) -> Tuple[Optional[float], Optional[float]]:
        sym_a, sym_b = ETF_PAIRS[pair_idx]
        price_a = self._fetch_price(sym_a, make_stk_contract(sym_a))
        price_b = self._fetch_price(sym_b, make_stk_contract(sym_b))
        return price_a, price_b

    def _test_bracket(self) -> OrderPairResult:
        """Bracket order: entry limit + take-profit + stop-loss."""
        sym_a, sym_b = ETF_PAIRS[0]
        result = OrderPairResult(
            test_name="bracket_order", order_type="BRACKET",
            symbol_long=sym_a, symbol_short=sym_b,
        )
        start = time.time()
        try:
            price_a, price_b = self._fetch_pair_prices(0)
            if not price_a or not price_b:
                result.error_message = f"No price: {sym_a}={price_a}, {sym_b}={price_b}"
                return result

            # Bracket BUY on sym_a
            ids_a = self._alloc_ids(3)  # parent, TP, SL
            parent_a = _lmt("BUY", round(price_a * OFFSET_BELOW, 2))
            parent_a.orderId = ids_a[0]
            parent_a.transmit = False

            tp_a = Order()
            tp_a.action = "SELL"
            tp_a.orderType = "LMT"
            tp_a.totalQuantity = TEST_QTY
            tp_a.lmtPrice = round(price_a * 1.02, 2)
            tp_a.parentId = ids_a[0]
            tp_a.orderId = ids_a[1]
            tp_a.transmit = False

            sl_a = Order()
            sl_a.action = "SELL"
            sl_a.orderType = "STP"
            sl_a.totalQuantity = TEST_QTY
            sl_a.auxPrice = round(price_a * 0.98, 2)
            sl_a.parentId = ids_a[0]
            sl_a.orderId = ids_a[2]
            sl_a.transmit = True

            con_a = make_stk_contract(sym_a)
            ok_a = (
                self._place_raw(ids_a[0], con_a, parent_a) and
                self._place_raw(ids_a[1], con_a, tp_a) and
                self._place_raw(ids_a[2], con_a, sl_a)
            )

            # Bracket SELL (short) on sym_b
            ids_b = self._alloc_ids(3)
            parent_b = _lmt("SELL", round(price_b * OFFSET_ABOVE, 2))
            parent_b.orderId = ids_b[0]
            parent_b.transmit = False

            tp_b = Order()
            tp_b.action = "BUY"
            tp_b.orderType = "LMT"
            tp_b.totalQuantity = TEST_QTY
            tp_b.lmtPrice = round(price_b * 0.98, 2)
            tp_b.parentId = ids_b[0]
            tp_b.orderId = ids_b[1]
            tp_b.transmit = False

            sl_b = Order()
            sl_b.action = "BUY"
            sl_b.orderType = "STP"
            sl_b.totalQuantity = TEST_QTY
            sl_b.auxPrice = round(price_b * 1.02, 2)
            sl_b.parentId = ids_b[0]
            sl_b.orderId = ids_b[2]
            sl_b.transmit = True

            con_b = make_stk_contract(sym_b)
            ok_b = (
                self._place_raw(ids_b[0], con_b, parent_b) and
                self._place_raw(ids_b[1], con_b, tp_b) and
                self._place_raw(ids_b[2], con_b, sl_b)
            )

            result.submitted = ok_a or ok_b
            result.long_order_id = ids_a[0]
            result.short_order_id = ids_b[0]
            result.notes = "Entry at ±0.5%; TP at ±2%; SL at ∓2%"

            if result.submitted:
                fill_side, fill_price = self._wait_fill_cancel_other(
                    ids_a[0], ids_b[0], timeout=300.0
                )
                result.fill_side = fill_side
                result.fill_price = fill_price
                result.cancel_ok = fill_side is not None

        except Exception as e:
            result.error_message = str(e)
        result.duration_seconds = time.time() - start
        return result

    def _test_oca(self) -> OrderPairResult:
        """OCA (One-Cancels-All) group on the same symbol."""
        sym_a, sym_b = ETF_PAIRS[1]
        result = OrderPairResult(
            test_name="one_cancels_all", order_type="OCA",
            symbol_long=sym_a, symbol_short=sym_b,
        )
        start = time.time()
        try:
            price_a, price_b = self._fetch_pair_prices(1)
            if not price_a or not price_b:
                result.error_message = f"No price: {sym_a}={price_a}, {sym_b}={price_b}"
                return result

            oca_group = f"oca_{uuid.uuid4().hex[:8]}"

            order_a = _lmt("BUY", round(price_a * OFFSET_BELOW, 2))
            order_a.ocaGroup = oca_group
            order_a.ocaType = 1  # cancel all remaining on fill

            order_b = _lmt("SELL", round(price_b * OFFSET_ABOVE, 2))
            order_b.ocaGroup = oca_group
            order_b.ocaType = 1

            oid_a = self._place(make_stk_contract(sym_a), order_a)
            oid_b = self._place(make_stk_contract(sym_b), order_b)

            result.submitted = oid_a is not None or oid_b is not None
            result.long_order_id = oid_a
            result.short_order_id = oid_b
            result.notes = f"OCA group '{oca_group}': buy {sym_a} / sell {sym_b}"

            if result.submitted:
                fill_side, fill_price = self._wait_fill_cancel_other(
                    oid_a, oid_b, timeout=300.0
                )
                result.fill_side = fill_side
                result.fill_price = fill_price
                result.cancel_ok = fill_side is not None

        except Exception as e:
            result.error_message = str(e)
        result.duration_seconds = time.time() - start
        return result

    def _test_scale(self) -> OrderPairResult:
        """Scale order: BUY in layers at decreasing prices."""
        sym_a, sym_b = ETF_PAIRS[2]
        result = OrderPairResult(
            test_name="scale_order", order_type="SCALE",
            symbol_long=sym_a, symbol_short=sym_b,
        )
        start = time.time()
        try:
            price_a, price_b = self._fetch_pair_prices(2)
            if not price_a or not price_b:
                result.error_message = f"No price: {sym_a}={price_a}, {sym_b}={price_b}"
                return result

            # Scale BUY on sym_a
            order_a = Order()
            order_a.action = "BUY"
            order_a.orderType = "LMT"
            order_a.totalQuantity = Decimal("5")        # 5 shares total
            order_a.lmtPrice = round(price_a * OFFSET_BELOW, 2)
            order_a.scaleInitLevelSize = 1              # 1 share per level
            order_a.scalePriceIncrement = Decimal("0.10")  # $0.10 steps
            order_a.scaleSubsLevelSize = 1
            order_a.scaleProfitOffset = Decimal("0.05")    # take profit $0.05 above cost

            # Scale SELL on sym_b (short)
            order_b = Order()
            order_b.action = "SELL"
            order_b.orderType = "LMT"
            order_b.totalQuantity = Decimal("5")
            order_b.lmtPrice = round(price_b * OFFSET_ABOVE, 2)
            order_b.scaleInitLevelSize = 1
            order_b.scalePriceIncrement = Decimal("0.10")
            order_b.scaleSubsLevelSize = 1
            order_b.scaleProfitOffset = Decimal("0.05")

            oid_a = self._place(make_stk_contract(sym_a), order_a)
            oid_b = self._place(make_stk_contract(sym_b), order_b)

            result.submitted = oid_a is not None or oid_b is not None
            result.long_order_id = oid_a
            result.short_order_id = oid_b
            result.notes = "5-share scale: 1/level, $0.10 steps, $0.05 profit offset"

            if result.submitted:
                fill_side, fill_price = self._wait_fill_cancel_other(
                    oid_a, oid_b, timeout=300.0
                )
                result.fill_side = fill_side
                result.fill_price = fill_price
                result.cancel_ok = fill_side is not None

        except Exception as e:
            result.error_message = str(e)
        result.duration_seconds = time.time() - start
        return result

    def _test_price_condition(self) -> OrderPairResult:
        """Limit order with a price condition on SPY."""
        sym_a, sym_b = ETF_PAIRS[3]
        result = OrderPairResult(
            test_name="price_condition", order_type="LMT+PRICE_COND",
            symbol_long=sym_a, symbol_short=sym_b,
        )
        start = time.time()
        try:
            price_a, price_b = self._fetch_pair_prices(3)
            if not price_a or not price_b:
                result.error_message = f"No price: {sym_a}={price_a}, {sym_b}={price_b}"
                return result

            spy_price = self._fetch_price("SPY", make_stk_contract("SPY"))
            if not spy_price:
                result.error_message = "Could not fetch SPY price for condition"
                return result

            # Condition: execute when SPY price > current SPY price * 0.99
            # (will trigger on almost any upward tick)
            trigger = round(spy_price * 0.99, 2)

            order_a = _lmt_with_price_cond(
                "BUY", round(price_a * OFFSET_BELOW, 2),
                trigger, _SPY_CON_ID, is_more=True,
            )
            order_b = _lmt_with_price_cond(
                "SELL", round(price_b * OFFSET_ABOVE, 2),
                round(spy_price * 1.01, 2), _SPY_CON_ID, is_more=False,
            )

            oid_a = self._place(make_stk_contract(sym_a), order_a)
            oid_b = self._place(make_stk_contract(sym_b), order_b)

            result.submitted = oid_a is not None or oid_b is not None
            result.long_order_id = oid_a
            result.short_order_id = oid_b
            result.notes = (
                f"Long when SPY > {trigger}; Short when SPY < {round(spy_price*1.01,2)}. "
                f"conId={_SPY_CON_ID} (verify for your IB instance)"
            )

            if result.submitted:
                fill_side, fill_price = self._wait_fill_cancel_other(
                    oid_a, oid_b, timeout=300.0
                )
                result.fill_side = fill_side
                result.fill_price = fill_price
                result.cancel_ok = fill_side is not None

        except Exception as e:
            result.error_message = str(e)
        result.duration_seconds = time.time() - start
        return result

    def _test_time_condition(self) -> OrderPairResult:
        """Limit order that is only active before end-of-day."""
        sym_a, sym_b = ETF_PAIRS[4]
        result = OrderPairResult(
            test_name="time_condition", order_type="LMT+TIME_COND",
            symbol_long=sym_a, symbol_short=sym_b,
        )
        start = time.time()
        try:
            price_a, price_b = self._fetch_pair_prices(4)
            if not price_a or not price_b:
                result.error_message = f"No price: {sym_a}={price_a}, {sym_b}={price_b}"
                return result

            # Condition: execute if time < 20:00 ET today
            from datetime import date
            today = date.today().strftime("%Y%m%d")
            cutoff = f"{today} 20:00:00"

            order_a = _lmt_with_time_cond(
                "BUY", round(price_a * OFFSET_BELOW, 2), cutoff
            )
            order_b = _lmt_with_time_cond(
                "SELL", round(price_b * OFFSET_ABOVE, 2), cutoff
            )

            oid_a = self._place(make_stk_contract(sym_a), order_a)
            oid_b = self._place(make_stk_contract(sym_b), order_b)

            result.submitted = oid_a is not None or oid_b is not None
            result.long_order_id = oid_a
            result.short_order_id = oid_b
            result.notes = f"Orders valid only before {cutoff} ET"

            if result.submitted:
                fill_side, fill_price = self._wait_fill_cancel_other(
                    oid_a, oid_b, timeout=300.0
                )
                result.fill_side = fill_side
                result.fill_price = fill_price
                result.cancel_ok = fill_side is not None

        except Exception as e:
            result.error_message = str(e)
        result.duration_seconds = time.time() - start
        return result

    def _test_volume_condition(self) -> OrderPairResult:
        """Limit order that triggers when QQQ volume exceeds a threshold."""
        sym_a, sym_b = ETF_PAIRS[5]
        result = OrderPairResult(
            test_name="volume_condition", order_type="LMT+VOL_COND",
            symbol_long=sym_a, symbol_short=sym_b,
        )
        start = time.time()
        try:
            price_a, price_b = self._fetch_pair_prices(5)
            if not price_a or not price_b:
                result.error_message = f"No price: {sym_a}={price_a}, {sym_b}={price_b}"
                return result

            # Trigger if QQQ volume > 1000 shares (will be met almost immediately)
            order_a = _lmt_with_volume_cond(
                "BUY", round(price_a * OFFSET_BELOW, 2),
                volume=1000, con_id=_QQQ_CON_ID, is_more=True,
            )
            order_b = _lmt_with_volume_cond(
                "SELL", round(price_b * OFFSET_ABOVE, 2),
                volume=1000, con_id=_QQQ_CON_ID, is_more=True,
            )

            oid_a = self._place(make_stk_contract(sym_a), order_a)
            oid_b = self._place(make_stk_contract(sym_b), order_b)

            result.submitted = oid_a is not None or oid_b is not None
            result.long_order_id = oid_a
            result.short_order_id = oid_b
            result.notes = (
                f"Trigger when QQQ volume > 1000. "
                f"conId={_QQQ_CON_ID} (verify for your IB instance)"
            )

            if result.submitted:
                fill_side, fill_price = self._wait_fill_cancel_other(
                    oid_a, oid_b, timeout=300.0
                )
                result.fill_side = fill_side
                result.fill_price = fill_price
                result.cancel_ok = fill_side is not None

        except Exception as e:
            result.error_message = str(e)
        result.duration_seconds = time.time() - start
        return result

    def _test_execution_condition(self) -> OrderPairResult:
        """Limit order that triggers after any execution in SPY."""
        sym_a, sym_b = ETF_PAIRS[6]
        result = OrderPairResult(
            test_name="execution_condition", order_type="LMT+EXEC_COND",
            symbol_long=sym_a, symbol_short=sym_b,
        )
        start = time.time()
        try:
            price_a, price_b = self._fetch_pair_prices(6)
            if not price_a or not price_b:
                result.error_message = f"No price: {sym_a}={price_a}, {sym_b}={price_b}"
                return result

            # Trigger after any SPY execution on SMART
            order_a = _lmt_with_exec_cond(
                "BUY", round(price_a * OFFSET_BELOW, 2),
                symbol="SPY", exchange="SMART", sec_type="STK",
            )
            order_b = _lmt_with_exec_cond(
                "SELL", round(price_b * OFFSET_ABOVE, 2),
                symbol="SPY", exchange="SMART", sec_type="STK",
            )

            oid_a = self._place(make_stk_contract(sym_a), order_a)
            oid_b = self._place(make_stk_contract(sym_b), order_b)

            result.submitted = oid_a is not None or oid_b is not None
            result.long_order_id = oid_a
            result.short_order_id = oid_b
            result.notes = "Trigger after any SPY execution on SMART"

            if result.submitted:
                fill_side, fill_price = self._wait_fill_cancel_other(
                    oid_a, oid_b, timeout=300.0
                )
                result.fill_side = fill_side
                result.fill_price = fill_price
                result.cancel_ok = fill_side is not None

        except Exception as e:
            result.error_message = str(e)
        result.duration_seconds = time.time() - start
        return result
