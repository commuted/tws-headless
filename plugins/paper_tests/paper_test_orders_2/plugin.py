"""
paper_test_orders_2/plugin.py – Limit-variant equity order types

Tests: Limit-on-Close, Limit-on-Open, Market-if-Touched,
       Limit-if-Touched, Midprice, Discretionary, Trailing Stop
"""

from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional

from ibapi.order import Order

from plugins.paper_tests.order_test_base import (
    OFFSET_ABOVE,
    OFFSET_BELOW,
    TEST_QTY,
    OrderTestCase,
    OrderTestPluginBase,
    TradeSignal,
)


def _loc(action: str, price: float) -> Order:
    """Limit-on-Close."""
    o = Order()
    o.action = action
    o.orderType = "LOC"
    o.totalQuantity = TEST_QTY
    o.lmtPrice = round(price, 2)
    return o


def _loo(action: str, price: float) -> Order:
    """Limit-on-Open (LMT + OPG tif)."""
    o = Order()
    o.action = action
    o.orderType = "LMT"
    o.totalQuantity = TEST_QTY
    o.lmtPrice = round(price, 2)
    o.tif = "OPG"
    return o


def _mit(action: str, trigger: float) -> Order:
    """Market-if-Touched."""
    o = Order()
    o.action = action
    o.orderType = "MIT"
    o.totalQuantity = TEST_QTY
    o.auxPrice = round(trigger, 2)
    return o


def _lit(action: str, lmt: float, trigger: float) -> Order:
    """Limit-if-Touched."""
    o = Order()
    o.action = action
    o.orderType = "LIT"
    o.totalQuantity = TEST_QTY
    o.lmtPrice = round(lmt, 2)
    o.auxPrice = round(trigger, 2)
    return o


def _midprice(action: str, price_cap: float) -> Order:
    """Midprice order (IB-specific)."""
    o = Order()
    o.action = action
    o.orderType = "MIDPRICE"
    o.totalQuantity = TEST_QTY
    o.lmtPrice = round(price_cap, 2)
    return o


def _discretionary(action: str, price: float, disc: float) -> Order:
    """Limit order with discretionary amount."""
    o = Order()
    o.action = action
    o.orderType = "LMT"
    o.totalQuantity = TEST_QTY
    o.lmtPrice = round(price, 2)
    o.discretionaryAmt = round(disc, 2)
    return o


def _trail(action: str, trail_pct: float) -> Order:
    """Trailing stop (by percentage)."""
    o = Order()
    o.action = action
    o.orderType = "TRAIL"
    o.totalQuantity = TEST_QTY
    o.trailingPercent = trail_pct
    return o


_CASES: List[OrderTestCase] = [
    OrderTestCase(
        name="limit_on_close",
        order_type_label="LOC",
        pair_index=0,
        build_long=lambda p: _loc("BUY",  p * OFFSET_BELOW),
        build_short=lambda p: _loc("SELL", p * OFFSET_ABOVE),
        fill_timeout=30.0,
        notes="LOC executes at closing auction at or better than limit",
    ),
    OrderTestCase(
        name="limit_on_open",
        order_type_label="LOO",
        pair_index=1,
        build_long=lambda p: _loo("BUY",  p * OFFSET_BELOW),
        build_short=lambda p: _loo("SELL", p * OFFSET_ABOVE),
        fill_timeout=30.0,
        notes="LOO executes at opening auction at or better than limit",
    ),
    OrderTestCase(
        name="market_if_touched",
        order_type_label="MIT",
        pair_index=2,
        # BUY MIT: triggers (→ market order) when price DROPS to trigger
        # SELL MIT: triggers when price RISES to trigger
        build_long=lambda p: _mit("BUY",  p * OFFSET_BELOW),
        build_short=lambda p: _mit("SELL", p * OFFSET_ABOVE),
        fill_timeout=300.0,
        notes="MIT becomes a market order when the trigger price is touched",
    ),
    OrderTestCase(
        name="limit_if_touched",
        order_type_label="LIT",
        pair_index=3,
        build_long=lambda p: _lit("BUY",
                                   p * OFFSET_BELOW,
                                   p * OFFSET_BELOW * 0.999),
        build_short=lambda p: _lit("SELL",
                                    p * OFFSET_ABOVE,
                                    p * OFFSET_ABOVE * 1.001),
        fill_timeout=300.0,
        notes="LIT becomes a limit order when trigger price is touched",
    ),
    OrderTestCase(
        name="midprice",
        order_type_label="MIDPRICE",
        pair_index=4,
        # Midprice fills at the mid of bid/ask; price_cap is a hard limit
        build_long=lambda p: _midprice("BUY",  p * 1.01),   # cap 1% above
        build_short=lambda p: _midprice("SELL", p * 0.99),   # cap 1% below
        fill_timeout=120.0,
        notes="IB midprice order executes at bid/ask midpoint up to the cap",
    ),
    OrderTestCase(
        name="discretionary",
        order_type_label="LMT+disc",
        pair_index=5,
        # Limit at price; discretionary amount allows broker to improve up to 0.5%
        build_long=lambda p: _discretionary("BUY",
                                             p * OFFSET_BELOW,
                                             p * 0.005),
        build_short=lambda p: _discretionary("SELL",
                                              p * OFFSET_ABOVE,
                                              p * 0.005),
        fill_timeout=300.0,
        notes="Discretionary: limit with a discretionary improvement amount",
    ),
    OrderTestCase(
        name="trailing_stop",
        order_type_label="TRAIL",
        pair_index=6,
        # SELL trailing stop: trail 1% below market (exit long)
        # BUY trailing stop: trail 1% above market (cover short)
        build_long=lambda p: _trail("SELL", 1.0),   # protect long position
        build_short=lambda p: _trail("BUY",  1.0),  # protect short position
        fill_timeout=300.0,
        notes="Trailing stop: SELL trails 1% below market; BUY trails 1% above",
    ),
]


class PaperTestOrders2Plugin(OrderTestPluginBase):
    """
    Plugin 2 of 5: Limit-variant equity order types.

    Tests: LOC, LOO, MIT, LIT, Midprice, Discretionary, Trailing Stop.
    Run via: plugin request paper_test_orders_2 run_tests
    """

    TEST_CASES = _CASES

    def __init__(
        self,
        base_path: Optional[Path] = None,
        portfolio=None,
        shared_holdings=None,
        message_bus=None,
    ):
        super().__init__(
            "paper_test_orders_2",
            base_path, portfolio, shared_holdings, message_bus,
        )

    @property
    def description(self) -> str:
        return (
            "Paper Test Orders 2 – Limit variants: "
            "LOC, LOO, MIT, LIT, Midprice, Discretionary, Trailing Stop"
        )
