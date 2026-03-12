"""
paper_test_orders_close/plugin.py — Market-close order type tests

Tests that must be submitted before the closing auction (~3:50 PM ET):
  MOC   Market-on-Close   (MOC order type)
  LOC   Limit-on-Close    (LOC order type)

Submit this plugin before 3:50 PM ET on a regular trading day.
IB stops accepting MOC/LOC modifications at 3:50 PM; new submissions may be
rejected after that threshold.  Orders not filled at the close are cancelled.

ETF pairs (sequential — no symbol conflicts):
  0 SPY / QQQ   MOC   (most liquid; close auction always fills at market)
  1 IWM / XLF   LOC   (conditional: fills only if close price ≤/≥ limit)
"""

from decimal import Decimal
from pathlib import Path
from typing import List, Optional

from ibapi.order import Order

from plugins.paper_tests.order_test_base import (
    OFFSET_ABOVE,
    OFFSET_BELOW,
    TEST_QTY,
    OrderTestCase,
    OrderTestPluginBase,
    TradeSignal,
)


def _moc(action: str) -> Order:
    """Market-on-Close."""
    o = Order()
    o.action = action
    o.orderType = "MOC"
    o.totalQuantity = TEST_QTY
    return o


def _loc(action: str, price: float) -> Order:
    """Limit-on-Close."""
    o = Order()
    o.action = action
    o.orderType = "LOC"
    o.totalQuantity = TEST_QTY
    o.lmtPrice = round(price, 2)
    return o


_CASES: List[OrderTestCase] = [
    OrderTestCase(
        name="market_on_close",
        order_type_label="MOC",
        pair_index=0,
        build_long=lambda p: _moc("BUY"),
        build_short=lambda p: _moc("SELL"),
        fill_timeout=60.0,
        immediate=True,
        notes="MOC executes at closing auction; submit before 3:50 PM ET",
    ),
    OrderTestCase(
        name="limit_on_close",
        order_type_label="LOC",
        pair_index=1,
        build_long=lambda p: _loc("BUY",  p * OFFSET_BELOW),
        build_short=lambda p: _loc("SELL", p * OFFSET_ABOVE),
        fill_timeout=60.0,
        notes="LOC fills at close if close price is at or better than limit",
    ),
]


class PaperTestOrdersClosePlugin(OrderTestPluginBase):
    """
    Market-close order types: MOC, LOC.

    Must be submitted before 3:50 PM ET on a regular trading day.
    Run via: plugin request paper_test_orders_close run_tests
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
            "paper_test_orders_close",
            base_path, portfolio, shared_holdings, message_bus,
        )

    @property
    def description(self) -> str:
        return (
            "Paper Test Orders Close — Market-close order types: "
            "MOC (Market-on-Close), LOC (Limit-on-Close)"
        )
