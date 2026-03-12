"""
paper_test_orders_open/plugin.py — Market-open order type tests

Tests that must be submitted before or at the opening auction (9:30 AM ET):
  MOO   Market-on-Open   (MKT + OPG tif)
  LOO   Limit-on-Open    (LMT + OPG tif)
  AUC   At-Auction       (MTL + AUC tif)

Submit this plugin before 9:25 AM ET on a regular trading day.
Orders that are not picked up at the open will be cancelled by the cleanup path.

ETF pairs (sequential — no symbol conflicts):
  0 SPY / QQQ   MOO   (most liquid; open auction always fills at market)
  1 IWM / XLF   LOO   (conditional: fills only if open price ≤/≥ limit)
  2 XLK / XLE   AUC   (participates in opening/closing auction)
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


def _moo(action: str) -> Order:
    """Market-on-Open (MKT + OPG tif)."""
    o = Order()
    o.action = action
    o.orderType = "MKT"
    o.totalQuantity = TEST_QTY
    o.tif = "OPG"
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


def _auction(action: str, price: float) -> Order:
    """At-Auction (MTL + AUC tif): participates in opening/closing auction."""
    o = Order()
    o.action = action
    o.orderType = "MTL"
    o.totalQuantity = TEST_QTY
    o.lmtPrice = round(price, 2)
    o.tif = "AUC"
    return o


_CASES: List[OrderTestCase] = [
    OrderTestCase(
        name="market_on_open",
        order_type_label="MOO",
        pair_index=0,
        build_long=lambda p: _moo("BUY"),
        build_short=lambda p: _moo("SELL"),
        fill_timeout=60.0,
        immediate=True,
        notes="MOO (MKT+OPG) executes at opening auction; submit before 9:25 AM ET",
    ),
    OrderTestCase(
        name="limit_on_open",
        order_type_label="LOO",
        pair_index=1,
        build_long=lambda p: _loo("BUY",  p * OFFSET_BELOW),
        build_short=lambda p: _loo("SELL", p * OFFSET_ABOVE),
        fill_timeout=60.0,
        notes="LOO (LMT+OPG) fills at open if price is at or better than limit",
    ),
    OrderTestCase(
        name="auction",
        order_type_label="MTL+AUC",
        pair_index=2,
        build_long=lambda p: _auction("BUY",  p * OFFSET_BELOW),
        build_short=lambda p: _auction("SELL", p * OFFSET_ABOVE),
        fill_timeout=60.0,
        notes="At-Auction (MTL+AUC tif): participates in opening/closing auction",
    ),
]


class PaperTestOrdersOpenPlugin(OrderTestPluginBase):
    """
    Market-open order types: MOO, LOO, Auction.

    Must be submitted before 9:25 AM ET on a regular trading day.
    Run via: plugin request paper_test_orders_open run_tests
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
            "paper_test_orders_open",
            base_path, portfolio, shared_holdings, message_bus,
        )

    @property
    def description(self) -> str:
        return (
            "Paper Test Orders Open — Market-open order types: "
            "MOO (MKT+OPG), LOO (LMT+OPG), At-Auction (MTL+AUC)"
        )
