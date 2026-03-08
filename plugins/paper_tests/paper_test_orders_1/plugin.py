"""
paper_test_orders_1/plugin.py – Basic equity order types

Tests: Market, Limit, Stop, Stop-Limit, Market-on-Close,
       Market-on-Open, Market-to-Limit

ETF pairs (one per order type):
  0 SPY / QQQ    1 IWM / XLF    2 XLK / XLE
  3 IWM / XLF    4 SPY / QQQ    5 XLK / XLE    6 SPY / QQQ
"""

from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional

from ibapi.order import Order

from plugins.paper_tests.order_test_base import (
    ETF_PAIRS,
    OFFSET_ABOVE,
    OFFSET_BELOW,
    TEST_QTY,
    OrderTestCase,
    OrderTestPluginBase,
    TradeSignal,
)


def _market(action: str) -> Order:
    o = Order()
    o.action = action
    o.orderType = "MKT"
    o.totalQuantity = TEST_QTY
    return o


def _limit(action: str, price: float) -> Order:
    o = Order()
    o.action = action
    o.orderType = "LMT"
    o.totalQuantity = TEST_QTY
    o.lmtPrice = round(price, 2)
    return o


def _stop(action: str, stop_price: float) -> Order:
    o = Order()
    o.action = action
    o.orderType = "STP"
    o.totalQuantity = TEST_QTY
    o.auxPrice = round(stop_price, 2)
    return o


def _stop_limit(action: str, lmt: float, stp: float) -> Order:
    o = Order()
    o.action = action
    o.orderType = "STP LMT"
    o.totalQuantity = TEST_QTY
    o.lmtPrice = round(lmt, 2)
    o.auxPrice = round(stp, 2)
    return o


def _moc(action: str) -> Order:
    o = Order()
    o.action = action
    o.orderType = "MOC"
    o.totalQuantity = TEST_QTY
    return o


def _moo(action: str) -> Order:
    o = Order()
    o.action = action
    o.orderType = "MKT"
    o.totalQuantity = TEST_QTY
    o.tif = "OPG"
    return o


def _mtl(action: str) -> Order:
    o = Order()
    o.action = action
    o.orderType = "MTL"
    o.totalQuantity = TEST_QTY
    return o


_CASES: List[OrderTestCase] = [
    OrderTestCase(
        name="market",
        order_type_label="MKT",
        pair_index=0,
        build_long=lambda p: _market("BUY"),
        build_short=lambda p: _market("SELL"),
        fill_timeout=30.0,
        immediate=True,
        notes="Market orders fill immediately at prevailing price",
    ),
    OrderTestCase(
        name="limit",
        order_type_label="LMT",
        pair_index=1,
        build_long=lambda p: _limit("BUY",  p * OFFSET_BELOW),
        build_short=lambda p: _limit("SELL", p * OFFSET_ABOVE),
        fill_timeout=300.0,
        notes="Buy limit 0.5% below market; sell limit 0.5% above",
    ),
    OrderTestCase(
        name="stop",
        order_type_label="STP",
        pair_index=2,
        # BUY stop triggers when price RISES to stop_price
        # SELL stop triggers when price FALLS to stop_price
        build_long=lambda p: _stop("BUY",  p * OFFSET_ABOVE),
        build_short=lambda p: _stop("SELL", p * OFFSET_BELOW),
        fill_timeout=300.0,
        notes="Buy stop 0.5% above; sell stop 0.5% below",
    ),
    OrderTestCase(
        name="stop_limit",
        order_type_label="STP LMT",
        pair_index=3,
        build_long=lambda p: _stop_limit("BUY",
                                          p * OFFSET_ABOVE * 1.005,
                                          p * OFFSET_ABOVE),
        build_short=lambda p: _stop_limit("SELL",
                                           p * OFFSET_BELOW * 0.995,
                                           p * OFFSET_BELOW),
        fill_timeout=300.0,
        notes="Stop triggers at ±0.5%; limit 0.5% beyond stop",
    ),
    OrderTestCase(
        name="market_on_close",
        order_type_label="MOC",
        pair_index=4,
        build_long=lambda p: _moc("BUY"),
        build_short=lambda p: _moc("SELL"),
        fill_timeout=30.0,
        immediate=True,
        notes="MOC executes at the closing auction; both sides fill at close",
    ),
    OrderTestCase(
        name="market_on_open",
        order_type_label="MOO",
        pair_index=5,
        build_long=lambda p: _moo("BUY"),
        build_short=lambda p: _moo("SELL"),
        fill_timeout=30.0,
        immediate=True,
        notes="MOO (MKT+OPG tif) executes at opening auction",
    ),
    OrderTestCase(
        name="market_to_limit",
        order_type_label="MTL",
        pair_index=6,
        build_long=lambda p: _mtl("BUY"),
        build_short=lambda p: _mtl("SELL"),
        fill_timeout=60.0,
        immediate=True,
        notes="MTL becomes a limit order at the execution price if not fully filled",
    ),
]


class PaperTestOrders1Plugin(OrderTestPluginBase):
    """
    Plugin 1 of 5: Basic equity order types.

    Tests: Market, Limit, Stop, Stop-Limit, MOC, MOO, Market-to-Limit.
    Run via: plugin request paper_test_orders_1 run_tests
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
            "paper_test_orders_1",
            base_path, portfolio, shared_holdings, message_bus,
        )

    @property
    def description(self) -> str:
        return (
            "Paper Test Orders 1 – Basic order types: "
            "Market, Limit, Stop, Stop-Limit, MOC, MOO, MTL"
        )
