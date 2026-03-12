"""
paper_test_orders_3/plugin.py – Pegged and trailing-variant order types

Tests: Trailing-Stop-Limit, Pegged-to-Market, Relative/Pegged-to-Primary,
       Passive-Relative, Pegged-to-Midpoint (IBKRATS), Adjusted Order

At-Auction (MTL+AUC) has been moved to paper_test_orders_open, as it must
be submitted within the pre-open window.
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


def _trail_limit(action: str, trail_amt: float, lmt_offset: float) -> Order:
    """Trailing stop-limit."""
    o = Order()
    o.action = action
    o.orderType = "TRAIL LIMIT"
    o.totalQuantity = TEST_QTY
    o.auxPrice = round(trail_amt, 4)        # trailing amount
    o.lmtPriceOffset = round(lmt_offset, 4) # offset beyond trail stop
    return o


def _peg_mkt(action: str, offset: float) -> Order:
    """Pegged-to-Market: pegs to bid (sell) or ask (buy) with an offset."""
    o = Order()
    o.action = action
    o.orderType = "PEG MKT"
    o.totalQuantity = TEST_QTY
    o.auxPrice = round(offset, 4)
    return o


def _relative(action: str, price_cap: float, offset: float) -> Order:
    """Relative / Pegged-to-Primary (REL)."""
    o = Order()
    o.action = action
    o.orderType = "REL"
    o.totalQuantity = TEST_QTY
    o.lmtPrice = round(price_cap, 2)   # cap (0 = no cap)
    o.auxPrice = round(offset, 4)      # offset from primary
    return o


def _passive_relative(action: str, offset: float) -> Order:
    """Passive Relative – less aggressive than REL."""
    o = Order()
    o.action = action
    o.orderType = "PASSV REL"
    o.totalQuantity = TEST_QTY
    o.auxPrice = round(offset, 4)
    return o


def _peg_mid(action: str, offset: float, price_cap: float) -> Order:
    """Pegged-to-Midpoint (IBKRATS / PEG MID)."""
    o = Order()
    o.action = action
    o.orderType = "PEG MID"
    o.totalQuantity = TEST_QTY
    o.auxPrice = round(offset, 4)
    o.lmtPrice = round(price_cap, 2)
    return o


def _adjusted_trail(action: str, stop_price: float, trail_amt: float,
                    trigger_price: float) -> Order:
    """
    Adjusted Order: starts as a Stop, then adjusts to a Trailing Stop
    when the trigger price is hit.
    """
    o = Order()
    o.action = action
    o.orderType = "STP"
    o.totalQuantity = TEST_QTY
    o.auxPrice = round(stop_price, 2)
    o.adjustedOrderType = "TRAIL"
    o.triggerPrice = round(trigger_price, 2)
    o.adjustedTrailingAmount = round(trail_amt, 4)
    return o


_CASES: List[OrderTestCase] = [
    OrderTestCase(
        name="trailing_stop_limit",
        order_type_label="TRAIL LIMIT",
        pair_index=0,
        # SELL trail-limit to protect a long; BUY trail-limit to protect a short
        build_long=lambda p: _trail_limit("SELL", p * 0.01, p * 0.002),
        build_short=lambda p: _trail_limit("BUY",  p * 0.01, p * 0.002),
        fill_timeout=300.0,
        notes="Trail amount = 1% of price; limit offset = 0.2%",
    ),
    OrderTestCase(
        name="pegged_to_market",
        order_type_label="PEG MKT",
        pair_index=1,
        # Offset is the amount from the NBBO; 0.01 = 1 cent aggressive
        build_long=lambda p: _peg_mkt("BUY",  0.01),
        build_short=lambda p: _peg_mkt("SELL", 0.01),
        fill_timeout=120.0,
        notes="Pegs to ask (buy) or bid (sell) with 0.01 offset",
    ),
    OrderTestCase(
        name="relative_pegged_to_primary",
        order_type_label="REL",
        pair_index=2,
        build_long=lambda p: _relative("BUY",  p * OFFSET_BELOW, 0.01),
        build_short=lambda p: _relative("SELL", p * OFFSET_ABOVE, 0.01),
        fill_timeout=300.0,
        notes="REL: pegs to primary with 0.01 offset; cap at ±0.5% from market",
    ),
    OrderTestCase(
        name="passive_relative",
        order_type_label="PASSV REL",
        pair_index=3,
        build_long=lambda p: _passive_relative("BUY",  0.01),
        build_short=lambda p: _passive_relative("SELL", 0.01),
        fill_timeout=300.0,
        notes="Passive relative: less aggressive than REL; 0.01 offset",
    ),
    OrderTestCase(
        name="pegged_to_midpoint_ibkrats",
        order_type_label="PEG MID",
        pair_index=4,
        # offset = 0 (at midpoint); cap = 0.5% above/below for safety
        build_long=lambda p: _peg_mid("BUY",  0.0, p * 1.005),
        build_short=lambda p: _peg_mid("SELL", 0.0, p * 0.995),
        fill_timeout=120.0,
        notes="IBKRATS/PEG MID: executes at bid-ask midpoint; cap at ±0.5%",
    ),
    OrderTestCase(
        name="adjusted_stop_to_trail",
        order_type_label="STP→TRAIL",
        pair_index=6,
        # Start as a stop; when trigger is hit, become a 1% trailing stop
        # SELL: stop at -1%, trigger at -0.5%, trail 1%
        build_long=lambda p: _adjusted_trail(
            "SELL",
            stop_price=p * 0.99,
            trail_amt=p * 0.01,
            trigger_price=p * 0.995,
        ),
        build_short=lambda p: _adjusted_trail(
            "BUY",
            stop_price=p * 1.01,
            trail_amt=p * 0.01,
            trigger_price=p * 1.005,
        ),
        fill_timeout=300.0,
        notes="Adjusted order: STP that converts to TRAIL when trigger price is hit",
    ),
]


class PaperTestOrders3Plugin(OrderTestPluginBase):
    """
    Plugin 3 of 5: Pegged and trailing-variant order types.

    Tests: Trailing-Stop-Limit, Pegged-to-Market, REL, Passive-REL,
           PEG MID (IBKRATS), Adjusted Order (STP→TRAIL).
    (At-Auction moved to paper_test_orders_open)
    Run via: plugin request paper_test_orders_3 run_tests
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
            "paper_test_orders_3",
            base_path, portfolio, shared_holdings, message_bus,
        )

    @property
    def description(self) -> str:
        return (
            "Paper Test Orders 3 – Pegged & trailing: "
            "Trail-Limit, PEG MKT, REL, PASSV REL, PEG MID, Adjusted"
        )
