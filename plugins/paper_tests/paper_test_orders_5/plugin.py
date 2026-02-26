"""
paper_test_orders_5/plugin.py – Remaining conditions and stub order types

Tests (real):
  Margin Condition, Price-Change Condition, Percent-Change Condition

Stubs (not directly testable for single-leg equity orders):
  Relative Limit Combo   – requires a BAG (spread) contract with two legs
  Relative Market Combo  – requires a BAG contract
  Combo Limit            – requires a BAG contract
  Hedging Order          – FX/beta/delta hedge; requires a correlated parent

NOT IMPLEMENTED (canonical list item):
  Pegged to Benchmark    – only available for options and bonds, not equities

NOTE on conIds: MarginCondition needs no conId.  PriceCondition and
PercentChangeCondition need conIds; we use SPY=756733 as the reference.
Verify these values for your IB instance via reqContractDetails.
"""

import time
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional

from ibapi.order import Order
from ibapi.order_condition import (
    MarginCondition,
    PercentChangeCondition,
    PriceCondition,
)

from plugins.paper_tests.order_test_base import (
    OFFSET_ABOVE,
    OFFSET_BELOW,
    TEST_QTY,
    OrderTestCase,
    OrderTestPluginBase,
    TradeSignal,
    make_stk_contract,
)

_SPY_CON_ID = 756733


def _lmt(action: str, price: float) -> Order:
    o = Order()
    o.action = action
    o.orderType = "LMT"
    o.totalQuantity = TEST_QTY
    o.lmtPrice = round(price, 2)
    return o


def _lmt_margin_cond(action: str, price: float, pct: float,
                      is_more: bool) -> Order:
    """Limit order conditional on margin cushion %."""
    o = _lmt(action, price)
    cond = MarginCondition()
    cond.isMore = is_more
    cond.percent = pct
    o.conditions = [cond]
    o.conditionIgnoreRth = True
    return o


def _lmt_price_change_cond(action: str, price: float,
                             change_pct: float, con_id: int,
                             is_more: bool) -> Order:
    """
    Limit order conditional on price change % of a reference security.
    PercentChangeCondition triggers when the ref security changes by ≥ X%.
    """
    o = _lmt(action, price)
    cond = PercentChangeCondition()
    cond.isMore = is_more
    cond.changePercent = change_pct
    cond.conId = con_id
    cond.exchange = "SMART"
    o.conditions = [cond]
    o.conditionIgnoreRth = True
    return o


_CASES: List[OrderTestCase] = [
    # -----------------------------------------------------------------------
    # Real – Margin Condition
    # -----------------------------------------------------------------------
    OrderTestCase(
        name="margin_condition",
        order_type_label="LMT+MARGIN_COND",
        pair_index=0,
        # Execute when margin cushion > 50% (very likely on a paper account)
        build_long=lambda p: _lmt_margin_cond("BUY",  p * OFFSET_BELOW, 50.0, True),
        build_short=lambda p: _lmt_margin_cond("SELL", p * OFFSET_ABOVE, 50.0, True),
        fill_timeout=300.0,
        notes="MarginCondition: fires when margin cushion % > 50. No conId needed.",
    ),
    # -----------------------------------------------------------------------
    # Real – Price-Change Condition (uses PercentChangeCondition on SPY)
    # -----------------------------------------------------------------------
    OrderTestCase(
        name="price_change_condition",
        order_type_label="LMT+PRICE_CHANGE_COND",
        pair_index=1,
        # Trigger when SPY changes by ≥ 0.1% in either direction
        build_long=lambda p: _lmt_price_change_cond(
            "BUY", p * OFFSET_BELOW, 0.1, _SPY_CON_ID, is_more=True,
        ),
        build_short=lambda p: _lmt_price_change_cond(
            "SELL", p * OFFSET_ABOVE, 0.1, _SPY_CON_ID, is_more=False,
        ),
        fill_timeout=300.0,
        notes=(
            f"PercentChangeCondition: fires when SPY changes ≥ 0.1%. "
            f"conId={_SPY_CON_ID} – verify for your IB instance."
        ),
    ),
    # -----------------------------------------------------------------------
    # Real – Percent-Change Condition (reuses PercentChangeCondition)
    # -----------------------------------------------------------------------
    OrderTestCase(
        name="percent_change_condition",
        order_type_label="LMT+PCT_CHANGE_COND",
        pair_index=2,
        # Same as price_change_condition but with a 0.5% threshold
        build_long=lambda p: _lmt_price_change_cond(
            "BUY", p * OFFSET_BELOW, 0.5, _SPY_CON_ID, is_more=True,
        ),
        build_short=lambda p: _lmt_price_change_cond(
            "SELL", p * OFFSET_ABOVE, 0.5, _SPY_CON_ID, is_more=False,
        ),
        fill_timeout=300.0,
        notes=(
            f"PercentChangeCondition 0.5% threshold on SPY. "
            f"conId={_SPY_CON_ID} – verify for your IB instance."
        ),
    ),
    # -----------------------------------------------------------------------
    # Stub – Relative Limit Combo
    # -----------------------------------------------------------------------
    OrderTestCase(
        name="relative_limit_combo",
        order_type_label="REL+LMT",
        pair_index=3,
        build_long=lambda p: Order(),   # never called
        build_short=lambda p: Order(),
        is_stub=True,
        stub_reason=(
            "REL+LMT combo requires a BAG (spread) contract built from two "
            "option or futures legs with matching ComboLeg definitions. "
            "Not applicable to single-leg equity orders."
        ),
        notes="Use OrderFactory.relative_limit_combo() with a BAG contract.",
    ),
    # -----------------------------------------------------------------------
    # Stub – Relative Market Combo
    # -----------------------------------------------------------------------
    OrderTestCase(
        name="relative_market_combo",
        order_type_label="REL+MKT",
        pair_index=4,
        build_long=lambda p: Order(),
        build_short=lambda p: Order(),
        is_stub=True,
        stub_reason=(
            "REL+MKT combo requires a BAG contract. "
            "Not applicable to single-leg equity orders."
        ),
        notes="Use OrderFactory.relative_market_combo() with a BAG contract.",
    ),
    # -----------------------------------------------------------------------
    # Stub – Combo Limit
    # -----------------------------------------------------------------------
    OrderTestCase(
        name="combo_limit",
        order_type_label="COMBO_LMT",
        pair_index=5,
        build_long=lambda p: Order(),
        build_short=lambda p: Order(),
        is_stub=True,
        stub_reason=(
            "Combo limit orders require a BAG secType contract with legs. "
            "Applicable to equity-pair spreads but requires BAG contract setup "
            "with matching conIds and exchange routing for each leg."
        ),
        notes=(
            "Use OrderFactory.combo_limit() with a BAG contract. "
            "See IB API docs: reqContractDetails with secType=BAG."
        ),
    ),
    # -----------------------------------------------------------------------
    # Stub – Hedging Order
    # -----------------------------------------------------------------------
    OrderTestCase(
        name="hedging_order",
        order_type_label="HEDGE",
        pair_index=6,
        build_long=lambda p: Order(),
        build_short=lambda p: Order(),
        is_stub=True,
        stub_reason=(
            "IB hedging orders (hedgeType=F/B/D/P) attach to a parent order. "
            "FX hedges (F) auto-size the FX trade to match the parent equity fill. "
            "Beta hedges (B) and delta hedges (D) require a correlated instrument "
            "and a pre-computed beta/delta value. "
            "These are not self-contained order types for a single equity leg."
        ),
        notes=(
            "Implement by placing a parent equity order with transmit=False, "
            "then a child order with hedgeType set and parentId referencing "
            "the parent. Transmit on the child."
        ),
    ),
]

# ---------------------------------------------------------------------------
# NOT IMPLEMENTED (canonical list item)
# ---------------------------------------------------------------------------
# Pegged to Benchmark (IB orderType="PEG BENCH"):
#   Available only for options and bonds. The option/bond price is benchmarked
#   against a reference price (e.g. the underlying), with optional offset and cap.
#   Not supported for plain equity (STK) orders.
#
# Combo Limit with Price per Leg:
#   Same BAG requirement as Combo Limit above, plus per-leg OrderComboLeg prices.
#   Implemented in OrderFactory.combo_limit_with_leg_prices(); requires BAG.


class PaperTestOrders5Plugin(OrderTestPluginBase):
    """
    Plugin 5 of 5: Remaining conditions and stub order types.

    Real tests:  Margin Condition, Price-Change Condition, %-Change Condition.
    Stubs:       REL+LMT combo, REL+MKT combo, Combo Limit, Hedging Order.
    Not impl:    Pegged to Benchmark (options/bonds only).

    Run via: plugin request paper_test_orders_5 run_tests
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
            "paper_test_orders_5",
            base_path, portfolio, shared_holdings, message_bus,
        )

    @property
    def description(self) -> str:
        return (
            "Paper Test Orders 5 – Conditions + stubs: "
            "Margin/PriceChange/PctChange conditions; "
            "stubs for combo and hedge order types"
        )
