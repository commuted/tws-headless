"""
order_builder.py - Comprehensive order factory methods

Based on the official IB API Testbed patterns. Provides static factory methods
for creating properly configured Order objects for all supported order types.
"""

from typing import List, Optional, Tuple
from decimal import Decimal
from ibapi.order import Order, OrderComboLeg
from ibapi.tag_value import TagValue


class OrderFactory:
    """Factory methods for creating IB Order objects."""

    # =========================================================================
    # Basic Market Orders
    # =========================================================================

    @staticmethod
    def market(action: str, quantity: Decimal) -> Order:
        """
        Create a market order.

        Args:
            action: "BUY" or "SELL"
            quantity: Number of shares/contracts

        Returns:
            Configured Order object
        """
        order = Order()
        order.action = action
        order.orderType = "MKT"
        order.totalQuantity = quantity
        return order

    @staticmethod
    def market_on_open(action: str, quantity: Decimal) -> Order:
        """Create a market-on-open (MOO) order."""
        order = OrderFactory.market(action, quantity)
        order.tif = "OPG"
        return order

    @staticmethod
    def market_on_close(action: str, quantity: Decimal) -> Order:
        """Create a market-on-close (MOC) order."""
        order = Order()
        order.action = action
        order.orderType = "MOC"
        order.totalQuantity = quantity
        return order

    @staticmethod
    def market_to_limit(action: str, quantity: Decimal) -> Order:
        """Create a market-to-limit (MTL) order."""
        order = Order()
        order.action = action
        order.orderType = "MTL"
        order.totalQuantity = quantity
        return order

    @staticmethod
    def market_with_protection(action: str, quantity: Decimal) -> Order:
        """Create a market with protection order (futures only)."""
        order = Order()
        order.action = action
        order.orderType = "MKT PRT"
        order.totalQuantity = quantity
        return order

    @staticmethod
    def market_if_touched(action: str, quantity: Decimal,
                          trigger_price: float) -> Order:
        """
        Create a market-if-touched (MIT) order.

        Buy MIT: Triggers when price falls to trigger_price
        Sell MIT: Triggers when price rises to trigger_price
        """
        order = Order()
        order.action = action
        order.orderType = "MIT"
        order.totalQuantity = quantity
        order.auxPrice = trigger_price
        return order

    # =========================================================================
    # Limit Orders
    # =========================================================================

    @staticmethod
    def limit(action: str, quantity: Decimal, limit_price: float) -> Order:
        """
        Create a limit order.

        Args:
            action: "BUY" or "SELL"
            quantity: Number of shares/contracts
            limit_price: Limit price

        Returns:
            Configured Order object
        """
        order = Order()
        order.action = action
        order.orderType = "LMT"
        order.totalQuantity = quantity
        order.lmtPrice = limit_price
        return order

    @staticmethod
    def limit_on_open(action: str, quantity: Decimal, limit_price: float) -> Order:
        """Create a limit-on-open (LOO) order."""
        order = OrderFactory.limit(action, quantity, limit_price)
        order.tif = "OPG"
        return order

    @staticmethod
    def limit_on_close(action: str, quantity: Decimal, limit_price: float) -> Order:
        """Create a limit-on-close (LOC) order."""
        order = Order()
        order.action = action
        order.orderType = "LOC"
        order.totalQuantity = quantity
        order.lmtPrice = limit_price
        return order

    @staticmethod
    def limit_if_touched(action: str, quantity: Decimal, limit_price: float,
                         trigger_price: float) -> Order:
        """Create a limit-if-touched (LIT) order."""
        order = Order()
        order.action = action
        order.orderType = "LIT"
        order.totalQuantity = quantity
        order.lmtPrice = limit_price
        order.auxPrice = trigger_price
        return order

    @staticmethod
    def limit_with_cash_qty(action: str, limit_price: float,
                            cash_qty: float) -> Order:
        """Create a limit order with cash quantity (forex)."""
        order = Order()
        order.action = action
        order.orderType = "LMT"
        order.lmtPrice = limit_price
        order.cashQty = cash_qty
        return order

    # =========================================================================
    # Stop Orders
    # =========================================================================

    @staticmethod
    def stop(action: str, quantity: Decimal, stop_price: float) -> Order:
        """
        Create a stop order.

        Args:
            action: "BUY" or "SELL"
            quantity: Number of shares/contracts
            stop_price: Stop trigger price

        Returns:
            Configured Order object
        """
        order = Order()
        order.action = action
        order.orderType = "STP"
        order.totalQuantity = quantity
        order.auxPrice = stop_price
        return order

    @staticmethod
    def stop_limit(action: str, quantity: Decimal, limit_price: float,
                   stop_price: float) -> Order:
        """Create a stop-limit order."""
        order = Order()
        order.action = action
        order.orderType = "STP LMT"
        order.totalQuantity = quantity
        order.lmtPrice = limit_price
        order.auxPrice = stop_price
        return order

    @staticmethod
    def stop_with_protection(action: str, quantity: Decimal,
                             stop_price: float) -> Order:
        """Create a stop with protection order (futures only)."""
        order = Order()
        order.action = action
        order.orderType = "STP PRT"
        order.totalQuantity = quantity
        order.auxPrice = stop_price
        return order

    # =========================================================================
    # Trailing Stop Orders
    # =========================================================================

    @staticmethod
    def trailing_stop(action: str, quantity: Decimal,
                      trail_amount: float = None,
                      trail_percent: float = None) -> Order:
        """
        Create a trailing stop order (by amount or percentage).

        Args:
            action: "BUY" or "SELL"
            quantity: Number of shares/contracts
            trail_amount: Trailing amount in absolute terms
            trail_percent: Trailing amount as percentage

        Returns:
            Configured Order object
        """
        order = Order()
        order.action = action
        order.orderType = "TRAIL"
        order.totalQuantity = quantity
        if trail_amount is not None:
            order.auxPrice = trail_amount
        if trail_percent is not None:
            order.trailingPercent = trail_percent
        return order

    @staticmethod
    def trailing_stop_limit(action: str, quantity: Decimal,
                            trail_amount: float = 0.0,
                            limit_offset: float = 0.0) -> Order:
        """Create a trailing stop-limit order."""
        order = Order()
        order.action = action
        order.orderType = "TRAIL LIMIT"
        order.totalQuantity = quantity
        order.auxPrice = trail_amount
        order.lmtPriceOffset = limit_offset
        return order

    # =========================================================================
    # Pegged Orders
    # =========================================================================

    @staticmethod
    def pegged_to_market(action: str, quantity: Decimal,
                         market_offset: float) -> Order:
        """
        Create a pegged-to-market order.

        Buy: Pegs to ask price - offset
        Sell: Pegs to bid price + offset
        """
        order = Order()
        order.action = action
        order.orderType = "PEG MKT"
        order.totalQuantity = quantity
        order.auxPrice = market_offset
        return order

    @staticmethod
    def pegged_to_midpoint(action: str, quantity: Decimal, offset: float = 0.0,
                           limit_price: float = 0.0) -> Order:
        """Create a pegged-to-midpoint order."""
        order = Order()
        order.action = action
        order.orderType = "PEG MID"
        order.totalQuantity = quantity
        order.auxPrice = offset
        order.lmtPrice = limit_price
        return order

    @staticmethod
    def pegged_to_stock(action: str, quantity: Decimal, delta: float,
                        stock_ref_price: float, starting_price: float) -> Order:
        """Create a pegged-to-stock order (options only)."""
        order = Order()
        order.action = action
        order.orderType = "PEG STK"
        order.totalQuantity = quantity
        order.delta = delta
        order.stockRefPrice = stock_ref_price
        order.startingPrice = starting_price
        return order

    @staticmethod
    def midprice(action: str, quantity: Decimal, price_cap: float) -> Order:
        """Create a midprice order."""
        order = Order()
        order.action = action
        order.orderType = "MIDPRICE"
        order.totalQuantity = quantity
        order.lmtPrice = price_cap
        return order

    # =========================================================================
    # Relative Orders
    # =========================================================================

    @staticmethod
    def relative(action: str, quantity: Decimal, offset_amount: float,
                 price_cap: float = 0.0) -> Order:
        """
        Create a relative (pegged-to-primary) order.

        More aggressive than NBBO, adjusts as market moves.
        """
        order = Order()
        order.action = action
        order.orderType = "REL"
        order.totalQuantity = quantity
        order.lmtPrice = price_cap
        order.auxPrice = offset_amount
        return order

    @staticmethod
    def passive_relative(action: str, quantity: Decimal, offset: float) -> Order:
        """Create a passive relative order (less aggressive than REL)."""
        order = Order()
        order.action = action
        order.orderType = "PASSV REL"
        order.totalQuantity = quantity
        order.auxPrice = offset
        return order

    # =========================================================================
    # Special Order Types
    # =========================================================================

    @staticmethod
    def discretionary(action: str, quantity: Decimal, price: float,
                      discretionary_amount: float) -> Order:
        """Create a discretionary order."""
        order = OrderFactory.limit(action, quantity, price)
        order.discretionaryAmt = discretionary_amount
        return order

    @staticmethod
    def sweep_to_fill(action: str, quantity: Decimal, price: float) -> Order:
        """Create a sweep-to-fill order (speed over price)."""
        order = OrderFactory.limit(action, quantity, price)
        order.sweepToFill = True
        return order

    @staticmethod
    def block(action: str, quantity: Decimal, price: float) -> Order:
        """Create a block order (50+ contracts, ISE options)."""
        order = OrderFactory.limit(action, quantity, price)
        order.blockOrder = True
        return order

    @staticmethod
    def box_top(action: str, quantity: Decimal) -> Order:
        """Create a BOX TOP order (BOX exchange)."""
        order = Order()
        order.action = action
        order.orderType = "BOX TOP"
        order.totalQuantity = quantity
        return order

    @staticmethod
    def volatility(action: str, quantity: Decimal, volatility_percent: float,
                   volatility_type: int) -> Order:
        """
        Create a volatility order (options).

        Args:
            volatility_type: 1=daily, 2=annual
        """
        order = Order()
        order.action = action
        order.orderType = "VOL"
        order.totalQuantity = quantity
        order.volatility = volatility_percent
        order.volatilityType = volatility_type
        return order

    @staticmethod
    def at_auction(action: str, quantity: Decimal, price: float) -> Order:
        """Create an at-auction order (pre-market)."""
        order = Order()
        order.action = action
        order.tif = "AUC"
        order.orderType = "MTL"
        order.totalQuantity = quantity
        order.lmtPrice = price
        return order

    # =========================================================================
    # Bracket Orders
    # =========================================================================

    @staticmethod
    def bracket(parent_order_id: int, action: str, quantity: Decimal,
                entry_price: float, take_profit_price: float,
                stop_loss_price: float) -> Tuple[Order, Order, Order]:
        """
        Create a bracket order (entry + take profit + stop loss).

        Args:
            parent_order_id: Order ID for the parent order
            action: "BUY" or "SELL" for entry
            quantity: Number of shares/contracts
            entry_price: Entry limit price
            take_profit_price: Take profit limit price
            stop_loss_price: Stop loss trigger price

        Returns:
            Tuple of (parent, take_profit, stop_loss) orders
        """
        # Parent entry order
        parent = Order()
        parent.orderId = parent_order_id
        parent.action = action
        parent.orderType = "LMT"
        parent.totalQuantity = quantity
        parent.lmtPrice = entry_price
        parent.transmit = False  # Don't transmit until all orders ready

        # Take profit order
        take_profit = Order()
        take_profit.orderId = parent_order_id + 1
        take_profit.action = "SELL" if action == "BUY" else "BUY"
        take_profit.orderType = "LMT"
        take_profit.totalQuantity = quantity
        take_profit.lmtPrice = take_profit_price
        take_profit.parentId = parent_order_id
        take_profit.transmit = False

        # Stop loss order
        stop_loss = Order()
        stop_loss.orderId = parent_order_id + 2
        stop_loss.action = "SELL" if action == "BUY" else "BUY"
        stop_loss.orderType = "STP"
        stop_loss.auxPrice = stop_loss_price
        stop_loss.totalQuantity = quantity
        stop_loss.parentId = parent_order_id
        stop_loss.transmit = True  # Transmit all orders

        return (parent, take_profit, stop_loss)

    @staticmethod
    def bracket_with_trailing_stop(parent_order_id: int, action: str,
                                   quantity: Decimal, entry_price: float,
                                   take_profit_price: float,
                                   trailing_percent: float) -> Tuple[Order, Order, Order]:
        """
        Create a bracket order with trailing stop instead of fixed stop.

        Returns:
            Tuple of (parent, take_profit, trailing_stop) orders
        """
        parent = Order()
        parent.orderId = parent_order_id
        parent.action = action
        parent.orderType = "LMT"
        parent.totalQuantity = quantity
        parent.lmtPrice = entry_price
        parent.transmit = False

        take_profit = Order()
        take_profit.orderId = parent_order_id + 1
        take_profit.action = "SELL" if action == "BUY" else "BUY"
        take_profit.orderType = "LMT"
        take_profit.totalQuantity = quantity
        take_profit.lmtPrice = take_profit_price
        take_profit.parentId = parent_order_id
        take_profit.transmit = False

        trailing_stop = Order()
        trailing_stop.orderId = parent_order_id + 2
        trailing_stop.action = "SELL" if action == "BUY" else "BUY"
        trailing_stop.orderType = "TRAIL"
        trailing_stop.totalQuantity = quantity
        trailing_stop.trailingPercent = trailing_percent
        trailing_stop.parentId = parent_order_id
        trailing_stop.transmit = True

        return (parent, take_profit, trailing_stop)

    # =========================================================================
    # OCA (One-Cancels-All) Orders
    # =========================================================================

    @staticmethod
    def one_cancels_all(oca_group: str, orders: List[Order],
                        oca_type: int = 1) -> List[Order]:
        """
        Apply OCA grouping to a list of orders.

        Args:
            oca_group: Unique OCA group identifier
            orders: List of orders to group
            oca_type: 1=Cancel all, 2=Reduce qty, 3=Reduce each

        Returns:
            List of orders with OCA attributes set
        """
        for order in orders:
            order.ocaGroup = oca_group
            order.ocaType = oca_type
        return orders

    # =========================================================================
    # Combo/Spread Orders
    # =========================================================================

    @staticmethod
    def combo_limit(action: str, quantity: Decimal, limit_price: float,
                    non_guaranteed: bool = True) -> Order:
        """Create a limit order for a combo/spread."""
        order = Order()
        order.action = action
        order.orderType = "LMT"
        order.totalQuantity = quantity
        order.lmtPrice = limit_price
        if non_guaranteed:
            order.smartComboRoutingParams = [TagValue("NonGuaranteed", "1")]
        return order

    @staticmethod
    def combo_market(action: str, quantity: Decimal,
                     non_guaranteed: bool = True) -> Order:
        """Create a market order for a combo/spread."""
        order = Order()
        order.action = action
        order.orderType = "MKT"
        order.totalQuantity = quantity
        if non_guaranteed:
            order.smartComboRoutingParams = [TagValue("NonGuaranteed", "1")]
        return order

    @staticmethod
    def combo_limit_with_leg_prices(action: str, quantity: Decimal,
                                    leg_prices: List[float],
                                    non_guaranteed: bool = True) -> Order:
        """Create a limit order with per-leg prices."""
        order = Order()
        order.action = action
        order.orderType = "LMT"
        order.totalQuantity = quantity
        order.orderComboLegs = []
        for price in leg_prices:
            combo_leg = OrderComboLeg()
            combo_leg.price = price
            order.orderComboLegs.append(combo_leg)
        if non_guaranteed:
            order.smartComboRoutingParams = [TagValue("NonGuaranteed", "1")]
        return order

    @staticmethod
    def relative_limit_combo(action: str, quantity: Decimal, limit_price: float,
                             non_guaranteed: bool = True) -> Order:
        """Create a relative + limit combo order."""
        order = Order()
        order.action = action
        order.totalQuantity = quantity
        order.orderType = "REL + LMT"
        order.lmtPrice = limit_price
        if non_guaranteed:
            order.smartComboRoutingParams = [TagValue("NonGuaranteed", "1")]
        return order

    @staticmethod
    def relative_market_combo(action: str, quantity: Decimal,
                              non_guaranteed: bool = True) -> Order:
        """Create a relative + market combo order."""
        order = Order()
        order.action = action
        order.totalQuantity = quantity
        order.orderType = "REL + MKT"
        if non_guaranteed:
            order.smartComboRoutingParams = [TagValue("NonGuaranteed", "1")]
        return order

    # =========================================================================
    # Hedge Orders
    # =========================================================================

    @staticmethod
    def fx_hedge(parent_order_id: int, action: str) -> Order:
        """Create an FX hedge order (attached to parent)."""
        order = OrderFactory.market(action, Decimal(0))  # FX hedges have qty=0
        order.parentId = parent_order_id
        order.hedgeType = "F"
        return order

    # =========================================================================
    # Order Modifiers
    # =========================================================================

    @staticmethod
    def set_good_till_date(order: Order, gtd: str) -> Order:
        """Set good-till-date on an order (format: YYYYMMDD-HH:MM:SS in UTC)."""
        order.goodTillDate = gtd
        order.tif = "GTD"
        return order

    @staticmethod
    def set_good_after_time(order: Order, gat: str) -> Order:
        """Set good-after-time on an order (format: YYYYMMDD-HH:MM:SS in UTC)."""
        order.goodAfterTime = gat
        return order

    @staticmethod
    def set_outside_rth(order: Order, outside_rth: bool = True) -> Order:
        """Allow order to execute outside regular trading hours."""
        order.outsideRth = outside_rth
        return order

    @staticmethod
    def set_all_or_none(order: Order, all_or_none: bool = True) -> Order:
        """Set all-or-none flag on an order."""
        order.allOrNone = all_or_none
        return order

    @staticmethod
    def set_hidden(order: Order, hidden: bool = True) -> Order:
        """Set hidden flag on an order."""
        order.hidden = hidden
        return order

    @staticmethod
    def set_min_qty(order: Order, min_qty: int) -> Order:
        """Set minimum quantity for partial fills."""
        order.minQty = min_qty
        return order

    @staticmethod
    def set_display_size(order: Order, display_size: int) -> Order:
        """Set display size (iceberg order)."""
        order.displaySize = display_size
        return order

    @staticmethod
    def set_account(order: Order, account: str) -> Order:
        """Set target account for the order."""
        order.account = account
        return order

    @staticmethod
    def set_fa_allocation(order: Order, fa_group: str, fa_method: str,
                          fa_percentage: str = "") -> Order:
        """Set FA allocation parameters."""
        order.faGroup = fa_group
        order.faMethod = fa_method
        if fa_percentage:
            order.faPercentage = fa_percentage
        return order

    # =========================================================================
    # Adaptive Order Helper
    # =========================================================================

    @staticmethod
    def make_adaptive(order: Order, priority: str = "Normal") -> Order:
        """
        Convert an order to use IB's Adaptive algo.

        Args:
            order: Base order to convert
            priority: "Patient", "Normal", or "Urgent"

        Returns:
            Order with Adaptive algo configured
        """
        order.algoStrategy = "Adaptive"
        order.algoParams = [TagValue("adaptivePriority", priority)]
        return order
