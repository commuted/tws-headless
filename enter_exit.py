"""
enter_exit.py - Advanced order entry and exit management

Provides a wrapper around buy/sell operations with support for complex
order types, bracket orders, scaled entries, and probability-based
position management.
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import uuid

from ibapi.contract import Contract
from ibapi.order import Order

from models import OrderAction, OrderRecord, OrderStatus
from portfolio import Portfolio

logger = logging.getLogger(__name__)


# =============================================================================
# Enums and Constants
# =============================================================================

class OrderType(Enum):
    """Supported order types"""
    MARKET = "MKT"
    LIMIT = "LMT"
    STOP = "STP"
    STOP_LIMIT = "STP LMT"
    TRAILING_STOP = "TRAIL"
    TRAILING_STOP_LIMIT = "TRAIL LIMIT"
    MARKET_ON_CLOSE = "MOC"
    LIMIT_ON_CLOSE = "LOC"
    MARKET_ON_OPEN = "MOO"
    LIMIT_ON_OPEN = "LOO"
    MIDPRICE = "MIDPRICE"
    ADAPTIVE = "MKT"  # Uses adaptive algo


class AlgoStrategy(Enum):
    """IB Algorithmic order strategies"""
    ADAPTIVE = "Adaptive"
    TWAP = "Twap"
    VWAP = "Vwap"
    ARRIVAL_PRICE = "ArrivalPx"
    DARK_ICE = "DarkIce"
    PERCENT_OF_VOLUME = "PctVol"
    CLOSE_PRICE = "ClosePx"


class TimeInForce(Enum):
    """Time in force options"""
    DAY = "DAY"
    GTC = "GTC"  # Good Till Cancelled
    IOC = "IOC"  # Immediate or Cancel
    FOK = "FOK"  # Fill or Kill
    GTD = "GTD"  # Good Till Date
    OPG = "OPG"  # At the Opening
    DTC = "DTC"  # Day Till Cancelled


# =============================================================================
# Order Configuration Data Classes
# =============================================================================

@dataclass
class OrderConfig:
    """Configuration for a single order"""
    order_type: OrderType = OrderType.MARKET
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    trail_amount: Optional[float] = None  # Absolute trail amount
    trail_percent: Optional[float] = None  # Trail as percentage
    time_in_force: TimeInForce = TimeInForce.DAY
    outside_rth: bool = False  # Allow outside regular trading hours
    all_or_none: bool = False
    hidden: bool = False  # Iceberg order
    display_size: Optional[int] = None  # Visible quantity for iceberg


@dataclass
class BracketConfig:
    """Configuration for bracket orders (entry + profit target + stop loss)"""
    profit_target_pct: Optional[float] = None  # Take profit at X% gain
    profit_target_price: Optional[float] = None  # Absolute take profit price
    stop_loss_pct: Optional[float] = None  # Stop loss at X% loss
    stop_loss_price: Optional[float] = None  # Absolute stop loss price
    trailing_stop: bool = False  # Use trailing stop instead of fixed
    trail_amount: Optional[float] = None
    trail_percent: Optional[float] = None


@dataclass
class ScaledOrderConfig:
    """Configuration for scaled entry/exit (multiple orders at different prices)"""
    num_orders: int = 3
    price_increment_pct: float = 0.5  # Spacing between orders
    quantity_distribution: str = "equal"  # equal, pyramid, inverse_pyramid
    start_price: Optional[float] = None  # First order price (or use current)


@dataclass
class AdaptiveConfig:
    """Configuration for adaptive algorithmic orders"""
    strategy: AlgoStrategy = AlgoStrategy.ADAPTIVE
    urgency: str = "Normal"  # Patient, Normal, Urgent
    start_time: Optional[str] = None  # HH:MM:SS format
    end_time: Optional[str] = None
    max_pct_volume: Optional[float] = None  # For VWAP/PctVol


@dataclass
class EntryExitResult:
    """Result of an entry/exit operation"""
    success: bool
    order_ids: List[int] = field(default_factory=list)
    parent_order_id: Optional[int] = None
    profit_order_id: Optional[int] = None
    stop_order_id: Optional[int] = None
    oca_group: Optional[str] = None
    message: str = ""
    error: Optional[str] = None

    @property
    def total_orders(self) -> int:
        return len(self.order_ids)


# =============================================================================
# Order Builders
# =============================================================================

class OrderBuilder:
    """
    Builder for creating IB Order objects with various configurations.
    """

    @staticmethod
    def create_base_order(
        action: str,
        quantity: int,
        order_type: OrderType = OrderType.MARKET,
        tif: TimeInForce = TimeInForce.DAY,
    ) -> Order:
        """Create a base order with common settings"""
        order = Order()
        order.action = action
        order.totalQuantity = quantity
        order.orderType = order_type.value
        order.tif = tif.value
        order.transmit = True
        return order

    @staticmethod
    def market_order(action: str, quantity: int) -> Order:
        """Create a market order"""
        return OrderBuilder.create_base_order(action, quantity, OrderType.MARKET)

    @staticmethod
    def limit_order(
        action: str,
        quantity: int,
        limit_price: float,
        tif: TimeInForce = TimeInForce.DAY,
    ) -> Order:
        """Create a limit order"""
        order = OrderBuilder.create_base_order(action, quantity, OrderType.LIMIT, tif)
        order.lmtPrice = limit_price
        return order

    @staticmethod
    def stop_order(
        action: str,
        quantity: int,
        stop_price: float,
        tif: TimeInForce = TimeInForce.GTC,
    ) -> Order:
        """Create a stop order"""
        order = OrderBuilder.create_base_order(action, quantity, OrderType.STOP, tif)
        order.auxPrice = stop_price
        return order

    @staticmethod
    def stop_limit_order(
        action: str,
        quantity: int,
        stop_price: float,
        limit_price: float,
        tif: TimeInForce = TimeInForce.GTC,
    ) -> Order:
        """Create a stop-limit order"""
        order = OrderBuilder.create_base_order(action, quantity, OrderType.STOP_LIMIT, tif)
        order.auxPrice = stop_price
        order.lmtPrice = limit_price
        return order

    @staticmethod
    def trailing_stop_order(
        action: str,
        quantity: int,
        trail_amount: Optional[float] = None,
        trail_percent: Optional[float] = None,
        tif: TimeInForce = TimeInForce.GTC,
    ) -> Order:
        """Create a trailing stop order"""
        order = OrderBuilder.create_base_order(action, quantity, OrderType.TRAILING_STOP, tif)
        if trail_percent is not None:
            order.trailingPercent = trail_percent
        elif trail_amount is not None:
            order.auxPrice = trail_amount
        return order

    @staticmethod
    def trailing_stop_limit_order(
        action: str,
        quantity: int,
        trail_amount: float,
        limit_offset: float,
        tif: TimeInForce = TimeInForce.GTC,
    ) -> Order:
        """Create a trailing stop-limit order"""
        order = OrderBuilder.create_base_order(action, quantity, OrderType.TRAILING_STOP_LIMIT, tif)
        order.auxPrice = trail_amount
        order.lmtPriceOffset = limit_offset
        return order

    @staticmethod
    def adaptive_order(
        action: str,
        quantity: int,
        order_type: OrderType = OrderType.MARKET,
        limit_price: Optional[float] = None,
        urgency: str = "Normal",
    ) -> Order:
        """Create an adaptive algorithmic order for better fills"""
        order = OrderBuilder.create_base_order(action, quantity, order_type)
        if limit_price is not None:
            order.lmtPrice = limit_price
        order.algoStrategy = "Adaptive"
        order.algoParams = []
        order.algoParams.append(("adaptivePriority", urgency))
        return order

    @staticmethod
    def twap_order(
        action: str,
        quantity: int,
        start_time: str,
        end_time: str,
        limit_price: Optional[float] = None,
    ) -> Order:
        """Create a TWAP (Time-Weighted Average Price) order"""
        order_type = OrderType.LIMIT if limit_price else OrderType.MARKET
        order = OrderBuilder.create_base_order(action, quantity, order_type)
        if limit_price:
            order.lmtPrice = limit_price
        order.algoStrategy = "Twap"
        order.algoParams = []
        order.algoParams.append(("startTime", start_time))
        order.algoParams.append(("endTime", end_time))
        order.algoParams.append(("allowPastEndTime", "1"))
        return order

    @staticmethod
    def vwap_order(
        action: str,
        quantity: int,
        start_time: str,
        end_time: str,
        max_pct_volume: float = 0.1,
        limit_price: Optional[float] = None,
    ) -> Order:
        """Create a VWAP (Volume-Weighted Average Price) order"""
        order_type = OrderType.LIMIT if limit_price else OrderType.MARKET
        order = OrderBuilder.create_base_order(action, quantity, order_type)
        if limit_price:
            order.lmtPrice = limit_price
        order.algoStrategy = "Vwap"
        order.algoParams = []
        order.algoParams.append(("startTime", start_time))
        order.algoParams.append(("endTime", end_time))
        order.algoParams.append(("maxPctVol", str(max_pct_volume)))
        order.algoParams.append(("allowPastEndTime", "1"))
        return order

    @staticmethod
    def iceberg_order(
        action: str,
        quantity: int,
        limit_price: float,
        display_size: int,
    ) -> Order:
        """Create an iceberg (hidden) order"""
        order = OrderBuilder.limit_order(action, quantity, limit_price)
        order.displaySize = display_size
        return order

    @staticmethod
    def midprice_order(
        action: str,
        quantity: int,
        price_cap: Optional[float] = None,
    ) -> Order:
        """Create a midprice order (pegged to midpoint)"""
        order = OrderBuilder.create_base_order(action, quantity, OrderType.MIDPRICE)
        if price_cap:
            order.lmtPrice = price_cap
        return order


# =============================================================================
# Enter/Exit Manager
# =============================================================================

class EnterExit:
    """
    Advanced order entry and exit manager.

    Provides high-level methods for entering and exiting positions
    using complex order types, bracket orders, and probability-based
    strategies.

    Usage:
        ee = EnterExit(portfolio)

        # Simple entry
        result = ee.enter(contract, quantity=100, order_type=OrderType.LIMIT, limit_price=450.0)

        # Bracket entry with profit target and stop loss
        result = ee.enter_bracket(
            contract,
            quantity=100,
            entry_price=450.0,
            profit_target_pct=5.0,
            stop_loss_pct=2.0,
        )

        # Scaled entry at multiple price levels
        result = ee.enter_scaled(
            contract,
            total_quantity=300,
            num_orders=3,
            base_price=450.0,
            price_increment_pct=0.5,
        )
    """

    def __init__(self, portfolio: Portfolio):
        """
        Initialize the EnterExit manager.

        Args:
            portfolio: Portfolio instance for placing orders
        """
        self.portfolio = portfolio
        self._oca_counter = 0
        self._active_brackets: Dict[str, Dict[str, Any]] = {}

    def _generate_oca_group(self) -> str:
        """Generate a unique OCA group ID"""
        self._oca_counter += 1
        return f"OCA_{datetime.now().strftime('%Y%m%d%H%M%S')}_{self._oca_counter}"

    def _get_next_order_id(self) -> Optional[int]:
        """Get the next available order ID from portfolio"""
        if not self.portfolio.connected:
            return None
        return self.portfolio._next_order_id

    def _place_order(self, contract: Contract, order: Order) -> Optional[int]:
        """Place an order through the portfolio"""
        if not self.portfolio.connected:
            logger.error("Not connected to IB")
            return None

        order_id = self._get_next_order_id()
        if order_id is None:
            logger.error("No order ID available")
            return None

        try:
            self.portfolio._next_order_id += 1
            self.portfolio.placeOrder(order_id, contract, order)

            # Track order
            record = OrderRecord(
                order_id=order_id,
                symbol=contract.symbol,
                action=order.action,
                quantity=int(order.totalQuantity),
                order_type=order.orderType,
            )
            self.portfolio._orders[order_id] = record

            logger.info(f"Placed order {order_id}: {order.action} {order.totalQuantity} {contract.symbol}")
            return order_id

        except Exception as e:
            logger.error(f"Failed to place order: {e}")
            return None

    # =========================================================================
    # Basic Entry/Exit Methods
    # =========================================================================

    def enter(
        self,
        contract: Contract,
        quantity: int,
        action: str = "BUY",
        config: Optional[OrderConfig] = None,
    ) -> EntryExitResult:
        """
        Enter a position with configurable order type.

        Args:
            contract: IB Contract to trade
            quantity: Number of shares/contracts
            action: BUY or SELL
            config: Order configuration (defaults to market order)

        Returns:
            EntryExitResult with order details
        """
        config = config or OrderConfig()

        # Build order based on type
        if config.order_type == OrderType.MARKET:
            order = OrderBuilder.market_order(action, quantity)
        elif config.order_type == OrderType.LIMIT:
            if config.limit_price is None:
                return EntryExitResult(False, error="Limit price required for limit order")
            order = OrderBuilder.limit_order(action, quantity, config.limit_price, config.time_in_force)
        elif config.order_type == OrderType.STOP:
            if config.stop_price is None:
                return EntryExitResult(False, error="Stop price required for stop order")
            order = OrderBuilder.stop_order(action, quantity, config.stop_price, config.time_in_force)
        elif config.order_type == OrderType.STOP_LIMIT:
            if config.stop_price is None or config.limit_price is None:
                return EntryExitResult(False, error="Stop and limit prices required")
            order = OrderBuilder.stop_limit_order(
                action, quantity, config.stop_price, config.limit_price, config.time_in_force
            )
        elif config.order_type == OrderType.TRAILING_STOP:
            order = OrderBuilder.trailing_stop_order(
                action, quantity, config.trail_amount, config.trail_percent, config.time_in_force
            )
        elif config.order_type == OrderType.MIDPRICE:
            order = OrderBuilder.midprice_order(action, quantity, config.limit_price)
        else:
            order = OrderBuilder.market_order(action, quantity)

        # Apply additional settings
        order.outsideRth = config.outside_rth
        order.allOrNone = config.all_or_none
        if config.hidden and config.display_size:
            order.displaySize = config.display_size

        # Place the order
        order_id = self._place_order(contract, order)
        if order_id is None:
            return EntryExitResult(False, error="Failed to place order")

        return EntryExitResult(
            success=True,
            order_ids=[order_id],
            message=f"Entered {action} {quantity} {contract.symbol}",
        )

    def exit(
        self,
        contract: Contract,
        quantity: int,
        config: Optional[OrderConfig] = None,
    ) -> EntryExitResult:
        """
        Exit a position (convenience wrapper for enter with opposite action).

        Args:
            contract: IB Contract to trade
            quantity: Number of shares/contracts to exit
            config: Order configuration

        Returns:
            EntryExitResult with order details
        """
        return self.enter(contract, quantity, action="SELL", config=config)

    # =========================================================================
    # Bracket Orders
    # =========================================================================

    def enter_bracket(
        self,
        contract: Contract,
        quantity: int,
        entry_price: Optional[float] = None,
        entry_type: OrderType = OrderType.LIMIT,
        bracket_config: Optional[BracketConfig] = None,
    ) -> EntryExitResult:
        """
        Enter a position with automatic profit target and stop loss orders.

        Creates a bracket order consisting of:
        - Parent: Entry order (limit or market)
        - Profit Target: Limit sell order
        - Stop Loss: Stop or trailing stop order

        Args:
            contract: IB Contract to trade
            quantity: Number of shares/contracts
            entry_price: Entry price (required for limit entry)
            entry_type: Type of entry order
            bracket_config: Bracket configuration for TP/SL

        Returns:
            EntryExitResult with all order IDs
        """
        config = bracket_config or BracketConfig()

        if entry_type == OrderType.LIMIT and entry_price is None:
            return EntryExitResult(False, error="Entry price required for limit entry")

        # Use current price if not specified
        if entry_price is None:
            pos = self.portfolio.get_position(contract.symbol)
            if pos and pos.current_price > 0:
                entry_price = pos.current_price
            else:
                return EntryExitResult(False, error="Cannot determine entry price")

        # Calculate profit target and stop loss prices
        if config.profit_target_price:
            profit_price = config.profit_target_price
        elif config.profit_target_pct:
            profit_price = entry_price * (1 + config.profit_target_pct / 100)
        else:
            profit_price = None

        if config.stop_loss_price:
            stop_price = config.stop_loss_price
        elif config.stop_loss_pct:
            stop_price = entry_price * (1 - config.stop_loss_pct / 100)
        else:
            stop_price = None

        if profit_price is None and stop_price is None:
            return EntryExitResult(False, error="At least one of profit target or stop loss required")

        # Get order IDs
        parent_id = self._get_next_order_id()
        if parent_id is None:
            return EntryExitResult(False, error="No order ID available")

        profit_id = parent_id + 1 if profit_price else None
        stop_id = parent_id + (2 if profit_price else 1) if stop_price else None

        # Create OCA group for profit/stop
        oca_group = self._generate_oca_group()

        order_ids = []

        try:
            # Parent order (entry)
            if entry_type == OrderType.LIMIT:
                parent_order = OrderBuilder.limit_order("BUY", quantity, entry_price)
            else:
                parent_order = OrderBuilder.market_order("BUY", quantity)

            parent_order.orderId = parent_id
            parent_order.transmit = False  # Don't transmit until all orders ready

            self.portfolio._next_order_id += 1
            self.portfolio.placeOrder(parent_id, contract, parent_order)
            order_ids.append(parent_id)

            # Profit target order
            if profit_price and profit_id:
                profit_order = OrderBuilder.limit_order("SELL", quantity, profit_price, TimeInForce.GTC)
                profit_order.orderId = profit_id
                profit_order.parentId = parent_id
                profit_order.ocaGroup = oca_group
                profit_order.ocaType = 1  # Cancel other orders in group on fill
                profit_order.transmit = False

                self.portfolio._next_order_id += 1
                self.portfolio.placeOrder(profit_id, contract, profit_order)
                order_ids.append(profit_id)

            # Stop loss order
            if stop_price and stop_id:
                if config.trailing_stop:
                    trail_amt = config.trail_amount or (entry_price - stop_price)
                    stop_order = OrderBuilder.trailing_stop_order(
                        "SELL", quantity, trail_amount=trail_amt
                    )
                else:
                    stop_order = OrderBuilder.stop_order("SELL", quantity, stop_price)

                stop_order.orderId = stop_id
                stop_order.parentId = parent_id
                stop_order.ocaGroup = oca_group
                stop_order.ocaType = 1
                stop_order.transmit = True  # Transmit all orders

                self.portfolio._next_order_id += 1
                self.portfolio.placeOrder(stop_id, contract, stop_order)
                order_ids.append(stop_id)

            # Track bracket
            self._active_brackets[oca_group] = {
                "parent_id": parent_id,
                "profit_id": profit_id,
                "stop_id": stop_id,
                "symbol": contract.symbol,
                "quantity": quantity,
            }

            tp_str = f"{profit_price:.2f}" if profit_price else "N/A"
            sl_str = f"{stop_price:.2f}" if stop_price else "N/A"
            return EntryExitResult(
                success=True,
                order_ids=order_ids,
                parent_order_id=parent_id,
                profit_order_id=profit_id,
                stop_order_id=stop_id,
                oca_group=oca_group,
                message=f"Bracket order placed for {contract.symbol}: entry={entry_price:.2f}, TP={tp_str}, SL={sl_str}",
            )

        except Exception as e:
            logger.error(f"Failed to place bracket order: {e}")
            return EntryExitResult(False, order_ids=order_ids, error=str(e))

    # =========================================================================
    # Scaled Orders
    # =========================================================================

    def enter_scaled(
        self,
        contract: Contract,
        total_quantity: int,
        config: Optional[ScaledOrderConfig] = None,
        base_price: Optional[float] = None,
    ) -> EntryExitResult:
        """
        Enter a position with multiple orders at different price levels.

        Distributes the total quantity across multiple limit orders
        at progressively better prices.

        Args:
            contract: IB Contract to trade
            total_quantity: Total shares/contracts to buy
            config: Scaled order configuration
            base_price: Starting price (uses current price if not specified)

        Returns:
            EntryExitResult with all order IDs
        """
        config = config or ScaledOrderConfig()

        if base_price is None:
            pos = self.portfolio.get_position(contract.symbol)
            if pos and pos.current_price > 0:
                base_price = pos.current_price
            else:
                return EntryExitResult(False, error="Cannot determine base price")

        # Calculate quantities for each order
        quantities = self._calculate_scaled_quantities(
            total_quantity, config.num_orders, config.quantity_distribution
        )

        # Calculate prices for each order
        prices = self._calculate_scaled_prices(
            base_price, config.num_orders, config.price_increment_pct
        )

        order_ids = []

        for i, (qty, price) in enumerate(zip(quantities, prices)):
            if qty <= 0:
                continue

            order = OrderBuilder.limit_order("BUY", qty, price, TimeInForce.GTC)
            order_id = self._place_order(contract, order)

            if order_id:
                order_ids.append(order_id)
                logger.info(f"Scaled order {i+1}/{config.num_orders}: {qty} @ ${price:.2f}")
            else:
                logger.warning(f"Failed to place scaled order {i+1}")

        if not order_ids:
            return EntryExitResult(False, error="Failed to place any scaled orders")

        return EntryExitResult(
            success=True,
            order_ids=order_ids,
            message=f"Placed {len(order_ids)} scaled orders for {total_quantity} {contract.symbol}",
        )

    def _calculate_scaled_quantities(
        self,
        total: int,
        num_orders: int,
        distribution: str,
    ) -> List[int]:
        """Calculate quantity distribution for scaled orders"""
        if distribution == "equal":
            base = total // num_orders
            remainder = total % num_orders
            quantities = [base] * num_orders
            for i in range(remainder):
                quantities[i] += 1

        elif distribution == "pyramid":
            # More at better prices (ascending weights)
            weights = list(range(1, num_orders + 1))
            total_weight = sum(weights)
            quantities = [int(total * w / total_weight) for w in weights]
            # Distribute remainder
            remainder = total - sum(quantities)
            for i in range(remainder):
                quantities[-(i+1)] += 1

        elif distribution == "inverse_pyramid":
            # More at current price (descending weights)
            weights = list(range(num_orders, 0, -1))
            total_weight = sum(weights)
            quantities = [int(total * w / total_weight) for w in weights]
            remainder = total - sum(quantities)
            for i in range(remainder):
                quantities[i] += 1

        else:
            # Default to equal
            return self._calculate_scaled_quantities(total, num_orders, "equal")

        return quantities

    def _calculate_scaled_prices(
        self,
        base_price: float,
        num_orders: int,
        increment_pct: float,
    ) -> List[float]:
        """Calculate prices for scaled orders (descending for buys)"""
        prices = []
        for i in range(num_orders):
            price = base_price * (1 - i * increment_pct / 100)
            prices.append(round(price, 2))
        return prices

    # =========================================================================
    # Adaptive/Algorithmic Orders
    # =========================================================================

    def enter_adaptive(
        self,
        contract: Contract,
        quantity: int,
        limit_price: Optional[float] = None,
        urgency: str = "Normal",
    ) -> EntryExitResult:
        """
        Enter using IB's adaptive algorithm for better fills.

        The adaptive algorithm automatically chooses the best order type
        and timing based on market conditions.

        Args:
            contract: IB Contract to trade
            quantity: Number of shares/contracts
            limit_price: Optional limit price
            urgency: Patient, Normal, or Urgent

        Returns:
            EntryExitResult with order details
        """
        order_type = OrderType.LIMIT if limit_price else OrderType.MARKET
        order = OrderBuilder.adaptive_order("BUY", quantity, order_type, limit_price, urgency)

        order_id = self._place_order(contract, order)
        if order_id is None:
            return EntryExitResult(False, error="Failed to place adaptive order")

        return EntryExitResult(
            success=True,
            order_ids=[order_id],
            message=f"Adaptive order placed for {quantity} {contract.symbol} (urgency: {urgency})",
        )

    def enter_twap(
        self,
        contract: Contract,
        quantity: int,
        start_time: str,
        end_time: str,
        limit_price: Optional[float] = None,
    ) -> EntryExitResult:
        """
        Enter using TWAP algorithm to spread execution over time.

        Args:
            contract: IB Contract to trade
            quantity: Number of shares/contracts
            start_time: Start time in HH:MM:SS format
            end_time: End time in HH:MM:SS format
            limit_price: Optional limit price

        Returns:
            EntryExitResult with order details
        """
        order = OrderBuilder.twap_order("BUY", quantity, start_time, end_time, limit_price)

        order_id = self._place_order(contract, order)
        if order_id is None:
            return EntryExitResult(False, error="Failed to place TWAP order")

        return EntryExitResult(
            success=True,
            order_ids=[order_id],
            message=f"TWAP order placed for {quantity} {contract.symbol} ({start_time}-{end_time})",
        )

    def enter_vwap(
        self,
        contract: Contract,
        quantity: int,
        start_time: str,
        end_time: str,
        max_pct_volume: float = 0.1,
        limit_price: Optional[float] = None,
    ) -> EntryExitResult:
        """
        Enter using VWAP algorithm to match volume-weighted average price.

        Args:
            contract: IB Contract to trade
            quantity: Number of shares/contracts
            start_time: Start time in HH:MM:SS format
            end_time: End time in HH:MM:SS format
            max_pct_volume: Maximum percentage of volume to capture
            limit_price: Optional limit price

        Returns:
            EntryExitResult with order details
        """
        order = OrderBuilder.vwap_order(
            "BUY", quantity, start_time, end_time, max_pct_volume, limit_price
        )

        order_id = self._place_order(contract, order)
        if order_id is None:
            return EntryExitResult(False, error="Failed to place VWAP order")

        return EntryExitResult(
            success=True,
            order_ids=[order_id],
            message=f"VWAP order placed for {quantity} {contract.symbol} ({start_time}-{end_time}, max {max_pct_volume*100}% vol)",
        )

    # =========================================================================
    # Probability-Based Entry
    # =========================================================================

    def enter_probability_based(
        self,
        contract: Contract,
        target_quantity: int,
        probability: float,
        current_price: float,
        expected_move_pct: float = 5.0,
        risk_reward_ratio: float = 2.0,
    ) -> EntryExitResult:
        """
        Enter a position sized and bracketed based on probability.

        Calculates position size, entry points, and exit levels based on
        probability of success and desired risk/reward ratio.

        Args:
            contract: IB Contract to trade
            target_quantity: Maximum quantity if 100% probability
            probability: Probability of success (0.0-1.0)
            current_price: Current market price
            expected_move_pct: Expected price move percentage
            risk_reward_ratio: Desired reward to risk ratio

        Returns:
            EntryExitResult with bracket order details
        """
        if not 0.0 < probability <= 1.0:
            return EntryExitResult(False, error="Probability must be between 0 and 1")

        # Scale quantity by probability
        adjusted_quantity = int(target_quantity * probability)
        if adjusted_quantity < 1:
            return EntryExitResult(False, error="Adjusted quantity too small")

        # Calculate expected profit and required stop loss
        expected_profit_pct = expected_move_pct
        required_stop_pct = expected_profit_pct / risk_reward_ratio

        # Calculate Kelly criterion position sizing (simplified)
        # f* = (p * b - q) / b where p=probability, b=odds, q=1-p
        win_loss_ratio = expected_profit_pct / required_stop_pct
        kelly_fraction = (probability * win_loss_ratio - (1 - probability)) / win_loss_ratio
        kelly_fraction = max(0, min(kelly_fraction, 0.25))  # Cap at 25%

        # Apply Kelly to quantity
        kelly_quantity = int(target_quantity * kelly_fraction)
        final_quantity = min(adjusted_quantity, kelly_quantity) if kelly_quantity > 0 else adjusted_quantity

        if final_quantity < 1:
            return EntryExitResult(
                False,
                error=f"Position size too small (Kelly: {kelly_fraction:.2%})",
            )

        # Create bracket order
        bracket_config = BracketConfig(
            profit_target_pct=expected_profit_pct,
            stop_loss_pct=required_stop_pct,
            trailing_stop=probability > 0.6,  # Use trailing stop for higher probability trades
            trail_percent=required_stop_pct if probability > 0.6 else None,
        )

        result = self.enter_bracket(
            contract=contract,
            quantity=final_quantity,
            entry_price=current_price,
            entry_type=OrderType.LIMIT,
            bracket_config=bracket_config,
        )

        if result.success:
            result.message = (
                f"Probability-based entry: {final_quantity} shares "
                f"(prob={probability:.0%}, kelly={kelly_fraction:.1%}), "
                f"TP={expected_profit_pct:.1f}%, SL={required_stop_pct:.1f}%"
            )

        return result

    # =========================================================================
    # Exit Management
    # =========================================================================

    def exit_with_trailing_stop(
        self,
        contract: Contract,
        quantity: int,
        trail_percent: Optional[float] = None,
        trail_amount: Optional[float] = None,
    ) -> EntryExitResult:
        """
        Exit with a trailing stop to protect profits.

        Args:
            contract: IB Contract to trade
            quantity: Number of shares to exit
            trail_percent: Trail as percentage of price
            trail_amount: Trail as absolute dollar amount

        Returns:
            EntryExitResult with order details
        """
        if trail_percent is None and trail_amount is None:
            return EntryExitResult(False, error="Either trail_percent or trail_amount required")

        order = OrderBuilder.trailing_stop_order(
            "SELL", quantity, trail_amount, trail_percent, TimeInForce.GTC
        )

        order_id = self._place_order(contract, order)
        if order_id is None:
            return EntryExitResult(False, error="Failed to place trailing stop")

        trail_desc = f"{trail_percent}%" if trail_percent else f"${trail_amount}"
        return EntryExitResult(
            success=True,
            order_ids=[order_id],
            message=f"Trailing stop placed: {quantity} {contract.symbol} (trail: {trail_desc})",
        )

    def cancel_bracket(self, oca_group: str) -> bool:
        """
        Cancel all orders in a bracket.

        Args:
            oca_group: OCA group identifier from enter_bracket result

        Returns:
            True if cancellation was successful
        """
        if oca_group not in self._active_brackets:
            logger.warning(f"Unknown OCA group: {oca_group}")
            return False

        bracket = self._active_brackets[oca_group]
        success = True

        for order_id in [bracket.get("parent_id"), bracket.get("profit_id"), bracket.get("stop_id")]:
            if order_id:
                try:
                    self.portfolio.cancelOrder(order_id, "")
                except Exception as e:
                    logger.error(f"Failed to cancel order {order_id}: {e}")
                    success = False

        if success:
            del self._active_brackets[oca_group]

        return success
