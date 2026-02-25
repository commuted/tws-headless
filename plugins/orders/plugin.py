"""
Orders Plugin - Execute all IB order types via socket interface

Supports:
- Market (MKT)
- Limit (LMT)
- Stop (STP)
- Stop-Limit (STP LMT)
- Trailing Stop (TRAIL)
- Trailing Stop Limit (TRAIL LIMIT)
- Market on Close (MOC)
- Limit on Close (LOC)
- Market on Open (MOO)
- Limit on Open (LOO)
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from ..base import PluginBase, PluginState

logger = logging.getLogger(__name__)

ORDERS_PLUGIN_NAME = "_orders"


class OrderType(Enum):
    """IB Order Types"""
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


class TimeInForce(Enum):
    """Time in Force options"""
    DAY = "DAY"          # Good for day
    GTC = "GTC"          # Good till cancelled
    IOC = "IOC"          # Immediate or cancel
    FOK = "FOK"          # Fill or kill
    GTD = "GTD"          # Good till date
    OPG = "OPG"          # At the open
    DTC = "DTC"          # Day till cancelled


# Mapping from user-friendly names to IB order types
ORDER_TYPE_ALIASES = {
    # Market
    "market": OrderType.MARKET,
    "mkt": OrderType.MARKET,
    # Limit
    "limit": OrderType.LIMIT,
    "lmt": OrderType.LIMIT,
    # Stop
    "stop": OrderType.STOP,
    "stp": OrderType.STOP,
    # Stop-Limit
    "stop-limit": OrderType.STOP_LIMIT,
    "stop_limit": OrderType.STOP_LIMIT,
    "stplmt": OrderType.STOP_LIMIT,
    "stp lmt": OrderType.STOP_LIMIT,
    # Trailing Stop
    "trail": OrderType.TRAILING_STOP,
    "trailing": OrderType.TRAILING_STOP,
    "trailing-stop": OrderType.TRAILING_STOP,
    "trailing_stop": OrderType.TRAILING_STOP,
    # Trailing Stop Limit
    "trail-limit": OrderType.TRAILING_STOP_LIMIT,
    "trail_limit": OrderType.TRAILING_STOP_LIMIT,
    "trailing-stop-limit": OrderType.TRAILING_STOP_LIMIT,
    # Market on Close
    "moc": OrderType.MARKET_ON_CLOSE,
    "market-on-close": OrderType.MARKET_ON_CLOSE,
    # Limit on Close
    "loc": OrderType.LIMIT_ON_CLOSE,
    "limit-on-close": OrderType.LIMIT_ON_CLOSE,
    # Market on Open
    "moo": OrderType.MARKET_ON_OPEN,
    "market-on-open": OrderType.MARKET_ON_OPEN,
    # Limit on Open
    "loo": OrderType.LIMIT_ON_OPEN,
    "limit-on-open": OrderType.LIMIT_ON_OPEN,
}

TIF_ALIASES = {
    "day": TimeInForce.DAY,
    "gtc": TimeInForce.GTC,
    "ioc": TimeInForce.IOC,
    "fok": TimeInForce.FOK,
    "gtd": TimeInForce.GTD,
    "opg": TimeInForce.OPG,
}


@dataclass
class OrderRecord:
    """Record of an order placed through this plugin"""
    order_id: int
    symbol: str
    action: str
    quantity: float
    order_type: str
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    trail_amount: Optional[float] = None
    trail_percent: Optional[float] = None
    tif: str = "DAY"
    placed_at: datetime = field(default_factory=datetime.now)
    status: str = "SUBMITTED"
    filled_qty: float = 0.0
    avg_fill_price: float = 0.0


class OrdersPlugin(PluginBase):
    """
    System plugin for executing orders via socket interface.

    This plugin does not generate signals or hold positions.
    It provides order execution capabilities through the socket command interface.
    """

    NAME = ORDERS_PLUGIN_NAME
    VERSION = "1.0.0"
    IS_SYSTEM_PLUGIN = True

    def __init__(
        self,
        portfolio=None,
        message_bus=None,
        **kwargs,
    ):
        super().__init__(
            name=ORDERS_PLUGIN_NAME,
            portfolio=portfolio,
            message_bus=message_bus,
            **kwargs,
        )
        self._orders: Dict[int, OrderRecord] = {}
        self._description = "System plugin for order execution via socket interface"

    @property
    def description(self) -> str:
        return self._description

    @property
    def required_bars(self) -> int:
        return 0

    def calculate_signals(self, market_data: Dict) -> List:
        """This plugin does not generate signals"""
        return []

    def load(self) -> bool:
        """Load plugin"""
        self._state = PluginState.LOADED
        logger.info(f"OrdersPlugin loaded")
        return True

    def start(self) -> bool:
        """Start plugin"""
        if self._state != PluginState.LOADED:
            return False
        self._state = PluginState.STARTED
        return True

    def stop(self) -> bool:
        """Stop plugin"""
        self._state = PluginState.STOPPED
        return True

    def freeze(self) -> bool:
        """Freeze plugin (same as stop for system plugin)"""
        self._state = PluginState.FROZEN
        return True

    def resume(self) -> bool:
        """Resume plugin from frozen state"""
        if self._state == PluginState.FROZEN:
            self._state = PluginState.STARTED
        return True

    def parse_order_type(self, type_str: str) -> Optional[OrderType]:
        """Parse order type from string"""
        return ORDER_TYPE_ALIASES.get(type_str.lower())

    def parse_tif(self, tif_str: str) -> Optional[TimeInForce]:
        """Parse time in force from string"""
        return TIF_ALIASES.get(tif_str.lower())

    def get_contract_for_symbol(self, symbol: str):
        """
        Get contract for a symbol.

        First checks existing positions, then uses ContractBuilder.
        """
        if not self.portfolio:
            return None

        # Check existing positions
        pos = self.portfolio.get_position(symbol)
        if pos and pos.contract:
            return pos.contract

        # Try to build contract
        try:
            from ..contract_builder import ContractBuilder
            builder = ContractBuilder()
            return builder.stock(symbol)
        except ImportError:
            # Fallback: create basic stock contract
            from ibapi.contract import Contract
            contract = Contract()
            contract.symbol = symbol.upper()
            contract.secType = "STK"
            contract.exchange = "SMART"
            contract.currency = "USD"
            return contract

    def execute_order(
        self,
        symbol: str,
        action: str,
        quantity: float,
        order_type: OrderType = OrderType.MARKET,
        limit_price: Optional[float] = None,
        stop_price: Optional[float] = None,
        trail_amount: Optional[float] = None,
        trail_percent: Optional[float] = None,
        tif: TimeInForce = TimeInForce.DAY,
        dry_run: bool = True,
    ) -> Tuple[bool, Optional[int], str]:
        """
        Execute an order.

        Args:
            symbol: Stock symbol
            action: BUY or SELL
            quantity: Number of shares
            order_type: Type of order
            limit_price: Limit price (for limit orders)
            stop_price: Stop price (for stop orders)
            trail_amount: Trailing amount in dollars (for trailing stop)
            trail_percent: Trailing percentage (for trailing stop)
            tif: Time in force
            dry_run: If True, don't actually place the order

        Returns:
            Tuple of (success, order_id, message)
        """
        if not self.portfolio:
            return False, None, "No portfolio connected"

        # Validate action
        action = action.upper()
        if action not in ("BUY", "SELL"):
            return False, None, f"Invalid action: {action}. Must be BUY or SELL."

        # Validate quantity
        if quantity <= 0:
            return False, None, f"Invalid quantity: {quantity}. Must be positive."

        # Validate order type requirements
        ot = order_type.value

        if order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT,
                          OrderType.LIMIT_ON_CLOSE, OrderType.LIMIT_ON_OPEN,
                          OrderType.TRAILING_STOP_LIMIT):
            if limit_price is None or limit_price <= 0:
                return False, None, f"Limit price required for {order_type.name} orders"

        if order_type in (OrderType.STOP, OrderType.STOP_LIMIT):
            if stop_price is None or stop_price <= 0:
                return False, None, f"Stop price required for {order_type.name} orders"

        if order_type in (OrderType.TRAILING_STOP, OrderType.TRAILING_STOP_LIMIT):
            if trail_amount is None and trail_percent is None:
                return False, None, "Trailing amount or percent required for trailing stop orders"

        # Get contract
        contract = self.get_contract_for_symbol(symbol)
        if not contract:
            return False, None, f"Could not resolve contract for {symbol}"

        # Build order description
        order_desc = f"{action} {quantity:.0f} {symbol} {order_type.name}"
        if limit_price:
            order_desc += f" @ ${limit_price:.2f}"
        if stop_price:
            order_desc += f" stop ${stop_price:.2f}"
        if trail_amount:
            order_desc += f" trail ${trail_amount:.2f}"
        elif trail_percent:
            order_desc += f" trail {trail_percent:.1f}%"
        order_desc += f" [{tif.value}]"

        if dry_run:
            return True, None, f"[DRY RUN] Would place: {order_desc}"

        # Place the order
        try:
            # For trailing stop orders, we need custom handling
            if order_type in (OrderType.TRAILING_STOP, OrderType.TRAILING_STOP_LIMIT):
                order_id = self._place_trailing_order(
                    contract, action, quantity, order_type,
                    limit_price, trail_amount, trail_percent, tif
                )
            elif order_type in (OrderType.MARKET_ON_CLOSE, OrderType.LIMIT_ON_CLOSE,
                                OrderType.MARKET_ON_OPEN, OrderType.LIMIT_ON_OPEN):
                order_id = self._place_timed_order(
                    contract, action, quantity, order_type, limit_price, tif
                )
            else:
                # Standard order types
                order_id = self.portfolio.place_order(
                    contract=contract,
                    action=action,
                    quantity=quantity,
                    order_type=ot,
                    limit_price=limit_price or 0.0,
                    stop_price=stop_price or 0.0,
                    tif=tif.value,
                )

            if order_id:
                # Record the order
                self._orders[order_id] = OrderRecord(
                    order_id=order_id,
                    symbol=symbol,
                    action=action,
                    quantity=quantity,
                    order_type=ot,
                    limit_price=limit_price,
                    stop_price=stop_price,
                    trail_amount=trail_amount,
                    trail_percent=trail_percent,
                    tif=tif.value,
                )
                return True, order_id, f"[EXECUTED] Order {order_id}: {order_desc}"
            else:
                return False, None, f"Failed to place order: {order_desc}"

        except Exception as e:
            logger.error(f"Error placing order: {e}")
            return False, None, f"Error placing order: {e}"

    def _place_trailing_order(
        self,
        contract,
        action: str,
        quantity: float,
        order_type: OrderType,
        limit_price: Optional[float],
        trail_amount: Optional[float],
        trail_percent: Optional[float],
        tif: TimeInForce,
    ) -> Optional[int]:
        """Place a trailing stop order"""
        from ibapi.order import Order

        if not self.portfolio.connected:
            return None

        with self.portfolio._lock:
            order_id = self.portfolio._next_order_id
            self.portfolio._next_order_id += 1

        order = Order()
        order.action = action
        order.totalQuantity = quantity
        order.orderType = order_type.value
        order.tif = tif.value

        if trail_amount:
            order.auxPrice = trail_amount
        elif trail_percent:
            order.trailingPercent = trail_percent

        if limit_price and order_type == OrderType.TRAILING_STOP_LIMIT:
            order.lmtPrice = limit_price
            order.lmtPriceOffset = 0.0

        try:
            self.portfolio.placeOrder(order_id, contract, order)
            return order_id
        except Exception as e:
            logger.error(f"Error placing trailing order: {e}")
            return None

    def _place_timed_order(
        self,
        contract,
        action: str,
        quantity: float,
        order_type: OrderType,
        limit_price: Optional[float],
        tif: TimeInForce,
    ) -> Optional[int]:
        """Place MOC/LOC/MOO/LOO orders"""
        from ibapi.order import Order

        if not self.portfolio.connected:
            return None

        with self.portfolio._lock:
            order_id = self.portfolio._next_order_id
            self.portfolio._next_order_id += 1

        order = Order()
        order.action = action
        order.totalQuantity = quantity
        order.orderType = order_type.value

        if order_type in (OrderType.LIMIT_ON_CLOSE, OrderType.LIMIT_ON_OPEN):
            order.lmtPrice = limit_price

        # MOC/LOC need special TIF handling
        if order_type in (OrderType.MARKET_ON_CLOSE, OrderType.LIMIT_ON_CLOSE):
            order.tif = "DAY"
        elif order_type in (OrderType.MARKET_ON_OPEN, OrderType.LIMIT_ON_OPEN):
            order.tif = "OPG"
        else:
            order.tif = tif.value

        try:
            self.portfolio.placeOrder(order_id, contract, order)
            return order_id
        except Exception as e:
            logger.error(f"Error placing timed order: {e}")
            return None

    def get_orders(self) -> List[OrderRecord]:
        """Get all orders placed through this plugin"""
        return list(self._orders.values())

    def get_order(self, order_id: int) -> Optional[OrderRecord]:
        """Get a specific order"""
        return self._orders.get(order_id)

    def handle_request(self, request: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle custom requests"""
        if request == "list_orders":
            orders = [
                {
                    "order_id": o.order_id,
                    "symbol": o.symbol,
                    "action": o.action,
                    "quantity": o.quantity,
                    "order_type": o.order_type,
                    "status": o.status,
                    "placed_at": o.placed_at.isoformat(),
                }
                for o in self._orders.values()
            ]
            return {"success": True, "orders": orders}

        elif request == "get_order":
            order_id = params.get("order_id")
            order = self._orders.get(order_id)
            if order:
                return {
                    "success": True,
                    "order": {
                        "order_id": order.order_id,
                        "symbol": order.symbol,
                        "action": order.action,
                        "quantity": order.quantity,
                        "order_type": order.order_type,
                        "limit_price": order.limit_price,
                        "stop_price": order.stop_price,
                        "tif": order.tif,
                        "status": order.status,
                        "placed_at": order.placed_at.isoformat(),
                    }
                }
            return {"success": False, "error": f"Order {order_id} not found"}

        return {"success": False, "error": f"Unknown request: {request}"}

    def get_status(self) -> Dict[str, Any]:
        """Get plugin status"""
        status = super().get_status()
        status.update({
            "is_system_plugin": True,
            "orders_count": len(self._orders),
        })
        return status

    def get_effective_holdings(self) -> Dict:
        """Orders plugin doesn't hold positions"""
        return {
            "plugin": self.name,
            "is_system_plugin": True,
            "cash": 0.0,
            "positions": [],
            "total_value": 0.0,
        }


def get_order_help() -> str:
    """Get help text for order command"""
    return """
Order Command - Execute all IB order types

Usage:
    order ACTION SYMBOL QTY [TYPE] [options] [--confirm]

Actions:
    buy, sell

Order Types:
    market (default)    Market order
    limit PRICE         Limit order at specified price
    stop PRICE          Stop order at specified price
    stop-limit STOP LIMIT   Stop-limit order
    trail AMOUNT|PERCENT    Trailing stop (e.g., trail 1.50 or trail 2%)
    moc                 Market on Close
    loc PRICE           Limit on Close
    moo                 Market on Open
    loo PRICE           Limit on Open

Options:
    --tif TIF           Time in force: day (default), gtc, ioc, fok
    --confirm           Actually place the order (otherwise dry run)

Examples:
    order buy SPY 100                           # Market order (dry run)
    order buy SPY 100 --confirm                 # Market order (execute)
    order buy SPY 100 limit 450.00              # Limit order at $450
    order sell QQQ 50 stop 380.00               # Stop order at $380
    order buy AAPL 25 stop-limit 175 170        # Stop-limit: stop $175, limit $170
    order sell MSFT 30 trail 2.00               # Trailing stop $2
    order sell MSFT 30 trail 1%                 # Trailing stop 1%
    order buy SPY 100 moc                       # Market on Close
    order sell QQQ 50 loc 380.00                # Limit on Close at $380
    order buy SPY 100 limit 450 --tif gtc       # Good till cancelled
"""
