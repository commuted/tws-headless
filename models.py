"""
models.py - Data models for the IB Portfolio system

Contains all data classes used throughout the application.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, Any
from ibapi.contract import Contract
from ibapi.order import Order


class AssetType(Enum):
    """Asset type classification"""
    EQUITY = "STK"
    OPTION = "OPT"
    FUTURE = "FUT"
    FOREX = "CASH"
    INDEX = "IND"
    BOND = "BOND"
    ETF = "ETF"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_sec_type(cls, sec_type: str) -> "AssetType":
        """Convert IB secType to AssetType"""
        mapping = {
            "STK": cls.EQUITY,
            "OPT": cls.OPTION,
            "FUT": cls.FUTURE,
            "CASH": cls.FOREX,
            "IND": cls.INDEX,
            "BOND": cls.BOND,
        }
        return mapping.get(sec_type, cls.UNKNOWN)


class OrderAction(Enum):
    """Order action types"""
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class RebalanceStrategy(Enum):
    """Available rebalancing strategies"""
    THRESHOLD = "threshold"      # Rebalance when drift exceeds threshold
    CALENDAR = "calendar"        # Rebalance on schedule
    TACTICAL = "tactical"        # Rebalance based on signals
    HYBRID = "hybrid"            # Combination of strategies


class BarSize(Enum):
    """Available bar sizes for streaming"""
    SEC_5 = "5 secs"          # Only size available for realTimeBars
    SEC_10 = "10 secs"
    SEC_15 = "15 secs"
    SEC_30 = "30 secs"
    MIN_1 = "1 min"
    MIN_2 = "2 mins"
    MIN_3 = "3 mins"
    MIN_5 = "5 mins"
    MIN_10 = "10 mins"
    MIN_15 = "15 mins"
    MIN_20 = "20 mins"
    MIN_30 = "30 mins"
    HOUR_1 = "1 hour"
    HOUR_2 = "2 hours"
    HOUR_3 = "3 hours"
    HOUR_4 = "4 hours"
    HOUR_8 = "8 hours"
    DAY_1 = "1 day"
    WEEK_1 = "1 week"
    MONTH_1 = "1 month"


@dataclass
class Position:
    """Represents a portfolio position with market data"""
    symbol: str
    asset_type: AssetType
    quantity: float
    avg_cost: float
    current_price: float = 0.0
    market_value: float = 0.0
    unrealized_pnl: float = 0.0
    allocation_pct: float = 0.0
    contract: Optional[Contract] = field(default=None, repr=False)
    account: str = ""

    def update_market_data(self, price: float):
        """Update position with current market price"""
        self.current_price = price
        self.market_value = self.quantity * price
        self.unrealized_pnl = self.market_value - (self.quantity * self.avg_cost)

    @property
    def cost_basis(self) -> float:
        """Total cost basis for the position"""
        return self.quantity * self.avg_cost

    @property
    def return_pct(self) -> float:
        """Return percentage for the position"""
        if self.cost_basis == 0:
            return 0.0
        return (self.unrealized_pnl / self.cost_basis) * 100

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "symbol": self.symbol,
            "asset_type": self.asset_type.value,
            "quantity": self.quantity,
            "avg_cost": self.avg_cost,
            "current_price": self.current_price,
            "market_value": self.market_value,
            "unrealized_pnl": self.unrealized_pnl,
            "allocation_pct": self.allocation_pct,
            "return_pct": self.return_pct,
        }

    def __repr__(self):
        return (f"Position({self.symbol}, qty={self.quantity}, "
                f"price=${self.current_price:.2f}, value=${self.market_value:.2f})")


@dataclass
class Bar:
    """Represents an OHLCV bar (candlestick)"""
    symbol: str
    timestamp: str              # ISO format timestamp
    open: float
    high: float
    low: float
    close: float
    volume: int = 0
    wap: float = 0.0           # Weighted average price
    bar_count: int = 0         # Number of trades in bar

    @property
    def range(self) -> float:
        """Price range of the bar"""
        return self.high - self.low

    @property
    def body(self) -> float:
        """Body size (absolute difference between open and close)"""
        return abs(self.close - self.open)

    @property
    def is_bullish(self) -> bool:
        """True if close > open"""
        return self.close > self.open

    @property
    def is_bearish(self) -> bool:
        """True if close < open"""
        return self.close < self.open

    @property
    def mid(self) -> float:
        """Midpoint of high and low"""
        return (self.high + self.low) / 2

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "wap": self.wap,
            "bar_count": self.bar_count,
        }

    def __repr__(self):
        direction = "+" if self.is_bullish else "-"
        return (f"Bar({self.symbol} {self.timestamp} "
                f"O:{self.open:.2f} H:{self.high:.2f} L:{self.low:.2f} C:{self.close:.2f} "
                f"V:{self.volume} {direction})")


@dataclass
class TargetAllocation:
    """Target allocation for a single asset"""
    symbol: str
    target_pct: float
    asset_type: AssetType = AssetType.EQUITY
    min_pct: float = 0.0          # Minimum allowed allocation
    max_pct: float = 100.0        # Maximum allowed allocation
    exchange: str = "SMART"
    currency: str = "USD"

    def __post_init__(self):
        if not 0 <= self.target_pct <= 100:
            raise ValueError(f"target_pct must be 0-100, got {self.target_pct}")
        if self.min_pct > self.target_pct or self.target_pct > self.max_pct:
            raise ValueError("Must have: min_pct <= target_pct <= max_pct")

    def create_contract(self) -> Contract:
        """Create an IB Contract for this allocation target"""
        contract = Contract()
        contract.symbol = self.symbol
        contract.secType = self.asset_type.value
        contract.exchange = self.exchange
        contract.currency = self.currency
        return contract


@dataclass
class RebalanceTrade:
    """Represents a trade needed to rebalance a position"""
    symbol: str
    action: OrderAction
    quantity: float
    current_allocation: float
    target_allocation: float
    drift: float
    estimated_value: float
    contract: Optional[Contract] = None
    reason: str = ""

    @property
    def is_actionable(self) -> bool:
        """Check if this trade should be executed"""
        return self.action != OrderAction.HOLD and self.quantity > 0

    def create_order(self, order_type: str = "MKT") -> Order:
        """Create an IB Order for this trade"""
        order = Order()
        order.action = self.action.value
        order.totalQuantity = abs(self.quantity)
        order.orderType = order_type
        return order

    def __repr__(self):
        return (f"RebalanceTrade({self.action.value} {self.quantity:.0f} {self.symbol}, "
                f"drift={self.drift:+.1f}%)")


@dataclass
class RebalanceResult:
    """Result of a rebalance calculation"""
    trades: list  # List[RebalanceTrade]
    total_portfolio_value: float
    total_buy_value: float = 0.0
    total_sell_value: float = 0.0
    timestamp: str = ""
    strategy: RebalanceStrategy = RebalanceStrategy.THRESHOLD
    threshold_used: float = 0.0

    def __post_init__(self):
        if not self.timestamp:
            from datetime import datetime
            self.timestamp = datetime.now().isoformat()

        # Calculate totals
        self.total_buy_value = sum(
            t.estimated_value for t in self.trades
            if t.action == OrderAction.BUY
        )
        self.total_sell_value = sum(
            t.estimated_value for t in self.trades
            if t.action == OrderAction.SELL
        )

    @property
    def net_cash_flow(self) -> float:
        """Net cash flow from rebalancing (positive = cash in)"""
        return self.total_sell_value - self.total_buy_value

    @property
    def actionable_trades(self) -> list:
        """Return only trades that should be executed"""
        return [t for t in self.trades if t.is_actionable]

    @property
    def trade_count(self) -> int:
        """Number of actionable trades"""
        return len(self.actionable_trades)

    def summary(self) -> str:
        """Return a summary of the rebalance result"""
        lines = [
            f"Rebalance Summary ({self.timestamp})",
            f"Strategy: {self.strategy.value}",
            f"Portfolio Value: ${self.total_portfolio_value:,.2f}",
            f"Trades: {self.trade_count}",
            f"Total Buys: ${self.total_buy_value:,.2f}",
            f"Total Sells: ${self.total_sell_value:,.2f}",
            f"Net Cash Flow: ${self.net_cash_flow:,.2f}",
        ]
        return "\n".join(lines)


@dataclass
class AccountSummary:
    """Summary of account values"""
    account_id: str
    net_liquidation: float = 0.0
    total_cash: float = 0.0
    buying_power: float = 0.0
    available_funds: float = 0.0
    currency: str = "USD"
    values: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_valid(self) -> bool:
        """Check if account summary has valid data"""
        return self.net_liquidation > 0


class OrderStatus(Enum):
    """Order status states"""
    PENDING = "PendingSubmit"
    SUBMITTED = "Submitted"
    FILLED = "Filled"
    PARTIALLY_FILLED = "PartiallyFilled"
    CANCELLED = "Cancelled"
    ERROR = "Error"
    UNKNOWN = "Unknown"

    @classmethod
    def from_ib_status(cls, status: str) -> "OrderStatus":
        """Convert IB status string to OrderStatus"""
        mapping = {
            "PendingSubmit": cls.PENDING,
            "PendingCancel": cls.PENDING,
            "PreSubmitted": cls.PENDING,
            "Submitted": cls.SUBMITTED,
            "Filled": cls.FILLED,
            "PartiallyFilled": cls.PARTIALLY_FILLED,
            "Cancelled": cls.CANCELLED,
            "ApiCancelled": cls.CANCELLED,
            "Error": cls.ERROR,
        }
        return mapping.get(status, cls.UNKNOWN)


@dataclass
class OrderRecord:
    """Tracks an order through its lifecycle"""
    order_id: int
    symbol: str
    action: str                    # BUY or SELL
    quantity: float
    order_type: str = "MKT"
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: float = 0.0
    avg_fill_price: float = 0.0
    last_fill_price: float = 0.0
    remaining: float = 0.0
    submitted_time: str = ""
    filled_time: str = ""
    error_message: str = ""

    @property
    def is_complete(self) -> bool:
        """Check if order is in a terminal state"""
        return self.status in (
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.ERROR,
        )

    @property
    def is_filled(self) -> bool:
        """Check if order was fully filled"""
        return self.status == OrderStatus.FILLED

    @property
    def fill_value(self) -> float:
        """Total value of filled shares"""
        return self.filled_quantity * self.avg_fill_price

    def __repr__(self):
        return (f"OrderRecord({self.order_id}: {self.action} {self.quantity} {self.symbol} "
                f"@ {self.order_type}, status={self.status.value}, "
                f"filled={self.filled_quantity}@${self.avg_fill_price:.2f})")


@dataclass
class ExecutionResult:
    """Result of executing a rebalance"""
    success: bool
    orders: list                   # List[OrderRecord]
    total_orders: int = 0
    filled_orders: int = 0
    failed_orders: int = 0
    total_buy_value: float = 0.0
    total_sell_value: float = 0.0
    start_time: str = ""
    end_time: str = ""
    errors: list = field(default_factory=list)  # List of error messages

    def __post_init__(self):
        if not self.start_time:
            from datetime import datetime
            self.start_time = datetime.now().isoformat()

        self.total_orders = len(self.orders)
        self.filled_orders = sum(1 for o in self.orders if o.is_filled)
        self.failed_orders = sum(1 for o in self.orders if o.status == OrderStatus.ERROR)

        self.total_buy_value = sum(
            o.fill_value for o in self.orders if o.action == "BUY" and o.is_filled
        )
        self.total_sell_value = sum(
            o.fill_value for o in self.orders if o.action == "SELL" and o.is_filled
        )

    def summary(self) -> str:
        """Return a summary of the execution"""
        lines = [
            f"Execution Result",
            f"  Status: {'SUCCESS' if self.success else 'FAILED'}",
            f"  Orders: {self.filled_orders}/{self.total_orders} filled",
            f"  Failed: {self.failed_orders}",
            f"  Buy Value: ${self.total_buy_value:,.2f}",
            f"  Sell Value: ${self.total_sell_value:,.2f}",
            f"  Started: {self.start_time}",
            f"  Ended: {self.end_time}",
        ]
        if self.errors:
            lines.append(f"  Errors: {len(self.errors)}")
            for err in self.errors[:5]:
                lines.append(f"    - {err}")
        return "\n".join(lines)
