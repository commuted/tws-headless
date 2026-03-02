"""
models.py - Data models for the IB Portfolio system

Contains all data classes used throughout the application.
Compatible with the official Interactive Brokers API.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List
from decimal import Decimal
from ibapi.contract import Contract
from ibapi.order import Order

from .const import (
    UNSET_INTEGER,
    UNSET_DOUBLE,
    UNSET_DECIMAL,
    NO_VALID_ID,
)


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
    INACTIVE = "Inactive"   # submitted to IB but not actively working
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
            "Inactive": cls.INACTIVE,
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
    why_held: str = ""  # populated from IB orderStatus whyHeld field

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


# =============================================================================
# Official IB API Compatible Classes
# =============================================================================


class OptionExerciseType(Enum):
    """Option exercise types from the official IB API"""
    NoneItem = (-1, "None")
    Exercise = (1, "Exercise")
    Lapse = (2, "Lapse")
    DoNothing = (3, "DoNothing")
    Assigned = (100, "Assigned")
    AutoexerciseClearing = (101, "AutoexerciseClearing")
    Expired = (102, "Expired")
    Netting = (103, "Netting")
    AutoexerciseTrading = (200, "AutoexerciseTrading")


@dataclass
class Execution:
    """
    Execution details from the official IB API.

    This class provides details about the execution of an order including
    execution ID, time, price, quantity, and other execution-specific info.
    """
    execId: str = ""
    time: str = ""
    acctNumber: str = ""
    exchange: str = ""
    side: str = ""                              # BOT or SLD
    shares: Decimal = field(default_factory=lambda: UNSET_DECIMAL)
    price: float = 0.0
    permId: int = 0
    clientId: int = 0
    orderId: int = 0
    liquidation: int = 0
    cumQty: Decimal = field(default_factory=lambda: UNSET_DECIMAL)
    avgPrice: float = 0.0
    orderRef: str = ""
    evRule: str = ""                            # Economic Value Rule
    evMultiplier: float = 0.0                   # Economic Value Multiplier
    modelCode: str = ""
    lastLiquidity: int = 0                      # Last liquidity indicator
    pendingPriceRevision: bool = False
    submitter: str = ""
    optExerciseOrLapseType: OptionExerciseType = OptionExerciseType.NoneItem

    def __str__(self) -> str:
        return (
            f"ExecId: {self.execId}, Time: {self.time}, Account: {self.acctNumber}, "
            f"Exchange: {self.exchange}, Side: {self.side}, Shares: {self.shares}, "
            f"Price: {self.price}, PermId: {self.permId}, ClientId: {self.clientId}, "
            f"OrderId: {self.orderId}, CumQty: {self.cumQty}, AvgPrice: {self.avgPrice}"
        )


@dataclass
class ExecutionFilter:
    """
    Filter criteria for execution queries from the official IB API.

    Used with reqExecutions() to filter which executions to return.
    """
    clientId: int = 0
    acctCode: str = ""
    time: str = ""
    symbol: str = ""
    secType: str = ""
    exchange: str = ""
    side: str = ""
    lastNDays: int = field(default_factory=lambda: UNSET_INTEGER)
    specificDates: Optional[List[str]] = None


@dataclass
class OrderAllocation:
    """
    Order allocation details for Financial Advisor accounts.

    Provides details about how an order is allocated across sub-accounts.
    """
    account: str = ""
    position: Decimal = field(default_factory=lambda: UNSET_DECIMAL)
    positionDesired: Decimal = field(default_factory=lambda: UNSET_DECIMAL)
    positionAfter: Decimal = field(default_factory=lambda: UNSET_DECIMAL)
    desiredAllocQty: Decimal = field(default_factory=lambda: UNSET_DECIMAL)
    allowedAllocQty: Decimal = field(default_factory=lambda: UNSET_DECIMAL)
    isMonetary: bool = False

    def __str__(self) -> str:
        return (
            f"Account: {self.account}, Position: {self.position}, "
            f"PositionDesired: {self.positionDesired}, PositionAfter: {self.positionAfter}"
        )


@dataclass
class OrderState:
    """
    Order state and margin impact from the official IB API.

    Contains status, margin impact (before/change/after), and commission info.
    This is particularly useful for what-if orders.
    """
    status: str = ""

    # Margin before order
    initMarginBefore: str = ""
    maintMarginBefore: str = ""
    equityWithLoanBefore: str = ""

    # Margin change from order
    initMarginChange: str = ""
    maintMarginChange: str = ""
    equityWithLoanChange: str = ""

    # Margin after order
    initMarginAfter: str = ""
    maintMarginAfter: str = ""
    equityWithLoanAfter: str = ""

    # Commission and fees
    commissionAndFees: float = field(default_factory=lambda: UNSET_DOUBLE)
    minCommissionAndFees: float = field(default_factory=lambda: UNSET_DOUBLE)
    maxCommissionAndFees: float = field(default_factory=lambda: UNSET_DOUBLE)
    commissionAndFeesCurrency: str = ""
    marginCurrency: str = ""

    # Outside RTH margin values
    initMarginBeforeOutsideRTH: float = field(default_factory=lambda: UNSET_DOUBLE)
    maintMarginBeforeOutsideRTH: float = field(default_factory=lambda: UNSET_DOUBLE)
    equityWithLoanBeforeOutsideRTH: float = field(default_factory=lambda: UNSET_DOUBLE)
    initMarginChangeOutsideRTH: float = field(default_factory=lambda: UNSET_DOUBLE)
    maintMarginChangeOutsideRTH: float = field(default_factory=lambda: UNSET_DOUBLE)
    equityWithLoanChangeOutsideRTH: float = field(default_factory=lambda: UNSET_DOUBLE)
    initMarginAfterOutsideRTH: float = field(default_factory=lambda: UNSET_DOUBLE)
    maintMarginAfterOutsideRTH: float = field(default_factory=lambda: UNSET_DOUBLE)
    equityWithLoanAfterOutsideRTH: float = field(default_factory=lambda: UNSET_DOUBLE)

    # Other fields
    suggestedSize: Decimal = field(default_factory=lambda: UNSET_DECIMAL)
    rejectReason: str = ""
    orderAllocations: Optional[List[OrderAllocation]] = None
    warningText: str = ""
    completedTime: str = ""
    completedStatus: str = ""

    def __str__(self) -> str:
        return (
            f"Status: {self.status}, InitMarginBefore: {self.initMarginBefore}, "
            f"MaintMarginBefore: {self.maintMarginBefore}, "
            f"CommissionAndFees: {self.commissionAndFees}"
        )


@dataclass
class CommissionAndFeesReport:
    """
    Commission and fees report from the official IB API.

    Received via commissionAndFeesReport callback after an execution.
    """
    execId: str = ""
    commissionAndFees: float = 0.0
    currency: str = ""
    realizedPNL: float = 0.0
    yield_: float = 0.0                         # yield is reserved word
    yieldRedemptionDate: int = 0                # YYYYMMDD format

    def __str__(self) -> str:
        return (
            f"ExecId: {self.execId}, CommissionAndFees: {self.commissionAndFees}, "
            f"Currency: {self.currency}, RealizedPnL: {self.realizedPNL}"
        )


@dataclass
class TickAttrib:
    """
    Tick attributes from the official IB API.

    Provides additional information about price ticks.
    """
    canAutoExecute: bool = False
    pastLimit: bool = False
    preOpen: bool = False

    def __str__(self) -> str:
        return (
            f"CanAutoExecute: {int(self.canAutoExecute)}, "
            f"PastLimit: {int(self.pastLimit)}, PreOpen: {int(self.preOpen)}"
        )


@dataclass
class TickAttribBidAsk:
    """
    Bid/Ask tick attributes from the official IB API.

    Used with tick-by-tick bid/ask data.
    """
    bidPastLow: bool = False
    askPastHigh: bool = False

    def __str__(self) -> str:
        return f"BidPastLow: {int(self.bidPastLow)}, AskPastHigh: {int(self.askPastHigh)}"


@dataclass
class TickAttribLast:
    """
    Last tick attributes from the official IB API.

    Used with tick-by-tick last trade data.
    """
    pastLimit: bool = False
    unreported: bool = False

    def __str__(self) -> str:
        return f"PastLimit: {int(self.pastLimit)}, Unreported: {int(self.unreported)}"


@dataclass
class HistoricalTick:
    """
    Historical tick data from the official IB API.
    """
    time: int = 0
    price: float = 0.0
    size: Decimal = field(default_factory=lambda: UNSET_DECIMAL)

    def __str__(self) -> str:
        return f"Time: {self.time}, Price: {self.price}, Size: {self.size}"


@dataclass
class HistoricalTickBidAsk:
    """
    Historical bid/ask tick data from the official IB API.
    """
    time: int = 0
    tickAttribBidAsk: TickAttribBidAsk = field(default_factory=TickAttribBidAsk)
    priceBid: float = 0.0
    priceAsk: float = 0.0
    sizeBid: Decimal = field(default_factory=lambda: UNSET_DECIMAL)
    sizeAsk: Decimal = field(default_factory=lambda: UNSET_DECIMAL)

    def __str__(self) -> str:
        return (
            f"Time: {self.time}, Bid: {self.priceBid}x{self.sizeBid}, "
            f"Ask: {self.priceAsk}x{self.sizeAsk}"
        )


@dataclass
class HistoricalTickLast:
    """
    Historical last trade tick data from the official IB API.
    """
    time: int = 0
    tickAttribLast: TickAttribLast = field(default_factory=TickAttribLast)
    price: float = 0.0
    size: Decimal = field(default_factory=lambda: UNSET_DECIMAL)
    exchange: str = ""
    specialConditions: str = ""

    def __str__(self) -> str:
        return (
            f"Time: {self.time}, Price: {self.price}, Size: {self.size}, "
            f"Exchange: {self.exchange}"
        )


@dataclass
class BarData:
    """
    Bar data (OHLCV) from the official IB API.

    This is the official IB format for historical bar data.
    """
    date: str = ""
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: Decimal = field(default_factory=lambda: UNSET_DECIMAL)
    wap: Decimal = field(default_factory=lambda: UNSET_DECIMAL)
    barCount: int = 0

    def __str__(self) -> str:
        return (
            f"Date: {self.date}, O: {self.open}, H: {self.high}, "
            f"L: {self.low}, C: {self.close}, V: {self.volume}"
        )

    def to_bar(self, symbol: str) -> "Bar":
        """Convert to our Bar model"""
        return Bar(
            symbol=symbol,
            timestamp=self.date,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=int(self.volume) if self.volume != UNSET_DECIMAL else 0,
            wap=float(self.wap) if self.wap != UNSET_DECIMAL else 0.0,
            bar_count=self.barCount,
        )


@dataclass
class RealTimeBar:
    """
    Real-time bar data from the official IB API.

    5-second bars from IB's realTimeBars streaming.
    """
    time: int = 0
    endTime: int = -1
    open_: float = 0.0                          # open is reserved
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: Decimal = field(default_factory=lambda: UNSET_DECIMAL)
    wap: Decimal = field(default_factory=lambda: UNSET_DECIMAL)
    count: int = 0

    def __str__(self) -> str:
        return (
            f"Time: {self.time}, O: {self.open_}, H: {self.high}, "
            f"L: {self.low}, C: {self.close}, V: {self.volume}"
        )


@dataclass
class HistogramData:
    """
    Histogram data point from the official IB API.
    """
    price: float = 0.0
    size: Decimal = field(default_factory=lambda: UNSET_DECIMAL)

    def __str__(self) -> str:
        return f"Price: {self.price}, Size: {self.size}"


@dataclass
class NewsProvider:
    """
    News provider information from the official IB API.
    """
    code: str = ""
    name: str = ""

    def __str__(self) -> str:
        return f"Code: {self.code}, Name: {self.name}"


@dataclass
class DepthMktDataDescription:
    """
    Market depth data description from the official IB API.
    """
    exchange: str = ""
    secType: str = ""
    listingExch: str = ""
    serviceDataType: str = ""
    aggGroup: int = field(default_factory=lambda: UNSET_INTEGER)

    def __str__(self) -> str:
        return (
            f"Exchange: {self.exchange}, SecType: {self.secType}, "
            f"ListingExch: {self.listingExch}"
        )


@dataclass
class SmartComponent:
    """
    SMART routing component from the official IB API.
    """
    bitNumber: int = 0
    exchange: str = ""
    exchangeLetter: str = ""

    def __str__(self) -> str:
        return f"BitNumber: {self.bitNumber}, Exchange: {self.exchange}"


@dataclass
class FamilyCode:
    """
    Family code for linked accounts from the official IB API.
    """
    accountID: str = ""
    familyCodeStr: str = ""

    def __str__(self) -> str:
        return f"AccountId: {self.accountID}, FamilyCodeStr: {self.familyCodeStr}"


@dataclass
class PriceIncrement:
    """
    Price increment rule from the official IB API.
    """
    lowEdge: float = 0.0
    increment: float = 0.0

    def __str__(self) -> str:
        return f"LowEdge: {self.lowEdge}, Increment: {self.increment}"


@dataclass
class HistoricalSession:
    """
    Historical trading session from the official IB API.
    """
    startDateTime: str = ""
    endDateTime: str = ""
    refDate: str = ""

    def __str__(self) -> str:
        return f"Start: {self.startDateTime}, End: {self.endDateTime}, Ref: {self.refDate}"


@dataclass
class WshEventData:
    """
    Wall Street Horizon event data from the official IB API.
    """
    conId: int = field(default_factory=lambda: UNSET_INTEGER)
    filter: str = ""
    fillWatchlist: bool = False
    fillPortfolio: bool = False
    fillCompetitors: bool = False
    startDate: str = ""
    endDate: str = ""
    totalLimit: int = field(default_factory=lambda: UNSET_INTEGER)

    def __str__(self) -> str:
        return f"ConId: {self.conId}, Filter: {self.filter}"


@dataclass
class PnLData:
    """
    Live P&L data from IB's reqPnL / reqPnLSingle subscriptions.

    For account-level updates (reqPnL), symbol is None.
    For per-position updates (reqPnLSingle), symbol identifies the contract.
    """
    account: str
    daily_pnl: float
    unrealized_pnl: float
    realized_pnl: float
    # Only set for reqPnLSingle:
    symbol: Optional[str] = None
    position: int = 0
    value: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)


# Type aliases from official API
TickerId = int
TagValueList = List
ListOfHistoricalTick = List[HistoricalTick]
ListOfHistoricalTickBidAsk = List[HistoricalTickBidAsk]
ListOfHistoricalTickLast = List[HistoricalTickLast]
ListOfHistoricalSessions = List[HistoricalSession]
HistogramDataList = List[HistogramData]
ListOfPriceIncrements = List[PriceIncrement]
ListOfNewsProviders = List[NewsProvider]
ListOfDepthExchanges = List[DepthMktDataDescription]
SmartComponentMap = Dict[int, SmartComponent]
ListOfFamilyCode = List[FamilyCode]
