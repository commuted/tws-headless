"""
IB Portfolio Rebalancer

A modular system for managing and rebalancing Interactive Brokers portfolios.

Modules:
    - models: Data classes (Position, TargetAllocation, RebalanceTrade, etc.)
    - client: Base IB API connection wrapper
    - portfolio: Portfolio loading and management
    - rebalancer: Rebalancing strategies and execution
    - command_server: Unix socket command interface

Quick Start:
    from ib import Portfolio, Rebalancer, TargetAllocation

    # Load portfolio
    portfolio = Portfolio()
    portfolio.connect()
    portfolio.load()

    # Setup rebalancer
    rebalancer = Rebalancer(portfolio)
    rebalancer.set_targets([
        TargetAllocation("SPY", 60.0),
        TargetAllocation("BND", 40.0),
    ])

    # Calculate and preview
    result = rebalancer.calculate()
    print(rebalancer.preview(result))

    portfolio.disconnect()
"""

__version__ = "0.1.0"

# Constants (IB API compatible)
from .const import (
    NO_VALID_ID,
    MAX_MSG_LEN,
    UNSET_INTEGER,
    UNSET_LONG,
    UNSET_DOUBLE,
    UNSET_DECIMAL,
    DOUBLE_INFINITY,
    CUSTOMER,
    FIRM,
    AUCTION_UNSET,
    AUCTION_MATCH,
    AUCTION_IMPROVEMENT,
    AUCTION_TRANSPARENT,
)

# Core classes
from .models import (
    Position,
    Bar,
    BarSize,
    TargetAllocation,
    RebalanceTrade,
    RebalanceResult,
    AccountSummary,
    AssetType,
    OrderAction,
    RebalanceStrategy,
    # IB API compatible classes
    Execution,
    ExecutionFilter,
    OrderState,
    OrderAllocation,
    CommissionAndFeesReport,
    TickAttrib,
    TickAttribBidAsk,
    TickAttribLast,
    HistoricalTick,
    HistoricalTickBidAsk,
    HistoricalTickLast,
    BarData,
    RealTimeBar,
    HistogramData,
    NewsProvider,
    DepthMktDataDescription,
    SmartComponent,
    FamilyCode,
    PriceIncrement,
    HistoricalSession,
    WshEventData,
    OptionExerciseType,
    # Type aliases
    TickerId,
    TagValueList,
)

from .client import IBClient

from .portfolio import Portfolio, quick_load

from .rebalancer import (
    Rebalancer,
    RebalanceConfig,
    RebalanceStrategyBase,
    ThresholdRebalancer,
    CalendarRebalancer,
    TacticalRebalancer,
    create_60_40_targets,
    create_three_fund_targets,
    create_equal_weight_targets,
)

from .command_server import (
    CommandServer,
    CommandResult,
    CommandStatus,
    send_command,
    DEFAULT_SOCKET_PATH,
)

from .enter_exit import (
    EnterExit,
    OrderBuilder,
    OrderType,
    AlgoStrategy,
    TimeInForce,
    OrderConfig,
    BracketConfig,
    ScaledOrderConfig,
    AdaptiveConfig,
    EntryExitResult,
)

from .security_pool import (
    SecurityPool,
    Security,
    AssetCategory,
    EquitySubCategory,
    FixedIncomeSubCategory,
    CommoditiesSubCategory,
    RealEstateSubCategory,
    CurrenciesSubCategory,
    CashSubCategory,
    CategoryInfo,
    load_security_pool,
)

from .algorithms import (
    AlgorithmBase,
    AlgorithmInstrument,
    AlgorithmResult,
    AlgorithmRegistry,
    Holdings,
    HoldingPosition,
    TradeSignal,
    SharedHoldings,
    SharedPosition,
    AlgorithmAllocation,
    CashAllocation,
    load_shared_holdings,
    AllocationManager,
    AllocationResult,
    TransferResult,
    AllocationSummary,
    create_manager,
    quick_allocate,
    Momentum5DayAlgorithm,
    DummyAlgorithm,
    get_algorithm,
    list_algorithms,
    create_registry,
)

# Contract and Order builders (from Testbed patterns)
from .contract_builder import ContractBuilder

from .order_builder import OrderFactory

from .algo_params import (
    AlgoParams,
    TWAP,
    VWAP,
    Adaptive,
    PctVol,
    ArrivalPrice,
    MinImpact,
    DarkIce,
    ClosePx,
)

# Connection and streaming infrastructure
from .connection_manager import (
    ConnectionManager,
    ConnectionConfig,
    ConnectionState,
    StreamSubscription,
)

from .data_feed import (
    DataFeed,
    DataType,
    TickData,
    DataBuffer,
    BarAggregator,
    InstrumentSubscription,
)

from .algorithm_runner import (
    AlgorithmRunner,
    ExecutionMode,
    OrderExecutionMode,
    AlgorithmConfig,
    PendingOrder,
    ExecutionResult,
    CircuitBreaker,
)

from .order_reconciler import (
    OrderReconciler,
    ReconciledOrder,
    ReconciliationMode,
    PendingSignal,
    ExecutionAllocation,
)

from .trading_engine import (
    TradingEngine,
    EngineConfig,
    EngineState,
    create_engine,
)

from .rate_limiter import (
    RateLimiter,
    RateLimiterConfig,
    RateLimiterStats,
    OrderRateLimiter,
)

from .auth import (
    TokenStore,
    Authenticator,
    AuthResult,
    generate_token,
    create_token_file,
    load_token,
)

__all__ = [
    # Version
    "__version__",
    # Constants
    "NO_VALID_ID",
    "MAX_MSG_LEN",
    "UNSET_INTEGER",
    "UNSET_LONG",
    "UNSET_DOUBLE",
    "UNSET_DECIMAL",
    "DOUBLE_INFINITY",
    "CUSTOMER",
    "FIRM",
    "AUCTION_UNSET",
    "AUCTION_MATCH",
    "AUCTION_IMPROVEMENT",
    "AUCTION_TRANSPARENT",
    # Models
    "Position",
    "Bar",
    "BarSize",
    "TargetAllocation",
    "RebalanceTrade",
    "RebalanceResult",
    "RebalanceConfig",
    "AccountSummary",
    "AssetType",
    "OrderAction",
    "RebalanceStrategy",
    # IB API compatible models
    "Execution",
    "ExecutionFilter",
    "OrderState",
    "OrderAllocation",
    "CommissionAndFeesReport",
    "TickAttrib",
    "TickAttribBidAsk",
    "TickAttribLast",
    "HistoricalTick",
    "HistoricalTickBidAsk",
    "HistoricalTickLast",
    "BarData",
    "RealTimeBar",
    "HistogramData",
    "NewsProvider",
    "DepthMktDataDescription",
    "SmartComponent",
    "FamilyCode",
    "PriceIncrement",
    "HistoricalSession",
    "WshEventData",
    "OptionExerciseType",
    "TickerId",
    "TagValueList",
    # Client
    "IBClient",
    # Portfolio
    "Portfolio",
    "quick_load",
    # Rebalancer
    "Rebalancer",
    "RebalanceStrategyBase",
    "ThresholdRebalancer",
    "CalendarRebalancer",
    "TacticalRebalancer",
    "create_60_40_targets",
    "create_three_fund_targets",
    "create_equal_weight_targets",
    # Command Server
    "CommandServer",
    "CommandResult",
    "CommandStatus",
    "send_command",
    "DEFAULT_SOCKET_PATH",
    # Enter/Exit
    "EnterExit",
    "OrderBuilder",
    "OrderType",
    "AlgoStrategy",
    "TimeInForce",
    "OrderConfig",
    "BracketConfig",
    "ScaledOrderConfig",
    "AdaptiveConfig",
    "EntryExitResult",
    # Security Pool
    "SecurityPool",
    "Security",
    "AssetCategory",
    "EquitySubCategory",
    "FixedIncomeSubCategory",
    "CommoditiesSubCategory",
    "RealEstateSubCategory",
    "CurrenciesSubCategory",
    "CashSubCategory",
    "CategoryInfo",
    "load_security_pool",
    # Algorithms
    "AlgorithmBase",
    "AlgorithmInstrument",
    "AlgorithmResult",
    "AlgorithmRegistry",
    "Holdings",
    "HoldingPosition",
    "TradeSignal",
    "SharedHoldings",
    "SharedPosition",
    "AlgorithmAllocation",
    "CashAllocation",
    "load_shared_holdings",
    "AllocationManager",
    "AllocationResult",
    "TransferResult",
    "AllocationSummary",
    "create_manager",
    "quick_allocate",
    "Momentum5DayAlgorithm",
    "DummyAlgorithm",
    "get_algorithm",
    "list_algorithms",
    "create_registry",
    # Contract and Order builders (Testbed patterns)
    "ContractBuilder",
    "OrderFactory",
    "AlgoParams",
    "TWAP",
    "VWAP",
    "Adaptive",
    "PctVol",
    "ArrivalPrice",
    "MinImpact",
    "DarkIce",
    "ClosePx",
    # Connection and streaming infrastructure
    "ConnectionManager",
    "ConnectionConfig",
    "ConnectionState",
    "StreamSubscription",
    "DataFeed",
    "DataType",
    "TickData",
    "DataBuffer",
    "BarAggregator",
    "InstrumentSubscription",
    "AlgorithmRunner",
    "ExecutionMode",
    "OrderExecutionMode",
    "AlgorithmConfig",
    "PendingOrder",
    "ExecutionResult",
    "CircuitBreaker",
    "TradingEngine",
    "EngineConfig",
    "EngineState",
    "create_engine",
    # Order reconciliation
    "OrderReconciler",
    "ReconciledOrder",
    "ReconciliationMode",
    "PendingSignal",
    "ExecutionAllocation",
    # Rate limiting
    "RateLimiter",
    "RateLimiterConfig",
    "RateLimiterStats",
    "OrderRateLimiter",
    # Authentication
    "TokenStore",
    "Authenticator",
    "AuthResult",
    "generate_token",
    "create_token_file",
    "load_token",
]
