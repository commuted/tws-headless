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

__all__ = [
    # Version
    "__version__",
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
]
