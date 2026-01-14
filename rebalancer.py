"""
rebalancer.py - Portfolio rebalancing algorithms

Contains strategies for calculating trades needed to rebalance
a portfolio to target allocations.

PLACEHOLDER: Implement your specific rebalancing logic in the
calculate() methods of each strategy class.
"""

import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from dataclasses import dataclass

from .models import (
    Position,
    TargetAllocation,
    RebalanceTrade,
    RebalanceResult,
    RebalanceStrategy,
    OrderAction,
    AssetType,
)
from .portfolio import Portfolio

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class RebalanceConfig:
    """Configuration for rebalancing behavior"""

    # Threshold settings
    drift_threshold_pct: float = 5.0      # Min drift % to trigger rebalance
    min_trade_value: float = 100.0         # Min trade value in dollars
    min_trade_shares: int = 1              # Min shares per trade

    # Position limits
    max_position_pct: float = 25.0         # Max single position %
    min_position_pct: float = 0.0          # Min position % (0 = can sell all)

    # Cash management
    cash_buffer_pct: float = 2.0           # Cash buffer to maintain
    use_available_cash: bool = True        # Use excess cash for buys

    # Order settings
    order_type: str = "MKT"                # Order type: MKT, LMT, etc.
    round_lots: bool = False               # Round to lot sizes

    # Safety settings
    max_trades_per_run: int = 20           # Max trades in single rebalance
    dry_run: bool = True                   # If True, don't execute trades


# =============================================================================
# Base Strategy
# =============================================================================

class RebalanceStrategyBase(ABC):
    """
    Abstract base class for rebalancing strategies.

    Subclass this to implement custom rebalancing logic.
    """

    def __init__(self, config: Optional[RebalanceConfig] = None):
        self.config = config or RebalanceConfig()

    @property
    @abstractmethod
    def strategy_type(self) -> RebalanceStrategy:
        """Return the strategy type enum"""
        pass

    @abstractmethod
    def calculate(
        self,
        positions: List[Position],
        targets: List[TargetAllocation],
        portfolio_value: float,
        available_cash: float = 0.0,
    ) -> RebalanceResult:
        """
        Calculate trades needed to rebalance portfolio.

        Args:
            positions: Current portfolio positions
            targets: Target allocations
            portfolio_value: Total portfolio value
            available_cash: Cash available for purchases

        Returns:
            RebalanceResult with calculated trades
        """
        pass

    def _create_trade(
        self,
        symbol: str,
        current_pct: float,
        target_pct: float,
        price: float,
        portfolio_value: float,
        contract=None,
    ) -> Optional[RebalanceTrade]:
        """
        Helper to create a RebalanceTrade.

        Args:
            symbol: Asset symbol
            current_pct: Current allocation percentage
            target_pct: Target allocation percentage
            price: Current price
            portfolio_value: Total portfolio value
            contract: Optional IB contract

        Returns:
            RebalanceTrade or None if no action needed
        """
        drift = current_pct - target_pct

        # Determine action
        if abs(drift) < self.config.drift_threshold_pct:
            action = OrderAction.HOLD
            quantity = 0
        elif drift > 0:
            action = OrderAction.SELL
            value_diff = (drift / 100) * portfolio_value
            quantity = int(value_diff / price) if price > 0 else 0
        else:
            action = OrderAction.BUY
            value_diff = abs(drift / 100) * portfolio_value
            quantity = int(value_diff / price) if price > 0 else 0

        # Check minimum trade size
        estimated_value = quantity * price
        if estimated_value < self.config.min_trade_value:
            action = OrderAction.HOLD
            quantity = 0

        return RebalanceTrade(
            symbol=symbol,
            action=action,
            quantity=quantity,
            current_allocation=current_pct,
            target_allocation=target_pct,
            drift=drift,
            estimated_value=quantity * price,
            contract=contract,
            reason=f"Drift {drift:+.1f}% exceeds threshold" if action != OrderAction.HOLD else "Within threshold",
        )


# =============================================================================
# Threshold-Based Strategy
# =============================================================================

class ThresholdRebalancer(RebalanceStrategyBase):
    """
    Rebalance when positions drift beyond a threshold.

    This is the most common rebalancing approach. Trades are only
    generated when a position's allocation deviates from target
    by more than the configured threshold.

    PLACEHOLDER: Customize the calculate() method for your needs.
    """

    @property
    def strategy_type(self) -> RebalanceStrategy:
        return RebalanceStrategy.THRESHOLD

    def calculate(
        self,
        positions: List[Position],
        targets: List[TargetAllocation],
        portfolio_value: float,
        available_cash: float = 0.0,
    ) -> RebalanceResult:
        """
        Calculate threshold-based rebalancing trades.

        TODO: Implement your threshold rebalancing logic here.
        """
        logger.info(f"Calculating threshold rebalance (threshold={self.config.drift_threshold_pct}%)")

        trades: List[RebalanceTrade] = []

        # Build lookup maps
        position_map = {p.symbol: p for p in positions}
        target_map = {t.symbol: t for t in targets}

        # Get all symbols (both current and target)
        all_symbols = set(position_map.keys()) | set(target_map.keys())

        for symbol in all_symbols:
            position = position_map.get(symbol)
            target = target_map.get(symbol)

            # Current allocation
            current_pct = position.allocation_pct if position else 0.0
            current_price = position.current_price if position else 0.0

            # Target allocation
            target_pct = target.target_pct if target else 0.0

            # Get price for new positions
            if current_price == 0 and target:
                # PLACEHOLDER: Fetch price for new position
                # current_price = fetch_price(symbol)
                logger.warning(f"No price for {symbol}, skipping")
                continue

            # Create trade if needed
            trade = self._create_trade(
                symbol=symbol,
                current_pct=current_pct,
                target_pct=target_pct,
                price=current_price,
                portfolio_value=portfolio_value,
                contract=position.contract if position else None,
            )

            if trade:
                trades.append(trade)

        # Sort: sells first (to generate cash), then buys
        trades.sort(key=lambda t: (t.action != OrderAction.SELL, -abs(t.drift)))

        return RebalanceResult(
            trades=trades,
            total_portfolio_value=portfolio_value,
            strategy=self.strategy_type,
            threshold_used=self.config.drift_threshold_pct,
        )


# =============================================================================
# Calendar-Based Strategy
# =============================================================================

class CalendarRebalancer(RebalanceStrategyBase):
    """
    Rebalance on a fixed schedule regardless of drift.

    Use this for time-based rebalancing (monthly, quarterly, etc.).
    The scheduler should call calculate() on the desired schedule.

    PLACEHOLDER: Implement scheduling logic as needed.
    """

    @property
    def strategy_type(self) -> RebalanceStrategy:
        return RebalanceStrategy.CALENDAR

    def calculate(
        self,
        positions: List[Position],
        targets: List[TargetAllocation],
        portfolio_value: float,
        available_cash: float = 0.0,
    ) -> RebalanceResult:
        """
        Calculate calendar-based rebalancing trades.

        TODO: Implement your calendar rebalancing logic here.
        Unlike threshold, this rebalances to exact targets.
        """
        logger.info("Calculating calendar rebalance")

        # PLACEHOLDER: For now, rebalance everything to targets
        # without threshold filtering
        old_threshold = self.config.drift_threshold_pct
        self.config.drift_threshold_pct = 0.01  # Very low threshold

        # Use threshold logic with minimal threshold
        threshold_calc = ThresholdRebalancer(self.config)
        result = threshold_calc.calculate(positions, targets, portfolio_value, available_cash)

        # Restore and update result
        self.config.drift_threshold_pct = old_threshold
        result.strategy = self.strategy_type

        return result


# =============================================================================
# Tactical Strategy
# =============================================================================

class TacticalRebalancer(RebalanceStrategyBase):
    """
    Rebalance based on market signals or conditions.

    Use this for dynamic allocation based on market conditions,
    momentum, volatility, or other signals.

    PLACEHOLDER: Implement your signal-based logic.
    """

    @property
    def strategy_type(self) -> RebalanceStrategy:
        return RebalanceStrategy.TACTICAL

    def calculate(
        self,
        positions: List[Position],
        targets: List[TargetAllocation],
        portfolio_value: float,
        available_cash: float = 0.0,
    ) -> RebalanceResult:
        """
        Calculate tactical rebalancing trades.

        TODO: Implement your tactical rebalancing logic here.

        Example signals to consider:
        - Moving average crossovers
        - Volatility regime changes
        - Momentum scores
        - Value/Growth rotation
        - Sector rotation
        """
        logger.info("Calculating tactical rebalance")

        # PLACEHOLDER: Return empty result
        # Implement your signal-based logic here
        return RebalanceResult(
            trades=[],
            total_portfolio_value=portfolio_value,
            strategy=self.strategy_type,
        )

    def adjust_targets_for_signals(
        self,
        targets: List[TargetAllocation],
        signals: Dict[str, float],
    ) -> List[TargetAllocation]:
        """
        Adjust target allocations based on signals.

        PLACEHOLDER: Implement signal-based adjustment.

        Args:
            targets: Base target allocations
            signals: Dict of symbol -> signal strength (-1 to 1)

        Returns:
            Adjusted target allocations
        """
        # TODO: Implement signal-based target adjustment
        return targets


# =============================================================================
# Rebalancer Manager
# =============================================================================

class Rebalancer:
    """
    Main rebalancer interface.

    Orchestrates the rebalancing process, combining portfolio data
    with target allocations and strategy selection.

    Usage:
        rebalancer = Rebalancer(portfolio)
        rebalancer.set_targets([
            TargetAllocation("SPY", 60.0),
            TargetAllocation("BND", 40.0),
        ])
        result = rebalancer.calculate()
        rebalancer.execute(result)  # Execute the trades
    """

    def __init__(
        self,
        portfolio: Optional[Portfolio] = None,
        config: Optional[RebalanceConfig] = None,
    ):
        """
        Initialize the rebalancer.

        Args:
            portfolio: Portfolio instance (optional, can set later)
            config: Rebalance configuration
        """
        self.portfolio = portfolio
        self.config = config or RebalanceConfig()
        self._targets: List[TargetAllocation] = []

        # Strategy instances
        self._strategies = {
            RebalanceStrategy.THRESHOLD: ThresholdRebalancer(self.config),
            RebalanceStrategy.CALENDAR: CalendarRebalancer(self.config),
            RebalanceStrategy.TACTICAL: TacticalRebalancer(self.config),
        }

    def set_portfolio(self, portfolio: Portfolio):
        """Set or update the portfolio instance"""
        self.portfolio = portfolio

    def set_targets(self, targets: List[TargetAllocation]):
        """
        Set target allocations.

        Args:
            targets: List of TargetAllocation objects

        Raises:
            ValueError: If targets don't sum to 100%
        """
        total = sum(t.target_pct for t in targets)
        if not 99.0 <= total <= 101.0:  # Allow small rounding error
            raise ValueError(f"Target allocations must sum to 100%, got {total}%")

        self._targets = targets
        logger.info(f"Set {len(targets)} target allocations")

    def get_targets(self) -> List[TargetAllocation]:
        """Get current target allocations"""
        return self._targets.copy()

    def calculate(
        self,
        strategy: RebalanceStrategy = RebalanceStrategy.THRESHOLD,
    ) -> RebalanceResult:
        """
        Calculate rebalancing trades.

        Args:
            strategy: Which strategy to use

        Returns:
            RebalanceResult with calculated trades
        """
        if not self.portfolio:
            raise ValueError("Portfolio not set")
        if not self._targets:
            raise ValueError("Target allocations not set")

        positions = self.portfolio.positions
        portfolio_value = self.portfolio.total_value

        # Get available cash
        account_summary = self.portfolio.get_account_summary()
        available_cash = account_summary.available_funds if account_summary else 0.0

        # Calculate using selected strategy
        strategy_impl = self._strategies[strategy]
        result = strategy_impl.calculate(
            positions=positions,
            targets=self._targets,
            portfolio_value=portfolio_value,
            available_cash=available_cash,
        )

        logger.info(f"Rebalance calculation complete: {result.trade_count} trades")
        return result

    def execute(self, result: RebalanceResult) -> bool:
        """
        Execute rebalancing trades.

        PLACEHOLDER: Implement trade execution logic.

        Args:
            result: RebalanceResult from calculate()

        Returns:
            True if all trades executed successfully
        """
        if self.config.dry_run:
            logger.info("DRY RUN - Trades not executed")
            for trade in result.actionable_trades:
                logger.info(f"  Would execute: {trade}")
            return True

        if not self.portfolio or not self.portfolio.connected:
            raise ValueError("Portfolio not connected")

        # PLACEHOLDER: Implement actual trade execution
        # For each trade:
        # 1. Create contract
        # 2. Create order
        # 3. Submit via portfolio.placeOrder()
        # 4. Monitor fill status

        logger.warning("Trade execution not implemented")
        return False

    def preview(self, result: RebalanceResult) -> str:
        """
        Generate a preview of rebalancing trades.

        Args:
            result: RebalanceResult from calculate()

        Returns:
            Formatted string preview
        """
        lines = [
            "",
            "=" * 70,
            result.summary(),
            "=" * 70,
            "",
            "Trades:",
            "-" * 70,
        ]

        if not result.actionable_trades:
            lines.append("  No trades needed - portfolio is balanced")
        else:
            for trade in result.actionable_trades:
                lines.append(
                    f"  {trade.action.value:4} {trade.quantity:>8.0f} {trade.symbol:8} "
                    f"| Current: {trade.current_allocation:>5.1f}% "
                    f"| Target: {trade.target_allocation:>5.1f}% "
                    f"| Drift: {trade.drift:>+5.1f}% "
                    f"| Value: ${trade.estimated_value:>10,.2f}"
                )

        lines.append("-" * 70)
        lines.append("")

        return "\n".join(lines)


# =============================================================================
# Convenience Functions
# =============================================================================

def create_equal_weight_targets(
    symbols: List[str],
    asset_type: AssetType = AssetType.EQUITY,
) -> List[TargetAllocation]:
    """
    Create equal-weight target allocations.

    Args:
        symbols: List of symbols
        asset_type: Asset type for all symbols

    Returns:
        List of TargetAllocation with equal weights
    """
    weight = 100.0 / len(symbols)
    return [
        TargetAllocation(symbol=s, target_pct=weight, asset_type=asset_type)
        for s in symbols
    ]


def create_60_40_targets() -> List[TargetAllocation]:
    """
    Create classic 60/40 stock/bond allocation.

    Returns:
        Target allocations for SPY (60%) and BND (40%)
    """
    return [
        TargetAllocation("SPY", 60.0, AssetType.EQUITY),
        TargetAllocation("BND", 40.0, AssetType.EQUITY),  # ETF
    ]


def create_three_fund_targets(
    us_pct: float = 50.0,
    intl_pct: float = 30.0,
    bond_pct: float = 20.0,
) -> List[TargetAllocation]:
    """
    Create three-fund portfolio targets.

    Args:
        us_pct: US stock allocation (default 50%)
        intl_pct: International stock allocation (default 30%)
        bond_pct: Bond allocation (default 20%)

    Returns:
        Target allocations for VTI, VXUS, BND
    """
    return [
        TargetAllocation("VTI", us_pct, AssetType.EQUITY),
        TargetAllocation("VXUS", intl_pct, AssetType.EQUITY),
        TargetAllocation("BND", bond_pct, AssetType.EQUITY),
    ]
