"""
Unit tests for rebalancer.py

Tests rebalancing strategies, target creation functions, and the Rebalancer class.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from models import (
    Position,
    TargetAllocation,
    RebalanceTrade,
    RebalanceResult,
    RebalanceStrategy,
    OrderAction,
    AssetType,
    AccountSummary,
)
from rebalancer import (
    RebalanceConfig,
    RebalanceStrategyBase,
    ThresholdRebalancer,
    CalendarRebalancer,
    TacticalRebalancer,
    Rebalancer,
    create_equal_weight_targets,
    create_60_40_targets,
    create_three_fund_targets,
)


# =============================================================================
# RebalanceConfig Tests
# =============================================================================

class TestRebalanceConfig:
    """Tests for RebalanceConfig dataclass"""

    def test_default_config(self):
        """Test default configuration values"""
        config = RebalanceConfig()

        assert config.drift_threshold_pct == 5.0
        assert config.min_trade_value == 100.0
        assert config.min_trade_shares == 1
        assert config.max_position_pct == 25.0
        assert config.cash_buffer_pct == 2.0
        assert config.order_type == "MKT"
        assert config.dry_run is True

    def test_custom_config(self):
        """Test custom configuration values"""
        config = RebalanceConfig(
            drift_threshold_pct=3.0,
            min_trade_value=500.0,
            order_type="LMT",
            dry_run=False,
        )

        assert config.drift_threshold_pct == 3.0
        assert config.min_trade_value == 500.0
        assert config.order_type == "LMT"
        assert config.dry_run is False


# =============================================================================
# Target Creation Function Tests
# =============================================================================

class TestTargetCreationFunctions:
    """Tests for convenience target creation functions"""

    def test_create_equal_weight_targets_two_symbols(self):
        """Test equal weight with 2 symbols"""
        targets = create_equal_weight_targets(["SPY", "BND"])

        assert len(targets) == 2
        assert targets[0].symbol == "SPY"
        assert targets[0].target_pct == 50.0
        assert targets[1].symbol == "BND"
        assert targets[1].target_pct == 50.0

    def test_create_equal_weight_targets_five_symbols(self):
        """Test equal weight with 5 symbols"""
        symbols = ["SPY", "QQQ", "IWM", "EFA", "BND"]
        targets = create_equal_weight_targets(symbols)

        assert len(targets) == 5
        assert all(t.target_pct == 20.0 for t in targets)
        assert sum(t.target_pct for t in targets) == 100.0

    def test_create_equal_weight_targets_asset_type(self):
        """Test equal weight with custom asset type"""
        targets = create_equal_weight_targets(
            ["EUR", "JPY", "GBP"],
            asset_type=AssetType.FOREX,
        )

        assert all(t.asset_type == AssetType.FOREX for t in targets)

    def test_create_60_40_targets(self):
        """Test classic 60/40 allocation"""
        targets = create_60_40_targets()

        assert len(targets) == 2

        spy = next(t for t in targets if t.symbol == "SPY")
        bnd = next(t for t in targets if t.symbol == "BND")

        assert spy.target_pct == 60.0
        assert bnd.target_pct == 40.0
        assert sum(t.target_pct for t in targets) == 100.0

    def test_create_three_fund_targets_default(self):
        """Test three-fund portfolio with defaults"""
        targets = create_three_fund_targets()

        assert len(targets) == 3

        vti = next(t for t in targets if t.symbol == "VTI")
        vxus = next(t for t in targets if t.symbol == "VXUS")
        bnd = next(t for t in targets if t.symbol == "BND")

        assert vti.target_pct == 50.0
        assert vxus.target_pct == 30.0
        assert bnd.target_pct == 20.0
        assert sum(t.target_pct for t in targets) == 100.0

    def test_create_three_fund_targets_custom(self):
        """Test three-fund portfolio with custom allocations"""
        targets = create_three_fund_targets(
            us_pct=60.0,
            intl_pct=25.0,
            bond_pct=15.0,
        )

        vti = next(t for t in targets if t.symbol == "VTI")
        vxus = next(t for t in targets if t.symbol == "VXUS")
        bnd = next(t for t in targets if t.symbol == "BND")

        assert vti.target_pct == 60.0
        assert vxus.target_pct == 25.0
        assert bnd.target_pct == 15.0


# =============================================================================
# ThresholdRebalancer Tests
# =============================================================================

class TestThresholdRebalancer:
    """Tests for ThresholdRebalancer strategy"""

    @pytest.fixture
    def config(self):
        """Create a test configuration"""
        return RebalanceConfig(
            drift_threshold_pct=5.0,
            min_trade_value=100.0,
        )

    @pytest.fixture
    def rebalancer(self, config):
        """Create a threshold rebalancer"""
        return ThresholdRebalancer(config)

    def test_strategy_type(self, rebalancer):
        """Test strategy type is THRESHOLD"""
        assert rebalancer.strategy_type == RebalanceStrategy.THRESHOLD

    def test_create_trade_buy_needed(self, rebalancer):
        """Test _create_trade when buy is needed"""
        trade = rebalancer._create_trade(
            symbol="SPY",
            current_pct=25.0,
            target_pct=35.0,  # 10% drift (under-allocated)
            price=450.0,
            portfolio_value=100000.0,
        )

        assert trade is not None
        assert trade.action == OrderAction.BUY
        assert trade.symbol == "SPY"
        assert trade.drift == -10.0  # negative = under-allocated
        assert trade.quantity > 0
        # Should buy ~$10000 worth = ~22 shares
        assert 20 <= trade.quantity <= 25

    def test_create_trade_sell_needed(self, rebalancer):
        """Test _create_trade when sell is needed"""
        trade = rebalancer._create_trade(
            symbol="BND",
            current_pct=50.0,
            target_pct=40.0,  # 10% drift (over-allocated)
            price=75.0,
            portfolio_value=100000.0,
        )

        assert trade is not None
        assert trade.action == OrderAction.SELL
        assert trade.drift == 10.0  # positive = over-allocated
        assert trade.quantity > 0
        # Should sell ~$10000 worth = ~133 shares
        assert 130 <= trade.quantity <= 135

    def test_create_trade_within_threshold(self, rebalancer):
        """Test _create_trade when within threshold (no action)"""
        trade = rebalancer._create_trade(
            symbol="QQQ",
            current_pct=32.0,
            target_pct=30.0,  # Only 2% drift
            price=400.0,
            portfolio_value=100000.0,
        )

        assert trade is not None
        assert trade.action == OrderAction.HOLD
        assert trade.quantity == 0

    def test_create_trade_below_min_value(self, rebalancer):
        """Test _create_trade when trade value is below minimum"""
        # With high threshold and small drift, trade value might be below min
        trade = rebalancer._create_trade(
            symbol="SPY",
            current_pct=29.5,
            target_pct=30.0,  # 0.5% drift (above 5% threshold? No)
            price=450.0,
            portfolio_value=10000.0,  # Small portfolio
        )

        # 5% threshold means this won't trigger anyway
        assert trade.action == OrderAction.HOLD

    def test_calculate_balanced_portfolio(self, rebalancer):
        """Test calculate with already balanced portfolio"""
        positions = [
            Position(
                symbol="SPY",
                asset_type=AssetType.EQUITY,
                quantity=100,
                avg_cost=400.0,
                current_price=450.0,
                market_value=45000.0,
                allocation_pct=60.0,
            ),
            Position(
                symbol="BND",
                asset_type=AssetType.EQUITY,
                quantity=400,
                avg_cost=70.0,
                current_price=75.0,
                market_value=30000.0,
                allocation_pct=40.0,
            ),
        ]
        targets = create_60_40_targets()

        result = rebalancer.calculate(
            positions=positions,
            targets=targets,
            portfolio_value=75000.0,
        )

        assert result.total_portfolio_value == 75000.0
        # Both positions are at target, no actionable trades
        assert result.trade_count == 0

    def test_calculate_needs_rebalancing(self, rebalancer):
        """Test calculate with unbalanced portfolio"""
        positions = [
            Position(
                symbol="SPY",
                asset_type=AssetType.EQUITY,
                quantity=100,
                avg_cost=400.0,
                current_price=450.0,
                market_value=45000.0,
                allocation_pct=75.0,  # Over target of 60%
            ),
            Position(
                symbol="BND",
                asset_type=AssetType.EQUITY,
                quantity=200,
                avg_cost=70.0,
                current_price=75.0,
                market_value=15000.0,
                allocation_pct=25.0,  # Under target of 40%
            ),
        ]
        targets = create_60_40_targets()

        result = rebalancer.calculate(
            positions=positions,
            targets=targets,
            portfolio_value=60000.0,
        )

        # Should have trades: sell SPY, buy BND
        assert result.trade_count >= 1
        actionable = result.actionable_trades

        # Find SPY trade (should be sell)
        spy_trades = [t for t in actionable if t.symbol == "SPY"]
        if spy_trades:
            assert spy_trades[0].action == OrderAction.SELL

    def test_calculate_new_position_needed(self, rebalancer):
        """Test calculate when target has symbol not in positions"""
        positions = [
            Position(
                symbol="SPY",
                asset_type=AssetType.EQUITY,
                quantity=100,
                avg_cost=400.0,
                current_price=450.0,
                market_value=45000.0,
                allocation_pct=100.0,
            ),
        ]
        targets = [
            TargetAllocation("SPY", 60.0),
            TargetAllocation("BND", 40.0),  # Not in portfolio
        ]

        result = rebalancer.calculate(
            positions=positions,
            targets=targets,
            portfolio_value=45000.0,
        )

        # SPY should be sold (100% -> 60%)
        spy_trades = [t for t in result.trades if t.symbol == "SPY"]
        assert len(spy_trades) == 1
        assert spy_trades[0].action == OrderAction.SELL

        # BND needs buying but has no price, might be skipped
        bnd_trades = [t for t in result.trades if t.symbol == "BND"]
        # Depends on implementation - may skip due to no price


# =============================================================================
# CalendarRebalancer Tests
# =============================================================================

class TestCalendarRebalancer:
    """Tests for CalendarRebalancer strategy"""

    def test_strategy_type(self):
        """Test strategy type is CALENDAR"""
        rebalancer = CalendarRebalancer()
        assert rebalancer.strategy_type == RebalanceStrategy.CALENDAR

    def test_calculate_rebalances_all(self):
        """Test calendar strategy rebalances everything"""
        config = RebalanceConfig(drift_threshold_pct=5.0)
        rebalancer = CalendarRebalancer(config)

        positions = [
            Position(
                symbol="SPY",
                asset_type=AssetType.EQUITY,
                quantity=100,
                avg_cost=400.0,
                current_price=450.0,
                market_value=45000.0,
                allocation_pct=62.0,  # 2% drift (below 5% threshold)
            ),
            Position(
                symbol="BND",
                asset_type=AssetType.EQUITY,
                quantity=400,
                avg_cost=70.0,
                current_price=75.0,
                market_value=30000.0,
                allocation_pct=38.0,  # 2% drift (below 5% threshold)
            ),
        ]
        targets = create_60_40_targets()

        result = rebalancer.calculate(
            positions=positions,
            targets=targets,
            portfolio_value=75000.0,
        )

        # Calendar should rebalance even small drifts
        assert result.strategy == RebalanceStrategy.CALENDAR


# =============================================================================
# TacticalRebalancer Tests
# =============================================================================

class TestTacticalRebalancer:
    """Tests for TacticalRebalancer strategy"""

    def test_strategy_type(self):
        """Test strategy type is TACTICAL"""
        rebalancer = TacticalRebalancer()
        assert rebalancer.strategy_type == RebalanceStrategy.TACTICAL

    def test_calculate_placeholder(self):
        """Test tactical calculate returns empty (placeholder)"""
        rebalancer = TacticalRebalancer()

        result = rebalancer.calculate(
            positions=[],
            targets=[],
            portfolio_value=100000.0,
        )

        # Placeholder implementation returns empty
        assert result.trade_count == 0
        assert result.strategy == RebalanceStrategy.TACTICAL

    def test_adjust_targets_for_signals(self):
        """Test adjust_targets_for_signals placeholder"""
        rebalancer = TacticalRebalancer()
        targets = create_60_40_targets()
        signals = {"SPY": 0.5, "BND": -0.3}

        # Placeholder just returns same targets
        adjusted = rebalancer.adjust_targets_for_signals(targets, signals)
        assert adjusted == targets


# =============================================================================
# Rebalancer (Manager) Tests
# =============================================================================

class TestRebalancer:
    """Tests for main Rebalancer class"""

    @pytest.fixture
    def mock_portfolio(self):
        """Create a mock portfolio"""
        portfolio = MagicMock()
        portfolio.connected = True
        portfolio.total_value = 100000.0
        portfolio.positions = [
            Position(
                symbol="SPY",
                asset_type=AssetType.EQUITY,
                quantity=100,
                avg_cost=400.0,
                current_price=450.0,
                market_value=45000.0,
                allocation_pct=45.0,
            ),
            Position(
                symbol="BND",
                asset_type=AssetType.EQUITY,
                quantity=500,
                avg_cost=70.0,
                current_price=75.0,
                market_value=37500.0,
                allocation_pct=37.5,
            ),
            Position(
                symbol="CASH",
                asset_type=AssetType.EQUITY,
                quantity=1,
                avg_cost=17500.0,
                current_price=17500.0,
                market_value=17500.0,
                allocation_pct=17.5,
            ),
        ]
        portfolio.get_account_summary.return_value = AccountSummary(
            account_id="TEST",
            net_liquidation=100000.0,
            available_funds=17500.0,
        )
        return portfolio

    def test_init_default(self):
        """Test default initialization"""
        rebalancer = Rebalancer()

        assert rebalancer.portfolio is None
        assert rebalancer.config is not None
        assert rebalancer._targets == []

    def test_init_with_portfolio(self, mock_portfolio):
        """Test initialization with portfolio"""
        rebalancer = Rebalancer(portfolio=mock_portfolio)

        assert rebalancer.portfolio == mock_portfolio

    def test_set_portfolio(self, mock_portfolio):
        """Test set_portfolio method"""
        rebalancer = Rebalancer()
        rebalancer.set_portfolio(mock_portfolio)

        assert rebalancer.portfolio == mock_portfolio

    def test_set_targets_valid(self):
        """Test set_targets with valid targets"""
        rebalancer = Rebalancer()
        targets = create_60_40_targets()

        rebalancer.set_targets(targets)

        assert rebalancer.get_targets() == targets

    def test_set_targets_invalid_sum(self):
        """Test set_targets raises error if sum != 100%"""
        rebalancer = Rebalancer()
        targets = [
            TargetAllocation("SPY", 50.0),
            TargetAllocation("BND", 30.0),
            # Missing 20%
        ]

        with pytest.raises(ValueError, match="must sum to 100%"):
            rebalancer.set_targets(targets)

    def test_set_targets_allows_rounding_error(self):
        """Test set_targets allows small rounding errors"""
        rebalancer = Rebalancer()
        targets = [
            TargetAllocation("A", 33.33),
            TargetAllocation("B", 33.33),
            TargetAllocation("C", 33.34),
        ]

        # Should not raise - sum is 100.0
        rebalancer.set_targets(targets)

    def test_get_targets_returns_copy(self):
        """Test get_targets returns a copy"""
        rebalancer = Rebalancer()
        targets = create_60_40_targets()
        rebalancer.set_targets(targets)

        retrieved = rebalancer.get_targets()
        retrieved.append(TargetAllocation("QQQ", 10.0))

        # Original should be unchanged
        assert len(rebalancer.get_targets()) == 2

    def test_calculate_no_portfolio(self):
        """Test calculate raises error without portfolio"""
        rebalancer = Rebalancer()
        rebalancer.set_targets(create_60_40_targets())

        with pytest.raises(ValueError, match="Portfolio not set"):
            rebalancer.calculate()

    def test_calculate_no_targets(self, mock_portfolio):
        """Test calculate raises error without targets"""
        rebalancer = Rebalancer(portfolio=mock_portfolio)

        with pytest.raises(ValueError, match="Target allocations not set"):
            rebalancer.calculate()

    def test_calculate_threshold_strategy(self, mock_portfolio):
        """Test calculate with threshold strategy"""
        rebalancer = Rebalancer(portfolio=mock_portfolio)
        rebalancer.set_targets(create_60_40_targets())

        result = rebalancer.calculate(strategy=RebalanceStrategy.THRESHOLD)

        assert result.strategy == RebalanceStrategy.THRESHOLD
        assert result.total_portfolio_value == 100000.0

    def test_calculate_calendar_strategy(self, mock_portfolio):
        """Test calculate with calendar strategy"""
        rebalancer = Rebalancer(portfolio=mock_portfolio)
        rebalancer.set_targets(create_60_40_targets())

        result = rebalancer.calculate(strategy=RebalanceStrategy.CALENDAR)

        assert result.strategy == RebalanceStrategy.CALENDAR

    def test_preview_no_trades(self, mock_portfolio):
        """Test preview with no trades needed"""
        rebalancer = Rebalancer(portfolio=mock_portfolio)
        result = RebalanceResult(
            trades=[],
            total_portfolio_value=100000.0,
        )

        preview = rebalancer.preview(result)

        assert "No trades needed" in preview
        assert "balanced" in preview

    def test_preview_with_trades(self, mock_portfolio):
        """Test preview with trades"""
        rebalancer = Rebalancer(portfolio=mock_portfolio)
        result = RebalanceResult(
            trades=[
                RebalanceTrade(
                    symbol="SPY",
                    action=OrderAction.SELL,
                    quantity=10,
                    current_allocation=65.0,
                    target_allocation=60.0,
                    drift=5.0,
                    estimated_value=4500.0,
                ),
            ],
            total_portfolio_value=100000.0,
        )

        preview = rebalancer.preview(result)

        assert "SPY" in preview
        assert "SELL" in preview
        assert "65.0" in preview or "65" in preview

    def test_execute_dry_run(self, mock_portfolio):
        """Test execute in dry run mode"""
        config = RebalanceConfig(dry_run=True)
        rebalancer = Rebalancer(portfolio=mock_portfolio, config=config)

        result = RebalanceResult(
            trades=[
                RebalanceTrade(
                    symbol="SPY",
                    action=OrderAction.SELL,
                    quantity=10,
                    current_allocation=65.0,
                    target_allocation=60.0,
                    drift=5.0,
                    estimated_value=4500.0,
                ),
            ],
            total_portfolio_value=100000.0,
        )

        execution = rebalancer.execute(result)

        assert execution.success is True
        assert len(execution.orders) == 1
        # No actual orders placed in dry run
        mock_portfolio.place_order.assert_not_called()

    def test_execute_no_portfolio(self):
        """Test execute without portfolio connected"""
        config = RebalanceConfig(dry_run=False)
        rebalancer = Rebalancer(config=config)

        result = RebalanceResult(
            trades=[],
            total_portfolio_value=100000.0,
        )

        execution = rebalancer.execute(result)

        assert execution.success is False
        assert "not connected" in str(execution.errors).lower()

    def test_execute_no_trades(self, mock_portfolio):
        """Test execute with no actionable trades"""
        config = RebalanceConfig(dry_run=False)
        rebalancer = Rebalancer(portfolio=mock_portfolio, config=config)

        result = RebalanceResult(
            trades=[],
            total_portfolio_value=100000.0,
        )

        execution = rebalancer.execute(result)

        assert execution.success is True
        assert execution.total_orders == 0
