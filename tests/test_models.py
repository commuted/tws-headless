"""
Unit tests for models.py

Tests data classes: Position, Bar, TargetAllocation, RebalanceTrade,
RebalanceResult, OrderRecord, ExecutionResult, and enums.
"""

import pytest
from unittest.mock import MagicMock
from models import (
    AssetType,
    OrderAction,
    RebalanceStrategy,
    BarSize,
    Position,
    Bar,
    TargetAllocation,
    RebalanceTrade,
    RebalanceResult,
    AccountSummary,
    OrderStatus,
    OrderRecord,
    ExecutionResult,
)


# =============================================================================
# AssetType Tests
# =============================================================================

class TestAssetType:
    """Tests for AssetType enum"""

    def test_from_sec_type_stock(self):
        """Test STK maps to EQUITY"""
        assert AssetType.from_sec_type("STK") == AssetType.EQUITY

    def test_from_sec_type_option(self):
        """Test OPT maps to OPTION"""
        assert AssetType.from_sec_type("OPT") == AssetType.OPTION

    def test_from_sec_type_future(self):
        """Test FUT maps to FUTURE"""
        assert AssetType.from_sec_type("FUT") == AssetType.FUTURE

    def test_from_sec_type_forex(self):
        """Test CASH maps to FOREX"""
        assert AssetType.from_sec_type("CASH") == AssetType.FOREX

    def test_from_sec_type_index(self):
        """Test IND maps to INDEX"""
        assert AssetType.from_sec_type("IND") == AssetType.INDEX

    def test_from_sec_type_bond(self):
        """Test BOND maps to BOND"""
        assert AssetType.from_sec_type("BOND") == AssetType.BOND

    def test_from_sec_type_unknown(self):
        """Test unknown type maps to UNKNOWN"""
        assert AssetType.from_sec_type("XYZ") == AssetType.UNKNOWN
        assert AssetType.from_sec_type("") == AssetType.UNKNOWN

    def test_enum_values(self):
        """Test enum values match IB secType strings"""
        assert AssetType.EQUITY.value == "STK"
        assert AssetType.OPTION.value == "OPT"
        assert AssetType.FUTURE.value == "FUT"


# =============================================================================
# Position Tests
# =============================================================================

class TestPosition:
    """Tests for Position dataclass"""

    @pytest.fixture
    def sample_position(self):
        """Create a sample position for testing"""
        return Position(
            symbol="AAPL",
            asset_type=AssetType.EQUITY,
            quantity=100,
            avg_cost=150.0,
            current_price=175.0,
            market_value=17500.0,
            unrealized_pnl=2500.0,
            allocation_pct=25.0,
        )

    def test_position_creation(self, sample_position):
        """Test position is created with correct values"""
        assert sample_position.symbol == "AAPL"
        assert sample_position.quantity == 100
        assert sample_position.avg_cost == 150.0
        assert sample_position.current_price == 175.0

    def test_update_market_data(self):
        """Test update_market_data updates price, value, and P&L"""
        pos = Position(
            symbol="AAPL",
            asset_type=AssetType.EQUITY,
            quantity=100,
            avg_cost=150.0,
        )
        pos.update_market_data(180.0)

        assert pos.current_price == 180.0
        assert pos.market_value == 18000.0  # 100 * 180
        assert pos.unrealized_pnl == 3000.0  # 18000 - 15000

    def test_update_market_data_loss(self):
        """Test update_market_data with price decrease (loss)"""
        pos = Position(
            symbol="AAPL",
            asset_type=AssetType.EQUITY,
            quantity=100,
            avg_cost=150.0,
        )
        pos.update_market_data(140.0)

        assert pos.current_price == 140.0
        assert pos.market_value == 14000.0
        assert pos.unrealized_pnl == -1000.0  # Loss

    def test_cost_basis(self, sample_position):
        """Test cost_basis property"""
        assert sample_position.cost_basis == 15000.0  # 100 * 150

    def test_return_pct_positive(self, sample_position):
        """Test return_pct with positive return"""
        # P&L is 2500, cost basis is 15000
        expected = (2500.0 / 15000.0) * 100
        assert abs(sample_position.return_pct - expected) < 0.01

    def test_return_pct_zero_cost(self):
        """Test return_pct with zero cost basis"""
        pos = Position(
            symbol="FREE",
            asset_type=AssetType.EQUITY,
            quantity=0,
            avg_cost=0.0,
        )
        assert pos.return_pct == 0.0

    def test_to_dict(self, sample_position):
        """Test to_dict produces correct dictionary"""
        d = sample_position.to_dict()

        assert d["symbol"] == "AAPL"
        assert d["asset_type"] == "STK"
        assert d["quantity"] == 100
        assert d["avg_cost"] == 150.0
        assert d["current_price"] == 175.0
        assert d["market_value"] == 17500.0
        assert d["unrealized_pnl"] == 2500.0
        assert d["allocation_pct"] == 25.0
        assert "return_pct" in d

    def test_repr(self, sample_position):
        """Test __repr__ produces readable string"""
        repr_str = repr(sample_position)
        assert "AAPL" in repr_str
        assert "100" in repr_str


# =============================================================================
# Bar Tests
# =============================================================================

class TestBar:
    """Tests for Bar dataclass (OHLCV candlestick)"""

    @pytest.fixture
    def bullish_bar(self):
        """Create a bullish bar (close > open)"""
        return Bar(
            symbol="SPY",
            timestamp="2024-01-15T10:00:00",
            open=450.0,
            high=455.0,
            low=448.0,
            close=454.0,
            volume=1000000,
        )

    @pytest.fixture
    def bearish_bar(self):
        """Create a bearish bar (close < open)"""
        return Bar(
            symbol="SPY",
            timestamp="2024-01-15T10:00:05",
            open=454.0,
            high=455.0,
            low=450.0,
            close=451.0,
            volume=800000,
        )

    def test_bar_creation(self, bullish_bar):
        """Test bar is created with correct values"""
        assert bullish_bar.symbol == "SPY"
        assert bullish_bar.open == 450.0
        assert bullish_bar.high == 455.0
        assert bullish_bar.low == 448.0
        assert bullish_bar.close == 454.0
        assert bullish_bar.volume == 1000000

    def test_range(self, bullish_bar):
        """Test range property (high - low)"""
        assert bullish_bar.range == 7.0  # 455 - 448

    def test_body(self, bullish_bar, bearish_bar):
        """Test body property (absolute difference open/close)"""
        assert bullish_bar.body == 4.0  # |454 - 450|
        assert bearish_bar.body == 3.0  # |451 - 454|

    def test_is_bullish(self, bullish_bar, bearish_bar):
        """Test is_bullish property"""
        assert bullish_bar.is_bullish is True
        assert bearish_bar.is_bullish is False

    def test_is_bearish(self, bullish_bar, bearish_bar):
        """Test is_bearish property"""
        assert bullish_bar.is_bearish is False
        assert bearish_bar.is_bearish is True

    def test_mid(self, bullish_bar):
        """Test mid property (midpoint of high/low)"""
        assert bullish_bar.mid == 451.5  # (455 + 448) / 2

    def test_doji_bar(self):
        """Test bar where open == close (doji)"""
        doji = Bar(
            symbol="SPY",
            timestamp="2024-01-15T10:00:10",
            open=450.0,
            high=452.0,
            low=448.0,
            close=450.0,
            volume=500000,
        )
        assert doji.is_bullish is False
        assert doji.is_bearish is False
        assert doji.body == 0.0

    def test_to_dict(self, bullish_bar):
        """Test to_dict produces correct dictionary"""
        d = bullish_bar.to_dict()

        assert d["symbol"] == "SPY"
        assert d["timestamp"] == "2024-01-15T10:00:00"
        assert d["open"] == 450.0
        assert d["high"] == 455.0
        assert d["low"] == 448.0
        assert d["close"] == 454.0
        assert d["volume"] == 1000000

    def test_repr(self, bullish_bar):
        """Test __repr__ shows direction indicator"""
        repr_str = repr(bullish_bar)
        assert "SPY" in repr_str
        assert "+" in repr_str  # Bullish indicator


# =============================================================================
# TargetAllocation Tests
# =============================================================================

class TestTargetAllocation:
    """Tests for TargetAllocation dataclass"""

    def test_valid_allocation(self):
        """Test creating a valid target allocation"""
        target = TargetAllocation(symbol="SPY", target_pct=60.0)
        assert target.symbol == "SPY"
        assert target.target_pct == 60.0
        assert target.asset_type == AssetType.EQUITY
        assert target.exchange == "SMART"
        assert target.currency == "USD"

    def test_allocation_with_bounds(self):
        """Test allocation with min/max bounds"""
        target = TargetAllocation(
            symbol="BND",
            target_pct=40.0,
            min_pct=30.0,
            max_pct=50.0,
        )
        assert target.min_pct == 30.0
        assert target.max_pct == 50.0

    def test_invalid_target_pct_negative(self):
        """Test that negative target_pct raises ValueError"""
        with pytest.raises(ValueError, match="target_pct must be 0-100"):
            TargetAllocation(symbol="SPY", target_pct=-5.0)

    def test_invalid_target_pct_over_100(self):
        """Test that target_pct > 100 raises ValueError"""
        with pytest.raises(ValueError, match="target_pct must be 0-100"):
            TargetAllocation(symbol="SPY", target_pct=150.0)

    def test_invalid_min_greater_than_target(self):
        """Test that min_pct > target_pct raises ValueError"""
        with pytest.raises(ValueError, match="min_pct <= target_pct <= max_pct"):
            TargetAllocation(symbol="SPY", target_pct=40.0, min_pct=50.0)

    def test_invalid_target_greater_than_max(self):
        """Test that target_pct > max_pct raises ValueError"""
        with pytest.raises(ValueError, match="min_pct <= target_pct <= max_pct"):
            TargetAllocation(symbol="SPY", target_pct=60.0, max_pct=50.0)

    def test_create_contract(self):
        """Test create_contract produces valid IB Contract"""
        target = TargetAllocation(
            symbol="AAPL",
            target_pct=25.0,
            asset_type=AssetType.EQUITY,
            exchange="NASDAQ",
            currency="USD",
        )
        contract = target.create_contract()

        assert contract.symbol == "AAPL"
        assert contract.secType == "STK"
        assert contract.exchange == "NASDAQ"
        assert contract.currency == "USD"


# =============================================================================
# RebalanceTrade Tests
# =============================================================================

class TestRebalanceTrade:
    """Tests for RebalanceTrade dataclass"""

    @pytest.fixture
    def buy_trade(self):
        """Create a sample buy trade"""
        return RebalanceTrade(
            symbol="SPY",
            action=OrderAction.BUY,
            quantity=10,
            current_allocation=25.0,
            target_allocation=30.0,
            drift=-5.0,
            estimated_value=5000.0,
        )

    @pytest.fixture
    def sell_trade(self):
        """Create a sample sell trade"""
        return RebalanceTrade(
            symbol="BND",
            action=OrderAction.SELL,
            quantity=20,
            current_allocation=45.0,
            target_allocation=40.0,
            drift=5.0,
            estimated_value=2000.0,
        )

    @pytest.fixture
    def hold_trade(self):
        """Create a hold (no action) trade"""
        return RebalanceTrade(
            symbol="QQQ",
            action=OrderAction.HOLD,
            quantity=0,
            current_allocation=30.0,
            target_allocation=30.0,
            drift=0.0,
            estimated_value=0.0,
        )

    def test_is_actionable_buy(self, buy_trade):
        """Test buy trade is actionable"""
        assert buy_trade.is_actionable is True

    def test_is_actionable_sell(self, sell_trade):
        """Test sell trade is actionable"""
        assert sell_trade.is_actionable is True

    def test_is_actionable_hold(self, hold_trade):
        """Test hold trade is not actionable"""
        assert hold_trade.is_actionable is False

    def test_is_actionable_zero_quantity(self):
        """Test trade with zero quantity is not actionable"""
        trade = RebalanceTrade(
            symbol="SPY",
            action=OrderAction.BUY,
            quantity=0,
            current_allocation=30.0,
            target_allocation=30.0,
            drift=0.0,
            estimated_value=0.0,
        )
        assert trade.is_actionable is False

    def test_create_order_market(self, buy_trade):
        """Test create_order creates market order"""
        order = buy_trade.create_order("MKT")
        assert order.action == "BUY"
        assert order.totalQuantity == 10
        assert order.orderType == "MKT"

    def test_create_order_limit(self, sell_trade):
        """Test create_order creates limit order"""
        order = sell_trade.create_order("LMT")
        assert order.action == "SELL"
        assert order.totalQuantity == 20
        assert order.orderType == "LMT"

    def test_repr(self, buy_trade):
        """Test __repr__ shows trade info"""
        repr_str = repr(buy_trade)
        assert "BUY" in repr_str
        assert "SPY" in repr_str
        assert "10" in repr_str


# =============================================================================
# RebalanceResult Tests
# =============================================================================

class TestRebalanceResult:
    """Tests for RebalanceResult dataclass"""

    @pytest.fixture
    def sample_result(self):
        """Create a sample rebalance result with trades"""
        trades = [
            RebalanceTrade(
                symbol="SPY",
                action=OrderAction.BUY,
                quantity=10,
                current_allocation=25.0,
                target_allocation=30.0,
                drift=-5.0,
                estimated_value=5000.0,
            ),
            RebalanceTrade(
                symbol="BND",
                action=OrderAction.SELL,
                quantity=20,
                current_allocation=45.0,
                target_allocation=40.0,
                drift=5.0,
                estimated_value=2000.0,
            ),
            RebalanceTrade(
                symbol="QQQ",
                action=OrderAction.HOLD,
                quantity=0,
                current_allocation=30.0,
                target_allocation=30.0,
                drift=0.0,
                estimated_value=0.0,
            ),
        ]
        return RebalanceResult(
            trades=trades,
            total_portfolio_value=100000.0,
        )

    def test_result_creation(self, sample_result):
        """Test result is created with correct values"""
        assert sample_result.total_portfolio_value == 100000.0
        assert len(sample_result.trades) == 3

    def test_total_buy_value(self, sample_result):
        """Test total_buy_value is calculated correctly"""
        assert sample_result.total_buy_value == 5000.0

    def test_total_sell_value(self, sample_result):
        """Test total_sell_value is calculated correctly"""
        assert sample_result.total_sell_value == 2000.0

    def test_net_cash_flow(self, sample_result):
        """Test net_cash_flow (sells - buys)"""
        assert sample_result.net_cash_flow == -3000.0  # 2000 - 5000

    def test_actionable_trades(self, sample_result):
        """Test actionable_trades filters out holds"""
        actionable = sample_result.actionable_trades
        assert len(actionable) == 2
        symbols = [t.symbol for t in actionable]
        assert "SPY" in symbols
        assert "BND" in symbols
        assert "QQQ" not in symbols

    def test_trade_count(self, sample_result):
        """Test trade_count returns actionable count"""
        assert sample_result.trade_count == 2

    def test_empty_result(self):
        """Test result with no trades"""
        result = RebalanceResult(trades=[], total_portfolio_value=100000.0)
        assert result.total_buy_value == 0.0
        assert result.total_sell_value == 0.0
        assert result.net_cash_flow == 0.0
        assert result.trade_count == 0

    def test_summary(self, sample_result):
        """Test summary produces readable string"""
        summary = sample_result.summary()
        assert "Rebalance Summary" in summary
        assert "100,000.00" in summary
        assert "2" in summary  # trade count


# =============================================================================
# OrderStatus Tests
# =============================================================================

class TestOrderStatus:
    """Tests for OrderStatus enum"""

    def test_from_ib_status_filled(self):
        """Test Filled status mapping"""
        assert OrderStatus.from_ib_status("Filled") == OrderStatus.FILLED

    def test_from_ib_status_submitted(self):
        """Test Submitted status mapping"""
        assert OrderStatus.from_ib_status("Submitted") == OrderStatus.SUBMITTED

    def test_from_ib_status_cancelled(self):
        """Test Cancelled status mapping"""
        assert OrderStatus.from_ib_status("Cancelled") == OrderStatus.CANCELLED
        assert OrderStatus.from_ib_status("ApiCancelled") == OrderStatus.CANCELLED

    def test_from_ib_status_pending_variants(self):
        """Test pending status variants"""
        assert OrderStatus.from_ib_status("PendingSubmit") == OrderStatus.PENDING
        assert OrderStatus.from_ib_status("PendingCancel") == OrderStatus.PENDING
        assert OrderStatus.from_ib_status("PreSubmitted") == OrderStatus.PENDING

    def test_from_ib_status_error(self):
        """Test Error status mapping"""
        assert OrderStatus.from_ib_status("Error") == OrderStatus.ERROR

    def test_from_ib_status_unknown(self):
        """Test unknown status maps to UNKNOWN"""
        assert OrderStatus.from_ib_status("SomeWeirdStatus") == OrderStatus.UNKNOWN
        assert OrderStatus.from_ib_status("") == OrderStatus.UNKNOWN


# =============================================================================
# OrderRecord Tests
# =============================================================================

class TestOrderRecord:
    """Tests for OrderRecord dataclass"""

    @pytest.fixture
    def pending_order(self):
        """Create a pending order"""
        return OrderRecord(
            order_id=1,
            symbol="AAPL",
            action="BUY",
            quantity=100,
            status=OrderStatus.PENDING,
        )

    @pytest.fixture
    def filled_order(self):
        """Create a filled order"""
        return OrderRecord(
            order_id=2,
            symbol="AAPL",
            action="BUY",
            quantity=100,
            status=OrderStatus.FILLED,
            filled_quantity=100,
            avg_fill_price=175.50,
        )

    @pytest.fixture
    def partial_order(self):
        """Create a partially filled order"""
        return OrderRecord(
            order_id=3,
            symbol="AAPL",
            action="BUY",
            quantity=100,
            status=OrderStatus.PARTIALLY_FILLED,
            filled_quantity=50,
            avg_fill_price=175.00,
            remaining=50,
        )

    def test_is_complete_pending(self, pending_order):
        """Test pending order is not complete"""
        assert pending_order.is_complete is False

    def test_is_complete_filled(self, filled_order):
        """Test filled order is complete"""
        assert filled_order.is_complete is True

    def test_is_complete_cancelled(self):
        """Test cancelled order is complete"""
        order = OrderRecord(
            order_id=4,
            symbol="AAPL",
            action="BUY",
            quantity=100,
            status=OrderStatus.CANCELLED,
        )
        assert order.is_complete is True

    def test_is_complete_error(self):
        """Test error order is complete"""
        order = OrderRecord(
            order_id=5,
            symbol="AAPL",
            action="BUY",
            quantity=100,
            status=OrderStatus.ERROR,
            error_message="Insufficient funds",
        )
        assert order.is_complete is True

    def test_is_filled(self, filled_order, pending_order, partial_order):
        """Test is_filled property"""
        assert filled_order.is_filled is True
        assert pending_order.is_filled is False
        assert partial_order.is_filled is False

    def test_fill_value(self, filled_order):
        """Test fill_value calculation"""
        assert filled_order.fill_value == 17550.0  # 100 * 175.50

    def test_fill_value_partial(self, partial_order):
        """Test fill_value for partial fill"""
        assert partial_order.fill_value == 8750.0  # 50 * 175.00

    def test_repr(self, filled_order):
        """Test __repr__ shows order info"""
        repr_str = repr(filled_order)
        assert "BUY" in repr_str
        assert "AAPL" in repr_str
        assert "100" in repr_str


# =============================================================================
# ExecutionResult Tests
# =============================================================================

class TestExecutionResult:
    """Tests for ExecutionResult dataclass"""

    @pytest.fixture
    def successful_execution(self):
        """Create a successful execution result"""
        orders = [
            OrderRecord(
                order_id=1,
                symbol="SPY",
                action="BUY",
                quantity=10,
                status=OrderStatus.FILLED,
                filled_quantity=10,
                avg_fill_price=450.0,
            ),
            OrderRecord(
                order_id=2,
                symbol="BND",
                action="SELL",
                quantity=20,
                status=OrderStatus.FILLED,
                filled_quantity=20,
                avg_fill_price=75.0,
            ),
        ]
        return ExecutionResult(success=True, orders=orders)

    def test_execution_counts(self, successful_execution):
        """Test order counts are calculated"""
        assert successful_execution.total_orders == 2
        assert successful_execution.filled_orders == 2
        assert successful_execution.failed_orders == 0

    def test_execution_values(self, successful_execution):
        """Test buy/sell values are calculated"""
        assert successful_execution.total_buy_value == 4500.0  # 10 * 450
        assert successful_execution.total_sell_value == 1500.0  # 20 * 75

    def test_execution_with_failures(self):
        """Test execution with some failed orders"""
        orders = [
            OrderRecord(
                order_id=1,
                symbol="SPY",
                action="BUY",
                quantity=10,
                status=OrderStatus.FILLED,
                filled_quantity=10,
                avg_fill_price=450.0,
            ),
            OrderRecord(
                order_id=2,
                symbol="BND",
                action="SELL",
                quantity=20,
                status=OrderStatus.ERROR,
                error_message="Rejected",
            ),
        ]
        result = ExecutionResult(success=False, orders=orders)

        assert result.total_orders == 2
        assert result.filled_orders == 1
        assert result.failed_orders == 1

    def test_summary(self, successful_execution):
        """Test summary produces readable string"""
        summary = successful_execution.summary()
        assert "SUCCESS" in summary
        assert "2/2" in summary


# =============================================================================
# AccountSummary Tests
# =============================================================================

class TestAccountSummary:
    """Tests for AccountSummary dataclass"""

    def test_is_valid_with_liquidation(self):
        """Test account is valid when net_liquidation > 0"""
        summary = AccountSummary(
            account_id="DU123456",
            net_liquidation=100000.0,
        )
        assert summary.is_valid is True

    def test_is_valid_zero_liquidation(self):
        """Test account is invalid when net_liquidation is 0"""
        summary = AccountSummary(
            account_id="DU123456",
            net_liquidation=0.0,
        )
        assert summary.is_valid is False

    def test_is_valid_negative_liquidation(self):
        """Test account is invalid when net_liquidation is negative"""
        summary = AccountSummary(
            account_id="DU123456",
            net_liquidation=-1000.0,
        )
        assert summary.is_valid is False
