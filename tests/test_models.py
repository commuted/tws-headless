"""
Unit tests for models.py

Tests data classes: Position, Bar, TargetAllocation, RebalanceTrade,
RebalanceResult, OrderRecord, ExecutionResult, and enums.
"""

import pytest
from unittest.mock import MagicMock
from decimal import Decimal
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
    OptionExerciseType,
)
from const import (
    UNSET_INTEGER,
    UNSET_DOUBLE,
    UNSET_DECIMAL,
    NO_VALID_ID,
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


# =============================================================================
# IB API Compatible Classes Tests
# =============================================================================


class TestConst:
    """Tests for IB API constants"""

    def test_unset_integer(self):
        """Test UNSET_INTEGER value"""
        assert UNSET_INTEGER == 2**31 - 1

    def test_unset_double(self):
        """Test UNSET_DOUBLE is max float"""
        import sys
        assert UNSET_DOUBLE == float(sys.float_info.max)

    def test_unset_decimal(self):
        """Test UNSET_DECIMAL value"""
        assert UNSET_DECIMAL == Decimal(2**127 - 1)

    def test_no_valid_id(self):
        """Test NO_VALID_ID value"""
        assert NO_VALID_ID == -1


class TestExecution:
    """Tests for Execution dataclass"""

    def test_execution_creation_defaults(self):
        """Test execution created with defaults"""
        exec_ = Execution()
        assert exec_.execId == ""
        assert exec_.time == ""
        assert exec_.side == ""
        assert exec_.price == 0.0
        assert exec_.shares == UNSET_DECIMAL

    def test_execution_creation_with_values(self):
        """Test execution created with values"""
        exec_ = Execution(
            execId="0001",
            time="20240115 10:30:00",
            acctNumber="DU123456",
            exchange="SMART",
            side="BOT",
            shares=Decimal(100),
            price=175.50,
            permId=12345,
            clientId=1,
            orderId=1001,
            cumQty=Decimal(100),
            avgPrice=175.50,
        )
        assert exec_.execId == "0001"
        assert exec_.side == "BOT"
        assert exec_.shares == Decimal(100)
        assert exec_.price == 175.50
        assert exec_.avgPrice == 175.50

    def test_execution_str(self):
        """Test execution string representation"""
        exec_ = Execution(
            execId="0001",
            time="20240115 10:30:00",
            acctNumber="DU123456",
            exchange="SMART",
            side="BOT",
            shares=Decimal(100),
            price=175.50,
        )
        s = str(exec_)
        assert "ExecId: 0001" in s
        assert "Side: BOT" in s
        assert "100" in s


class TestExecutionFilter:
    """Tests for ExecutionFilter dataclass"""

    def test_execution_filter_defaults(self):
        """Test execution filter with defaults"""
        filter_ = ExecutionFilter()
        assert filter_.clientId == 0
        assert filter_.acctCode == ""
        assert filter_.symbol == ""
        assert filter_.lastNDays == UNSET_INTEGER

    def test_execution_filter_with_values(self):
        """Test execution filter with values"""
        filter_ = ExecutionFilter(
            clientId=1,
            acctCode="DU123456",
            symbol="AAPL",
            secType="STK",
            side="BOT",
        )
        assert filter_.clientId == 1
        assert filter_.symbol == "AAPL"
        assert filter_.secType == "STK"


class TestOrderAllocation:
    """Tests for OrderAllocation dataclass"""

    def test_order_allocation_defaults(self):
        """Test order allocation with defaults"""
        alloc = OrderAllocation()
        assert alloc.account == ""
        assert alloc.position == UNSET_DECIMAL
        assert alloc.isMonetary is False

    def test_order_allocation_with_values(self):
        """Test order allocation with values"""
        alloc = OrderAllocation(
            account="DU123456",
            position=Decimal(100),
            positionDesired=Decimal(150),
            positionAfter=Decimal(150),
        )
        assert alloc.account == "DU123456"
        assert alloc.position == Decimal(100)

    def test_order_allocation_str(self):
        """Test order allocation string representation"""
        alloc = OrderAllocation(account="DU123456", position=Decimal(100))
        s = str(alloc)
        assert "DU123456" in s


class TestOrderState:
    """Tests for OrderState dataclass"""

    def test_order_state_defaults(self):
        """Test order state with defaults"""
        state = OrderState()
        assert state.status == ""
        assert state.initMarginBefore == ""
        assert state.commissionAndFees == UNSET_DOUBLE

    def test_order_state_with_values(self):
        """Test order state with values"""
        state = OrderState(
            status="Filled",
            initMarginBefore="10000.00",
            maintMarginBefore="5000.00",
            commissionAndFees=1.50,
            commissionAndFeesCurrency="USD",
        )
        assert state.status == "Filled"
        assert state.initMarginBefore == "10000.00"
        assert state.commissionAndFees == 1.50

    def test_order_state_str(self):
        """Test order state string representation"""
        state = OrderState(status="Filled", commissionAndFees=1.50)
        s = str(state)
        assert "Filled" in s


class TestCommissionAndFeesReport:
    """Tests for CommissionAndFeesReport dataclass"""

    def test_report_defaults(self):
        """Test report with defaults"""
        report = CommissionAndFeesReport()
        assert report.execId == ""
        assert report.commissionAndFees == 0.0
        assert report.realizedPNL == 0.0

    def test_report_with_values(self):
        """Test report with values"""
        report = CommissionAndFeesReport(
            execId="0001",
            commissionAndFees=1.50,
            currency="USD",
            realizedPNL=500.0,
        )
        assert report.execId == "0001"
        assert report.commissionAndFees == 1.50
        assert report.realizedPNL == 500.0

    def test_report_str(self):
        """Test report string representation"""
        report = CommissionAndFeesReport(execId="0001", commissionAndFees=1.50)
        s = str(report)
        assert "0001" in s
        assert "1.5" in s


class TestTickAttrib:
    """Tests for TickAttrib dataclass"""

    def test_tick_attrib_defaults(self):
        """Test tick attrib with defaults"""
        attrib = TickAttrib()
        assert attrib.canAutoExecute is False
        assert attrib.pastLimit is False
        assert attrib.preOpen is False

    def test_tick_attrib_with_values(self):
        """Test tick attrib with values"""
        attrib = TickAttrib(canAutoExecute=True, preOpen=True)
        assert attrib.canAutoExecute is True
        assert attrib.preOpen is True

    def test_tick_attrib_str(self):
        """Test tick attrib string representation"""
        attrib = TickAttrib(canAutoExecute=True)
        s = str(attrib)
        assert "CanAutoExecute: 1" in s


class TestTickAttribBidAsk:
    """Tests for TickAttribBidAsk dataclass"""

    def test_tick_attrib_bid_ask_defaults(self):
        """Test bid/ask tick attrib with defaults"""
        attrib = TickAttribBidAsk()
        assert attrib.bidPastLow is False
        assert attrib.askPastHigh is False

    def test_tick_attrib_bid_ask_str(self):
        """Test bid/ask tick attrib string representation"""
        attrib = TickAttribBidAsk(bidPastLow=True)
        s = str(attrib)
        assert "BidPastLow: 1" in s


class TestTickAttribLast:
    """Tests for TickAttribLast dataclass"""

    def test_tick_attrib_last_defaults(self):
        """Test last tick attrib with defaults"""
        attrib = TickAttribLast()
        assert attrib.pastLimit is False
        assert attrib.unreported is False

    def test_tick_attrib_last_str(self):
        """Test last tick attrib string representation"""
        attrib = TickAttribLast(unreported=True)
        s = str(attrib)
        assert "Unreported: 1" in s


class TestHistoricalTick:
    """Tests for HistoricalTick dataclass"""

    def test_historical_tick_defaults(self):
        """Test historical tick with defaults"""
        tick = HistoricalTick()
        assert tick.time == 0
        assert tick.price == 0.0
        assert tick.size == UNSET_DECIMAL

    def test_historical_tick_with_values(self):
        """Test historical tick with values"""
        tick = HistoricalTick(time=1705315800, price=175.50, size=Decimal(100))
        assert tick.time == 1705315800
        assert tick.price == 175.50
        assert tick.size == Decimal(100)


class TestHistoricalTickBidAsk:
    """Tests for HistoricalTickBidAsk dataclass"""

    def test_historical_tick_bid_ask_defaults(self):
        """Test bid/ask historical tick with defaults"""
        tick = HistoricalTickBidAsk()
        assert tick.time == 0
        assert tick.priceBid == 0.0
        assert tick.priceAsk == 0.0

    def test_historical_tick_bid_ask_with_values(self):
        """Test bid/ask historical tick with values"""
        tick = HistoricalTickBidAsk(
            time=1705315800,
            priceBid=175.40,
            priceAsk=175.50,
            sizeBid=Decimal(100),
            sizeAsk=Decimal(200),
        )
        assert tick.priceBid == 175.40
        assert tick.priceAsk == 175.50


class TestHistoricalTickLast:
    """Tests for HistoricalTickLast dataclass"""

    def test_historical_tick_last_defaults(self):
        """Test last historical tick with defaults"""
        tick = HistoricalTickLast()
        assert tick.time == 0
        assert tick.price == 0.0
        assert tick.exchange == ""

    def test_historical_tick_last_with_values(self):
        """Test last historical tick with values"""
        tick = HistoricalTickLast(
            time=1705315800,
            price=175.50,
            size=Decimal(100),
            exchange="NASDAQ",
        )
        assert tick.price == 175.50
        assert tick.exchange == "NASDAQ"


class TestBarData:
    """Tests for BarData dataclass"""

    def test_bar_data_defaults(self):
        """Test bar data with defaults"""
        bar = BarData()
        assert bar.date == ""
        assert bar.open == 0.0
        assert bar.volume == UNSET_DECIMAL

    def test_bar_data_with_values(self):
        """Test bar data with values"""
        bar = BarData(
            date="20240115",
            open=175.0,
            high=180.0,
            low=174.0,
            close=178.0,
            volume=Decimal(1000000),
        )
        assert bar.date == "20240115"
        assert bar.open == 175.0
        assert bar.close == 178.0

    def test_bar_data_to_bar(self):
        """Test converting BarData to Bar"""
        bar_data = BarData(
            date="20240115 10:00:00",
            open=175.0,
            high=180.0,
            low=174.0,
            close=178.0,
            volume=Decimal(1000000),
            wap=Decimal("177.50"),
            barCount=5000,
        )
        bar = bar_data.to_bar("AAPL")
        assert bar.symbol == "AAPL"
        assert bar.timestamp == "20240115 10:00:00"
        assert bar.open == 175.0
        assert bar.close == 178.0
        assert bar.volume == 1000000


class TestRealTimeBar:
    """Tests for RealTimeBar dataclass"""

    def test_real_time_bar_defaults(self):
        """Test real-time bar with defaults"""
        bar = RealTimeBar()
        assert bar.time == 0
        assert bar.open_ == 0.0
        assert bar.volume == UNSET_DECIMAL

    def test_real_time_bar_with_values(self):
        """Test real-time bar with values"""
        bar = RealTimeBar(
            time=1705315800,
            open_=175.0,
            high=175.5,
            low=174.8,
            close=175.2,
            volume=Decimal(10000),
            count=500,
        )
        assert bar.time == 1705315800
        assert bar.open_ == 175.0
        assert bar.close == 175.2


class TestHistogramData:
    """Tests for HistogramData dataclass"""

    def test_histogram_data_defaults(self):
        """Test histogram data with defaults"""
        data = HistogramData()
        assert data.price == 0.0
        assert data.size == UNSET_DECIMAL

    def test_histogram_data_with_values(self):
        """Test histogram data with values"""
        data = HistogramData(price=175.50, size=Decimal(50000))
        assert data.price == 175.50
        assert data.size == Decimal(50000)


class TestOptionExerciseType:
    """Tests for OptionExerciseType enum"""

    def test_option_exercise_types(self):
        """Test option exercise type values"""
        assert OptionExerciseType.NoneItem.value == (-1, "None")
        assert OptionExerciseType.Exercise.value == (1, "Exercise")
        assert OptionExerciseType.Lapse.value == (2, "Lapse")
        assert OptionExerciseType.Assigned.value == (100, "Assigned")
        assert OptionExerciseType.Expired.value == (102, "Expired")
