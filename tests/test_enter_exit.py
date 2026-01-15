"""
Unit tests for enter_exit.py

Tests the advanced order entry and exit management module.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime

from ibapi.contract import Contract
from ibapi.order import Order

from enter_exit import (
    OrderType,
    AlgoStrategy,
    TimeInForce,
    OrderConfig,
    BracketConfig,
    ScaledOrderConfig,
    AdaptiveConfig,
    EntryExitResult,
    OrderBuilder,
    EnterExit,
)
from models import Position


# =============================================================================
# Helper Functions
# =============================================================================

def make_contract(symbol: str = "SPY") -> Contract:
    """Create a test contract"""
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    return contract


def make_mock_portfolio():
    """Create a mock portfolio for testing"""
    portfolio = MagicMock()
    portfolio.connected = True
    portfolio._next_order_id = 1
    portfolio._orders = {}
    portfolio.placeOrder = MagicMock()
    portfolio.cancelOrder = MagicMock()
    return portfolio


# =============================================================================
# Enum Tests
# =============================================================================

class TestOrderType:
    """Tests for OrderType enum"""

    def test_market_value(self):
        assert OrderType.MARKET.value == "MKT"

    def test_limit_value(self):
        assert OrderType.LIMIT.value == "LMT"

    def test_stop_value(self):
        assert OrderType.STOP.value == "STP"

    def test_stop_limit_value(self):
        assert OrderType.STOP_LIMIT.value == "STP LMT"

    def test_trailing_stop_value(self):
        assert OrderType.TRAILING_STOP.value == "TRAIL"

    def test_trailing_stop_limit_value(self):
        assert OrderType.TRAILING_STOP_LIMIT.value == "TRAIL LIMIT"

    def test_market_on_close_value(self):
        assert OrderType.MARKET_ON_CLOSE.value == "MOC"

    def test_limit_on_close_value(self):
        assert OrderType.LIMIT_ON_CLOSE.value == "LOC"

    def test_midprice_value(self):
        assert OrderType.MIDPRICE.value == "MIDPRICE"


class TestAlgoStrategy:
    """Tests for AlgoStrategy enum"""

    def test_adaptive_value(self):
        assert AlgoStrategy.ADAPTIVE.value == "Adaptive"

    def test_twap_value(self):
        assert AlgoStrategy.TWAP.value == "Twap"

    def test_vwap_value(self):
        assert AlgoStrategy.VWAP.value == "Vwap"

    def test_arrival_price_value(self):
        assert AlgoStrategy.ARRIVAL_PRICE.value == "ArrivalPx"

    def test_dark_ice_value(self):
        assert AlgoStrategy.DARK_ICE.value == "DarkIce"

    def test_percent_volume_value(self):
        assert AlgoStrategy.PERCENT_OF_VOLUME.value == "PctVol"


class TestTimeInForce:
    """Tests for TimeInForce enum"""

    def test_day_value(self):
        assert TimeInForce.DAY.value == "DAY"

    def test_gtc_value(self):
        assert TimeInForce.GTC.value == "GTC"

    def test_ioc_value(self):
        assert TimeInForce.IOC.value == "IOC"

    def test_fok_value(self):
        assert TimeInForce.FOK.value == "FOK"

    def test_gtd_value(self):
        assert TimeInForce.GTD.value == "GTD"

    def test_opg_value(self):
        assert TimeInForce.OPG.value == "OPG"


# =============================================================================
# Config Dataclass Tests
# =============================================================================

class TestOrderConfig:
    """Tests for OrderConfig dataclass"""

    def test_default_values(self):
        config = OrderConfig()
        assert config.order_type == OrderType.MARKET
        assert config.limit_price is None
        assert config.stop_price is None
        assert config.trail_amount is None
        assert config.trail_percent is None
        assert config.time_in_force == TimeInForce.DAY
        assert config.outside_rth is False
        assert config.all_or_none is False
        assert config.hidden is False
        assert config.display_size is None

    def test_custom_values(self):
        config = OrderConfig(
            order_type=OrderType.LIMIT,
            limit_price=450.0,
            time_in_force=TimeInForce.GTC,
            outside_rth=True,
        )
        assert config.order_type == OrderType.LIMIT
        assert config.limit_price == 450.0
        assert config.time_in_force == TimeInForce.GTC
        assert config.outside_rth is True


class TestBracketConfig:
    """Tests for BracketConfig dataclass"""

    def test_default_values(self):
        config = BracketConfig()
        assert config.profit_target_pct is None
        assert config.profit_target_price is None
        assert config.stop_loss_pct is None
        assert config.stop_loss_price is None
        assert config.trailing_stop is False
        assert config.trail_amount is None
        assert config.trail_percent is None

    def test_percentage_config(self):
        config = BracketConfig(profit_target_pct=5.0, stop_loss_pct=2.0)
        assert config.profit_target_pct == 5.0
        assert config.stop_loss_pct == 2.0

    def test_absolute_price_config(self):
        config = BracketConfig(
            profit_target_price=480.0,
            stop_loss_price=420.0,
        )
        assert config.profit_target_price == 480.0
        assert config.stop_loss_price == 420.0

    def test_trailing_stop_config(self):
        config = BracketConfig(
            trailing_stop=True,
            trail_percent=2.0,
        )
        assert config.trailing_stop is True
        assert config.trail_percent == 2.0


class TestScaledOrderConfig:
    """Tests for ScaledOrderConfig dataclass"""

    def test_default_values(self):
        config = ScaledOrderConfig()
        assert config.num_orders == 3
        assert config.price_increment_pct == 0.5
        assert config.quantity_distribution == "equal"
        assert config.start_price is None

    def test_custom_values(self):
        config = ScaledOrderConfig(
            num_orders=5,
            price_increment_pct=1.0,
            quantity_distribution="pyramid",
        )
        assert config.num_orders == 5
        assert config.price_increment_pct == 1.0
        assert config.quantity_distribution == "pyramid"


class TestAdaptiveConfig:
    """Tests for AdaptiveConfig dataclass"""

    def test_default_values(self):
        config = AdaptiveConfig()
        assert config.strategy == AlgoStrategy.ADAPTIVE
        assert config.urgency == "Normal"
        assert config.start_time is None
        assert config.end_time is None
        assert config.max_pct_volume is None

    def test_custom_values(self):
        config = AdaptiveConfig(
            strategy=AlgoStrategy.VWAP,
            urgency="Urgent",
            start_time="09:30:00",
            end_time="16:00:00",
            max_pct_volume=0.05,
        )
        assert config.strategy == AlgoStrategy.VWAP
        assert config.urgency == "Urgent"
        assert config.start_time == "09:30:00"
        assert config.end_time == "16:00:00"
        assert config.max_pct_volume == 0.05


class TestEntryExitResult:
    """Tests for EntryExitResult dataclass"""

    def test_success_result(self):
        result = EntryExitResult(
            success=True,
            order_ids=[1, 2, 3],
            message="Orders placed",
        )
        assert result.success is True
        assert result.order_ids == [1, 2, 3]
        assert result.message == "Orders placed"
        assert result.error is None

    def test_failure_result(self):
        result = EntryExitResult(
            success=False,
            error="Connection failed",
        )
        assert result.success is False
        assert result.error == "Connection failed"
        assert result.order_ids == []

    def test_total_orders_property(self):
        result = EntryExitResult(success=True, order_ids=[1, 2, 3, 4])
        assert result.total_orders == 4

    def test_bracket_order_ids(self):
        result = EntryExitResult(
            success=True,
            order_ids=[1, 2, 3],
            parent_order_id=1,
            profit_order_id=2,
            stop_order_id=3,
            oca_group="OCA_123",
        )
        assert result.parent_order_id == 1
        assert result.profit_order_id == 2
        assert result.stop_order_id == 3
        assert result.oca_group == "OCA_123"


# =============================================================================
# OrderBuilder Tests
# =============================================================================

class TestOrderBuilderCreateBase:
    """Tests for OrderBuilder.create_base_order"""

    def test_creates_order_with_action(self):
        order = OrderBuilder.create_base_order("BUY", 100)
        assert order.action == "BUY"

    def test_creates_order_with_quantity(self):
        order = OrderBuilder.create_base_order("SELL", 50)
        assert order.totalQuantity == 50

    def test_creates_order_with_type(self):
        order = OrderBuilder.create_base_order("BUY", 100, OrderType.LIMIT)
        assert order.orderType == "LMT"

    def test_creates_order_with_tif(self):
        order = OrderBuilder.create_base_order("BUY", 100, tif=TimeInForce.GTC)
        assert order.tif == "GTC"

    def test_order_transmit_default_true(self):
        order = OrderBuilder.create_base_order("BUY", 100)
        assert order.transmit is True


class TestOrderBuilderMarket:
    """Tests for OrderBuilder.market_order"""

    def test_creates_market_order(self):
        order = OrderBuilder.market_order("BUY", 100)
        assert order.orderType == "MKT"
        assert order.action == "BUY"
        assert order.totalQuantity == 100

    def test_sell_market_order(self):
        order = OrderBuilder.market_order("SELL", 50)
        assert order.action == "SELL"
        assert order.totalQuantity == 50


class TestOrderBuilderLimit:
    """Tests for OrderBuilder.limit_order"""

    def test_creates_limit_order(self):
        order = OrderBuilder.limit_order("BUY", 100, 450.0)
        assert order.orderType == "LMT"
        assert order.lmtPrice == 450.0

    def test_limit_order_with_tif(self):
        order = OrderBuilder.limit_order("BUY", 100, 450.0, TimeInForce.GTC)
        assert order.tif == "GTC"


class TestOrderBuilderStop:
    """Tests for OrderBuilder.stop_order"""

    def test_creates_stop_order(self):
        order = OrderBuilder.stop_order("SELL", 100, 440.0)
        assert order.orderType == "STP"
        assert order.auxPrice == 440.0

    def test_stop_order_default_gtc(self):
        order = OrderBuilder.stop_order("SELL", 100, 440.0)
        assert order.tif == "GTC"


class TestOrderBuilderStopLimit:
    """Tests for OrderBuilder.stop_limit_order"""

    def test_creates_stop_limit_order(self):
        order = OrderBuilder.stop_limit_order("SELL", 100, 440.0, 438.0)
        assert order.orderType == "STP LMT"
        assert order.auxPrice == 440.0
        assert order.lmtPrice == 438.0

    def test_stop_limit_order_with_tif(self):
        order = OrderBuilder.stop_limit_order("SELL", 100, 440.0, 438.0, TimeInForce.DAY)
        assert order.tif == "DAY"


class TestOrderBuilderTrailingStop:
    """Tests for OrderBuilder.trailing_stop_order"""

    def test_creates_trailing_stop_with_amount(self):
        order = OrderBuilder.trailing_stop_order("SELL", 100, trail_amount=5.0)
        assert order.orderType == "TRAIL"
        assert order.auxPrice == 5.0

    def test_creates_trailing_stop_with_percent(self):
        order = OrderBuilder.trailing_stop_order("SELL", 100, trail_percent=2.0)
        assert order.orderType == "TRAIL"
        assert order.trailingPercent == 2.0

    def test_trailing_stop_percent_takes_precedence(self):
        order = OrderBuilder.trailing_stop_order("SELL", 100, trail_amount=5.0, trail_percent=2.0)
        assert order.trailingPercent == 2.0


class TestOrderBuilderTrailingStopLimit:
    """Tests for OrderBuilder.trailing_stop_limit_order"""

    def test_creates_trailing_stop_limit(self):
        order = OrderBuilder.trailing_stop_limit_order("SELL", 100, 5.0, 0.5)
        assert order.orderType == "TRAIL LIMIT"
        assert order.auxPrice == 5.0
        assert order.lmtPriceOffset == 0.5


class TestOrderBuilderAdaptive:
    """Tests for OrderBuilder.adaptive_order"""

    def test_creates_adaptive_market(self):
        order = OrderBuilder.adaptive_order("BUY", 100)
        assert order.algoStrategy == "Adaptive"
        assert order.orderType == "MKT"

    def test_creates_adaptive_limit(self):
        order = OrderBuilder.adaptive_order("BUY", 100, OrderType.LIMIT, 450.0)
        assert order.lmtPrice == 450.0

    def test_adaptive_urgency_param(self):
        order = OrderBuilder.adaptive_order("BUY", 100, urgency="Urgent")
        assert ("adaptivePriority", "Urgent") in order.algoParams


class TestOrderBuilderTwap:
    """Tests for OrderBuilder.twap_order"""

    def test_creates_twap_order(self):
        order = OrderBuilder.twap_order("BUY", 100, "09:30:00", "16:00:00")
        assert order.algoStrategy == "Twap"

    def test_twap_time_params(self):
        order = OrderBuilder.twap_order("BUY", 100, "09:30:00", "16:00:00")
        assert ("startTime", "09:30:00") in order.algoParams
        assert ("endTime", "16:00:00") in order.algoParams
        assert ("allowPastEndTime", "1") in order.algoParams

    def test_twap_with_limit_price(self):
        order = OrderBuilder.twap_order("BUY", 100, "09:30:00", "16:00:00", 450.0)
        assert order.orderType == "LMT"
        assert order.lmtPrice == 450.0

    def test_twap_without_limit_is_market(self):
        order = OrderBuilder.twap_order("BUY", 100, "09:30:00", "16:00:00")
        assert order.orderType == "MKT"


class TestOrderBuilderVwap:
    """Tests for OrderBuilder.vwap_order"""

    def test_creates_vwap_order(self):
        order = OrderBuilder.vwap_order("BUY", 100, "09:30:00", "16:00:00")
        assert order.algoStrategy == "Vwap"

    def test_vwap_max_pct_volume(self):
        order = OrderBuilder.vwap_order("BUY", 100, "09:30:00", "16:00:00", 0.05)
        assert ("maxPctVol", "0.05") in order.algoParams

    def test_vwap_with_limit_price(self):
        order = OrderBuilder.vwap_order("BUY", 100, "09:30:00", "16:00:00", limit_price=450.0)
        assert order.lmtPrice == 450.0


class TestOrderBuilderIceberg:
    """Tests for OrderBuilder.iceberg_order"""

    def test_creates_iceberg_order(self):
        order = OrderBuilder.iceberg_order("BUY", 1000, 450.0, 100)
        assert order.orderType == "LMT"
        assert order.lmtPrice == 450.0
        assert order.displaySize == 100


class TestOrderBuilderMidprice:
    """Tests for OrderBuilder.midprice_order"""

    def test_creates_midprice_order(self):
        order = OrderBuilder.midprice_order("BUY", 100)
        assert order.orderType == "MIDPRICE"

    def test_midprice_with_cap(self):
        order = OrderBuilder.midprice_order("BUY", 100, 450.0)
        assert order.lmtPrice == 450.0


# =============================================================================
# EnterExit Tests - Initialization
# =============================================================================

class TestEnterExitInit:
    """Tests for EnterExit initialization"""

    def test_init_stores_portfolio(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        assert ee.portfolio is portfolio

    def test_init_oca_counter(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        assert ee._oca_counter == 0

    def test_init_active_brackets(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        assert ee._active_brackets == {}


class TestEnterExitHelpers:
    """Tests for EnterExit helper methods"""

    def test_generate_oca_group_increments(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        oca1 = ee._generate_oca_group()
        oca2 = ee._generate_oca_group()
        assert "OCA_" in oca1
        assert "OCA_" in oca2
        assert oca1 != oca2

    def test_get_next_order_id_when_connected(self):
        portfolio = make_mock_portfolio()
        portfolio._next_order_id = 42
        ee = EnterExit(portfolio)
        assert ee._get_next_order_id() == 42

    def test_get_next_order_id_when_disconnected(self):
        portfolio = make_mock_portfolio()
        portfolio.connected = False
        ee = EnterExit(portfolio)
        assert ee._get_next_order_id() is None


# =============================================================================
# EnterExit Tests - Basic Entry/Exit
# =============================================================================

class TestEnterExitEnter:
    """Tests for EnterExit.enter method"""

    def test_enter_market_order_success(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter(contract, 100)

        assert result.success is True
        assert len(result.order_ids) == 1
        assert "SPY" in result.message
        portfolio.placeOrder.assert_called_once()

    def test_enter_limit_order_success(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        config = OrderConfig(order_type=OrderType.LIMIT, limit_price=450.0)

        result = ee.enter(contract, 100, config=config)

        assert result.success is True
        portfolio.placeOrder.assert_called_once()

    def test_enter_limit_order_without_price_fails(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        config = OrderConfig(order_type=OrderType.LIMIT)

        result = ee.enter(contract, 100, config=config)

        assert result.success is False
        assert "Limit price required" in result.error

    def test_enter_stop_order_success(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        config = OrderConfig(order_type=OrderType.STOP, stop_price=440.0)

        result = ee.enter(contract, 100, config=config)

        assert result.success is True

    def test_enter_stop_order_without_price_fails(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        config = OrderConfig(order_type=OrderType.STOP)

        result = ee.enter(contract, 100, config=config)

        assert result.success is False
        assert "Stop price required" in result.error

    def test_enter_stop_limit_order_success(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        config = OrderConfig(
            order_type=OrderType.STOP_LIMIT,
            stop_price=440.0,
            limit_price=438.0,
        )

        result = ee.enter(contract, 100, config=config)

        assert result.success is True

    def test_enter_stop_limit_without_prices_fails(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        config = OrderConfig(order_type=OrderType.STOP_LIMIT)

        result = ee.enter(contract, 100, config=config)

        assert result.success is False
        assert "prices required" in result.error

    def test_enter_trailing_stop_order(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        config = OrderConfig(
            order_type=OrderType.TRAILING_STOP,
            trail_percent=2.0,
        )

        result = ee.enter(contract, 100, config=config)

        assert result.success is True

    def test_enter_midprice_order(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        config = OrderConfig(order_type=OrderType.MIDPRICE)

        result = ee.enter(contract, 100, config=config)

        assert result.success is True

    def test_enter_sell_action(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter(contract, 100, action="SELL")

        assert result.success is True
        assert "SELL" in result.message

    def test_enter_when_disconnected_fails(self):
        portfolio = make_mock_portfolio()
        portfolio.connected = False
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter(contract, 100)

        assert result.success is False
        assert "Failed to place order" in result.error

    def test_enter_increments_order_id(self):
        portfolio = make_mock_portfolio()
        portfolio._next_order_id = 5
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        ee.enter(contract, 100)

        assert portfolio._next_order_id == 6

    def test_enter_tracks_order(self):
        portfolio = make_mock_portfolio()
        portfolio._next_order_id = 10
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        ee.enter(contract, 100)

        assert 10 in portfolio._orders


class TestEnterExitExit:
    """Tests for EnterExit.exit method"""

    def test_exit_calls_enter_with_sell(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.exit(contract, 100)

        assert result.success is True
        # Verify it was a SELL
        call_args = portfolio.placeOrder.call_args
        order = call_args[0][2]  # Third argument is the order
        assert order.action == "SELL"


# =============================================================================
# EnterExit Tests - Bracket Orders
# =============================================================================

class TestEnterExitBracket:
    """Tests for EnterExit.enter_bracket method"""

    def test_enter_bracket_success(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        bracket_config = BracketConfig(
            profit_target_pct=5.0,
            stop_loss_pct=2.0,
        )

        result = ee.enter_bracket(
            contract,
            quantity=100,
            entry_price=450.0,
            bracket_config=bracket_config,
        )

        assert result.success is True
        assert len(result.order_ids) == 3  # Entry + TP + SL
        assert result.parent_order_id is not None
        assert result.profit_order_id is not None
        assert result.stop_order_id is not None
        assert result.oca_group is not None

    def test_enter_bracket_tracks_active_bracket(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        bracket_config = BracketConfig(
            profit_target_pct=5.0,
            stop_loss_pct=2.0,
        )

        result = ee.enter_bracket(
            contract,
            quantity=100,
            entry_price=450.0,
            bracket_config=bracket_config,
        )

        assert result.oca_group in ee._active_brackets
        bracket = ee._active_brackets[result.oca_group]
        assert bracket["symbol"] == "SPY"
        assert bracket["quantity"] == 100

    def test_enter_bracket_limit_needs_price(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        bracket_config = BracketConfig(profit_target_pct=5.0)

        result = ee.enter_bracket(
            contract,
            quantity=100,
            entry_price=None,
            entry_type=OrderType.LIMIT,
            bracket_config=bracket_config,
        )

        assert result.success is False
        assert "Entry price required" in result.error

    def test_enter_bracket_uses_position_price(self):
        portfolio = make_mock_portfolio()
        position = MagicMock()
        position.current_price = 450.0
        portfolio.get_position = MagicMock(return_value=position)
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        bracket_config = BracketConfig(
            profit_target_pct=5.0,
            stop_loss_pct=2.0,
        )

        result = ee.enter_bracket(
            contract,
            quantity=100,
            entry_price=None,
            entry_type=OrderType.MARKET,
            bracket_config=bracket_config,
        )

        assert result.success is True

    def test_enter_bracket_fails_without_price_or_position(self):
        portfolio = make_mock_portfolio()
        portfolio.get_position = MagicMock(return_value=None)
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        bracket_config = BracketConfig(profit_target_pct=5.0)

        result = ee.enter_bracket(
            contract,
            quantity=100,
            entry_price=None,
            entry_type=OrderType.MARKET,
            bracket_config=bracket_config,
        )

        assert result.success is False
        assert "Cannot determine entry price" in result.error

    def test_enter_bracket_absolute_prices(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        bracket_config = BracketConfig(
            profit_target_price=480.0,
            stop_loss_price=420.0,
        )

        result = ee.enter_bracket(
            contract,
            quantity=100,
            entry_price=450.0,
            bracket_config=bracket_config,
        )

        assert result.success is True
        assert len(result.order_ids) == 3

    def test_enter_bracket_profit_only(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        bracket_config = BracketConfig(profit_target_pct=5.0)

        result = ee.enter_bracket(
            contract,
            quantity=100,
            entry_price=450.0,
            bracket_config=bracket_config,
        )

        assert result.success is True
        assert len(result.order_ids) == 2  # Entry + TP
        assert result.stop_order_id is None

    def test_enter_bracket_stop_only(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        bracket_config = BracketConfig(stop_loss_pct=2.0)

        result = ee.enter_bracket(
            contract,
            quantity=100,
            entry_price=450.0,
            bracket_config=bracket_config,
        )

        assert result.success is True
        assert len(result.order_ids) == 2  # Entry + SL
        assert result.profit_order_id is None

    def test_enter_bracket_needs_tp_or_sl(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        bracket_config = BracketConfig()  # No TP or SL

        result = ee.enter_bracket(
            contract,
            quantity=100,
            entry_price=450.0,
            bracket_config=bracket_config,
        )

        assert result.success is False
        assert "profit target or stop loss required" in result.error

    def test_enter_bracket_trailing_stop(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        bracket_config = BracketConfig(
            profit_target_pct=5.0,
            stop_loss_pct=2.0,
            trailing_stop=True,
        )

        result = ee.enter_bracket(
            contract,
            quantity=100,
            entry_price=450.0,
            bracket_config=bracket_config,
        )

        assert result.success is True

    def test_enter_bracket_disconnected_fails(self):
        portfolio = make_mock_portfolio()
        portfolio.connected = False
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        bracket_config = BracketConfig(profit_target_pct=5.0)

        result = ee.enter_bracket(
            contract,
            quantity=100,
            entry_price=450.0,
            bracket_config=bracket_config,
        )

        assert result.success is False


# =============================================================================
# EnterExit Tests - Scaled Orders
# =============================================================================

class TestEnterExitScaled:
    """Tests for EnterExit.enter_scaled method"""

    def test_enter_scaled_success(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        config = ScaledOrderConfig(num_orders=3)

        result = ee.enter_scaled(contract, 300, config, 450.0)

        assert result.success is True
        assert len(result.order_ids) == 3
        assert portfolio.placeOrder.call_count == 3

    def test_enter_scaled_uses_position_price(self):
        portfolio = make_mock_portfolio()
        position = MagicMock()
        position.current_price = 450.0
        portfolio.get_position = MagicMock(return_value=position)
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")
        config = ScaledOrderConfig(num_orders=3)

        result = ee.enter_scaled(contract, 300, config, base_price=None)

        assert result.success is True

    def test_enter_scaled_fails_without_price(self):
        portfolio = make_mock_portfolio()
        portfolio.get_position = MagicMock(return_value=None)
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_scaled(contract, 300, base_price=None)

        assert result.success is False
        assert "Cannot determine base price" in result.error

    def test_enter_scaled_equal_distribution(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        quantities = ee._calculate_scaled_quantities(300, 3, "equal")
        assert sum(quantities) == 300
        assert quantities == [100, 100, 100]

    def test_enter_scaled_equal_with_remainder(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        quantities = ee._calculate_scaled_quantities(100, 3, "equal")
        assert sum(quantities) == 100
        assert quantities == [34, 33, 33]

    def test_enter_scaled_pyramid_distribution(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        quantities = ee._calculate_scaled_quantities(300, 3, "pyramid")
        assert sum(quantities) == 300
        # Pyramid: ascending weights (1, 2, 3) = (50, 100, 150)
        assert quantities[0] < quantities[1] < quantities[2]

    def test_enter_scaled_inverse_pyramid_distribution(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        quantities = ee._calculate_scaled_quantities(300, 3, "inverse_pyramid")
        assert sum(quantities) == 300
        # Inverse pyramid: descending weights (3, 2, 1) = (150, 100, 50)
        assert quantities[0] > quantities[1] > quantities[2]

    def test_enter_scaled_price_calculation(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        prices = ee._calculate_scaled_prices(100.0, 3, 1.0)
        assert prices == [100.0, 99.0, 98.0]

    def test_enter_scaled_price_increment(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        prices = ee._calculate_scaled_prices(450.0, 3, 0.5)
        assert prices[0] == 450.0
        assert prices[1] == 447.75
        assert prices[2] == 445.50


# =============================================================================
# EnterExit Tests - Adaptive/Algo Orders
# =============================================================================

class TestEnterExitAdaptive:
    """Tests for EnterExit.enter_adaptive method"""

    def test_enter_adaptive_success(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_adaptive(contract, 100)

        assert result.success is True
        assert "Adaptive" in result.message

    def test_enter_adaptive_with_limit(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_adaptive(contract, 100, limit_price=450.0)

        assert result.success is True

    def test_enter_adaptive_urgency(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_adaptive(contract, 100, urgency="Urgent")

        assert result.success is True
        assert "Urgent" in result.message

    def test_enter_adaptive_disconnected_fails(self):
        portfolio = make_mock_portfolio()
        portfolio.connected = False
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_adaptive(contract, 100)

        assert result.success is False


class TestEnterExitTwap:
    """Tests for EnterExit.enter_twap method"""

    def test_enter_twap_success(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_twap(contract, 1000, "09:30:00", "16:00:00")

        assert result.success is True
        assert "TWAP" in result.message
        assert "09:30:00" in result.message
        assert "16:00:00" in result.message

    def test_enter_twap_with_limit(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_twap(contract, 1000, "09:30:00", "16:00:00", limit_price=450.0)

        assert result.success is True

    def test_enter_twap_disconnected_fails(self):
        portfolio = make_mock_portfolio()
        portfolio.connected = False
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_twap(contract, 1000, "09:30:00", "16:00:00")

        assert result.success is False


class TestEnterExitVwap:
    """Tests for EnterExit.enter_vwap method"""

    def test_enter_vwap_success(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_vwap(contract, 1000, "09:30:00", "16:00:00")

        assert result.success is True
        assert "VWAP" in result.message

    def test_enter_vwap_max_volume(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_vwap(contract, 1000, "09:30:00", "16:00:00", max_pct_volume=0.05)

        assert result.success is True
        assert "5.0%" in result.message

    def test_enter_vwap_with_limit(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_vwap(contract, 1000, "09:30:00", "16:00:00", limit_price=450.0)

        assert result.success is True

    def test_enter_vwap_disconnected_fails(self):
        portfolio = make_mock_portfolio()
        portfolio.connected = False
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_vwap(contract, 1000, "09:30:00", "16:00:00")

        assert result.success is False


# =============================================================================
# EnterExit Tests - Probability-Based Entry
# =============================================================================

class TestEnterExitProbabilityBased:
    """Tests for EnterExit.enter_probability_based method"""

    def test_probability_based_success(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_probability_based(
            contract,
            target_quantity=100,
            probability=0.6,
            current_price=450.0,
        )

        assert result.success is True
        assert "prob=60%" in result.message

    def test_probability_scales_quantity(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_probability_based(
            contract,
            target_quantity=100,
            probability=0.5,
            current_price=450.0,
            expected_move_pct=10.0,
            risk_reward_ratio=2.0,
        )

        assert result.success is True
        # Quantity should be scaled by probability and Kelly

    def test_probability_invalid_range_fails(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_probability_based(
            contract,
            target_quantity=100,
            probability=0.0,  # Invalid
            current_price=450.0,
        )

        assert result.success is False
        assert "Probability must be between 0 and 1" in result.error

    def test_probability_over_one_fails(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_probability_based(
            contract,
            target_quantity=100,
            probability=1.5,
            current_price=450.0,
        )

        assert result.success is False

    def test_probability_very_low_quantity_fails(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_probability_based(
            contract,
            target_quantity=1,
            probability=0.1,  # Very low - will result in 0 shares
            current_price=450.0,
        )

        assert result.success is False
        assert "too small" in result.error

    def test_probability_uses_trailing_stop_for_high_prob(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_probability_based(
            contract,
            target_quantity=100,
            probability=0.7,  # > 0.6 uses trailing stop
            current_price=450.0,
        )

        assert result.success is True

    def test_probability_kelly_criterion_caps_at_25pct(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        # Even with high probability, Kelly caps at 25%
        result = ee.enter_probability_based(
            contract,
            target_quantity=1000,
            probability=0.9,
            current_price=450.0,
        )

        assert result.success is True


# =============================================================================
# EnterExit Tests - Exit Management
# =============================================================================

class TestEnterExitTrailingStop:
    """Tests for EnterExit.exit_with_trailing_stop method"""

    def test_exit_trailing_stop_percent_success(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.exit_with_trailing_stop(contract, 100, trail_percent=2.0)

        assert result.success is True
        assert "2.0%" in result.message

    def test_exit_trailing_stop_amount_success(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.exit_with_trailing_stop(contract, 100, trail_amount=5.0)

        assert result.success is True
        assert "$5.0" in result.message

    def test_exit_trailing_stop_needs_trail_value(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.exit_with_trailing_stop(contract, 100)

        assert result.success is False
        assert "trail_percent or trail_amount required" in result.error

    def test_exit_trailing_stop_disconnected_fails(self):
        portfolio = make_mock_portfolio()
        portfolio.connected = False
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.exit_with_trailing_stop(contract, 100, trail_percent=2.0)

        assert result.success is False


class TestEnterExitCancelBracket:
    """Tests for EnterExit.cancel_bracket method"""

    def test_cancel_bracket_success(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        # First create a bracket
        bracket_config = BracketConfig(profit_target_pct=5.0, stop_loss_pct=2.0)
        result = ee.enter_bracket(contract, 100, 450.0, bracket_config=bracket_config)
        oca_group = result.oca_group

        # Now cancel it
        success = ee.cancel_bracket(oca_group)

        assert success is True
        assert oca_group not in ee._active_brackets
        assert portfolio.cancelOrder.call_count == 3

    def test_cancel_bracket_unknown_group_fails(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)

        success = ee.cancel_bracket("UNKNOWN_OCA")

        assert success is False

    def test_cancel_bracket_removes_from_tracking(self):
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        bracket_config = BracketConfig(profit_target_pct=5.0)
        result = ee.enter_bracket(contract, 100, 450.0, bracket_config=bracket_config)
        oca_group = result.oca_group

        assert oca_group in ee._active_brackets

        ee.cancel_bracket(oca_group)

        assert oca_group not in ee._active_brackets


# =============================================================================
# Integration-Style Tests
# =============================================================================

class TestEnterExitIntegration:
    """Integration-style tests for EnterExit"""

    def test_full_bracket_flow(self):
        """Test complete bracket order workflow"""
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        # Enter bracket
        bracket_config = BracketConfig(
            profit_target_pct=5.0,
            stop_loss_pct=2.0,
        )
        result = ee.enter_bracket(
            contract,
            quantity=100,
            entry_price=450.0,
            bracket_config=bracket_config,
        )

        assert result.success
        assert result.total_orders == 3

        # Verify bracket is tracked
        assert result.oca_group in ee._active_brackets

        # Cancel bracket
        success = ee.cancel_bracket(result.oca_group)
        assert success
        assert result.oca_group not in ee._active_brackets

    def test_full_scaled_flow(self):
        """Test complete scaled order workflow"""
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        config = ScaledOrderConfig(
            num_orders=5,
            price_increment_pct=0.5,
            quantity_distribution="pyramid",
        )

        result = ee.enter_scaled(contract, 500, config, 450.0)

        assert result.success
        assert result.total_orders == 5
        assert portfolio.placeOrder.call_count == 5

    def test_probability_creates_bracket(self):
        """Test probability-based entry creates bracket order"""
        portfolio = make_mock_portfolio()
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result = ee.enter_probability_based(
            contract,
            target_quantity=100,
            probability=0.7,
            current_price=450.0,
            expected_move_pct=5.0,
            risk_reward_ratio=2.0,
        )

        assert result.success
        # Should have entry + profit + stop orders
        assert result.total_orders >= 2
        assert result.oca_group is not None

    def test_multiple_entries_different_order_ids(self):
        """Test multiple entries get unique order IDs"""
        portfolio = make_mock_portfolio()
        portfolio._next_order_id = 1
        ee = EnterExit(portfolio)
        contract = make_contract("SPY")

        result1 = ee.enter(contract, 100)
        result2 = ee.enter(contract, 100)
        result3 = ee.enter(contract, 100)

        assert result1.order_ids[0] != result2.order_ids[0]
        assert result2.order_ids[0] != result3.order_ids[0]
