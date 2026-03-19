"""
Unit tests for contract_builder.py, order_builder.py, and algo_params.py

Tests the Testbed-style builder classes for contracts, orders, and algo parameters.
"""

import pytest
from decimal import Decimal
from unittest.mock import MagicMock

from ibapi.contract import Contract, ComboLeg
from ibapi.order import Order
from ibapi.tag_value import TagValue

from contract_builder import ContractBuilder
from order_builder import OrderFactory
from algo_params import (
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


# =============================================================================
# ContractBuilder Tests
# =============================================================================

class TestContractBuilderStock:
    """Tests for stock/equity contract creation"""

    def test_stock_basic(self):
        """Test basic stock contract"""
        contract = ContractBuilder.stock("AAPL")
        assert contract.symbol == "AAPL"
        assert contract.secType == "STK"
        assert contract.exchange == "SMART"
        assert contract.currency == "USD"

    def test_stock_with_exchange(self):
        """Test stock contract with specific exchange"""
        contract = ContractBuilder.stock("AAPL", exchange="NASDAQ")
        assert contract.symbol == "AAPL"
        assert contract.exchange == "NASDAQ"

    def test_stock_with_primary_exchange(self):
        """Test stock contract with primary exchange"""
        contract = ContractBuilder.stock("AAPL", primary_exchange="NASDAQ")
        assert contract.primaryExchange == "NASDAQ"

    def test_us_stock(self):
        """Test US stock convenience method"""
        contract = ContractBuilder.us_stock("SPY")
        assert contract.symbol == "SPY"
        assert contract.currency == "USD"

    def test_european_stock(self):
        """Test European stock contract"""
        contract = ContractBuilder.european_stock("BMW", "EUR")
        assert contract.symbol == "BMW"
        assert contract.currency == "EUR"
        assert contract.exchange == "SMART"

    def test_etf(self):
        """Test ETF contract (same as stock)"""
        contract = ContractBuilder.etf("QQQ")
        assert contract.symbol == "QQQ"
        assert contract.secType == "STK"


class TestContractBuilderOption:
    """Tests for option contract creation"""

    def test_option_call(self):
        """Test call option contract"""
        contract = ContractBuilder.option("AAPL", "20251219", 150.0, "C")
        assert contract.symbol == "AAPL"
        assert contract.secType == "OPT"
        assert contract.lastTradeDateOrContractMonth == "20251219"
        assert contract.strike == 150.0
        assert contract.right == "C"
        assert contract.multiplier == "100"

    def test_option_put(self):
        """Test put option contract"""
        contract = ContractBuilder.option("SPY", "20251220", 450.0, "P")
        assert contract.right == "P"
        assert contract.strike == 450.0

    def test_option_by_local_symbol(self):
        """Test option by local symbol"""
        contract = ContractBuilder.option_by_local_symbol("AAPL  251219C00150000")
        assert contract.localSymbol == "AAPL  251219C00150000"
        assert contract.secType == "OPT"
        assert contract.exchange == "SMART"
        assert contract.currency == "USD"

    def test_option_chain_query(self):
        """Test option chain query contract"""
        contract = ContractBuilder.option_chain_query("AAPL")
        assert contract.symbol == "AAPL"
        assert contract.secType == "OPT"


class TestContractBuilderFuture:
    """Tests for futures contract creation"""

    def test_future_basic(self):
        """Test basic futures contract"""
        contract = ContractBuilder.future("ES", "202512", "CME")
        assert contract.symbol == "ES"
        assert contract.secType == "FUT"
        assert contract.lastTradeDateOrContractMonth == "202512"
        assert contract.exchange == "CME"

    def test_future_with_multiplier(self):
        """Test futures contract with multiplier"""
        contract = ContractBuilder.future("ES", "202512", "CME", multiplier="50")
        assert contract.multiplier == "50"

    def test_future_by_local_symbol(self):
        """Test future by local symbol"""
        contract = ContractBuilder.future_by_local_symbol("ESZ5", "CME")
        assert contract.localSymbol == "ESZ5"
        assert contract.exchange == "CME"

    def test_continuous_future(self):
        """Test continuous future contract"""
        contract = ContractBuilder.continuous_future("ES", "CME")
        assert contract.secType == "CONTFUT"
        assert contract.symbol == "ES"


class TestContractBuilderForex:
    """Tests for forex contract creation"""

    def test_forex_eurusd(self):
        """Test EUR/USD forex contract"""
        contract = ContractBuilder.forex("EUR", "USD")
        assert contract.symbol == "EUR"
        assert contract.secType == "CASH"
        assert contract.currency == "USD"
        assert contract.exchange == "IDEALPRO"

    def test_forex_gbpusd(self):
        """Test GBP/USD forex contract"""
        contract = ContractBuilder.forex("GBP", "USD")
        assert contract.symbol == "GBP"
        assert contract.currency == "USD"


class TestContractBuilderIndex:
    """Tests for index contract creation"""

    def test_index_spx(self):
        """Test SPX index contract"""
        contract = ContractBuilder.index("SPX", "CBOE")
        assert contract.symbol == "SPX"
        assert contract.secType == "IND"
        assert contract.exchange == "CBOE"
        assert contract.currency == "USD"


class TestContractBuilderBond:
    """Tests for bond contract creation"""

    def test_bond_by_cusip(self):
        """Test bond by CUSIP"""
        contract = ContractBuilder.bond_by_cusip("912828C57")
        assert contract.secIdType == "CUSIP"
        assert contract.secId == "912828C57"
        assert contract.secType == "BOND"
        assert contract.exchange == "SMART"
        assert contract.currency == "USD"

    def test_bond_by_conid(self):
        """Test bond by contract ID"""
        contract = ContractBuilder.bond_by_conid(123456)
        assert contract.conId == 123456
        assert contract.secType == "BOND"
        assert contract.exchange == "SMART"


class TestContractBuilderOther:
    """Tests for other contract types"""

    def test_cfd(self):
        """Test CFD contract"""
        contract = ContractBuilder.cfd("IBDE30")
        assert contract.symbol == "IBDE30"
        assert contract.secType == "CFD"

    def test_commodity(self):
        """Test commodity contract"""
        contract = ContractBuilder.commodity("XAUUSD")
        assert contract.symbol == "XAUUSD"
        assert contract.secType == "CMDTY"

    def test_crypto(self):
        """Test crypto contract"""
        contract = ContractBuilder.crypto("BTC")
        assert contract.symbol == "BTC"
        assert contract.secType == "CRYPTO"
        assert contract.currency == "USD"

    def test_mutual_fund(self):
        """Test mutual fund contract"""
        contract = ContractBuilder.mutual_fund("VINIX")
        assert contract.symbol == "VINIX"
        assert contract.secType == "FUND"


class TestContractBuilderByIdentifier:
    """Tests for contract lookup by identifier"""

    def test_by_conid(self):
        """Test lookup by contract ID"""
        contract = ContractBuilder.by_conid(265598)
        assert contract.conId == 265598

    def test_by_isin(self):
        """Test lookup by ISIN"""
        contract = ContractBuilder.by_isin("US0378331005")
        assert contract.secIdType == "ISIN"
        assert contract.secId == "US0378331005"

    def test_by_figi(self):
        """Test lookup by FIGI"""
        contract = ContractBuilder.by_figi("BBG000B9XRY4")
        assert contract.secIdType == "FIGI"
        assert contract.secId == "BBG000B9XRY4"


class TestContractBuilderCombo:
    """Tests for combo/spread contract creation"""

    def test_combo_leg_creation(self):
        """Test combo leg creation"""
        leg = ContractBuilder.create_combo_leg(265598, "BUY", 1)
        assert leg.conId == 265598
        assert leg.action == "BUY"
        assert leg.ratio == 1
        assert leg.exchange == "SMART"

    def test_combo_contract(self):
        """Test combo contract creation"""
        leg1 = ContractBuilder.create_combo_leg(265598, "BUY", 1)
        leg2 = ContractBuilder.create_combo_leg(265599, "SELL", 1)
        contract = ContractBuilder.combo("SPY", [leg1, leg2])
        assert contract.symbol == "SPY"
        assert contract.secType == "BAG"
        assert len(contract.comboLegs) == 2


class TestContractBuilderAlgoVenue:
    """Tests for algo venue contract creation"""

    def test_jefferies_stock(self):
        """Test Jefferies algo venue stock"""
        contract = ContractBuilder.jefferies_stock("AAPL")
        assert contract.symbol == "AAPL"
        assert contract.exchange == "JEFFALGO"

    def test_csfb_stock(self):
        """Test CSFB algo venue stock"""
        contract = ContractBuilder.csfb_stock("AAPL")
        assert contract.exchange == "CSFBALGO"

    def test_ibkrats_stock(self):
        """Test IBKRATS algo venue stock"""
        contract = ContractBuilder.ibkrats_stock("AAPL")
        assert contract.exchange == "IBKRATS"


# =============================================================================
# OrderFactory Tests
# =============================================================================

class TestOrderFactoryMarket:
    """Tests for market order creation"""

    def test_market_buy(self):
        """Test market buy order"""
        order = OrderFactory.market("BUY", Decimal("100"))
        assert order.action == "BUY"
        assert order.totalQuantity == Decimal("100")
        assert order.orderType == "MKT"

    def test_market_sell(self):
        """Test market sell order"""
        order = OrderFactory.market("SELL", Decimal("50"))
        assert order.action == "SELL"
        assert order.totalQuantity == Decimal("50")

    def test_market_on_open(self):
        """Test market on open order"""
        order = OrderFactory.market_on_open("BUY", Decimal("100"))
        assert order.orderType == "MKT"
        assert order.tif == "OPG"

    def test_market_on_close(self):
        """Test market on close order"""
        order = OrderFactory.market_on_close("BUY", Decimal("100"))
        assert order.orderType == "MOC"

    def test_market_to_limit(self):
        """Test market to limit order"""
        order = OrderFactory.market_to_limit("BUY", Decimal("100"))
        assert order.orderType == "MTL"

    def test_market_with_protection(self):
        """Test market with protection order"""
        order = OrderFactory.market_with_protection("BUY", Decimal("100"))
        assert order.orderType == "MKT PRT"

    def test_market_if_touched(self):
        """Test market if touched order"""
        order = OrderFactory.market_if_touched("BUY", Decimal("100"), 150.0)
        assert order.orderType == "MIT"
        assert order.auxPrice == 150.0


class TestOrderFactoryLimit:
    """Tests for limit order creation"""

    def test_limit_buy(self):
        """Test limit buy order"""
        order = OrderFactory.limit("BUY", Decimal("100"), 150.0)
        assert order.action == "BUY"
        assert order.totalQuantity == Decimal("100")
        assert order.orderType == "LMT"
        assert order.lmtPrice == 150.0

    def test_limit_sell(self):
        """Test limit sell order"""
        order = OrderFactory.limit("SELL", Decimal("50"), 155.0)
        assert order.action == "SELL"
        assert order.lmtPrice == 155.0

    def test_limit_on_open(self):
        """Test limit on open order"""
        order = OrderFactory.limit_on_open("BUY", Decimal("100"), 150.0)
        assert order.orderType == "LMT"
        assert order.tif == "OPG"

    def test_limit_on_close(self):
        """Test limit on close order"""
        order = OrderFactory.limit_on_close("BUY", Decimal("100"), 150.0)
        assert order.orderType == "LOC"

    def test_limit_if_touched(self):
        """Test limit if touched order"""
        order = OrderFactory.limit_if_touched("BUY", Decimal("100"), 150.0, 148.0)
        assert order.orderType == "LIT"
        assert order.lmtPrice == 150.0
        assert order.auxPrice == 148.0


class TestOrderFactoryStop:
    """Tests for stop order creation"""

    def test_stop_buy(self):
        """Test stop buy order"""
        order = OrderFactory.stop("BUY", Decimal("100"), 155.0)
        assert order.orderType == "STP"
        assert order.auxPrice == 155.0

    def test_stop_limit(self):
        """Test stop limit order"""
        order = OrderFactory.stop_limit("SELL", Decimal("100"), 145.0, 144.0)
        assert order.orderType == "STP LMT"
        assert order.lmtPrice == 145.0
        assert order.auxPrice == 144.0

    def test_stop_with_protection(self):
        """Test stop with protection order"""
        order = OrderFactory.stop_with_protection("SELL", Decimal("100"), 145.0)
        assert order.orderType == "STP PRT"


class TestOrderFactoryTrailing:
    """Tests for trailing stop order creation"""

    def test_trailing_stop_amount(self):
        """Test trailing stop with amount"""
        order = OrderFactory.trailing_stop("SELL", Decimal("100"), trail_amount=2.0)
        assert order.orderType == "TRAIL"
        assert order.auxPrice == 2.0
        assert order.totalQuantity == Decimal("100")
        assert order.action == "SELL"

    def test_trailing_stop_percent(self):
        """Test trailing stop with percent"""
        order = OrderFactory.trailing_stop("SELL", Decimal("100"), trail_percent=5.0)
        assert order.orderType == "TRAIL"
        assert order.trailingPercent == 5.0
        assert order.totalQuantity == Decimal("100")
        assert order.action == "SELL"

    def test_trailing_stop_limit(self):
        """Test trailing stop limit order"""
        order = OrderFactory.trailing_stop_limit(
            "SELL", Decimal("100"), trail_amount=2.0, limit_offset=0.5
        )
        assert order.orderType == "TRAIL LIMIT"
        assert order.lmtPriceOffset == 0.5
        assert order.auxPrice == 2.0


class TestOrderFactoryPegged:
    """Tests for pegged order creation"""

    def test_pegged_to_market(self):
        """Test pegged to market order"""
        order = OrderFactory.pegged_to_market("BUY", Decimal("100"), 0.05)
        assert order.orderType == "PEG MKT"
        assert order.auxPrice == 0.05

    def test_pegged_to_midpoint(self):
        """Test pegged to midpoint order"""
        order = OrderFactory.pegged_to_midpoint("BUY", Decimal("100"))
        assert order.orderType == "PEG MID"
        assert order.auxPrice == 0.0
        assert order.lmtPrice == 0.0

    def test_midprice(self):
        """Test midprice order"""
        order = OrderFactory.midprice("BUY", Decimal("100"), 150.0)
        assert order.orderType == "MIDPRICE"


class TestOrderFactoryRelative:
    """Tests for relative order creation"""

    def test_relative(self):
        """Test relative order"""
        order = OrderFactory.relative("BUY", Decimal("100"), 0.01)
        assert order.orderType == "REL"
        assert order.auxPrice == 0.01
        assert order.lmtPrice == 0.0

    def test_passive_relative(self):
        """Test passive relative order"""
        order = OrderFactory.passive_relative("BUY", Decimal("100"), 0.01)
        assert order.orderType == "PASSV REL"


class TestOrderFactoryBracket:
    """Tests for bracket order creation"""

    def test_bracket_order(self):
        """Test bracket order creation"""
        parent, take_profit, stop_loss = OrderFactory.bracket(
            1, "BUY", Decimal("100"), 150.0, 160.0, 140.0
        )

        # Parent order
        assert parent.orderId == 1
        assert parent.action == "BUY"
        assert parent.orderType == "LMT"
        assert parent.lmtPrice == 150.0
        assert parent.transmit is False

        # Take profit
        assert take_profit.parentId == 1
        assert take_profit.action == "SELL"
        assert take_profit.orderType == "LMT"
        assert take_profit.lmtPrice == 160.0
        assert take_profit.transmit is False

        # Stop loss
        assert stop_loss.parentId == 1
        assert stop_loss.action == "SELL"
        assert stop_loss.orderType == "STP"
        assert stop_loss.auxPrice == 140.0
        assert stop_loss.transmit is True  # Last order transmits all

    def test_bracket_sell_order(self):
        """Test bracket sell order (short position)"""
        parent, take_profit, stop_loss = OrderFactory.bracket(
            1, "SELL", Decimal("100"), 150.0, 140.0, 160.0
        )

        # Take profit should be BUY (covering)
        assert take_profit.action == "BUY"
        assert stop_loss.action == "BUY"

    def test_bracket_with_trailing_stop(self):
        """Test bracket with trailing stop"""
        parent, take_profit, trailing_stop = OrderFactory.bracket_with_trailing_stop(
            1, "BUY", Decimal("100"), 150.0, 160.0, 5.0
        )

        assert trailing_stop.orderType == "TRAIL"
        assert trailing_stop.trailingPercent == 5.0


class TestOrderFactoryOCA:
    """Tests for OCA order creation"""

    def test_one_cancels_all(self):
        """Test OCA order group"""
        order1 = OrderFactory.limit("BUY", Decimal("100"), 150.0)
        order2 = OrderFactory.limit("BUY", Decimal("100"), 148.0)

        oca_orders = OrderFactory.one_cancels_all("OCA_GROUP_1", [order1, order2])

        assert len(oca_orders) == 2
        assert oca_orders[0].ocaGroup == "OCA_GROUP_1"
        assert oca_orders[1].ocaGroup == "OCA_GROUP_1"
        assert oca_orders[0].ocaType == 1


class TestOrderFactoryCombo:
    """Tests for combo/spread order creation"""

    def test_combo_limit(self):
        """Test combo limit order"""
        order = OrderFactory.combo_limit("BUY", Decimal("1"), 5.0)
        assert order.orderType == "LMT"
        assert order.lmtPrice == 5.0

    def test_combo_market(self):
        """Test combo market order"""
        order = OrderFactory.combo_market("BUY", Decimal("1"))
        assert order.orderType == "MKT"


class TestOrderFactoryModifiers:
    """Tests for order modifiers"""

    def test_set_good_till_date(self):
        """Test good till date modifier"""
        order = OrderFactory.limit("BUY", Decimal("100"), 150.0)
        OrderFactory.set_good_till_date(order, "20251231-21:00:00")  # 4 PM ET in UTC
        assert order.tif == "GTD"
        assert order.goodTillDate == "20251231-21:00:00"

    def test_set_good_after_time(self):
        """Test good after time modifier"""
        order = OrderFactory.limit("BUY", Decimal("100"), 150.0)
        OrderFactory.set_good_after_time(order, "20251215-14:30:00")  # 9:30 AM ET in UTC
        assert order.goodAfterTime == "20251215-14:30:00"

    def test_set_outside_rth(self):
        """Test outside regular trading hours modifier"""
        order = OrderFactory.limit("BUY", Decimal("100"), 150.0)
        OrderFactory.set_outside_rth(order, True)
        assert order.outsideRth is True

    def test_set_all_or_none(self):
        """Test all or none modifier"""
        order = OrderFactory.limit("BUY", Decimal("100"), 150.0)
        OrderFactory.set_all_or_none(order, True)
        assert order.allOrNone is True

    def test_set_hidden(self):
        """Test hidden order modifier"""
        order = OrderFactory.limit("BUY", Decimal("100"), 150.0)
        OrderFactory.set_hidden(order, True)
        assert order.hidden is True

    def test_set_min_qty(self):
        """Test minimum quantity modifier"""
        order = OrderFactory.limit("BUY", Decimal("100"), 150.0)
        OrderFactory.set_min_qty(order, 50)
        assert order.minQty == 50

    def test_set_display_size(self):
        """Test display size (iceberg) modifier"""
        order = OrderFactory.limit("BUY", Decimal("1000"), 150.0)
        OrderFactory.set_display_size(order, 100)
        assert order.displaySize == 100

    def test_make_adaptive(self):
        """Test making order adaptive"""
        order = OrderFactory.limit("BUY", Decimal("100"), 150.0)
        OrderFactory.make_adaptive(order, "Patient")
        assert order.algoStrategy == "Adaptive"
        assert len(order.algoParams) == 1


# =============================================================================
# AlgoParams Tests
# =============================================================================

class TestAlgoParamsTWAP:
    """Tests for TWAP algorithm parameters"""

    def test_fill_twap(self):
        """Test TWAP parameter configuration"""
        order = Order()
        AlgoParams.fill_twap(order, "Marketable", "09:30:00", "16:00:00")

        assert order.algoStrategy == "Twap"
        assert len(order.algoParams) == 4

        params = {p.tag: p.value for p in order.algoParams}
        assert params["strategyType"] == "Marketable"
        assert params["startTime"] == "09:30:00"
        assert params["endTime"] == "16:00:00"

    def test_twap_alias(self):
        """Test TWAP convenience alias"""
        order = Order()
        TWAP(order, "Marketable", "09:30:00", "16:00:00")
        assert order.algoStrategy == "Twap"


class TestAlgoParamsVWAP:
    """Tests for VWAP algorithm parameters"""

    def test_fill_vwap(self):
        """Test VWAP parameter configuration"""
        order = Order()
        AlgoParams.fill_vwap(order, 0.1, "09:30:00", "16:00:00")

        assert order.algoStrategy == "Vwap"
        params = {p.tag: p.value for p in order.algoParams}
        assert params["maxPctVol"] == str(0.1)
        assert params["startTime"] == "09:30:00"
        assert params["endTime"] == "16:00:00"

    def test_vwap_alias(self):
        """Test VWAP convenience alias"""
        order = Order()
        VWAP(order, 0.1, "09:30:00", "16:00:00")
        assert order.algoStrategy == "Vwap"


class TestAlgoParamsAdaptive:
    """Tests for Adaptive algorithm parameters"""

    def test_fill_adaptive_normal(self):
        """Test Adaptive with normal priority"""
        order = Order()
        AlgoParams.fill_adaptive(order, "Normal")

        assert order.algoStrategy == "Adaptive"
        params = {p.tag: p.value for p in order.algoParams}
        assert params["adaptivePriority"] == "Normal"

    def test_fill_adaptive_patient(self):
        """Test Adaptive with patient priority"""
        order = Order()
        AlgoParams.fill_adaptive(order, "Patient")
        params = {p.tag: p.value for p in order.algoParams}
        assert params["adaptivePriority"] == "Patient"

    def test_adaptive_alias(self):
        """Test Adaptive convenience alias"""
        order = Order()
        Adaptive(order, "Urgent")
        assert order.algoStrategy == "Adaptive"


class TestAlgoParamsPctVol:
    """Tests for Percentage of Volume algorithm parameters"""

    def test_fill_pct_vol(self):
        """Test PctVol parameter configuration"""
        order = Order()
        AlgoParams.fill_pct_vol(order, 0.05, "09:30:00", "16:00:00")

        assert order.algoStrategy == "PctVol"
        params = {p.tag: p.value for p in order.algoParams}
        assert params["pctVol"] == str(0.05)
        assert params["startTime"] == "09:30:00"
        assert params["endTime"] == "16:00:00"

    def test_pct_vol_alias(self):
        """Test PctVol convenience alias"""
        order = Order()
        PctVol(order, 0.05, "09:30:00", "16:00:00")
        assert order.algoStrategy == "PctVol"


class TestAlgoParamsArrivalPrice:
    """Tests for Arrival Price algorithm parameters"""

    def test_fill_arrival_price(self):
        """Test ArrivalPrice parameter configuration"""
        order = Order()
        AlgoParams.fill_arrival_price(order, 0.1, "Medium", "09:30:00", "16:00:00")

        assert order.algoStrategy == "ArrivalPx"
        params = {p.tag: p.value for p in order.algoParams}
        assert params["riskAversion"] == "Medium"

    def test_arrival_price_alias(self):
        """Test ArrivalPrice convenience alias"""
        order = Order()
        ArrivalPrice(order, 0.1, "Low", "09:30:00", "16:00:00")
        assert order.algoStrategy == "ArrivalPx"


class TestAlgoParamsMinImpact:
    """Tests for Minimum Impact algorithm parameters"""

    def test_fill_min_impact(self):
        """Test MinImpact parameter configuration"""
        order = Order()
        AlgoParams.fill_min_impact(order, 0.05)

        assert order.algoStrategy == "MinImpact"
        params = {p.tag: p.value for p in order.algoParams}
        assert params["maxPctVol"] == str(0.05)

    def test_min_impact_alias(self):
        """Test MinImpact convenience alias"""
        order = Order()
        MinImpact(order, 0.05)
        assert order.algoStrategy == "MinImpact"


class TestAlgoParamsDarkIce:
    """Tests for DarkIce algorithm parameters"""

    def test_fill_dark_ice(self):
        """Test DarkIce parameter configuration"""
        order = Order()
        AlgoParams.fill_dark_ice(order, 100, "09:30:00", "16:00:00")

        assert order.algoStrategy == "DarkIce"
        params = {p.tag: p.value for p in order.algoParams}
        assert params["displaySize"] == str(100)

    def test_dark_ice_alias(self):
        """Test DarkIce convenience alias"""
        order = Order()
        DarkIce(order, 100, "09:30:00", "16:00:00")
        assert order.algoStrategy == "DarkIce"


class TestAlgoParamsClosePrice:
    """Tests for Close Price algorithm parameters"""

    def test_fill_close_price(self):
        """Test ClosePrice parameter configuration"""
        order = Order()
        AlgoParams.fill_close_price(order, 0.1, "Medium", "14:00:00")

        assert order.algoStrategy == "ClosePx"
        params = {p.tag: p.value for p in order.algoParams}
        assert params["riskAversion"] == "Medium"

    def test_close_price_alias(self):
        """Test ClosePx convenience alias"""
        order = Order()
        ClosePx(order, 0.1, "High", "14:00:00")
        assert order.algoStrategy == "ClosePx"


class TestAlgoParamsAccumulateDistribute:
    """Tests for Accumulate/Distribute algorithm parameters"""

    def test_fill_accumulate_distribute(self):
        """Test AD parameter configuration"""
        order = Order()
        AlgoParams.fill_accumulate_distribute(
            order,
            component_size=100,
            time_between_orders=60,
            randomize_time_20=True,
            randomize_size_55=True
        )

        assert order.algoStrategy == "AD"
        params = {p.tag: p.value for p in order.algoParams}
        assert params["componentSize"] == str(100)
        assert params["timeBetweenOrders"] == str(60)
        assert params["randomizeTime20"] == str(1)
        assert params["randomizeSize55"] == str(1)


class TestAlgoParamsScale:
    """Tests for Scale order parameters"""

    def test_fill_scale_params(self):
        """Test scale order parameters"""
        order = Order()
        AlgoParams.fill_scale_params(
            order,
            init_level_size=100,
            subs_level_size=50,
            random_percent=True,
            price_increment=0.50
        )

        assert order.scaleInitLevelSize == 100
        assert order.scaleSubsLevelSize == 50
        assert order.scaleRandomPercent is True
        assert order.scalePriceIncrement == 0.50


class TestAlgoParamsBalanceImpactRisk:
    """Tests for Balance Impact Risk algorithm parameters"""

    def test_fill_balance_impact_risk(self):
        """Test BalanceImpactRisk parameter configuration"""
        order = Order()
        AlgoParams.fill_balance_impact_risk(order, 0.1, "Medium")

        assert order.algoStrategy == "BalanceImpactRisk"
        params = {p.tag: p.value for p in order.algoParams}
        assert params["riskAversion"] == "Medium"


class TestAlgoParamsThirdParty:
    """Tests for third-party algo provider parameters"""

    def test_fill_jefferies_vwap(self):
        """Test Jefferies VWAP parameters"""
        order = Order()
        AlgoParams.fill_jefferies_vwap(order, "09:30:00", "16:00:00")

        assert order.algoStrategy == "VWAP"
        params = {p.tag: p.value for p in order.algoParams}
        assert params["StartTime"] == "09:30:00"

    def test_fill_csfb_inline(self):
        """Test CSFB Inline parameters"""
        order = Order()
        AlgoParams.fill_csfb_inline(order, "09:30:00", "16:00:00")

        assert order.algoStrategy == "INLINE"

    def test_fill_qbalgo_strobe(self):
        """Test QB Algo Strobe parameters"""
        order = Order()
        AlgoParams.fill_qbalgo_strobe(order, "09:30:00", "16:00:00")

        assert order.algoStrategy == "STROBE"
