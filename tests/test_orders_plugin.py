"""
Tests for OrdersPlugin - System plugin for executing all IB order types
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from typing import Optional

from ib.plugins.orders.plugin import (
    OrdersPlugin,
    OrderType,
    TimeInForce,
    OrderRecord,
    ORDER_TYPE_ALIASES,
    TIF_ALIASES,
    ORDERS_PLUGIN_NAME,
)
from ib.plugins.base import PluginState


class MockContract:
    """Mock IB contract"""
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.secType = "STK"
        self.exchange = "SMART"
        self.currency = "USD"


class MockPosition:
    """Mock portfolio position"""
    def __init__(self, symbol: str, quantity: float, price: float):
        self.symbol = symbol
        self.quantity = quantity
        self.current_price = price
        self.contract = MockContract(symbol)


class MockPortfolio:
    """Mock portfolio for testing"""
    def __init__(self):
        self.positions = []
        self.connected = True
        self._next_order_id = 1000
        self._lock = MagicMock()
        self._orders_placed = []

    def get_position(self, symbol: str) -> Optional[MockPosition]:
        for p in self.positions:
            if p.symbol == symbol:
                return p
        return None

    def place_order(
        self,
        contract,
        action: str,
        quantity: float,
        order_type: str = "MKT",
        limit_price: float = 0.0,
        stop_price: float = 0.0,
        tif: str = "DAY",
    ) -> Optional[int]:
        order_id = self._next_order_id
        self._next_order_id += 1
        self._orders_placed.append({
            "order_id": order_id,
            "symbol": contract.symbol,
            "action": action,
            "quantity": quantity,
            "order_type": order_type,
            "limit_price": limit_price,
            "stop_price": stop_price,
            "tif": tif,
        })
        return order_id

    def placeOrder(self, order_id, contract, order):
        """IB API method"""
        self._orders_placed.append({
            "order_id": order_id,
            "symbol": contract.symbol,
            "action": order.action,
            "quantity": order.totalQuantity,
            "order_type": order.orderType,
        })


class TestOrdersPluginBasic:
    """Basic tests for OrdersPlugin"""

    def test_plugin_name(self):
        """Test plugin has correct reserved name"""
        plugin = OrdersPlugin()
        assert plugin.name == ORDERS_PLUGIN_NAME
        assert plugin.name == "_orders"

    def test_is_system_plugin(self):
        """Test plugin is marked as system plugin"""
        plugin = OrdersPlugin()
        assert plugin.IS_SYSTEM_PLUGIN == True
        assert plugin.is_system_plugin == True

    def test_description(self):
        """Test plugin has a description"""
        plugin = OrdersPlugin()
        assert "order" in plugin.description.lower()

    def test_load(self):
        """Test plugin can be loaded"""
        plugin = OrdersPlugin()
        assert plugin.load() == True
        assert plugin.state == PluginState.LOADED

    def test_start_stop(self):
        """Test plugin lifecycle"""
        plugin = OrdersPlugin()
        plugin.load()

        assert plugin.start() == True
        assert plugin.state == PluginState.STARTED

        assert plugin.stop() == True
        assert plugin.state == PluginState.STOPPED

    def test_no_signals(self):
        """Test plugin doesn't generate signals"""
        plugin = OrdersPlugin()
        signals = plugin.calculate_signals({"SPY": [{"close": 450.0}]})
        assert signals == []

    def test_required_bars_zero(self):
        """Test no bars are required"""
        plugin = OrdersPlugin()
        assert plugin.required_bars == 0

    def test_no_holdings(self):
        """Test plugin doesn't hold positions"""
        plugin = OrdersPlugin()
        holdings = plugin.get_effective_holdings()
        assert holdings["cash"] == 0.0
        assert holdings["positions"] == []
        assert holdings["total_value"] == 0.0


class TestOrderTypeAliases:
    """Test order type parsing"""

    def test_market_aliases(self):
        """Test market order aliases"""
        plugin = OrdersPlugin()
        assert plugin.parse_order_type("market") == OrderType.MARKET
        assert plugin.parse_order_type("mkt") == OrderType.MARKET
        assert plugin.parse_order_type("MARKET") == OrderType.MARKET

    def test_limit_aliases(self):
        """Test limit order aliases"""
        plugin = OrdersPlugin()
        assert plugin.parse_order_type("limit") == OrderType.LIMIT
        assert plugin.parse_order_type("lmt") == OrderType.LIMIT

    def test_stop_aliases(self):
        """Test stop order aliases"""
        plugin = OrdersPlugin()
        assert plugin.parse_order_type("stop") == OrderType.STOP
        assert plugin.parse_order_type("stp") == OrderType.STOP

    def test_stop_limit_aliases(self):
        """Test stop-limit order aliases"""
        plugin = OrdersPlugin()
        assert plugin.parse_order_type("stop-limit") == OrderType.STOP_LIMIT
        assert plugin.parse_order_type("stop_limit") == OrderType.STOP_LIMIT
        assert plugin.parse_order_type("stplmt") == OrderType.STOP_LIMIT

    def test_trailing_stop_aliases(self):
        """Test trailing stop aliases"""
        plugin = OrdersPlugin()
        assert plugin.parse_order_type("trail") == OrderType.TRAILING_STOP
        assert plugin.parse_order_type("trailing") == OrderType.TRAILING_STOP
        assert plugin.parse_order_type("trailing-stop") == OrderType.TRAILING_STOP

    def test_moc_loc_aliases(self):
        """Test MOC/LOC aliases"""
        plugin = OrdersPlugin()
        assert plugin.parse_order_type("moc") == OrderType.MARKET_ON_CLOSE
        assert plugin.parse_order_type("loc") == OrderType.LIMIT_ON_CLOSE
        assert plugin.parse_order_type("moo") == OrderType.MARKET_ON_OPEN
        assert plugin.parse_order_type("loo") == OrderType.LIMIT_ON_OPEN

    def test_unknown_type(self):
        """Test unknown order type returns None"""
        plugin = OrdersPlugin()
        assert plugin.parse_order_type("invalid") is None


class TestTimeInForceAliases:
    """Test time in force parsing"""

    def test_tif_aliases(self):
        """Test TIF aliases"""
        plugin = OrdersPlugin()
        assert plugin.parse_tif("day") == TimeInForce.DAY
        assert plugin.parse_tif("gtc") == TimeInForce.GTC
        assert plugin.parse_tif("ioc") == TimeInForce.IOC
        assert plugin.parse_tif("fok") == TimeInForce.FOK

    def test_unknown_tif(self):
        """Test unknown TIF returns None"""
        plugin = OrdersPlugin()
        assert plugin.parse_tif("invalid") is None


class TestOrderExecution:
    """Test order execution"""

    def setup_method(self):
        self.portfolio = MockPortfolio()
        self.portfolio.positions = [
            MockPosition("SPY", 100, 450.0),
            MockPosition("QQQ", 50, 380.0),
        ]
        self.plugin = OrdersPlugin(portfolio=self.portfolio)
        self.plugin.load()
        self.plugin.start()

    def test_execute_no_portfolio(self):
        """Test execute fails without portfolio"""
        plugin = OrdersPlugin()
        success, order_id, message = plugin.execute_order(
            symbol="SPY", action="BUY", quantity=100
        )
        assert success == False
        assert "No portfolio" in message

    def test_execute_invalid_action(self):
        """Test execute fails with invalid action"""
        success, order_id, message = self.plugin.execute_order(
            symbol="SPY", action="HOLD", quantity=100
        )
        assert success == False
        assert "Invalid action" in message

    def test_execute_invalid_quantity(self):
        """Test execute fails with invalid quantity"""
        success, order_id, message = self.plugin.execute_order(
            symbol="SPY", action="BUY", quantity=-10
        )
        assert success == False
        assert "Invalid quantity" in message

    def test_execute_market_dry_run(self):
        """Test market order dry run"""
        success, order_id, message = self.plugin.execute_order(
            symbol="SPY", action="BUY", quantity=100, dry_run=True
        )
        assert success == True
        assert order_id is None
        assert "DRY RUN" in message
        assert "BUY 100 SPY MARKET" in message

    def test_execute_market_order(self):
        """Test market order execution"""
        success, order_id, message = self.plugin.execute_order(
            symbol="SPY", action="BUY", quantity=100, dry_run=False
        )
        assert success == True
        assert order_id is not None
        assert "EXECUTED" in message

    def test_execute_limit_order(self):
        """Test limit order execution"""
        success, order_id, message = self.plugin.execute_order(
            symbol="SPY",
            action="BUY",
            quantity=100,
            order_type=OrderType.LIMIT,
            limit_price=450.00,
            dry_run=False,
        )
        assert success == True
        assert order_id is not None

        # Check order was placed correctly
        order = self.portfolio._orders_placed[-1]
        assert order["order_type"] == "LMT"
        assert order["limit_price"] == 450.00

    def test_execute_limit_requires_price(self):
        """Test limit order requires price"""
        success, order_id, message = self.plugin.execute_order(
            symbol="SPY",
            action="BUY",
            quantity=100,
            order_type=OrderType.LIMIT,
            dry_run=False,
        )
        assert success == False
        assert "Limit price required" in message

    def test_execute_stop_order(self):
        """Test stop order execution"""
        success, order_id, message = self.plugin.execute_order(
            symbol="SPY",
            action="SELL",
            quantity=100,
            order_type=OrderType.STOP,
            stop_price=440.00,
            dry_run=False,
        )
        assert success == True
        assert order_id is not None

        order = self.portfolio._orders_placed[-1]
        assert order["order_type"] == "STP"
        assert order["stop_price"] == 440.00

    def test_execute_stop_requires_price(self):
        """Test stop order requires price"""
        success, order_id, message = self.plugin.execute_order(
            symbol="SPY",
            action="SELL",
            quantity=100,
            order_type=OrderType.STOP,
            dry_run=False,
        )
        assert success == False
        assert "Stop price required" in message

    def test_execute_stop_limit_order(self):
        """Test stop-limit order execution"""
        success, order_id, message = self.plugin.execute_order(
            symbol="SPY",
            action="SELL",
            quantity=100,
            order_type=OrderType.STOP_LIMIT,
            stop_price=440.00,
            limit_price=438.00,
            dry_run=False,
        )
        assert success == True
        assert order_id is not None

        order = self.portfolio._orders_placed[-1]
        assert order["order_type"] == "STP LMT"
        assert order["stop_price"] == 440.00
        assert order["limit_price"] == 438.00

    def test_execute_trailing_stop_amount(self):
        """Test trailing stop with dollar amount"""
        success, order_id, message = self.plugin.execute_order(
            symbol="SPY",
            action="SELL",
            quantity=100,
            order_type=OrderType.TRAILING_STOP,
            trail_amount=2.00,
            dry_run=True,
        )
        assert success == True
        assert "trail $2.00" in message

    def test_execute_trailing_stop_percent(self):
        """Test trailing stop with percentage"""
        success, order_id, message = self.plugin.execute_order(
            symbol="SPY",
            action="SELL",
            quantity=100,
            order_type=OrderType.TRAILING_STOP,
            trail_percent=1.0,
            dry_run=True,
        )
        assert success == True
        assert "trail 1.0%" in message

    def test_execute_trailing_requires_amount_or_percent(self):
        """Test trailing stop requires amount or percent"""
        success, order_id, message = self.plugin.execute_order(
            symbol="SPY",
            action="SELL",
            quantity=100,
            order_type=OrderType.TRAILING_STOP,
            dry_run=False,
        )
        assert success == False
        assert "Trailing amount or percent required" in message

    def test_execute_with_tif(self):
        """Test order with time in force"""
        success, order_id, message = self.plugin.execute_order(
            symbol="SPY",
            action="BUY",
            quantity=100,
            tif=TimeInForce.GTC,
            dry_run=True,
        )
        assert success == True
        assert "[GTC]" in message

    def test_order_recorded(self):
        """Test executed order is recorded"""
        success, order_id, message = self.plugin.execute_order(
            symbol="SPY",
            action="BUY",
            quantity=100,
            dry_run=False,
        )
        assert success == True

        orders = self.plugin.get_orders()
        assert len(orders) == 1
        assert orders[0].symbol == "SPY"
        assert orders[0].action == "BUY"
        assert orders[0].quantity == 100

    def test_get_order(self):
        """Test getting specific order"""
        success, order_id, message = self.plugin.execute_order(
            symbol="SPY",
            action="BUY",
            quantity=100,
            dry_run=False,
        )

        order = self.plugin.get_order(order_id)
        assert order is not None
        assert order.order_id == order_id
        assert order.symbol == "SPY"


class TestOrdersPluginRequests:
    """Test custom request handling"""

    def setup_method(self):
        self.portfolio = MockPortfolio()
        self.portfolio.positions = [MockPosition("SPY", 100, 450.0)]
        self.plugin = OrdersPlugin(portfolio=self.portfolio)
        self.plugin.load()
        self.plugin.start()

    def test_list_orders_request(self):
        """Test list_orders request"""
        # Place an order first
        self.plugin.execute_order("SPY", "BUY", 100, dry_run=False)

        response = self.plugin.handle_request("list_orders", {})
        assert response["success"] == True
        assert len(response["orders"]) == 1
        assert response["orders"][0]["symbol"] == "SPY"

    def test_get_order_request(self):
        """Test get_order request"""
        success, order_id, _ = self.plugin.execute_order("SPY", "BUY", 100, dry_run=False)

        response = self.plugin.handle_request("get_order", {"order_id": order_id})
        assert response["success"] == True
        assert response["order"]["order_id"] == order_id

    def test_get_order_not_found(self):
        """Test get_order with unknown order"""
        response = self.plugin.handle_request("get_order", {"order_id": 9999})
        assert response["success"] == False
        assert "not found" in response["error"]

    def test_unknown_request(self):
        """Test unknown request"""
        response = self.plugin.handle_request("invalid_request", {})
        assert response["success"] == False


class TestOrdersPluginStatus:
    """Test status reporting"""

    def test_status_includes_system_flag(self):
        """Test status includes is_system_plugin"""
        plugin = OrdersPlugin()
        plugin.load()

        status = plugin.get_status()
        assert status["is_system_plugin"] == True

    def test_status_includes_orders_count(self):
        """Test status includes orders count"""
        portfolio = MockPortfolio()
        portfolio.positions = [MockPosition("SPY", 100, 450.0)]
        plugin = OrdersPlugin(portfolio=portfolio)
        plugin.load()
        plugin.start()

        # Place some orders
        plugin.execute_order("SPY", "BUY", 100, dry_run=False)
        plugin.execute_order("SPY", "SELL", 50, dry_run=False)

        status = plugin.get_status()
        assert status["orders_count"] == 2


class TestOrderCommand:
    """Test order command handler integration"""

    def setup_method(self):
        from ib.run_engine import EngineCommandHandler

        self.portfolio = MockPortfolio()
        self.portfolio.positions = [MockPosition("SPY", 100, 450.0)]

        # Create mock engine
        self.engine = Mock()
        self.engine.portfolio = self.portfolio
        self.engine.plugin_executive = None

        self.handler = EngineCommandHandler(self.engine)

    def test_order_missing_args(self):
        """Test order with missing arguments"""
        result = self.handler.handle_order(["buy"])
        assert result.status.value == "error"
        assert "Usage:" in result.message

    def test_order_invalid_quantity(self):
        """Test order with invalid quantity"""
        result = self.handler.handle_order(["buy", "SPY", "abc"])
        assert result.status.value == "error"
        assert "Invalid quantity" in result.message

    def test_order_dry_run_without_plugin(self):
        """Test order dry run through direct portfolio"""
        result = self.handler.handle_order(["buy", "SPY", "100"])
        assert result.status.value == "success"
        assert "DRY RUN" in result.message

    def test_order_with_confirm_without_plugin(self):
        """Test order execution through direct portfolio"""
        result = self.handler.handle_order(["buy", "SPY", "100", "--confirm"])
        assert result.status.value == "success"
        assert "EXECUTED" in result.message
        assert result.data["order_id"] is not None

    def test_order_limit_without_plugin(self):
        """Test limit order through direct portfolio"""
        result = self.handler.handle_order(["buy", "SPY", "100", "limit", "450.00", "--confirm"])
        assert result.status.value == "success"

        order = self.portfolio._orders_placed[-1]
        assert order["order_type"] == "LMT"
        assert order["limit_price"] == 450.00

    def test_order_stop_without_plugin(self):
        """Test stop order through direct portfolio"""
        result = self.handler.handle_order(["sell", "SPY", "50", "stop", "440.00", "--confirm"])
        assert result.status.value == "success"

        order = self.portfolio._orders_placed[-1]
        assert order["order_type"] == "STP"
        assert order["stop_price"] == 440.00
