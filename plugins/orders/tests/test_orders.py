"""
Unit tests for plugins/orders/plugin.py
"""
from datetime import datetime
from unittest.mock import Mock
import pytest

from plugins.orders.plugin import OrdersPlugin, OrderRecord, OrderType, TimeInForce


def _make_plugin():
    portfolio = Mock()
    portfolio.get_position = Mock(return_value=None)
    plugin = OrdersPlugin(portfolio=portfolio)
    plugin.load()
    plugin.start()
    return plugin, portfolio


def _add_order(plugin, order_id=1001, symbol="SPY", action="BUY",
               quantity=100, order_type="MKT", status="SUBMITTED"):
    rec = OrderRecord(
        order_id=order_id,
        symbol=symbol,
        action=action,
        quantity=quantity,
        order_type=order_type,
        status=status,
    )
    plugin._orders[order_id] = rec
    return rec


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

class TestLifecycle:
    def test_load_and_start(self):
        plugin, _ = _make_plugin()
        from plugins.base import PluginState
        assert plugin._state == PluginState.STARTED

    def test_stop(self):
        plugin, _ = _make_plugin()
        assert plugin.stop() is True

    def test_freeze_and_resume(self):
        plugin, _ = _make_plugin()
        assert plugin.freeze() is True
        assert plugin.resume() is True

    def test_calculate_signals_returns_empty(self):
        plugin, _ = _make_plugin()
        assert plugin.calculate_signals() == []

    def test_is_system_plugin(self):
        plugin, _ = _make_plugin()
        assert plugin.IS_SYSTEM_PLUGIN is True


# ---------------------------------------------------------------------------
# Order type / TIF parsing
# ---------------------------------------------------------------------------

class TestParsing:
    def test_parse_market(self):
        plugin, _ = _make_plugin()
        assert plugin.parse_order_type("market") == OrderType.MARKET
        assert plugin.parse_order_type("MKT") == OrderType.MARKET

    def test_parse_limit(self):
        plugin, _ = _make_plugin()
        assert plugin.parse_order_type("limit") == OrderType.LIMIT
        assert plugin.parse_order_type("lmt") == OrderType.LIMIT

    def test_parse_stop(self):
        plugin, _ = _make_plugin()
        assert plugin.parse_order_type("stop") == OrderType.STOP

    def test_parse_stop_limit(self):
        plugin, _ = _make_plugin()
        assert plugin.parse_order_type("stop-limit") == OrderType.STOP_LIMIT
        assert plugin.parse_order_type("stp lmt") == OrderType.STOP_LIMIT

    def test_parse_trail(self):
        plugin, _ = _make_plugin()
        assert plugin.parse_order_type("trail") == OrderType.TRAILING_STOP

    def test_parse_moc(self):
        plugin, _ = _make_plugin()
        assert plugin.parse_order_type("moc") == OrderType.MARKET_ON_CLOSE

    def test_parse_loc(self):
        plugin, _ = _make_plugin()
        assert plugin.parse_order_type("loc") == OrderType.LIMIT_ON_CLOSE

    def test_parse_moo(self):
        plugin, _ = _make_plugin()
        assert plugin.parse_order_type("moo") == OrderType.MARKET_ON_OPEN

    def test_parse_loo(self):
        plugin, _ = _make_plugin()
        assert plugin.parse_order_type("loo") == OrderType.LIMIT_ON_OPEN

    def test_parse_unknown_returns_none(self):
        plugin, _ = _make_plugin()
        assert plugin.parse_order_type("unknown_type") is None

    def test_parse_tif_day(self):
        plugin, _ = _make_plugin()
        assert plugin.parse_tif("day") == TimeInForce.DAY

    def test_parse_tif_gtc(self):
        plugin, _ = _make_plugin()
        assert plugin.parse_tif("gtc") == TimeInForce.GTC

    def test_parse_tif_unknown_returns_none(self):
        plugin, _ = _make_plugin()
        assert plugin.parse_tif("unknown") is None


# ---------------------------------------------------------------------------
# handle_request: list_orders
# ---------------------------------------------------------------------------

class TestListOrders:
    def test_empty_order_list(self):
        plugin, _ = _make_plugin()
        result = plugin.handle_request("list_orders", {})
        assert result["success"] is True
        assert result["orders"] == []

    def test_lists_all_orders(self):
        plugin, _ = _make_plugin()
        _add_order(plugin, 1001, "SPY", "BUY", 100)
        _add_order(plugin, 1002, "QQQ", "SELL", 50)
        result = plugin.handle_request("list_orders", {})
        assert result["success"] is True
        assert len(result["orders"]) == 2
        symbols = {o["symbol"] for o in result["orders"]}
        assert symbols == {"SPY", "QQQ"}

    def test_order_fields_present(self):
        plugin, _ = _make_plugin()
        _add_order(plugin, 1001, "SPY", "BUY", 100)
        result = plugin.handle_request("list_orders", {})
        order = result["orders"][0]
        assert "order_id" in order
        assert "symbol" in order
        assert "action" in order
        assert "quantity" in order
        assert "order_type" in order
        assert "status" in order
        assert "placed_at" in order


# ---------------------------------------------------------------------------
# handle_request: get_order
# ---------------------------------------------------------------------------

class TestGetOrder:
    def test_get_existing_order(self):
        plugin, _ = _make_plugin()
        _add_order(plugin, 1001, "AAPL", "BUY", 25, "LMT")
        result = plugin.handle_request("get_order", {"order_id": 1001})
        assert result["success"] is True
        assert result["order"]["symbol"] == "AAPL"
        assert result["order"]["order_type"] == "LMT"

    def test_get_nonexistent_order(self):
        plugin, _ = _make_plugin()
        result = plugin.handle_request("get_order", {"order_id": 9999})
        assert result["success"] is False

    def test_get_order_all_fields(self):
        plugin, _ = _make_plugin()
        _add_order(plugin, 1001, "SPY", "BUY", 100, "MKT")
        result = plugin.handle_request("get_order", {"order_id": 1001})
        order = result["order"]
        assert "order_id" in order
        assert "symbol" in order
        assert "action" in order
        assert "quantity" in order
        assert "order_type" in order
        assert "limit_price" in order
        assert "stop_price" in order
        assert "tif" in order
        assert "status" in order
        assert "placed_at" in order


# ---------------------------------------------------------------------------
# Unknown request
# ---------------------------------------------------------------------------

class TestUnknownRequest:
    def test_unknown_request(self):
        plugin, _ = _make_plugin()
        result = plugin.handle_request("nonexistent", {})
        assert result["success"] is False


# ---------------------------------------------------------------------------
# get_effective_holdings — orders plugin holds nothing
# ---------------------------------------------------------------------------

class TestEffectiveHoldings:
    def test_no_positions(self):
        plugin, _ = _make_plugin()
        holdings = plugin.get_effective_holdings()
        assert holdings["positions"] == []
        assert holdings["cash"] == 0.0
        assert holdings["is_system_plugin"] is True
