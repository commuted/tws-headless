"""
Tests for the three callback additions:
  1. TradeSignal.quantity → Decimal
  2. Per-plugin order fill / status callbacks
  3. Per-plugin IB error routing
"""

import threading
import time
from decimal import Decimal
from pathlib import Path
from typing import Dict, List
from unittest.mock import Mock, MagicMock, patch

import pytest

from ibapi.contract import Contract

from plugins.base import PluginBase, TradeSignal
from ib.plugin_executive import PluginExecutive, StreamManager, _IB_INFO_CODES
from ib.models import OrderRecord, OrderStatus
from order_reconciler import OrderReconciler


# ===========================================================================
# Shared helpers
# ===========================================================================

class ConcretePlugin(PluginBase):
    """Minimal concrete plugin that records all callback invocations."""

    def __init__(self, name="test_plugin", **kwargs):
        super().__init__(name, **kwargs)
        self.fill_calls: List[OrderRecord] = []
        self.status_calls: List[OrderRecord] = []
        self.error_calls: List[tuple] = []

    @property
    def description(self):
        return "Test plugin"

    def start(self): return True
    def stop(self): return True
    def freeze(self): return True
    def resume(self): return True
    def handle_request(self, request_type, payload): return {}
    def calculate_signals(self): return []

    def on_order_fill(self, order_record):
        self.fill_calls.append(order_record)

    def on_order_status(self, order_record):
        self.status_calls.append(order_record)

    def on_ib_error(self, req_id, error_code, error_string):
        self.error_calls.append((req_id, error_code, error_string))


def make_order(order_id, status=OrderStatus.SUBMITTED, symbol="SPY",
               avg_fill_price=0.0, filled_quantity=0.0):
    return OrderRecord(
        order_id=order_id,
        symbol=symbol,
        action="BUY",
        quantity=1,
        status=status,
        avg_fill_price=avg_fill_price,
        filled_quantity=filled_quantity,
    )


def make_contract(symbol="SPY"):
    c = Contract()
    c.symbol = symbol
    c.secType = "STK"
    c.exchange = "SMART"
    c.currency = "USD"
    return c


def make_executive(portfolio=None):
    return PluginExecutive(portfolio=portfolio, data_feed=None)


# ===========================================================================
# 1.  TradeSignal.quantity → Decimal
# ===========================================================================

class TestTradeSignalDecimal:
    """TradeSignal.quantity is typed as Decimal with a Decimal default."""

    def test_default_is_decimal_zero(self):
        sig = TradeSignal(symbol="SPY", action="HOLD")
        assert isinstance(sig.quantity, Decimal)
        assert sig.quantity == Decimal("0")

    def test_explicit_decimal_quantity(self):
        sig = TradeSignal(symbol="SPY", action="BUY", quantity=Decimal("10"))
        assert isinstance(sig.quantity, Decimal)
        assert sig.quantity == Decimal("10")

    def test_is_actionable_nonzero_decimal(self):
        sig = TradeSignal(symbol="SPY", action="BUY", quantity=Decimal("5"))
        assert sig.is_actionable is True

    def test_is_actionable_zero_decimal(self):
        sig = TradeSignal(symbol="SPY", action="BUY", quantity=Decimal("0"))
        assert sig.is_actionable is False

    def test_is_actionable_hold_action_always_false(self):
        sig = TradeSignal(symbol="SPY", action="HOLD", quantity=Decimal("100"))
        assert sig.is_actionable is False


class TestOrderReconcilerWithDecimal:
    """ReconciledOrder.net_quantity is Decimal; arithmetic stays in Decimal."""

    def test_net_quantity_is_decimal(self):
        reconciler = OrderReconciler()
        contract = make_contract()

        buy = TradeSignal(symbol="SPY", action="BUY", quantity=Decimal("100"))
        sell = TradeSignal(symbol="SPY", action="SELL", quantity=Decimal("30"))
        reconciler.add_signal("a", buy, contract)
        reconciler.add_signal("b", sell, contract)

        orders = reconciler.reconcile()

        assert len(orders) == 1
        assert isinstance(orders[0].net_quantity, Decimal)
        assert orders[0].net_quantity == Decimal("70")

    def test_full_netting_produces_no_order(self):
        reconciler = OrderReconciler()
        contract = make_contract()

        buy = TradeSignal(symbol="SPY", action="BUY", quantity=Decimal("50"))
        sell = TradeSignal(symbol="SPY", action="SELL", quantity=Decimal("50"))
        reconciler.add_signal("a", buy, contract)
        reconciler.add_signal("b", sell, contract)

        orders = reconciler.reconcile()
        assert orders == []

    def test_sell_dominates_produces_sell_order(self):
        reconciler = OrderReconciler()
        contract = make_contract()

        buy = TradeSignal(symbol="SPY", action="BUY", quantity=Decimal("20"))
        sell = TradeSignal(symbol="SPY", action="SELL", quantity=Decimal("80"))
        reconciler.add_signal("a", buy, contract)
        reconciler.add_signal("b", sell, contract)

        orders = reconciler.reconcile()
        assert len(orders) == 1
        assert orders[0].action == "SELL"
        assert orders[0].net_quantity == Decimal("60")

    def test_shares_saved_stat_is_decimal(self):
        reconciler = OrderReconciler()
        contract = make_contract()

        buy = TradeSignal(symbol="SPY", action="BUY", quantity=Decimal("100"))
        sell = TradeSignal(symbol="SPY", action="SELL", quantity=Decimal("40"))
        reconciler.add_signal("a", buy, contract)
        reconciler.add_signal("b", sell, contract)

        reconciler.reconcile()
        # shares_saved = 100 + 40 - abs(100 - 40) = 80
        assert reconciler.stats["shares_saved"] == 80


# ===========================================================================
# 2a. PluginExecutive: register_order_for_plugin
# ===========================================================================

class TestRegisterOrderForPlugin:
    """register_order_for_plugin() populates _order_id_to_plugins."""

    def test_single_plugin(self):
        ex = make_executive()
        ex.register_order_for_plugin(42, "my_plugin")
        assert "my_plugin" in ex._order_id_to_plugins[42]

    def test_multiple_plugins_same_order(self):
        ex = make_executive()
        ex.register_order_for_plugin(1, "plugin_a")
        ex.register_order_for_plugin(1, "plugin_b")
        assert set(ex._order_id_to_plugins[1]) == {"plugin_a", "plugin_b"}

    def test_duplicate_registration_is_idempotent(self):
        ex = make_executive()
        ex.register_order_for_plugin(5, "plugin_a")
        ex.register_order_for_plugin(5, "plugin_a")
        assert ex._order_id_to_plugins[5].count("plugin_a") == 1

    def test_different_orders_tracked_independently(self):
        ex = make_executive()
        ex.register_order_for_plugin(10, "alpha")
        ex.register_order_for_plugin(11, "beta")
        assert ex._order_id_to_plugins[10] == ["alpha"]
        assert ex._order_id_to_plugins[11] == ["beta"]


# ===========================================================================
# 2b. PluginExecutive: _handle_order_status_for_plugins routing
# ===========================================================================

class TestHandleOrderStatusForPlugins:
    """_handle_order_status_for_plugins() dispatches to the right plugins."""

    def _exec_with_plugin(self, plugin):
        ex = make_executive()
        ex.register_plugin(plugin)
        ex.register_order_for_plugin(101, plugin.name)
        return ex

    def test_on_order_status_called_for_submitted(self):
        plugin = ConcretePlugin()
        ex = self._exec_with_plugin(plugin)

        rec = make_order(101, OrderStatus.SUBMITTED)
        ex._handle_order_status_for_plugins(rec)

        assert len(plugin.status_calls) == 1
        assert plugin.status_calls[0] is rec

    def test_on_order_fill_called_when_filled(self):
        plugin = ConcretePlugin()
        ex = self._exec_with_plugin(plugin)

        rec = make_order(101, OrderStatus.FILLED, avg_fill_price=150.0)
        ex._handle_order_status_for_plugins(rec)

        assert len(plugin.fill_calls) == 1
        assert plugin.fill_calls[0] is rec
        # on_order_status is also called
        assert len(plugin.status_calls) == 1

    def test_on_order_fill_not_called_for_cancelled(self):
        plugin = ConcretePlugin()
        ex = self._exec_with_plugin(plugin)

        rec = make_order(101, OrderStatus.CANCELLED)
        ex._handle_order_status_for_plugins(rec)

        assert len(plugin.fill_calls) == 0
        assert len(plugin.status_calls) == 1

    def test_unregistered_order_not_dispatched(self):
        plugin = ConcretePlugin()
        ex = make_executive()
        ex.register_plugin(plugin)  # plugin registered but order is not

        ex._handle_order_status_for_plugins(make_order(999, OrderStatus.FILLED))

        assert len(plugin.fill_calls) == 0
        assert len(plugin.status_calls) == 0

    def test_multiple_plugins_all_notified(self):
        plugin_a = ConcretePlugin("plugin_a")
        plugin_b = ConcretePlugin("plugin_b")

        ex = make_executive()
        ex.register_plugin(plugin_a)
        ex.register_plugin(plugin_b)
        ex.register_order_for_plugin(200, "plugin_a")
        ex.register_order_for_plugin(200, "plugin_b")

        rec = make_order(200, OrderStatus.FILLED)
        ex._handle_order_status_for_plugins(rec)

        assert len(plugin_a.fill_calls) == 1
        assert len(plugin_b.fill_calls) == 1

    def test_plugin_exception_does_not_stop_others(self):
        """An exception in one plugin's callback must not prevent others."""

        class BadPlugin(ConcretePlugin):
            def on_order_status(self, order_record):
                raise RuntimeError("boom")

        bad = BadPlugin("bad_plugin")
        good = ConcretePlugin("good_plugin")

        ex = make_executive()
        ex.register_plugin(bad)
        ex.register_plugin(good)
        ex.register_order_for_plugin(77, "bad_plugin")
        ex.register_order_for_plugin(77, "good_plugin")

        rec = make_order(77, OrderStatus.SUBMITTED)
        ex._handle_order_status_for_plugins(rec)  # must not raise

        assert len(good.status_calls) == 1

    def test_portfolio_callback_wired_at_init(self):
        """portfolio._callbacks['orderStatus'] points to handler after init."""
        portfolio = Mock()
        portfolio._callbacks = {}
        portfolio._on_commission = None

        ex = PluginExecutive(portfolio=portfolio, data_feed=None)

        assert portfolio._callbacks["orderStatus"] == ex._handle_order_status_for_plugins


# ===========================================================================
# 2c. PluginBase: register_order / hook no-ops
# ===========================================================================

class TestPluginBaseOrderHooks:
    def test_register_order_delegates_to_executive(self):
        plugin = ConcretePlugin()
        mock_exec = Mock()
        plugin.set_executive(mock_exec)

        plugin.register_order(77)

        mock_exec.register_order_for_plugin.assert_called_once_with(77, plugin.name)

    def test_register_order_without_executive_is_silent(self):
        plugin = ConcretePlugin()
        plugin.register_order(77)  # no executive set — must not raise

    def test_on_order_fill_default_is_noop(self):
        """PluginBase.on_order_fill does nothing (no exception, no side-effect)."""
        class BaseNoop(PluginBase):
            @property
            def description(self): return ""
            def start(self): return True
            def stop(self): return True
            def freeze(self): return True
            def resume(self): return True
            def handle_request(self, r, p): return {}
            def calculate_signals(self, md): return []

        p = BaseNoop("noop")
        p.on_order_fill(make_order(1, OrderStatus.FILLED))   # no exception
        p.on_order_status(make_order(1, OrderStatus.SUBMITTED))  # no exception

    def test_on_ib_error_default_is_noop(self):
        class BaseNoop(PluginBase):
            @property
            def description(self): return ""
            def start(self): return True
            def stop(self): return True
            def freeze(self): return True
            def resume(self): return True
            def handle_request(self, r, p): return {}
            def calculate_signals(self, md): return []

        p = BaseNoop("noop")
        p.on_ib_error(42, 201, "Order rejected")  # no exception


# ===========================================================================
# 2d. OrderTestPluginBase fill-event and terminal-state behaviour
# ===========================================================================

from plugins.paper_tests.order_test_base import OrderTestPluginBase, OrderTestCase


class ConcreteOrderTestPlugin(OrderTestPluginBase):
    """Minimal concrete subclass for testing the base class behaviour."""

    TEST_CASES = []

    def __init__(self, **kwargs):
        super().__init__("test_order_plugin", **kwargs)

    @property
    def description(self):
        return "Test order plugin"


class _MockPortfolioForOrderTest:
    """Minimal mock portfolio for OrderTestPluginBase tests."""

    def __init__(self, orders: Dict[int, OrderRecord]):
        self._orders = orders
        self.cancelled = []

    def get_order(self, order_id):
        return self._orders.get(order_id)

    def cancel_order(self, order_id):
        self.cancelled.append(order_id)
        return True

    def place_order_custom(self, contract, order):
        return None

    def place_order_raw(self, order_id, contract, order):
        return False

    def allocate_order_ids(self, count):
        return []


class TestOrderTestPluginFillEvents:
    """on_order_fill / on_order_status set the fill event correctly."""

    def _make_plugin_with_event(self, oid_a, oid_b):
        plugin = ConcreteOrderTestPlugin()
        ev = threading.Event()
        if oid_a is not None:
            plugin._fill_events[oid_a] = ev
        if oid_b is not None:
            plugin._fill_events[oid_b] = ev
        return plugin, ev

    def test_on_order_fill_sets_event(self):
        plugin, ev = self._make_plugin_with_event(10, 11)
        rec = make_order(10, OrderStatus.FILLED)

        plugin.on_order_fill(rec)

        assert ev.is_set()

    def test_on_order_fill_only_matches_registered_id(self):
        plugin, ev = self._make_plugin_with_event(10, 11)
        rec = make_order(99, OrderStatus.FILLED)

        plugin.on_order_fill(rec)

        assert not ev.is_set()

    def test_on_order_status_sets_event_for_terminal_states(self):
        for terminal in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.ERROR):
            plugin, ev = self._make_plugin_with_event(10, 11)
            rec = make_order(10, terminal)

            plugin.on_order_status(rec)

            assert ev.is_set(), f"Event not set for status {terminal}"

    def test_on_order_status_does_not_set_event_for_non_terminal(self):
        for non_terminal in (OrderStatus.SUBMITTED, OrderStatus.PENDING):
            plugin, ev = self._make_plugin_with_event(10, 11)
            rec = make_order(10, non_terminal)

            plugin.on_order_status(rec)

            assert not ev.is_set(), f"Event incorrectly set for status {non_terminal}"


class TestWaitFillCancelOther:
    """_wait_fill_cancel_other correctly handles fills, rejections, and timeout."""

    def _make_plugin(self, orders):
        port = _MockPortfolioForOrderTest(orders)
        plugin = ConcreteOrderTestPlugin(portfolio=port)
        return plugin, port

    def test_returns_long_side_when_long_fills(self):
        orders = {
            1: make_order(1, OrderStatus.FILLED, avg_fill_price=100.0),
            2: make_order(2, OrderStatus.SUBMITTED),
        }
        plugin, port = self._make_plugin(orders)

        side, price = plugin._wait_fill_cancel_other(1, 2, timeout=5.0)

        assert side == "long"
        assert price == 100.0
        assert 2 in port.cancelled

    def test_returns_short_side_when_short_fills(self):
        orders = {
            1: make_order(1, OrderStatus.SUBMITTED),
            2: make_order(2, OrderStatus.FILLED, avg_fill_price=200.0),
        }
        plugin, port = self._make_plugin(orders)

        side, price = plugin._wait_fill_cancel_other(1, 2, timeout=5.0)

        assert side == "short"
        assert price == 200.0
        assert 1 in port.cancelled

    def test_returns_both_when_immediate_and_both_fill(self):
        orders = {
            1: make_order(1, OrderStatus.FILLED, avg_fill_price=100.0),
            2: make_order(2, OrderStatus.FILLED, avg_fill_price=200.0),
        }
        plugin, port = self._make_plugin(orders)

        side, price = plugin._wait_fill_cancel_other(1, 2, timeout=5.0, immediate=True)

        assert side == "both"
        assert price == pytest.approx(150.0)

    def test_exits_early_when_long_order_cancelled(self):
        """Terminal non-fill state on long side → early return without waiting."""
        orders = {
            1: make_order(1, OrderStatus.CANCELLED),
            2: make_order(2, OrderStatus.SUBMITTED),
        }
        plugin, port = self._make_plugin(orders)

        start = time.monotonic()
        side, price = plugin._wait_fill_cancel_other(1, 2, timeout=30.0)
        elapsed = time.monotonic() - start

        assert side is None
        assert price == 0.0
        assert elapsed < 1.0  # must not have waited 30 s
        assert 2 in port.cancelled

    def test_exits_early_when_short_order_rejected(self):
        orders = {
            1: make_order(1, OrderStatus.SUBMITTED),
            2: make_order(2, OrderStatus.ERROR),
        }
        plugin, port = self._make_plugin(orders)

        start = time.monotonic()
        side, price = plugin._wait_fill_cancel_other(1, 2, timeout=30.0)
        elapsed = time.monotonic() - start

        assert side is None
        assert elapsed < 1.0
        assert 1 in port.cancelled

    def test_cancels_both_on_timeout(self):
        """Neither order fills before the timeout."""
        orders = {
            1: make_order(1, OrderStatus.SUBMITTED),
            2: make_order(2, OrderStatus.SUBMITTED),
        }
        plugin, port = self._make_plugin(orders)

        side, price = plugin._wait_fill_cancel_other(1, 2, timeout=0.3)

        assert side is None
        assert 1 in port.cancelled
        assert 2 in port.cancelled

    def test_fill_event_cleaned_up_after_return(self):
        """_fill_events entries are removed when the call completes."""
        orders = {
            1: make_order(1, OrderStatus.FILLED, avg_fill_price=50.0),
            2: make_order(2, OrderStatus.SUBMITTED),
        }
        plugin, port = self._make_plugin(orders)
        plugin._fill_events[1] = threading.Event()
        plugin._fill_events[2] = plugin._fill_events[1]

        plugin._wait_fill_cancel_other(1, 2, timeout=5.0)

        assert 1 not in plugin._fill_events
        assert 2 not in plugin._fill_events

    def test_callback_wakeup_shortens_wait(self):
        """on_order_fill callback wakes the wait before the 0.5 s poll fires."""
        orders = {
            1: make_order(1, OrderStatus.SUBMITTED),
            2: make_order(2, OrderStatus.SUBMITTED),
        }
        plugin, port = self._make_plugin(orders)

        def deliver_fill():
            time.sleep(0.05)  # brief delay then flip the order to filled
            orders[1] = make_order(1, OrderStatus.FILLED, avg_fill_price=99.0)
            plugin.on_order_fill(make_order(1, OrderStatus.FILLED, avg_fill_price=99.0))

        t = threading.Thread(target=deliver_fill, daemon=True)
        t.start()

        start = time.monotonic()
        side, price = plugin._wait_fill_cancel_other(1, 2, timeout=5.0)
        elapsed = time.monotonic() - start

        assert side == "long"
        assert elapsed < 0.4  # woke quickly, did not wait the full 0.5 s interval


class TestOrderTestPluginOnIBError:
    """on_ib_error override logs a warning (does not raise)."""

    def test_on_ib_error_logs_and_does_not_raise(self, caplog):
        import logging
        plugin = ConcreteOrderTestPlugin()

        with caplog.at_level(logging.WARNING, logger="plugins.paper_tests.order_test_base"):
            plugin.on_ib_error(42, 201, "Order rejected")

        assert any("201" in r.message for r in caplog.records)


# ===========================================================================
# 3a. _IB_INFO_CODES constant
# ===========================================================================

class TestIBInfoCodes:
    """_IB_INFO_CODES contains exactly the informational error codes."""

    def test_contains_market_data_farm_codes(self):
        assert 2104 in _IB_INFO_CODES
        assert 2106 in _IB_INFO_CODES
        assert 2158 in _IB_INFO_CODES
        assert 2119 in _IB_INFO_CODES

    def test_contains_delayed_data_notification(self):
        assert 10167 in _IB_INFO_CODES

    def test_real_error_codes_not_in_info_set(self):
        assert 201 not in _IB_INFO_CODES   # order rejected
        assert 354 not in _IB_INFO_CODES   # no data for contract
        assert 504 not in _IB_INFO_CODES   # not connected


# ===========================================================================
# 3b. StreamManager.plugins_for_symbol
# ===========================================================================

class TestStreamManagerPluginsForSymbol:
    def test_returns_subscribed_plugins(self):
        sm = StreamManager(data_feed=None)
        sm._plugin_streams["alpha"] = {"SPY": Mock()}
        sm._plugin_streams["beta"] = {"SPY": Mock(), "QQQ": Mock()}

        result = sm.plugins_for_symbol("SPY")

        assert set(result) == {"alpha", "beta"}

    def test_excludes_plugins_not_subscribed_to_symbol(self):
        sm = StreamManager(data_feed=None)
        sm._plugin_streams["alpha"] = {"QQQ": Mock()}

        result = sm.plugins_for_symbol("SPY")

        assert result == []

    def test_empty_stream_map_returns_empty(self):
        sm = StreamManager(data_feed=None)
        assert sm.plugins_for_symbol("SPY") == []

    def test_multiple_plugins_single_symbol(self):
        sm = StreamManager(data_feed=None)
        for name in ("p1", "p2", "p3"):
            sm._plugin_streams[name] = {"SPY": Mock()}

        result = sm.plugins_for_symbol("SPY")
        assert set(result) == {"p1", "p2", "p3"}


# ===========================================================================
# 3c. PluginExecutive._handle_ib_error_for_plugins routing
# ===========================================================================

class TestHandleIBErrorForPlugins:

    def _exec_with_plugin(self, plugin):
        ex = make_executive()
        ex.register_plugin(plugin)
        return ex

    # --- filtering ---

    def test_system_message_req_id_minus1_not_routed(self):
        plugin = ConcretePlugin()
        ex = self._exec_with_plugin(plugin)
        ex.register_order_for_plugin(1, plugin.name)

        ex._handle_ib_error_for_plugins(-1, 504, "Not connected")

        assert plugin.error_calls == []

    def test_info_codes_not_routed(self):
        plugin = ConcretePlugin()
        ex = self._exec_with_plugin(plugin)
        ex.register_order_for_plugin(1, plugin.name)

        for code in list(_IB_INFO_CODES):
            ex._handle_ib_error_for_plugins(1, code, "info")

        assert plugin.error_calls == []

    # --- order routing ---

    def test_order_error_routed_by_order_id(self):
        plugin = ConcretePlugin()
        ex = self._exec_with_plugin(plugin)
        ex.register_order_for_plugin(42, plugin.name)

        ex._handle_ib_error_for_plugins(42, 201, "Order rejected")

        assert plugin.error_calls == [(42, 201, "Order rejected")]

    def test_order_error_not_routed_for_unregistered_id(self):
        plugin = ConcretePlugin()
        ex = self._exec_with_plugin(plugin)

        ex._handle_ib_error_for_plugins(999, 201, "Order rejected")

        assert plugin.error_calls == []

    def test_order_error_routed_to_multiple_plugins(self):
        pa = ConcretePlugin("pa")
        pb = ConcretePlugin("pb")
        ex = make_executive()
        ex.register_plugin(pa)
        ex.register_plugin(pb)
        ex.register_order_for_plugin(50, "pa")
        ex.register_order_for_plugin(50, "pb")

        ex._handle_ib_error_for_plugins(50, 201, "Rejected")

        assert len(pa.error_calls) == 1
        assert len(pb.error_calls) == 1

    # --- stream routing ---

    def test_tick_stream_error_routed_via_stream_subscriptions(self):
        plugin = ConcretePlugin()
        ex = self._exec_with_plugin(plugin)

        portfolio = Mock()
        portfolio._stream_subscriptions = {77: "SPY"}
        portfolio._bar_subscriptions = {}
        ex.portfolio = portfolio
        ex.stream_manager._plugin_streams[plugin.name] = {"SPY": Mock()}

        ex._handle_ib_error_for_plugins(77, 354, "No data for SPY")

        assert plugin.error_calls == [(77, 354, "No data for SPY")]

    def test_bar_stream_error_routed_via_bar_subscriptions(self):
        plugin = ConcretePlugin()
        ex = self._exec_with_plugin(plugin)

        portfolio = Mock()
        portfolio._stream_subscriptions = {}
        portfolio._bar_subscriptions = {88: "QQQ"}
        ex.portfolio = portfolio
        ex.stream_manager._plugin_streams[plugin.name] = {"QQQ": Mock()}

        ex._handle_ib_error_for_plugins(88, 354, "No data for QQQ")

        assert len(plugin.error_calls) == 1

    def test_stream_error_not_routed_when_no_plugin_subscribed(self):
        plugin = ConcretePlugin()
        ex = self._exec_with_plugin(plugin)

        portfolio = Mock()
        portfolio._stream_subscriptions = {77: "SPY"}
        portfolio._bar_subscriptions = {}
        ex.portfolio = portfolio
        # plugin is NOT subscribed to SPY in stream_manager

        ex._handle_ib_error_for_plugins(77, 354, "No data")

        assert plugin.error_calls == []

    def test_order_routing_takes_priority_over_stream(self):
        """If req_id is an order, stream routing is not attempted."""
        order_plugin = ConcretePlugin("order_p")
        stream_plugin = ConcretePlugin("stream_p")

        ex = make_executive()
        ex.register_plugin(order_plugin)
        ex.register_plugin(stream_plugin)
        ex.register_order_for_plugin(55, "order_p")

        portfolio = Mock()
        portfolio._stream_subscriptions = {55: "SPY"}
        portfolio._bar_subscriptions = {}
        ex.portfolio = portfolio
        ex.stream_manager._plugin_streams["stream_p"] = {"SPY": Mock()}

        ex._handle_ib_error_for_plugins(55, 201, "Order rejected")

        assert len(order_plugin.error_calls) == 1
        assert stream_plugin.error_calls == []

    def test_plugin_exception_does_not_stop_others(self):
        class BadPlugin(ConcretePlugin):
            def on_ib_error(self, req_id, error_code, error_string):
                raise RuntimeError("boom")

        bad = BadPlugin("bad")
        good = ConcretePlugin("good")

        ex = make_executive()
        ex.register_plugin(bad)
        ex.register_plugin(good)
        ex.register_order_for_plugin(1, "bad")
        ex.register_order_for_plugin(1, "good")

        ex._handle_ib_error_for_plugins(1, 201, "Rejected")  # must not raise

        assert len(good.error_calls) == 1

    def test_error_callback_registered_on_portfolio_at_init(self):
        portfolio = Mock()
        portfolio._callbacks = {}
        portfolio._on_commission = None

        ex = PluginExecutive(portfolio=portfolio, data_feed=None)

        assert portfolio._callbacks["error"] == ex._handle_ib_error_for_plugins
