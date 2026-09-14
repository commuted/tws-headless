"""
Unit tests for GldUsdSwapPlugin.on_order_fill and on_order_status.

Covers:
  - BUY fill  → _holding_gld=True, _overnight_holds+1, state persisted
  - SELL fill → _holding_gld=False, state persisted
  - Unknown order ID → safe no-op in both callbacks
  - Terminal order status (CANCELLED/INACTIVE/ERROR) → pending action
    popped, _holding_gld left unchanged
  - Non-terminal status → pending action left in place, state unchanged
"""

import json
from pathlib import Path

import pytest

from ib.models import OrderRecord, OrderStatus
from plugins.gld_usd_swap.plugin import GldUsdSwapPlugin


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_plugin(tmp_path: Path) -> GldUsdSwapPlugin:
    """Offline plugin instance — no portfolio, no IB connection."""
    return GldUsdSwapPlugin(base_path=tmp_path)


def _order(order_id: int, action: str, status: OrderStatus,
           filled_quantity: float = 100.0,
           avg_fill_price: float = 225.50) -> OrderRecord:
    return OrderRecord(
        order_id=order_id,
        symbol="GLD",
        action=action,
        quantity=filled_quantity,
        status=status,
        filled_quantity=filled_quantity,
        avg_fill_price=avg_fill_price,
    )


def _saved_state(tmp_path: Path) -> dict:
    return json.loads((tmp_path / "state.json").read_text())["state"]


# ---------------------------------------------------------------------------
# Order field completeness
# ---------------------------------------------------------------------------

class _CapturingPortfolio:
    """Stands in for Portfolio.place_order_custom, keeping the Order object."""

    def __init__(self):
        self.orders = []

    def place_order_custom(self, contract, order):
        self.orders.append(order)
        return 100 + len(self.orders)


class TestOrderTimeInForce:
    """Every order this plugin builds must carry a time in force.

    ibapi's Order() leaves tif empty and Gateway rejects that with error 10052
    ("Invalid time in force:Empty") at local validation — no orderStatus
    callback follows, so the order looks pending forever. Commit 1c01e41 set
    tif on the three MKT builders and missed all three MOC builders, which is
    how a live MOC re-entry was silently rejected on 2026-09-14.
    """

    def _plugin(self, tmp_path):
        p = _make_plugin(tmp_path)
        p.portfolio = _CapturingPortfolio()
        p._gld_price = 393.5
        p._gll_price = 40.0
        return p

    def test_moc_buy_has_day_tif(self, tmp_path):
        p = self._plugin(tmp_path)
        p._place_moc_buy(25, "test")
        order = p.portfolio.orders[-1]
        assert order.orderType == "MOC"
        assert order.tif == "DAY"

    def test_moc_short_has_day_tif(self, tmp_path):
        p = self._plugin(tmp_path)
        p._place_moc_short(25, "test")
        order = p.portfolio.orders[-1]
        assert order.orderType == "MOC"
        assert order.tif == "DAY"

    def test_mkt_sell_has_day_tif(self, tmp_path):
        p = self._plugin(tmp_path)
        p._emit_sell(25, "test")
        order = p.portfolio.orders[-1]
        assert order.orderType == "MKT"
        assert order.tif == "DAY"


class TestTerminalRejectCodes:
    """The plugin shares the engine's placement-reject list so the two can
    never disagree about which errors end an order's life."""

    def test_shares_the_engine_list(self):
        from ib.models import TERMINAL_ORDER_REJECT_CODES
        from plugins.gld_usd_swap.plugin import _TERMINAL_REJECT_CODES
        assert TERMINAL_ORDER_REJECT_CODES <= _TERMINAL_REJECT_CODES

    def test_covers_both_live_rejection_modes(self):
        from plugins.gld_usd_swap.plugin import _TERMINAL_REJECT_CODES
        assert 435 in _TERMINAL_REJECT_CODES      # missing account
        assert 10052 in _TERMINAL_REJECT_CODES    # empty time in force


# ---------------------------------------------------------------------------
# on_order_fill
# ---------------------------------------------------------------------------

class TestOnOrderFill:
    def test_buy_fill_sets_holding_gld(self, tmp_path):
        p = _make_plugin(tmp_path)
        p._pending_order_actions[42] = "BUY"
        p.on_order_fill(_order(42, "BUY", OrderStatus.FILLED))
        assert p._holding_gld is True

    def test_buy_fill_increments_overnight_holds(self, tmp_path):
        p = _make_plugin(tmp_path)
        p._pending_order_actions[42] = "BUY"
        p.on_order_fill(_order(42, "BUY", OrderStatus.FILLED))
        assert p._overnight_holds == 1

    def test_buy_fill_does_not_touch_intraday_holds(self, tmp_path):
        p = _make_plugin(tmp_path)
        p._pending_order_actions[42] = "BUY"
        p.on_order_fill(_order(42, "BUY", OrderStatus.FILLED))
        assert p._intraday_holds == 0

    def test_sell_fill_clears_holding_gld(self, tmp_path):
        p = _make_plugin(tmp_path)
        p._holding_gld = True
        p._pending_order_actions[99] = "SELL"
        p.on_order_fill(_order(99, "SELL", OrderStatus.FILLED))
        assert p._holding_gld is False

    def test_sell_fill_does_not_increment_overnight_holds(self, tmp_path):
        p = _make_plugin(tmp_path)
        p._holding_gld = True
        p._pending_order_actions[99] = "SELL"
        p.on_order_fill(_order(99, "SELL", OrderStatus.FILLED))
        assert p._overnight_holds == 0

    def test_fill_removes_order_from_pending(self, tmp_path):
        p = _make_plugin(tmp_path)
        p._pending_order_actions[7] = "BUY"
        p.on_order_fill(_order(7, "BUY", OrderStatus.FILLED))
        assert 7 not in p._pending_order_actions

    def test_buy_fill_persists_state(self, tmp_path):
        p = _make_plugin(tmp_path)
        p._pending_order_actions[1] = "BUY"
        p.on_order_fill(_order(1, "BUY", OrderStatus.FILLED))
        state = _saved_state(tmp_path)
        assert state["holding_gld"] is True
        assert state["overnight_holds"] == 1

    def test_sell_fill_persists_state(self, tmp_path):
        p = _make_plugin(tmp_path)
        p._holding_gld = True
        p._pending_order_actions[2] = "SELL"
        p.on_order_fill(_order(2, "SELL", OrderStatus.FILLED))
        state = _saved_state(tmp_path)
        assert state["holding_gld"] is False

    def test_unknown_order_id_is_noop(self, tmp_path):
        p = _make_plugin(tmp_path)
        p._holding_gld = False
        p._overnight_holds = 0
        p.on_order_fill(_order(999, "BUY", OrderStatus.FILLED))
        assert p._holding_gld is False
        assert p._overnight_holds == 0
        assert not (tmp_path / "state.json").exists()


# ---------------------------------------------------------------------------
# on_order_status
# ---------------------------------------------------------------------------

TERMINAL_STATUSES = [OrderStatus.CANCELLED, OrderStatus.INACTIVE, OrderStatus.ERROR]
NON_TERMINAL_STATUSES = [OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED]


class TestOnOrderStatus:
    @pytest.mark.parametrize("status", TERMINAL_STATUSES)
    def test_terminal_status_pops_pending_action(self, tmp_path, status):
        p = _make_plugin(tmp_path)
        p._pending_order_actions[10] = "BUY"
        p.on_order_status(_order(10, "BUY", status))
        assert 10 not in p._pending_order_actions

    @pytest.mark.parametrize("status", TERMINAL_STATUSES)
    def test_buy_terminal_leaves_holding_gld_false(self, tmp_path, status):
        p = _make_plugin(tmp_path)
        p._holding_gld = False
        p._pending_order_actions[11] = "BUY"
        p.on_order_status(_order(11, "BUY", status))
        assert p._holding_gld is False

    @pytest.mark.parametrize("status", TERMINAL_STATUSES)
    def test_sell_terminal_leaves_holding_gld_true(self, tmp_path, status):
        p = _make_plugin(tmp_path)
        p._holding_gld = True
        p._pending_order_actions[12] = "SELL"
        p.on_order_status(_order(12, "SELL", status))
        assert p._holding_gld is True

    @pytest.mark.parametrize("status", NON_TERMINAL_STATUSES)
    def test_non_terminal_status_leaves_pending_action(self, tmp_path, status):
        p = _make_plugin(tmp_path)
        p._pending_order_actions[20] = "BUY"
        p.on_order_status(_order(20, "BUY", status))
        assert 20 in p._pending_order_actions

    @pytest.mark.parametrize("status", NON_TERMINAL_STATUSES)
    def test_non_terminal_status_does_not_change_holding_gld(self, tmp_path, status):
        p = _make_plugin(tmp_path)
        p._holding_gld = True
        p._pending_order_actions[21] = "SELL"
        p.on_order_status(_order(21, "SELL", status))
        assert p._holding_gld is True

    def test_unknown_order_id_is_noop(self, tmp_path):
        p = _make_plugin(tmp_path)
        p._holding_gld = True
        p.on_order_status(_order(999, "BUY", OrderStatus.CANCELLED))
        assert p._holding_gld is True
