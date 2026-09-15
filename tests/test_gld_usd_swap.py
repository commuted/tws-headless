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
from unittest.mock import MagicMock, patch

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
# Failed orders: clear the tracker, keep the shares, resync the flag
# ---------------------------------------------------------------------------

def _with_holdings(p, gld_shares: float = 0.0, cash: float = 10_000.0):
    """Give a plugin a real Holdings ledger. Without one every share accessor
    reports 0, which is 'unreadable', not 'flat'."""
    from plugins.base import Holdings
    p._holdings = Holdings(plugin_name=p.name, current_cash=cash)
    if gld_shares:
        p._holdings.add_position("GLD", gld_shares,
                                 cost_basis=390.0, current_price=393.0)
    return p


class TestResyncHoldingFlag:
    def test_shares_present_means_holding(self, tmp_path):
        p = _with_holdings(_make_plugin(tmp_path), gld_shares=50)
        p._holding_gld = False
        p._resync_holding_flag()
        assert p._holding_gld is True

    def test_no_shares_means_flat(self, tmp_path):
        """The 2026-09-14 case: a rejected BUY had left the flag True with an
        empty ledger, which would have made the next close skip its entry."""
        p = _with_holdings(_make_plugin(tmp_path), gld_shares=0)
        p._holding_gld = True
        p._resync_holding_flag()
        assert p._holding_gld is False

    def test_unreadable_ledger_never_flips_the_flag(self, tmp_path):
        """Holdings unloaded reports 0 shares; treating that as flat would
        place a duplicate entry at the next close."""
        p = _make_plugin(tmp_path)
        assert p.holdings is None
        p._holding_gld = True
        p._resync_holding_flag()
        assert p._holding_gld is True


class TestFailedSellKeepsShares:
    def test_rejected_sell_clears_tracker_and_keeps_holding(self, tmp_path):
        p = _with_holdings(_make_plugin(tmp_path), gld_shares=50)
        p._holding_gld = True
        p._pending_order_actions[7] = "SELL"
        p._pending_order_placed_at[7] = 0.0
        p._pending_order_types[7] = "MKT"

        p.on_ib_error(7, 10052, "Invalid time in force:Empty")

        assert 7 not in p._pending_order_actions
        assert 7 not in p._pending_order_types
        assert p._holding_gld is True
        assert p._current_gld_shares() == 50     # unsold shares still held

    def test_rejected_buy_leaves_the_plugin_flat(self, tmp_path):
        p = _with_holdings(_make_plugin(tmp_path), gld_shares=0)
        p._holding_gld = True                    # as the restore path had left it
        p._pending_order_actions[8] = "BUY"
        p._pending_order_placed_at[8] = 0.0
        p._pending_order_types[8] = "MOC"

        p.on_ib_error(8, 10052, "Invalid time in force:Empty")

        assert 8 not in p._pending_order_actions
        assert p._holding_gld is False

    def test_next_sell_is_sized_from_the_remaining_shares(self, tmp_path):
        """A sell that failed leaves the shares in holdings, and the next sell
        sizes off holdings — so the remainder is what goes out."""
        p = _with_holdings(_make_plugin(tmp_path), gld_shares=50)
        p.portfolio = _CapturingPortfolio()
        p._gld_price = 393.0
        p._emit_sell(p._current_gld_shares(), "retry after a failed sell")
        assert p.portfolio.orders[-1].totalQuantity == 50

    def test_partial_fill_leaves_only_the_remainder_to_sell(self, tmp_path):
        p = _with_holdings(_make_plugin(tmp_path), gld_shares=50)
        p._holdings.remove_position("GLD", 42)   # 42 of 50 filled
        assert p._current_gld_shares() == 8


class TestPendingTrackerIsFullyCleared:
    """Every exit from the tracker must drop all three parallel dicts, or the
    type map leaks entries that outlive their orders and get persisted."""

    def _pending(self, tmp_path, oid=31, action="SELL", order_type="MKT"):
        p = _with_holdings(_make_plugin(tmp_path), gld_shares=50)
        p._pending_order_actions[oid] = action
        p._pending_order_placed_at[oid] = 0.0
        p._pending_order_types[oid] = order_type
        return p

    def _assert_gone(self, p, oid=31):
        assert oid not in p._pending_order_actions
        assert oid not in p._pending_order_placed_at
        assert oid not in p._pending_order_types
        assert oid not in p._pending_alerted

    def test_fill_clears_every_tracker(self, tmp_path):
        p = self._pending(tmp_path)
        p.on_order_fill(_order(31, "SELL", OrderStatus.FILLED,
                               filled_quantity=50.0, avg_fill_price=393.0))
        self._assert_gone(p)

    @pytest.mark.parametrize(
        "status", [OrderStatus.CANCELLED, OrderStatus.INACTIVE, OrderStatus.ERROR]
    )
    def test_terminal_status_clears_every_tracker(self, tmp_path, status):
        p = self._pending(tmp_path)
        p.on_order_status(_order(31, "SELL", status))
        self._assert_gone(p)

    def test_reject_clears_every_tracker(self, tmp_path):
        p = self._pending(tmp_path)
        p.on_ib_error(31, 10052, "Invalid time in force:Empty")
        self._assert_gone(p)


class TestPendingOrderExpiry:
    """A dead order has to leave the tracker, but only once it genuinely
    cannot fill — and IB has to be told before the plugin forgets it."""

    def _pending(self, tmp_path, order_type, placed_et, action="SELL"):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        p = _with_holdings(_make_plugin(tmp_path), gld_shares=50)
        p._holding_gld = True
        p.portfolio = MagicMock()
        p.portfolio.cancel_order.return_value = True
        p._pending_order_actions[9] = action
        p._pending_order_types[9] = order_type
        p._pending_order_placed_at[9] = placed_et.timestamp()
        return p

    def _at(self, hh, mm):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        return datetime(2026, 9, 14, hh, mm, tzinfo=ZoneInfo("America/New_York"))

    def _run_at(self, p, hh, mm):
        """Run the sweep as if the ET wall clock read hh:mm. Only now() is
        stubbed; fromtimestamp passes through to the real datetime."""
        import plugins.gld_usd_swap.plugin as mod
        real_datetime = mod.datetime
        with patch.object(mod, "datetime", wraps=real_datetime) as dt:
            dt.now.return_value = self._at(hh, mm)
            p._expire_stale_pending_orders()

    def test_mkt_sell_survives_inside_its_window(self, tmp_path):
        p = self._pending(tmp_path, "MKT", self._at(9, 31))
        self._run_at(p, 9, 40)
        assert 9 in p._pending_order_actions
        p.portfolio.cancel_order.assert_not_called()

    def test_mkt_sell_expires_once_the_window_has_passed(self, tmp_path):
        p = self._pending(tmp_path, "MKT", self._at(9, 31))
        self._run_at(p, 10, 5)
        assert 9 not in p._pending_order_actions
        assert 9 not in p._pending_order_types
        p.portfolio.cancel_order.assert_called_once_with(9)

    def test_expiry_keeps_the_unsold_shares_and_the_hold_flag(self, tmp_path):
        p = self._pending(tmp_path, "MKT", self._at(9, 31))
        self._run_at(p, 10, 5)
        assert p._current_gld_shares() == 50
        assert p._holding_gld is True

    def test_moc_is_not_expired_before_the_closing_auction(self, tmp_path):
        """The auction prints at 16:00 — expiring at 15:55 would cancel a
        live, working order."""
        p = self._pending(tmp_path, "MOC", self._at(15, 45), action="BUY")
        self._run_at(p, 15, 58)
        assert 9 in p._pending_order_actions
        p.portfolio.cancel_order.assert_not_called()

    def test_moc_expires_after_the_auction_has_printed(self, tmp_path):
        p = self._pending(tmp_path, "MOC", self._at(15, 45), action="BUY")
        self._run_at(p, 16, 20)
        assert 9 not in p._pending_order_actions
        p.portfolio.cancel_order.assert_called_once_with(9)

    def test_unknown_order_type_takes_the_later_deadline(self, tmp_path):
        """State files written before order types were tracked have none;
        an unknown type must never expire an order that might still work."""
        p = self._pending(tmp_path, "MKT", self._at(15, 45))
        del p._pending_order_types[9]
        self._run_at(p, 15, 58)
        assert 9 in p._pending_order_actions


# ---------------------------------------------------------------------------
# Order field completeness
# ---------------------------------------------------------------------------

class _CapturingPortfolio:
    """Stands in for Portfolio.place_order_custom, keeping the Order object."""

    def __init__(self):
        self.orders = []
        self.orders_ids = []

    def place_order_custom(self, contract, order):
        self.orders.append(order)
        self.orders_ids.append(100 + len(self.orders))
        return self.orders_ids[-1]


class TestDeployableCapital:
    """Size follows the capital the plugin holds, not a constant.

    A fixed dollar figure ratchets, and only downward: a losing plugin is
    held down by its shrunken cash while a winning one has its gains
    stranded as idle cash. It also ignored funding — transferring shares in
    raised holdings but not the amount the plugin would trade, which is how
    a 50-share seed came to re-enter at 25 on 2026-09-14.
    """

    def _funded(self, tmp_path, cash=0.0, shares=0.0, price=400.0):
        p = _with_holdings(_make_plugin(tmp_path), gld_shares=shares, cash=cash)
        p.portfolio = _CapturingPortfolio()
        p._gld_price = price
        return p

    def test_uncapped_deploys_everything_it_holds(self, tmp_path):
        p = self._funded(tmp_path, cash=19_592.27)
        p.allocation_dollars = 0.0
        assert p._deployable_capital() == pytest.approx(19_592.27)

    def test_gains_raise_the_size_rather_than_idling(self, tmp_path):
        """The asymmetry that mattered: profit used to sit as dead cash."""
        p = self._funded(tmp_path, cash=25_000.0)
        p.allocation_dollars = 0.0
        assert p._deployable_capital() == pytest.approx(25_000.0)

    def test_transferred_shares_count_toward_size(self, tmp_path):
        """Funding a plugin with stock, not cash, must raise what it trades."""
        p = self._funded(tmp_path, cash=0.0, shares=50, price=400.0)
        p.allocation_dollars = 0.0
        assert p._deployable_capital() == pytest.approx(20_000.0)

    def test_positive_allocation_is_only_a_ceiling(self, tmp_path):
        p = self._funded(tmp_path, cash=19_592.27)
        p.allocation_dollars = 10_000.0
        assert p._deployable_capital() == pytest.approx(10_000.0)

    def test_ceiling_does_not_invent_capital_it_lacks(self, tmp_path):
        p = self._funded(tmp_path, cash=5_000.0)
        p.allocation_dollars = 50_000.0
        assert p._deployable_capital() == pytest.approx(5_000.0)

    def test_unfunded_plugin_deploys_nothing(self, tmp_path):
        p = self._funded(tmp_path, cash=0.0)
        p.allocation_dollars = 0.0
        assert p._deployable_capital() == 0.0

    def test_offline_falls_back_to_the_constant(self, tmp_path):
        """No portfolio means no NAV to read — backtests still need a size."""
        p = _make_plugin(tmp_path)
        p.allocation_dollars = 10_000.0
        assert p.portfolio is None
        assert p._deployable_capital() == pytest.approx(10_000.0)

    def test_default_is_uncapped(self, tmp_path):
        assert _make_plugin(tmp_path).allocation_dollars == 0.0

    def test_moc_entry_sizes_off_holdings(self, tmp_path):
        """End to end: the cash a sale realised is what the re-entry spends."""
        p = self._funded(tmp_path, cash=19_592.27, price=393.43)
        p.allocation_dollars = 0.0
        p._place_moc_buy(int(p._deployable_capital() / p._gld_price), "test")
        assert p.portfolio.orders[-1].totalQuantity == 49


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

    # Every order-placing method, with the symbol/type/action it must build.
    # Enumerated rather than sampled: the live bug was one family of builders
    # being fixed and the other silently left behind.
    BUILDERS = [
        ("_place_moc_buy",   "GLD", "MOC", "BUY",  "BUY"),
        ("_emit_sell",       "GLD", "MKT", "SELL", "SELL"),
        ("_place_moc_short", "GLD", "MOC", "SELL", "SHORT_OPEN"),
        ("_place_cover_buy", "GLD", "MKT", "BUY",  "SHORT_COVER"),
        ("_place_gll_buy",   "GLL", "MOC", "BUY",  "GLL_OPEN"),
        ("_place_gll_sell",  "GLL", "MKT", "SELL", "GLL_CLOSE"),
    ]

    @pytest.mark.parametrize(
        "method,symbol,order_type,action,pending_action", BUILDERS,
        ids=[b[0] for b in BUILDERS],
    )
    def test_every_builder_sets_day_tif(self, tmp_path, method, symbol,
                                        order_type, action, pending_action):
        p = self._plugin(tmp_path)
        getattr(p, method)(25, "test")
        order = p.portfolio.orders[-1]
        assert order.orderType == order_type
        assert order.action == action
        assert order.tif == "DAY", f"{method} would be rejected with 10052"

    @pytest.mark.parametrize(
        "method,symbol,order_type,action,pending_action", BUILDERS,
        ids=[b[0] for b in BUILDERS],
    )
    def test_every_builder_records_its_order_type(self, tmp_path, method, symbol,
                                                  order_type, action, pending_action):
        """The expiry deadline is chosen by order type, so a builder that
        forgets to record it would get the conservative MOC deadline and
        linger past its window."""
        p = self._plugin(tmp_path)
        getattr(p, method)(25, "test")
        oid = p.portfolio.orders_ids[-1]
        assert p._pending_order_types[oid] == order_type
        assert p._pending_order_actions[oid] == pending_action


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
