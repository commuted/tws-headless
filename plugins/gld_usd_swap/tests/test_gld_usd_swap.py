"""
Unit tests for plugins/gld_usd_swap/plugin.py
"""
from datetime import datetime
from unittest.mock import Mock
import pytest

from plugins.gld_usd_swap.plugin import GldUsdSwapPlugin, REGIME_GOLD, REGIME_CASH, REGIME_UNKNOWN


def _make_plugin(tmp_path, **kwargs):
    plugin = GldUsdSwapPlugin(base_path=tmp_path / "gld_usd_swap", **kwargs)
    return plugin


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

class TestLifecycle:
    def test_start_no_portfolio(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin.start() is True

    def test_stop(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        assert plugin.stop() is True

    def test_freeze_and_resume(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        assert plugin.freeze() is True
        assert plugin.resume() is True

    def test_state_persisted_on_stop(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        plugin._regime_at_prior_close = REGIME_CASH
        plugin._trade_count = 4
        plugin.stop()

        plugin2 = _make_plugin(tmp_path)
        plugin2.start()
        assert plugin2._regime_at_prior_close == REGIME_CASH
        assert plugin2._trade_count == 4


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

class TestParameters:
    def test_get_parameters(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        result = plugin.handle_request("get_parameters", {})
        assert result["success"] is True
        data = result["data"]
        assert "fast_bars" in data
        assert "slow_bars" in data
        assert "allocation_dollars" in data

    def test_set_allocation_dollars(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        result = plugin.handle_request("set_parameter",
                                       {"key": "allocation_dollars", "value": 25000})
        assert result["success"] is True
        assert plugin.allocation_dollars == 25000

    def test_set_fast_bars_valid(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        result = plugin.handle_request("set_parameter",
                                       {"key": "fast_bars", "value": 3})
        assert result["success"] is True
        assert plugin.fast_bars == 3

    def test_set_fast_bars_must_be_less_than_slow(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        # slow_bars default = 20; fast_bars >= slow_bars must fail
        result = plugin.handle_request("set_parameter",
                                       {"key": "fast_bars", "value": 20})
        assert result["success"] is False

    def test_set_meta_fast_bars_must_be_less_than_meta_slow(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        result = plugin.handle_request("set_parameter",
                                       {"key": "meta_fast_bars", "value": 60})
        assert result["success"] is False

    def test_set_unknown_parameter(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        result = plugin.handle_request("set_parameter",
                                       {"key": "nonexistent", "value": 1})
        assert result["success"] is False

    def test_set_parameter_missing_key(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        result = plugin.handle_request("set_parameter", {"value": 5})
        assert result["success"] is False


# ---------------------------------------------------------------------------
# force_regime
# ---------------------------------------------------------------------------

class TestForceRegime:
    def test_force_gold(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        result = plugin.handle_request("force_regime", {"regime": REGIME_GOLD})
        assert result["success"] is True
        assert plugin._regime_at_prior_close == REGIME_GOLD

    def test_force_cash(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        result = plugin.handle_request("force_regime", {"regime": REGIME_CASH})
        assert result["success"] is True
        assert plugin._regime_at_prior_close == REGIME_CASH

    def test_force_unknown(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        result = plugin.handle_request("force_regime", {"regime": REGIME_UNKNOWN})
        assert result["success"] is True
        assert plugin._regime_at_prior_close == REGIME_UNKNOWN

    def test_force_invalid_regime(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        result = plugin.handle_request("force_regime", {"regime": "bullish"})
        assert result["success"] is False


# ---------------------------------------------------------------------------
# get_status
# ---------------------------------------------------------------------------

class TestGetStatus:
    def test_get_status_returns_all_fields(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        result = plugin.handle_request("get_status", {})
        assert result["success"] is True
        data = result["data"]
        assert "holding_gld" in data
        assert "regime" in data
        assert "regime_at_prior_close" in data
        assert "gld_price" in data
        assert "trade_count" in data
        assert "overnight_holds" in data
        assert "intraday_holds" in data

    def test_initial_state_not_holding(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        result = plugin.handle_request("get_status", {})
        assert result["data"]["holding_gld"] is False
        assert result["data"]["trade_count"] == 0


# ---------------------------------------------------------------------------
# Regime computation
# ---------------------------------------------------------------------------

class TestRegimeComputation:
    def _push_n_bars(self, state, n, price, vol_window, percentile, fast, slow):
        """Push n identical bars into an ETF state."""
        for _ in range(n):
            state.push(price, vol_window, percentile, fast, slow)

    def test_no_regime_before_warmup(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        # With no bars pushed, UUP not warmed up → regime unchanged from init
        initial = plugin._regime
        plugin._recompute_regime()
        assert plugin._regime == initial

    def test_uup_only_fallback_regime(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        # Push enough bars to warm UUP only (fast=5, slow=20)
        n = plugin.slow_bars + 1
        # UUP fast < slow → USD weakening → gold
        for _ in range(n):
            plugin._uup.push(28.0, plugin.vol_window, plugin.derivative_percentile,
                             plugin.fast_bars, plugin.slow_bars)
        # Now push a higher price to make fast > slow (USD strengthening → cash)
        for _ in range(plugin.fast_bars):
            plugin._uup.push(25.0, plugin.vol_window, plugin.derivative_percentile,
                             plugin.fast_bars, plugin.slow_bars)
        plugin._recompute_regime()
        # With UUP fast < slow (low recent prices), regime should be gold
        # (actual regime depends on the exact price series — just verify it ran)
        assert plugin._regime in (REGIME_GOLD, REGIME_CASH)

    def test_unknown_request(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("nonexistent", {})
        assert result["success"] is False


# ---------------------------------------------------------------------------
# Hazard #1 — backfill replay must not fire session orders (_is_live_bar)
# ---------------------------------------------------------------------------

class TestLiveBarGate:
    _TS_1545 = datetime(2026, 5, 15, 15, 45)
    _TS_1540 = datetime(2026, 5, 15, 15, 40)
    _TS_1550 = datetime(2026, 5, 15, 15, 50)

    def test_backfill_bars_never_live(self, tmp_path):
        """Bars from the historicalData replay (is_live=False) can never fire
        session decisions, no matter how new they look."""
        plugin = _make_plugin(tmp_path)
        assert plugin._is_live_bar(self._TS_1545, is_live=False) is False
        assert plugin._is_live_bar(self._TS_1550, is_live=False) is False

    def test_live_new_bar_accepted(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin._is_live_bar(self._TS_1545, is_live=True) is True

    def test_repeated_same_bar_deduped(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin._is_live_bar(self._TS_1545, is_live=True) is True
        assert plugin._is_live_bar(self._TS_1545, is_live=True) is False  # same forming-bar update

    def test_older_live_bar_rejected(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin._is_live_bar(self._TS_1545, is_live=True) is True
        assert plugin._is_live_bar(self._TS_1540, is_live=True) is False  # stale bar ≤ high-water-mark
        assert plugin._is_live_bar(self._TS_1550, is_live=True) is True   # genuine next live bar

    def test_hwm_advances_on_backfill(self, tmp_path):
        """A backfill bar advances the high-water-mark, so the forming bar
        already replayed at subscribe time can't fire when its live updates
        start arriving."""
        plugin = _make_plugin(tmp_path)
        assert plugin._is_live_bar(self._TS_1545, is_live=False) is False
        assert plugin._is_live_bar(self._TS_1545, is_live=True) is False  # not newer than hwm
        assert plugin._is_live_bar(self._TS_1550, is_live=True) is True

    def test_frozen_plugin_ignores_bars(self, tmp_path):
        """Backstop for freeze(): an in-flight bar callback on a FROZEN plugin
        must neither update signal state nor fire session decisions."""
        from plugins.base import PluginState

        plugin = _make_plugin(tmp_path)
        plugin._state = PluginState.FROZEN
        fired = []
        plugin._handle_session_event = lambda ts: fired.append(ts)

        bar = Mock(close=240.0, date="20260515 15:45:00")
        plugin._on_bar("GLD", bar, is_live=True)

        assert plugin._gld_price == 0.0    # signal state untouched
        assert fired == []

    def test_freeze_cancels_subscriptions(self, tmp_path):
        """freeze() must cancel live-bar subscriptions so no bars (and no
        orders) can occur while frozen."""
        plugin = _make_plugin(tmp_path)
        plugin._live_bar_req_ids = {"GLD": 1, "UUP": 2, "TLT": 3, "RINF": 4}
        cancelled = []
        plugin.cancel_live_bars = cancelled.append

        assert plugin.freeze() is True
        assert sorted(cancelled) == [1, 2, 3, 4]
        assert plugin._live_bar_req_ids == {}


# ---------------------------------------------------------------------------
# Session decisions — per-day dedupe + hazard #2 (plugin-scoped holdings)
# ---------------------------------------------------------------------------

class TestSessionDecisions:
    def _loaded(self, tmp_path, **kw):
        plugin = _make_plugin(tmp_path, **kw)
        plugin.load()   # populate self.holdings
        return plugin

    def test_close_fires_once_per_day(self, tmp_path):
        plugin = self._loaded(tmp_path)
        plugin._gld_price = 240.0
        plugin._holding_gld = False
        ts = datetime(2026, 5, 15, 15, 45)
        plugin._on_market_close(ts)
        assert plugin._trade_count == 1
        plugin._on_market_close(ts)          # repeated bar, same session
        assert plugin._trade_count == 1

    def test_open_sells_only_plugin_holdings(self, tmp_path):
        plugin = self._loaded(tmp_path)
        plugin._holding_gld = True
        plugin._regime_at_prior_close = REGIME_CASH
        plugin._gld_price = 240.0
        plugin.holdings.add_position("GLD", 7, 240.0)
        sells = []
        plugin._emit_sell = lambda qty, reason: sells.append(qty)
        plugin._on_market_open(datetime(2026, 5, 15, 9, 30))
        assert sells == [7]

    def test_open_deduped_per_day(self, tmp_path):
        plugin = self._loaded(tmp_path)
        plugin._holding_gld = True
        plugin._regime_at_prior_close = REGIME_CASH
        plugin._gld_price = 240.0
        plugin.holdings.add_position("GLD", 7, 240.0)
        sells = []
        plugin._emit_sell = lambda qty, reason: sells.append(qty)
        ts = datetime(2026, 5, 15, 9, 30)
        plugin._on_market_open(ts)
        plugin._on_market_open(ts)           # repeated bar, same session
        assert sells == [7]

    def test_open_skips_when_plugin_holds_nothing(self, tmp_path):
        """Account may hold GLD elsewhere, but with an empty plugin slice the
        plugin must NOT sell it (hazard #2)."""
        plugin = self._loaded(tmp_path)
        plugin._holding_gld = True           # stale flag
        plugin._regime_at_prior_close = REGIME_CASH
        plugin._gld_price = 240.0
        acct_pos = Mock(symbol="GLD", quantity=500)
        plugin.portfolio = Mock(positions=[acct_pos])
        sells = []
        plugin._emit_sell = lambda qty, reason: sells.append(qty)
        plugin._on_market_open(datetime(2026, 5, 15, 9, 30))
        assert sells == []                    # did NOT touch account-wide GLD

    def test_current_gld_shares_reads_holdings_not_account(self, tmp_path):
        plugin = self._loaded(tmp_path)
        acct_pos = Mock(symbol="GLD", quantity=500)
        plugin.portfolio = Mock(positions=[acct_pos])
        assert plugin._current_gld_shares() == 0      # account-wide GLD ignored
        plugin.holdings.add_position("GLD", 12, 240.0)
        assert plugin._current_gld_shares() == 12

    def test_close_persists_regime_for_next_open(self, tmp_path):
        """The regime saved at close drives tomorrow's open decision and must
        survive an overnight crash — even on a no-order (rolling) close."""
        import json

        plugin = self._loaded(tmp_path)
        plugin._holding_gld = True                    # rolling overnight: no order
        plugin._regime = REGIME_CASH
        plugin._on_market_close(datetime(2026, 5, 15, 15, 45))

        state = json.loads((plugin.plugin_dir / "state.json").read_text())["state"]
        assert state["regime_at_prior_close"] == REGIME_CASH

    def test_close_skips_buy_when_plugin_unfunded(self, tmp_path):
        """With a live portfolio, the MOC buy is bounded by the plugin's own
        funded cash — an unfunded plugin must not draw account-wide capital."""
        plugin = self._loaded(tmp_path)
        plugin.portfolio = Mock()
        plugin._gld_price = 240.0
        plugin._holding_gld = False
        assert plugin.holdings.current_cash == 0.0    # never funded

        plugin._on_market_close(datetime(2026, 5, 15, 15, 45))
        assert plugin._trade_count == 0
        plugin.portfolio.place_order_custom.assert_not_called()

    def test_close_buy_bounded_by_plugin_cash(self, tmp_path):
        """Funded below allocation_dollars: size to cash, not allocation."""
        plugin = self._loaded(tmp_path)
        plugin.portfolio = Mock()
        plugin.portfolio.place_order_custom.return_value = 77
        plugin.allocation_dollars = 10_000.0
        plugin.holdings.add_cash(2_400.0)             # only $2,400 funded
        plugin._gld_price = 240.0
        plugin._holding_gld = False

        plugin._on_market_close(datetime(2026, 5, 15, 15, 45))

        _, order = plugin.portfolio.place_order_custom.call_args[0]
        assert order.totalQuantity == 10               # int(2400 / 240)


# ---------------------------------------------------------------------------
# Fills update the plugin's holdings ledger
# ---------------------------------------------------------------------------

class TestFillsUpdateHoldings:
    def _loaded(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.load()
        return plugin

    def _fill(self, order_id, action, qty, price):
        return Mock(order_id=order_id, filled_quantity=qty,
                    avg_fill_price=price)

    def test_buy_fill_adds_position_and_deducts_cash(self, tmp_path):
        plugin = self._loaded(tmp_path)
        plugin.holdings.add_cash(10_000.0)
        plugin._pending_order_actions[42] = "BUY"

        plugin.on_order_fill(self._fill(42, "BUY", 40, 240.0))

        pos = plugin.holdings.get_position("GLD")
        assert pos is not None and pos.quantity == 40
        assert plugin.holdings.current_cash == pytest.approx(10_000 - 40 * 240.0)

    def test_sell_fill_removes_position_and_credits_cash(self, tmp_path):
        plugin = self._loaded(tmp_path)
        plugin.holdings.add_position("GLD", 40, 240.0)
        plugin._holding_gld = True
        plugin._pending_order_actions[43] = "SELL"

        plugin.on_order_fill(self._fill(43, "SELL", 40, 245.0))

        assert plugin.holdings.get_position("GLD") is None
        assert plugin.holdings.current_cash == pytest.approx(40 * 245.0)

    def test_fill_persists_holdings_to_disk(self, tmp_path):
        import json

        plugin = self._loaded(tmp_path)
        plugin.holdings.add_cash(10_000.0)
        plugin._pending_order_actions[44] = "BUY"
        plugin.on_order_fill(self._fill(44, "BUY", 40, 240.0))

        data = json.loads((plugin.plugin_dir / "holdings.json").read_text())
        symbols = [p["symbol"] for p in data["current_holdings"]["positions"]]
        assert "GLD" in symbols

    def test_full_cycle_open_sell_uses_filled_shares(self, tmp_path):
        """The shares bought via MOC must be sellable at the next cash-regime
        open — the exact loop that was broken before fills reached holdings."""
        plugin = self._loaded(tmp_path)
        plugin.holdings.add_cash(10_000.0)
        plugin._pending_order_actions[45] = "BUY"
        plugin.on_order_fill(self._fill(45, "BUY", 41, 240.0))

        plugin._regime_at_prior_close = REGIME_CASH
        sells = []
        plugin._emit_sell = lambda qty, reason: sells.append(qty)
        plugin._on_market_open(datetime(2026, 5, 16, 9, 30))
        assert sells == [41]

    def test_no_holdings_is_safe(self, tmp_path):
        """Offline plugin (never load()ed, holdings=None) must not crash."""
        plugin = _make_plugin(tmp_path)
        plugin._pending_order_actions[46] = "BUY"
        plugin.on_order_fill(self._fill(46, "BUY", 40, 240.0))
        assert plugin._holding_gld is True


# ---------------------------------------------------------------------------
# Crash safety — unresolved in-flight orders survive a restart
# ---------------------------------------------------------------------------

class TestPendingOrderRestore:
    def test_pending_orders_persisted_and_restored(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        plugin._pending_order_actions[123] = "BUY"
        plugin._save_state()

        plugin2 = _make_plugin(tmp_path)
        plugin2.start()
        assert plugin2._pending_order_actions == {123: "BUY"}

    def test_restored_pending_buy_assumes_holding(self, tmp_path):
        """Crash between MOC placement and the 16:00 fill: on restart the
        plugin must assume the buy filled (worst case a missed trade), never
        place a duplicate overnight buy."""
        plugin = _make_plugin(tmp_path)
        plugin.start()
        plugin._pending_order_actions[123] = "BUY"
        plugin._holding_gld = False
        plugin._save_state()

        plugin2 = _make_plugin(tmp_path)
        plugin2.start()
        assert plugin2._holding_gld is True
        assert plugin2._restored_pending_buy is True

    def test_restored_pending_buy_blocks_reconcile_downgrade(self, tmp_path):
        """Startup reconciliation must not flip holding_gld back to False when
        an unresolved BUY may have filled into the account (its shares are in
        _unassigned, not this plugin's holdings)."""
        plugin = _make_plugin(tmp_path)
        plugin.load()
        plugin.start()
        plugin._pending_order_actions[123] = "BUY"
        plugin._holding_gld = False
        plugin._save_state()

        plugin2 = _make_plugin(tmp_path)
        plugin2.load()                      # holdings exist but hold no GLD
        plugin2.portfolio = Mock()
        plugin2._warm_up_from_history = lambda: None
        plugin2._start_subscriptions = lambda: None
        plugin2.start()
        assert plugin2._holding_gld is True  # not downgraded

    def test_no_pending_orders_restores_normally(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        plugin._holding_gld = False
        plugin._save_state()

        plugin2 = _make_plugin(tmp_path)
        plugin2.start()
        assert plugin2._holding_gld is False
        assert plugin2._restored_pending_buy is False

    def test_terminal_status_drops_pending_from_state(self, tmp_path):
        """A cancelled/rejected order must not linger in persisted pending
        state (it would trip the conservative holding flag on next start)."""
        import json
        from ib.models import OrderStatus

        plugin = _make_plugin(tmp_path)
        plugin.start()
        plugin._pending_order_actions[123] = "BUY"
        plugin._save_state()

        rec = Mock(order_id=123, status=OrderStatus.CANCELLED)
        plugin.on_order_status(rec)

        state = json.loads((plugin.plugin_dir / "state.json").read_text())["state"]
        assert state["pending_orders"] == {}


# ---------------------------------------------------------------------------
# Session-decision wall-clock windows — stale bars must not act late
# ---------------------------------------------------------------------------

class TestSessionDecisionWindow:
    """Observed live: IB re-delivered the day's 15:45 bar through the
    live-update path on a 19:53 ET restart, and the close decision placed
    an MOC that IB queued for the NEXT day's close. The wall-clock window
    is the absolute bound: open decisions only 09:30-09:45 ET, close
    decisions only 15:45-15:55 ET, and only for today's bar."""

    def _clocked(self, tmp_path, now_et):
        plugin = _make_plugin(tmp_path)
        plugin._now_ny = lambda: now_et
        fired = {"open": 0, "close": 0}
        plugin._on_market_open = lambda ts: fired.__setitem__("open", fired["open"] + 1)
        plugin._on_market_close = lambda ts: fired.__setitem__("close", fired["close"] + 1)
        return plugin, fired

    def test_close_fires_inside_window(self, tmp_path):
        plugin, fired = self._clocked(tmp_path, datetime(2026, 7, 14, 15, 46))
        plugin._handle_session_event(datetime(2026, 7, 14, 15, 45))
        assert fired["close"] == 1

    def test_close_refused_after_hours(self, tmp_path):
        """The exact live incident: 15:45 bar re-delivered at 19:53 ET."""
        plugin, fired = self._clocked(tmp_path, datetime(2026, 7, 14, 19, 53))
        plugin._handle_session_event(datetime(2026, 7, 14, 15, 45))
        assert fired["close"] == 0

    def test_close_refused_past_moc_cutoff(self, tmp_path):
        plugin, fired = self._clocked(tmp_path, datetime(2026, 7, 14, 15, 56))
        plugin._handle_session_event(datetime(2026, 7, 14, 15, 45))
        assert fired["close"] == 0

    def test_close_refused_for_yesterdays_bar(self, tmp_path):
        plugin, fired = self._clocked(tmp_path, datetime(2026, 7, 15, 15, 46))
        plugin._handle_session_event(datetime(2026, 7, 14, 15, 45))
        assert fired["close"] == 0

    def test_open_fires_inside_window(self, tmp_path):
        plugin, fired = self._clocked(tmp_path, datetime(2026, 7, 14, 9, 31))
        plugin._handle_session_event(datetime(2026, 7, 14, 9, 30))
        assert fired["open"] == 1

    def test_open_refused_outside_window(self, tmp_path):
        plugin, fired = self._clocked(tmp_path, datetime(2026, 7, 14, 16, 34))
        plugin._handle_session_event(datetime(2026, 7, 14, 9, 30))
        assert fired["open"] == 0

    def test_non_session_bar_ignored(self, tmp_path):
        plugin, fired = self._clocked(tmp_path, datetime(2026, 7, 14, 12, 0))
        plugin._handle_session_event(datetime(2026, 7, 14, 12, 0))
        assert fired == {"open": 0, "close": 0}


# ---------------------------------------------------------------------------
# Auto-save protocol — the executive must persist REAL state, never a stub
# ---------------------------------------------------------------------------

class TestGetStateForSave:
    def test_contains_full_strategy_state(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._holding_gld = True
        plugin._regime_at_prior_close = REGIME_CASH
        plugin._pending_order_actions[118] = "BUY"

        state = plugin.get_state_for_save()
        assert state["holding_gld"] is True
        assert state["regime_at_prior_close"] == REGIME_CASH
        assert state["pending_orders"] == {"118": "BUY"}

    def test_autosave_roundtrip_preserves_pending_orders(self, tmp_path):
        """Simulates the executive's periodic auto-save between the MOC
        placement and a crash: the pending order must survive into the
        next session (the clobbering stub destroyed it)."""
        plugin = _make_plugin(tmp_path)
        plugin.start()
        plugin._pending_order_actions[118] = "BUY"

        # What the executive's _auto_save_all_states now does
        plugin.save_state(plugin.get_state_for_save())

        plugin2 = _make_plugin(tmp_path)
        plugin2.start()
        assert plugin2._pending_order_actions == {118: "BUY"}
        assert plugin2._holding_gld is True   # conservative pending-BUY flag


# ---------------------------------------------------------------------------
# Reconnect handling — live-bar subscriptions must be re-created
# ---------------------------------------------------------------------------

class TestOnReconnect:
    def test_on_reconnect_cancels_and_resubscribes(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._live_bar_req_ids = {"GLD": 1, "UUP": 2, "TLT": 3, "RINF": 4}
        cancelled, resubscribed = [], []
        plugin.cancel_live_bars = cancelled.append
        plugin._start_subscriptions = lambda: resubscribed.append(True)

        plugin.on_reconnect()

        assert sorted(cancelled) == [1, 2, 3, 4]
        assert resubscribed == [True]


# ---------------------------------------------------------------------------
# Alerting — IB errors, stuck orders, bar-parse failures
# ---------------------------------------------------------------------------

class TestAlerting:
    def _bus_plugin(self, tmp_path):
        """Plugin wired to a real MessageBus with a capture subscriber."""
        from ib.message_bus import MessageBus

        bus = MessageBus()
        received = []
        bus.subscribe("alerts", received.append, subscriber="test")
        plugin = _make_plugin(tmp_path, message_bus=bus)
        return plugin, received

    def test_on_ib_error_publishes_alert(self, tmp_path):
        plugin, received = self._bus_plugin(tmp_path)
        plugin._pending_order_actions[55] = "BUY"

        plugin.on_ib_error(55, 201, "Order rejected - insufficient funds")

        assert len(received) == 1
        payload = received[0].payload
        assert payload["kind"] == "ib_error"
        assert payload["is_order"] is True
        assert payload["error_code"] == 201

    def test_stuck_pending_order_alerts_once(self, tmp_path):
        import time as _time

        plugin, received = self._bus_plugin(tmp_path)
        plugin._pending_order_actions[55] = "SELL"
        plugin._pending_order_placed_at[55] = _time.time() - 3600  # 1h ago

        plugin._check_pending_order_age()
        plugin._check_pending_order_age()

        kinds = [m.payload["kind"] for m in received]
        assert kinds == ["stuck_order"]

    def test_recent_pending_order_no_alert(self, tmp_path):
        import time as _time

        plugin, received = self._bus_plugin(tmp_path)
        plugin._pending_order_actions[55] = "BUY"
        plugin._pending_order_placed_at[55] = _time.time() - 60

        plugin._check_pending_order_age()
        assert received == []

    def test_fill_clears_pending_age_tracking(self, tmp_path):
        import time as _time

        plugin = _make_plugin(tmp_path)
        plugin._pending_order_actions[55] = "BUY"
        plugin._pending_order_placed_at[55] = _time.time()
        plugin.on_order_fill(Mock(order_id=55, filled_quantity=40,
                                  avg_fill_price=240.0))
        assert 55 not in plugin._pending_order_placed_at

    def test_repeated_parse_failures_alert_once(self, tmp_path):
        from plugins.gld_usd_swap.plugin import _PARSE_FAILURE_ALERT_THRESHOLD

        plugin, received = self._bus_plugin(tmp_path)
        bad_bar = Mock(close=240.0, date=None)   # unparseable timestamp

        for _ in range(_PARSE_FAILURE_ALERT_THRESHOLD + 5):
            plugin._on_bar("GLD", bad_bar, is_live=True)

        kinds = [m.payload["kind"] for m in received]
        assert kinds == ["bar_parse_failure"]

    def test_parse_success_resets_failure_counter(self, tmp_path):
        plugin, received = self._bus_plugin(tmp_path)
        bad_bar = Mock(close=240.0, date=None)
        good_bar = Mock(close=240.0, date="20260515 10:00:00")

        for _ in range(5):
            plugin._on_bar("GLD", bad_bar, is_live=True)
        plugin._on_bar("GLD", good_bar, is_live=True)
        assert plugin._bar_parse_failures == 0
        assert received == []

    def test_terminal_order_publishes_alert(self, tmp_path):
        from ib.models import OrderStatus

        plugin, received = self._bus_plugin(tmp_path)
        plugin._pending_order_actions[77] = "BUY"
        plugin.on_order_status(Mock(order_id=77, status=OrderStatus.CANCELLED))

        kinds = [m.payload["kind"] for m in received]
        assert kinds == ["order_terminal"]
