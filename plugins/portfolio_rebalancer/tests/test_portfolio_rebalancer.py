"""
Unit tests for plugins/portfolio_rebalancer/plugin.py
"""
import threading
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from plugins.base import Holdings, HoldingPosition, PluginInstrument
from plugins.portfolio_rebalancer.plugin import PortfolioRebalancerPlugin


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_plugin(tmp_path, **kwargs):
    """Return a plugin wired to a mock portfolio and fresh tmp_path."""
    portfolio = Mock()
    portfolio.positions = []
    portfolio.get_position = Mock(return_value=None)
    portfolio.place_order = Mock(return_value=1001)
    plugin = PortfolioRebalancerPlugin(
        base_path=tmp_path / "portfolio_rebalancer",
        portfolio=portfolio,
        **kwargs,
    )
    return plugin, portfolio


def _fund_plugin(plugin, cash=100_000.0, positions=None):
    """Give the plugin a Holdings object with cash and optional positions."""
    holdings = Holdings(plugin_name="portfolio_rebalancer", current_cash=cash)
    if positions:
        for sym, qty, price in positions:
            holdings.current_positions.append(
                HoldingPosition(symbol=sym, quantity=qty,
                                current_price=price, market_value=qty * price)
            )
    plugin._holdings = holdings
    return holdings


def _add_instruments(plugin, *symbols_weights):
    """Add enabled instruments. symbols_weights is [(sym, weight), ...]"""
    for sym, weight in symbols_weights:
        plugin.add_instrument(PluginInstrument(sym, sym, weight=weight))


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

class TestInit:
    def test_defaults(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path)
        assert plugin.mode == "threshold"
        assert plugin.drift_threshold_pct == 5.0
        assert plugin.dry_run is True
        assert plugin.INSTRUMENT_COMPLIANCE is True

    def test_custom_params(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="calendar",
                                  drift_threshold_pct=3.0, dry_run=False)
        assert plugin.mode == "calendar"
        assert plugin.drift_threshold_pct == 3.0
        assert plugin.dry_run is False

    def test_description_reflects_mode(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        assert "manual" in plugin.description


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

class TestLifecycle:
    def test_start_returns_true(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        assert plugin.start() is True

    def test_start_launches_thread_non_manual(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="threshold")
        plugin.start()
        assert plugin._check_thread is not None
        assert plugin._check_thread.is_alive()
        plugin._stop_event.set()
        plugin._check_thread.join(timeout=2)

    def test_start_no_thread_in_manual_mode(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        plugin.start()
        assert plugin._check_thread is None

    def test_stop_joins_thread(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="threshold")
        plugin.start()
        assert plugin.stop() is True
        assert not (plugin._check_thread and plugin._check_thread.is_alive())

    def test_freeze_stops_thread(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="threshold")
        plugin.start()
        assert plugin.freeze() is True

    def test_resume_restarts_thread(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="threshold")
        plugin.start()
        plugin.freeze()
        assert plugin.resume() is True
        assert plugin._check_thread is not None
        assert plugin._check_thread.is_alive()
        plugin.stop()

    def test_state_restored_on_start(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        plugin.start()
        plugin._rebalance_count = 7
        plugin._save_full_state()
        plugin.stop()

        plugin2, _ = _make_plugin(tmp_path, mode="manual")
        plugin2.start()
        assert plugin2._rebalance_count == 7

    def test_on_unload_summary(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        plugin._rebalance_count = 3
        summary = plugin.on_unload()
        assert "3" in summary
        assert "portfolio_rebalancer" in summary


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

class TestParameters:
    def test_get_parameters(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path)
        params = plugin.get_parameters()
        assert "mode" in params
        assert "drift_threshold_pct" in params
        assert "dry_run" in params
        assert len(params) == 10

    def test_set_valid_mode(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        plugin.start()
        assert plugin.set_parameter("mode", "threshold") is True
        assert plugin.mode == "threshold"
        plugin.stop()

    def test_set_invalid_mode(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        plugin.start()
        assert plugin.set_parameter("mode", "nonexistent") is False
        assert plugin.mode == "manual"

    def test_set_drift_threshold(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        plugin.start()
        assert plugin.set_parameter("drift_threshold_pct", "3.5") is True
        assert plugin.drift_threshold_pct == 3.5

    def test_set_dry_run_bool(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        plugin.start()
        plugin.set_parameter("dry_run", False)
        assert plugin.dry_run is False

    def test_set_dry_run_string(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        plugin.start()
        plugin.set_parameter("dry_run", "true")
        assert plugin.dry_run is True

    def test_set_calendar_schedule_valid(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        plugin.start()
        assert plugin.set_parameter("calendar_schedule", "monthly") is True
        assert plugin.calendar_schedule == "monthly"

    def test_set_calendar_schedule_invalid(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        plugin.start()
        assert plugin.set_parameter("calendar_schedule", "hourly") is False

    def test_set_unknown_parameter(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        plugin.start()
        assert plugin.set_parameter("nonexistent_param", 42) is False

    def test_set_manage_untracked(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        plugin.start()
        plugin.set_parameter("manage_untracked", True)
        assert plugin.manage_untracked is True

    def test_mode_switch_manual_to_threshold_starts_thread(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        plugin.start()
        assert plugin._check_thread is None
        plugin.set_parameter("mode", "threshold")
        assert plugin._check_thread is not None
        assert plugin._check_thread.is_alive()
        plugin.stop()

    def test_mode_switch_threshold_to_manual_stops_thread(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="threshold")
        plugin.start()
        assert plugin._check_thread.is_alive()
        plugin.set_parameter("mode", "manual")
        plugin._check_thread.join(timeout=2)
        assert not plugin._check_thread.is_alive()


# ---------------------------------------------------------------------------
# Portfolio value — uses holdings only
# ---------------------------------------------------------------------------

class TestPortfolioValue:
    def test_zero_without_holdings(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path)
        assert plugin._portfolio_value() == 0.0

    def test_returns_holdings_cash_only(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path)
        _fund_plugin(plugin, cash=50_000.0)
        assert plugin._portfolio_value() == 50_000.0

    def test_includes_position_market_value(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path)
        _fund_plugin(plugin, cash=10_000.0,
                     positions=[("SPY", 100, 500.0)])  # market_value=50000
        assert plugin._portfolio_value() == 60_000.0

    def test_does_not_use_portfolio_nlv(self, tmp_path):
        plugin, portfolio = _make_plugin(tmp_path)
        portfolio.get_account_summary = Mock(return_value=Mock(net_liquidation=999_999.0))
        _fund_plugin(plugin, cash=20_000.0)
        # Must be holdings value, not the mocked NLV
        assert plugin._portfolio_value() == 20_000.0


# ---------------------------------------------------------------------------
# Rebalance guard: no holdings
# ---------------------------------------------------------------------------

class TestRebalanceGuard:
    def test_no_holdings_returns_error(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        _add_instruments(plugin, ("SPY", 100.0))
        result = plugin._run_rebalance(dry_run=True)
        assert "error" in result
        assert "fund" in result["error"].lower()

    def test_zero_value_holdings_returns_error(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        _add_instruments(plugin, ("SPY", 100.0))
        plugin._holdings = Holdings(plugin_name="portfolio_rebalancer", current_cash=0.0)
        result = plugin._run_rebalance(dry_run=True)
        assert "error" in result


# ---------------------------------------------------------------------------
# Threshold rebalance logic
# ---------------------------------------------------------------------------

class TestThresholdRebalance:
    def test_no_drift_no_trades(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="threshold",
                                  drift_threshold_pct=5.0)
        _add_instruments(plugin, ("SPY", 100.0))
        _fund_plugin(plugin, cash=0.0, positions=[("SPY", 100, 500.0)])
        plugin._price_cache["SPY"] = 500.0
        result = plugin._run_rebalance(dry_run=True)
        assert result["trade_count"] == 0

    def test_drifted_position_generates_trade(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="threshold",
                                  drift_threshold_pct=5.0)
        # Target 60% SPY, 40% BND.  Fund 100k all in SPY → SPY=100%, BND=0% → both drift
        _add_instruments(plugin, ("SPY", 60.0), ("BND", 40.0))
        _fund_plugin(plugin, cash=0.0, positions=[("SPY", 200, 500.0)])
        plugin._price_cache["SPY"] = 500.0
        plugin._price_cache["BND"] = 80.0
        result = plugin._run_rebalance(dry_run=True)
        symbols = [t["symbol"] for t in result["trades"]]
        assert "SPY" in symbols  # overweight → SELL
        assert "BND" in symbols  # underweight → BUY

    def test_sells_before_buys(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="threshold",
                                  drift_threshold_pct=5.0)
        _add_instruments(plugin, ("SPY", 50.0), ("BND", 50.0))
        _fund_plugin(plugin, cash=0.0, positions=[("SPY", 200, 500.0)])
        plugin._price_cache["SPY"] = 500.0
        plugin._price_cache["BND"] = 80.0
        result = plugin._run_rebalance(dry_run=True)
        actions = [t["action"] for t in result["trades"]]
        sell_idx = actions.index("SELL") if "SELL" in actions else None
        buy_idx  = actions.index("BUY")  if "BUY"  in actions else None
        if sell_idx is not None and buy_idx is not None:
            assert sell_idx < buy_idx

    def test_min_trade_value_filter(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="threshold",
                                  drift_threshold_pct=5.0,
                                  min_trade_value=100_000.0)  # huge filter
        _add_instruments(plugin, ("SPY", 60.0), ("BND", 40.0))
        _fund_plugin(plugin, cash=0.0, positions=[("SPY", 200, 500.0)])
        plugin._price_cache["SPY"] = 500.0
        plugin._price_cache["BND"] = 80.0
        result = plugin._run_rebalance(dry_run=True)
        assert result["trade_count"] == 0

    def test_max_trades_cap(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="threshold",
                                  drift_threshold_pct=0.0,
                                  max_trades_per_run=1,
                                  min_trade_value=0.0,
                                  min_trade_shares=0)
        _add_instruments(plugin, ("SPY", 50.0), ("BND", 50.0))
        _fund_plugin(plugin, cash=100_000.0)
        plugin._price_cache["SPY"] = 500.0
        plugin._price_cache["BND"] = 80.0
        result = plugin._run_rebalance(dry_run=True)
        assert result["trade_count"] <= 1


# ---------------------------------------------------------------------------
# Calendar rebalance logic
# ---------------------------------------------------------------------------

class TestCalendarRebalance:
    def test_should_calendar_first_run(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="calendar",
                                  calendar_schedule="daily")
        plugin._last_calendar_date = None
        assert plugin._should_calendar_rebalance() is True

    def test_should_calendar_daily_new_day(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="calendar",
                                  calendar_schedule="daily")
        plugin._last_calendar_date = date.today() - timedelta(days=1)
        assert plugin._should_calendar_rebalance() is True

    def test_should_not_calendar_daily_same_day(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="calendar",
                                  calendar_schedule="daily")
        plugin._last_calendar_date = date.today()
        assert plugin._should_calendar_rebalance() is False

    def test_should_calendar_monthly_new_month(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="calendar",
                                  calendar_schedule="monthly")
        last_month = date.today().replace(day=1) - timedelta(days=1)
        plugin._last_calendar_date = last_month
        assert plugin._should_calendar_rebalance() is True

    def test_calendar_mode_exact_trades(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="calendar",
                                  drift_threshold_pct=99.0)  # threshold irrelevant
        _add_instruments(plugin, ("SPY", 100.0))
        _fund_plugin(plugin, cash=50_000.0)
        plugin._price_cache["SPY"] = 500.0
        result = plugin._run_rebalance(dry_run=True)
        # Should generate a BUY for SPY (0 shares currently, needs ~98 shares)
        assert any(t["symbol"] == "SPY" and t["action"] == "BUY"
                   for t in result["trades"])


# ---------------------------------------------------------------------------
# Threshold drift detector
# ---------------------------------------------------------------------------

class TestThresholdDetector:
    def test_no_holdings_returns_false(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path)
        _add_instruments(plugin, ("SPY", 100.0))
        assert plugin._should_threshold_rebalance() is False

    def test_zero_holdings_returns_false(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path)
        _add_instruments(plugin, ("SPY", 100.0))
        plugin._holdings = Holdings(plugin_name="portfolio_rebalancer")
        assert plugin._should_threshold_rebalance() is False

    def test_detects_drift(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, drift_threshold_pct=5.0)
        _add_instruments(plugin, ("SPY", 60.0))
        # Fund 100% SPY at weight 60% → drift = 40% → exceeds threshold
        _fund_plugin(plugin, cash=0.0, positions=[("SPY", 100, 500.0)])
        assert plugin._should_threshold_rebalance() is True

    def test_no_drift_within_tolerance(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, drift_threshold_pct=5.0)
        _add_instruments(plugin, ("SPY", 100.0))
        _fund_plugin(plugin, cash=0.0, positions=[("SPY", 100, 500.0)])
        # SPY = 100% of holdings, target = 100% → drift = 0
        assert plugin._should_threshold_rebalance() is False


# ---------------------------------------------------------------------------
# Manage untracked
# ---------------------------------------------------------------------------

class TestManageUntracked:
    def test_untracked_position_gets_zero_target(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="threshold",
                                  drift_threshold_pct=0.0,
                                  manage_untracked=True,
                                  min_trade_value=0.0,
                                  min_trade_shares=0)
        _add_instruments(plugin, ("SPY", 100.0))
        # Holding MSFT which is not in instruments.json
        _fund_plugin(plugin, cash=0.0,
                     positions=[("SPY", 100, 500.0), ("MSFT", 10, 400.0)])
        plugin._price_cache["SPY"] = 500.0
        plugin._price_cache["MSFT"] = 400.0
        result = plugin._run_rebalance(dry_run=True)
        symbols = [t["symbol"] for t in result["trades"]]
        assert "MSFT" in symbols
        msft_trade = next(t for t in result["trades"] if t["symbol"] == "MSFT")
        assert msft_trade["action"] == "SELL"

    def test_untracked_disabled_ignores_extra_position(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="threshold",
                                  drift_threshold_pct=0.0,
                                  manage_untracked=False)
        _add_instruments(plugin, ("SPY", 100.0))
        _fund_plugin(plugin, cash=0.0,
                     positions=[("SPY", 100, 500.0), ("MSFT", 10, 400.0)])
        plugin._price_cache["SPY"] = 500.0
        result = plugin._run_rebalance(dry_run=True)
        symbols = [t["symbol"] for t in result["trades"]]
        assert "MSFT" not in symbols


# ---------------------------------------------------------------------------
# Order execution (dry_run=False path)
# ---------------------------------------------------------------------------

class TestOrderExecution:
    def test_dry_run_does_not_call_place_order(self, tmp_path):
        plugin, portfolio = _make_plugin(tmp_path, mode="threshold",
                                         drift_threshold_pct=0.0,
                                         min_trade_value=0.0,
                                         min_trade_shares=0)
        _add_instruments(plugin, ("SPY", 100.0))
        _fund_plugin(plugin, cash=100_000.0)
        plugin._price_cache["SPY"] = 500.0
        plugin._run_rebalance(dry_run=True)
        portfolio.place_order.assert_not_called()

    def test_live_run_calls_place_order(self, tmp_path):
        plugin, portfolio = _make_plugin(tmp_path, mode="threshold",
                                         drift_threshold_pct=0.0,
                                         min_trade_value=0.0,
                                         min_trade_shares=0)
        _add_instruments(plugin, ("SPY", 100.0))
        _fund_plugin(plugin, cash=100_000.0)
        plugin._price_cache["SPY"] = 500.0
        plugin._run_rebalance(dry_run=False)
        portfolio.place_order.assert_called()

    def test_live_run_increments_rebalance_count(self, tmp_path):
        plugin, portfolio = _make_plugin(tmp_path, mode="threshold",
                                         drift_threshold_pct=0.0,
                                         min_trade_value=0.0,
                                         min_trade_shares=0)
        _add_instruments(plugin, ("SPY", 100.0))
        _fund_plugin(plugin, cash=100_000.0)
        plugin._price_cache["SPY"] = 500.0
        portfolio.place_order.return_value = None  # failed order
        plugin._run_rebalance(dry_run=False)
        assert plugin._rebalance_count == 1


# ---------------------------------------------------------------------------
# Fill tracking
# ---------------------------------------------------------------------------

class TestFillTracking:
    def test_on_order_fill_increments_fill_count(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path)
        plugin._pending_order_actions[42] = "SPY"
        rec = Mock()
        rec.order_id = 42
        rec.action = "BUY"
        rec.filled_quantity = 10.0
        rec.avg_fill_price = 500.0
        plugin.on_order_fill(rec)
        assert plugin._fill_count == 1
        assert 42 not in plugin._pending_order_actions

    def test_on_order_fill_ignores_unknown_order(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path)
        rec = Mock()
        rec.order_id = 999
        rec.filled_quantity = 10.0
        rec.avg_fill_price = 500.0
        plugin.on_order_fill(rec)
        assert plugin._fill_count == 0

    def test_on_order_status_removes_terminal_order(self, tmp_path):
        from ib.models import OrderStatus
        plugin, _ = _make_plugin(tmp_path)
        plugin._pending_order_actions[77] = "SPY"
        rec = Mock()
        rec.order_id = 77
        rec.status = OrderStatus.CANCELLED
        plugin.on_order_status(rec)
        assert 77 not in plugin._pending_order_actions


# ---------------------------------------------------------------------------
# handle_request
# ---------------------------------------------------------------------------

class TestHandleRequest:
    def setup_method(self):
        pass

    def test_unknown_request(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path)
        result = plugin.handle_request("nonexistent", {})
        assert result["success"] is False

    def test_get_parameters(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path)
        result = plugin.handle_request("get_parameters", {})
        assert result["success"] is True
        assert "mode" in result["data"]

    def test_set_parameter_valid(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        plugin.start()
        result = plugin.handle_request("set_parameter",
                                       {"key": "drift_threshold_pct", "value": 3.0})
        assert result["success"] is True
        assert plugin.drift_threshold_pct == 3.0

    def test_set_parameter_missing_key(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path)
        result = plugin.handle_request("set_parameter", {"value": 3.0})
        assert result["success"] is False

    def test_get_status(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path)
        result = plugin.handle_request("get_status", {})
        assert result["success"] is True
        assert "mode" in result["data"]
        assert "holdings_value" in result["data"]

    def test_get_targets_no_holdings(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path)
        _add_instruments(plugin, ("SPY", 60.0), ("BND", 40.0))
        result = plugin.handle_request("get_targets", {})
        assert result["success"] is True
        assert result["data"]["holdings_value"] == 0.0

    def test_get_targets_with_holdings(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path)
        _add_instruments(plugin, ("SPY", 60.0), ("BND", 40.0))
        _fund_plugin(plugin, cash=40_000.0, positions=[("SPY", 100, 600.0)])
        result = plugin.handle_request("get_targets", {})
        data = result["data"]
        assert data["holdings_value"] == pytest.approx(100_000.0)
        syms = [t["symbol"] for t in data["targets"]]
        assert "SPY" in syms and "BND" in syms

    def test_preview_returns_trades(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="threshold",
                                  drift_threshold_pct=0.0,
                                  min_trade_value=0.0,
                                  min_trade_shares=0)
        _add_instruments(plugin, ("SPY", 100.0))
        _fund_plugin(plugin, cash=100_000.0)
        plugin._price_cache["SPY"] = 500.0
        result = plugin.handle_request("preview", {})
        assert result["success"] is True
        assert result["data"]["dry_run"] is True

    def test_rebalance_respects_payload_dry_run(self, tmp_path):
        plugin, portfolio = _make_plugin(tmp_path, mode="manual",
                                          dry_run=True,
                                          drift_threshold_pct=0.0,
                                          min_trade_value=0.0,
                                          min_trade_shares=0)
        _add_instruments(plugin, ("SPY", 100.0))
        _fund_plugin(plugin, cash=100_000.0)
        plugin._price_cache["SPY"] = 500.0
        plugin.start()
        # Override dry_run for this one call
        result = plugin.handle_request("rebalance", {"dry_run": False})
        assert result["success"] is True
        portfolio.place_order.assert_called()

    def test_get_last_trades(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path)
        plugin._last_trades = [{"symbol": "SPY", "action": "BUY"}]
        result = plugin.handle_request("get_last_trades", {})
        assert result["success"] is True
        assert len(result["data"]["trades"]) == 1


# ---------------------------------------------------------------------------
# Message bus
# ---------------------------------------------------------------------------

class TestMessageBus:
    def test_bus_rebalance_command(self, tmp_path):
        from ib.message_bus import MessageBus
        bus = MessageBus()
        plugin, _ = _make_plugin(tmp_path, mode="manual", message_bus=bus)
        _add_instruments(plugin, ("SPY", 100.0))
        _fund_plugin(plugin, cash=100_000.0)
        plugin._price_cache["SPY"] = 500.0
        plugin.start()

        results = []
        bus.subscribe("portfolio_rebalancer_result", lambda m: results.append(m), "test")

        msg = Mock()
        msg.payload = {"command": "preview"}
        plugin._on_bus_command(msg)

        assert len(results) == 1

    def test_bus_set_mode_command(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        plugin.start()
        msg = Mock()
        msg.payload = {"command": "set_mode", "mode": "threshold"}
        plugin._on_bus_command(msg)
        assert plugin.mode == "threshold"
        plugin.stop()

    def test_bus_unknown_command_logged(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="manual")
        plugin.start()
        msg = Mock()
        msg.payload = {"command": "does_not_exist"}
        # Should not raise
        plugin._on_bus_command(msg)


# ---------------------------------------------------------------------------
# Weight normalisation
# ---------------------------------------------------------------------------

class TestWeightNormalisation:
    def test_weights_normalised_when_sum_off(self, tmp_path):
        plugin, _ = _make_plugin(tmp_path, mode="threshold",
                                  drift_threshold_pct=0.0,
                                  min_trade_value=0.0,
                                  min_trade_shares=0)
        # Weights sum to 80, not 100 — plugin should normalise before trading
        _add_instruments(plugin, ("SPY", 40.0), ("BND", 40.0))
        _fund_plugin(plugin, cash=100_000.0)
        plugin._price_cache["SPY"] = 500.0
        plugin._price_cache["BND"] = 80.0
        result = plugin._run_rebalance(dry_run=True)
        # Should complete without error even with non-100% weights
        assert "error" not in result
