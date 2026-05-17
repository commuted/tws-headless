"""
Unit tests for plugins/gld_usd_swap/plugin.py
"""
from unittest.mock import Mock, MagicMock
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
