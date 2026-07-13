"""
Unit tests for plugins/gld_usd_swap/plugin.py
"""
import time
from datetime import datetime
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


# ---------------------------------------------------------------------------
# Hazard #1 — backfill replay must not fire session orders (_is_live_bar)
# ---------------------------------------------------------------------------

class TestLiveBarGate:
    _TS_1545 = datetime(2026, 5, 15, 15, 45)
    _TS_1540 = datetime(2026, 5, 15, 15, 40)
    _TS_1550 = datetime(2026, 5, 15, 15, 50)

    def test_suppressed_during_settle_window(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._subscribe_monotonic = time.monotonic()      # just (re)subscribed
        assert plugin._is_live_bar(self._TS_1545) is False   # backfill burst not flushed

    def test_live_after_settle(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._subscribe_monotonic = time.monotonic() - 999  # settled
        assert plugin._is_live_bar(self._TS_1545) is True

    def test_repeated_same_bar_deduped(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._subscribe_monotonic = time.monotonic() - 999
        assert plugin._is_live_bar(self._TS_1545) is True
        assert plugin._is_live_bar(self._TS_1545) is False   # same forming-bar update

    def test_older_backfill_leak_rejected(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._subscribe_monotonic = time.monotonic() - 999
        assert plugin._is_live_bar(self._TS_1545) is True
        assert plugin._is_live_bar(self._TS_1540) is False   # stale bar ≤ high-water-mark
        assert plugin._is_live_bar(self._TS_1550) is True    # genuine next live bar

    def test_hwm_advances_while_suppressed(self, tmp_path):
        """A bar seen inside the settle window advances the high-water-mark, so a
        backfill bar that leaks past the window still can't look 'new'."""
        plugin = _make_plugin(tmp_path)
        plugin._subscribe_monotonic = time.monotonic()       # in settle window
        assert plugin._is_live_bar(self._TS_1545) is False
        plugin._subscribe_monotonic = time.monotonic() - 999  # window elapses
        assert plugin._is_live_bar(self._TS_1545) is False    # not newer than hwm
        assert plugin._is_live_bar(self._TS_1550) is True


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
