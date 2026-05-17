"""
Unit tests for plugins/unassigned/plugin.py
"""
from datetime import datetime
from unittest.mock import Mock
import pytest

from plugins.unassigned.plugin import UnassignedPlugin, UNASSIGNED_PLUGIN_NAME
from plugins.base import HoldingPosition, Holdings, PluginState


def _make_plugin(tmp_path, portfolio=None):
    if portfolio is None:
        portfolio = Mock()
        portfolio.positions = []
        portfolio.get_account_summary = Mock(return_value=None)
        portfolio.cash = 0.0
    plugin = UnassignedPlugin(
        base_path=tmp_path / "_unassigned",
        portfolio=portfolio,
    )
    return plugin


def _mock_portfolio(positions=None, available_funds=0.0):
    """Build a mock portfolio with given positions and cash."""
    portfolio = Mock()
    portfolio.cash = available_funds
    summary = Mock()
    summary.is_valid = True
    summary.available_funds = available_funds
    portfolio.get_account_summary = Mock(return_value=summary)

    pos_list = []
    for sym, qty, price in (positions or []):
        p = Mock()
        p.symbol = sym
        p.quantity = qty
        p.avg_cost = price * 0.95
        p.current_price = price
        p.market_value = qty * price
        pos_list.append(p)
    portfolio.positions = pos_list
    return portfolio


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

class TestMetadata:
    def test_plugin_name(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin.name == UNASSIGNED_PLUGIN_NAME

    def test_is_system_plugin(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin.IS_SYSTEM_PLUGIN is True

    def test_calculate_signals_empty(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin.calculate_signals() == []


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

class TestLifecycle:
    def test_start_returns_true(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        assert plugin.start() is True

    def test_stop_returns_true(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        assert plugin.stop() is True

    def test_freeze_and_resume(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        assert plugin.freeze() is True
        assert plugin.resume() is True

    def test_cash_persisted_across_restart(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        plugin._cash_balance = 42_000.0
        plugin.stop()

        plugin2 = _make_plugin(tmp_path)
        plugin2.start()
        assert plugin2._cash_balance == 42_000.0


# ---------------------------------------------------------------------------
# sync_from_portfolio
# ---------------------------------------------------------------------------

class TestSync:
    def test_sync_sets_cash(self, tmp_path):
        portfolio = _mock_portfolio(available_funds=75_000.0)
        plugin = _make_plugin(tmp_path, portfolio=portfolio)
        plugin.start()
        plugin.sync_from_portfolio()
        assert plugin._cash_balance == pytest.approx(75_000.0)

    def test_sync_excludes_claimed_symbols(self, tmp_path):
        portfolio = _mock_portfolio(
            positions=[("SPY", 100, 500.0), ("QQQ", 50, 450.0)],
            available_funds=10_000.0,
        )
        plugin = _make_plugin(tmp_path, portfolio=portfolio)
        plugin.start()
        plugin.sync_from_portfolio(claimed_symbols={"SPY"})
        syms = [p.symbol for p in plugin._holdings.current_positions]
        assert "QQQ" in syms
        assert "SPY" not in syms

    def test_sync_includes_unclaimed_positions(self, tmp_path):
        portfolio = _mock_portfolio(
            positions=[("MSFT", 30, 400.0)],
            available_funds=5_000.0,
        )
        plugin = _make_plugin(tmp_path, portfolio=portfolio)
        plugin.start()
        plugin.sync_from_portfolio()
        syms = [p.symbol for p in plugin._holdings.current_positions]
        assert "MSFT" in syms

    def test_sync_no_portfolio_returns_false(self, tmp_path):
        plugin = UnassignedPlugin(base_path=tmp_path / "_unassigned", portfolio=None)
        result = plugin.sync_from_portfolio()
        assert result is False

    def test_sync_cash_minus_claimed(self, tmp_path):
        portfolio = _mock_portfolio(available_funds=100_000.0)
        plugin = _make_plugin(tmp_path, portfolio=portfolio)
        plugin.start()
        plugin.sync_from_portfolio(claimed_cash=30_000.0)
        assert plugin._cash_balance == pytest.approx(70_000.0)


# ---------------------------------------------------------------------------
# handle_request
# ---------------------------------------------------------------------------

class TestHandleRequest:
    def test_get_cash(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        plugin._cash_balance = 12_345.0
        result = plugin.handle_request("get_cash", {})
        assert result["success"] is True
        assert result["cash"] == 12_345.0

    def test_get_unassigned_no_positions(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin.start()
        plugin._cash_balance = 5_000.0
        result = plugin.handle_request("get_unassigned", {})
        assert result["success"] is True
        assert result["cash"] == 5_000.0
        assert result["positions"] == []

    def test_get_unassigned_with_holdings(self, tmp_path):
        portfolio = _mock_portfolio(
            positions=[("SPY", 10, 500.0)],
            available_funds=5_000.0,
        )
        plugin = _make_plugin(tmp_path, portfolio=portfolio)
        plugin.start()
        plugin.sync_from_portfolio()
        result = plugin.handle_request("get_unassigned", {})
        assert result["success"] is True
        assert len(result["positions"]) == 1
        assert result["positions"][0]["symbol"] == "SPY"

    def test_sync_request(self, tmp_path):
        portfolio = _mock_portfolio(available_funds=50_000.0)
        plugin = _make_plugin(tmp_path, portfolio=portfolio)
        plugin.start()
        result = plugin.handle_request("sync", {"claimed_symbols": ["SPY"]})
        assert result["success"] is True

    def test_unknown_request(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        result = plugin.handle_request("nonexistent", {})
        assert result["success"] is False
