"""
Tests for the account-separation feature introduced to fix paper/live conflation.

Covers:
  - PluginBase.set_account() path re-rooting
  - PluginExecutive._account field and set_account() method
  - PluginExecutive calling plugin.set_account() before plugin.load()
    in both the register_plugin() and load_plugin_from_file() code paths
  - PluginBase._bar_store lazy property
  - PluginBase.subscribe_live_bars() caching wrapper
  - PluginBase.get_bars_cached() delegation
"""

import os
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

from plugins.base import PluginBase, TradeSignal, PluginState
from ib.plugin_executive import PluginExecutive


# ---------------------------------------------------------------------------
# Minimal concrete plugin for testing
# ---------------------------------------------------------------------------

class SimplePlugin(PluginBase):
    @property
    def description(self):
        return "test"

    def start(self):      return True
    def stop(self):       return True
    def freeze(self):     return True
    def resume(self):     return True
    def handle_request(self, request_type, payload): return {"success": True}
    def calculate_signals(self): return []


# ---------------------------------------------------------------------------
# PluginBase.set_account — path re-rooting
# ---------------------------------------------------------------------------

class TestPluginBaseSetAccount:
    def test_account_id_stored(self, tmp_path):
        plugin = SimplePlugin("alpha", base_path=str(tmp_path / "alpha"))
        plugin.set_account("DU1234567")
        assert plugin._account_id == "DU1234567"

    def test_base_path_includes_account(self, monkeypatch):
        monkeypatch.setenv("IB_PLUGIN_DIR", "/plugins")
        plugin = SimplePlugin("alpha")
        plugin.set_account("DU1234567")
        assert plugin._base_path == Path("/plugins/alpha/DU1234567")

    def test_state_file_under_account(self, monkeypatch):
        monkeypatch.setenv("IB_PLUGIN_DIR", "/plugins")
        plugin = SimplePlugin("alpha")
        plugin.set_account("DU1234567")
        assert plugin._state_file == Path("/plugins/alpha/DU1234567/state.json")

    def test_instruments_file_under_account(self, monkeypatch):
        monkeypatch.setenv("IB_PLUGIN_DIR", "/plugins")
        plugin = SimplePlugin("alpha")
        plugin.set_account("DU1234567")
        assert plugin._instruments_file == Path("/plugins/alpha/DU1234567/instruments.json")

    def test_holdings_file_under_account(self, monkeypatch):
        monkeypatch.setenv("IB_PLUGIN_DIR", "/plugins")
        plugin = SimplePlugin("alpha")
        plugin.set_account("DU1234567")
        assert plugin._holdings_file == Path("/plugins/alpha/DU1234567/holdings.json")

    def test_paper_and_live_paths_differ(self, monkeypatch):
        monkeypatch.setenv("IB_PLUGIN_DIR", "/plugins")
        p = SimplePlugin("alpha")
        l = SimplePlugin("alpha")
        p.set_account("DU1234567")
        l.set_account("U1234567")
        assert p._base_path != l._base_path

    def test_slot_used_not_name_when_different(self, monkeypatch):
        monkeypatch.setenv("IB_PLUGIN_DIR", "/plugins")
        plugin = SimplePlugin("alpha")
        plugin.slot = "alpha_v2"
        plugin.set_account("DU1234567")
        assert "alpha_v2" in str(plugin._base_path)

    def test_default_account_id_is_empty(self):
        plugin = SimplePlugin("alpha")
        assert plugin._account_id == ""

    def test_env_var_overrides_default_plugin_dir(self, monkeypatch, tmp_path):
        monkeypatch.setenv("IB_PLUGIN_DIR", str(tmp_path))
        plugin = SimplePlugin("beta")
        plugin.set_account("U9876543")
        assert plugin._base_path == tmp_path / "beta" / "U9876543"

    def test_state_saved_to_account_subdir(self, tmp_path, monkeypatch):
        """save_state() must create and write into the account subfolder."""
        monkeypatch.setenv("IB_PLUGIN_DIR", str(tmp_path))
        plugin = SimplePlugin("gamma")
        plugin.set_account("DU0000001")
        plugin.load()
        plugin.save_state({})  # triggers mkdir + write
        assert plugin._state_file.exists()
        assert "DU0000001" in str(plugin._state_file)


# ---------------------------------------------------------------------------
# PluginExecutive.set_account and _account field
# ---------------------------------------------------------------------------

class TestPluginExecutiveSetAccount:
    def test_default_account_is_empty(self):
        executive = PluginExecutive(None, None)
        assert executive._account == ""

    def test_set_account_stores_value(self):
        executive = PluginExecutive(None, None)
        executive.set_account("DU1234567")
        assert executive._account == "DU1234567"

    def test_set_account_overwrites(self):
        executive = PluginExecutive(None, None)
        executive.set_account("DU1111111")
        executive.set_account("U9999999")
        assert executive._account == "U9999999"


# ---------------------------------------------------------------------------
# PluginExecutive.register_plugin — calls set_account before load when needed
# ---------------------------------------------------------------------------

class TestRegisterPluginCallsSetAccount:
    def test_set_account_called_when_plugin_unloaded(self, monkeypatch):
        """register_plugin must call plugin.set_account before plugin.load()
        when the plugin hasn't been loaded yet."""
        executive = PluginExecutive(None, None)
        executive.set_account("DU1234567")

        plugin = SimplePlugin("delta")
        assert not plugin.is_loaded  # starts unloaded

        call_order = []
        original_set = plugin.set_account
        original_load = plugin.load

        def tracked_set_account(acct):
            call_order.append(("set_account", acct))
            original_set(acct)

        def tracked_load():
            call_order.append("load")
            return original_load()

        monkeypatch.setattr(plugin, "set_account", tracked_set_account)
        monkeypatch.setattr(plugin, "load", tracked_load)

        executive.register_plugin(plugin)

        assert call_order[0] == ("set_account", "DU1234567")
        assert call_order[1] == "load"

    def test_set_account_not_called_when_already_loaded(self):
        """If plugin is already loaded, the auto-load branch is skipped entirely."""
        executive = PluginExecutive(None, None)
        executive.set_account("DU1234567")

        plugin = SimplePlugin("epsilon")
        plugin.load()  # load before register
        assert plugin.is_loaded

        set_account_calls = []
        original_set = plugin.set_account

        def tracked_set_account(acct):
            set_account_calls.append(acct)
            original_set(acct)

        plugin.set_account = tracked_set_account

        executive.register_plugin(plugin)

        # set_account should NOT have been called (plugin was already loaded)
        assert set_account_calls == []

    def test_no_account_no_set_account_call(self):
        """If _account is empty, set_account must never be called on the plugin."""
        executive = PluginExecutive(None, None)
        # _account left at default ""

        plugin = SimplePlugin("zeta")
        set_account_calls = []
        original_set = plugin.set_account

        def tracked(acct):
            set_account_calls.append(acct)
            original_set(acct)

        plugin.set_account = tracked
        executive.register_plugin(plugin)
        assert set_account_calls == []


# ---------------------------------------------------------------------------
# PluginBase._bar_store — lazy property
# ---------------------------------------------------------------------------

class TestBarStoreProperty:
    def test_returns_none_when_import_fails(self, monkeypatch):
        """If ib.bar_store cannot be imported, _bar_store returns None."""
        plugin = SimplePlugin("test")
        import sys
        monkeypatch.setitem(sys.modules, "ib.bar_store", None)
        # Clear cached instance if any
        if hasattr(plugin, "_bar_store_instance"):
            del plugin._bar_store_instance
        result = plugin._bar_store
        assert result is None

    def test_returns_bar_store_instance(self, tmp_path, monkeypatch):
        monkeypatch.setenv("IB_HIST_DB", str(tmp_path / "bars.db"))
        plugin = SimplePlugin("test")
        if hasattr(plugin, "_bar_store_instance"):
            del plugin._bar_store_instance
        from ib.bar_store import BarStore
        result = plugin._bar_store
        assert isinstance(result, BarStore)

    def test_cached_on_second_access(self, tmp_path, monkeypatch):
        monkeypatch.setenv("IB_HIST_DB", str(tmp_path / "bars.db"))
        plugin = SimplePlugin("test")
        if hasattr(plugin, "_bar_store_instance"):
            del plugin._bar_store_instance
        first  = plugin._bar_store
        second = plugin._bar_store
        assert first is second


# ---------------------------------------------------------------------------
# PluginBase.subscribe_live_bars — caching wrapper
# ---------------------------------------------------------------------------

class TestSubscribeLiveBarsCache:
    def _make_plugin_with_portfolio(self):
        plugin = SimplePlugin("test")
        portfolio = MagicMock()
        portfolio.request_historical_data.return_value = 42
        plugin.portfolio = portfolio
        return plugin, portfolio

    def test_user_callback_called(self, tmp_path, monkeypatch):
        monkeypatch.setenv("IB_HIST_DB", str(tmp_path / "bars.db"))
        plugin, portfolio = self._make_plugin_with_portfolio()
        if hasattr(plugin, "_bar_store_instance"):
            del plugin._bar_store_instance

        received = []
        plugin.subscribe_live_bars(
            contract=MagicMock(symbol="GLD"),
            on_bar=received.append,
        )
        # Capture the caching wrapper passed to portfolio
        _, kwargs = portfolio.request_historical_data.call_args
        caching_on_bar = kwargs["on_bar"]

        fake_bar = MagicMock()
        fake_bar.date = "20250101 10:00:00"
        caching_on_bar(fake_bar)
        assert fake_bar in received

    def test_insert_bar_called_when_store_available(self, tmp_path, monkeypatch):
        monkeypatch.setenv("IB_HIST_DB", str(tmp_path / "bars.db"))
        plugin, portfolio = self._make_plugin_with_portfolio()
        if hasattr(plugin, "_bar_store_instance"):
            del plugin._bar_store_instance

        plugin.subscribe_live_bars(
            contract=MagicMock(symbol="GLD"),
            on_bar=lambda b: None,
            bar_size_setting="5 mins",
            what_to_show="TRADES",
            use_rth=True,
        )
        _, kwargs = portfolio.request_historical_data.call_args
        caching_on_bar = kwargs["on_bar"]

        store = plugin._bar_store
        insert_calls = []
        store.insert_bar = lambda *a, **kw: insert_calls.append(a)

        fake_bar = MagicMock()
        fake_bar.date = "20250101 10:00:00"
        fake_bar.open = fake_bar.high = fake_bar.low = fake_bar.close = 185.0
        fake_bar.volume = 1000
        caching_on_bar(fake_bar)

        assert len(insert_calls) == 1
        symbol, bar_size, what_to_show, use_rth, bar = insert_calls[0]
        assert symbol == "GLD"
        assert bar_size == "5 mins"

    def test_user_callback_called_before_store_error(self, tmp_path, monkeypatch):
        """User callback must fire even if insert_bar raises."""
        monkeypatch.setenv("IB_HIST_DB", str(tmp_path / "bars.db"))
        plugin, portfolio = self._make_plugin_with_portfolio()
        if hasattr(plugin, "_bar_store_instance"):
            del plugin._bar_store_instance

        received = []
        plugin.subscribe_live_bars(
            contract=MagicMock(symbol="GLD"),
            on_bar=received.append,
        )
        _, kwargs = portfolio.request_historical_data.call_args
        caching_on_bar = kwargs["on_bar"]

        # Make insert_bar raise
        store = plugin._bar_store
        store.insert_bar = MagicMock(side_effect=RuntimeError("db failure"))

        fake_bar = MagicMock()
        fake_bar.date = "20250101 10:00:00"
        # Should not raise; user callback was already called
        caching_on_bar(fake_bar)
        assert fake_bar in received

    def test_no_portfolio_returns_none(self):
        plugin = SimplePlugin("test")
        plugin.portfolio = None
        result = plugin.subscribe_live_bars(
            contract=MagicMock(symbol="GLD"),
            on_bar=lambda b: None,
        )
        assert result is None


# ---------------------------------------------------------------------------
# PluginBase.get_bars_cached — delegation to BarStore.get_bars
# ---------------------------------------------------------------------------

class TestGetBarsCached:
    def _make_contract(self, symbol="GLD"):
        c = MagicMock()
        c.symbol = symbol
        return c

    def test_calls_bar_store_get_bars(self, tmp_path, monkeypatch):
        from datetime import datetime, timezone
        monkeypatch.setenv("IB_HIST_DB", str(tmp_path / "bars.db"))
        plugin = SimplePlugin("test")
        if hasattr(plugin, "_bar_store_instance"):
            del plugin._bar_store_instance

        store = plugin._bar_store
        get_bars_calls = []
        store.get_bars = lambda **kw: (get_bars_calls.append(kw) or [])

        UTC = timezone.utc
        plugin.get_bars_cached(
            contract=self._make_contract("GLD"),
            start_dt=datetime(2025, 1, 1, tzinfo=UTC),
            end_dt=datetime(2025, 1, 3, tzinfo=UTC),
            bar_size_setting="5 mins",
        )

        assert len(get_bars_calls) == 1
        assert get_bars_calls[0]["symbol"] == "GLD"
        assert get_bars_calls[0]["bar_size"] == "5 mins"

    def test_falls_back_to_get_historical_data_when_no_store(self, monkeypatch):
        """When _bar_store is None, get_bars_cached calls get_historical_data."""
        from datetime import datetime, timezone
        plugin = SimplePlugin("test")
        # Force _bar_store to None
        plugin._bar_store_instance = None

        historical_calls = []

        def fake_get_historical(contract, end_date_time, duration_str,
                                bar_size_setting, what_to_show, use_rth):
            historical_calls.append(bar_size_setting)
            return []

        monkeypatch.setattr(plugin, "get_historical_data", fake_get_historical)

        UTC = timezone.utc
        result = plugin.get_bars_cached(
            contract=self._make_contract("GLD"),
            start_dt=datetime(2025, 1, 1, tzinfo=UTC),
            end_dt=datetime(2025, 1, 3, tzinfo=UTC),
            bar_size_setting="5 mins",
        )
        assert historical_calls == ["5 mins"]
        assert result == []

    def test_force_flag_passed_through(self, tmp_path, monkeypatch):
        from datetime import datetime, timezone
        monkeypatch.setenv("IB_HIST_DB", str(tmp_path / "bars.db"))
        plugin = SimplePlugin("test")
        if hasattr(plugin, "_bar_store_instance"):
            del plugin._bar_store_instance

        store = plugin._bar_store
        force_values = []
        store.get_bars = lambda **kw: (force_values.append(kw.get("force")) or [])

        UTC = timezone.utc
        plugin.get_bars_cached(
            contract=self._make_contract("GLD"),
            start_dt=datetime(2025, 1, 1, tzinfo=UTC),
            end_dt=datetime(2025, 1, 3, tzinfo=UTC),
            force=True,
        )
        assert force_values == [True]
