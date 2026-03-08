"""
Tests for ib/plugin_store.py

Uses tmp_path to redirect DB to a temp directory. No IB connection needed.
"""

import json
import threading
from datetime import datetime
from pathlib import Path

import pytest

from ib.plugin_store import PluginStore, get_plugin_store
from plugins.base import Holdings, HoldingPosition


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def store(tmp_path):
    """Fresh PluginStore backed by a temp DB."""
    return PluginStore(db_path=tmp_path / "test_plugin_store.db")


@pytest.fixture
def sample_holdings():
    """A Holdings object with initial and current positions."""
    h = Holdings(
        plugin_name="test_plugin",
        initial_cash=10000.0,
        current_cash=8500.0,
        created_at=datetime(2024, 1, 1, 9, 0, 0),
        last_updated=datetime(2024, 1, 2, 10, 0, 0),
    )
    h.initial_positions = [
        HoldingPosition(symbol="SPY", quantity=10, cost_basis=450.0,
                        current_price=450.0, market_value=4500.0),
    ]
    h.current_positions = [
        HoldingPosition(symbol="SPY", quantity=10, cost_basis=450.0,
                        current_price=460.0, market_value=4600.0),
        HoldingPosition(symbol="QQQ", quantity=5, cost_basis=380.0,
                        current_price=385.0, market_value=1925.0),
    ]
    return h


# =============================================================================
# 1. Init
# =============================================================================


class TestPluginStoreInit:
    def test_db_created(self, tmp_path):
        db = tmp_path / "init.db"
        PluginStore(db_path=db)
        assert db.exists()

    def test_schema_versions_populated(self, store):
        import sqlite3
        with sqlite3.connect(store.db_path) as conn:
            row = conn.execute(
                "SELECT version FROM schema_versions WHERE component = 'plugin_store'"
            ).fetchone()
        assert row is not None
        assert row[0] == 1

    def test_all_tables_exist(self, store):
        import sqlite3
        with sqlite3.connect(store.db_path) as conn:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
        expected = {
            "schema_versions", "plugin_states", "plugin_holdings",
            "plugin_positions", "forex_cost_basis", "migration_log",
        }
        assert expected.issubset(tables)


# =============================================================================
# 2. State
# =============================================================================


class TestPluginStoreState:
    def test_save_and_load_roundtrip(self, store):
        state = {"counter": 42, "symbols": ["SPY", "QQQ"]}
        assert store.save_state("alpha", "1.0", state)
        loaded = store.load_state("alpha")
        assert loaded == state

    def test_load_missing_returns_none(self, store):
        assert store.load_state("nonexistent") is None

    def test_unicode_and_nested_dict(self, store):
        state = {"emoji": "🚢", "nested": {"a": {"b": 3}}, "list": [1, 2, 3]}
        store.save_state("beta", "2.0", state)
        assert store.load_state("beta") == state

    def test_overwrite_replaces(self, store):
        store.save_state("gamma", "1.0", {"v": 1})
        store.save_state("gamma", "1.0", {"v": 2})
        assert store.load_state("gamma") == {"v": 2}

    def test_clear_state(self, store):
        store.save_state("delta", "1.0", {"x": 1})
        assert store.clear_state("delta")
        assert store.load_state("delta") is None

    def test_clear_nonexistent_returns_true(self, store):
        assert store.clear_state("no_such_plugin")


# =============================================================================
# 3. Holdings
# =============================================================================


class TestPluginStoreHoldings:
    def test_save_and_load_roundtrip(self, store, sample_holdings):
        assert store.save_holdings(sample_holdings)
        loaded = store.load_holdings("test_plugin")
        assert loaded is not None
        assert loaded.plugin_name == "test_plugin"
        assert loaded.initial_cash == 10000.0
        assert loaded.current_cash == 8500.0

    def test_positions_preserved(self, store, sample_holdings):
        store.save_holdings(sample_holdings)
        loaded = store.load_holdings("test_plugin")
        assert len(loaded.initial_positions) == 1
        assert len(loaded.current_positions) == 2
        syms = {p.symbol for p in loaded.current_positions}
        assert syms == {"SPY", "QQQ"}

    def test_position_values_preserved(self, store, sample_holdings):
        store.save_holdings(sample_holdings)
        loaded = store.load_holdings("test_plugin")
        spy = next(p for p in loaded.current_positions if p.symbol == "SPY")
        assert spy.quantity == 10
        assert spy.cost_basis == 450.0
        assert spy.current_price == 460.0
        assert spy.market_value == 4600.0

    def test_overwrite_replaces_fully(self, store, sample_holdings):
        store.save_holdings(sample_holdings)
        # Create updated holdings with fewer positions
        h2 = Holdings(
            plugin_name="test_plugin",
            initial_cash=10000.0,
            current_cash=9000.0,
        )
        h2.current_positions = [
            HoldingPosition(symbol="TSLA", quantity=2, cost_basis=200.0)
        ]
        store.save_holdings(h2)
        loaded = store.load_holdings("test_plugin")
        assert loaded.current_cash == 9000.0
        assert len(loaded.current_positions) == 1
        assert loaded.current_positions[0].symbol == "TSLA"

    def test_load_missing_returns_none(self, store):
        assert store.load_holdings("no_plugin") is None

    def test_delete_holdings(self, store, sample_holdings):
        store.save_holdings(sample_holdings)
        assert store.delete_holdings("test_plugin")
        assert store.load_holdings("test_plugin") is None


# =============================================================================
# 4. Forex cost basis
# =============================================================================


class TestPluginStoreForex:
    def test_empty_db_returns_empty_dict(self, store):
        assert store.load_forex_cost_basis() == {}

    def test_save_and_load_roundtrip(self, store):
        basis = {"EUR": 1.08, "GBP": 1.25}
        store.save_forex_cost_basis(basis)
        assert store.load_forex_cost_basis() == basis

    def test_save_replaces_all_rows(self, store):
        store.save_forex_cost_basis({"EUR": 1.08, "GBP": 1.25})
        store.save_forex_cost_basis({"JPY": 0.0067})
        loaded = store.load_forex_cost_basis()
        assert "EUR" not in loaded
        assert loaded == {"JPY": 0.0067}

    def test_upsert_single_entry(self, store):
        store.save_forex_cost_basis({"EUR": 1.08})
        store.set_forex_cost_basis_entry("EUR", 1.10)
        store.set_forex_cost_basis_entry("GBP", 1.25)
        loaded = store.load_forex_cost_basis()
        assert loaded["EUR"] == pytest.approx(1.10)
        assert loaded["GBP"] == pytest.approx(1.25)


# =============================================================================
# 5. Migration
# =============================================================================


class TestPluginStoreMigration:
    def _write_state_json(self, path: Path, plugin_name: str, state: dict):
        path.mkdir(parents=True, exist_ok=True)
        data = {
            "plugin_name": plugin_name,
            "plugin_version": "1.0",
            "state": state,
            "saved_at": datetime.now().isoformat(),
        }
        (path / "state.json").write_text(json.dumps(data))

    def _write_holdings_json(self, path: Path, holdings: Holdings):
        path.mkdir(parents=True, exist_ok=True)
        (path / "holdings.json").write_text(json.dumps(holdings.to_dict()))

    def test_state_json_imported(self, store, tmp_path):
        plugin_dir = tmp_path / "my_plugin"
        self._write_state_json(plugin_dir, "my_plugin", {"key": "val"})
        store.migrate_from_json("my_plugin", plugin_dir)
        assert store.load_state("my_plugin") == {"key": "val"}

    def test_migration_idempotent(self, store, tmp_path):
        plugin_dir = tmp_path / "my_plugin"
        self._write_state_json(plugin_dir, "my_plugin", {"key": "val"})
        store.migrate_from_json("my_plugin", plugin_dir)
        # Overwrite JSON with different data
        self._write_state_json(plugin_dir, "my_plugin", {"key": "new"})
        store.migrate_from_json("my_plugin", plugin_dir)
        # Second call must NOT overwrite the already-migrated data
        assert store.load_state("my_plugin") == {"key": "val"}

    def test_missing_state_json_skipped_gracefully(self, store, tmp_path):
        plugin_dir = tmp_path / "empty_plugin"
        plugin_dir.mkdir()
        # No state.json — should not raise
        store.migrate_from_json("empty_plugin", plugin_dir)
        assert store.load_state("empty_plugin") is None

    def test_holdings_json_imported(self, store, tmp_path, sample_holdings):
        plugin_dir = tmp_path / "test_plugin"
        self._write_holdings_json(plugin_dir, sample_holdings)
        store.migrate_from_json("test_plugin", plugin_dir)
        loaded = store.load_holdings("test_plugin")
        assert loaded is not None
        assert loaded.initial_cash == sample_holdings.initial_cash

    def test_forex_migration(self, store, tmp_path):
        forex_file = tmp_path / ".ib_forex_cost_basis.json"
        forex_file.write_text(json.dumps({"EUR": 1.08, "GBP": 1.25}))
        store.migrate_forex_cost_basis(forex_file)
        assert store.load_forex_cost_basis() == {"EUR": 1.08, "GBP": 1.25}

    def test_forex_migration_idempotent(self, store, tmp_path):
        forex_file = tmp_path / ".ib_forex_cost_basis.json"
        forex_file.write_text(json.dumps({"EUR": 1.08}))
        store.migrate_forex_cost_basis(forex_file)
        # Change file content; second call must not re-import
        forex_file.write_text(json.dumps({"EUR": 9.99}))
        store.migrate_forex_cost_basis(forex_file)
        assert store.load_forex_cost_basis()["EUR"] == pytest.approx(1.08)

    def test_missing_forex_file_skipped(self, store, tmp_path):
        forex_file = tmp_path / "missing.json"
        store.migrate_forex_cost_basis(forex_file)
        assert store.load_forex_cost_basis() == {}


# =============================================================================
# 6. Concurrency
# =============================================================================


class TestPluginStoreConcurrency:
    def test_concurrent_writes_no_corruption(self, store):
        errors = []

        def write_plugin(name):
            try:
                for i in range(20):
                    store.save_state(name, "1.0", {"i": i, "name": name})
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=write_plugin, args=(f"plugin_{n}",))
            for n in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Concurrent write errors: {errors}"

        # Verify each plugin has valid data
        for n in range(10):
            state = store.load_state(f"plugin_{n}")
            assert state is not None
            assert state["name"] == f"plugin_{n}"


# =============================================================================
# 7. Schema versioning
# =============================================================================


class TestPluginStoreSchemaVersioning:
    def test_version_row_present(self, store):
        import sqlite3
        with sqlite3.connect(store.db_path) as conn:
            row = conn.execute(
                "SELECT version, applied_at FROM schema_versions WHERE component='plugin_store'"
            ).fetchone()
        assert row is not None
        assert row[0] == 1
        assert row[1]  # non-empty timestamp

    def test_reinit_does_not_reset_version(self, tmp_path):
        db = tmp_path / "versioned.db"
        s1 = PluginStore(db_path=db)
        s1.save_state("p", "1.0", {"v": 1})
        # Re-init same DB
        s2 = PluginStore(db_path=db)
        assert s2.load_state("p") == {"v": 1}


# =============================================================================
# 8. Global singleton
# =============================================================================


class TestPluginStoreGlobalSingleton:
    def test_same_object_returned(self):
        a = get_plugin_store()
        b = get_plugin_store()
        assert a is b

    def test_singleton_is_plugin_store(self):
        assert isinstance(get_plugin_store(), PluginStore)
