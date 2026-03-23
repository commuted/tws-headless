"""
Tests for ib/plugin_store.py

Registry-only store — schema_versions and plugin_registry tables only.
Uses tmp_path to redirect DB to a temp directory. No IB connection needed.
"""

import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from ib.plugin_store import PluginStore, get_plugin_store, configure_plugin_store


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def store(tmp_path):
    """Fresh PluginStore backed by a temp DB."""
    return PluginStore(db_path=tmp_path / "test_plugin_store.db")


# =============================================================================
# 1. Init
# =============================================================================


class TestPluginStoreInit:
    def test_db_created(self, tmp_path):
        db = tmp_path / "init.db"
        PluginStore(db_path=db)
        assert db.exists()

    def test_schema_versions_populated(self, store):
        with sqlite3.connect(store.db_path) as conn:
            row = conn.execute(
                "SELECT version FROM schema_versions WHERE component = 'plugin_store'"
            ).fetchone()
        assert row is not None
        assert row[0] == 3

    def test_all_tables_exist(self, store):
        with sqlite3.connect(store.db_path) as conn:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
        expected = {"schema_versions", "plugin_registry"}
        assert expected.issubset(tables)


# =============================================================================
# 2. Registry-only — removed tables must NOT exist
# =============================================================================


class TestPluginStoreRegistryOnly:
    """Assert that tables removed in the simplification do not exist."""

    REMOVED_TABLES = {
        "plugin_states",
        "plugin_holdings",
        "plugin_positions",
        "plugin_instruments",
        "forex_cost_basis",
        "migration_log",
    }

    def test_removed_tables_absent(self, store):
        with sqlite3.connect(store.db_path) as conn:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
        for table in self.REMOVED_TABLES:
            assert table not in tables, f"Table '{table}' should not exist"


# =============================================================================
# 3. Schema versioning
# =============================================================================


class TestPluginStoreSchemaVersioning:
    def test_version_row_present(self, store):
        with sqlite3.connect(store.db_path) as conn:
            row = conn.execute(
                "SELECT version, applied_at FROM schema_versions WHERE component='plugin_store'"
            ).fetchone()
        assert row is not None
        assert row[0] == 3
        assert row[1]  # non-empty timestamp

    def test_reinit_same_db_preserves_registry(self, tmp_path):
        db = tmp_path / "versioned.db"
        s1 = PluginStore(db_path=db)
        s1.upsert_registry("p", "/plugins/p.py", "1.0", "unloaded")
        # Re-init same DB
        s2 = PluginStore(db_path=db)
        assert s2.get_registry_entry("p") is not None


# =============================================================================
# 4. Global singleton
# =============================================================================


class TestPluginStoreGlobalSingleton:
    def test_same_object_returned(self):
        a = get_plugin_store()
        b = get_plugin_store()
        assert a is b

    def test_singleton_is_plugin_store(self):
        assert isinstance(get_plugin_store(), PluginStore)


# =============================================================================
# 5. Registry
# =============================================================================


class TestPluginRegistry:
    def test_upsert_and_get(self, store):
        ok = store.upsert_registry(
            slot="nav1", class_path="/plugins/nav.py",
            version="1.0", status="unloaded",
        )
        assert ok
        entry = store.get_registry_entry("nav1")
        assert entry is not None
        assert entry["slot"] == "nav1"
        assert entry["class_path"] == "/plugins/nav.py"
        assert entry["version"] == "1.0"
        assert entry["status"] == "unloaded"
        assert entry["config"] is None

    def test_upsert_with_config(self, store):
        cfg = {"symbol": "SPY", "threshold": 0.5}
        store.upsert_registry(
            slot="nav2", class_path="/plugins/nav.py",
            version="1.0", status="started", config=cfg,
        )
        entry = store.get_registry_entry("nav2")
        assert entry["config"] == cfg
        assert entry["status"] == "started"

    def test_status_update(self, store):
        store.upsert_registry("nav3", "/p/nav.py", "1.0", "unloaded")
        store.upsert_registry("nav3", "/p/nav.py", "1.0", "started")
        entry = store.get_registry_entry("nav3")
        assert entry["status"] == "started"

    def test_created_at_preserved_on_update(self, store):
        store.upsert_registry("nav4", "/p/nav.py", "1.0", "unloaded")
        first = store.get_registry_entry("nav4")["created_at"]
        store.upsert_registry("nav4", "/p/nav.py", "1.0", "started")
        second = store.get_registry_entry("nav4")["created_at"]
        # created_at should be unchanged (ON CONFLICT preserves it)
        assert first == second

    def test_list_no_filter(self, store):
        store.upsert_registry("a", "/p/a.py", "1.0", "unloaded")
        store.upsert_registry("b", "/p/b.py", "1.0", "started")
        entries = store.list_registry()
        slots = [e["slot"] for e in entries]
        assert "a" in slots
        assert "b" in slots

    def test_list_with_status_filter(self, store):
        store.upsert_registry("x1", "/p/x.py", "1.0", "unloaded")
        store.upsert_registry("x2", "/p/x.py", "1.0", "started")
        store.upsert_registry("x3", "/p/x.py", "1.0", "frozen")
        started = store.list_registry(status_filter="started")
        assert len(started) == 1
        assert started[0]["slot"] == "x2"

    def test_delete_registry_entry(self, store):
        store.upsert_registry("del1", "/p/d.py", "1.0", "unloaded")
        assert store.get_registry_entry("del1") is not None
        store.delete_registry_entry("del1")
        assert store.get_registry_entry("del1") is None

    def test_get_missing_returns_none(self, store):
        assert store.get_registry_entry("does_not_exist") is None

    def test_schema_version_is_3(self, store):
        with sqlite3.connect(store.db_path) as conn:
            row = conn.execute(
                "SELECT version FROM schema_versions WHERE component = 'plugin_store'"
            ).fetchone()
        assert row[0] == 3


# =============================================================================
# 6. configure_plugin_store — account-keyed singleton
# =============================================================================


class TestConfigurePluginStore:
    def test_creates_account_keyed_db_path(self, tmp_path, monkeypatch):
        """configure_plugin_store should use ~/.ib_plugin_store_{account}.db."""
        import ib.plugin_store as ps
        monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
        configure_plugin_store("DU1234567")
        expected = tmp_path / ".ib_plugin_store_DU1234567.db"
        assert ps._plugin_store.db_path == expected

    def test_replaces_singleton(self, monkeypatch):
        """After configure_plugin_store, get_plugin_store returns the new instance."""
        import ib.plugin_store as ps
        original = ps._plugin_store
        configure_plugin_store("DU9999999")
        new_store = ps._plugin_store
        assert new_store is not original

    def test_live_and_paper_get_different_stores(self, tmp_path, monkeypatch):
        """Paper and live accounts must produce distinct DB paths."""
        import ib.plugin_store as ps
        monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)

        configure_plugin_store("DU1234567")
        paper_path = ps._plugin_store.db_path

        configure_plugin_store("U1234567")
        live_path = ps._plugin_store.db_path

        assert paper_path != live_path
        assert "DU1234567" in str(paper_path)
        assert "U1234567"  in str(live_path)

    def test_new_store_is_functional(self, tmp_path, monkeypatch):
        """The store created by configure_plugin_store should accept registry writes."""
        import ib.plugin_store as ps
        monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
        configure_plugin_store("DU0000001")
        store = get_plugin_store()
        ok = store.upsert_registry("myslot", "/plugins/foo.py", "1.0", "unloaded")
        assert ok
        assert store.get_registry_entry("myslot") is not None
