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
        assert row[0] == 3

    def test_all_tables_exist(self, store):
        import sqlite3
        with sqlite3.connect(store.db_path) as conn:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
        expected = {
            "schema_versions", "plugin_states", "plugin_holdings",
            "plugin_positions", "forex_cost_basis", "migration_log",
            "plugin_registry",
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
        assert row[0] == 3
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


# =============================================================================
# 9. Instrument storage
# =============================================================================


class TestPluginStoreInstruments:
    """Tests for plugin_instruments CRUD methods."""

    def _make_instrument(self, symbol="SPY", name="S&P 500 ETF", weight=1.0,
                         min_weight=0.0, max_weight=100.0, enabled=True,
                         exchange="SMART", currency="USD", sec_type="STK"):
        from plugins.base import PluginInstrument
        return PluginInstrument(
            symbol=symbol, name=name, weight=weight,
            min_weight=min_weight, max_weight=max_weight,
            enabled=enabled, exchange=exchange, currency=currency,
            sec_type=sec_type,
        )

    def test_load_returns_none_when_empty(self, store):
        assert store.load_instruments("no_such_plugin") is None

    def test_save_and_load_roundtrip(self, store):
        instruments = [
            self._make_instrument("SPY"),
            self._make_instrument("QQQ", name="Nasdaq ETF", weight=0.5),
        ]
        store.save_instruments("myplugin", instruments)
        loaded = store.load_instruments("myplugin")
        assert loaded is not None
        assert len(loaded) == 2
        symbols = {i.symbol for i in loaded}
        assert symbols == {"SPY", "QQQ"}

    def test_save_overwrites_fully(self, store):
        """Second save replaces all rows — no stale entries."""
        store.save_instruments("myplugin", [self._make_instrument("SPY"), self._make_instrument("QQQ")])
        store.save_instruments("myplugin", [self._make_instrument("AAPL")])
        loaded = store.load_instruments("myplugin")
        assert len(loaded) == 1
        assert loaded[0].symbol == "AAPL"

    def test_upsert_adds_new(self, store):
        store.upsert_instrument("myplugin", self._make_instrument("SPY"))
        loaded = store.load_instruments("myplugin")
        assert loaded is not None and len(loaded) == 1

    def test_upsert_updates_existing(self, store):
        store.upsert_instrument("myplugin", self._make_instrument("SPY", weight=1.0))
        store.upsert_instrument("myplugin", self._make_instrument("SPY", weight=2.0))
        loaded = store.load_instruments("myplugin")
        assert len(loaded) == 1
        assert loaded[0].weight == pytest.approx(2.0)

    def test_remove_instrument(self, store):
        store.upsert_instrument("myplugin", self._make_instrument("SPY"))
        store.upsert_instrument("myplugin", self._make_instrument("QQQ"))
        store.remove_instrument("myplugin", "SPY")
        loaded = store.load_instruments("myplugin")
        assert len(loaded) == 1
        assert loaded[0].symbol == "QQQ"

    def test_remove_nonexistent_is_noop(self, store):
        store.remove_instrument("myplugin", "MISSING")  # should not raise

    def test_set_instrument_enabled(self, store):
        store.upsert_instrument("myplugin", self._make_instrument("SPY", enabled=True))
        store.set_instrument_enabled("myplugin", "SPY", False)
        loaded = store.load_instruments("myplugin")
        assert loaded[0].enabled is False
        store.set_instrument_enabled("myplugin", "SPY", True)
        loaded = store.load_instruments("myplugin")
        assert loaded[0].enabled is True

    def test_clear_instruments(self, store):
        store.upsert_instrument("myplugin", self._make_instrument("SPY"))
        store.upsert_instrument("myplugin", self._make_instrument("QQQ"))
        store.clear_instruments("myplugin")
        assert store.load_instruments("myplugin") is None

    def test_instruments_isolated_by_slot(self, store):
        store.upsert_instrument("slot_a", self._make_instrument("SPY"))
        store.upsert_instrument("slot_b", self._make_instrument("QQQ"))
        a = store.load_instruments("slot_a")
        b = store.load_instruments("slot_b")
        assert a[0].symbol == "SPY"
        assert b[0].symbol == "QQQ"

    def test_schema_version_is_3(self, store):
        import sqlite3
        with sqlite3.connect(store.db_path) as conn:
            row = conn.execute(
                "SELECT version FROM schema_versions WHERE component='plugin_store'"
            ).fetchone()
        assert row[0] == 3

    def test_migrate_instruments_from_json(self, store, tmp_path):
        (tmp_path / "instruments.json").write_text(json.dumps({
            "instruments": [
                {"symbol": "SPY", "name": "S&P ETF", "weight": 1.0,
                 "min_weight": 0.0, "max_weight": 100.0, "enabled": True,
                 "exchange": "SMART", "currency": "USD", "sec_type": "STK"},
            ]
        }))
        store.migrate_instruments_from_json("myplugin", str(tmp_path))
        loaded = store.load_instruments("myplugin")
        assert loaded is not None
        assert loaded[0].symbol == "SPY"

    def test_migrate_instruments_idempotent(self, store, tmp_path):
        (tmp_path / "instruments.json").write_text(json.dumps({
            "instruments": [{"symbol": "SPY", "name": "", "weight": 1.0,
                             "min_weight": 0.0, "max_weight": 100.0, "enabled": True,
                             "exchange": "SMART", "currency": "USD", "sec_type": "STK"}]
        }))
        store.migrate_instruments_from_json("myplugin", str(tmp_path))
        store.migrate_instruments_from_json("myplugin", str(tmp_path))
        loaded = store.load_instruments("myplugin")
        assert len(loaded) == 1  # not duplicated

    def test_migrate_instruments_missing_file_skipped(self, store, tmp_path):
        store.migrate_instruments_from_json("myplugin", str(tmp_path / "nonexistent"))
        # Should not raise; no instruments stored since file absent


# =============================================================================
# 7. Registry
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
        import sqlite3
        with sqlite3.connect(store.db_path) as conn:
            row = conn.execute(
                "SELECT version FROM schema_versions WHERE component = 'plugin_store'"
            ).fetchone()
        assert row[0] == 3


# =============================================================================
# 8. Export / Import
# =============================================================================


class TestExportImport:
    def _seed_full(self, store):
        """Seed a complete slot for roundtrip tests."""
        store.upsert_registry(
            slot="ship1",
            class_path="/plugins/ship.py",
            version="2.0",
            status="started",
            config={"symbol": "SPY"},
        )
        store.save_state("ship1", "2.0", {"rudder": 0.5, "keel": -100.0})
        from plugins.base import Holdings, HoldingPosition
        h = Holdings(
            plugin_name="ship1",
            initial_cash=50000.0,
            current_cash=48000.0,
            created_at=datetime(2024, 1, 1),
            last_updated=datetime(2024, 6, 1),
        )
        h.initial_positions = [
            HoldingPosition("SPY", 10, 450.0, 450.0, 4500.0)
        ]
        h.current_positions = [
            HoldingPosition("SPY", 10, 450.0, 460.0, 4600.0)
        ]
        store.save_holdings(h)
        from plugins.base import PluginInstrument
        store.save_instruments("ship1", [
            PluginInstrument(symbol="SPY", name="S&P ETF", weight=1.0)
        ])

    def test_export_not_found(self, store):
        result = store.export_instance("nonexistent_slot")
        assert result is None

    def test_export_contains_expected_keys(self, store):
        self._seed_full(store)
        doc = store.export_instance("ship1")
        assert doc is not None
        for key in ("slot", "class_path", "version", "config", "state",
                    "holdings", "instruments", "exported_at", "schema_version"):
            assert key in doc, f"Missing key: {key}"

    def test_export_roundtrip(self, store, tmp_path):
        """Export → import to fresh store → re-export — key fields match."""
        self._seed_full(store)
        doc = store.export_instance("ship1")
        assert doc is not None

        store2 = PluginStore(db_path=tmp_path / "import_test.db")
        assert store2.import_instance(doc)

        doc2 = store2.export_instance("ship1")
        assert doc2 is not None

        assert doc2["slot"] == doc["slot"]
        assert doc2["class_path"] == doc["class_path"]
        assert doc2["version"] == doc["version"]
        assert doc2["config"] == doc["config"]
        assert doc2["state"] == doc["state"]
        assert doc2["holdings"]["initial_funding"]["cash"] == \
               doc["holdings"]["initial_funding"]["cash"]
        assert len(doc2["instruments"]) == len(doc["instruments"])
        assert doc2["instruments"][0]["symbol"] == doc["instruments"][0]["symbol"]

    def test_import_forces_unloaded_status(self, store, tmp_path):
        self._seed_full(store)
        doc = store.export_instance("ship1")
        # doc has status='started' in registry; import should force 'unloaded'
        store2 = PluginStore(db_path=tmp_path / "import_unloaded.db")
        store2.import_instance(doc)
        entry = store2.get_registry_entry("ship1")
        assert entry["status"] == "unloaded"

    def test_import_missing_required_keys_returns_false(self, store):
        # Missing both slot and class_path
        assert store.import_instance({}) is False
        # Missing class_path
        assert store.import_instance({"slot": "s1"}) is False
        # Missing slot
        assert store.import_instance({"class_path": "/p.py"}) is False

    def test_import_no_state_no_holdings(self, store):
        """Import with only registry data (no state/holdings) should succeed."""
        doc = {
            "slot": "bare",
            "class_path": "/plugins/bare.py",
            "version": "1.0",
            "config": None,
            "state": None,
            "holdings": None,
            "instruments": [],
        }
        assert store.import_instance(doc)
        entry = store.get_registry_entry("bare")
        assert entry is not None
        assert entry["slot"] == "bare"

    def test_export_state_values(self, store):
        self._seed_full(store)
        doc = store.export_instance("ship1")
        assert doc["state"]["rudder"] == 0.5
        assert doc["state"]["keel"] == -100.0

    def test_export_holdings_values(self, store):
        self._seed_full(store)
        doc = store.export_instance("ship1")
        assert doc["holdings"]["initial_funding"]["cash"] == 50000.0
        assert doc["holdings"]["current_holdings"]["cash"] == 48000.0
        assert len(doc["holdings"]["initial_funding"]["positions"]) == 1
        assert doc["holdings"]["initial_funding"]["positions"][0]["symbol"] == "SPY"

    def test_export_instruments(self, store):
        self._seed_full(store)
        doc = store.export_instance("ship1")
        assert len(doc["instruments"]) == 1
        assert doc["instruments"][0]["symbol"] == "SPY"
        assert doc["instruments"][0]["name"] == "S&P ETF"
