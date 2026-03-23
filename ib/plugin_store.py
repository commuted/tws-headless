"""
plugin_store.py — SQLite registry for plugin instance tracking.
Stores which plugins should be auto-reloaded on engine restart.
"""

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path.home() / ".ib_plugin_store.db"

SCHEMA_VERSION = 3


class PluginStore:
    """
    SQLite registry for plugin instance tracking.

    Stores which plugin instances should be auto-reloaded on engine restart,
    along with their last known lifecycle status and per-instance config.

    Usage:
        store = get_plugin_store()
        store.upsert_registry("my_slot", "plugins.my_plugin", "1.0", "started")
        entry = store.get_registry_entry("my_slot")
    """

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or DEFAULT_DB_PATH
        self._init_database()

    # =========================================================================
    # Schema init
    # =========================================================================

    def _init_database(self):
        """Create registry tables and set WAL mode."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            cursor = conn.cursor()

            cursor.executescript("""
                CREATE TABLE IF NOT EXISTS schema_versions (
                    component TEXT PRIMARY KEY,
                    version   INTEGER NOT NULL,
                    applied_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS plugin_registry (
                    slot        TEXT PRIMARY KEY,
                    class_path  TEXT NOT NULL,
                    version     TEXT NOT NULL DEFAULT '',
                    status      TEXT NOT NULL DEFAULT 'unloaded'
                                CHECK(status IN ('unloaded','started','frozen')),
                    config      TEXT,
                    created_at  TEXT NOT NULL,
                    updated_at  TEXT NOT NULL
                );
            """)

            # Upsert schema version
            cursor.execute(
                """INSERT INTO schema_versions (component, version, applied_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(component) DO UPDATE SET
                     version=excluded.version, applied_at=excluded.applied_at
                   WHERE excluded.version > version""",
                ("plugin_store", SCHEMA_VERSION, datetime.now().isoformat()),
            )

            conn.commit()
            logger.debug(f"PluginStore initialized at {self.db_path}")

    def _conn(self) -> sqlite3.Connection:
        """Open a WAL-mode connection (caller must use as context manager)."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        return conn

    # =========================================================================
    # Registry
    # =========================================================================

    def upsert_registry(
        self,
        slot: str,
        class_path: str,
        version: str,
        status: str,
        config: Any = None,
    ) -> bool:
        """INSERT OR REPLACE a plugin registry row, preserving created_at."""
        try:
            now = datetime.now().isoformat()
            config_json = json.dumps(config, default=str) if config is not None else None
            with self._conn() as conn:
                conn.execute(
                    """INSERT INTO plugin_registry
                       (slot, class_path, version, status, config, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(slot) DO UPDATE SET
                         class_path=excluded.class_path,
                         version=excluded.version,
                         status=excluded.status,
                         config=excluded.config,
                         updated_at=excluded.updated_at""",
                    (slot, class_path, version, status, config_json, now, now),
                )
            logger.debug(f"Upserted registry entry for slot '{slot}' status={status}")
            return True
        except Exception as e:
            logger.error(f"Failed to upsert registry for slot '{slot}': {e}")
            return False

    def get_registry_entry(self, slot: str) -> Optional[Dict]:
        """Return registry row as dict (config parsed from JSON), or None."""
        try:
            with self._conn() as conn:
                row = conn.execute(
                    "SELECT * FROM plugin_registry WHERE slot = ?", (slot,)
                ).fetchone()
            if row is None:
                return None
            d = dict(row)
            if d.get("config"):
                d["config"] = json.loads(d["config"])
            return d
        except Exception as e:
            logger.error(f"Failed to get registry entry for slot '{slot}': {e}")
            return None

    def list_registry(self, status_filter: Optional[str] = None) -> List[Dict]:
        """Return all registry rows, optionally filtered by status."""
        try:
            with self._conn() as conn:
                if status_filter:
                    rows = conn.execute(
                        "SELECT * FROM plugin_registry WHERE status = ? ORDER BY slot",
                        (status_filter,),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT * FROM plugin_registry ORDER BY slot"
                    ).fetchall()
            result = []
            for row in rows:
                d = dict(row)
                if d.get("config"):
                    d["config"] = json.loads(d["config"])
                result.append(d)
            return result
        except Exception as e:
            logger.error(f"Failed to list registry: {e}")
            return []

    def delete_registry_entry(self, slot: str) -> bool:
        """Remove one registry row."""
        try:
            with self._conn() as conn:
                conn.execute(
                    "DELETE FROM plugin_registry WHERE slot = ?", (slot,)
                )
            logger.debug(f"Deleted registry entry for slot '{slot}'")
            return True
        except Exception as e:
            logger.error(f"Failed to delete registry entry for slot '{slot}': {e}")
            return False


# =============================================================================
# Global singleton
# =============================================================================

_plugin_store: Optional[PluginStore] = None


def get_plugin_store() -> PluginStore:
    """Return the global PluginStore singleton."""
    global _plugin_store
    if _plugin_store is None:
        _plugin_store = PluginStore()
    return _plugin_store


def configure_plugin_store(account_id: str) -> None:
    """Re-initialise the global PluginStore singleton keyed to account_id."""
    global _plugin_store
    db_path = Path.home() / f".ib_plugin_store_{account_id}.db"
    _plugin_store = PluginStore(db_path)
