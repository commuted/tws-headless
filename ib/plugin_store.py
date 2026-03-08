"""
plugin_store.py - SQLite persistence for plugin state, holdings, and forex cost basis

Replaces undocumented JSON files (state.json, holdings.json, ~/.ib_forex_cost_basis.json)
with a single atomic SQLite database at ~/.ib_plugin_store.db.

Connection pattern: connection-per-call with WAL mode (same as execution_db.py)
"""

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from plugins.base import Holdings

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path.home() / ".ib_plugin_store.db"

SCHEMA_VERSION = 2


class PluginStore:
    """
    SQLite store for plugin state, holdings, and forex cost basis.

    Usage:
        store = get_plugin_store()
        store.save_state("my_plugin", "1.0", {"key": "value"})
        state = store.load_state("my_plugin")
    """

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or DEFAULT_DB_PATH
        self._init_database()

    # =========================================================================
    # Schema init
    # =========================================================================

    def _init_database(self):
        """Create all tables and set WAL mode."""
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

                CREATE TABLE IF NOT EXISTS plugin_states (
                    plugin_name    TEXT PRIMARY KEY,
                    plugin_version TEXT,
                    state          TEXT NOT NULL,
                    saved_at       TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS plugin_holdings (
                    plugin_name  TEXT PRIMARY KEY,
                    initial_cash REAL NOT NULL DEFAULT 0.0,
                    current_cash REAL NOT NULL DEFAULT 0.0,
                    created_at   TEXT,
                    last_updated TEXT
                );

                CREATE TABLE IF NOT EXISTS plugin_positions (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    plugin_name  TEXT NOT NULL,
                    slot         TEXT NOT NULL CHECK(slot IN ('initial','current')),
                    symbol       TEXT NOT NULL,
                    quantity     REAL NOT NULL DEFAULT 0.0,
                    cost_basis   REAL NOT NULL DEFAULT 0.0,
                    current_price REAL NOT NULL DEFAULT 0.0,
                    market_value  REAL NOT NULL DEFAULT 0.0,
                    UNIQUE(plugin_name, slot, symbol),
                    FOREIGN KEY(plugin_name) REFERENCES plugin_holdings(plugin_name)
                );

                CREATE TABLE IF NOT EXISTS forex_cost_basis (
                    currency   TEXT PRIMARY KEY,
                    avg_cost   REAL NOT NULL,
                    quantity   REAL NOT NULL DEFAULT 0.0,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS plugin_instruments (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    instance_key TEXT NOT NULL,
                    symbol       TEXT NOT NULL,
                    name         TEXT NOT NULL DEFAULT '',
                    weight       REAL NOT NULL DEFAULT 0.0,
                    min_weight   REAL NOT NULL DEFAULT 0.0,
                    max_weight   REAL NOT NULL DEFAULT 100.0,
                    enabled      INTEGER NOT NULL DEFAULT 1,
                    exchange     TEXT NOT NULL DEFAULT 'SMART',
                    currency     TEXT NOT NULL DEFAULT 'USD',
                    sec_type     TEXT NOT NULL DEFAULT 'STK',
                    UNIQUE(instance_key, symbol)
                );

                CREATE TABLE IF NOT EXISTS migration_log (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_file TEXT UNIQUE NOT NULL,
                    plugin_name TEXT,
                    migrated_at TEXT NOT NULL
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
    # State
    # =========================================================================

    def save_state(self, plugin_name: str, plugin_version: str, state_dict: Dict[str, Any]) -> bool:
        """INSERT OR REPLACE plugin state."""
        try:
            with self._conn() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO plugin_states
                       (plugin_name, plugin_version, state, saved_at)
                       VALUES (?, ?, ?, ?)""",
                    (plugin_name, plugin_version, json.dumps(state_dict, default=str),
                     datetime.now().isoformat()),
                )
            logger.debug(f"Saved state for plugin '{plugin_name}'")
            return True
        except Exception as e:
            logger.error(f"Failed to save state for '{plugin_name}': {e}")
            return False

    def load_state(self, plugin_name: str) -> Optional[Dict[str, Any]]:
        """Return state dict, or None if not found."""
        try:
            with self._conn() as conn:
                row = conn.execute(
                    "SELECT state, saved_at FROM plugin_states WHERE plugin_name = ?",
                    (plugin_name,),
                ).fetchone()

            if row is None:
                return None

            state = json.loads(row["state"])
            logger.debug(f"Loaded state for '{plugin_name}' (saved {row['saved_at']})")
            return state
        except Exception as e:
            logger.error(f"Failed to load state for '{plugin_name}': {e}")
            return None

    def clear_state(self, plugin_name: str) -> bool:
        """Delete state row for plugin."""
        try:
            with self._conn() as conn:
                conn.execute(
                    "DELETE FROM plugin_states WHERE plugin_name = ?",
                    (plugin_name,),
                )
            logger.debug(f"Cleared state for '{plugin_name}'")
            return True
        except Exception as e:
            logger.error(f"Failed to clear state for '{plugin_name}': {e}")
            return False

    # =========================================================================
    # Holdings
    # =========================================================================

    def save_holdings(self, holdings: "Holdings") -> bool:
        """Replace holdings header + all position rows atomically."""
        try:
            with self._conn() as conn:
                now = datetime.now().isoformat()
                created = (
                    holdings.created_at.isoformat()
                    if holdings.created_at else now
                )
                updated = (
                    holdings.last_updated.isoformat()
                    if holdings.last_updated else now
                )

                # Upsert header row
                conn.execute(
                    """INSERT OR REPLACE INTO plugin_holdings
                       (plugin_name, initial_cash, current_cash, created_at, last_updated)
                       VALUES (?, ?, ?, ?, ?)""",
                    (holdings.plugin_name, holdings.initial_cash,
                     holdings.current_cash, created, updated),
                )

                # Replace all positions atomically
                conn.execute(
                    "DELETE FROM plugin_positions WHERE plugin_name = ?",
                    (holdings.plugin_name,),
                )

                for pos in holdings.initial_positions:
                    conn.execute(
                        """INSERT INTO plugin_positions
                           (plugin_name, slot, symbol, quantity, cost_basis,
                            current_price, market_value)
                           VALUES (?, 'initial', ?, ?, ?, ?, ?)""",
                        (holdings.plugin_name, pos.symbol, pos.quantity,
                         pos.cost_basis, pos.current_price, pos.market_value),
                    )

                for pos in holdings.current_positions:
                    conn.execute(
                        """INSERT INTO plugin_positions
                           (plugin_name, slot, symbol, quantity, cost_basis,
                            current_price, market_value)
                           VALUES (?, 'current', ?, ?, ?, ?, ?)""",
                        (holdings.plugin_name, pos.symbol, pos.quantity,
                         pos.cost_basis, pos.current_price, pos.market_value),
                    )

            logger.debug(f"Saved holdings for '{holdings.plugin_name}'")
            return True
        except Exception as e:
            logger.error(f"Failed to save holdings for '{holdings.plugin_name}': {e}")
            return False

    def load_holdings(self, plugin_name: str) -> Optional["Holdings"]:
        """Return Holdings object, or None if not found."""
        # Import here to avoid circular imports
        from plugins.base import Holdings, HoldingPosition

        try:
            with self._conn() as conn:
                header = conn.execute(
                    "SELECT * FROM plugin_holdings WHERE plugin_name = ?",
                    (plugin_name,),
                ).fetchone()

                if header is None:
                    return None

                rows = conn.execute(
                    "SELECT * FROM plugin_positions WHERE plugin_name = ? ORDER BY id",
                    (plugin_name,),
                ).fetchall()

            initial_positions = [
                HoldingPosition(
                    symbol=r["symbol"],
                    quantity=r["quantity"],
                    cost_basis=r["cost_basis"],
                    current_price=r["current_price"],
                    market_value=r["market_value"],
                )
                for r in rows if r["slot"] == "initial"
            ]
            current_positions = [
                HoldingPosition(
                    symbol=r["symbol"],
                    quantity=r["quantity"],
                    cost_basis=r["cost_basis"],
                    current_price=r["current_price"],
                    market_value=r["market_value"],
                )
                for r in rows if r["slot"] == "current"
            ]

            created_at = (
                datetime.fromisoformat(header["created_at"])
                if header["created_at"] else None
            )
            last_updated = (
                datetime.fromisoformat(header["last_updated"])
                if header["last_updated"] else None
            )

            return Holdings(
                plugin_name=header["plugin_name"],
                initial_cash=header["initial_cash"],
                initial_positions=initial_positions,
                current_cash=header["current_cash"],
                current_positions=current_positions,
                created_at=created_at,
                last_updated=last_updated,
            )

        except Exception as e:
            logger.error(f"Failed to load holdings for '{plugin_name}': {e}")
            return None

    def delete_holdings(self, plugin_name: str) -> bool:
        """Remove holdings header and all position rows."""
        try:
            with self._conn() as conn:
                conn.execute(
                    "DELETE FROM plugin_positions WHERE plugin_name = ?",
                    (plugin_name,),
                )
                conn.execute(
                    "DELETE FROM plugin_holdings WHERE plugin_name = ?",
                    (plugin_name,),
                )
            logger.debug(f"Deleted holdings for '{plugin_name}'")
            return True
        except Exception as e:
            logger.error(f"Failed to delete holdings for '{plugin_name}': {e}")
            return False

    # =========================================================================
    # Instruments
    # =========================================================================

    def save_instruments(self, instance_key: str, instruments: list) -> bool:
        """Replace all instruments for this instance atomically."""
        try:
            with self._conn() as conn:
                conn.execute(
                    "DELETE FROM plugin_instruments WHERE instance_key = ?",
                    (instance_key,),
                )
                conn.executemany(
                    """INSERT INTO plugin_instruments
                       (instance_key, symbol, name, weight, min_weight, max_weight,
                        enabled, exchange, currency, sec_type)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [
                        (instance_key, inst.symbol, inst.name, inst.weight,
                         inst.min_weight, inst.max_weight,
                         1 if inst.enabled else 0,
                         inst.exchange, inst.currency, inst.sec_type)
                        for inst in instruments
                    ],
                )
            logger.debug(f"Saved {len(instruments)} instruments for '{instance_key}'")
            return True
        except Exception as e:
            logger.error(f"Failed to save instruments for '{instance_key}': {e}")
            return False

    def load_instruments(self, instance_key: str) -> Optional[list]:
        """Return list of PluginInstrument, or None if none stored."""
        from plugins.base import PluginInstrument
        try:
            with self._conn() as conn:
                rows = conn.execute(
                    "SELECT * FROM plugin_instruments WHERE instance_key = ? ORDER BY id",
                    (instance_key,),
                ).fetchall()
            if not rows:
                return None
            return [
                PluginInstrument(
                    symbol=r["symbol"],
                    name=r["name"],
                    weight=r["weight"],
                    min_weight=r["min_weight"],
                    max_weight=r["max_weight"],
                    enabled=bool(r["enabled"]),
                    exchange=r["exchange"],
                    currency=r["currency"],
                    sec_type=r["sec_type"],
                )
                for r in rows
            ]
        except Exception as e:
            logger.error(f"Failed to load instruments for '{instance_key}': {e}")
            return None

    def upsert_instrument(self, instance_key: str, instrument) -> bool:
        """Insert or replace a single instrument entry."""
        try:
            with self._conn() as conn:
                conn.execute(
                    """INSERT INTO plugin_instruments
                       (instance_key, symbol, name, weight, min_weight, max_weight,
                        enabled, exchange, currency, sec_type)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(instance_key, symbol) DO UPDATE SET
                         name=excluded.name, weight=excluded.weight,
                         min_weight=excluded.min_weight, max_weight=excluded.max_weight,
                         enabled=excluded.enabled, exchange=excluded.exchange,
                         currency=excluded.currency, sec_type=excluded.sec_type""",
                    (instance_key, instrument.symbol, instrument.name,
                     instrument.weight, instrument.min_weight, instrument.max_weight,
                     1 if instrument.enabled else 0,
                     instrument.exchange, instrument.currency, instrument.sec_type),
                )
            return True
        except Exception as e:
            logger.error(f"Failed to upsert instrument '{instrument.symbol}' for '{instance_key}': {e}")
            return False

    def remove_instrument(self, instance_key: str, symbol: str) -> bool:
        """Delete a single instrument by symbol."""
        try:
            with self._conn() as conn:
                conn.execute(
                    "DELETE FROM plugin_instruments WHERE instance_key = ? AND symbol = ?",
                    (instance_key, symbol.upper()),
                )
            return True
        except Exception as e:
            logger.error(f"Failed to remove instrument '{symbol}' for '{instance_key}': {e}")
            return False

    def set_instrument_enabled(self, instance_key: str, symbol: str, enabled: bool) -> bool:
        """Enable or disable a single instrument without removing it."""
        try:
            with self._conn() as conn:
                conn.execute(
                    """UPDATE plugin_instruments SET enabled = ?
                       WHERE instance_key = ? AND symbol = ?""",
                    (1 if enabled else 0, instance_key, symbol.upper()),
                )
            return True
        except Exception as e:
            logger.error(f"Failed to set enabled for '{symbol}' in '{instance_key}': {e}")
            return False

    def clear_instruments(self, instance_key: str) -> bool:
        """Remove all instruments for this instance."""
        try:
            with self._conn() as conn:
                conn.execute(
                    "DELETE FROM plugin_instruments WHERE instance_key = ?",
                    (instance_key,),
                )
            logger.debug(f"Cleared instruments for '{instance_key}'")
            return True
        except Exception as e:
            logger.error(f"Failed to clear instruments for '{instance_key}': {e}")
            return False

    # =========================================================================
    # Forex cost basis
    # =========================================================================

    def save_forex_cost_basis(self, basis_dict: Dict[str, float]) -> bool:
        """Replace entire forex_cost_basis table from dict {currency: avg_cost}."""
        try:
            now = datetime.now().isoformat()
            with self._conn() as conn:
                conn.execute("DELETE FROM forex_cost_basis")
                conn.executemany(
                    """INSERT INTO forex_cost_basis (currency, avg_cost, quantity, updated_at)
                       VALUES (?, ?, 0.0, ?)""",
                    [(currency, cost, now) for currency, cost in basis_dict.items()],
                )
            logger.debug(f"Saved forex cost basis: {list(basis_dict.keys())}")
            return True
        except Exception as e:
            logger.error(f"Failed to save forex cost basis: {e}")
            return False

    def set_forex_cost_basis_entry(
        self, currency: str, avg_cost: float, quantity: float = 0.0
    ) -> bool:
        """Upsert a single currency entry."""
        try:
            with self._conn() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO forex_cost_basis
                       (currency, avg_cost, quantity, updated_at)
                       VALUES (?, ?, ?, ?)""",
                    (currency, avg_cost, quantity, datetime.now().isoformat()),
                )
            return True
        except Exception as e:
            logger.error(f"Failed to set forex entry for '{currency}': {e}")
            return False

    def load_forex_cost_basis(self) -> Dict[str, float]:
        """Return {currency: avg_cost} dict, empty if nothing stored."""
        try:
            with self._conn() as conn:
                rows = conn.execute(
                    "SELECT currency, avg_cost FROM forex_cost_basis"
                ).fetchall()
            return {r["currency"]: r["avg_cost"] for r in rows}
        except Exception as e:
            logger.error(f"Failed to load forex cost basis: {e}")
            return {}

    # =========================================================================
    # Migration helpers
    # =========================================================================

    def _already_migrated(self, conn: sqlite3.Connection, source_file: str) -> bool:
        row = conn.execute(
            "SELECT id FROM migration_log WHERE source_file = ?", (source_file,)
        ).fetchone()
        return row is not None

    def _record_migration(
        self, conn: sqlite3.Connection, source_file: str, plugin_name: Optional[str]
    ):
        conn.execute(
            """INSERT OR IGNORE INTO migration_log (source_file, plugin_name, migrated_at)
               VALUES (?, ?, ?)""",
            (source_file, plugin_name, datetime.now().isoformat()),
        )

    def migrate_instruments_from_json(self, instance_key: str, base_path: Path) -> None:
        """
        Import instruments.json once if not already migrated.

        Idempotent — safe to call on every plugin load.
        JSON file left in place as backup.
        """
        instruments_file = Path(base_path) / "instruments.json"
        source_key = f"instruments:{instance_key}:{instruments_file}"

        with self._conn() as conn:
            if self._already_migrated(conn, source_key):
                return

            if not instruments_file.exists():
                self._record_migration(conn, source_key, instance_key)
                return

            try:
                from plugins.base import PluginInstrument
                with open(instruments_file) as f:
                    data = json.load(f)
                instruments = [
                    PluginInstrument.from_dict(d)
                    for d in data.get("instruments", [])
                ]
                conn.execute(
                    "DELETE FROM plugin_instruments WHERE instance_key = ?",
                    (instance_key,),
                )
                conn.executemany(
                    """INSERT OR IGNORE INTO plugin_instruments
                       (instance_key, symbol, name, weight, min_weight, max_weight,
                        enabled, exchange, currency, sec_type)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [
                        (instance_key, i.symbol, i.name, i.weight,
                         i.min_weight, i.max_weight,
                         1 if i.enabled else 0,
                         i.exchange, i.currency, i.sec_type)
                        for i in instruments
                    ],
                )
                self._record_migration(conn, source_key, instance_key)
                logger.info(
                    f"Migrated {instruments_file} → plugin_instruments "
                    f"({len(instruments)} instruments)"
                )
            except Exception as e:
                logger.warning(f"Failed to migrate {instruments_file}: {e}")

    def migrate_from_json(self, plugin_name: str, base_path: Path) -> None:
        """
        Import state.json and holdings.json once if not already in migration_log.

        Idempotent — safe to call on every plugin start.
        JSON source files are left in place as backup.
        """
        state_file = Path(base_path) / "state.json"
        holdings_file = Path(base_path) / "holdings.json"

        with self._conn() as conn:
            # --- state.json ---
            source_key = str(state_file)
            if not self._already_migrated(conn, source_key) and state_file.exists():
                try:
                    with open(state_file) as f:
                        data = json.load(f)
                    state = data.get("state", {})
                    version = data.get("plugin_version", "")
                    conn.execute(
                        """INSERT OR REPLACE INTO plugin_states
                           (plugin_name, plugin_version, state, saved_at)
                           VALUES (?, ?, ?, ?)""",
                        (plugin_name, version, json.dumps(state),
                         data.get("saved_at", datetime.now().isoformat())),
                    )
                    self._record_migration(conn, source_key, plugin_name)
                    logger.info(f"Migrated {state_file} → plugin_states")
                except Exception as e:
                    logger.warning(f"Failed to migrate {state_file}: {e}")
            elif not state_file.exists():
                # Record as "migrated" (nothing to migrate) so we skip next time
                if not self._already_migrated(conn, source_key):
                    self._record_migration(conn, source_key, plugin_name)

            # --- holdings.json ---
            source_key = str(holdings_file)
            if not self._already_migrated(conn, source_key) and holdings_file.exists():
                try:
                    from plugins.base import Holdings
                    with open(holdings_file) as f:
                        data = json.load(f)
                    holdings = Holdings.from_dict(data)
                    # Save via the same connection would be complex; use a separate call
                    conn.execute(
                        """INSERT OR REPLACE INTO plugin_holdings
                           (plugin_name, initial_cash, current_cash, created_at, last_updated)
                           VALUES (?, ?, ?, ?, ?)""",
                        (plugin_name, holdings.initial_cash, holdings.current_cash,
                         holdings.created_at.isoformat() if holdings.created_at else None,
                         holdings.last_updated.isoformat() if holdings.last_updated else None),
                    )
                    conn.execute(
                        "DELETE FROM plugin_positions WHERE plugin_name = ?",
                        (plugin_name,),
                    )
                    for pos in holdings.initial_positions:
                        conn.execute(
                            """INSERT OR IGNORE INTO plugin_positions
                               (plugin_name, slot, symbol, quantity, cost_basis,
                                current_price, market_value)
                               VALUES (?, 'initial', ?, ?, ?, ?, ?)""",
                            (plugin_name, pos.symbol, pos.quantity,
                             pos.cost_basis, pos.current_price, pos.market_value),
                        )
                    for pos in holdings.current_positions:
                        conn.execute(
                            """INSERT OR IGNORE INTO plugin_positions
                               (plugin_name, slot, symbol, quantity, cost_basis,
                                current_price, market_value)
                               VALUES (?, 'current', ?, ?, ?, ?, ?)""",
                            (plugin_name, pos.symbol, pos.quantity,
                             pos.cost_basis, pos.current_price, pos.market_value),
                        )
                    self._record_migration(conn, source_key, plugin_name)
                    logger.info(f"Migrated {holdings_file} → plugin_holdings")
                except Exception as e:
                    logger.warning(f"Failed to migrate {holdings_file}: {e}")
            elif not holdings_file.exists():
                if not self._already_migrated(conn, source_key):
                    self._record_migration(conn, source_key, plugin_name)

    def migrate_forex_cost_basis(self, path: Path) -> None:
        """
        Import ~/.ib_forex_cost_basis.json once if not already migrated.

        Idempotent — safe to call on every Portfolio init.
        """
        source_key = str(path)
        with self._conn() as conn:
            if self._already_migrated(conn, source_key):
                return

            if not path.exists():
                self._record_migration(conn, source_key, None)
                return

            try:
                with open(path) as f:
                    data = json.load(f)

                now = datetime.now().isoformat()
                conn.execute("DELETE FROM forex_cost_basis")
                conn.executemany(
                    """INSERT INTO forex_cost_basis
                       (currency, avg_cost, quantity, updated_at) VALUES (?, ?, 0.0, ?)""",
                    [(currency, cost, now) for currency, cost in data.items()],
                )
                self._record_migration(conn, source_key, None)
                logger.info(f"Migrated {path} → forex_cost_basis")
            except Exception as e:
                logger.warning(f"Failed to migrate {path}: {e}")


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
