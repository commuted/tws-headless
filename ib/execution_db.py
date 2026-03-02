"""
execution_db.py - SQLite database for storing execution and commission reports

Stores execution details and commission reports from IB for historical tracking,
P&L analysis, and cost basis calculation.
"""

import asyncio
import sqlite3
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Default database path
DEFAULT_DB_PATH = Path.home() / ".ib_executions.db"


@dataclass
class ExecutionRecord:
    """Execution report record"""
    exec_id: str
    order_id: int
    symbol: str
    sec_type: str
    exchange: str
    currency: str
    shares: float
    cum_qty: float
    avg_price: float
    side: str  # BOT or SLD
    timestamp: datetime
    account: str = ""
    local_symbol: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "exec_id": self.exec_id,
            "order_id": self.order_id,
            "symbol": self.symbol,
            "sec_type": self.sec_type,
            "exchange": self.exchange,
            "currency": self.currency,
            "shares": self.shares,
            "cum_qty": self.cum_qty,
            "avg_price": self.avg_price,
            "side": self.side,
            "timestamp": self.timestamp.isoformat(),
            "account": self.account,
            "local_symbol": self.local_symbol,
        }


@dataclass
class CommissionRecord:
    """Commission report record"""
    exec_id: str
    commission: float
    currency: str
    realized_pnl: Optional[float]  # None if IB returns max float
    timestamp: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            "exec_id": self.exec_id,
            "commission": self.commission,
            "currency": self.currency,
            "realized_pnl": self.realized_pnl,
            "timestamp": self.timestamp.isoformat(),
        }


class ExecutionDatabase:
    """
    SQLite database for execution and commission reports.

    Usage:
        db = ExecutionDatabase()
        db.insert_execution(exec_record)
        db.insert_commission(comm_record)

        # Query executions
        executions = db.get_executions_by_symbol("QQQ")

        # Get cost basis
        cost_basis = db.get_cost_basis("EUR.USD")
    """

    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize the execution database.

        Args:
            db_path: Path to SQLite database file. Defaults to ~/.ib_executions.db
        """
        self.db_path = db_path or DEFAULT_DB_PATH
        self._init_database()

    def _init_database(self):
        """Create database tables if they don't exist."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Executions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS executions (
                    exec_id TEXT PRIMARY KEY,
                    order_id INTEGER,
                    symbol TEXT NOT NULL,
                    sec_type TEXT NOT NULL,
                    exchange TEXT,
                    currency TEXT,
                    local_symbol TEXT,
                    shares REAL NOT NULL,
                    cum_qty REAL,
                    avg_price REAL NOT NULL,
                    side TEXT NOT NULL,
                    account TEXT,
                    timestamp TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Commissions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS commissions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    exec_id TEXT NOT NULL,
                    commission REAL NOT NULL,
                    currency TEXT,
                    realized_pnl REAL,
                    timestamp TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (exec_id) REFERENCES executions(exec_id)
                )
            """)

            # Indexes for common queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_executions_symbol
                ON executions(symbol)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_executions_timestamp
                ON executions(timestamp)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_executions_side
                ON executions(side)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_commissions_exec_id
                ON commissions(exec_id)
            """)

            conn.commit()
            logger.debug(f"Initialized execution database at {self.db_path}")

    async def insert_execution_async(self, execution: ExecutionRecord) -> bool:
        """Async wrapper — run sync insert in a thread pool."""
        return await asyncio.to_thread(self.insert_execution, execution)

    async def insert_commission_async(self, commission: CommissionRecord) -> bool:
        """Async wrapper — run sync insert in a thread pool."""
        return await asyncio.to_thread(self.insert_commission, commission)

    def insert_execution(self, execution: ExecutionRecord) -> bool:
        """
        Insert an execution record.

        Args:
            execution: ExecutionRecord to insert

        Returns:
            True if inserted, False if duplicate exec_id
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR IGNORE INTO executions
                    (exec_id, order_id, symbol, sec_type, exchange, currency,
                     local_symbol, shares, cum_qty, avg_price, side, account, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    execution.exec_id,
                    execution.order_id,
                    execution.symbol,
                    execution.sec_type,
                    execution.exchange,
                    execution.currency,
                    execution.local_symbol,
                    execution.shares,
                    execution.cum_qty,
                    execution.avg_price,
                    execution.side,
                    execution.account,
                    execution.timestamp.isoformat(),
                ))
                conn.commit()

                if cursor.rowcount > 0:
                    logger.info(f"Stored execution: {execution.exec_id} {execution.side} {execution.shares} {execution.symbol} @ {execution.avg_price}")
                    return True
                return False

        except Exception as e:
            logger.error(f"Failed to insert execution: {e}")
            return False

    def insert_commission(self, commission: CommissionRecord) -> bool:
        """
        Insert a commission record.

        Args:
            commission: CommissionRecord to insert

        Returns:
            True if inserted successfully
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Check if we already have this commission (by exec_id)
                cursor.execute(
                    "SELECT id FROM commissions WHERE exec_id = ?",
                    (commission.exec_id,)
                )
                if cursor.fetchone():
                    logger.debug(f"Commission already exists for exec_id: {commission.exec_id}")
                    return False

                cursor.execute("""
                    INSERT INTO commissions
                    (exec_id, commission, currency, realized_pnl, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    commission.exec_id,
                    commission.commission,
                    commission.currency,
                    commission.realized_pnl,
                    commission.timestamp.isoformat(),
                ))
                conn.commit()

                logger.info(f"Stored commission: {commission.exec_id} ${commission.commission} {commission.currency}")
                return True

        except Exception as e:
            logger.error(f"Failed to insert commission: {e}")
            return False

    def get_executions_by_symbol(
        self,
        symbol: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[ExecutionRecord]:
        """
        Get executions for a symbol.

        Args:
            symbol: Trading symbol
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            List of ExecutionRecord
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                query = "SELECT * FROM executions WHERE symbol = ?"
                params = [symbol]

                if start_date:
                    query += " AND timestamp >= ?"
                    params.append(start_date.isoformat())
                if end_date:
                    query += " AND timestamp <= ?"
                    params.append(end_date.isoformat())

                query += " ORDER BY timestamp DESC"

                cursor.execute(query, params)
                rows = cursor.fetchall()

                return [
                    ExecutionRecord(
                        exec_id=row["exec_id"],
                        order_id=row["order_id"],
                        symbol=row["symbol"],
                        sec_type=row["sec_type"],
                        exchange=row["exchange"] or "",
                        currency=row["currency"] or "",
                        local_symbol=row["local_symbol"] or "",
                        shares=row["shares"],
                        cum_qty=row["cum_qty"] or 0,
                        avg_price=row["avg_price"],
                        side=row["side"],
                        account=row["account"] or "",
                        timestamp=datetime.fromisoformat(row["timestamp"]),
                    )
                    for row in rows
                ]

        except Exception as e:
            logger.error(f"Failed to get executions: {e}")
            return []

    def get_all_executions(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[ExecutionRecord]:
        """Get all executions with optional date filter."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                query = "SELECT * FROM executions WHERE 1=1"
                params = []

                if start_date:
                    query += " AND timestamp >= ?"
                    params.append(start_date.isoformat())
                if end_date:
                    query += " AND timestamp <= ?"
                    params.append(end_date.isoformat())

                query += " ORDER BY timestamp DESC LIMIT ?"
                params.append(limit)

                cursor.execute(query, params)
                rows = cursor.fetchall()

                return [
                    ExecutionRecord(
                        exec_id=row["exec_id"],
                        order_id=row["order_id"],
                        symbol=row["symbol"],
                        sec_type=row["sec_type"],
                        exchange=row["exchange"] or "",
                        currency=row["currency"] or "",
                        local_symbol=row["local_symbol"] or "",
                        shares=row["shares"],
                        cum_qty=row["cum_qty"] or 0,
                        avg_price=row["avg_price"],
                        side=row["side"],
                        account=row["account"] or "",
                        timestamp=datetime.fromisoformat(row["timestamp"]),
                    )
                    for row in rows
                ]

        except Exception as e:
            logger.error(f"Failed to get executions: {e}")
            return []

    def get_commission_for_execution(self, exec_id: str) -> Optional[CommissionRecord]:
        """Get commission record for an execution."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                cursor.execute(
                    "SELECT * FROM commissions WHERE exec_id = ?",
                    (exec_id,)
                )
                row = cursor.fetchone()

                if row:
                    return CommissionRecord(
                        exec_id=row["exec_id"],
                        commission=row["commission"],
                        currency=row["currency"] or "USD",
                        realized_pnl=row["realized_pnl"],
                        timestamp=datetime.fromisoformat(row["timestamp"]),
                    )
                return None

        except Exception as e:
            logger.error(f"Failed to get commission: {e}")
            return None

    def get_cost_basis(self, symbol: str) -> Optional[float]:
        """
        Calculate average cost basis for a symbol based on BOT executions.

        Returns weighted average price of all buy executions.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT SUM(shares * avg_price) / SUM(shares) as avg_cost
                    FROM executions
                    WHERE (symbol = ? OR local_symbol = ?) AND side = 'BOT'
                """, (symbol, symbol))

                row = cursor.fetchone()
                if row and row[0]:
                    return row[0]
                return None

        except Exception as e:
            logger.error(f"Failed to get cost basis: {e}")
            return None

    def get_total_commission(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> float:
        """Get total commission, optionally filtered by symbol and date range."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                if symbol:
                    query = """
                        SELECT COALESCE(SUM(c.commission), 0)
                        FROM commissions c
                        JOIN executions e ON c.exec_id = e.exec_id
                        WHERE (e.symbol = ? OR e.local_symbol = ?)
                    """
                    params = [symbol, symbol]
                else:
                    query = "SELECT COALESCE(SUM(commission), 0) FROM commissions WHERE 1=1"
                    params = []

                if start_date:
                    query += " AND c.timestamp >= ?" if symbol else " AND timestamp >= ?"
                    params.append(start_date.isoformat())
                if end_date:
                    query += " AND c.timestamp <= ?" if symbol else " AND timestamp <= ?"
                    params.append(end_date.isoformat())

                cursor.execute(query, params)
                row = cursor.fetchone()
                return row[0] if row else 0.0

        except Exception as e:
            logger.error(f"Failed to get total commission: {e}")
            return 0.0

    def get_position_summary(self, symbol: str) -> Dict[str, Any]:
        """
        Get position summary for a symbol including:
        - Total bought/sold
        - Average cost
        - Total commission
        - Net position
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Get buy stats
                cursor.execute("""
                    SELECT COALESCE(SUM(shares), 0),
                           COALESCE(SUM(shares * avg_price), 0)
                    FROM executions
                    WHERE (symbol = ? OR local_symbol = ?) AND side = 'BOT'
                """, (symbol, symbol))
                buy_row = cursor.fetchone()
                total_bought = buy_row[0]
                total_buy_value = buy_row[1]

                # Get sell stats
                cursor.execute("""
                    SELECT COALESCE(SUM(shares), 0),
                           COALESCE(SUM(shares * avg_price), 0)
                    FROM executions
                    WHERE (symbol = ? OR local_symbol = ?) AND side = 'SLD'
                """, (symbol, symbol))
                sell_row = cursor.fetchone()
                total_sold = sell_row[0]
                total_sell_value = sell_row[1]

                # Get commission
                cursor.execute("""
                    SELECT COALESCE(SUM(c.commission), 0)
                    FROM commissions c
                    JOIN executions e ON c.exec_id = e.exec_id
                    WHERE e.symbol = ? OR e.local_symbol = ?
                """, (symbol, symbol))
                comm_row = cursor.fetchone()
                total_commission = comm_row[0]

                net_position = total_bought - total_sold
                avg_cost = total_buy_value / total_bought if total_bought > 0 else 0

                return {
                    "symbol": symbol,
                    "total_bought": total_bought,
                    "total_sold": total_sold,
                    "net_position": net_position,
                    "avg_buy_price": avg_cost,
                    "total_buy_value": total_buy_value,
                    "total_sell_value": total_sell_value,
                    "total_commission": total_commission,
                }

        except Exception as e:
            logger.error(f"Failed to get position summary: {e}")
            return {}

    def get_execution_count(self) -> int:
        """Get total number of executions in database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM executions")
                return cursor.fetchone()[0]
        except:
            return 0

    def get_commission_count(self) -> int:
        """Get total number of commission records in database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM commissions")
                return cursor.fetchone()[0]
        except:
            return 0


# Global database instance
_execution_db: Optional[ExecutionDatabase] = None


def get_execution_db() -> ExecutionDatabase:
    """Get the global execution database instance."""
    global _execution_db
    if _execution_db is None:
        _execution_db = ExecutionDatabase()
    return _execution_db
