"""
execution_db.py - SQLite database for storing execution and commission reports

Stores execution details and commission reports from IB for historical tracking,
P&L analysis, and cost basis calculation.
"""

import asyncio
import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Default database path
DEFAULT_DB_PATH = Path.home() / ".ib_executions.db"



def _ts(raw: str) -> datetime:
    """A stored timestamp as a UTC-aware datetime.

    Rows written before 2026-09-23 are NAIVE, and worse, they are naive in
    whatever zone the writing host used — Pacific for the ones inherited from
    descartes, UTC for anything EC2 wrote. Returning them as-is beside the
    aware values written since would raise on the first comparison between
    the two, which is exactly the sort of break that surfaces inside a cost
    report rather than at a boundary.

    So a naive value is read as UTC. That is a documented assumption, not a
    recovery: for the legacy rows the true zone is unknown and the stored
    instant was never the execution time anyway (see
    Portfolio._parse_ib_exec_time).
    """
    dt = datetime.fromisoformat(raw)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)



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
    # Price the strategy saw when it decided to trade, captured at placement.
    # Without it a fill can only be compared to itself: commission is knowable
    # from the fill alone, but slippage — usually the larger cost — needs the
    # decision price, and it is gone by the time the execution arrives.
    decision_price: Optional[float] = None

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
            "decision_price": self.decision_price,
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
                    decision_price REAL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Migration for databases created before decision_price existed.
            # ADD COLUMN is cheap and non-rewriting in SQLite; existing rows
            # read NULL, which the report renders as "unknown" rather than
            # pretending the slippage was zero.
            cols = {r[1] for r in cursor.execute("PRAGMA table_info(executions)")}
            if "decision_price" not in cols:
                cursor.execute("ALTER TABLE executions ADD COLUMN decision_price REAL")

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
                     local_symbol, shares, cum_qty, avg_price, side, account,
                     timestamp, decision_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    execution.decision_price,
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
                        timestamp=_ts(row["timestamp"]),
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
                        timestamp=_ts(row["timestamp"]),
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
                        timestamp=_ts(row["timestamp"]),
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

    def get_cost_report(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Trading cost broken into its parts, per order and in aggregate.

        get_commission_report answers "what were the fees". That is only half
        the cost and usually the smaller half, and it sits next to realized
        P&L — which is not a cost at all, and on a seeded position dwarfs the
        fees by three orders of magnitude. Read for cost signal, that report
        misleads.

        This separates:

          commission  knowable from the fill alone, normalised to bp of
                      notional so it can be compared against an edge. The raw
                      dollar figure hides that a per-order minimum makes a
                      small fill several times more expensive than a large one.
          slippage    fill against the price the strategy decided at, signed
                      so positive always means it cost money: a buy filled
                      above the decision price, or a sell filled below.

        Orders placed before decision_price was recorded report slippage as
        None rather than zero — unknown and free are different claims.

        Round trips pair each closing fill against the open it closes, FIFO
        per symbol, so gross P&L can be stated next to the cost of achieving
        it. A position transferred in rather than bought has no opening fill
        and is reported as an unmatched close.
        """
        rows = self._cost_rows(symbol, start_date, end_date)
        orders: Dict[int, Dict[str, Any]] = {}
        for r in rows:
            o = orders.setdefault(r["order_id"], {
                "order_id": r["order_id"], "symbol": r["symbol"],
                "side": r["side"], "timestamp": r["timestamp"],
                "shares": 0.0, "notional": 0.0, "commission": 0.0,
                "decision_price": r["decision_price"], "fills": 0,
            })
            o["shares"] += r["shares"]
            o["notional"] += r["shares"] * r["avg_price"]
            o["commission"] += r["commission"] or 0.0
            o["fills"] += 1
            if o["decision_price"] is None:
                o["decision_price"] = r["decision_price"]

        out: List[Dict[str, Any]] = []
        for o in sorted(orders.values(), key=lambda x: x["timestamp"]):
            n = o["notional"]
            o["avg_price"] = n / o["shares"] if o["shares"] else 0.0
            o["commission_bp"] = (o["commission"] / n * 1e4) if n else None
            dp = o["decision_price"]
            if dp:
                sign = 1.0 if o["side"] == "BOT" else -1.0
                o["slippage_bp"] = sign * (o["avg_price"] - dp) / dp * 1e4
            else:
                o["slippage_bp"] = None
            o["total_cost_bp"] = (
                (o["commission_bp"] or 0.0) + o["slippage_bp"]
                if o["slippage_bp"] is not None else None)
            out.append(o)

        tot_n = sum(o["notional"] for o in out)
        tot_c = sum(o["commission"] for o in out)
        known = [o for o in out if o["slippage_bp"] is not None]
        slip_n = sum(o["notional"] for o in known)
        slip_bp = (sum(o["slippage_bp"] * o["notional"] for o in known) / slip_n
                   if slip_n else None)
        return {
            "orders": out,
            "round_trips": self._round_trips(out),
            "totals": {
                "notional": tot_n,
                "commission": tot_c,
                "commission_bp": (tot_c / tot_n * 1e4) if tot_n else None,
                "slippage_bp": slip_bp,
                "slippage_coverage": (slip_n / tot_n) if tot_n else 0.0,
                "total_cost_bp": ((tot_c / tot_n * 1e4) + slip_bp)
                                 if (tot_n and slip_bp is not None) else None,
                "order_count": len(out),
            },
        }

    def _cost_rows(self, symbol, start_date, end_date) -> List[Dict[str, Any]]:
        where, params = ["1=1"], []
        if symbol:
            where.append("(e.symbol = ? OR e.local_symbol = ?)")
            params.extend([symbol, symbol])
        if start_date:
            where.append("e.timestamp >= ?")
            params.append(start_date.isoformat())
        if end_date:
            where.append("e.timestamp <= ?")
            params.append(end_date.isoformat())
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(r) for r in conn.execute(
                f"""select e.order_id, e.symbol, e.side, e.shares, e.avg_price,
                           e.timestamp, e.decision_price, c.commission
                    from executions e
                    left join commissions c on c.exec_id = e.exec_id
                    where {' and '.join(where)}
                    order by e.timestamp""", params)]

    @staticmethod
    def _round_trips(orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """FIFO-match closes against opens, per symbol."""
        books: Dict[str, List[Dict[str, Any]]] = {}
        trips: List[Dict[str, Any]] = []
        for o in orders:
            book = books.setdefault(o["symbol"], [])
            opposite = [x for x in book if x["side"] != o["side"]]
            qty = o["shares"]
            if not opposite:
                book.append(dict(o, remaining=qty))
                continue
            while qty > 0 and opposite:
                open_o = opposite[0]
                take = min(qty, open_o["remaining"])
                buy, sell = ((open_o, o) if open_o["side"] == "BOT"
                             else (o, open_o))
                gross = (sell["avg_price"] - buy["avg_price"]) * take
                comm = (open_o["commission"] * take / open_o["shares"]
                        + o["commission"] * take / o["shares"])
                trips.append({
                    "symbol": o["symbol"], "qty": take,
                    "opened": open_o["timestamp"], "closed": o["timestamp"],
                    "buy_price": buy["avg_price"], "sell_price": sell["avg_price"],
                    "gross_pnl": gross, "commission": comm,
                    "net_pnl": gross - comm,
                })
                qty -= take
                open_o["remaining"] -= take
                if open_o["remaining"] <= 0:
                    book.remove(open_o)
                    opposite.pop(0)
            if qty > 0:
                book.append(dict(o, remaining=qty))
        return trips

    def get_commission_report(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Commission and fee totals with a per-symbol breakdown.

        get_total_commission answers "how much in total"; this answers "where
        did it go", which is the question worth asking when fees are eating a
        strategy's edge. Commissions carry no symbol of their own, so every row
        is joined back to its execution.

        Rows whose execution is missing (a commission that arrived without its
        fill, which IB does occasionally) are still counted in the totals but
        appear under symbol ``None`` in the breakdown, rather than being
        silently dropped — an unattributable fee is still money.

        Returns a dict with total_commission, total_realized_pnl,
        record_count, currency (when unambiguous), and by_symbol, sorted by
        commission, largest first.
        """
        report: Dict[str, Any] = {
            "symbol": symbol,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
            "total_commission": 0.0,
            "total_realized_pnl": 0.0,
            "record_count": 0,
            "currency": None,
            "by_symbol": [],
        }

        where = ["1=1"]
        params: List[Any] = []
        if symbol:
            where.append("(e.symbol = ? OR e.local_symbol = ?)")
            params.extend([symbol, symbol])
        if start_date:
            where.append("c.timestamp >= ?")
            params.append(start_date.isoformat())
        if end_date:
            where.append("c.timestamp <= ?")
            params.append(end_date.isoformat())
        clause = " AND ".join(where)

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                cursor.execute(f"""
                    SELECT COALESCE(SUM(c.commission), 0),
                           COALESCE(SUM(c.realized_pnl), 0),
                           COUNT(*)
                    FROM commissions c
                    LEFT JOIN executions e ON c.exec_id = e.exec_id
                    WHERE {clause}
                """, params)
                row = cursor.fetchone()
                if row:
                    report["total_commission"] = row[0]
                    report["total_realized_pnl"] = row[1]
                    report["record_count"] = row[2]

                cursor.execute(f"""
                    SELECT DISTINCT c.currency
                    FROM commissions c
                    LEFT JOIN executions e ON c.exec_id = e.exec_id
                    WHERE {clause} AND c.currency IS NOT NULL
                """, params)
                currencies = [r[0] for r in cursor.fetchall()]
                if len(currencies) == 1:
                    report["currency"] = currencies[0]

                cursor.execute(f"""
                    SELECT e.symbol,
                           COALESCE(SUM(c.commission), 0),
                           COALESCE(SUM(c.realized_pnl), 0),
                           COUNT(*)
                    FROM commissions c
                    LEFT JOIN executions e ON c.exec_id = e.exec_id
                    WHERE {clause}
                    GROUP BY e.symbol
                    ORDER BY SUM(c.commission) DESC
                """, params)
                report["by_symbol"] = [
                    {
                        "symbol": r[0],
                        "commission": r[1],
                        "realized_pnl": r[2],
                        "count": r[3],
                    }
                    for r in cursor.fetchall()
                ]

        except Exception as e:
            logger.error(f"Failed to build commission report: {e}")

        return report

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


def configure_execution_db(account_id: str) -> None:
    """Re-initialise the global ExecutionDatabase singleton keyed to account_id.

    Keeps paper and live fills/commissions in separate databases. Mirrors
    ib.plugin_store.configure_plugin_store. Call once the active account is known.
    """
    global _execution_db
    try:
        from .environment import execution_db_path_for
    except ImportError:  # module loaded standalone (e.g. test bootstrap) — no package context
        from ib.environment import execution_db_path_for
    _execution_db = ExecutionDatabase(execution_db_path_for(account_id))
