"""
plugin_execution_log.py - Plugin-level execution logging with commission tracking

Provides data structures and utilities for logging plugin executions with
commission apportionment for performance tracking and P&L analysis.
"""

import json
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Dict, Any


@dataclass
class PluginExecutionLog:
    """
    Single execution log entry for a plugin.

    Captures all details needed to track P&L and performance by plugin,
    including commission apportionment for multi-plugin orders.
    """
    # Timing
    timestamp: datetime

    # Plugin identification
    plugin_name: str

    # Order/execution identification
    order_id: int
    exec_id: str

    # Trade details
    symbol: str
    action: str  # BUY, SELL
    quantity: int
    fill_price: float

    # Costs and P&L
    commission: float = 0.0
    fees: float = 0.0
    realized_pnl: float = 0.0

    # Multi-plugin order info
    is_combined_order: bool = False
    allocation_pct: float = 1.0  # 1.0 = 100%
    total_order_quantity: int = 0

    # Position tracking
    position_before: int = 0
    position_after: int = 0
    avg_cost_before: float = 0.0
    avg_cost_after: float = 0.0

    @property
    def net_amount(self) -> float:
        """
        Calculate net amount after commission and fees.

        For BUY: negative (cash outflow)
        For SELL: positive (cash inflow)
        """
        gross = self.quantity * self.fill_price
        total_costs = self.commission + self.fees

        if self.action == "BUY":
            return -(gross + total_costs)
        else:  # SELL
            return gross - total_costs

    @property
    def gross_amount(self) -> float:
        """Gross trade amount before commission and fees"""
        gross = self.quantity * self.fill_price
        if self.action == "BUY":
            return -gross
        return gross

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "ts": self.timestamp.isoformat(),
            "plugin": self.plugin_name,
            "order_id": self.order_id,
            "exec_id": self.exec_id,
            "symbol": self.symbol,
            "action": self.action,
            "qty": self.quantity,
            "price": self.fill_price,
            "commission": self.commission,
            "fees": self.fees,
            "pnl": self.realized_pnl,
            "net": self.net_amount,
            "combined": self.is_combined_order,
            "alloc_pct": self.allocation_pct,
            "total_qty": self.total_order_quantity,
            "pos_before": self.position_before,
            "pos_after": self.position_after,
            "avg_cost_before": self.avg_cost_before,
            "avg_cost_after": self.avg_cost_after,
        }

    def to_json(self) -> str:
        """Convert to JSON string for log file"""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PluginExecutionLog":
        """Create from dictionary (e.g., parsed from JSON)"""
        return cls(
            timestamp=datetime.fromisoformat(data["ts"]),
            plugin_name=data["plugin"],
            order_id=data["order_id"],
            exec_id=data["exec_id"],
            symbol=data["symbol"],
            action=data["action"],
            quantity=data["qty"],
            fill_price=data["price"],
            commission=data.get("commission", 0.0),
            fees=data.get("fees", 0.0),
            realized_pnl=data.get("pnl", 0.0),
            is_combined_order=data.get("combined", False),
            allocation_pct=data.get("alloc_pct", 1.0),
            total_order_quantity=data.get("total_qty", 0),
            position_before=data.get("pos_before", 0),
            position_after=data.get("pos_after", 0),
            avg_cost_before=data.get("avg_cost_before", 0.0),
            avg_cost_after=data.get("avg_cost_after", 0.0),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "PluginExecutionLog":
        """Create from JSON string"""
        return cls.from_dict(json.loads(json_str))


class ExecutionLogWriter:
    """
    Writes plugin execution logs to JSONL files with daily rotation.

    Log files are stored in the format:
        logs/plugin_executions.jsonl (current day)
        logs/plugin_executions.YYYY-MM-DD.jsonl (previous days)
    """

    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._current_date: Optional[date] = None
        self._current_file: Optional[Path] = None

    def _get_log_file(self) -> Path:
        """Get the current log file path, rotating if needed"""
        today = date.today()

        if self._current_date != today:
            # Rotate previous day's file if exists
            if self._current_date is not None:
                current_file = self.log_dir / "plugin_executions.jsonl"
                if current_file.exists():
                    archive_name = f"plugin_executions.{self._current_date.isoformat()}.jsonl"
                    archive_file = self.log_dir / archive_name
                    current_file.rename(archive_file)

            self._current_date = today
            self._current_file = self.log_dir / "plugin_executions.jsonl"

        return self._current_file

    def write(self, log_entry: PluginExecutionLog) -> bool:
        """
        Write a log entry to the current log file.

        Returns True on success, False on failure.
        """
        try:
            log_file = self._get_log_file()
            with open(log_file, "a") as f:
                f.write(log_entry.to_json() + "\n")
            return True
        except Exception:
            return False

    def write_batch(self, log_entries: List[PluginExecutionLog]) -> int:
        """
        Write multiple log entries.

        Returns the number of entries successfully written.
        """
        count = 0
        for entry in log_entries:
            if self.write(entry):
                count += 1
        return count


class ExecutionLogReader:
    """
    Reads and parses plugin execution logs from JSONL files.
    """

    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)

    def read_current(self) -> List[PluginExecutionLog]:
        """Read all entries from the current day's log file"""
        current_file = self.log_dir / "plugin_executions.jsonl"
        return self._read_file(current_file)

    def read_date(self, log_date: date) -> List[PluginExecutionLog]:
        """Read all entries from a specific date's log file"""
        if log_date == date.today():
            return self.read_current()

        archive_file = self.log_dir / f"plugin_executions.{log_date.isoformat()}.jsonl"
        return self._read_file(archive_file)

    def read_range(
        self,
        start_date: date,
        end_date: date,
    ) -> List[PluginExecutionLog]:
        """Read all entries from a date range (inclusive)"""
        entries = []
        current = start_date

        while current <= end_date:
            entries.extend(self.read_date(current))
            current = date(
                current.year,
                current.month,
                current.day + 1 if current.day < 28 else 1,
            )
            # Simple date increment
            from datetime import timedelta
            current = start_date + timedelta(days=(current - start_date).days + 1)
            if current > end_date:
                break

        return entries

    def read_all(self) -> List[PluginExecutionLog]:
        """Read all entries from all log files"""
        entries = []

        # Read current file
        entries.extend(self.read_current())

        # Read all archived files
        for log_file in sorted(self.log_dir.glob("plugin_executions.*.jsonl")):
            entries.extend(self._read_file(log_file))

        # Sort by timestamp
        entries.sort(key=lambda e: e.timestamp)
        return entries

    def read_plugin(self, plugin_name: str) -> List[PluginExecutionLog]:
        """Read all entries for a specific plugin"""
        all_entries = self.read_all()
        return [e for e in all_entries if e.plugin_name == plugin_name]

    def _read_file(self, file_path: Path) -> List[PluginExecutionLog]:
        """Read all entries from a single log file"""
        entries = []

        if not file_path.exists():
            return entries

        try:
            with open(file_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            entry = PluginExecutionLog.from_json(line)
                            entries.append(entry)
                        except (json.JSONDecodeError, KeyError):
                            # Skip malformed lines
                            continue
        except Exception:
            pass

        return entries

    def list_available_dates(self) -> List[date]:
        """List all dates with available log files"""
        dates = []

        # Check current file
        current_file = self.log_dir / "plugin_executions.jsonl"
        if current_file.exists():
            dates.append(date.today())

        # Check archived files
        for log_file in self.log_dir.glob("plugin_executions.*.jsonl"):
            try:
                date_str = log_file.stem.replace("plugin_executions.", "")
                log_date = date.fromisoformat(date_str)
                dates.append(log_date)
            except ValueError:
                continue

        return sorted(dates)
