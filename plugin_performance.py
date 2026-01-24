"""
plugin_performance.py - Plugin performance tracking and P&L analysis

Provides tools for analyzing plugin trading performance including:
- P&L calculation from execution logs
- Win/loss tracking
- Commission cost analysis
- Performance metrics by plugin
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from collections import defaultdict

from .plugin_execution_log import PluginExecutionLog, ExecutionLogReader


@dataclass
class PluginPnLSummary:
    """P&L summary for a plugin"""
    plugin_name: str
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    gross_pnl: float = 0.0
    total_commission: float = 0.0
    total_fees: float = 0.0
    net_pnl: float = 0.0
    total_volume: float = 0.0  # Total dollar volume traded

    @property
    def win_rate(self) -> float:
        """Calculate win rate as percentage"""
        if self.total_trades == 0:
            return 0.0
        return (self.winning_trades / self.total_trades) * 100.0

    @property
    def loss_rate(self) -> float:
        """Calculate loss rate as percentage"""
        if self.total_trades == 0:
            return 0.0
        return (self.losing_trades / self.total_trades) * 100.0

    @property
    def avg_trade_pnl(self) -> float:
        """Average P&L per trade"""
        if self.total_trades == 0:
            return 0.0
        return self.net_pnl / self.total_trades

    @property
    def commission_ratio(self) -> float:
        """Commission as percentage of gross P&L"""
        if abs(self.gross_pnl) < 0.01:
            return 0.0
        return (self.total_commission / abs(self.gross_pnl)) * 100.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "plugin_name": self.plugin_name,
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": self.win_rate,
            "gross_pnl": self.gross_pnl,
            "total_commission": self.total_commission,
            "total_fees": self.total_fees,
            "net_pnl": self.net_pnl,
            "total_volume": self.total_volume,
            "avg_trade_pnl": self.avg_trade_pnl,
            "commission_ratio": self.commission_ratio,
        }


@dataclass
class SymbolPerformance:
    """Performance metrics for a single symbol"""
    symbol: str
    trades: int = 0
    buy_quantity: int = 0
    sell_quantity: int = 0
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    realized_pnl: float = 0.0
    commission: float = 0.0

    @property
    def net_pnl(self) -> float:
        """Net P&L after commission"""
        return self.realized_pnl - self.commission

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "symbol": self.symbol,
            "trades": self.trades,
            "buy_quantity": self.buy_quantity,
            "sell_quantity": self.sell_quantity,
            "buy_volume": self.buy_volume,
            "sell_volume": self.sell_volume,
            "realized_pnl": self.realized_pnl,
            "commission": self.commission,
            "net_pnl": self.net_pnl,
        }


class PluginPerformanceTracker:
    """
    Tracks and analyzes plugin trading performance.

    Reads execution logs and calculates performance metrics including
    P&L, win rate, commission costs, and per-symbol breakdowns.

    Usage:
        tracker = PluginPerformanceTracker()

        # Get P&L for a specific plugin
        pnl = tracker.get_plugin_pnl("momentum_5day")
        print(f"Net P&L: ${pnl['net_pnl']:.2f}")

        # Get metrics for all plugins
        metrics = tracker.get_all_plugin_metrics()

        # Get detailed breakdown
        report = tracker.generate_report("momentum_5day")
    """

    def __init__(self, log_dir: str = "logs"):
        """
        Initialize performance tracker.

        Args:
            log_dir: Directory containing execution log files
        """
        self._log_reader = ExecutionLogReader(log_dir)
        self._cache: Dict[str, Any] = {}
        self._cache_timestamp: Optional[datetime] = None
        self._cache_ttl_seconds: float = 60.0  # Cache for 1 minute

    def _is_cache_valid(self) -> bool:
        """Check if cache is still valid"""
        if self._cache_timestamp is None:
            return False
        elapsed = (datetime.now() - self._cache_timestamp).total_seconds()
        return elapsed < self._cache_ttl_seconds

    def _invalidate_cache(self):
        """Invalidate the cache"""
        self._cache.clear()
        self._cache_timestamp = None

    def record_execution(self, log_entry: PluginExecutionLog):
        """
        Record a new execution.

        Note: This doesn't write to the log file (that's done by ExecutionLogWriter).
        This just invalidates the cache so new data will be loaded.

        Args:
            log_entry: The execution log entry
        """
        self._invalidate_cache()

    def get_plugin_pnl(self, plugin_name: str) -> Dict[str, Any]:
        """
        Get P&L summary for a specific plugin.

        Args:
            plugin_name: Name of the plugin

        Returns:
            Dictionary with P&L metrics
        """
        summary = self._calculate_pnl_summary(plugin_name)
        return summary.to_dict()

    def get_plugin_metrics(self, plugin_name: str) -> Dict[str, Any]:
        """
        Get comprehensive metrics for a plugin.

        Args:
            plugin_name: Name of the plugin

        Returns:
            Dictionary with all metrics including per-symbol breakdown
        """
        logs = self._log_reader.read_plugin(plugin_name)

        if not logs:
            return {
                "plugin_name": plugin_name,
                "pnl": PluginPnLSummary(plugin_name).to_dict(),
                "by_symbol": {},
                "recent_trades": [],
            }

        # Calculate overall P&L
        pnl_summary = self._calculate_pnl_summary_from_logs(plugin_name, logs)

        # Calculate per-symbol breakdown
        by_symbol = self._calculate_symbol_breakdown(logs)

        # Get recent trades
        recent = logs[-10:] if len(logs) > 10 else logs
        recent_trades = [log.to_dict() for log in recent]

        return {
            "plugin_name": plugin_name,
            "pnl": pnl_summary.to_dict(),
            "by_symbol": {sym: perf.to_dict() for sym, perf in by_symbol.items()},
            "recent_trades": recent_trades,
        }

    def get_all_plugin_metrics(self) -> Dict[str, Dict[str, Any]]:
        """
        Get metrics for all plugins with execution history.

        Returns:
            Dictionary mapping plugin_name to metrics
        """
        logs = self._log_reader.read_all()

        # Group by plugin
        by_plugin: Dict[str, List[PluginExecutionLog]] = defaultdict(list)
        for log in logs:
            by_plugin[log.plugin_name].append(log)

        metrics = {}
        for plugin_name, plugin_logs in by_plugin.items():
            pnl_summary = self._calculate_pnl_summary_from_logs(plugin_name, plugin_logs)
            metrics[plugin_name] = {
                "pnl": pnl_summary.to_dict(),
                "trade_count": len(plugin_logs),
            }

        return metrics

    def generate_report(
        self,
        plugin_name: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """
        Generate a detailed performance report for a plugin.

        Args:
            plugin_name: Name of the plugin
            start_date: Start date for report (None = all time)
            end_date: End date for report (None = today)

        Returns:
            Comprehensive performance report dictionary
        """
        logs = self._log_reader.read_plugin(plugin_name)

        # Filter by date range if specified
        if start_date or end_date:
            filtered = []
            for log in logs:
                log_date = log.timestamp.date()
                if start_date and log_date < start_date:
                    continue
                if end_date and log_date > end_date:
                    continue
                filtered.append(log)
            logs = filtered

        if not logs:
            return {
                "plugin": plugin_name,
                "period": {
                    "start": start_date.isoformat() if start_date else None,
                    "end": end_date.isoformat() if end_date else None,
                },
                "summary": PluginPnLSummary(plugin_name).to_dict(),
                "by_symbol": {},
                "daily_pnl": {},
            }

        # Calculate summary
        summary = self._calculate_pnl_summary_from_logs(plugin_name, logs)

        # Calculate per-symbol breakdown
        by_symbol = self._calculate_symbol_breakdown(logs)

        # Calculate daily P&L
        daily_pnl = self._calculate_daily_pnl(logs)

        return {
            "plugin": plugin_name,
            "period": {
                "start": (start_date or logs[0].timestamp.date()).isoformat(),
                "end": (end_date or logs[-1].timestamp.date()).isoformat(),
            },
            "summary": summary.to_dict(),
            "by_symbol": {sym: perf.to_dict() for sym, perf in by_symbol.items()},
            "daily_pnl": daily_pnl,
        }

    def _calculate_pnl_summary(self, plugin_name: str) -> PluginPnLSummary:
        """Calculate P&L summary for a plugin from logs"""
        logs = self._log_reader.read_plugin(plugin_name)
        return self._calculate_pnl_summary_from_logs(plugin_name, logs)

    def _calculate_pnl_summary_from_logs(
        self,
        plugin_name: str,
        logs: List[PluginExecutionLog],
    ) -> PluginPnLSummary:
        """Calculate P&L summary from a list of execution logs"""
        summary = PluginPnLSummary(plugin_name=plugin_name)

        for log in logs:
            summary.total_trades += 1
            summary.total_commission += log.commission
            summary.total_fees += log.fees
            summary.gross_pnl += log.realized_pnl
            summary.total_volume += abs(log.quantity * log.fill_price)

            # Track wins/losses (only for closing trades with realized P&L)
            if log.realized_pnl > 0:
                summary.winning_trades += 1
            elif log.realized_pnl < 0:
                summary.losing_trades += 1

        summary.net_pnl = summary.gross_pnl - summary.total_commission - summary.total_fees

        return summary

    def _calculate_symbol_breakdown(
        self,
        logs: List[PluginExecutionLog],
    ) -> Dict[str, SymbolPerformance]:
        """Calculate per-symbol performance breakdown"""
        by_symbol: Dict[str, SymbolPerformance] = {}

        for log in logs:
            if log.symbol not in by_symbol:
                by_symbol[log.symbol] = SymbolPerformance(symbol=log.symbol)

            perf = by_symbol[log.symbol]
            perf.trades += 1
            perf.realized_pnl += log.realized_pnl
            perf.commission += log.commission

            trade_value = log.quantity * log.fill_price
            if log.action == "BUY":
                perf.buy_quantity += log.quantity
                perf.buy_volume += trade_value
            else:
                perf.sell_quantity += log.quantity
                perf.sell_volume += trade_value

        return by_symbol

    def _calculate_daily_pnl(
        self,
        logs: List[PluginExecutionLog],
    ) -> Dict[str, Dict[str, float]]:
        """Calculate daily P&L from logs"""
        daily: Dict[str, Dict[str, float]] = {}

        for log in logs:
            day_str = log.timestamp.date().isoformat()
            if day_str not in daily:
                daily[day_str] = {
                    "gross_pnl": 0.0,
                    "commission": 0.0,
                    "net_pnl": 0.0,
                    "trades": 0,
                }

            daily[day_str]["gross_pnl"] += log.realized_pnl
            daily[day_str]["commission"] += log.commission
            daily[day_str]["trades"] += 1

        # Calculate net P&L for each day
        for day_data in daily.values():
            day_data["net_pnl"] = day_data["gross_pnl"] - day_data["commission"]

        return daily

    def export_logs(
        self,
        plugin_name: Optional[str] = None,
        format: str = "json",
    ) -> str:
        """
        Export execution logs.

        Args:
            plugin_name: Filter by plugin (None = all)
            format: Output format ("json" or "csv")

        Returns:
            Formatted string of execution logs
        """
        import json

        if plugin_name:
            logs = self._log_reader.read_plugin(plugin_name)
        else:
            logs = self._log_reader.read_all()

        if format == "json":
            return json.dumps([log.to_dict() for log in logs], indent=2)
        elif format == "csv":
            if not logs:
                return ""

            # CSV header
            headers = [
                "timestamp", "plugin", "order_id", "exec_id", "symbol",
                "action", "quantity", "price", "commission", "pnl", "net"
            ]
            lines = [",".join(headers)]

            for log in logs:
                row = [
                    log.timestamp.isoformat(),
                    log.plugin_name,
                    str(log.order_id),
                    log.exec_id,
                    log.symbol,
                    log.action,
                    str(log.quantity),
                    f"{log.fill_price:.2f}",
                    f"{log.commission:.4f}",
                    f"{log.realized_pnl:.2f}",
                    f"{log.net_amount:.2f}",
                ]
                lines.append(",".join(row))

            return "\n".join(lines)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def list_plugins(self) -> List[str]:
        """
        List all plugins with execution history.

        Returns:
            List of plugin names
        """
        logs = self._log_reader.read_all()
        plugins = set(log.plugin_name for log in logs)
        return sorted(plugins)
