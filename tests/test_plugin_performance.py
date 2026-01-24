"""
Tests for plugin_performance.py - Plugin performance tracking
"""

import pytest
import tempfile
import os
from datetime import datetime, date, timedelta
from pathlib import Path

from ib.plugin_execution_log import PluginExecutionLog, ExecutionLogWriter
from ib.plugin_performance import (
    PluginPerformanceTracker,
    PluginPnLSummary,
    SymbolPerformance,
)


class TestPluginPnLSummary:
    """Tests for PluginPnLSummary dataclass"""

    def test_create_summary(self):
        """Test creating a P&L summary"""
        summary = PluginPnLSummary(
            plugin_name="test_plugin",
            total_trades=10,
            winning_trades=6,
            losing_trades=4,
            gross_pnl=1000.0,
            total_commission=25.0,
            total_fees=5.0,
            net_pnl=970.0,
            total_volume=50000.0,
        )

        assert summary.plugin_name == "test_plugin"
        assert summary.total_trades == 10
        assert summary.net_pnl == 970.0

    def test_win_rate(self):
        """Test win rate calculation"""
        summary = PluginPnLSummary(
            plugin_name="test",
            total_trades=10,
            winning_trades=7,
            losing_trades=3,
        )

        assert summary.win_rate == 70.0

    def test_win_rate_zero_trades(self):
        """Test win rate with zero trades"""
        summary = PluginPnLSummary(plugin_name="test")
        assert summary.win_rate == 0.0

    def test_avg_trade_pnl(self):
        """Test average trade P&L calculation"""
        summary = PluginPnLSummary(
            plugin_name="test",
            total_trades=10,
            net_pnl=500.0,
        )

        assert summary.avg_trade_pnl == 50.0

    def test_commission_ratio(self):
        """Test commission ratio calculation"""
        summary = PluginPnLSummary(
            plugin_name="test",
            gross_pnl=1000.0,
            total_commission=50.0,
        )

        assert summary.commission_ratio == 5.0  # 50/1000 * 100

    def test_to_dict(self):
        """Test conversion to dictionary"""
        summary = PluginPnLSummary(
            plugin_name="test",
            total_trades=5,
            gross_pnl=200.0,
            total_commission=10.0,
            net_pnl=190.0,
        )

        d = summary.to_dict()
        assert d["plugin_name"] == "test"
        assert d["total_trades"] == 5
        assert d["gross_pnl"] == 200.0


class TestSymbolPerformance:
    """Tests for SymbolPerformance dataclass"""

    def test_create_symbol_performance(self):
        """Test creating symbol performance"""
        perf = SymbolPerformance(
            symbol="SPY",
            trades=20,
            buy_quantity=1000,
            sell_quantity=800,
            buy_volume=450000.0,
            sell_volume=368000.0,
            realized_pnl=5000.0,
            commission=50.0,
        )

        assert perf.symbol == "SPY"
        assert perf.trades == 20
        assert perf.realized_pnl == 5000.0

    def test_net_pnl(self):
        """Test net P&L calculation"""
        perf = SymbolPerformance(
            symbol="QQQ",
            realized_pnl=1000.0,
            commission=25.0,
        )

        assert perf.net_pnl == 975.0

    def test_to_dict(self):
        """Test conversion to dictionary"""
        perf = SymbolPerformance(symbol="AAPL", trades=5)

        d = perf.to_dict()
        assert d["symbol"] == "AAPL"
        assert d["trades"] == 5


class TestPluginPerformanceTracker:
    """Tests for PluginPerformanceTracker"""

    def _create_test_logs(self, tmpdir):
        """Create test log files"""
        writer = ExecutionLogWriter(tmpdir)

        # Create some test executions
        logs = [
            # Plugin A: 2 winning trades, 1 losing trade
            PluginExecutionLog(
                timestamp=datetime(2026, 1, 20, 10, 0, 0),
                plugin_name="plugin_a",
                order_id=1,
                exec_id="001",
                symbol="SPY",
                action="BUY",
                quantity=100,
                fill_price=450.0,
                commission=1.0,
                realized_pnl=0.0,  # Opening trade
            ),
            PluginExecutionLog(
                timestamp=datetime(2026, 1, 20, 14, 0, 0),
                plugin_name="plugin_a",
                order_id=2,
                exec_id="002",
                symbol="SPY",
                action="SELL",
                quantity=100,
                fill_price=455.0,
                commission=1.0,
                realized_pnl=500.0,  # Closing trade with profit
            ),
            PluginExecutionLog(
                timestamp=datetime(2026, 1, 21, 10, 0, 0),
                plugin_name="plugin_a",
                order_id=3,
                exec_id="003",
                symbol="QQQ",
                action="BUY",
                quantity=50,
                fill_price=380.0,
                commission=0.5,
                realized_pnl=0.0,
            ),
            PluginExecutionLog(
                timestamp=datetime(2026, 1, 21, 14, 0, 0),
                plugin_name="plugin_a",
                order_id=4,
                exec_id="004",
                symbol="QQQ",
                action="SELL",
                quantity=50,
                fill_price=375.0,
                commission=0.5,
                realized_pnl=-250.0,  # Closing trade with loss
            ),
            # Plugin B: 1 trade
            PluginExecutionLog(
                timestamp=datetime(2026, 1, 20, 11, 0, 0),
                plugin_name="plugin_b",
                order_id=5,
                exec_id="005",
                symbol="AAPL",
                action="BUY",
                quantity=25,
                fill_price=180.0,
                commission=0.25,
                realized_pnl=0.0,
            ),
        ]

        for log in logs:
            writer.write(log)

        return len(logs)

    def test_create_tracker(self):
        """Test creating a performance tracker"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = PluginPerformanceTracker(tmpdir)
            assert tracker is not None

    def test_get_plugin_pnl(self):
        """Test getting P&L for a plugin"""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_test_logs(tmpdir)
            tracker = PluginPerformanceTracker(tmpdir)

            pnl = tracker.get_plugin_pnl("plugin_a")

            assert pnl["plugin_name"] == "plugin_a"
            assert pnl["total_trades"] == 4
            # gross = 500 - 250 = 250
            assert pnl["gross_pnl"] == 250.0
            # commission = 1 + 1 + 0.5 + 0.5 = 3
            assert pnl["total_commission"] == 3.0

    def test_get_plugin_pnl_nonexistent(self):
        """Test getting P&L for nonexistent plugin"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = PluginPerformanceTracker(tmpdir)

            pnl = tracker.get_plugin_pnl("nonexistent")

            assert pnl["total_trades"] == 0
            assert pnl["net_pnl"] == 0.0

    def test_get_plugin_metrics(self):
        """Test getting comprehensive metrics"""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_test_logs(tmpdir)
            tracker = PluginPerformanceTracker(tmpdir)

            metrics = tracker.get_plugin_metrics("plugin_a")

            assert metrics["plugin_name"] == "plugin_a"
            assert "pnl" in metrics
            assert "by_symbol" in metrics
            assert "recent_trades" in metrics
            assert "SPY" in metrics["by_symbol"]
            assert "QQQ" in metrics["by_symbol"]

    def test_get_all_plugin_metrics(self):
        """Test getting metrics for all plugins"""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_test_logs(tmpdir)
            tracker = PluginPerformanceTracker(tmpdir)

            all_metrics = tracker.get_all_plugin_metrics()

            assert "plugin_a" in all_metrics
            assert "plugin_b" in all_metrics
            assert all_metrics["plugin_a"]["trade_count"] == 4
            assert all_metrics["plugin_b"]["trade_count"] == 1

    def test_generate_report(self):
        """Test generating a performance report"""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_test_logs(tmpdir)
            tracker = PluginPerformanceTracker(tmpdir)

            report = tracker.generate_report("plugin_a")

            assert report["plugin"] == "plugin_a"
            assert "period" in report
            assert "summary" in report
            assert "by_symbol" in report
            assert "daily_pnl" in report

    def test_generate_report_with_date_filter(self):
        """Test generating report with date filter"""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_test_logs(tmpdir)
            tracker = PluginPerformanceTracker(tmpdir)

            report = tracker.generate_report(
                "plugin_a",
                start_date=date(2026, 1, 20),
                end_date=date(2026, 1, 20),
            )

            # Should only include trades from Jan 20
            assert report["summary"]["total_trades"] == 2

    def test_export_logs_json(self):
        """Test exporting logs as JSON"""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_test_logs(tmpdir)
            tracker = PluginPerformanceTracker(tmpdir)

            json_str = tracker.export_logs("plugin_a", format="json")

            import json
            logs = json.loads(json_str)
            assert len(logs) == 4
            assert all(log["plugin"] == "plugin_a" for log in logs)

    def test_export_logs_csv(self):
        """Test exporting logs as CSV"""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_test_logs(tmpdir)
            tracker = PluginPerformanceTracker(tmpdir)

            csv_str = tracker.export_logs("plugin_a", format="csv")

            lines = csv_str.strip().split("\n")
            assert len(lines) == 5  # header + 4 data rows
            assert "timestamp" in lines[0]
            assert "plugin_a" in lines[1]

    def test_list_plugins(self):
        """Test listing plugins with execution history"""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_test_logs(tmpdir)
            tracker = PluginPerformanceTracker(tmpdir)

            plugins = tracker.list_plugins()

            assert "plugin_a" in plugins
            assert "plugin_b" in plugins
            assert len(plugins) == 2


class TestPerformanceCalculations:
    """Tests for specific performance calculations"""

    def test_win_loss_tracking(self):
        """Test that wins and losses are tracked correctly"""
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ExecutionLogWriter(tmpdir)

            # Create winning and losing trades
            logs = [
                PluginExecutionLog(
                    timestamp=datetime.now(),
                    plugin_name="test",
                    order_id=1,
                    exec_id="001",
                    symbol="SPY",
                    action="SELL",
                    quantity=100,
                    fill_price=455.0,
                    realized_pnl=500.0,  # Win
                ),
                PluginExecutionLog(
                    timestamp=datetime.now(),
                    plugin_name="test",
                    order_id=2,
                    exec_id="002",
                    symbol="QQQ",
                    action="SELL",
                    quantity=50,
                    fill_price=375.0,
                    realized_pnl=-100.0,  # Loss
                ),
                PluginExecutionLog(
                    timestamp=datetime.now(),
                    plugin_name="test",
                    order_id=3,
                    exec_id="003",
                    symbol="AAPL",
                    action="SELL",
                    quantity=25,
                    fill_price=185.0,
                    realized_pnl=125.0,  # Win
                ),
            ]

            for log in logs:
                writer.write(log)

            tracker = PluginPerformanceTracker(tmpdir)
            pnl = tracker.get_plugin_pnl("test")

            assert pnl["winning_trades"] == 2
            assert pnl["losing_trades"] == 1
            assert pnl["win_rate"] == pytest.approx(66.67, rel=0.01)

    def test_symbol_breakdown(self):
        """Test per-symbol performance breakdown"""
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ExecutionLogWriter(tmpdir)

            logs = [
                PluginExecutionLog(
                    timestamp=datetime.now(),
                    plugin_name="test",
                    order_id=1,
                    exec_id="001",
                    symbol="SPY",
                    action="BUY",
                    quantity=100,
                    fill_price=450.0,
                    commission=1.0,
                ),
                PluginExecutionLog(
                    timestamp=datetime.now(),
                    plugin_name="test",
                    order_id=2,
                    exec_id="002",
                    symbol="SPY",
                    action="SELL",
                    quantity=50,
                    fill_price=455.0,
                    commission=0.5,
                    realized_pnl=250.0,
                ),
                PluginExecutionLog(
                    timestamp=datetime.now(),
                    plugin_name="test",
                    order_id=3,
                    exec_id="003",
                    symbol="QQQ",
                    action="BUY",
                    quantity=75,
                    fill_price=380.0,
                    commission=0.75,
                ),
            ]

            for log in logs:
                writer.write(log)

            tracker = PluginPerformanceTracker(tmpdir)
            metrics = tracker.get_plugin_metrics("test")

            spy_perf = metrics["by_symbol"]["SPY"]
            assert spy_perf["trades"] == 2
            assert spy_perf["buy_quantity"] == 100
            assert spy_perf["sell_quantity"] == 50
            assert spy_perf["commission"] == 1.5

            qqq_perf = metrics["by_symbol"]["QQQ"]
            assert qqq_perf["trades"] == 1
            assert qqq_perf["buy_quantity"] == 75

    def test_daily_pnl(self):
        """Test daily P&L aggregation"""
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ExecutionLogWriter(tmpdir)

            # Create trades on different days
            logs = [
                PluginExecutionLog(
                    timestamp=datetime(2026, 1, 20, 10, 0, 0),
                    plugin_name="test",
                    order_id=1,
                    exec_id="001",
                    symbol="SPY",
                    action="SELL",
                    quantity=100,
                    fill_price=455.0,
                    commission=1.0,
                    realized_pnl=500.0,
                ),
                PluginExecutionLog(
                    timestamp=datetime(2026, 1, 20, 14, 0, 0),
                    plugin_name="test",
                    order_id=2,
                    exec_id="002",
                    symbol="QQQ",
                    action="SELL",
                    quantity=50,
                    fill_price=382.0,
                    commission=0.5,
                    realized_pnl=100.0,
                ),
                PluginExecutionLog(
                    timestamp=datetime(2026, 1, 21, 10, 0, 0),
                    plugin_name="test",
                    order_id=3,
                    exec_id="003",
                    symbol="SPY",
                    action="SELL",
                    quantity=100,
                    fill_price=448.0,
                    commission=1.0,
                    realized_pnl=-200.0,
                ),
            ]

            for log in logs:
                writer.write(log)

            tracker = PluginPerformanceTracker(tmpdir)
            report = tracker.generate_report("test")

            daily = report["daily_pnl"]
            assert "2026-01-20" in daily
            assert "2026-01-21" in daily

            jan20 = daily["2026-01-20"]
            assert jan20["gross_pnl"] == 600.0  # 500 + 100
            assert jan20["commission"] == 1.5  # 1.0 + 0.5
            assert jan20["trades"] == 2

            jan21 = daily["2026-01-21"]
            assert jan21["gross_pnl"] == -200.0
            assert jan21["trades"] == 1
