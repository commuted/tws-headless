"""
Tests for plugin_execution_log.py - Plugin execution logging utilities
"""

import pytest
import json
import tempfile
import os
from datetime import datetime, date, timedelta
from pathlib import Path

from ib.plugin_execution_log import (
    PluginExecutionLog,
    ExecutionLogWriter,
    ExecutionLogReader,
)


class TestPluginExecutionLog:
    """Tests for PluginExecutionLog dataclass"""

    def test_create_buy_execution(self):
        """Test creating a buy execution log"""
        log = PluginExecutionLog(
            timestamp=datetime(2026, 1, 23, 10, 30, 0),
            plugin_name="momentum_5day",
            order_id=12345,
            exec_id="0001",
            symbol="SPY",
            action="BUY",
            quantity=100,
            fill_price=455.25,
            commission=1.00,
            fees=0.02,
        )

        assert log.plugin_name == "momentum_5day"
        assert log.symbol == "SPY"
        assert log.action == "BUY"
        assert log.quantity == 100
        assert log.fill_price == 455.25
        assert log.commission == 1.00

    def test_create_sell_execution(self):
        """Test creating a sell execution log"""
        log = PluginExecutionLog(
            timestamp=datetime(2026, 1, 23, 10, 30, 0),
            plugin_name="mean_reversion",
            order_id=12346,
            exec_id="0002",
            symbol="QQQ",
            action="SELL",
            quantity=50,
            fill_price=380.50,
            commission=0.50,
        )

        assert log.action == "SELL"
        assert log.quantity == 50

    def test_net_amount_buy(self):
        """Test net amount calculation for buy orders"""
        log = PluginExecutionLog(
            timestamp=datetime.now(),
            plugin_name="test",
            order_id=1,
            exec_id="001",
            symbol="SPY",
            action="BUY",
            quantity=100,
            fill_price=100.00,
            commission=1.00,
            fees=0.10,
        )

        # BUY: -(quantity * price + commission + fees)
        # -(100 * 100 + 1.00 + 0.10) = -10001.10
        assert log.net_amount == -10001.10

    def test_net_amount_sell(self):
        """Test net amount calculation for sell orders"""
        log = PluginExecutionLog(
            timestamp=datetime.now(),
            plugin_name="test",
            order_id=1,
            exec_id="001",
            symbol="SPY",
            action="SELL",
            quantity=100,
            fill_price=100.00,
            commission=1.00,
            fees=0.10,
        )

        # SELL: quantity * price - commission - fees
        # 100 * 100 - 1.00 - 0.10 = 9998.90
        assert log.net_amount == 9998.90

    def test_gross_amount_buy(self):
        """Test gross amount for buy orders"""
        log = PluginExecutionLog(
            timestamp=datetime.now(),
            plugin_name="test",
            order_id=1,
            exec_id="001",
            symbol="SPY",
            action="BUY",
            quantity=100,
            fill_price=50.00,
        )

        assert log.gross_amount == -5000.00

    def test_gross_amount_sell(self):
        """Test gross amount for sell orders"""
        log = PluginExecutionLog(
            timestamp=datetime.now(),
            plugin_name="test",
            order_id=1,
            exec_id="001",
            symbol="SPY",
            action="SELL",
            quantity=100,
            fill_price=50.00,
        )

        assert log.gross_amount == 5000.00

    def test_combined_order_fields(self):
        """Test combined order tracking fields"""
        log = PluginExecutionLog(
            timestamp=datetime.now(),
            plugin_name="momentum",
            order_id=1,
            exec_id="001",
            symbol="SPY",
            action="BUY",
            quantity=60,
            fill_price=100.00,
            is_combined_order=True,
            allocation_pct=0.6,
            total_order_quantity=100,
        )

        assert log.is_combined_order is True
        assert log.allocation_pct == 0.6
        assert log.total_order_quantity == 100

    def test_position_tracking_fields(self):
        """Test position tracking fields"""
        log = PluginExecutionLog(
            timestamp=datetime.now(),
            plugin_name="test",
            order_id=1,
            exec_id="001",
            symbol="SPY",
            action="BUY",
            quantity=100,
            fill_price=455.00,
            position_before=50,
            position_after=150,
            avg_cost_before=450.00,
            avg_cost_after=451.67,
        )

        assert log.position_before == 50
        assert log.position_after == 150
        assert log.avg_cost_before == 450.00
        assert log.avg_cost_after == 451.67


class TestPluginExecutionLogSerialization:
    """Tests for serialization/deserialization"""

    def test_to_dict(self):
        """Test conversion to dictionary"""
        ts = datetime(2026, 1, 23, 10, 30, 0, 123456)
        log = PluginExecutionLog(
            timestamp=ts,
            plugin_name="momentum",
            order_id=12345,
            exec_id="exec001",
            symbol="SPY",
            action="BUY",
            quantity=100,
            fill_price=455.25,
            commission=1.00,
            fees=0.02,
            realized_pnl=0.0,
            is_combined_order=True,
            allocation_pct=0.5,
            total_order_quantity=200,
        )

        d = log.to_dict()

        assert d["plugin"] == "momentum"
        assert d["order_id"] == 12345
        assert d["exec_id"] == "exec001"
        assert d["symbol"] == "SPY"
        assert d["action"] == "BUY"
        assert d["qty"] == 100
        assert d["price"] == 455.25
        assert d["commission"] == 1.00
        assert d["fees"] == 0.02
        assert d["combined"] is True
        assert d["alloc_pct"] == 0.5
        assert "2026-01-23" in d["ts"]

    def test_to_json(self):
        """Test conversion to JSON string"""
        log = PluginExecutionLog(
            timestamp=datetime(2026, 1, 23, 10, 30, 0),
            plugin_name="test",
            order_id=1,
            exec_id="001",
            symbol="SPY",
            action="BUY",
            quantity=100,
            fill_price=100.00,
        )

        json_str = log.to_json()

        # Should be valid JSON
        parsed = json.loads(json_str)
        assert parsed["plugin"] == "test"
        assert parsed["symbol"] == "SPY"

    def test_from_dict(self):
        """Test creation from dictionary"""
        data = {
            "ts": "2026-01-23T10:30:00",
            "plugin": "momentum",
            "order_id": 12345,
            "exec_id": "exec001",
            "symbol": "QQQ",
            "action": "SELL",
            "qty": 50,
            "price": 380.50,
            "commission": 0.50,
            "fees": 0.01,
            "pnl": 125.00,
            "combined": False,
            "alloc_pct": 1.0,
            "total_qty": 50,
        }

        log = PluginExecutionLog.from_dict(data)

        assert log.plugin_name == "momentum"
        assert log.symbol == "QQQ"
        assert log.action == "SELL"
        assert log.quantity == 50
        assert log.commission == 0.50
        assert log.realized_pnl == 125.00

    def test_from_json(self):
        """Test creation from JSON string"""
        json_str = '{"ts":"2026-01-23T10:30:00","plugin":"test","order_id":1,"exec_id":"001","symbol":"SPY","action":"BUY","qty":100,"price":100.0}'

        log = PluginExecutionLog.from_json(json_str)

        assert log.plugin_name == "test"
        assert log.symbol == "SPY"
        assert log.quantity == 100

    def test_roundtrip_serialization(self):
        """Test that to_json -> from_json preserves data"""
        original = PluginExecutionLog(
            timestamp=datetime(2026, 1, 23, 10, 30, 0),
            plugin_name="momentum_5day",
            order_id=12345,
            exec_id="exec001",
            symbol="SPY",
            action="BUY",
            quantity=100,
            fill_price=455.25,
            commission=1.00,
            fees=0.02,
            realized_pnl=0.0,
            is_combined_order=True,
            allocation_pct=0.6,
            total_order_quantity=166,
            position_before=50,
            position_after=150,
            avg_cost_before=450.00,
            avg_cost_after=451.75,
        )

        json_str = original.to_json()
        restored = PluginExecutionLog.from_json(json_str)

        assert restored.plugin_name == original.plugin_name
        assert restored.symbol == original.symbol
        assert restored.action == original.action
        assert restored.quantity == original.quantity
        assert restored.fill_price == original.fill_price
        assert restored.commission == original.commission
        assert restored.is_combined_order == original.is_combined_order
        assert restored.allocation_pct == original.allocation_pct
        assert restored.position_before == original.position_before
        assert restored.position_after == original.position_after


class TestExecutionLogWriter:
    """Tests for ExecutionLogWriter"""

    def test_create_writer(self):
        """Test creating a log writer"""
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ExecutionLogWriter(tmpdir)
            assert writer.log_dir == Path(tmpdir)

    def test_creates_log_directory(self):
        """Test that writer creates log directory"""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = os.path.join(tmpdir, "logs", "nested")
            writer = ExecutionLogWriter(log_dir)
            assert Path(log_dir).exists()

    def test_write_single_entry(self):
        """Test writing a single log entry"""
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ExecutionLogWriter(tmpdir)

            log = PluginExecutionLog(
                timestamp=datetime.now(),
                plugin_name="test",
                order_id=1,
                exec_id="001",
                symbol="SPY",
                action="BUY",
                quantity=100,
                fill_price=100.00,
            )

            result = writer.write(log)

            assert result is True
            log_file = Path(tmpdir) / "plugin_executions.jsonl"
            assert log_file.exists()

    def test_write_multiple_entries(self):
        """Test writing multiple log entries"""
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ExecutionLogWriter(tmpdir)

            for i in range(5):
                log = PluginExecutionLog(
                    timestamp=datetime.now(),
                    plugin_name=f"plugin_{i}",
                    order_id=i,
                    exec_id=f"00{i}",
                    symbol="SPY",
                    action="BUY",
                    quantity=100,
                    fill_price=100.00,
                )
                writer.write(log)

            # Read and verify
            log_file = Path(tmpdir) / "plugin_executions.jsonl"
            with open(log_file) as f:
                lines = f.readlines()
            assert len(lines) == 5

    def test_write_batch(self):
        """Test writing a batch of entries"""
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ExecutionLogWriter(tmpdir)

            entries = [
                PluginExecutionLog(
                    timestamp=datetime.now(),
                    plugin_name=f"plugin_{i}",
                    order_id=i,
                    exec_id=f"00{i}",
                    symbol="SPY",
                    action="BUY",
                    quantity=100,
                    fill_price=100.00,
                )
                for i in range(3)
            ]

            count = writer.write_batch(entries)

            assert count == 3


class TestExecutionLogReader:
    """Tests for ExecutionLogReader"""

    def test_create_reader(self):
        """Test creating a log reader"""
        with tempfile.TemporaryDirectory() as tmpdir:
            reader = ExecutionLogReader(tmpdir)
            assert reader.log_dir == Path(tmpdir)

    def test_read_empty_directory(self):
        """Test reading from empty directory"""
        with tempfile.TemporaryDirectory() as tmpdir:
            reader = ExecutionLogReader(tmpdir)
            entries = reader.read_current()
            assert entries == []

    def test_read_current(self):
        """Test reading current log file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write some entries
            writer = ExecutionLogWriter(tmpdir)
            for i in range(3):
                log = PluginExecutionLog(
                    timestamp=datetime.now(),
                    plugin_name=f"plugin_{i}",
                    order_id=i,
                    exec_id=f"00{i}",
                    symbol="SPY",
                    action="BUY",
                    quantity=100,
                    fill_price=100.00,
                )
                writer.write(log)

            # Read them back
            reader = ExecutionLogReader(tmpdir)
            entries = reader.read_current()

            assert len(entries) == 3
            assert entries[0].plugin_name == "plugin_0"
            assert entries[2].plugin_name == "plugin_2"

    def test_read_archived_date(self):
        """Test reading archived log file by date"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create an archived log file
            archive_date = date(2026, 1, 20)
            archive_file = Path(tmpdir) / f"plugin_executions.{archive_date.isoformat()}.jsonl"

            log = PluginExecutionLog(
                timestamp=datetime(2026, 1, 20, 10, 0, 0),
                plugin_name="archived",
                order_id=1,
                exec_id="001",
                symbol="SPY",
                action="BUY",
                quantity=50,
                fill_price=450.00,
            )

            with open(archive_file, "w") as f:
                f.write(log.to_json() + "\n")

            # Read it back
            reader = ExecutionLogReader(tmpdir)
            entries = reader.read_date(archive_date)

            assert len(entries) == 1
            assert entries[0].plugin_name == "archived"

    def test_read_all(self):
        """Test reading all log files"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create current log
            writer = ExecutionLogWriter(tmpdir)
            log1 = PluginExecutionLog(
                timestamp=datetime.now(),
                plugin_name="current",
                order_id=1,
                exec_id="001",
                symbol="SPY",
                action="BUY",
                quantity=100,
                fill_price=100.00,
            )
            writer.write(log1)

            # Create archived log
            archive_date = date(2026, 1, 20)
            archive_file = Path(tmpdir) / f"plugin_executions.{archive_date.isoformat()}.jsonl"
            log2 = PluginExecutionLog(
                timestamp=datetime(2026, 1, 20, 10, 0, 0),
                plugin_name="archived",
                order_id=2,
                exec_id="002",
                symbol="QQQ",
                action="SELL",
                quantity=50,
                fill_price=380.00,
            )
            with open(archive_file, "w") as f:
                f.write(log2.to_json() + "\n")

            # Read all
            reader = ExecutionLogReader(tmpdir)
            entries = reader.read_all()

            assert len(entries) == 2

    def test_read_plugin(self):
        """Test reading entries for specific plugin"""
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ExecutionLogWriter(tmpdir)

            # Write entries for different plugins
            for plugin in ["plugin_a", "plugin_b", "plugin_a", "plugin_c"]:
                log = PluginExecutionLog(
                    timestamp=datetime.now(),
                    plugin_name=plugin,
                    order_id=1,
                    exec_id="001",
                    symbol="SPY",
                    action="BUY",
                    quantity=100,
                    fill_price=100.00,
                )
                writer.write(log)

            # Read only plugin_a
            reader = ExecutionLogReader(tmpdir)
            entries = reader.read_plugin("plugin_a")

            assert len(entries) == 2
            assert all(e.plugin_name == "plugin_a" for e in entries)

    def test_list_available_dates(self):
        """Test listing available log dates"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create current log
            writer = ExecutionLogWriter(tmpdir)
            log = PluginExecutionLog(
                timestamp=datetime.now(),
                plugin_name="test",
                order_id=1,
                exec_id="001",
                symbol="SPY",
                action="BUY",
                quantity=100,
                fill_price=100.00,
            )
            writer.write(log)

            # Create archived logs
            for d in [date(2026, 1, 20), date(2026, 1, 21)]:
                archive_file = Path(tmpdir) / f"plugin_executions.{d.isoformat()}.jsonl"
                with open(archive_file, "w") as f:
                    f.write(log.to_json() + "\n")

            reader = ExecutionLogReader(tmpdir)
            dates = reader.list_available_dates()

            assert len(dates) >= 2  # At least the 2 archived dates
            assert date(2026, 1, 20) in dates
            assert date(2026, 1, 21) in dates

    def test_handles_malformed_lines(self):
        """Test that malformed lines are skipped"""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "plugin_executions.jsonl"

            # Write some valid and invalid lines
            valid_log = PluginExecutionLog(
                timestamp=datetime.now(),
                plugin_name="valid",
                order_id=1,
                exec_id="001",
                symbol="SPY",
                action="BUY",
                quantity=100,
                fill_price=100.00,
            )

            with open(log_file, "w") as f:
                f.write(valid_log.to_json() + "\n")
                f.write("not valid json\n")
                f.write('{"incomplete": "json"}\n')
                f.write(valid_log.to_json() + "\n")

            reader = ExecutionLogReader(tmpdir)
            entries = reader.read_current()

            # Should only have the 2 valid entries
            assert len(entries) == 2


class TestCommissionApportionment:
    """Tests for commission apportionment in combined orders"""

    def test_single_plugin_full_allocation(self):
        """Test single plugin gets full commission"""
        log = PluginExecutionLog(
            timestamp=datetime.now(),
            plugin_name="single_plugin",
            order_id=1,
            exec_id="001",
            symbol="SPY",
            action="BUY",
            quantity=100,
            fill_price=455.00,
            commission=1.00,
            is_combined_order=False,
            allocation_pct=1.0,
            total_order_quantity=100,
        )

        assert log.allocation_pct == 1.0
        assert log.commission == 1.00

    def test_two_plugin_split(self):
        """Test commission split between two plugins"""
        total_commission = 1.00

        # Plugin A: 60% allocation
        log_a = PluginExecutionLog(
            timestamp=datetime.now(),
            plugin_name="plugin_a",
            order_id=1,
            exec_id="001",
            symbol="SPY",
            action="BUY",
            quantity=60,
            fill_price=455.00,
            commission=total_commission * 0.6,
            is_combined_order=True,
            allocation_pct=0.6,
            total_order_quantity=100,
        )

        # Plugin B: 40% allocation
        log_b = PluginExecutionLog(
            timestamp=datetime.now(),
            plugin_name="plugin_b",
            order_id=1,
            exec_id="001",
            symbol="SPY",
            action="BUY",
            quantity=40,
            fill_price=455.00,
            commission=total_commission * 0.4,
            is_combined_order=True,
            allocation_pct=0.4,
            total_order_quantity=100,
        )

        assert log_a.commission == 0.60
        assert log_b.commission == 0.40
        assert log_a.commission + log_b.commission == total_commission

    def test_three_plugin_split(self):
        """Test commission split between three plugins"""
        total_commission = 1.50

        allocations = [0.5, 0.3, 0.2]
        plugins = ["momentum", "value", "growth"]

        logs = []
        for plugin, alloc in zip(plugins, allocations):
            log = PluginExecutionLog(
                timestamp=datetime.now(),
                plugin_name=plugin,
                order_id=1,
                exec_id="001",
                symbol="SPY",
                action="BUY",
                quantity=int(100 * alloc),
                fill_price=455.00,
                commission=total_commission * alloc,
                is_combined_order=True,
                allocation_pct=alloc,
                total_order_quantity=100,
            )
            logs.append(log)

        assert logs[0].commission == pytest.approx(0.75)  # 50%
        assert logs[1].commission == pytest.approx(0.45)  # 30%
        assert logs[2].commission == pytest.approx(0.30)  # 20%

        total = sum(log.commission for log in logs)
        assert abs(total - total_commission) < 0.01
