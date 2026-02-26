"""
Unit tests for execution_db.py

Tests ExecutionDatabase, ExecutionRecord, and CommissionRecord classes.
"""

import pytest
import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def temp_db_path():
    """Create a temporary database path for testing"""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        path = Path(f.name)
    yield path
    # Cleanup
    if path.exists():
        path.unlink()


@pytest.fixture
def execution_db(temp_db_path):
    """Create an ExecutionDatabase instance for testing"""
    from execution_db import ExecutionDatabase
    return ExecutionDatabase(db_path=temp_db_path)


@pytest.fixture
def sample_execution():
    """Create a sample ExecutionRecord"""
    from execution_db import ExecutionRecord
    return ExecutionRecord(
        exec_id="00025b49.697abf9c.01.01",
        order_id=12345,
        symbol="QQQ",
        sec_type="STK",
        exchange="SMART",
        currency="USD",
        local_symbol="QQQ",
        shares=100.0,
        cum_qty=100.0,
        avg_price=450.25,
        side="BOT",
        account="DU123456",
        timestamp=datetime(2024, 1, 15, 10, 30, 0),
    )


@pytest.fixture
def sample_commission():
    """Create a sample CommissionRecord"""
    from execution_db import CommissionRecord
    return CommissionRecord(
        exec_id="00025b49.697abf9c.01.01",
        commission=1.00,
        currency="USD",
        realized_pnl=None,
        timestamp=datetime(2024, 1, 15, 10, 30, 1),
    )


# =============================================================================
# ExecutionRecord Tests
# =============================================================================

class TestExecutionRecord:
    """Tests for ExecutionRecord dataclass"""

    def test_create_execution_record(self, sample_execution):
        """Test creating an ExecutionRecord"""
        assert sample_execution.exec_id == "00025b49.697abf9c.01.01"
        assert sample_execution.order_id == 12345
        assert sample_execution.symbol == "QQQ"
        assert sample_execution.sec_type == "STK"
        assert sample_execution.shares == 100.0
        assert sample_execution.avg_price == 450.25
        assert sample_execution.side == "BOT"

    def test_execution_record_to_dict(self, sample_execution):
        """Test ExecutionRecord.to_dict()"""
        d = sample_execution.to_dict()

        assert d["exec_id"] == "00025b49.697abf9c.01.01"
        assert d["order_id"] == 12345
        assert d["symbol"] == "QQQ"
        assert d["shares"] == 100.0
        assert d["side"] == "BOT"
        assert "timestamp" in d

    def test_execution_record_default_values(self):
        """Test ExecutionRecord with default values"""
        from execution_db import ExecutionRecord

        exec_record = ExecutionRecord(
            exec_id="test123",
            order_id=1,
            symbol="SPY",
            sec_type="STK",
            exchange="",
            currency="",
            shares=50.0,
            cum_qty=50.0,
            avg_price=400.0,
            side="SLD",
            timestamp=datetime.now(),
        )

        assert exec_record.account == ""
        assert exec_record.local_symbol == ""


# =============================================================================
# CommissionRecord Tests
# =============================================================================

class TestCommissionRecord:
    """Tests for CommissionRecord dataclass"""

    def test_create_commission_record(self, sample_commission):
        """Test creating a CommissionRecord"""
        assert sample_commission.exec_id == "00025b49.697abf9c.01.01"
        assert sample_commission.commission == 1.00
        assert sample_commission.currency == "USD"
        assert sample_commission.realized_pnl is None

    def test_commission_record_with_realized_pnl(self):
        """Test CommissionRecord with realized P&L"""
        from execution_db import CommissionRecord

        comm = CommissionRecord(
            exec_id="test123",
            commission=0.50,
            currency="USD",
            realized_pnl=150.25,
            timestamp=datetime.now(),
        )

        assert comm.realized_pnl == 150.25

    def test_commission_record_to_dict(self, sample_commission):
        """Test CommissionRecord.to_dict()"""
        d = sample_commission.to_dict()

        assert d["exec_id"] == "00025b49.697abf9c.01.01"
        assert d["commission"] == 1.00
        assert d["currency"] == "USD"
        assert d["realized_pnl"] is None
        assert "timestamp" in d


# =============================================================================
# ExecutionDatabase Initialization Tests
# =============================================================================

class TestExecutionDatabaseInit:
    """Tests for ExecutionDatabase initialization"""

    def test_creates_database_file(self, temp_db_path):
        """Test that database file is created"""
        from execution_db import ExecutionDatabase

        db = ExecutionDatabase(db_path=temp_db_path)

        assert temp_db_path.exists()

    def test_creates_tables(self, execution_db, temp_db_path):
        """Test that tables are created"""
        with sqlite3.connect(temp_db_path) as conn:
            cursor = conn.cursor()

            # Check executions table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='executions'")
            assert cursor.fetchone() is not None

            # Check commissions table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='commissions'")
            assert cursor.fetchone() is not None

    def test_creates_indexes(self, execution_db, temp_db_path):
        """Test that indexes are created"""
        with sqlite3.connect(temp_db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
            indexes = [row[0] for row in cursor.fetchall()]

            assert "idx_executions_symbol" in indexes
            assert "idx_executions_timestamp" in indexes
            assert "idx_executions_side" in indexes
            assert "idx_commissions_exec_id" in indexes

    def test_default_db_path(self):
        """Test default database path"""
        from execution_db import DEFAULT_DB_PATH

        assert DEFAULT_DB_PATH == Path.home() / ".ib_executions.db"


# =============================================================================
# ExecutionDatabase Insert Tests
# =============================================================================

class TestExecutionDatabaseInsert:
    """Tests for inserting records"""

    def test_insert_execution(self, execution_db, sample_execution):
        """Test inserting an execution record"""
        result = execution_db.insert_execution(sample_execution)

        assert result is True
        assert execution_db.get_execution_count() == 1

    def test_insert_execution_duplicate(self, execution_db, sample_execution):
        """Test that duplicate exec_id is ignored"""
        execution_db.insert_execution(sample_execution)
        result = execution_db.insert_execution(sample_execution)

        assert result is False
        assert execution_db.get_execution_count() == 1

    def test_insert_commission(self, execution_db, sample_execution, sample_commission):
        """Test inserting a commission record"""
        # Insert execution first (foreign key)
        execution_db.insert_execution(sample_execution)

        result = execution_db.insert_commission(sample_commission)

        assert result is True
        assert execution_db.get_commission_count() == 1

    def test_insert_commission_duplicate(self, execution_db, sample_execution, sample_commission):
        """Test that duplicate commission is ignored"""
        execution_db.insert_execution(sample_execution)
        execution_db.insert_commission(sample_commission)
        result = execution_db.insert_commission(sample_commission)

        assert result is False
        assert execution_db.get_commission_count() == 1

    def test_insert_multiple_executions(self, execution_db):
        """Test inserting multiple executions"""
        from execution_db import ExecutionRecord

        for i in range(5):
            exec_record = ExecutionRecord(
                exec_id=f"exec_{i}",
                order_id=i,
                symbol="SPY" if i % 2 == 0 else "QQQ",
                sec_type="STK",
                exchange="SMART",
                currency="USD",
                shares=100.0,
                cum_qty=100.0,
                avg_price=400.0 + i,
                side="BOT",
                timestamp=datetime.now(),
            )
            execution_db.insert_execution(exec_record)

        assert execution_db.get_execution_count() == 5


# =============================================================================
# ExecutionDatabase Query Tests
# =============================================================================

class TestExecutionDatabaseQuery:
    """Tests for querying records"""

    def test_get_executions_by_symbol(self, execution_db):
        """Test getting executions by symbol"""
        from execution_db import ExecutionRecord

        # Insert executions for different symbols
        for i, symbol in enumerate(["SPY", "SPY", "QQQ", "SPY"]):
            exec_record = ExecutionRecord(
                exec_id=f"exec_{i}",
                order_id=i,
                symbol=symbol,
                sec_type="STK",
                exchange="SMART",
                currency="USD",
                shares=100.0,
                cum_qty=100.0,
                avg_price=400.0,
                side="BOT",
                timestamp=datetime.now(),
            )
            execution_db.insert_execution(exec_record)

        spy_executions = execution_db.get_executions_by_symbol("SPY")
        qqq_executions = execution_db.get_executions_by_symbol("QQQ")

        assert len(spy_executions) == 3
        assert len(qqq_executions) == 1

    def test_get_executions_by_symbol_with_date_filter(self, execution_db):
        """Test getting executions with date filter"""
        from execution_db import ExecutionRecord

        base_time = datetime(2024, 1, 15, 10, 0, 0)

        for i in range(3):
            exec_record = ExecutionRecord(
                exec_id=f"exec_{i}",
                order_id=i,
                symbol="SPY",
                sec_type="STK",
                exchange="SMART",
                currency="USD",
                shares=100.0,
                cum_qty=100.0,
                avg_price=400.0,
                side="BOT",
                timestamp=base_time + timedelta(hours=i),
            )
            execution_db.insert_execution(exec_record)

        # Filter by date range
        executions = execution_db.get_executions_by_symbol(
            "SPY",
            start_date=base_time + timedelta(minutes=30),
            end_date=base_time + timedelta(hours=1, minutes=30),
        )

        assert len(executions) == 1  # Only the middle one

    def test_get_all_executions(self, execution_db):
        """Test getting all executions"""
        from execution_db import ExecutionRecord

        for i in range(10):
            exec_record = ExecutionRecord(
                exec_id=f"exec_{i}",
                order_id=i,
                symbol="SPY",
                sec_type="STK",
                exchange="SMART",
                currency="USD",
                shares=100.0,
                cum_qty=100.0,
                avg_price=400.0,
                side="BOT",
                timestamp=datetime.now() - timedelta(hours=i),
            )
            execution_db.insert_execution(exec_record)

        # Get with limit
        executions = execution_db.get_all_executions(limit=5)
        assert len(executions) == 5

        # Get all
        all_executions = execution_db.get_all_executions(limit=100)
        assert len(all_executions) == 10

    def test_get_commission_for_execution(self, execution_db, sample_execution, sample_commission):
        """Test getting commission for an execution"""
        execution_db.insert_execution(sample_execution)
        execution_db.insert_commission(sample_commission)

        comm = execution_db.get_commission_for_execution(sample_execution.exec_id)

        assert comm is not None
        assert comm.commission == 1.00

    def test_get_commission_for_execution_not_found(self, execution_db):
        """Test getting commission for non-existent execution"""
        comm = execution_db.get_commission_for_execution("nonexistent")

        assert comm is None


# =============================================================================
# ExecutionDatabase Cost Basis Tests
# =============================================================================

class TestExecutionDatabaseCostBasis:
    """Tests for cost basis calculations"""

    def test_get_cost_basis(self, execution_db):
        """Test calculating cost basis"""
        from execution_db import ExecutionRecord

        # Insert buy executions at different prices
        for i, (shares, price) in enumerate([(100, 400.0), (50, 410.0), (150, 420.0)]):
            exec_record = ExecutionRecord(
                exec_id=f"exec_{i}",
                order_id=i,
                symbol="SPY",
                sec_type="STK",
                exchange="SMART",
                currency="USD",
                shares=shares,
                cum_qty=shares,
                avg_price=price,
                side="BOT",
                timestamp=datetime.now(),
            )
            execution_db.insert_execution(exec_record)

        cost_basis = execution_db.get_cost_basis("SPY")

        # Weighted average: (100*400 + 50*410 + 150*420) / 300 = 123500 / 300 = 411.67
        assert cost_basis is not None
        assert abs(cost_basis - 411.67) < 0.01

    def test_get_cost_basis_no_buys(self, execution_db):
        """Test cost basis with no buy executions"""
        from execution_db import ExecutionRecord

        # Insert sell execution only
        exec_record = ExecutionRecord(
            exec_id="exec_0",
            order_id=0,
            symbol="SPY",
            sec_type="STK",
            exchange="SMART",
            currency="USD",
            shares=100.0,
            cum_qty=100.0,
            avg_price=400.0,
            side="SLD",
            timestamp=datetime.now(),
        )
        execution_db.insert_execution(exec_record)

        cost_basis = execution_db.get_cost_basis("SPY")

        assert cost_basis is None

    def test_get_cost_basis_unknown_symbol(self, execution_db):
        """Test cost basis for unknown symbol"""
        cost_basis = execution_db.get_cost_basis("UNKNOWN")

        assert cost_basis is None


# =============================================================================
# ExecutionDatabase Commission Tests
# =============================================================================

class TestExecutionDatabaseCommissions:
    """Tests for commission calculations"""

    def test_get_total_commission(self, execution_db):
        """Test getting total commission"""
        from execution_db import ExecutionRecord, CommissionRecord

        for i in range(3):
            exec_record = ExecutionRecord(
                exec_id=f"exec_{i}",
                order_id=i,
                symbol="SPY",
                sec_type="STK",
                exchange="SMART",
                currency="USD",
                shares=100.0,
                cum_qty=100.0,
                avg_price=400.0,
                side="BOT",
                timestamp=datetime.now(),
            )
            execution_db.insert_execution(exec_record)

            comm = CommissionRecord(
                exec_id=f"exec_{i}",
                commission=1.0 + i * 0.5,  # 1.0, 1.5, 2.0
                currency="USD",
                realized_pnl=None,
                timestamp=datetime.now(),
            )
            execution_db.insert_commission(comm)

        total = execution_db.get_total_commission()

        assert total == 4.5  # 1.0 + 1.5 + 2.0

    def test_get_total_commission_by_symbol(self, execution_db):
        """Test getting total commission by symbol"""
        from execution_db import ExecutionRecord, CommissionRecord

        # SPY executions
        for i in range(2):
            exec_record = ExecutionRecord(
                exec_id=f"spy_exec_{i}",
                order_id=i,
                symbol="SPY",
                sec_type="STK",
                exchange="SMART",
                currency="USD",
                shares=100.0,
                cum_qty=100.0,
                avg_price=400.0,
                side="BOT",
                timestamp=datetime.now(),
            )
            execution_db.insert_execution(exec_record)
            comm = CommissionRecord(
                exec_id=f"spy_exec_{i}",
                commission=1.0,
                currency="USD",
                realized_pnl=None,
                timestamp=datetime.now(),
            )
            execution_db.insert_commission(comm)

        # QQQ execution
        exec_record = ExecutionRecord(
            exec_id="qqq_exec_0",
            order_id=10,
            symbol="QQQ",
            sec_type="STK",
            exchange="SMART",
            currency="USD",
            shares=50.0,
            cum_qty=50.0,
            avg_price=350.0,
            side="BOT",
            timestamp=datetime.now(),
        )
        execution_db.insert_execution(exec_record)
        comm = CommissionRecord(
            exec_id="qqq_exec_0",
            commission=0.5,
            currency="USD",
            realized_pnl=None,
            timestamp=datetime.now(),
        )
        execution_db.insert_commission(comm)

        spy_comm = execution_db.get_total_commission(symbol="SPY")
        qqq_comm = execution_db.get_total_commission(symbol="QQQ")
        total_comm = execution_db.get_total_commission()

        assert spy_comm == 2.0
        assert qqq_comm == 0.5
        assert total_comm == 2.5

    def test_get_total_commission_empty(self, execution_db):
        """Test total commission with no records"""
        total = execution_db.get_total_commission()

        assert total == 0.0


# =============================================================================
# ExecutionDatabase Position Summary Tests
# =============================================================================

class TestExecutionDatabasePositionSummary:
    """Tests for position summary"""

    def test_get_position_summary(self, execution_db):
        """Test getting position summary"""
        from execution_db import ExecutionRecord, CommissionRecord

        # Buy 100 shares at 400
        exec1 = ExecutionRecord(
            exec_id="exec_1",
            order_id=1,
            symbol="SPY",
            sec_type="STK",
            exchange="SMART",
            currency="USD",
            shares=100.0,
            cum_qty=100.0,
            avg_price=400.0,
            side="BOT",
            timestamp=datetime.now(),
        )
        execution_db.insert_execution(exec1)

        # Buy 50 more at 410
        exec2 = ExecutionRecord(
            exec_id="exec_2",
            order_id=2,
            symbol="SPY",
            sec_type="STK",
            exchange="SMART",
            currency="USD",
            shares=50.0,
            cum_qty=150.0,
            avg_price=410.0,
            side="BOT",
            timestamp=datetime.now(),
        )
        execution_db.insert_execution(exec2)

        # Sell 30 at 420
        exec3 = ExecutionRecord(
            exec_id="exec_3",
            order_id=3,
            symbol="SPY",
            sec_type="STK",
            exchange="SMART",
            currency="USD",
            shares=30.0,
            cum_qty=30.0,
            avg_price=420.0,
            side="SLD",
            timestamp=datetime.now(),
        )
        execution_db.insert_execution(exec3)

        # Add commissions
        for exec_id in ["exec_1", "exec_2", "exec_3"]:
            comm = CommissionRecord(
                exec_id=exec_id,
                commission=1.0,
                currency="USD",
                realized_pnl=None,
                timestamp=datetime.now(),
            )
            execution_db.insert_commission(comm)

        summary = execution_db.get_position_summary("SPY")

        assert summary["symbol"] == "SPY"
        assert summary["total_bought"] == 150.0  # 100 + 50
        assert summary["total_sold"] == 30.0
        assert summary["net_position"] == 120.0  # 150 - 30
        assert summary["total_commission"] == 3.0

    def test_get_position_summary_unknown_symbol(self, execution_db):
        """Test position summary for unknown symbol"""
        summary = execution_db.get_position_summary("UNKNOWN")

        assert summary["symbol"] == "UNKNOWN"
        assert summary["total_bought"] == 0
        assert summary["total_sold"] == 0
        assert summary["net_position"] == 0


# =============================================================================
# ExecutionDatabase Count Tests
# =============================================================================

class TestExecutionDatabaseCounts:
    """Tests for count methods"""

    def test_get_execution_count_empty(self, execution_db):
        """Test execution count when empty"""
        assert execution_db.get_execution_count() == 0

    def test_get_commission_count_empty(self, execution_db):
        """Test commission count when empty"""
        assert execution_db.get_commission_count() == 0

    def test_get_execution_count_after_inserts(self, execution_db, sample_execution):
        """Test execution count after inserts"""
        execution_db.insert_execution(sample_execution)
        assert execution_db.get_execution_count() == 1


# =============================================================================
# Global Instance Tests
# =============================================================================

class TestGlobalInstance:
    """Tests for global database instance"""

    def test_get_execution_db_returns_instance(self):
        """Test get_execution_db returns an instance"""
        from execution_db import get_execution_db, _execution_db

        # Reset global
        import execution_db as db_module
        db_module._execution_db = None

        db = get_execution_db()

        assert db is not None
        assert isinstance(db, db_module.ExecutionDatabase)

    def test_get_execution_db_returns_same_instance(self):
        """Test get_execution_db returns same instance"""
        from execution_db import get_execution_db

        db1 = get_execution_db()
        db2 = get_execution_db()

        assert db1 is db2


# =============================================================================
# Error Handling Tests
# =============================================================================

class TestExecutionDatabaseErrorHandling:
    """Tests for error handling"""

    def test_insert_execution_handles_error(self, execution_db):
        """Test insert_execution handles database errors"""
        from execution_db import ExecutionRecord

        # Create a record with invalid data that would cause an error
        exec_record = ExecutionRecord(
            exec_id="test",
            order_id=1,
            symbol="SPY",
            sec_type="STK",
            exchange="",
            currency="",
            shares=100.0,
            cum_qty=100.0,
            avg_price=400.0,
            side="BOT",
            timestamp=datetime.now(),
        )

        # Mock to simulate error
        with patch.object(execution_db, '_init_database'):
            with patch('sqlite3.connect', side_effect=Exception("DB Error")):
                result = execution_db.insert_execution(exec_record)
                assert result is False

    def test_get_executions_handles_error(self, execution_db):
        """Test get_executions_by_symbol handles database errors"""
        with patch('sqlite3.connect', side_effect=Exception("DB Error")):
            result = execution_db.get_executions_by_symbol("SPY")
            assert result == []
