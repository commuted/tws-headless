"""
Tests for order_reconciler.py - Order reconciliation and netting
"""

import pytest
from datetime import datetime
from decimal import Decimal
from unittest.mock import Mock, MagicMock
from ibapi.contract import Contract
from ibapi.order import Order

from order_reconciler import (
    OrderReconciler,
    ReconciliationMode,
    ReconciledOrder,
    PendingSignal,
    ExecutionAllocation,
)
from plugins.base import TradeSignal


def create_contract(symbol: str) -> Contract:
    """Helper to create a test contract"""
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    return contract


def create_signal(symbol: str, action: str, quantity: Decimal, confidence: float = 0.8) -> TradeSignal:
    """Helper to create a test trade signal"""
    return TradeSignal(
        symbol=symbol,
        action=action,
        quantity=Decimal(quantity),
        confidence=confidence,
        reason="test signal",
    )


class TestOrderReconcilerInit:
    """Tests for OrderReconciler initialization"""

    def test_default_initialization(self):
        """Test default initialization"""
        reconciler = OrderReconciler()

        assert reconciler.mode == ReconciliationMode.NET
        assert reconciler.batch_window_ms == 100
        assert reconciler.stats["signals_received"] == 0
        assert reconciler.stats["orders_netted"] == 0
        assert reconciler.stats["shares_saved"] == 0

    def test_custom_mode(self):
        """Test initialization with custom mode"""
        reconciler = OrderReconciler(mode=ReconciliationMode.FIFO)
        assert reconciler.mode == ReconciliationMode.FIFO

    def test_custom_batch_window(self):
        """Test initialization with custom batch window"""
        reconciler = OrderReconciler(batch_window_ms=500)
        assert reconciler.batch_window_ms == 500


class TestAddSignal:
    """Tests for adding signals to the reconciler"""

    def test_add_buy_signal(self):
        """Test adding a buy signal"""
        reconciler = OrderReconciler()
        contract = create_contract("SPY")
        signal = create_signal("SPY", "BUY", 100)

        result = reconciler.add_signal("algo1", signal, contract)

        assert result is True
        assert reconciler.get_pending_count() == 1
        assert reconciler.get_pending_count("SPY") == 1
        assert reconciler.stats["signals_received"] == 1

    def test_add_sell_signal(self):
        """Test adding a sell signal"""
        reconciler = OrderReconciler()
        contract = create_contract("AAPL")
        signal = create_signal("AAPL", "SELL", 50)

        result = reconciler.add_signal("algo1", signal, contract)

        assert result is True
        assert reconciler.get_pending_count("AAPL") == 1

    def test_add_multiple_signals_same_symbol(self):
        """Test adding multiple signals for the same symbol"""
        reconciler = OrderReconciler()
        contract = create_contract("SPY")

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), contract)
        reconciler.add_signal("algo2", create_signal("SPY", "SELL", 30), contract)
        reconciler.add_signal("algo3", create_signal("SPY", "BUY", 20), contract)

        assert reconciler.get_pending_count("SPY") == 3
        assert reconciler.stats["signals_received"] == 3

    def test_add_signals_multiple_symbols(self):
        """Test adding signals for multiple symbols"""
        reconciler = OrderReconciler()

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), create_contract("SPY"))
        reconciler.add_signal("algo1", create_signal("AAPL", "BUY", 50), create_contract("AAPL"))
        reconciler.add_signal("algo2", create_signal("QQQ", "SELL", 25), create_contract("QQQ"))

        assert reconciler.get_pending_count() == 3
        assert reconciler.get_pending_count("SPY") == 1
        assert reconciler.get_pending_count("AAPL") == 1
        assert reconciler.get_pending_count("QQQ") == 1

    def test_add_non_actionable_signal(self):
        """Test that non-actionable signals are rejected"""
        reconciler = OrderReconciler()
        contract = create_contract("SPY")
        signal = create_signal("SPY", "HOLD", 0)  # HOLD is not actionable

        result = reconciler.add_signal("algo1", signal, contract)

        assert result is False
        assert reconciler.get_pending_count() == 0

    def test_get_pending_symbols(self):
        """Test getting list of pending symbols"""
        reconciler = OrderReconciler()

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), create_contract("SPY"))
        reconciler.add_signal("algo1", create_signal("AAPL", "BUY", 50), create_contract("AAPL"))

        symbols = reconciler.get_pending_symbols()
        assert set(symbols) == {"SPY", "AAPL"}


class TestReconcileNet:
    """Tests for NET reconciliation mode"""

    def test_reconcile_single_buy(self):
        """Test reconciling a single buy order"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)
        contract = create_contract("SPY")

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), contract)
        orders = reconciler.reconcile()

        assert len(orders) == 1
        assert orders[0].symbol == "SPY"
        assert orders[0].action == "BUY"
        assert orders[0].net_quantity == 100
        assert len(orders[0].contributing_signals) == 1

    def test_reconcile_net_buys(self):
        """Test netting multiple buy orders"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)
        contract = create_contract("SPY")

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), contract)
        reconciler.add_signal("algo2", create_signal("SPY", "BUY", 50), contract)
        orders = reconciler.reconcile()

        assert len(orders) == 1
        assert orders[0].action == "BUY"
        assert orders[0].net_quantity == 150
        assert len(orders[0].contributing_signals) == 2

    def test_reconcile_net_buy_sell(self):
        """Test netting buy and sell orders - the example from docstring"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)
        contract = create_contract("SPY")

        # Algorithm A: BUY 100 SPY
        reconciler.add_signal("algo_a", create_signal("SPY", "BUY", 100), contract)
        # Algorithm B: SELL 30 SPY
        reconciler.add_signal("algo_b", create_signal("SPY", "SELL", 30), contract)
        # Algorithm C: BUY 20 SPY
        reconciler.add_signal("algo_c", create_signal("SPY", "BUY", 20), contract)

        orders = reconciler.reconcile()

        # Net: BUY 100 - 30 + 20 = BUY 90
        assert len(orders) == 1
        assert orders[0].action == "BUY"
        assert orders[0].net_quantity == 90
        assert len(orders[0].contributing_signals) == 3

    def test_reconcile_net_to_sell(self):
        """Test netting results in sell when sells exceed buys"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)
        contract = create_contract("SPY")

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 30), contract)
        reconciler.add_signal("algo2", create_signal("SPY", "SELL", 100), contract)
        orders = reconciler.reconcile()

        assert len(orders) == 1
        assert orders[0].action == "SELL"
        assert orders[0].net_quantity == 70

    def test_reconcile_net_to_zero(self):
        """Test that perfectly offsetting orders produce no order"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)
        contract = create_contract("SPY")

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), contract)
        reconciler.add_signal("algo2", create_signal("SPY", "SELL", 100), contract)
        orders = reconciler.reconcile()

        assert len(orders) == 0

    def test_reconcile_shares_saved_calculation(self):
        """Test that shares saved is calculated correctly"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)
        contract = create_contract("SPY")

        # Total shares: 100 + 30 = 130
        # Net order: BUY 70
        # Shares saved: 130 - 70 = 60
        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), contract)
        reconciler.add_signal("algo2", create_signal("SPY", "SELL", 30), contract)
        reconciler.reconcile()

        assert reconciler.stats["shares_saved"] == 60

    def test_reconcile_multiple_symbols(self):
        """Test reconciling multiple symbols at once"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), create_contract("SPY"))
        reconciler.add_signal("algo1", create_signal("AAPL", "BUY", 50), create_contract("AAPL"))
        reconciler.add_signal("algo2", create_signal("SPY", "SELL", 30), create_contract("SPY"))

        orders = reconciler.reconcile()

        assert len(orders) == 2
        spy_order = next(o for o in orders if o.symbol == "SPY")
        aapl_order = next(o for o in orders if o.symbol == "AAPL")

        assert spy_order.action == "BUY"
        assert spy_order.net_quantity == 70
        assert aapl_order.action == "BUY"
        assert aapl_order.net_quantity == 50

    def test_reconcile_specific_symbol(self):
        """Test reconciling only a specific symbol"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), create_contract("SPY"))
        reconciler.add_signal("algo1", create_signal("AAPL", "BUY", 50), create_contract("AAPL"))

        orders = reconciler.reconcile(symbol="SPY")

        assert len(orders) == 1
        assert orders[0].symbol == "SPY"
        assert reconciler.get_pending_count("SPY") == 0
        assert reconciler.get_pending_count("AAPL") == 1  # Still pending

    def test_reconcile_clears_pending(self):
        """Test that reconcile clears pending signals"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)
        contract = create_contract("SPY")

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), contract)
        assert reconciler.get_pending_count() == 1

        reconciler.reconcile()
        assert reconciler.get_pending_count() == 0

    def test_reconcile_updates_stats(self):
        """Test that stats are updated after reconcile"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)
        contract = create_contract("SPY")

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), contract)
        reconciler.reconcile()

        assert reconciler.stats["orders_netted"] == 1


class TestReconcileFIFO:
    """Tests for FIFO reconciliation mode"""

    def test_fifo_no_netting(self):
        """Test FIFO mode does not net orders"""
        reconciler = OrderReconciler(mode=ReconciliationMode.FIFO)
        contract = create_contract("SPY")

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), contract)
        reconciler.add_signal("algo2", create_signal("SPY", "SELL", 30), contract)
        orders = reconciler.reconcile()

        assert len(orders) == 2
        assert orders[0].action == "BUY"
        assert orders[0].net_quantity == 100
        assert orders[1].action == "SELL"
        assert orders[1].net_quantity == 30

    def test_fifo_preserves_order(self):
        """Test FIFO mode preserves signal order"""
        reconciler = OrderReconciler(mode=ReconciliationMode.FIFO)
        contract = create_contract("SPY")

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), contract)
        reconciler.add_signal("algo2", create_signal("SPY", "SELL", 50), contract)
        reconciler.add_signal("algo3", create_signal("SPY", "BUY", 25), contract)

        orders = reconciler.reconcile()

        assert len(orders) == 3
        assert [o.net_quantity for o in orders] == [100, 50, 25]


class TestReconcileImmediate:
    """Tests for IMMEDIATE reconciliation mode"""

    def test_immediate_same_as_fifo(self):
        """Test IMMEDIATE mode behaves same as FIFO for now"""
        reconciler = OrderReconciler(mode=ReconciliationMode.IMMEDIATE)
        contract = create_contract("SPY")

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), contract)
        reconciler.add_signal("algo2", create_signal("SPY", "SELL", 30), contract)
        orders = reconciler.reconcile()

        assert len(orders) == 2


class TestClearPending:
    """Tests for clearing pending signals"""

    def test_clear_all_pending(self):
        """Test clearing all pending signals"""
        reconciler = OrderReconciler()

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), create_contract("SPY"))
        reconciler.add_signal("algo1", create_signal("AAPL", "BUY", 50), create_contract("AAPL"))
        assert reconciler.get_pending_count() == 2

        reconciler.clear_pending()
        assert reconciler.get_pending_count() == 0

    def test_clear_specific_symbol(self):
        """Test clearing pending signals for specific symbol"""
        reconciler = OrderReconciler()

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), create_contract("SPY"))
        reconciler.add_signal("algo1", create_signal("AAPL", "BUY", 50), create_contract("AAPL"))

        reconciler.clear_pending("SPY")

        assert reconciler.get_pending_count("SPY") == 0
        assert reconciler.get_pending_count("AAPL") == 1


class TestReconciledOrder:
    """Tests for ReconciledOrder dataclass"""

    def test_algorithm_breakdown(self):
        """Test algorithm_breakdown property"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)
        contract = create_contract("SPY")

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), contract)
        reconciler.add_signal("algo2", create_signal("SPY", "SELL", 30), contract)
        orders = reconciler.reconcile()

        breakdown = orders[0].algorithm_breakdown
        assert "algo1" in breakdown
        assert "algo2" in breakdown
        assert breakdown["algo1"] == ("BUY", 100)
        assert breakdown["algo2"] == ("SELL", 30)


class TestExecutionAllocation:
    """Tests for execution registration and fill allocation"""

    def test_register_execution(self):
        """Test registering an execution"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)
        contract = create_contract("SPY")

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), contract)
        reconciler.add_signal("algo2", create_signal("SPY", "BUY", 50), contract)
        orders = reconciler.reconcile()

        reconciler.register_execution(12345, orders[0])

        allocation = reconciler.get_allocation(12345)
        assert allocation is not None
        assert allocation.symbol == "SPY"
        assert allocation.order_id == 12345

    def test_allocate_fill_proportional(self):
        """Test fill allocation is proportional to signal sizes"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)
        contract = create_contract("SPY")

        # algo1: 100 shares, algo2: 50 shares -> total 150 shares
        # Net order: BUY 150
        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), contract)
        reconciler.add_signal("algo2", create_signal("SPY", "BUY", 50), contract)
        orders = reconciler.reconcile()

        reconciler.register_execution(12345, orders[0])

        # Full fill
        result = reconciler.allocate_fill(12345, 150, 450.00)

        # algo1 should get 2/3, algo2 should get 1/3
        assert "algo1" in result
        assert "algo2" in result
        assert result["algo1"][0] == 100  # 100/150 * 150
        assert result["algo2"][0] == 50   # 50/150 * 150
        assert result["algo1"][1] == 450.00
        assert result["algo2"][1] == 450.00

    def test_allocate_fill_partial(self):
        """Test partial fill allocation"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)
        contract = create_contract("SPY")

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), contract)
        reconciler.add_signal("algo2", create_signal("SPY", "BUY", 50), contract)
        orders = reconciler.reconcile()

        reconciler.register_execution(12345, orders[0])

        # Partial fill of 75 shares
        result = reconciler.allocate_fill(12345, 75, 450.00)

        # algo1 should get 50, algo2 should get 25
        assert result["algo1"][0] == 50
        assert result["algo2"][0] == 25

    def test_allocate_fill_unknown_order(self):
        """Test allocating fill for unknown order"""
        reconciler = OrderReconciler()
        result = reconciler.allocate_fill(99999, 100, 450.00)
        assert result == {}

    def test_allocation_updates_total_filled(self):
        """Test that allocation updates total filled"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)
        contract = create_contract("SPY")

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), contract)
        orders = reconciler.reconcile()

        reconciler.register_execution(12345, orders[0])
        reconciler.allocate_fill(12345, 100, 450.00)

        allocation = reconciler.get_allocation(12345)
        assert allocation.total_filled == 100
        assert allocation.avg_price == 450.00


class TestCreateIBOrder:
    """Tests for creating IB orders from reconciled orders"""

    def test_create_buy_order(self):
        """Test creating a buy order"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)
        contract = create_contract("SPY")

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), contract)
        orders = reconciler.reconcile()

        ib_order = reconciler.create_ib_order(orders[0])

        assert ib_order.action == "BUY"
        assert ib_order.totalQuantity == 100
        assert ib_order.orderType == "MKT"
        assert ib_order.tif == "DAY"
        assert "reconciled:" in ib_order.orderRef
        assert "algo1" in ib_order.orderRef

    def test_create_sell_order(self):
        """Test creating a sell order"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)
        contract = create_contract("SPY")

        reconciler.add_signal("algo1", create_signal("SPY", "SELL", 50), contract)
        orders = reconciler.reconcile()

        ib_order = reconciler.create_ib_order(orders[0])

        assert ib_order.action == "SELL"
        assert ib_order.totalQuantity == 50

    def test_create_order_multiple_algos(self):
        """Test order reference includes multiple algorithms"""
        reconciler = OrderReconciler(mode=ReconciliationMode.NET)
        contract = create_contract("SPY")

        reconciler.add_signal("algo1", create_signal("SPY", "BUY", 100), contract)
        reconciler.add_signal("algo2", create_signal("SPY", "BUY", 50), contract)
        orders = reconciler.reconcile()

        ib_order = reconciler.create_ib_order(orders[0])

        assert "algo1" in ib_order.orderRef
        assert "algo2" in ib_order.orderRef


class TestThreadSafety:
    """Tests for thread safety"""

    def test_concurrent_add_signal(self):
        """Test adding signals from multiple threads"""
        import threading
        import time

        reconciler = OrderReconciler()
        contract = create_contract("SPY")
        errors = []

        def add_signals(algo_name, count):
            try:
                for i in range(count):
                    signal = create_signal("SPY", "BUY", 10)
                    reconciler.add_signal(algo_name, signal, contract)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=add_signals, args=(f"algo{i}", 100))
            for i in range(5)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert reconciler.get_pending_count() == 500  # 5 threads * 100 signals
        assert reconciler.stats["signals_received"] == 500


class TestPendingSignal:
    """Tests for PendingSignal dataclass"""

    def test_pending_signal_creation(self):
        """Test creating a PendingSignal"""
        signal = create_signal("SPY", "BUY", 100)
        contract = create_contract("SPY")

        pending = PendingSignal(
            algorithm_name="algo1",
            signal=signal,
            contract=contract,
        )

        assert pending.algorithm_name == "algo1"
        assert pending.signal == signal
        assert pending.contract == contract
        assert isinstance(pending.received_at, datetime)


class TestReconciliationMode:
    """Tests for ReconciliationMode enum"""

    def test_mode_values(self):
        """Test ReconciliationMode values"""
        assert ReconciliationMode.NET.value == "net"
        assert ReconciliationMode.FIFO.value == "fifo"
        assert ReconciliationMode.IMMEDIATE.value == "immediate"
