"""
Unit tests for algorithms module

Tests the trading algorithm framework including base classes,
momentum_5day algorithm, dummy algorithm, and registry.
"""

import json
import pytest
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from algorithms import (
    AlgorithmBase,
    AlgorithmInstrument,
    AlgorithmResult,
    AlgorithmRegistry,
    Holdings,
    HoldingPosition,
    TradeSignal,
    Momentum5DayAlgorithm,
    DummyAlgorithm,
    get_algorithm,
    list_algorithms,
    create_registry,
    SharedHoldings,
    SharedPosition,
    AlgorithmAllocation,
    CashAllocation,
    load_shared_holdings,
    AllocationManager,
    AllocationResult,
    TransferResult,
    AllocationSummary,
    create_manager,
)
from algorithms.momentum_5day.algorithm import MomentumMetrics, create_default_momentum_5day
from algorithms.dummy.algorithm import create_default_dummy


# =============================================================================
# HoldingPosition Tests
# =============================================================================

class TestHoldingPosition:
    """Tests for HoldingPosition dataclass"""

    def test_create_position(self):
        pos = HoldingPosition(
            symbol="SPY",
            quantity=100,
            cost_basis=450.0,
            current_price=455.0,
            market_value=45500.0,
        )
        assert pos.symbol == "SPY"
        assert pos.quantity == 100
        assert pos.market_value == 45500.0

    def test_to_dict(self):
        pos = HoldingPosition("SPY", 100, 450.0, 455.0, 45500.0)
        data = pos.to_dict()
        assert data["symbol"] == "SPY"
        assert data["quantity"] == 100
        assert data["market_value"] == 45500.0

    def test_from_dict(self):
        data = {
            "symbol": "QQQ",
            "quantity": 50,
            "cost_basis": 380.0,
            "current_price": 385.0,
            "market_value": 19250.0,
        }
        pos = HoldingPosition.from_dict(data)
        assert pos.symbol == "QQQ"
        assert pos.quantity == 50

    def test_roundtrip(self):
        original = HoldingPosition("IWM", 200, 200.0, 205.0, 41000.0)
        restored = HoldingPosition.from_dict(original.to_dict())
        assert restored.symbol == original.symbol
        assert restored.quantity == original.quantity


# =============================================================================
# Holdings Tests
# =============================================================================

class TestHoldings:
    """Tests for Holdings dataclass"""

    def test_create_holdings(self):
        holdings = Holdings(
            algorithm_name="test",
            initial_cash=100000.0,
            current_cash=95000.0,
        )
        assert holdings.algorithm_name == "test"
        assert holdings.initial_cash == 100000.0
        assert holdings.current_cash == 95000.0

    def test_total_value_cash_only(self):
        holdings = Holdings(
            algorithm_name="test",
            current_cash=50000.0,
        )
        assert holdings.total_value == 50000.0

    def test_total_value_with_positions(self):
        holdings = Holdings(
            algorithm_name="test",
            current_cash=50000.0,
            current_positions=[
                HoldingPosition("SPY", 100, 450.0, 450.0, 45000.0),
            ],
        )
        assert holdings.total_value == 95000.0

    def test_initial_value(self):
        holdings = Holdings(
            algorithm_name="test",
            initial_cash=50000.0,
            initial_positions=[
                HoldingPosition("SPY", 100, 450.0, 0, 0),
            ],
        )
        assert holdings.initial_value == 95000.0

    def test_total_return(self):
        holdings = Holdings(
            algorithm_name="test",
            initial_cash=100000.0,
            current_cash=0.0,
            current_positions=[
                HoldingPosition("SPY", 100, 0, 0, 110000.0),
            ],
        )
        assert holdings.total_return == 10.0  # 10% return

    def test_get_position(self):
        holdings = Holdings(
            algorithm_name="test",
            current_positions=[
                HoldingPosition("SPY", 100, 450.0, 450.0, 45000.0),
                HoldingPosition("QQQ", 50, 380.0, 380.0, 19000.0),
            ],
        )
        pos = holdings.get_position("SPY")
        assert pos is not None
        assert pos.symbol == "SPY"
        assert holdings.get_position("XYZ") is None

    def test_to_dict(self):
        holdings = Holdings(
            algorithm_name="test",
            initial_cash=100000.0,
            current_cash=50000.0,
            created_at=datetime(2026, 1, 1),
        )
        data = holdings.to_dict()
        assert data["algorithm"] == "test"
        assert data["initial_funding"]["cash"] == 100000.0
        assert data["current_holdings"]["cash"] == 50000.0

    def test_from_dict(self):
        data = {
            "algorithm": "momentum",
            "initial_funding": {"cash": 100000.0, "positions": []},
            "current_holdings": {"cash": 80000.0, "positions": []},
            "created_at": "2026-01-01T00:00:00",
        }
        holdings = Holdings.from_dict(data)
        assert holdings.algorithm_name == "momentum"
        assert holdings.initial_cash == 100000.0
        assert holdings.current_cash == 80000.0


# =============================================================================
# AlgorithmInstrument Tests
# =============================================================================

class TestAlgorithmInstrument:
    """Tests for AlgorithmInstrument dataclass"""

    def test_create_instrument(self):
        inst = AlgorithmInstrument(
            symbol="SPY",
            name="S&P 500 ETF",
            weight=30.0,
        )
        assert inst.symbol == "SPY"
        assert inst.name == "S&P 500 ETF"
        assert inst.weight == 30.0

    def test_default_values(self):
        inst = AlgorithmInstrument("SPY", "S&P 500")
        assert inst.weight == 0.0
        assert inst.min_weight == 0.0
        assert inst.max_weight == 100.0
        assert inst.enabled is True
        assert inst.exchange == "SMART"
        assert inst.currency == "USD"

    def test_to_contract(self):
        inst = AlgorithmInstrument("SPY", "S&P 500")
        contract = inst.to_contract()
        assert contract.symbol == "SPY"
        assert contract.secType == "STK"
        assert contract.exchange == "SMART"

    def test_to_dict(self):
        inst = AlgorithmInstrument("SPY", "S&P 500", weight=30.0)
        data = inst.to_dict()
        assert data["symbol"] == "SPY"
        assert data["weight"] == 30.0

    def test_from_dict(self):
        data = {
            "symbol": "QQQ",
            "name": "Nasdaq 100",
            "weight": 20.0,
            "min_weight": 5.0,
            "max_weight": 40.0,
        }
        inst = AlgorithmInstrument.from_dict(data)
        assert inst.symbol == "QQQ"
        assert inst.weight == 20.0
        assert inst.min_weight == 5.0


# =============================================================================
# TradeSignal Tests
# =============================================================================

class TestTradeSignal:
    """Tests for TradeSignal dataclass"""

    def test_create_signal(self):
        signal = TradeSignal(
            symbol="SPY",
            action="BUY",
            quantity=10,
            reason="Momentum positive",
        )
        assert signal.symbol == "SPY"
        assert signal.action == "BUY"
        assert signal.quantity == 10

    def test_is_actionable_buy(self):
        signal = TradeSignal("SPY", "BUY", 10)
        assert signal.is_actionable is True

    def test_is_actionable_sell(self):
        signal = TradeSignal("SPY", "SELL", 10)
        assert signal.is_actionable is True

    def test_is_actionable_hold(self):
        signal = TradeSignal("SPY", "HOLD", 0)
        assert signal.is_actionable is False

    def test_is_actionable_zero_quantity(self):
        signal = TradeSignal("SPY", "BUY", 0)
        assert signal.is_actionable is False


# =============================================================================
# AlgorithmResult Tests
# =============================================================================

class TestAlgorithmResult:
    """Tests for AlgorithmResult dataclass"""

    def test_create_result(self):
        result = AlgorithmResult(
            algorithm_name="test",
            timestamp=datetime.now(),
            success=True,
        )
        assert result.algorithm_name == "test"
        assert result.success is True

    def test_actionable_signals(self):
        result = AlgorithmResult(
            algorithm_name="test",
            timestamp=datetime.now(),
            signals=[
                TradeSignal("SPY", "BUY", 10),
                TradeSignal("QQQ", "HOLD", 0),
                TradeSignal("IWM", "SELL", 5),
            ],
        )
        actionable = result.actionable_signals
        assert len(actionable) == 2
        assert actionable[0].symbol == "SPY"
        assert actionable[1].symbol == "IWM"


# =============================================================================
# MomentumMetrics Tests
# =============================================================================

class TestMomentumMetrics:
    """Tests for MomentumMetrics dataclass"""

    def test_create_metrics(self):
        metrics = MomentumMetrics(
            symbol="SPY",
            returns_5d=2.5,
            returns_1d=0.5,
            momentum_score=1.2,
            trend="up",
        )
        assert metrics.symbol == "SPY"
        assert metrics.returns_5d == 2.5
        assert metrics.trend == "up"


# =============================================================================
# Momentum5DayAlgorithm Tests
# =============================================================================

class TestMomentum5DayAlgorithm:
    """Tests for Momentum5DayAlgorithm"""

    def test_init(self):
        algo = Momentum5DayAlgorithm()
        assert algo.name == "momentum_5day"
        assert algo.lookback_days == 5
        assert algo.required_bars == 5

    def test_description(self):
        algo = Momentum5DayAlgorithm()
        assert "momentum" in algo.description.lower()
        assert "5" in algo.description

    def test_load_from_files(self):
        algo = Momentum5DayAlgorithm()
        result = algo.load()
        assert result is True
        assert algo.is_loaded
        assert len(algo.instruments) > 0

    def test_enabled_instruments(self):
        algo = Momentum5DayAlgorithm()
        algo.load()
        enabled = algo.enabled_instruments
        assert all(i.enabled for i in enabled)

    def test_calculate_signals_insufficient_data(self):
        algo = Momentum5DayAlgorithm()
        algo.load()

        # Only 2 bars (need 5)
        market_data = {
            "SPY": [
                {"date": "2026-01-01", "close": 450.0},
                {"date": "2026-01-02", "close": 455.0},
            ]
        }

        signals = algo.calculate_signals(market_data)
        # Should still return signals, but with warnings
        assert isinstance(signals, list)

    def test_calculate_signals_with_data(self):
        algo = Momentum5DayAlgorithm()
        algo.load()

        # 5 days of data for SPY with upward momentum
        market_data = {
            "SPY": [
                {"date": "2026-01-10", "close": 450.0},
                {"date": "2026-01-11", "close": 452.0},
                {"date": "2026-01-12", "close": 454.0},
                {"date": "2026-01-13", "close": 456.0},
                {"date": "2026-01-14", "close": 460.0},
            ],
            "QQQ": [
                {"date": "2026-01-10", "close": 380.0},
                {"date": "2026-01-11", "close": 378.0},
                {"date": "2026-01-12", "close": 375.0},
                {"date": "2026-01-13", "close": 373.0},
                {"date": "2026-01-14", "close": 370.0},
            ],
        }

        signals = algo.calculate_signals(market_data)
        assert len(signals) > 0

    def test_momentum_calculation(self):
        algo = Momentum5DayAlgorithm()
        bars = [
            {"close": 100.0},
            {"close": 101.0},
            {"close": 102.0},
            {"close": 103.0},
            {"close": 105.0},
        ]

        metrics = algo._calculate_momentum("TEST", bars)
        assert metrics.symbol == "TEST"
        assert metrics.returns_5d > 0  # Price went up
        assert metrics.trend == "up"

    def test_momentum_calculation_downtrend(self):
        algo = Momentum5DayAlgorithm()
        bars = [
            {"close": 100.0},
            {"close": 99.0},
            {"close": 97.0},
            {"close": 95.0},
            {"close": 93.0},
        ]

        metrics = algo._calculate_momentum("TEST", bars)
        assert metrics.returns_5d < 0
        assert metrics.trend == "down"

    def test_run_returns_result(self):
        algo = Momentum5DayAlgorithm()
        algo.load()

        market_data = {
            "SPY": [{"close": 450 + i} for i in range(5)],
        }

        result = algo.run(market_data)
        assert isinstance(result, AlgorithmResult)
        assert result.algorithm_name == "momentum_5day"

    def test_run_not_loaded(self):
        algo = Momentum5DayAlgorithm()
        # Don't load

        result = algo.run({})
        assert result.success is False
        assert "not loaded" in result.error.lower()

    def test_get_momentum_summary(self):
        algo = Momentum5DayAlgorithm()
        algo.load()

        market_data = {
            "SPY": [{"close": 450 + i} for i in range(5)],
            "QQQ": [{"close": 380 - i} for i in range(5)],
        }

        algo.calculate_signals(market_data)
        summary = algo.get_momentum_summary()

        assert "Momentum Summary" in summary
        assert "SPY" in summary

    def test_create_default_momentum_5day(self):
        algo = create_default_momentum_5day()
        assert algo.name == "momentum_5day"
        assert len(algo.instruments) == 6
        assert algo.holdings is not None
        assert algo.holdings.initial_cash == 100000.0


# =============================================================================
# DummyAlgorithm Tests
# =============================================================================

class TestDummyAlgorithm:
    """Tests for DummyAlgorithm"""

    def test_init(self):
        algo = DummyAlgorithm()
        assert algo.name == "dummy"

    def test_description(self):
        algo = DummyAlgorithm()
        assert "placeholder" in algo.description.lower()

    def test_required_bars(self):
        algo = DummyAlgorithm()
        assert algo.required_bars == 1

    def test_load_from_files(self):
        algo = DummyAlgorithm()
        result = algo.load()
        assert result is True
        assert algo.is_loaded

    def test_calculate_signals_all_hold(self):
        algo = DummyAlgorithm()
        algo.load()

        signals = algo.calculate_signals({})
        assert all(s.action == "HOLD" for s in signals)

    def test_run_returns_result(self):
        algo = DummyAlgorithm()
        algo.load()

        result = algo.run({})
        assert isinstance(result, AlgorithmResult)
        assert result.success is True
        assert all(s.action == "HOLD" for s in result.signals)

    def test_create_default_dummy(self):
        algo = create_default_dummy()
        assert algo.name == "dummy"
        assert len(algo.instruments) == 2  # SPY and BND
        assert algo.holdings is not None


# =============================================================================
# AlgorithmRegistry Tests
# =============================================================================

class TestAlgorithmRegistry:
    """Tests for AlgorithmRegistry"""

    def test_init(self):
        registry = AlgorithmRegistry()
        assert registry.count == 0

    def test_register(self):
        registry = AlgorithmRegistry()
        algo = registry.register(DummyAlgorithm)
        assert registry.count == 1
        assert "dummy" in registry.algorithm_names

    def test_unregister(self):
        registry = AlgorithmRegistry()
        registry.register(DummyAlgorithm)
        assert registry.unregister("dummy") is True
        assert registry.count == 0
        assert registry.unregister("nonexistent") is False

    def test_get(self):
        registry = AlgorithmRegistry()
        registry.register(DummyAlgorithm)
        algo = registry.get("dummy")
        assert algo is not None
        assert algo.name == "dummy"
        assert registry.get("nonexistent") is None

    def test_discover(self):
        registry = AlgorithmRegistry()
        count = registry.discover()
        assert count >= 2  # momentum_5day and dummy
        assert "momentum_5day" in registry.algorithm_names
        assert "dummy" in registry.algorithm_names

    def test_load_all(self):
        registry = AlgorithmRegistry()
        registry.discover()
        results = registry.load_all()
        assert all(v is True for v in results.values())

    def test_run_all(self):
        registry = AlgorithmRegistry()
        registry.discover()
        registry.load_all()

        market_data = {
            "SPY": [{"close": 450 + i} for i in range(5)],
        }

        results = registry.run_all(market_data)
        assert "momentum_5day" in results
        assert "dummy" in results

    def test_get_all_instruments(self):
        registry = AlgorithmRegistry()
        registry.discover()
        registry.load_all()

        instruments = registry.get_all_instruments()
        assert "momentum_5day" in instruments
        assert "dummy" in instruments

    def test_get_unique_symbols(self):
        registry = AlgorithmRegistry()
        registry.discover()
        registry.load_all()

        symbols = registry.get_unique_symbols()
        assert "SPY" in symbols

    def test_contains(self):
        registry = AlgorithmRegistry()
        registry.register(DummyAlgorithm)
        assert "dummy" in registry
        assert "nonexistent" not in registry

    def test_getitem(self):
        registry = AlgorithmRegistry()
        registry.register(DummyAlgorithm)
        algo = registry["dummy"]
        assert algo.name == "dummy"

        with pytest.raises(KeyError):
            _ = registry["nonexistent"]

    def test_iter(self):
        registry = AlgorithmRegistry()
        registry.discover()

        algos = list(registry)
        assert len(algos) >= 2

    def test_len(self):
        registry = AlgorithmRegistry()
        registry.discover()
        assert len(registry) >= 2

    def test_summary(self):
        registry = AlgorithmRegistry()
        registry.discover()
        registry.load_all()

        summary = registry.summary()
        assert "Algorithm Registry" in summary
        assert "momentum_5day" in summary
        assert "dummy" in summary


# =============================================================================
# Convenience Function Tests
# =============================================================================

class TestConvenienceFunctions:
    """Tests for module-level convenience functions"""

    def test_get_algorithm_momentum(self):
        algo = get_algorithm("momentum_5day")
        assert algo is not None
        assert algo.name == "momentum_5day"

    def test_get_algorithm_dummy(self):
        algo = get_algorithm("dummy")
        assert algo is not None
        assert algo.name == "dummy"

    def test_get_algorithm_nonexistent(self):
        algo = get_algorithm("nonexistent")
        assert algo is None

    def test_list_algorithms(self):
        algos = list_algorithms()
        assert "momentum_5day" in algos
        assert "dummy" in algos

    def test_create_registry(self):
        registry = create_registry()
        assert registry.count >= 2
        assert "momentum_5day" in registry
        assert "dummy" in registry


# =============================================================================
# Integration Tests
# =============================================================================

class TestAlgorithmsIntegration:
    """Integration tests for algorithms"""

    def test_full_workflow(self):
        """Test complete algorithm workflow"""
        # Create and load algorithm
        algo = Momentum5DayAlgorithm()
        assert algo.load() is True

        # Set up market data
        market_data = {}
        for inst in algo.enabled_instruments:
            # Generate 5 days of synthetic data
            market_data[inst.symbol] = [
                {"date": f"2026-01-{10+i}", "close": 100 + i * 0.5}
                for i in range(5)
            ]

        # Run algorithm
        result = algo.run(market_data)
        assert result.success is True

        # Execute dry run
        exec_result = algo.execute(result.signals, dry_run=True)
        assert exec_result.success is True

    def test_multiple_algorithms_coexist(self):
        """Test that multiple algorithms can coexist"""
        registry = create_registry()
        registry.load_all()

        # Both algorithms should be independent
        mom = registry.get("momentum_5day")
        dummy = registry.get("dummy")

        assert mom.name != dummy.name
        assert mom.instruments != dummy.instruments

        # Run both
        market_data = {"SPY": [{"close": 450 + i} for i in range(5)]}

        mom_result = mom.run(market_data)
        dummy_result = dummy.run(market_data)

        assert mom_result.success
        assert dummy_result.success

        # Dummy should always HOLD
        assert all(s.action == "HOLD" for s in dummy_result.signals)

    def test_save_and_load_holdings(self):
        """Test holdings persistence"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir) / "test_algo"
            temp_path.mkdir()

            # Create algorithm with custom path
            algo = DummyAlgorithm(base_path=temp_path)

            # Add instrument and holdings manually
            algo.add_instrument(AlgorithmInstrument("SPY", "S&P 500", weight=100.0))
            algo._holdings = Holdings(
                algorithm_name="test",
                initial_cash=100000.0,
                current_cash=95000.0,
                current_positions=[
                    HoldingPosition("SPY", 10, 450.0, 455.0, 4550.0),
                ],
                created_at=datetime.now(),
            )

            # Save
            algo.save_holdings()
            algo.save_instruments()

            # Verify files exist
            assert (temp_path / "holdings.json").exists()
            assert (temp_path / "instruments.json").exists()

            # Load into new instance
            algo2 = DummyAlgorithm(base_path=temp_path)
            algo2.load()

            assert algo2.holdings.current_cash == 95000.0
            assert len(algo2.holdings.current_positions) == 1
            assert algo2.holdings.get_position("SPY").quantity == 10

    def test_algorithm_with_custom_parameters(self):
        """Test algorithm with custom parameters"""
        algo = Momentum5DayAlgorithm(
            lookback_days=10,
            rebalance_threshold=3.0,
            momentum_weight=0.7,
        )

        assert algo.lookback_days == 10
        assert algo.rebalance_threshold == 3.0
        assert algo.momentum_weight == 0.7
        assert algo.required_bars == 10


# =============================================================================
# AlgorithmAllocation Tests
# =============================================================================

class TestAlgorithmAllocation:
    """Tests for AlgorithmAllocation dataclass"""

    def test_create_allocation(self):
        alloc = AlgorithmAllocation(
            algorithm="momentum_5day",
            quantity=100,
            cost_basis=450.0,
        )
        assert alloc.algorithm == "momentum_5day"
        assert alloc.quantity == 100
        assert alloc.cost_basis == 450.0

    def test_allocation_with_timestamp(self):
        now = datetime.now()
        alloc = AlgorithmAllocation(
            algorithm="dummy",
            quantity=50,
            allocated_at=now,
        )
        assert alloc.allocated_at == now

    def test_to_dict(self):
        alloc = AlgorithmAllocation(
            algorithm="test",
            quantity=100,
            cost_basis=50.0,
        )
        data = alloc.to_dict()
        assert data["algorithm"] == "test"
        assert data["quantity"] == 100
        assert data["cost_basis"] == 50.0

    def test_from_dict(self):
        data = {
            "algorithm": "momentum_5day",
            "quantity": 200,
            "cost_basis": 100.0,
        }
        alloc = AlgorithmAllocation.from_dict(data)
        assert alloc.algorithm == "momentum_5day"
        assert alloc.quantity == 200


# =============================================================================
# CashAllocation Tests
# =============================================================================

class TestCashAllocation:
    """Tests for CashAllocation dataclass"""

    def test_create_cash_allocation(self):
        alloc = CashAllocation(
            algorithm="momentum_5day",
            amount=50000.0,
        )
        assert alloc.algorithm == "momentum_5day"
        assert alloc.amount == 50000.0

    def test_to_dict(self):
        alloc = CashAllocation(algorithm="test", amount=10000.0)
        data = alloc.to_dict()
        assert data["algorithm"] == "test"
        assert data["amount"] == 10000.0

    def test_from_dict(self):
        data = {"algorithm": "dummy", "amount": 25000.0}
        alloc = CashAllocation.from_dict(data)
        assert alloc.algorithm == "dummy"
        assert alloc.amount == 25000.0


# =============================================================================
# SharedPosition Tests
# =============================================================================

class TestSharedPosition:
    """Tests for SharedPosition dataclass"""

    def test_create_position(self):
        pos = SharedPosition(
            symbol="SPY",
            total_quantity=100,
            current_price=450.0,
        )
        assert pos.symbol == "SPY"
        assert pos.total_quantity == 100
        assert pos.current_price == 450.0
        assert pos.allocations == []

    def test_market_value(self):
        pos = SharedPosition(symbol="SPY", total_quantity=100, current_price=450.0, market_value=45000.0)
        assert pos.market_value == 45000.0

    def test_allocated_quantity_empty(self):
        pos = SharedPosition(symbol="SPY")
        assert pos.allocated_quantity == 0

    def test_allocated_quantity_with_allocations(self):
        pos = SharedPosition(
            symbol="SPY",
            total_quantity=100,
            allocations=[
                AlgorithmAllocation("algo1", 60),
                AlgorithmAllocation("algo2", 30),
            ],
        )
        assert pos.allocated_quantity == 90

    def test_unallocated_quantity(self):
        pos = SharedPosition(
            symbol="SPY",
            total_quantity=100,
            allocations=[AlgorithmAllocation("algo1", 60)],
        )
        assert pos.unallocated_quantity == 40

    def test_get_algorithm_quantity_existing(self):
        pos = SharedPosition(
            symbol="SPY",
            allocations=[AlgorithmAllocation("algo1", 60)],
        )
        assert pos.get_algorithm_quantity("algo1") == 60

    def test_get_algorithm_quantity_nonexistent(self):
        pos = SharedPosition(symbol="SPY")
        assert pos.get_algorithm_quantity("algo1") == 0

    def test_allocate_new(self):
        pos = SharedPosition(symbol="SPY", total_quantity=100)
        result = pos.allocate("algo1", 50, cost_basis=450.0)
        assert result is True
        assert pos.get_algorithm_quantity("algo1") == 50
        assert len(pos.allocations) == 1

    def test_allocate_existing_adds(self):
        pos = SharedPosition(
            symbol="SPY",
            total_quantity=100,
            allocations=[AlgorithmAllocation("algo1", 30)],
        )
        result = pos.allocate("algo1", 20)
        assert result is True
        assert pos.get_algorithm_quantity("algo1") == 50

    def test_allocate_exceeds_available(self):
        pos = SharedPosition(symbol="SPY", total_quantity=100)
        result = pos.allocate("algo1", 150)
        assert result is False
        assert pos.get_algorithm_quantity("algo1") == 0

    def test_deallocate_full(self):
        pos = SharedPosition(
            symbol="SPY",
            total_quantity=100,
            allocations=[AlgorithmAllocation("algo1", 50)],
        )
        returned = pos.deallocate("algo1", 50)
        assert returned == 50
        assert pos.get_algorithm_quantity("algo1") == 0

    def test_deallocate_partial(self):
        pos = SharedPosition(
            symbol="SPY",
            total_quantity=100,
            allocations=[AlgorithmAllocation("algo1", 50)],
        )
        returned = pos.deallocate("algo1", 20)
        assert returned == 20
        assert pos.get_algorithm_quantity("algo1") == 30

    def test_deallocate_more_than_available(self):
        pos = SharedPosition(
            symbol="SPY",
            allocations=[AlgorithmAllocation("algo1", 30)],
        )
        returned = pos.deallocate("algo1", 50)
        assert returned == 30
        assert pos.get_algorithm_quantity("algo1") == 0

    def test_deallocate_nonexistent(self):
        pos = SharedPosition(symbol="SPY")
        returned = pos.deallocate("algo1", 10)
        assert returned == 0

    def test_to_dict(self):
        pos = SharedPosition(
            symbol="SPY",
            total_quantity=100,
            current_price=450.0,
            allocations=[AlgorithmAllocation("algo1", 50)],
        )
        data = pos.to_dict()
        assert data["symbol"] == "SPY"
        assert data["total_quantity"] == 100
        assert len(data["allocations"]) == 1

    def test_from_dict(self):
        data = {
            "symbol": "QQQ",
            "total_quantity": 50,
            "current_price": 380.0,
            "allocations": [{"algorithm": "algo1", "quantity": 25, "cost_basis": 370.0}],
        }
        pos = SharedPosition.from_dict(data)
        assert pos.symbol == "QQQ"
        assert pos.total_quantity == 50
        assert pos.get_algorithm_quantity("algo1") == 25


# =============================================================================
# SharedHoldings Tests
# =============================================================================

class TestSharedHoldings:
    """Tests for SharedHoldings class"""

    def test_create_empty(self):
        shared = SharedHoldings()
        assert shared.total_cash == 0
        assert len(shared.positions) == 0
        assert len(shared.algorithms) == 0

    def test_register_algorithm(self):
        shared = SharedHoldings()
        shared.register_algorithm("algo1")
        assert "algo1" in shared.algorithms
        shared.register_algorithm("algo2")
        assert len(shared.algorithms) == 2

    def test_register_algorithm_duplicate(self):
        shared = SharedHoldings()
        shared.register_algorithm("algo1")
        shared.register_algorithm("algo1")  # Should return False for duplicate
        assert len([a for a in shared.algorithms if a == "algo1"]) == 1

    def test_allocate_cash(self):
        shared = SharedHoldings()
        shared._total_cash = 100000.0
        shared.register_algorithm("algo1")
        result = shared.allocate_cash("algo1", 50000.0)
        assert result is True
        assert shared.get_algorithm_cash("algo1") == 50000.0

    def test_allocate_cash_exceeds_available(self):
        shared = SharedHoldings()
        shared._total_cash = 100000.0
        shared.register_algorithm("algo1")
        result = shared.allocate_cash("algo1", 150000.0)
        assert result is False

    def test_deallocate_cash(self):
        shared = SharedHoldings()
        shared._total_cash = 100000.0
        shared.register_algorithm("algo1")
        shared.allocate_cash("algo1", 50000.0)
        returned = shared.deallocate_cash("algo1", 20000.0)
        assert returned == 20000.0
        assert shared.get_algorithm_cash("algo1") == 30000.0

    def test_position_via_positions_dict(self):
        """Test adding positions directly to _positions dict"""
        shared = SharedHoldings()
        shared._positions["SPY"] = SharedPosition(
            symbol="SPY",
            total_quantity=100,
            current_price=450.0,
            market_value=45000.0,
        )
        assert shared.get_position("SPY") is not None
        pos = shared.get_position("SPY")
        assert pos.total_quantity == 100
        assert pos.current_price == 450.0

    def test_update_position_quantity(self):
        """Test updating position quantity"""
        shared = SharedHoldings()
        shared._positions["SPY"] = SharedPosition(
            symbol="SPY",
            total_quantity=100,
            current_price=450.0,
            market_value=45000.0,
        )
        # Update quantity
        shared._positions["SPY"].total_quantity = 150
        shared._positions["SPY"].current_price = 455.0
        pos = shared.get_position("SPY")
        assert pos.total_quantity == 150
        assert pos.current_price == 455.0

    def test_remove_position_via_del(self):
        """Test removing position from _positions dict"""
        shared = SharedHoldings()
        shared._positions["SPY"] = SharedPosition(symbol="SPY", total_quantity=100)
        del shared._positions["SPY"]
        assert shared.get_position("SPY") is None

    def test_allocate_position(self):
        shared = SharedHoldings()
        shared._positions["SPY"] = SharedPosition(
            symbol="SPY",
            total_quantity=100,
            current_price=450.0,
            market_value=45000.0,
        )
        shared.register_algorithm("algo1")
        result = shared.allocate_position("algo1", "SPY", 60)
        assert result is True
        assert shared.get_position("SPY").get_algorithm_quantity("algo1") == 60

    def test_deallocate_position(self):
        shared = SharedHoldings()
        shared._positions["SPY"] = SharedPosition(
            symbol="SPY",
            total_quantity=100,
            current_price=450.0,
            market_value=45000.0,
        )
        shared.register_algorithm("algo1")
        shared.allocate_position("algo1", "SPY", 60)
        returned = shared.deallocate_position("algo1", "SPY", 30)
        assert returned == 30
        assert shared.get_position("SPY").get_algorithm_quantity("algo1") == 30

    def test_total_value(self):
        shared = SharedHoldings()
        shared._total_cash = 50000.0
        shared._positions["SPY"] = SharedPosition(
            symbol="SPY", total_quantity=100, current_price=450.0, market_value=45000.0
        )
        shared._positions["QQQ"] = SharedPosition(
            symbol="QQQ", total_quantity=50, current_price=380.0, market_value=19000.0
        )
        assert shared.total_value == 50000.0 + 45000.0 + 19000.0

    def test_get_algorithm_holdings(self):
        shared = SharedHoldings()
        shared._total_cash = 100000.0
        shared._positions["SPY"] = SharedPosition(
            symbol="SPY", total_quantity=100, current_price=450.0, market_value=45000.0
        )
        shared._positions["QQQ"] = SharedPosition(
            symbol="QQQ", total_quantity=50, current_price=380.0, market_value=19000.0
        )
        shared.register_algorithm("algo1")
        shared.allocate_cash("algo1", 50000.0)
        shared.allocate_position("algo1", "SPY", 60)

        holdings = shared.get_algorithm_holdings("algo1")
        assert holdings["cash"] == 50000.0
        assert len(holdings["positions"]) == 1
        assert holdings["positions"][0]["symbol"] == "SPY"
        assert holdings["positions"][0]["quantity"] == 60

    def test_save_and_load(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "test_shared.json"

            # Create and populate
            shared = SharedHoldings(str(path))
            shared._total_cash = 100000.0
            shared._positions["SPY"] = SharedPosition(
                symbol="SPY", total_quantity=100, current_price=450.0, market_value=45000.0
            )
            shared.register_algorithm("algo1")
            shared.allocate_cash("algo1", 50000.0)
            shared.allocate_position("algo1", "SPY", 60)

            # Save
            result = shared.save()
            assert result is True
            assert path.exists()

            # Load into new instance
            shared2 = SharedHoldings(str(path))
            shared2.load()

            assert shared2.total_cash == 100000.0
            assert shared2.get_position("SPY") is not None
            assert "algo1" in shared2.algorithms
            assert shared2.get_algorithm_cash("algo1") == 50000.0
            assert shared2.get_position("SPY").get_algorithm_quantity("algo1") == 60

    def test_load_shared_holdings_function(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "holdings.json"

            # Create file
            data = {
                "total_cash": 75000.0,
                "cash_allocations": [],
                "positions": [],
                "registered_algorithms": ["test_algo"],
                "last_updated": datetime.now().isoformat(),
            }
            path.write_text(json.dumps(data))

            # Load using convenience function
            shared = load_shared_holdings(str(path))
            assert shared.total_cash == 75000.0
            assert "test_algo" in shared.algorithms


# =============================================================================
# SharedHoldings Integration Tests
# =============================================================================

class TestSharedHoldingsIntegration:
    """Integration tests for shared holdings with algorithms"""

    def test_algorithm_with_shared_holdings(self):
        """Test algorithm using shared holdings"""
        shared = SharedHoldings()
        shared._total_cash = 100000.0
        shared._positions["SPY"] = SharedPosition(
            symbol="SPY", total_quantity=100, current_price=450.0, market_value=45000.0
        )
        shared.register_algorithm("momentum_5day")
        shared.allocate_cash("momentum_5day", 50000.0)
        shared.allocate_position("momentum_5day", "SPY", 50)

        algo = Momentum5DayAlgorithm(shared_holdings=shared)
        assert algo.uses_shared_holdings is True

        holdings = algo.get_effective_holdings()
        assert holdings["cash"] == 50000.0

    def test_registry_with_shared_holdings(self):
        """Test registry passing shared holdings to algorithms"""
        shared = SharedHoldings()
        shared._total_cash = 100000.0
        shared.register_algorithm("momentum_5day")
        shared.register_algorithm("dummy")

        registry = AlgorithmRegistry(shared_holdings=shared)
        registry.discover()

        assert registry.uses_shared_holdings is True
        assert registry.shared_holdings is shared

        # Algorithms should receive shared holdings
        mom = registry.get("momentum_5day")
        assert mom.uses_shared_holdings is True

    def test_multiple_algorithms_same_position(self):
        """Test multiple algorithms sharing the same position"""
        shared = SharedHoldings()
        shared._total_cash = 100000.0
        shared._positions["SPY"] = SharedPosition(
            symbol="SPY", total_quantity=100, current_price=450.0, market_value=45000.0
        )
        shared.register_algorithm("algo1")
        shared.register_algorithm("algo2")

        # Allocate to both algorithms
        shared.allocate_position("algo1", "SPY", 60)
        shared.allocate_position("algo2", "SPY", 30)

        # Check allocations
        pos = shared.get_position("SPY")
        assert pos.get_algorithm_quantity("algo1") == 60
        assert pos.get_algorithm_quantity("algo2") == 30
        assert pos.unallocated_quantity == 10

    def test_reconcile_with_mock_portfolio(self):
        """Test reconciliation with a mock portfolio"""
        shared = SharedHoldings()
        shared._total_cash = 100000.0
        shared._positions["SPY"] = SharedPosition(
            symbol="SPY", total_quantity=100, current_price=450.0, market_value=45000.0
        )
        shared._positions["QQQ"] = SharedPosition(
            symbol="QQQ", total_quantity=50, current_price=380.0, market_value=19000.0
        )

        # Create mock portfolio with proper attribute access
        mock_spy = MagicMock()
        mock_spy.symbol = "SPY"
        mock_spy.quantity = 100
        mock_spy.market_value = 46000.0
        mock_spy.current_price = 460.0
        mock_spy.avg_cost = 450.0
        mock_spy.unrealized_pnl = 1000.0

        mock_qqq = MagicMock()
        mock_qqq.symbol = "QQQ"
        mock_qqq.quantity = 60
        mock_qqq.market_value = 23400.0
        mock_qqq.current_price = 390.0
        mock_qqq.avg_cost = 380.0
        mock_qqq.unrealized_pnl = 600.0

        mock_portfolio = MagicMock()
        mock_portfolio.positions = [mock_spy, mock_qqq]
        mock_portfolio._cash_balance = 100000.0

        # Reconcile
        result = shared.reconcile(mock_portfolio)
        assert result["success"] is True

        # SPY should have updated price
        spy_pos = shared.get_position("SPY")
        assert spy_pos.current_price == 460.0

        # QQQ should have updated quantity
        qqq_pos = shared.get_position("QQQ")
        assert qqq_pos.total_quantity == 60

    def test_registry_reconcile(self):
        """Test reconciliation through registry"""
        shared = SharedHoldings()
        shared._total_cash = 100000.0

        registry = AlgorithmRegistry(shared_holdings=shared)

        # Mock portfolio with proper attributes
        mock_portfolio = MagicMock()
        mock_portfolio.positions = []
        mock_portfolio._cash_balance = 100000.0

        result = registry.reconcile(mock_portfolio)
        assert result["success"] is True

    def test_get_combined_signals(self):
        """Test combining signals from multiple algorithms"""
        shared = SharedHoldings()
        shared._total_cash = 100000.0

        registry = AlgorithmRegistry(shared_holdings=shared)
        registry.discover()
        registry.load_all()

        market_data = {"SPY": [{"close": 450 + i} for i in range(5)]}

        combined = registry.get_combined_signals(market_data)
        # Should have signals from both algorithms for SPY
        assert isinstance(combined, dict)


# =============================================================================
# AllocationManager Tests
# =============================================================================

class TestAllocationManager:
    """Tests for AllocationManager executive functions"""

    def test_create_manager(self):
        """Test creating allocation manager"""
        manager = AllocationManager()
        assert manager.holdings is not None
        assert manager.portfolio is None
        assert not manager.is_synced

    def test_create_manager_with_holdings(self):
        """Test creating manager with existing holdings"""
        shared = SharedHoldings()
        shared._total_cash = 100000.0

        manager = AllocationManager(shared_holdings=shared)
        assert manager.holdings is shared
        assert manager.holdings.total_cash == 100000.0

    def test_allocate_position(self):
        """Test allocating position to algorithm"""
        manager = AllocationManager()
        manager.holdings._total_cash = 100000.0
        manager.holdings._positions["SPY"] = SharedPosition(
            symbol="SPY", total_quantity=100, current_price=450.0, market_value=45000.0
        )

        result = manager.allocate("momentum_5day", "SPY", 60)

        assert result.success is True
        assert result.algorithm == "momentum_5day"
        assert result.symbol == "SPY"
        assert result.quantity == 60
        assert manager.holdings.get_position("SPY").get_algorithm_quantity("momentum_5day") == 60

    def test_allocate_exceeds_available(self):
        """Test allocation fails when exceeds available"""
        manager = AllocationManager()
        manager.holdings._positions["SPY"] = SharedPosition(
            symbol="SPY", total_quantity=100, current_price=450.0
        )

        result = manager.allocate("momentum_5day", "SPY", 150)

        assert result.success is False
        assert "Insufficient" in result.message

    def test_allocate_nonexistent_position(self):
        """Test allocation fails for nonexistent position"""
        manager = AllocationManager()

        result = manager.allocate("momentum_5day", "XXX", 100)

        assert result.success is False
        assert "not found" in result.message

    def test_deallocate_position(self):
        """Test deallocating from algorithm"""
        manager = AllocationManager()
        manager.holdings._positions["SPY"] = SharedPosition(
            symbol="SPY", total_quantity=100, current_price=450.0,
            allocations=[AlgorithmAllocation("momentum_5day", 60)]
        )

        result = manager.deallocate("momentum_5day", "SPY", 30)

        assert result.success is True
        assert result.quantity == 30  # Remaining
        assert result.previous_quantity == 60

    def test_transfer_between_algorithms(self):
        """Test transferring allocation between algorithms"""
        manager = AllocationManager()
        manager.holdings._positions["SPY"] = SharedPosition(
            symbol="SPY", total_quantity=100, current_price=450.0,
            allocations=[AlgorithmAllocation("algo1", 60, cost_basis=440.0)]
        )
        manager.holdings.register_algorithm("algo1")
        manager.holdings.register_algorithm("algo2")

        result = manager.transfer("algo1", "algo2", "SPY", 25)

        assert result.success is True
        assert result.quantity == 25
        pos = manager.holdings.get_position("SPY")
        assert pos.get_algorithm_quantity("algo1") == 35
        assert pos.get_algorithm_quantity("algo2") == 25

    def test_transfer_insufficient(self):
        """Test transfer fails with insufficient quantity"""
        manager = AllocationManager()
        manager.holdings._positions["SPY"] = SharedPosition(
            symbol="SPY", total_quantity=100,
            allocations=[AlgorithmAllocation("algo1", 30)]
        )

        result = manager.transfer("algo1", "algo2", "SPY", 50)

        assert result.success is False
        assert "only has" in result.message

    def test_allocate_cash(self):
        """Test allocating cash to algorithm"""
        manager = AllocationManager()
        manager.holdings._total_cash = 100000.0

        result = manager.allocate_cash("momentum_5day", 50000.0)

        assert result.success is True
        assert manager.holdings.get_algorithm_cash("momentum_5day") == 50000.0

    def test_allocate_cash_exceeds_available(self):
        """Test cash allocation fails when exceeds available"""
        manager = AllocationManager()
        manager.holdings._total_cash = 100000.0

        result = manager.allocate_cash("momentum_5day", 150000.0)

        assert result.success is False

    def test_transfer_cash(self):
        """Test transferring cash between algorithms"""
        manager = AllocationManager()
        manager.holdings._total_cash = 100000.0
        manager.holdings.register_algorithm("algo1")
        manager.holdings.allocate_cash("algo1", 50000.0)

        result = manager.transfer_cash("algo1", "algo2", 20000.0)

        assert result.success is True
        assert manager.holdings.get_algorithm_cash("algo1") == 30000.0
        assert manager.holdings.get_algorithm_cash("algo2") == 20000.0

    def test_auto_allocate(self):
        """Test auto-allocating all unallocated to algorithm"""
        manager = AllocationManager()
        manager.holdings._total_cash = 100000.0
        manager.holdings._positions["SPY"] = SharedPosition(
            symbol="SPY", total_quantity=100, current_price=450.0, market_value=45000.0, avg_cost=440.0
        )
        manager.holdings._positions["QQQ"] = SharedPosition(
            symbol="QQQ", total_quantity=50, current_price=380.0, market_value=19000.0, avg_cost=370.0
        )

        results = manager.auto_allocate("momentum_5day", include_cash=True)

        assert len(results["positions_allocated"]) == 2
        assert results["cash_allocated"] == 100000.0
        assert manager.holdings.get_position("SPY").get_algorithm_quantity("momentum_5day") == 100
        assert manager.holdings.get_position("QQQ").get_algorithm_quantity("momentum_5day") == 50

    def test_distribute_equally(self):
        """Test distributing equally among algorithms"""
        manager = AllocationManager()
        manager.holdings._positions["SPY"] = SharedPosition(
            symbol="SPY", total_quantity=100, current_price=450.0, avg_cost=440.0
        )

        results = manager.distribute_equally(["algo1", "algo2"], "SPY")

        pos = manager.holdings.get_position("SPY")
        assert pos.get_algorithm_quantity("algo1") == 50
        assert pos.get_algorithm_quantity("algo2") == 50

    def test_get_status(self):
        """Test getting allocation status"""
        manager = AllocationManager()
        manager.holdings._total_cash = 100000.0
        manager.holdings._positions["SPY"] = SharedPosition(
            symbol="SPY", total_quantity=100, current_price=450.0, market_value=45000.0,
            allocations=[AlgorithmAllocation("momentum_5day", 60)]
        )
        manager.holdings.register_algorithm("momentum_5day")

        status = manager.get_status()

        assert isinstance(status, AllocationSummary)
        assert status.total_value == 145000.0
        assert status.total_cash == 100000.0
        assert len(status.positions) == 1
        assert "momentum_5day" in status.algorithms

    def test_get_algorithm_summary(self):
        """Test getting algorithm summary"""
        manager = AllocationManager()
        manager.holdings._total_cash = 100000.0
        manager.holdings._positions["SPY"] = SharedPosition(
            symbol="SPY", total_quantity=100, current_price=450.0, market_value=45000.0,
            allocations=[AlgorithmAllocation("momentum_5day", 60)]
        )
        manager.holdings.register_algorithm("momentum_5day")
        manager.holdings.allocate_cash("momentum_5day", 50000.0)

        summary = manager.get_algorithm_summary("momentum_5day")

        assert summary["algorithm"] == "momentum_5day"
        assert summary["holdings"]["cash"] == 50000.0
        assert len(summary["holdings"]["positions"]) == 1

    def test_format_status(self):
        """Test formatted status output"""
        manager = AllocationManager()
        manager.holdings._total_cash = 100000.0
        manager.holdings._positions["SPY"] = SharedPosition(
            symbol="SPY", total_quantity=100, current_price=450.0, market_value=45000.0
        )

        output = manager.format_status()

        assert "ALLOCATION STATUS" in output
        assert "SPY" in output
        assert "$145,000" in output

    def test_save_and_reload(self):
        """Test saving and reloading holdings"""
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "test_holdings.json"

            # Create and save
            manager = AllocationManager(holdings_file=str(path))
            manager.holdings._total_cash = 75000.0
            manager.holdings._positions["SPY"] = SharedPosition(
                symbol="SPY", total_quantity=50, current_price=450.0, market_value=22500.0
            )
            manager.holdings.register_algorithm("test_algo")
            manager.allocate("test_algo", "SPY", 30)
            manager.save()

            # Reload
            manager2 = AllocationManager(holdings_file=str(path))
            manager2.reload()

            assert manager2.holdings.total_cash == 75000.0
            assert manager2.holdings.get_position("SPY").get_algorithm_quantity("test_algo") == 30

    def test_sync_with_mock_portfolio(self):
        """Test syncing with mock portfolio"""
        manager = AllocationManager()

        # Create mock portfolio
        mock_spy = MagicMock()
        mock_spy.symbol = "SPY"
        mock_spy.quantity = 100
        mock_spy.current_price = 450.0
        mock_spy.market_value = 45000.0
        mock_spy.avg_cost = 440.0
        mock_spy.unrealized_pnl = 1000.0

        mock_portfolio = MagicMock()
        mock_portfolio.positions = [mock_spy]
        mock_portfolio._cash_balance = 50000.0

        manager.portfolio = mock_portfolio
        results = manager.sync()

        assert results["success"] is True
        assert manager.is_synced is True
        assert manager.holdings.get_position("SPY") is not None


class TestAllocationManagerConvenience:
    """Tests for AllocationManager convenience functions"""

    def test_create_manager_function(self):
        """Test create_manager convenience function"""
        manager = create_manager(portfolio=None, auto_sync=False)
        assert isinstance(manager, AllocationManager)

    def test_create_manager_with_sync(self):
        """Test create_manager with auto sync"""
        mock_portfolio = MagicMock()
        mock_portfolio.positions = []
        mock_portfolio._cash_balance = 100000.0

        manager = create_manager(portfolio=mock_portfolio, auto_sync=True)
        assert manager.is_synced is True
