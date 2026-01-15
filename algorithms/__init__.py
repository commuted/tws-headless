"""
algorithms - Trading Algorithm Framework

Provides a framework for implementing and managing multiple trading algorithms.
Each algorithm has its own:
- Instruments file (allowed securities with target weights)
- Trading logic

Holdings can be:
- Per-algorithm (legacy): Each algorithm has its own holdings.json
- Shared: All algorithms share a single shared_holdings.json

Available Algorithms:
- momentum_5day: 5-day momentum-based reallocation
- dummy: Placeholder algorithm (always HOLD)

Usage:
    from algorithms import AlgorithmRegistry, SharedHoldings

    # Create shared holdings for multiple algorithms
    shared = SharedHoldings()
    shared.load()
    shared.reconcile(portfolio)  # Sync with IB

    # Use registry to manage multiple algorithms
    registry = AlgorithmRegistry(shared_holdings=shared)
    registry.discover()
    registry.load_all()

    # Run all algorithms together
    results = registry.run_all(market_data)
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Type

from .base import (
    AlgorithmBase,
    AlgorithmInstrument,
    AlgorithmResult,
    Holdings,
    HoldingPosition,
    TradeSignal,
)

from .shared_holdings import (
    SharedHoldings,
    SharedPosition,
    AlgorithmAllocation,
    CashAllocation,
    load_shared_holdings,
)

from .allocation_manager import (
    AllocationManager,
    AllocationResult,
    TransferResult,
    AllocationSummary,
    create_manager,
    quick_allocate,
)

from .momentum_5day import Momentum5DayAlgorithm
from .dummy import DummyAlgorithm

logger = logging.getLogger(__name__)


# =============================================================================
# Algorithm Registry
# =============================================================================

class AlgorithmRegistry:
    """
    Registry for managing multiple trading algorithms.

    Provides:
    - Algorithm discovery and registration
    - Access to all registered algorithms
    - Shared holdings support for position tracking across algorithms
    - Bulk operations (load all, run all, etc.)
    - Reconciliation with IB portfolio

    Usage:
        # With shared holdings (recommended for multiple algorithms)
        shared = SharedHoldings()
        shared.load()
        registry = AlgorithmRegistry(shared_holdings=shared)
        registry.discover()
        registry.load_all()

        # Reconcile with IB portfolio
        registry.reconcile(portfolio)

        # Run all algorithms
        results = registry.run_all(market_data)

        # Without shared holdings (legacy per-algorithm holdings)
        registry = AlgorithmRegistry()
        registry.discover()
    """

    # Known algorithm classes
    KNOWN_ALGORITHMS: Dict[str, Type[AlgorithmBase]] = {
        "momentum_5day": Momentum5DayAlgorithm,
        "dummy": DummyAlgorithm,
    }

    def __init__(self, shared_holdings: Optional[SharedHoldings] = None):
        self._algorithms: Dict[str, AlgorithmBase] = {}
        self._shared_holdings = shared_holdings
        self._loaded = False

    @property
    def shared_holdings(self) -> Optional[SharedHoldings]:
        """Get shared holdings instance"""
        return self._shared_holdings

    @property
    def uses_shared_holdings(self) -> bool:
        """Whether registry uses shared holdings"""
        return self._shared_holdings is not None

    @property
    def algorithms(self) -> List[AlgorithmBase]:
        """Get list of all registered algorithms"""
        return list(self._algorithms.values())

    @property
    def algorithm_names(self) -> List[str]:
        """Get list of registered algorithm names"""
        return list(self._algorithms.keys())

    @property
    def count(self) -> int:
        """Number of registered algorithms"""
        return len(self._algorithms)

    def register(
        self,
        algorithm_class: Type[AlgorithmBase],
        name: Optional[str] = None,
        **kwargs,
    ) -> AlgorithmBase:
        """
        Register an algorithm class.

        Args:
            algorithm_class: The algorithm class to register
            name: Optional name override
            **kwargs: Arguments to pass to algorithm constructor

        Returns:
            The instantiated algorithm
        """
        # Pass shared holdings to algorithm if available
        if self._shared_holdings and "shared_holdings" not in kwargs:
            kwargs["shared_holdings"] = self._shared_holdings

        algo = algorithm_class(**kwargs)
        name = name or algo.name

        # Register algorithm with shared holdings
        if self._shared_holdings and name not in self._shared_holdings.algorithms:
            self._shared_holdings.register_algorithm(name)

        self._algorithms[name] = algo
        logger.info(f"Registered algorithm: {name}")
        return algo

    def unregister(self, name: str) -> bool:
        """
        Unregister an algorithm.

        Args:
            name: Algorithm name

        Returns:
            True if unregistered, False if not found
        """
        if name in self._algorithms:
            del self._algorithms[name]
            return True
        return False

    def get(self, name: str) -> Optional[AlgorithmBase]:
        """
        Get an algorithm by name.

        Args:
            name: Algorithm name

        Returns:
            Algorithm instance or None
        """
        return self._algorithms.get(name)

    def discover(self) -> int:
        """
        Auto-discover and register known algorithms.

        Returns:
            Number of algorithms discovered
        """
        count = 0
        for name, algo_class in self.KNOWN_ALGORITHMS.items():
            if name not in self._algorithms:
                try:
                    self.register(algo_class)
                    count += 1
                except Exception as e:
                    logger.error(f"Failed to register {name}: {e}")
        return count

    def load_all(self) -> Dict[str, bool]:
        """
        Load all registered algorithms.

        Returns:
            Dict mapping algorithm name to load success
        """
        results = {}
        for name, algo in self._algorithms.items():
            try:
                results[name] = algo.load()
            except Exception as e:
                logger.error(f"Failed to load {name}: {e}")
                results[name] = False
        self._loaded = True
        return results

    def run_all(
        self,
        market_data: Dict[str, List[Dict]],
    ) -> Dict[str, AlgorithmResult]:
        """
        Run all registered algorithms.

        Args:
            market_data: Market data to pass to algorithms

        Returns:
            Dict mapping algorithm name to result
        """
        results = {}
        for name, algo in self._algorithms.items():
            if not algo.is_loaded:
                algo.load()
            results[name] = algo.run(market_data)
        return results

    def get_all_instruments(self) -> Dict[str, List[AlgorithmInstrument]]:
        """
        Get all instruments from all algorithms.

        Returns:
            Dict mapping algorithm name to its instruments
        """
        return {
            name: algo.instruments
            for name, algo in self._algorithms.items()
        }

    def get_unique_symbols(self) -> List[str]:
        """
        Get unique symbols across all algorithms.

        Returns:
            List of unique trading symbols
        """
        symbols = set()
        for algo in self._algorithms.values():
            for inst in algo.instruments:
                symbols.add(inst.symbol)
        return sorted(symbols)

    def reconcile(self, portfolio) -> Dict:
        """
        Reconcile shared holdings with IB portfolio.

        Updates prices, quantities, and tracks discrepancies.

        Args:
            portfolio: Portfolio instance with loaded positions

        Returns:
            Dict with reconciliation results
        """
        if not self._shared_holdings:
            return {"success": False, "error": "No shared holdings configured"}

        return self._shared_holdings.reconcile(portfolio)

    def get_combined_signals(
        self,
        market_data: Dict[str, List[Dict]],
    ) -> Dict[str, List]:
        """
        Run all algorithms and combine their signals.

        Useful for analyzing how different algorithms view the same data.

        Args:
            market_data: Market data to pass to algorithms

        Returns:
            Dict with signals organized by symbol
        """
        results = self.run_all(market_data)

        combined = {}
        for algo_name, result in results.items():
            for signal in result.signals:
                if signal.symbol not in combined:
                    combined[signal.symbol] = []
                combined[signal.symbol].append({
                    "algorithm": algo_name,
                    "action": signal.action,
                    "quantity": signal.quantity,
                    "target_weight": signal.target_weight,
                    "reason": signal.reason,
                })

        return combined

    def summary(self) -> str:
        """Get a formatted summary of all algorithms"""
        lines = [
            f"Algorithm Registry: {self.count} algorithms",
            f"Shared Holdings: {'Yes' if self._shared_holdings else 'No'}",
            "=" * 60,
        ]

        for name, algo in self._algorithms.items():
            loaded = "loaded" if algo.is_loaded else "not loaded"
            inst_count = len(algo.instruments)
            lines.append(f"\n{name} ({loaded})")
            lines.append(f"  Description: {algo.description[:60]}...")
            lines.append(f"  Instruments: {inst_count}")

            # Show holdings from appropriate source
            if algo.is_loaded:
                total_value = algo.get_effective_total_value()
                if total_value > 0:
                    lines.append(f"  Portfolio Value: ${total_value:,.2f}")

        if self._shared_holdings:
            lines.append("\n" + "-" * 60)
            lines.append("Shared Holdings Summary:")
            lines.append(f"  Total Value: ${self._shared_holdings.total_value:,.2f}")
            lines.append(f"  Positions: {len(self._shared_holdings.positions)}")
            lines.append(f"  Algorithms: {', '.join(sorted(self._shared_holdings.algorithms))}")

        return "\n".join(lines)

    def __contains__(self, name: str) -> bool:
        return name in self._algorithms

    def __getitem__(self, name: str) -> AlgorithmBase:
        algo = self.get(name)
        if algo is None:
            raise KeyError(f"Algorithm '{name}' not found")
        return algo

    def __iter__(self):
        return iter(self._algorithms.values())

    def __len__(self):
        return len(self._algorithms)


# =============================================================================
# Convenience Functions
# =============================================================================

def get_algorithm(name: str, **kwargs) -> Optional[AlgorithmBase]:
    """
    Get an algorithm instance by name.

    Args:
        name: Algorithm name
        **kwargs: Arguments to pass to constructor

    Returns:
        Algorithm instance or None if not found
    """
    algo_class = AlgorithmRegistry.KNOWN_ALGORITHMS.get(name)
    if algo_class:
        return algo_class(**kwargs)
    return None


def list_algorithms() -> List[str]:
    """Get list of available algorithm names"""
    return list(AlgorithmRegistry.KNOWN_ALGORITHMS.keys())


def create_registry(
    with_shared_holdings: bool = True,
    shared_holdings_file: Optional[str] = None,
) -> AlgorithmRegistry:
    """
    Create and populate an algorithm registry.

    Args:
        with_shared_holdings: Whether to use shared holdings (default True)
        shared_holdings_file: Optional path to shared holdings file

    Returns:
        AlgorithmRegistry with all known algorithms registered
    """
    shared = None
    if with_shared_holdings:
        shared = SharedHoldings(shared_holdings_file) if shared_holdings_file else SharedHoldings()
        shared.load()

    registry = AlgorithmRegistry(shared_holdings=shared)
    registry.discover()
    return registry


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Base classes
    "AlgorithmBase",
    "AlgorithmInstrument",
    "AlgorithmResult",
    "Holdings",
    "HoldingPosition",
    "TradeSignal",
    # Shared Holdings
    "SharedHoldings",
    "SharedPosition",
    "AlgorithmAllocation",
    "CashAllocation",
    "load_shared_holdings",
    # Allocation Manager
    "AllocationManager",
    "AllocationResult",
    "TransferResult",
    "AllocationSummary",
    "create_manager",
    "quick_allocate",
    # Algorithms
    "Momentum5DayAlgorithm",
    "DummyAlgorithm",
    # Registry
    "AlgorithmRegistry",
    # Functions
    "get_algorithm",
    "list_algorithms",
    "create_registry",
]
