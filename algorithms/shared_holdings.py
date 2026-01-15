"""
algorithms/shared_holdings.py - Shared holdings management across algorithms

Provides a centralized system for tracking position ownership across multiple
algorithms, reconciling with the actual IB portfolio, and managing allocations.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class AlgorithmAllocation:
    """Tracks an algorithm's allocation in a position"""
    algorithm: str
    quantity: float
    cost_basis: float = 0.0
    allocated_at: Optional[datetime] = None

    def to_dict(self) -> Dict:
        return {
            "algorithm": self.algorithm,
            "quantity": self.quantity,
            "cost_basis": self.cost_basis,
            "allocated_at": self.allocated_at.isoformat() if self.allocated_at else None,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "AlgorithmAllocation":
        allocated_at = None
        if data.get("allocated_at"):
            allocated_at = datetime.fromisoformat(data["allocated_at"])
        return cls(
            algorithm=data["algorithm"],
            quantity=data.get("quantity", 0),
            cost_basis=data.get("cost_basis", 0.0),
            allocated_at=allocated_at,
        )


@dataclass
class SharedPosition:
    """A position shared across multiple algorithms"""
    symbol: str
    total_quantity: float = 0.0
    current_price: float = 0.0
    market_value: float = 0.0
    avg_cost: float = 0.0
    unrealized_pnl: float = 0.0
    allocations: List[AlgorithmAllocation] = field(default_factory=list)

    @property
    def allocated_quantity(self) -> float:
        """Total quantity allocated to algorithms"""
        return sum(a.quantity for a in self.allocations)

    @property
    def unallocated_quantity(self) -> float:
        """Quantity not allocated to any algorithm"""
        return self.total_quantity - self.allocated_quantity

    def get_allocation(self, algorithm: str) -> Optional[AlgorithmAllocation]:
        """Get allocation for a specific algorithm"""
        for alloc in self.allocations:
            if alloc.algorithm == algorithm:
                return alloc
        return None

    def get_algorithm_quantity(self, algorithm: str) -> float:
        """Get quantity owned by a specific algorithm"""
        alloc = self.get_allocation(algorithm)
        return alloc.quantity if alloc else 0.0

    def get_algorithm_value(self, algorithm: str) -> float:
        """Get market value owned by a specific algorithm"""
        qty = self.get_algorithm_quantity(algorithm)
        return qty * self.current_price

    def allocate(self, algorithm: str, quantity: float, cost_basis: float = 0.0) -> bool:
        """
        Allocate quantity to an algorithm.

        Returns True if successful, False if insufficient unallocated quantity.
        """
        if quantity > self.unallocated_quantity + 0.001:  # Small tolerance
            logger.warning(
                f"Cannot allocate {quantity} {self.symbol} to {algorithm}: "
                f"only {self.unallocated_quantity} unallocated"
            )
            return False

        existing = self.get_allocation(algorithm)
        if existing:
            existing.quantity += quantity
            if cost_basis > 0:
                # Update weighted average cost basis
                total_cost = existing.cost_basis * (existing.quantity - quantity) + cost_basis * quantity
                existing.cost_basis = total_cost / existing.quantity if existing.quantity > 0 else 0
        else:
            self.allocations.append(AlgorithmAllocation(
                algorithm=algorithm,
                quantity=quantity,
                cost_basis=cost_basis or self.avg_cost,
                allocated_at=datetime.now(),
            ))
        return True

    def deallocate(self, algorithm: str, quantity: float) -> float:
        """
        Deallocate quantity from an algorithm.

        Returns the actual quantity deallocated.
        """
        alloc = self.get_allocation(algorithm)
        if not alloc:
            return 0.0

        actual = min(quantity, alloc.quantity)
        alloc.quantity -= actual

        # Remove allocation if empty
        if alloc.quantity <= 0:
            self.allocations.remove(alloc)

        return actual

    def to_dict(self) -> Dict:
        return {
            "symbol": self.symbol,
            "total_quantity": self.total_quantity,
            "current_price": self.current_price,
            "market_value": self.market_value,
            "avg_cost": self.avg_cost,
            "unrealized_pnl": self.unrealized_pnl,
            "allocations": [a.to_dict() for a in self.allocations],
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "SharedPosition":
        return cls(
            symbol=data["symbol"],
            total_quantity=data.get("total_quantity", 0),
            current_price=data.get("current_price", 0),
            market_value=data.get("market_value", 0),
            avg_cost=data.get("avg_cost", 0),
            unrealized_pnl=data.get("unrealized_pnl", 0),
            allocations=[AlgorithmAllocation.from_dict(a) for a in data.get("allocations", [])],
        )


@dataclass
class CashAllocation:
    """Cash allocation for an algorithm"""
    algorithm: str
    amount: float
    allocated_at: Optional[datetime] = None

    def to_dict(self) -> Dict:
        return {
            "algorithm": self.algorithm,
            "amount": self.amount,
            "allocated_at": self.allocated_at.isoformat() if self.allocated_at else None,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "CashAllocation":
        allocated_at = None
        if data.get("allocated_at"):
            allocated_at = datetime.fromisoformat(data["allocated_at"])
        return cls(
            algorithm=data["algorithm"],
            amount=data.get("amount", 0),
            allocated_at=allocated_at,
        )


# =============================================================================
# Shared Holdings Manager
# =============================================================================

class SharedHoldings:
    """
    Manages shared holdings across multiple algorithms.

    Tracks:
    - Total portfolio value and positions from IB
    - Algorithm ownership of each position
    - Cash allocations per algorithm
    - Reconciliation with actual IB portfolio

    Usage:
        holdings = SharedHoldings()
        holdings.load()

        # Reconcile with IB portfolio
        holdings.reconcile(portfolio)

        # Get algorithm's view
        algo_holdings = holdings.get_algorithm_holdings("momentum_5day")

        # Allocate position to algorithm
        holdings.allocate_position("momentum_5day", "SPY", 100, cost_basis=450.0)
    """

    DEFAULT_FILE = Path(__file__).parent / "shared_holdings.json"

    def __init__(self, file_path: Optional[str] = None):
        self._file_path = Path(file_path) if file_path else self.DEFAULT_FILE

        # Portfolio data
        self._total_cash: float = 0.0
        self._cash_allocations: List[CashAllocation] = []
        self._positions: Dict[str, SharedPosition] = {}

        # Metadata
        self._last_reconciled: Optional[datetime] = None
        self._last_updated: Optional[datetime] = None
        self._registered_algorithms: Set[str] = set()

        self._loaded = False

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def total_cash(self) -> float:
        """Total cash in portfolio"""
        return self._total_cash

    @property
    def allocated_cash(self) -> float:
        """Total cash allocated to algorithms"""
        return sum(a.amount for a in self._cash_allocations)

    @property
    def unallocated_cash(self) -> float:
        """Cash not allocated to any algorithm"""
        return self._total_cash - self.allocated_cash

    @property
    def total_value(self) -> float:
        """Total portfolio value (cash + positions)"""
        position_value = sum(p.market_value for p in self._positions.values())
        return self._total_cash + position_value

    @property
    def positions(self) -> List[SharedPosition]:
        """All positions"""
        return list(self._positions.values())

    @property
    def symbols(self) -> List[str]:
        """All position symbols"""
        return list(self._positions.keys())

    @property
    def algorithms(self) -> Set[str]:
        """Registered algorithm names"""
        return self._registered_algorithms.copy()

    @property
    def is_loaded(self) -> bool:
        return self._loaded

    # =========================================================================
    # Load/Save
    # =========================================================================

    def load(self) -> bool:
        """Load holdings from file"""
        if not self._file_path.exists():
            logger.info(f"No holdings file found at {self._file_path}, starting fresh")
            self._loaded = True
            return True

        try:
            with open(self._file_path) as f:
                data = json.load(f)

            self._total_cash = data.get("total_cash", 0)
            self._cash_allocations = [
                CashAllocation.from_dict(a)
                for a in data.get("cash_allocations", [])
            ]
            self._positions = {
                p["symbol"]: SharedPosition.from_dict(p)
                for p in data.get("positions", [])
            }
            self._registered_algorithms = set(data.get("registered_algorithms", []))

            if data.get("last_reconciled"):
                self._last_reconciled = datetime.fromisoformat(data["last_reconciled"])
            if data.get("last_updated"):
                self._last_updated = datetime.fromisoformat(data["last_updated"])

            self._loaded = True
            logger.info(f"Loaded shared holdings: {len(self._positions)} positions, "
                       f"{len(self._registered_algorithms)} algorithms")
            return True

        except Exception as e:
            logger.error(f"Failed to load holdings: {e}")
            return False

    def save(self) -> bool:
        """Save holdings to file"""
        try:
            self._last_updated = datetime.now()

            data = {
                "total_cash": self._total_cash,
                "cash_allocations": [a.to_dict() for a in self._cash_allocations],
                "positions": [p.to_dict() for p in self._positions.values()],
                "registered_algorithms": list(self._registered_algorithms),
                "last_reconciled": self._last_reconciled.isoformat() if self._last_reconciled else None,
                "last_updated": self._last_updated.isoformat(),
            }

            # Ensure directory exists
            self._file_path.parent.mkdir(parents=True, exist_ok=True)

            with open(self._file_path, "w") as f:
                json.dump(data, f, indent=2)

            logger.info(f"Saved shared holdings to {self._file_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to save holdings: {e}")
            return False

    # =========================================================================
    # Algorithm Registration
    # =========================================================================

    def register_algorithm(self, algorithm: str, initial_cash: float = 0.0) -> bool:
        """
        Register an algorithm with the shared holdings.

        Args:
            algorithm: Algorithm name
            initial_cash: Initial cash allocation

        Returns:
            True if registered, False if already registered
        """
        if algorithm in self._registered_algorithms:
            logger.warning(f"Algorithm '{algorithm}' already registered")
            return False

        self._registered_algorithms.add(algorithm)

        if initial_cash > 0:
            self.allocate_cash(algorithm, initial_cash)

        logger.info(f"Registered algorithm '{algorithm}' with ${initial_cash:,.2f}")
        return True

    def unregister_algorithm(self, algorithm: str) -> bool:
        """
        Unregister an algorithm and release its allocations.

        Args:
            algorithm: Algorithm name

        Returns:
            True if unregistered
        """
        if algorithm not in self._registered_algorithms:
            return False

        # Release all position allocations
        for pos in self._positions.values():
            alloc = pos.get_allocation(algorithm)
            if alloc:
                pos.deallocate(algorithm, alloc.quantity)

        # Release cash allocation
        self._cash_allocations = [
            a for a in self._cash_allocations
            if a.algorithm != algorithm
        ]

        self._registered_algorithms.remove(algorithm)
        logger.info(f"Unregistered algorithm '{algorithm}'")
        return True

    # =========================================================================
    # Cash Management
    # =========================================================================

    def get_algorithm_cash(self, algorithm: str) -> float:
        """Get cash allocated to an algorithm"""
        for alloc in self._cash_allocations:
            if alloc.algorithm == algorithm:
                return alloc.amount
        return 0.0

    def allocate_cash(self, algorithm: str, amount: float) -> bool:
        """
        Allocate cash to an algorithm.

        Args:
            algorithm: Algorithm name
            amount: Amount to allocate

        Returns:
            True if successful
        """
        if amount > self.unallocated_cash + 0.01:
            logger.warning(
                f"Cannot allocate ${amount:,.2f} to {algorithm}: "
                f"only ${self.unallocated_cash:,.2f} unallocated"
            )
            return False

        # Find existing allocation
        for alloc in self._cash_allocations:
            if alloc.algorithm == algorithm:
                alloc.amount += amount
                return True

        # Create new allocation
        self._cash_allocations.append(CashAllocation(
            algorithm=algorithm,
            amount=amount,
            allocated_at=datetime.now(),
        ))
        return True

    def deallocate_cash(self, algorithm: str, amount: float) -> float:
        """
        Deallocate cash from an algorithm.

        Returns actual amount deallocated.
        """
        for alloc in self._cash_allocations:
            if alloc.algorithm == algorithm:
                actual = min(amount, alloc.amount)
                alloc.amount -= actual
                if alloc.amount <= 0:
                    self._cash_allocations.remove(alloc)
                return actual
        return 0.0

    # =========================================================================
    # Position Management
    # =========================================================================

    def get_position(self, symbol: str) -> Optional[SharedPosition]:
        """Get a position by symbol"""
        return self._positions.get(symbol.upper())

    def allocate_position(
        self,
        algorithm: str,
        symbol: str,
        quantity: float,
        cost_basis: float = 0.0,
    ) -> bool:
        """
        Allocate a position quantity to an algorithm.

        Args:
            algorithm: Algorithm name
            symbol: Position symbol
            quantity: Quantity to allocate
            cost_basis: Cost basis for the allocation

        Returns:
            True if successful
        """
        symbol = symbol.upper()
        pos = self._positions.get(symbol)

        if not pos:
            logger.warning(f"Position {symbol} not found")
            return False

        return pos.allocate(algorithm, quantity, cost_basis)

    def deallocate_position(
        self,
        algorithm: str,
        symbol: str,
        quantity: float,
    ) -> float:
        """
        Deallocate position quantity from an algorithm.

        Returns actual quantity deallocated.
        """
        symbol = symbol.upper()
        pos = self._positions.get(symbol)

        if not pos:
            return 0.0

        return pos.deallocate(algorithm, quantity)

    def get_algorithm_position(self, algorithm: str, symbol: str) -> Tuple[float, float]:
        """
        Get an algorithm's allocation in a position.

        Returns:
            Tuple of (quantity, market_value)
        """
        pos = self.get_position(symbol)
        if not pos:
            return (0.0, 0.0)

        qty = pos.get_algorithm_quantity(algorithm)
        value = pos.get_algorithm_value(algorithm)
        return (qty, value)

    # =========================================================================
    # Algorithm View
    # =========================================================================

    def get_algorithm_holdings(self, algorithm: str) -> Dict:
        """
        Get a complete view of an algorithm's holdings.

        Returns:
            Dict with cash, positions, and total value
        """
        cash = self.get_algorithm_cash(algorithm)

        positions = []
        position_value = 0.0

        for pos in self._positions.values():
            qty = pos.get_algorithm_quantity(algorithm)
            if qty > 0:
                value = pos.get_algorithm_value(algorithm)
                alloc = pos.get_allocation(algorithm)
                positions.append({
                    "symbol": pos.symbol,
                    "quantity": qty,
                    "current_price": pos.current_price,
                    "market_value": value,
                    "cost_basis": alloc.cost_basis if alloc else 0,
                    "unrealized_pnl": value - (alloc.cost_basis * qty if alloc else 0),
                })
                position_value += value

        total_value = cash + position_value

        return {
            "algorithm": algorithm,
            "cash": cash,
            "positions": positions,
            "position_value": position_value,
            "total_value": total_value,
        }

    def get_algorithm_weights(self, algorithm: str) -> Dict[str, float]:
        """
        Get position weights for an algorithm.

        Returns:
            Dict mapping symbol to weight (0-100)
        """
        holdings = self.get_algorithm_holdings(algorithm)
        total = holdings["total_value"]

        if total <= 0:
            return {}

        weights = {"_CASH": (holdings["cash"] / total) * 100}

        for pos in holdings["positions"]:
            weights[pos["symbol"]] = (pos["market_value"] / total) * 100

        return weights

    # =========================================================================
    # Reconciliation
    # =========================================================================

    def reconcile(self, portfolio) -> Dict:
        """
        Reconcile shared holdings with actual IB portfolio.

        Updates prices, quantities, and identifies discrepancies.

        Args:
            portfolio: Portfolio instance with loaded positions

        Returns:
            Dict with reconciliation results
        """
        if not portfolio or not hasattr(portfolio, 'positions'):
            return {"success": False, "error": "Invalid portfolio"}

        results = {
            "success": True,
            "updated_positions": [],
            "new_positions": [],
            "removed_positions": [],
            "discrepancies": [],
        }

        # Get actual positions from portfolio
        actual_positions = {p.symbol: p for p in portfolio.positions}
        actual_cash = getattr(portfolio, '_cash_balance', 0.0)

        # Update cash
        old_cash = self._total_cash
        self._total_cash = actual_cash
        if abs(old_cash - actual_cash) > 0.01:
            results["discrepancies"].append({
                "type": "cash",
                "expected": old_cash,
                "actual": actual_cash,
            })

        # Update existing positions and add new ones
        for symbol, actual in actual_positions.items():
            if symbol in self._positions:
                pos = self._positions[symbol]
                old_qty = pos.total_quantity

                # Update from actual
                pos.total_quantity = actual.quantity
                pos.current_price = actual.current_price
                pos.market_value = actual.market_value
                pos.avg_cost = actual.avg_cost
                pos.unrealized_pnl = actual.unrealized_pnl

                results["updated_positions"].append(symbol)

                # Check for quantity discrepancy
                if abs(old_qty - actual.quantity) > 0.001:
                    results["discrepancies"].append({
                        "type": "quantity",
                        "symbol": symbol,
                        "expected": old_qty,
                        "actual": actual.quantity,
                    })

            else:
                # New position
                self._positions[symbol] = SharedPosition(
                    symbol=symbol,
                    total_quantity=actual.quantity,
                    current_price=actual.current_price,
                    market_value=actual.market_value,
                    avg_cost=actual.avg_cost,
                    unrealized_pnl=actual.unrealized_pnl,
                )
                results["new_positions"].append(symbol)

        # Check for positions that no longer exist
        for symbol in list(self._positions.keys()):
            if symbol not in actual_positions:
                # Position closed - check if any algorithms still have allocations
                pos = self._positions[symbol]
                if pos.allocated_quantity > 0:
                    results["discrepancies"].append({
                        "type": "position_closed",
                        "symbol": symbol,
                        "allocated_quantity": pos.allocated_quantity,
                    })
                del self._positions[symbol]
                results["removed_positions"].append(symbol)

        self._last_reconciled = datetime.now()
        self.save()

        logger.info(
            f"Reconciled: {len(results['updated_positions'])} updated, "
            f"{len(results['new_positions'])} new, "
            f"{len(results['removed_positions'])} removed, "
            f"{len(results['discrepancies'])} discrepancies"
        )

        return results

    def auto_allocate_unallocated(self, algorithm: str) -> Dict:
        """
        Automatically allocate all unallocated positions and cash to an algorithm.

        Useful for migrating to shared holdings or single-algorithm setups.

        Returns:
            Dict with allocation results
        """
        results = {
            "cash_allocated": 0.0,
            "positions_allocated": [],
        }

        # Allocate unallocated cash
        if self.unallocated_cash > 0:
            amount = self.unallocated_cash
            if self.allocate_cash(algorithm, amount):
                results["cash_allocated"] = amount

        # Allocate unallocated positions
        for pos in self._positions.values():
            unalloc = pos.unallocated_quantity
            if unalloc > 0:
                if pos.allocate(algorithm, unalloc, pos.avg_cost):
                    results["positions_allocated"].append({
                        "symbol": pos.symbol,
                        "quantity": unalloc,
                    })

        self.save()
        return results

    # =========================================================================
    # Summary
    # =========================================================================

    def summary(self) -> str:
        """Get a formatted summary of shared holdings"""
        lines = [
            "Shared Holdings Summary",
            "=" * 70,
            f"Total Value: ${self.total_value:,.2f}",
            f"Cash: ${self._total_cash:,.2f} (${self.unallocated_cash:,.2f} unallocated)",
            f"Positions: {len(self._positions)}",
            f"Algorithms: {', '.join(sorted(self._registered_algorithms)) or 'None'}",
            "",
            "Positions by Algorithm:",
            "-" * 70,
        ]

        for pos in sorted(self._positions.values(), key=lambda p: -p.market_value):
            lines.append(
                f"{pos.symbol:8} Total: {pos.total_quantity:>8.0f} @ ${pos.current_price:>8.2f} "
                f"= ${pos.market_value:>12,.2f}"
            )
            for alloc in pos.allocations:
                pct = (alloc.quantity / pos.total_quantity * 100) if pos.total_quantity > 0 else 0
                lines.append(
                    f"         {alloc.algorithm:20} {alloc.quantity:>8.0f} ({pct:>5.1f}%)"
                )
            if pos.unallocated_quantity > 0:
                pct = (pos.unallocated_quantity / pos.total_quantity * 100)
                lines.append(
                    f"         {'[unallocated]':20} {pos.unallocated_quantity:>8.0f} ({pct:>5.1f}%)"
                )

        lines.extend([
            "",
            "Cash by Algorithm:",
            "-" * 70,
        ])

        for alloc in self._cash_allocations:
            pct = (alloc.amount / self._total_cash * 100) if self._total_cash > 0 else 0
            lines.append(f"  {alloc.algorithm:20} ${alloc.amount:>12,.2f} ({pct:>5.1f}%)")

        if self.unallocated_cash > 0:
            pct = (self.unallocated_cash / self._total_cash * 100) if self._total_cash > 0 else 0
            lines.append(f"  {'[unallocated]':20} ${self.unallocated_cash:>12,.2f} ({pct:>5.1f}%)")

        if self._last_reconciled:
            lines.append(f"\nLast reconciled: {self._last_reconciled.isoformat()}")

        return "\n".join(lines)

    def __repr__(self) -> str:
        return (
            f"SharedHoldings(value=${self.total_value:,.2f}, "
            f"positions={len(self._positions)}, "
            f"algorithms={len(self._registered_algorithms)})"
        )


# =============================================================================
# Convenience Functions
# =============================================================================

def load_shared_holdings(file_path: Optional[str] = None) -> SharedHoldings:
    """Load and return shared holdings"""
    holdings = SharedHoldings(file_path)
    holdings.load()
    return holdings
