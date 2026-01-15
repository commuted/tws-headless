"""
algorithms/allocation_manager.py - Executive functions for managing algorithm allocations

Provides high-level operations for allocating portfolio positions between
algorithms using the Portfolio interface and SharedHoldings system.

Executive Functions:
- sync_with_portfolio: Reconcile SharedHoldings with IB Portfolio
- allocate_to_algorithm: Assign position quantity to an algorithm
- transfer_allocation: Move allocation from one algorithm to another
- auto_allocate: Automatically allocate unallocated positions
- get_allocation_status: View current allocation state

Usage:
    from portfolio import Portfolio
    from algorithms import SharedHoldings
    from algorithms.allocation_manager import AllocationManager

    portfolio = Portfolio()
    portfolio.connect()
    portfolio.load()

    manager = AllocationManager(portfolio)
    manager.sync()
    manager.allocate("momentum_5day", "SPY", 100)
    manager.save()
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .shared_holdings import SharedHoldings, SharedPosition, load_shared_holdings

logger = logging.getLogger(__name__)


@dataclass
class AllocationResult:
    """Result of an allocation operation"""
    success: bool
    message: str
    algorithm: str = ""
    symbol: str = ""
    quantity: float = 0.0
    previous_quantity: float = 0.0


@dataclass
class TransferResult:
    """Result of a transfer operation"""
    success: bool
    message: str
    from_algorithm: str = ""
    to_algorithm: str = ""
    symbol: str = ""
    quantity: float = 0.0


@dataclass
class AllocationSummary:
    """Summary of current allocations"""
    total_value: float
    total_cash: float
    unallocated_cash: float
    positions: List[Dict]
    algorithms: List[str]
    discrepancies: List[Dict]


class AllocationManager:
    """
    Executive manager for algorithm allocations.

    Provides high-level functions for managing position allocations
    between trading algorithms using the IB Portfolio interface.

    Usage:
        portfolio = Portfolio()
        portfolio.connect()
        portfolio.load()

        manager = AllocationManager(portfolio)
        manager.sync()

        # Allocate 100 shares of SPY to momentum algorithm
        result = manager.allocate("momentum_5day", "SPY", 100)

        # Transfer 50 shares from momentum to dummy
        result = manager.transfer("momentum_5day", "dummy", "SPY", 50)

        # Save changes
        manager.save()
    """

    def __init__(
        self,
        portfolio=None,
        shared_holdings: Optional[SharedHoldings] = None,
        holdings_file: Optional[str] = None,
    ):
        """
        Initialize the allocation manager.

        Args:
            portfolio: Portfolio instance (optional, can set later)
            shared_holdings: Existing SharedHoldings instance
            holdings_file: Path to shared holdings JSON file
        """
        self._portfolio = portfolio

        if shared_holdings:
            self._holdings = shared_holdings
        elif holdings_file:
            self._holdings = load_shared_holdings(holdings_file)
        else:
            self._holdings = SharedHoldings()
            self._holdings.load()

        self._last_sync: Optional[datetime] = None
        self._sync_results: Optional[Dict] = None

    @property
    def portfolio(self):
        """Get the portfolio instance"""
        return self._portfolio

    @portfolio.setter
    def portfolio(self, value):
        """Set the portfolio instance"""
        self._portfolio = value

    @property
    def holdings(self) -> SharedHoldings:
        """Get the shared holdings instance"""
        return self._holdings

    @property
    def is_synced(self) -> bool:
        """Whether holdings have been synced with portfolio"""
        return self._last_sync is not None

    @property
    def last_sync(self) -> Optional[datetime]:
        """Last sync timestamp"""
        return self._last_sync

    # =========================================================================
    # Sync Operations
    # =========================================================================

    def sync(self, force: bool = False) -> Dict:
        """
        Synchronize SharedHoldings with the IB Portfolio.

        Updates prices, quantities, and identifies any discrepancies
        between tracked holdings and actual portfolio.

        Args:
            force: Force sync even if recently synced

        Returns:
            Dict with sync results
        """
        if not self._portfolio:
            return {"success": False, "error": "No portfolio connected"}

        if not hasattr(self._portfolio, 'positions'):
            return {"success": False, "error": "Portfolio not loaded"}

        # Perform reconciliation
        results = self._holdings.reconcile(self._portfolio)

        self._last_sync = datetime.now()
        self._sync_results = results

        logger.info(
            f"Sync complete: {len(results.get('updated_positions', []))} updated, "
            f"{len(results.get('new_positions', []))} new, "
            f"{len(results.get('discrepancies', []))} discrepancies"
        )

        return results

    def refresh_prices(self) -> bool:
        """
        Refresh position prices from portfolio without full sync.

        Returns:
            True if successful
        """
        if not self._portfolio:
            return False

        for pos in self._portfolio.positions:
            shared_pos = self._holdings.get_position(pos.symbol)
            if shared_pos:
                shared_pos.current_price = pos.current_price
                shared_pos.market_value = pos.market_value

        return True

    # =========================================================================
    # Allocation Operations
    # =========================================================================

    def allocate(
        self,
        algorithm: str,
        symbol: str,
        quantity: float,
        cost_basis: Optional[float] = None,
    ) -> AllocationResult:
        """
        Allocate position quantity to an algorithm.

        Args:
            algorithm: Algorithm name
            symbol: Position symbol
            quantity: Quantity to allocate
            cost_basis: Optional cost basis (uses position avg_cost if not provided)

        Returns:
            AllocationResult with operation outcome
        """
        symbol = symbol.upper()

        # Validate algorithm
        if algorithm not in self._holdings.algorithms:
            # Auto-register if not registered
            self._holdings.register_algorithm(algorithm)
            logger.info(f"Auto-registered algorithm: {algorithm}")

        # Get position
        pos = self._holdings.get_position(symbol)
        if not pos:
            return AllocationResult(
                success=False,
                message=f"Position {symbol} not found. Run sync() first.",
                algorithm=algorithm,
                symbol=symbol,
            )

        # Check available quantity
        if quantity > pos.unallocated_quantity + 0.001:
            return AllocationResult(
                success=False,
                message=f"Insufficient unallocated quantity. "
                        f"Requested: {quantity}, Available: {pos.unallocated_quantity}",
                algorithm=algorithm,
                symbol=symbol,
                quantity=quantity,
            )

        # Get previous allocation
        prev_qty = pos.get_algorithm_quantity(algorithm)

        # Perform allocation
        basis = cost_basis or pos.avg_cost
        success = pos.allocate(algorithm, quantity, basis)

        if success:
            new_qty = pos.get_algorithm_quantity(algorithm)
            return AllocationResult(
                success=True,
                message=f"Allocated {quantity} {symbol} to {algorithm}",
                algorithm=algorithm,
                symbol=symbol,
                quantity=new_qty,
                previous_quantity=prev_qty,
            )
        else:
            return AllocationResult(
                success=False,
                message=f"Allocation failed",
                algorithm=algorithm,
                symbol=symbol,
            )

    def deallocate(
        self,
        algorithm: str,
        symbol: str,
        quantity: float,
    ) -> AllocationResult:
        """
        Deallocate position quantity from an algorithm.

        Args:
            algorithm: Algorithm name
            symbol: Position symbol
            quantity: Quantity to deallocate

        Returns:
            AllocationResult with operation outcome
        """
        symbol = symbol.upper()

        # Get position
        pos = self._holdings.get_position(symbol)
        if not pos:
            return AllocationResult(
                success=False,
                message=f"Position {symbol} not found",
                algorithm=algorithm,
                symbol=symbol,
            )

        # Get current allocation
        current_qty = pos.get_algorithm_quantity(algorithm)
        if current_qty == 0:
            return AllocationResult(
                success=False,
                message=f"Algorithm {algorithm} has no allocation in {symbol}",
                algorithm=algorithm,
                symbol=symbol,
            )

        # Deallocate
        actual = pos.deallocate(algorithm, quantity)

        return AllocationResult(
            success=True,
            message=f"Deallocated {actual} {symbol} from {algorithm}",
            algorithm=algorithm,
            symbol=symbol,
            quantity=current_qty - actual,
            previous_quantity=current_qty,
        )

    def transfer(
        self,
        from_algorithm: str,
        to_algorithm: str,
        symbol: str,
        quantity: float,
    ) -> TransferResult:
        """
        Transfer allocation from one algorithm to another.

        Args:
            from_algorithm: Source algorithm
            to_algorithm: Destination algorithm
            symbol: Position symbol
            quantity: Quantity to transfer

        Returns:
            TransferResult with operation outcome
        """
        symbol = symbol.upper()

        # Get position
        pos = self._holdings.get_position(symbol)
        if not pos:
            return TransferResult(
                success=False,
                message=f"Position {symbol} not found",
                from_algorithm=from_algorithm,
                to_algorithm=to_algorithm,
                symbol=symbol,
            )

        # Check source has enough
        source_qty = pos.get_algorithm_quantity(from_algorithm)
        if source_qty < quantity:
            return TransferResult(
                success=False,
                message=f"{from_algorithm} only has {source_qty} {symbol}, "
                        f"cannot transfer {quantity}",
                from_algorithm=from_algorithm,
                to_algorithm=to_algorithm,
                symbol=symbol,
            )

        # Ensure destination is registered
        if to_algorithm not in self._holdings.algorithms:
            self._holdings.register_algorithm(to_algorithm)

        # Get cost basis from source
        source_alloc = pos.get_allocation(from_algorithm)
        cost_basis = source_alloc.cost_basis if source_alloc else pos.avg_cost

        # Deallocate from source
        actual = pos.deallocate(from_algorithm, quantity)

        # Allocate to destination
        pos.allocate(to_algorithm, actual, cost_basis)

        return TransferResult(
            success=True,
            message=f"Transferred {actual} {symbol} from {from_algorithm} to {to_algorithm}",
            from_algorithm=from_algorithm,
            to_algorithm=to_algorithm,
            symbol=symbol,
            quantity=actual,
        )

    # =========================================================================
    # Cash Operations
    # =========================================================================

    def allocate_cash(
        self,
        algorithm: str,
        amount: float,
    ) -> AllocationResult:
        """
        Allocate cash to an algorithm.

        Args:
            algorithm: Algorithm name
            amount: Cash amount to allocate

        Returns:
            AllocationResult with operation outcome
        """
        # Ensure algorithm is registered
        if algorithm not in self._holdings.algorithms:
            self._holdings.register_algorithm(algorithm)

        # Get current allocation
        prev_amount = self._holdings.get_algorithm_cash(algorithm)

        # Allocate
        success = self._holdings.allocate_cash(algorithm, amount)

        if success:
            new_amount = self._holdings.get_algorithm_cash(algorithm)
            return AllocationResult(
                success=True,
                message=f"Allocated ${amount:,.2f} cash to {algorithm}",
                algorithm=algorithm,
                symbol="_CASH",
                quantity=new_amount,
                previous_quantity=prev_amount,
            )
        else:
            return AllocationResult(
                success=False,
                message=f"Insufficient unallocated cash. "
                        f"Available: ${self._holdings.unallocated_cash:,.2f}",
                algorithm=algorithm,
                symbol="_CASH",
            )

    def transfer_cash(
        self,
        from_algorithm: str,
        to_algorithm: str,
        amount: float,
    ) -> TransferResult:
        """
        Transfer cash from one algorithm to another.

        Args:
            from_algorithm: Source algorithm
            to_algorithm: Destination algorithm
            amount: Cash amount to transfer

        Returns:
            TransferResult with operation outcome
        """
        # Check source has enough
        source_cash = self._holdings.get_algorithm_cash(from_algorithm)
        if source_cash < amount:
            return TransferResult(
                success=False,
                message=f"{from_algorithm} only has ${source_cash:,.2f}, "
                        f"cannot transfer ${amount:,.2f}",
                from_algorithm=from_algorithm,
                to_algorithm=to_algorithm,
                symbol="_CASH",
            )

        # Ensure destination is registered
        if to_algorithm not in self._holdings.algorithms:
            self._holdings.register_algorithm(to_algorithm)

        # Deallocate from source
        actual = self._holdings.deallocate_cash(from_algorithm, amount)

        # Allocate to destination
        self._holdings.allocate_cash(to_algorithm, actual)

        return TransferResult(
            success=True,
            message=f"Transferred ${actual:,.2f} cash from {from_algorithm} to {to_algorithm}",
            from_algorithm=from_algorithm,
            to_algorithm=to_algorithm,
            symbol="_CASH",
            quantity=actual,
        )

    # =========================================================================
    # Auto-Allocation
    # =========================================================================

    def auto_allocate(
        self,
        algorithm: str,
        include_cash: bool = True,
    ) -> Dict:
        """
        Automatically allocate all unallocated positions and cash to an algorithm.

        Useful for:
        - Initial setup with a single algorithm
        - Migrating from manual tracking to shared holdings
        - Assigning orphaned positions

        Args:
            algorithm: Algorithm to receive allocations
            include_cash: Whether to also allocate cash

        Returns:
            Dict with allocation results
        """
        # Ensure algorithm is registered
        if algorithm not in self._holdings.algorithms:
            self._holdings.register_algorithm(algorithm)

        results = {
            "algorithm": algorithm,
            "positions_allocated": [],
            "cash_allocated": 0.0,
            "errors": [],
        }

        # Allocate unallocated positions
        for pos in self._holdings.positions:
            unalloc = pos.unallocated_quantity
            if unalloc > 0.001:
                if pos.allocate(algorithm, unalloc, pos.avg_cost):
                    results["positions_allocated"].append({
                        "symbol": pos.symbol,
                        "quantity": unalloc,
                        "value": unalloc * pos.current_price,
                    })
                else:
                    results["errors"].append(f"Failed to allocate {pos.symbol}")

        # Allocate unallocated cash
        if include_cash and self._holdings.unallocated_cash > 0:
            amount = self._holdings.unallocated_cash
            if self._holdings.allocate_cash(algorithm, amount):
                results["cash_allocated"] = amount
            else:
                results["errors"].append("Failed to allocate cash")

        logger.info(
            f"Auto-allocated to {algorithm}: "
            f"{len(results['positions_allocated'])} positions, "
            f"${results['cash_allocated']:,.2f} cash"
        )

        return results

    def distribute_equally(
        self,
        algorithms: List[str],
        symbol: Optional[str] = None,
    ) -> Dict:
        """
        Distribute unallocated quantities equally among algorithms.

        Args:
            algorithms: List of algorithms to distribute to
            symbol: Specific symbol (or all if None)

        Returns:
            Dict with distribution results
        """
        if not algorithms:
            return {"success": False, "error": "No algorithms specified"}

        # Register algorithms
        for algo in algorithms:
            if algo not in self._holdings.algorithms:
                self._holdings.register_algorithm(algo)

        results = {
            "distributions": [],
            "errors": [],
        }

        # Get positions to distribute
        if symbol:
            positions = [self._holdings.get_position(symbol.upper())]
            positions = [p for p in positions if p]
        else:
            positions = self._holdings.positions

        n_algos = len(algorithms)

        for pos in positions:
            unalloc = pos.unallocated_quantity
            if unalloc > 0.001:
                # Split equally
                qty_per_algo = unalloc / n_algos

                for algo in algorithms:
                    if pos.allocate(algo, qty_per_algo, pos.avg_cost):
                        results["distributions"].append({
                            "algorithm": algo,
                            "symbol": pos.symbol,
                            "quantity": qty_per_algo,
                        })
                    else:
                        results["errors"].append(
                            f"Failed to allocate {pos.symbol} to {algo}"
                        )

        return results

    # =========================================================================
    # Status and Reporting
    # =========================================================================

    def get_status(self) -> AllocationSummary:
        """
        Get current allocation status summary.

        Returns:
            AllocationSummary with current state
        """
        positions = []

        for pos in self._holdings.positions:
            pos_info = {
                "symbol": pos.symbol,
                "total_quantity": pos.total_quantity,
                "current_price": pos.current_price,
                "market_value": pos.market_value,
                "allocated": pos.allocated_quantity,
                "unallocated": pos.unallocated_quantity,
                "allocations": {},
            }

            for alloc in pos.allocations:
                pos_info["allocations"][alloc.algorithm] = {
                    "quantity": alloc.quantity,
                    "cost_basis": alloc.cost_basis,
                    "value": alloc.quantity * pos.current_price,
                }

            positions.append(pos_info)

        return AllocationSummary(
            total_value=self._holdings.total_value,
            total_cash=self._holdings.total_cash,
            unallocated_cash=self._holdings.unallocated_cash,
            positions=positions,
            algorithms=list(self._holdings.algorithms),
            discrepancies=self._sync_results.get("discrepancies", []) if self._sync_results else [],
        )

    def get_algorithm_summary(self, algorithm: str) -> Dict:
        """
        Get detailed summary for a specific algorithm.

        Args:
            algorithm: Algorithm name

        Returns:
            Dict with algorithm's holdings and weights
        """
        holdings = self._holdings.get_algorithm_holdings(algorithm)
        weights = self._holdings.get_algorithm_weights(algorithm)

        return {
            "algorithm": algorithm,
            "holdings": holdings,
            "weights": weights,
        }

    def format_status(self) -> str:
        """
        Get formatted string of current allocation status.

        Returns:
            Formatted status string
        """
        status = self.get_status()

        lines = [
            "=" * 80,
            "ALLOCATION STATUS",
            "=" * 80,
            f"Total Portfolio Value: ${status.total_value:,.2f}",
            f"Total Cash: ${status.total_cash:,.2f} (Unallocated: ${status.unallocated_cash:,.2f})",
            f"Registered Algorithms: {', '.join(status.algorithms) or 'None'}",
            f"Last Sync: {self._last_sync.strftime('%Y-%m-%d %H:%M:%S') if self._last_sync else 'Never'}",
            "",
            "-" * 80,
            "POSITIONS",
            "-" * 80,
        ]

        # Position header
        lines.append(
            f"{'Symbol':<8} {'Total':>10} {'Price':>10} {'Value':>12} "
            f"{'Alloc':>10} {'Unalloc':>10}"
        )
        lines.append("-" * 80)

        for pos in sorted(status.positions, key=lambda p: -p["market_value"]):
            lines.append(
                f"{pos['symbol']:<8} {pos['total_quantity']:>10.0f} "
                f"${pos['current_price']:>9.2f} ${pos['market_value']:>11,.2f} "
                f"{pos['allocated']:>10.0f} {pos['unallocated']:>10.0f}"
            )

            # Show allocations
            for algo, alloc in pos["allocations"].items():
                lines.append(
                    f"  -> {algo:<20} {alloc['quantity']:>8.0f} "
                    f"@ ${alloc['cost_basis']:>7.2f} = ${alloc['value']:>10,.2f}"
                )

        # Cash allocations
        lines.extend([
            "",
            "-" * 80,
            "CASH ALLOCATIONS",
            "-" * 80,
        ])

        for algo in status.algorithms:
            cash = self._holdings.get_algorithm_cash(algo)
            if cash > 0:
                pct = (cash / status.total_cash * 100) if status.total_cash > 0 else 0
                lines.append(f"  {algo:<30} ${cash:>12,.2f} ({pct:>5.1f}%)")

        if status.unallocated_cash > 0:
            pct = (status.unallocated_cash / status.total_cash * 100) if status.total_cash > 0 else 0
            lines.append(f"  {'[unallocated]':<30} ${status.unallocated_cash:>12,.2f} ({pct:>5.1f}%)")

        # Discrepancies
        if status.discrepancies:
            lines.extend([
                "",
                "-" * 80,
                "DISCREPANCIES",
                "-" * 80,
            ])
            for d in status.discrepancies:
                lines.append(f"  {d}")

        lines.append("=" * 80)

        return "\n".join(lines)

    # =========================================================================
    # Persistence
    # =========================================================================

    def save(self) -> bool:
        """
        Save current holdings state to file.

        Returns:
            True if successful
        """
        return self._holdings.save()

    def reload(self) -> bool:
        """
        Reload holdings from file (discards unsaved changes).

        Returns:
            True if successful
        """
        return self._holdings.load()


# =============================================================================
# Convenience Functions
# =============================================================================

def create_manager(
    portfolio=None,
    holdings_file: Optional[str] = None,
    auto_sync: bool = True,
) -> AllocationManager:
    """
    Create and optionally sync an AllocationManager.

    Args:
        portfolio: Portfolio instance
        holdings_file: Optional path to holdings file
        auto_sync: Whether to sync immediately

    Returns:
        Configured AllocationManager
    """
    manager = AllocationManager(
        portfolio=portfolio,
        holdings_file=holdings_file,
    )

    if portfolio and auto_sync:
        manager.sync()

    return manager


def quick_allocate(
    portfolio,
    algorithm: str,
    symbol: str,
    quantity: float,
    holdings_file: Optional[str] = None,
) -> AllocationResult:
    """
    Quickly allocate a position to an algorithm.

    Args:
        portfolio: Connected Portfolio instance
        algorithm: Algorithm name
        symbol: Position symbol
        quantity: Quantity to allocate
        holdings_file: Optional path to holdings file

    Returns:
        AllocationResult
    """
    manager = create_manager(portfolio, holdings_file)
    result = manager.allocate(algorithm, symbol, quantity)
    if result.success:
        manager.save()
    return result
