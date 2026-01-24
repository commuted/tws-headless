"""
order_reconciler.py - Order reconciliation and netting

Aggregates orders from multiple algorithms trading the same instruments,
netting them before sending to IB to minimize market impact and costs.

Example:
    Algorithm A: BUY 100 SPY
    Algorithm B: SELL 30 SPY
    Algorithm C: BUY 20 SPY
    --------------------------
    Net order:   BUY 90 SPY (sent to IB)
"""

import logging
from threading import Lock
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from collections import defaultdict

from ibapi.contract import Contract
from ibapi.order import Order

from .algorithms.base import TradeSignal

logger = logging.getLogger(__name__)


@dataclass
class PendingSignal:
    """A pending trade signal from an algorithm"""
    algorithm_name: str
    signal: TradeSignal
    contract: Contract
    received_at: datetime = field(default_factory=datetime.now)


@dataclass
class ReconciledOrder:
    """A reconciled/netted order ready for execution"""
    symbol: str
    contract: Contract
    action: str  # BUY or SELL
    net_quantity: int
    contributing_signals: List[PendingSignal]
    created_at: datetime = field(default_factory=datetime.now)

    @property
    def algorithm_breakdown(self) -> Dict[str, Tuple[str, int]]:
        """Get breakdown of contributions by algorithm"""
        breakdown = {}
        for ps in self.contributing_signals:
            breakdown[ps.algorithm_name] = (ps.signal.action, ps.signal.quantity)
        return breakdown


@dataclass
class ExecutionAllocation:
    """Tracks how an execution should be allocated back to algorithms"""
    symbol: str
    order_id: int
    total_filled: int
    avg_price: float
    allocations: Dict[str, int] = field(default_factory=dict)  # algo_name -> shares
    allocation_pcts: Dict[str, float] = field(default_factory=dict)  # algo_name -> percentage (0.0-1.0)

    def get_allocation_pct(self, algorithm_name: str) -> float:
        """Get allocation percentage for a specific algorithm"""
        return self.allocation_pcts.get(algorithm_name, 0.0)

    def is_combined_order(self) -> bool:
        """Check if this order combines signals from multiple algorithms"""
        return len(self.allocations) > 1


class ReconciliationMode(Enum):
    """How to handle order reconciliation"""
    NET = "net"  # Net all orders for same symbol
    FIFO = "fifo"  # First-in-first-out (no netting)
    IMMEDIATE = "immediate"  # Execute each signal immediately (no batching)


class OrderReconciler:
    """
    Reconciles and nets orders from multiple algorithms.

    Collects pending signals, nets them by symbol, and produces
    reconciled orders for execution.

    Usage:
        reconciler = OrderReconciler()

        # Add signals from algorithms
        reconciler.add_signal("algo1", buy_signal, contract)
        reconciler.add_signal("algo2", sell_signal, contract)

        # Get netted orders
        orders = reconciler.reconcile()
        for order in orders:
            print(f"Net: {order.action} {order.net_quantity} {order.symbol}")

        # After execution, allocate fills back
        reconciler.allocate_fill(order_id, symbol, filled_qty, avg_price)
    """

    def __init__(
        self,
        mode: ReconciliationMode = ReconciliationMode.NET,
        batch_window_ms: int = 100,
    ):
        """
        Initialize order reconciler.

        Args:
            mode: Reconciliation mode
            batch_window_ms: Time window to batch signals (for future use)
        """
        self.mode = mode
        self.batch_window_ms = batch_window_ms

        self._lock = Lock()
        self._pending: Dict[str, List[PendingSignal]] = defaultdict(list)  # symbol -> signals
        self._reconciled: List[ReconciledOrder] = []
        self._execution_allocations: Dict[int, ExecutionAllocation] = {}  # order_id -> allocation

        # Statistics
        self._stats = {
            "signals_received": 0,
            "orders_netted": 0,
            "shares_saved": 0,  # Shares not traded due to netting
        }

    @property
    def stats(self) -> Dict:
        """Get reconciliation statistics"""
        return self._stats.copy()

    def add_signal(
        self,
        algorithm_name: str,
        signal: TradeSignal,
        contract: Contract,
    ) -> bool:
        """
        Add a trade signal to the reconciliation queue.

        Args:
            algorithm_name: Name of the algorithm
            signal: The trade signal
            contract: IB Contract for the symbol

        Returns:
            True if added successfully
        """
        if not signal.is_actionable:
            return False

        with self._lock:
            pending = PendingSignal(
                algorithm_name=algorithm_name,
                signal=signal,
                contract=contract,
            )
            self._pending[signal.symbol].append(pending)
            self._stats["signals_received"] += 1

            logger.debug(
                f"Added signal: {algorithm_name} {signal.action} "
                f"{signal.quantity} {signal.symbol}"
            )
            return True

    def reconcile(self, symbol: Optional[str] = None) -> List[ReconciledOrder]:
        """
        Reconcile pending signals into net orders.

        Args:
            symbol: Specific symbol to reconcile (None = all)

        Returns:
            List of ReconciledOrder objects ready for execution
        """
        with self._lock:
            if self.mode == ReconciliationMode.NET:
                return self._reconcile_net(symbol)
            elif self.mode == ReconciliationMode.FIFO:
                return self._reconcile_fifo(symbol)
            else:  # IMMEDIATE
                return self._reconcile_immediate(symbol)

    def _reconcile_net(self, symbol: Optional[str] = None) -> List[ReconciledOrder]:
        """Net all orders for the same symbol"""
        orders = []
        symbols_to_process = [symbol] if symbol else list(self._pending.keys())

        for sym in symbols_to_process:
            if sym not in self._pending or not self._pending[sym]:
                continue

            signals = self._pending[sym]

            # Calculate net position
            net_qty = 0
            contract = None
            for ps in signals:
                contract = ps.contract
                if ps.signal.action == "BUY":
                    net_qty += ps.signal.quantity
                elif ps.signal.action == "SELL":
                    net_qty -= ps.signal.quantity

            # Calculate shares saved
            total_shares = sum(ps.signal.quantity for ps in signals)
            shares_saved = total_shares - abs(net_qty)
            self._stats["shares_saved"] += shares_saved

            if net_qty != 0 and contract:
                action = "BUY" if net_qty > 0 else "SELL"
                order = ReconciledOrder(
                    symbol=sym,
                    contract=contract,
                    action=action,
                    net_quantity=abs(net_qty),
                    contributing_signals=signals.copy(),
                )
                orders.append(order)
                self._reconciled.append(order)
                self._stats["orders_netted"] += 1

                logger.info(
                    f"Reconciled {sym}: {len(signals)} signals -> "
                    f"{action} {abs(net_qty)} (saved {shares_saved} shares)"
                )

            # Clear processed signals
            self._pending[sym].clear()

        return orders

    def _reconcile_fifo(self, symbol: Optional[str] = None) -> List[ReconciledOrder]:
        """Process signals in order without netting"""
        orders = []
        symbols_to_process = [symbol] if symbol else list(self._pending.keys())

        for sym in symbols_to_process:
            if sym not in self._pending:
                continue

            for ps in self._pending[sym]:
                order = ReconciledOrder(
                    symbol=sym,
                    contract=ps.contract,
                    action=ps.signal.action,
                    net_quantity=ps.signal.quantity,
                    contributing_signals=[ps],
                )
                orders.append(order)
                self._reconciled.append(order)

            self._pending[sym].clear()

        return orders

    def _reconcile_immediate(self, symbol: Optional[str] = None) -> List[ReconciledOrder]:
        """Same as FIFO for now"""
        return self._reconcile_fifo(symbol)

    def get_pending_count(self, symbol: Optional[str] = None) -> int:
        """Get count of pending signals"""
        with self._lock:
            if symbol:
                return len(self._pending.get(symbol, []))
            return sum(len(signals) for signals in self._pending.values())

    def get_pending_symbols(self) -> List[str]:
        """Get list of symbols with pending signals"""
        with self._lock:
            return [sym for sym, signals in self._pending.items() if signals]

    def clear_pending(self, symbol: Optional[str] = None):
        """Clear pending signals without reconciling"""
        with self._lock:
            if symbol:
                self._pending[symbol].clear()
            else:
                self._pending.clear()

    def register_execution(
        self,
        order_id: int,
        reconciled_order: ReconciledOrder,
    ):
        """
        Register a reconciled order that's being executed.

        Call this when placing the order so we can track allocations.

        Args:
            order_id: The IB order ID
            reconciled_order: The reconciled order being executed
        """
        with self._lock:
            allocation = ExecutionAllocation(
                symbol=reconciled_order.symbol,
                order_id=order_id,
                total_filled=0,
                avg_price=0.0,
            )

            # Pre-calculate proportional allocations
            total_qty = sum(
                ps.signal.quantity for ps in reconciled_order.contributing_signals
            )

            for ps in reconciled_order.contributing_signals:
                # Proportional allocation based on signal quantity
                proportion = ps.signal.quantity / total_qty if total_qty > 0 else 0
                allocated = int(reconciled_order.net_quantity * proportion)
                allocation.allocations[ps.algorithm_name] = allocated
                allocation.allocation_pcts[ps.algorithm_name] = proportion

            self._execution_allocations[order_id] = allocation

    def allocate_fill(
        self,
        order_id: int,
        filled_quantity: int,
        avg_price: float,
    ) -> Dict[str, Tuple[int, float]]:
        """
        Allocate a fill back to contributing algorithms.

        Args:
            order_id: The order ID that was filled
            filled_quantity: Total quantity filled
            avg_price: Average fill price

        Returns:
            Dict mapping algorithm_name -> (shares_allocated, avg_price)
        """
        with self._lock:
            allocation = self._execution_allocations.get(order_id)
            if not allocation:
                logger.warning(f"No allocation found for order {order_id}")
                return {}

            allocation.total_filled = filled_quantity
            allocation.avg_price = avg_price

            # Calculate actual allocations based on fill
            total_allocated = sum(allocation.allocations.values())
            result = {}

            for algo_name, target_qty in allocation.allocations.items():
                if total_allocated > 0:
                    # Proportional fill
                    actual = int(filled_quantity * (target_qty / total_allocated))
                else:
                    actual = 0
                result[algo_name] = (actual, avg_price)

            logger.info(
                f"Allocated fill for order {order_id}: "
                f"{filled_quantity} shares @ ${avg_price:.2f} -> {result}"
            )

            return result

    def get_allocation(self, order_id: int) -> Optional[ExecutionAllocation]:
        """Get allocation details for an order"""
        with self._lock:
            return self._execution_allocations.get(order_id)

    def get_allocation_percentages(self, order_id: int) -> Dict[str, float]:
        """
        Get allocation percentages for an order.

        Used for commission apportionment in multi-algorithm orders.

        Args:
            order_id: The IB order ID

        Returns:
            Dict mapping algorithm_name -> allocation percentage (0.0-1.0)
            Returns empty dict if order not found.
        """
        with self._lock:
            allocation = self._execution_allocations.get(order_id)
            if allocation:
                return allocation.allocation_pcts.copy()
            return {}

    def is_combined_order(self, order_id: int) -> bool:
        """
        Check if an order combines signals from multiple algorithms.

        Args:
            order_id: The IB order ID

        Returns:
            True if order has multiple contributing algorithms
        """
        with self._lock:
            allocation = self._execution_allocations.get(order_id)
            return allocation.is_combined_order() if allocation else False

    def create_ib_order(self, reconciled: ReconciledOrder) -> Order:
        """
        Create an IB Order from a reconciled order.

        Args:
            reconciled: The reconciled order

        Returns:
            IB Order object
        """
        order = Order()
        order.action = reconciled.action
        order.totalQuantity = reconciled.net_quantity
        order.orderType = "MKT"
        order.tif = "DAY"

        # Add reference to algorithms in order reference
        algo_names = [ps.algorithm_name for ps in reconciled.contributing_signals]
        order.orderRef = f"reconciled:{','.join(algo_names)}"

        return order
