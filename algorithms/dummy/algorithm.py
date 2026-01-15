"""
algorithms/dummy/algorithm.py - Dummy Placeholder Algorithm

A simple placeholder algorithm that:
- Maintains static target allocations
- Does not react to market data
- Useful for testing and as a template for new algorithms
"""

import logging
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path

from algorithms.base import (
    AlgorithmBase,
    TradeSignal,
    AlgorithmInstrument,
    Holdings,
)

logger = logging.getLogger(__name__)


class DummyAlgorithm(AlgorithmBase):
    """
    Dummy Placeholder Algorithm.

    This algorithm:
    - Simply returns HOLD signals for all instruments
    - Does not analyze market data
    - Maintains whatever positions exist

    Use this as:
    - A placeholder when developing other algorithms
    - A template for creating new algorithms
    - A baseline for comparing algorithm performance

    Usage:
        algo = DummyAlgorithm()
        algo.load()

        result = algo.run()
        # All signals will be HOLD
    """

    def __init__(
        self,
        base_path: Optional[Path] = None,
        portfolio=None,
        shared_holdings=None,
    ):
        super().__init__("dummy", base_path, portfolio, shared_holdings)

    @property
    def description(self) -> str:
        return (
            "Dummy Algorithm: A placeholder that maintains static allocations "
            "and always returns HOLD signals. Use as a template for new algorithms."
        )

    @property
    def required_bars(self) -> int:
        return 1  # Minimal requirement

    def calculate_signals(
        self,
        market_data: Dict[str, List[Dict]],
    ) -> List[TradeSignal]:
        """
        Calculate trading signals - always returns HOLD.

        This is a placeholder implementation that doesn't analyze data.

        Args:
            market_data: Dict mapping symbol to list of daily bars (ignored)

        Returns:
            List of HOLD signals for all instruments
        """
        signals = []

        for inst in self.enabled_instruments:
            # Get current position info if available
            current_weight = 0.0
            if self._holdings:
                pos = self._holdings.get_position(inst.symbol)
                if pos:
                    total = self._holdings.total_value or 1
                    current_weight = (pos.market_value / total * 100)

            signals.append(TradeSignal(
                symbol=inst.symbol,
                action="HOLD",
                quantity=0,
                target_weight=inst.weight,
                current_weight=current_weight,
                reason="Dummy algorithm - no action taken",
                confidence=1.0,
            ))

        logger.info(f"Dummy algorithm generated {len(signals)} HOLD signals")
        return signals


def create_default_dummy() -> DummyAlgorithm:
    """
    Create a DummyAlgorithm with default instruments.

    Uses a simple 60/40 equity/bond allocation.
    """
    algo = DummyAlgorithm()

    # Add default instruments
    default_instruments = [
        AlgorithmInstrument("SPY", "S&P 500 ETF", weight=60.0),
        AlgorithmInstrument("BND", "Total Bond ETF", weight=40.0),
    ]

    for inst in default_instruments:
        algo.add_instrument(inst)

    # Create default holdings with cash
    algo._holdings = Holdings(
        algorithm_name="dummy",
        initial_cash=100000.0,
        current_cash=100000.0,
        created_at=datetime.now(),
    )

    return algo
