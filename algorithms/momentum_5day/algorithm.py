"""
algorithms/momentum_5day/algorithm.py - 5-Day Momentum Reallocation Algorithm

A basic reallocation algorithm that:
- Uses 5 days of daily bar data
- Calculates momentum based on returns
- Allocates more weight to assets with positive momentum
- Reduces weight for assets with negative momentum
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path

from algorithms.base import (
    AlgorithmBase,
    TradeSignal,
    AlgorithmInstrument,
    Holdings,
    HoldingPosition,
)

logger = logging.getLogger(__name__)


@dataclass
class MomentumMetrics:
    """Momentum metrics for a single instrument"""
    symbol: str
    returns_5d: float = 0.0  # 5-day return
    returns_1d: float = 0.0  # 1-day return
    avg_return: float = 0.0  # Average daily return
    volatility: float = 0.0  # Standard deviation of returns
    momentum_score: float = 0.0  # Combined momentum score
    trend: str = "neutral"  # up, down, neutral


class Momentum5DayAlgorithm(AlgorithmBase):
    """
    5-Day Momentum Reallocation Algorithm.

    Strategy:
    - Calculate 5-day returns and momentum for each instrument
    - Rank instruments by momentum score
    - Allocate higher weights to positive momentum assets
    - Reduce or eliminate positions in negative momentum assets

    Parameters:
    - lookback_days: Number of days for momentum calculation (default: 5)
    - rebalance_threshold: Minimum deviation to trigger rebalance (default: 5%)
    - momentum_weight: How much momentum affects allocation (0-1, default: 0.5)
    - min_position_size: Minimum position value in dollars (default: 1000)

    Usage:
        algo = Momentum5DayAlgorithm()
        algo.load()

        # Set market data (5 daily bars per symbol)
        algo.set_market_data("SPY", bars)

        # Run algorithm
        result = algo.run()
        for signal in result.signals:
            print(f"{signal.action} {signal.quantity} {signal.symbol}")
    """

    def __init__(
        self,
        base_path: Optional[Path] = None,
        portfolio=None,
        shared_holdings=None,
        lookback_days: int = 5,
        rebalance_threshold: float = 5.0,
        momentum_weight: float = 0.5,
        min_position_size: float = 1000.0,
    ):
        super().__init__("momentum_5day", base_path, portfolio, shared_holdings)

        # Algorithm parameters
        self.lookback_days = lookback_days
        self.rebalance_threshold = rebalance_threshold
        self.momentum_weight = momentum_weight
        self.min_position_size = min_position_size

        # State
        self._momentum_metrics: Dict[str, MomentumMetrics] = {}

    @property
    def description(self) -> str:
        return (
            "5-Day Momentum Reallocation: Allocates based on recent price momentum, "
            "overweighting assets with positive 5-day returns and underweighting "
            "those with negative returns."
        )

    @property
    def required_bars(self) -> int:
        return self.lookback_days

    @property
    def momentum_metrics(self) -> Dict[str, MomentumMetrics]:
        """Get calculated momentum metrics"""
        return self._momentum_metrics

    def calculate_signals(
        self,
        market_data: Dict[str, List[Dict]],
    ) -> List[TradeSignal]:
        """
        Calculate trading signals based on 5-day momentum.

        Args:
            market_data: Dict mapping symbol to list of daily bars
                        Each bar: {"date", "open", "high", "low", "close", "volume"}

        Returns:
            List of TradeSignal objects
        """
        signals = []
        self._momentum_metrics.clear()

        # Get enabled instruments
        instruments = self.enabled_instruments
        if not instruments:
            logger.warning("No enabled instruments")
            return signals

        # Calculate momentum for each instrument
        for inst in instruments:
            bars = market_data.get(inst.symbol, [])
            if len(bars) < self.lookback_days:
                logger.warning(
                    f"Insufficient data for {inst.symbol}: "
                    f"{len(bars)}/{self.lookback_days} bars"
                )
                continue

            metrics = self._calculate_momentum(inst.symbol, bars)
            self._momentum_metrics[inst.symbol] = metrics

        if not self._momentum_metrics:
            return signals

        # Calculate target weights based on momentum
        target_weights = self._calculate_target_weights()

        # Get current prices and holdings
        current_prices = self._get_current_prices(market_data)
        current_positions = self._get_current_positions()

        # Calculate total portfolio value
        if self._holdings:
            total_value = self._holdings.total_value
            if total_value <= 0:
                total_value = self._holdings.current_cash
        else:
            total_value = 100000.0  # Default

        # Generate signals
        for symbol, target_weight in target_weights.items():
            current_weight = current_positions.get(symbol, {}).get("weight", 0.0)
            current_qty = current_positions.get(symbol, {}).get("quantity", 0)
            price = current_prices.get(symbol, 0)

            if price <= 0:
                continue

            # Calculate target quantity
            target_value = total_value * (target_weight / 100.0)
            target_qty = int(target_value / price)

            # Check if rebalance needed
            weight_diff = abs(target_weight - current_weight)
            if weight_diff < self.rebalance_threshold:
                signals.append(TradeSignal(
                    symbol=symbol,
                    action="HOLD",
                    quantity=0,
                    target_weight=target_weight,
                    current_weight=current_weight,
                    reason=f"Within threshold ({weight_diff:.1f}% < {self.rebalance_threshold}%)",
                ))
                continue

            # Determine action
            qty_diff = target_qty - current_qty
            metrics = self._momentum_metrics.get(symbol)

            if qty_diff > 0:
                action = "BUY"
                reason = f"Increase position (momentum: {metrics.momentum_score:.2f})"
            elif qty_diff < 0:
                action = "SELL"
                qty_diff = abs(qty_diff)
                reason = f"Reduce position (momentum: {metrics.momentum_score:.2f})"
            else:
                action = "HOLD"
                reason = "No change needed"

            # Skip tiny positions
            if abs(qty_diff) * price < self.min_position_size:
                action = "HOLD"
                reason = f"Below minimum size (${abs(qty_diff) * price:.0f})"
                qty_diff = 0

            signals.append(TradeSignal(
                symbol=symbol,
                action=action,
                quantity=abs(qty_diff),
                target_weight=target_weight,
                current_weight=current_weight,
                reason=reason,
                confidence=min(1.0, abs(metrics.momentum_score) if metrics else 0.5),
            ))

        return signals

    def _calculate_momentum(self, symbol: str, bars: List[Dict]) -> MomentumMetrics:
        """
        Calculate momentum metrics for an instrument.

        Args:
            symbol: Trading symbol
            bars: List of daily bars (most recent last)

        Returns:
            MomentumMetrics for the symbol
        """
        # Ensure we have enough bars
        if len(bars) < 2:
            return MomentumMetrics(symbol=symbol)

        # Get closes (assume bars are ordered oldest to newest)
        closes = [bar.get("close", bar.get("Close", 0)) for bar in bars]
        closes = [c for c in closes if c > 0]

        if len(closes) < 2:
            return MomentumMetrics(symbol=symbol)

        # Calculate returns
        returns = []
        for i in range(1, len(closes)):
            ret = (closes[i] - closes[i-1]) / closes[i-1]
            returns.append(ret)

        # 5-day return (or available period)
        first_close = closes[0]
        last_close = closes[-1]
        returns_5d = (last_close - first_close) / first_close if first_close > 0 else 0

        # 1-day return
        returns_1d = returns[-1] if returns else 0

        # Average daily return
        avg_return = sum(returns) / len(returns) if returns else 0

        # Volatility (standard deviation of returns)
        if len(returns) > 1:
            mean = avg_return
            variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
            volatility = variance ** 0.5
        else:
            volatility = 0

        # Momentum score: risk-adjusted return
        if volatility > 0:
            momentum_score = avg_return / volatility  # Sharpe-like ratio
        else:
            momentum_score = avg_return * 10  # Scale if no volatility

        # Determine trend
        if returns_5d > 0.02:
            trend = "up"
        elif returns_5d < -0.02:
            trend = "down"
        else:
            trend = "neutral"

        return MomentumMetrics(
            symbol=symbol,
            returns_5d=returns_5d * 100,  # As percentage
            returns_1d=returns_1d * 100,
            avg_return=avg_return * 100,
            volatility=volatility * 100,
            momentum_score=momentum_score,
            trend=trend,
        )

    def _calculate_target_weights(self) -> Dict[str, float]:
        """
        Calculate target weights based on momentum scores.

        Combines base weights from instruments with momentum adjustments.
        """
        if not self._momentum_metrics:
            return {}

        # Get base weights
        base_weights = {
            inst.symbol: inst.weight
            for inst in self.enabled_instruments
        }

        # Calculate momentum adjustments
        scores = {s: m.momentum_score for s, m in self._momentum_metrics.items()}

        # Normalize scores to -1 to 1 range
        if scores:
            max_abs = max(abs(v) for v in scores.values()) or 1
            normalized = {s: v / max_abs for s, v in scores.items()}
        else:
            normalized = {}

        # Apply momentum adjustments
        target_weights = {}
        total_adjustment = 0

        for symbol, base_weight in base_weights.items():
            if symbol in normalized:
                # Adjust weight by momentum
                adjustment = base_weight * self.momentum_weight * normalized[symbol]
                new_weight = base_weight + adjustment

                # Get instrument constraints
                inst = self.get_instrument(symbol)
                if inst:
                    new_weight = max(inst.min_weight, min(inst.max_weight, new_weight))

                target_weights[symbol] = max(0, new_weight)
                total_adjustment += new_weight - base_weight
            else:
                target_weights[symbol] = base_weight

        # Normalize to 100% if needed
        total = sum(target_weights.values())
        if total > 0 and abs(total - 100) > 0.1:
            factor = 100 / total
            target_weights = {s: w * factor for s, w in target_weights.items()}

        return target_weights

    def _get_current_prices(self, market_data: Dict[str, List[Dict]]) -> Dict[str, float]:
        """Get current prices from market data"""
        prices = {}
        for symbol, bars in market_data.items():
            if bars:
                last_bar = bars[-1]
                prices[symbol] = last_bar.get("close", last_bar.get("Close", 0))
        return prices

    def _get_current_positions(self) -> Dict[str, Dict]:
        """Get current positions from holdings"""
        positions = {}

        if not self._holdings:
            return positions

        total_value = self._holdings.total_value or 1

        for pos in self._holdings.current_positions:
            weight = (pos.market_value / total_value * 100) if total_value > 0 else 0
            positions[pos.symbol] = {
                "quantity": pos.quantity,
                "value": pos.market_value,
                "weight": weight,
            }

        return positions

    def get_momentum_summary(self) -> str:
        """Get a formatted summary of momentum metrics"""
        if not self._momentum_metrics:
            return "No momentum data calculated"

        lines = [
            "Momentum Summary (5-Day):",
            "-" * 60,
            f"{'Symbol':<8} {'5D Ret':>8} {'1D Ret':>8} {'Vol':>8} {'Score':>8} {'Trend':>8}",
            "-" * 60,
        ]

        # Sort by momentum score
        sorted_metrics = sorted(
            self._momentum_metrics.values(),
            key=lambda m: m.momentum_score,
            reverse=True,
        )

        for m in sorted_metrics:
            lines.append(
                f"{m.symbol:<8} {m.returns_5d:>7.2f}% {m.returns_1d:>7.2f}% "
                f"{m.volatility:>7.2f}% {m.momentum_score:>8.2f} {m.trend:>8}"
            )

        return "\n".join(lines)


def create_default_momentum_5day() -> Momentum5DayAlgorithm:
    """
    Create a Momentum5DayAlgorithm with default instruments.

    Uses a balanced portfolio of equity, bonds, and alternatives.
    """
    algo = Momentum5DayAlgorithm()

    # Add default instruments with target weights
    default_instruments = [
        AlgorithmInstrument("SPY", "S&P 500 ETF", weight=30.0, min_weight=10.0, max_weight=50.0),
        AlgorithmInstrument("QQQ", "Nasdaq 100 ETF", weight=20.0, min_weight=5.0, max_weight=35.0),
        AlgorithmInstrument("IWM", "Russell 2000 ETF", weight=10.0, min_weight=0.0, max_weight=20.0),
        AlgorithmInstrument("TLT", "20+ Year Treasury ETF", weight=20.0, min_weight=10.0, max_weight=40.0),
        AlgorithmInstrument("GLD", "Gold ETF", weight=10.0, min_weight=0.0, max_weight=20.0),
        AlgorithmInstrument("VNQ", "Real Estate ETF", weight=10.0, min_weight=0.0, max_weight=20.0),
    ]

    for inst in default_instruments:
        algo.add_instrument(inst)

    # Create default holdings
    algo._holdings = Holdings(
        algorithm_name="momentum_5day",
        initial_cash=100000.0,
        current_cash=100000.0,
        created_at=datetime.now(),
    )

    return algo
