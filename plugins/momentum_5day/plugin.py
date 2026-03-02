"""
plugins/momentum_5day/plugin.py - 5-Day Momentum Reallocation Plugin

A reallocation plugin that:
- Uses 5 days of daily bar data
- Calculates momentum based on returns
- Allocates more weight to assets with positive momentum
- Reduces weight for assets with negative momentum
"""

import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from decimal import Decimal
from typing import List, Dict, Optional, Any
from pathlib import Path

from ib.contract_builder import ContractBuilder
from ..base import (
    PluginBase,
    TradeSignal,
    PluginInstrument,
    Holdings,
    HoldingPosition,
    PluginState,
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

    def to_dict(self) -> Dict:
        return asdict(self)


class Momentum5DayPlugin(PluginBase):
    """
    5-Day Momentum Reallocation Plugin.

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
        plugin = Momentum5DayPlugin()
        plugin.load()
        plugin.start()

        # Market data will be fed by PluginExecutive
        result = plugin.run()
        for signal in result.signals:
            print(f"{signal.action} {signal.quantity} {signal.symbol}")
    """

    VERSION = "1.0.0"

    def __init__(
        self,
        base_path: Optional[Path] = None,
        portfolio=None,
        shared_holdings=None,
        message_bus=None,
        lookback_days: int = 5,
        rebalance_threshold: float = 5.0,
        momentum_weight: float = 0.5,
        min_position_size: float = 1000.0,
    ):
        super().__init__(
            "momentum_5day",
            base_path,
            portfolio,
            shared_holdings,
            message_bus,
        )

        # Algorithm parameters
        self.lookback_days = lookback_days
        self.rebalance_threshold = rebalance_threshold
        self.momentum_weight = momentum_weight
        self.min_position_size = min_position_size

        # State
        self._momentum_metrics: Dict[str, MomentumMetrics] = {}
        self._run_counter = 0
        self._last_target_weights: Dict[str, float] = {}
        self._signals_history: List[Dict] = []
        self._max_signals_history = 100

    @property
    def description(self) -> str:
        return (
            "5-Day Momentum Reallocation: Allocates based on recent price momentum, "
            "overweighting assets with positive 5-day returns and underweighting "
            "those with negative returns."
        )

    @property
    def momentum_metrics(self) -> Dict[str, MomentumMetrics]:
        """Get calculated momentum metrics"""
        return self._momentum_metrics

    # =========================================================================
    # MANDATORY LIFECYCLE METHODS
    # =========================================================================

    def start(self) -> bool:
        """
        Start the plugin.

        Loads saved state and subscribes to relevant channels.
        """
        logger.info(f"Starting plugin '{self.name}'")

        # Load any saved state
        saved_state = self.load_state()
        if saved_state:
            self._run_counter = saved_state.get("run_counter", 0)
            self._last_target_weights = saved_state.get("last_target_weights", {})
            self._signals_history = saved_state.get("signals_history", [])

            # Restore momentum metrics
            metrics_data = saved_state.get("momentum_metrics", {})
            for symbol, data in metrics_data.items():
                self._momentum_metrics[symbol] = MomentumMetrics(**data)

            logger.info(
                f"Restored state: run_counter={self._run_counter}, "
                f"metrics for {len(self._momentum_metrics)} symbols"
            )

        return True

    def stop(self) -> bool:
        """
        Stop the plugin.

        Saves state and cleans up.
        """
        logger.info(f"Stopping plugin '{self.name}'")

        # Save state
        self._save_full_state()

        # Unsubscribe from all channels
        self.unsubscribe_all()

        return True

    def freeze(self) -> bool:
        """
        Freeze the plugin.

        Saves state for later resume.
        """
        logger.info(f"Freezing plugin '{self.name}'")

        # Save state
        self._save_full_state()

        return True

    def resume(self) -> bool:
        """
        Resume the plugin from frozen state.

        State should already be in memory from before freeze.
        """
        logger.info(f"Resuming plugin '{self.name}'")
        return True

    def handle_request(self, request_type: str, payload: Dict) -> Dict:
        """
        Handle custom requests.

        Supported requests:
        - get_metrics: Get current momentum metrics
        - get_stats: Get plugin statistics
        - get_parameters: Get tunable parameters
        - set_parameter: Set a parameter value
        - get_signals_history: Get recent signals history
        - get_momentum_summary: Get formatted momentum summary
        """
        if request_type == "get_metrics":
            return {
                "success": True,
                "data": {
                    symbol: m.to_dict()
                    for symbol, m in self._momentum_metrics.items()
                },
            }

        elif request_type == "get_stats":
            return {
                "success": True,
                "data": {
                    "run_counter": self._run_counter,
                    "instruments": len(self._instruments),
                    "enabled_instruments": len(self.enabled_instruments),
                    "state": self._state.value,
                    "lookback_days": self.lookback_days,
                    "rebalance_threshold": self.rebalance_threshold,
                    "momentum_weight": self.momentum_weight,
                },
            }

        elif request_type == "get_parameters":
            return {
                "success": True,
                "data": self.get_parameters(),
            }

        elif request_type == "set_parameter":
            key = payload.get("key")
            value = payload.get("value")
            if not key:
                return {"success": False, "message": "Missing 'key' in payload"}
            if self.set_parameter(key, value):
                return {"success": True, "message": f"Parameter '{key}' set to {value}"}
            return {"success": False, "message": f"Unknown parameter: {key}"}

        elif request_type == "get_signals_history":
            count = payload.get("count", 10)
            return {
                "success": True,
                "data": {
                    "history": self._signals_history[-count:],
                },
            }

        elif request_type == "get_momentum_summary":
            return {
                "success": True,
                "data": {
                    "summary": self.get_momentum_summary(),
                },
            }

        else:
            return {
                "success": False,
                "message": f"Unknown request type: {request_type}",
            }

    # =========================================================================
    # PARAMETER INTERFACE
    # =========================================================================

    def get_parameters(self) -> Dict[str, Any]:
        """Get configurable parameters"""
        return {
            "lookback_days": self.lookback_days,
            "rebalance_threshold": self.rebalance_threshold,
            "momentum_weight": self.momentum_weight,
            "min_position_size": self.min_position_size,
        }

    def set_parameter(self, key: str, value: Any) -> bool:
        """Set a parameter value at runtime"""
        if key == "lookback_days":
            self.lookback_days = int(value)
            return True
        elif key == "rebalance_threshold":
            self.rebalance_threshold = float(value)
            return True
        elif key == "momentum_weight":
            self.momentum_weight = max(0.0, min(1.0, float(value)))
            return True
        elif key == "min_position_size":
            self.min_position_size = float(value)
            return True
        return False

    def get_parameter_schema(self) -> Dict[str, Dict[str, Any]]:
        """Get schema for configurable parameters"""
        return {
            "lookback_days": {
                "type": "int",
                "description": "Number of days for momentum calculation",
                "min": 1,
                "max": 100,
                "default": 5,
            },
            "rebalance_threshold": {
                "type": "float",
                "description": "Minimum weight deviation to trigger rebalance (%)",
                "min": 0.0,
                "max": 50.0,
                "default": 5.0,
            },
            "momentum_weight": {
                "type": "float",
                "description": "How much momentum affects allocation (0-1)",
                "min": 0.0,
                "max": 1.0,
                "default": 0.5,
            },
            "min_position_size": {
                "type": "float",
                "description": "Minimum position value in dollars",
                "min": 0.0,
                "max": 100000.0,
                "default": 1000.0,
            },
        }

    # =========================================================================
    # TRADING INTERFACE
    # =========================================================================

    def calculate_signals(self) -> List[TradeSignal]:
        """
        Calculate trading signals based on 5-day momentum.

        Returns:
            List of TradeSignal objects
        """
        signals = []
        self._momentum_metrics.clear()
        self._run_counter += 1

        # Get enabled instruments
        instruments = self.enabled_instruments
        if not instruments:
            logger.warning("No enabled instruments")
            return signals

        # Fetch daily bars for each instrument
        market_data = {}
        for inst in instruments:
            bars = self._fetch_daily_bars(inst)
            if bars:
                market_data[inst.symbol] = bars

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
        self._last_target_weights = target_weights

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
                    quantity=Decimal("0"),
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
                quantity=Decimal(abs(qty_diff)),
                target_weight=target_weight,
                current_weight=current_weight,
                reason=reason,
                confidence=min(1.0, abs(metrics.momentum_score) if metrics else 0.5),
            ))

        # Store signals in history
        signals_record = {
            "run_number": self._run_counter,
            "timestamp": datetime.now().isoformat(),
            "signals": [
                {
                    "symbol": s.symbol,
                    "action": s.action,
                    "quantity": s.quantity,
                    "target_weight": s.target_weight,
                    "reason": s.reason,
                }
                for s in signals
            ],
        }
        self._signals_history.append(signals_record)
        if len(self._signals_history) > self._max_signals_history:
            self._signals_history = self._signals_history[-self._max_signals_history:]

        # Publish signals and metrics to MessageBus
        self.publish(
            f"{self.name}_signals",
            signals_record,
            message_type="signal",
        )

        self.publish(
            f"{self.name}_metrics",
            {
                "run_number": self._run_counter,
                "timestamp": datetime.now().isoformat(),
                "metrics": {
                    symbol: m.to_dict()
                    for symbol, m in self._momentum_metrics.items()
                },
            },
            message_type="metric",
        )

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
            else:
                target_weights[symbol] = base_weight

        # Normalize to 100% if needed
        total = sum(target_weights.values())
        if total > 0 and abs(total - 100) > 0.1:
            factor = 100 / total
            target_weights = {s: w * factor for s, w in target_weights.items()}

        return target_weights

    def _fetch_daily_bars(self, inst: PluginInstrument) -> List[Dict]:
        """Fetch recent daily bars for an instrument using historical data API."""
        if not self.portfolio:
            return []
        raw = self.get_historical_data(
            contract=inst.to_contract(),
            duration_str=f"{self.lookback_days + 5} D",
            bar_size_setting="1 day",
            what_to_show="TRADES",
            use_rth=True,
        )
        if not raw:
            return []
        return [
            {
                "date": b.date,
                "open": float(b.open),
                "high": float(b.high),
                "low": float(b.low),
                "close": float(b.close),
                "volume": float(b.volume),
            }
            for b in raw
        ]

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

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

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

    def _save_full_state(self):
        """Save full plugin state"""
        self.save_state({
            "run_counter": self._run_counter,
            "last_target_weights": self._last_target_weights,
            "signals_history": self._signals_history,
            "momentum_metrics": {
                symbol: m.to_dict()
                for symbol, m in self._momentum_metrics.items()
            },
            "parameters": self.get_parameters(),
        })

    def get_state_for_save(self) -> Dict[str, Any]:
        """Get current state for auto-save"""
        return {
            "run_counter": self._run_counter,
            "last_target_weights": self._last_target_weights,
            "signals_history": self._signals_history[-10:],  # Keep last 10 for auto-save
            "momentum_metrics": {
                symbol: m.to_dict()
                for symbol, m in self._momentum_metrics.items()
            },
        }


def create_default_momentum_5day() -> Momentum5DayPlugin:
    """
    Create a Momentum5DayPlugin with default instruments.

    Uses a balanced portfolio of equity, bonds, and alternatives.
    """
    plugin = Momentum5DayPlugin()

    # Add default instruments with target weights
    default_instruments = [
        PluginInstrument("SPY", "S&P 500 ETF", weight=30.0, min_weight=10.0, max_weight=50.0),
        PluginInstrument("QQQ", "Nasdaq 100 ETF", weight=20.0, min_weight=5.0, max_weight=35.0),
        PluginInstrument("IWM", "Russell 2000 ETF", weight=10.0, min_weight=0.0, max_weight=20.0),
        PluginInstrument("TLT", "20+ Year Treasury ETF", weight=20.0, min_weight=10.0, max_weight=40.0),
        PluginInstrument("GLD", "Gold ETF", weight=10.0, min_weight=0.0, max_weight=20.0),
        PluginInstrument("VNQ", "Real Estate ETF", weight=10.0, min_weight=0.0, max_weight=20.0),
    ]

    for inst in default_instruments:
        plugin.add_instrument(inst)

    # Create default holdings
    plugin._holdings = Holdings(
        plugin_name="momentum_5day",
        initial_cash=100000.0,
        current_cash=100000.0,
        created_at=datetime.now(),
    )

    return plugin
