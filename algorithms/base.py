"""
algorithms/base.py - Base class for trading algorithms

Provides the foundation for implementing trading algorithms with
standardized interfaces for instruments, holdings, and execution.
"""

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple

from ibapi.contract import Contract

logger = logging.getLogger(__name__)


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class HoldingPosition:
    """A position in the holdings"""
    symbol: str
    quantity: float
    cost_basis: float = 0.0
    current_price: float = 0.0
    market_value: float = 0.0

    def to_dict(self) -> Dict:
        return {
            "symbol": self.symbol,
            "quantity": self.quantity,
            "cost_basis": self.cost_basis,
            "current_price": self.current_price,
            "market_value": self.market_value,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "HoldingPosition":
        return cls(
            symbol=data["symbol"],
            quantity=data.get("quantity", 0),
            cost_basis=data.get("cost_basis", 0.0),
            current_price=data.get("current_price", 0.0),
            market_value=data.get("market_value", 0.0),
        )


@dataclass
class Holdings:
    """
    Tracks algorithm holdings including cash and positions.

    Manages initial funding, current holdings, and historical snapshots.
    """
    algorithm_name: str
    initial_cash: float = 0.0
    initial_positions: List[HoldingPosition] = field(default_factory=list)
    current_cash: float = 0.0
    current_positions: List[HoldingPosition] = field(default_factory=list)
    last_updated: Optional[datetime] = None
    created_at: Optional[datetime] = None

    @property
    def total_value(self) -> float:
        """Total portfolio value (cash + positions)"""
        position_value = sum(p.market_value for p in self.current_positions)
        return self.current_cash + position_value

    @property
    def initial_value(self) -> float:
        """Initial portfolio value"""
        position_value = sum(p.quantity * p.cost_basis for p in self.initial_positions)
        return self.initial_cash + position_value

    @property
    def total_return(self) -> float:
        """Total return as percentage"""
        if self.initial_value == 0:
            return 0.0
        return ((self.total_value - self.initial_value) / self.initial_value) * 100

    def get_position(self, symbol: str) -> Optional[HoldingPosition]:
        """Get a position by symbol"""
        for pos in self.current_positions:
            if pos.symbol == symbol:
                return pos
        return None

    def to_dict(self) -> Dict:
        return {
            "algorithm": self.algorithm_name,
            "initial_funding": {
                "cash": self.initial_cash,
                "positions": [p.to_dict() for p in self.initial_positions],
            },
            "current_holdings": {
                "cash": self.current_cash,
                "positions": [p.to_dict() for p in self.current_positions],
            },
            "last_updated": self.last_updated.isoformat() if self.last_updated else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "Holdings":
        initial = data.get("initial_funding", {})
        current = data.get("current_holdings", {})

        last_updated = None
        if data.get("last_updated"):
            last_updated = datetime.fromisoformat(data["last_updated"])

        created_at = None
        if data.get("created_at"):
            created_at = datetime.fromisoformat(data["created_at"])

        return cls(
            algorithm_name=data.get("algorithm", "unknown"),
            initial_cash=initial.get("cash", 0.0),
            initial_positions=[HoldingPosition.from_dict(p) for p in initial.get("positions", [])],
            current_cash=current.get("cash", 0.0),
            current_positions=[HoldingPosition.from_dict(p) for p in current.get("positions", [])],
            last_updated=last_updated,
            created_at=created_at,
        )


@dataclass
class AlgorithmInstrument:
    """An instrument approved for trading by an algorithm"""
    symbol: str
    name: str
    weight: float = 0.0  # Target weight in portfolio (0-100)
    min_weight: float = 0.0
    max_weight: float = 100.0
    enabled: bool = True
    exchange: str = "SMART"
    currency: str = "USD"
    sec_type: str = "STK"

    def to_contract(self) -> Contract:
        contract = Contract()
        contract.symbol = self.symbol
        contract.secType = self.sec_type
        contract.exchange = self.exchange
        contract.currency = self.currency
        return contract

    def to_dict(self) -> Dict:
        return {
            "symbol": self.symbol,
            "name": self.name,
            "weight": self.weight,
            "min_weight": self.min_weight,
            "max_weight": self.max_weight,
            "enabled": self.enabled,
            "exchange": self.exchange,
            "currency": self.currency,
            "sec_type": self.sec_type,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "AlgorithmInstrument":
        return cls(
            symbol=data["symbol"],
            name=data.get("name", data["symbol"]),
            weight=data.get("weight", 0.0),
            min_weight=data.get("min_weight", 0.0),
            max_weight=data.get("max_weight", 100.0),
            enabled=data.get("enabled", True),
            exchange=data.get("exchange", "SMART"),
            currency=data.get("currency", "USD"),
            sec_type=data.get("sec_type", "STK"),
        )


@dataclass
class TradeSignal:
    """A signal to trade from an algorithm"""
    symbol: str
    action: str  # BUY, SELL, HOLD
    quantity: int = 0
    target_weight: float = 0.0
    current_weight: float = 0.0
    reason: str = ""
    confidence: float = 1.0  # 0.0 to 1.0
    urgency: str = "Normal"  # Patient, Normal, Urgent

    @property
    def is_actionable(self) -> bool:
        return self.action in ("BUY", "SELL") and self.quantity > 0


@dataclass
class AlgorithmResult:
    """Result of algorithm execution"""
    algorithm_name: str
    timestamp: datetime
    signals: List[TradeSignal] = field(default_factory=list)
    executed_trades: List[Dict] = field(default_factory=list)
    metrics: Dict[str, float] = field(default_factory=dict)
    notes: str = ""
    success: bool = True
    error: Optional[str] = None

    @property
    def actionable_signals(self) -> List[TradeSignal]:
        return [s for s in self.signals if s.is_actionable]


# =============================================================================
# Base Algorithm Class
# =============================================================================

class AlgorithmBase(ABC):
    """
    Abstract base class for trading algorithms.

    Each algorithm manages its own:
    - Instruments file (allowed securities with target weights)
    - Holdings (either per-algorithm or shared across algorithms)
    - Trading logic (implemented in subclasses)

    Supports two holdings modes:
    - Per-algorithm holdings (legacy): Each algorithm has its own holdings.json
    - Shared holdings: All algorithms share positions via SharedHoldings

    Usage:
        class MyAlgorithm(AlgorithmBase):
            def __init__(self):
                super().__init__("my_algorithm")

            def calculate_signals(self, market_data):
                # Implement trading logic
                return [TradeSignal(...)]

        # With shared holdings:
        from algorithms.shared_holdings import SharedHoldings
        shared = SharedHoldings()
        shared.load()
        algo = MyAlgorithm(shared_holdings=shared)
    """

    def __init__(
        self,
        name: str,
        base_path: Optional[Path] = None,
        portfolio=None,
        shared_holdings=None,
    ):
        """
        Initialize the algorithm.

        Args:
            name: Unique algorithm name (used for file paths)
            base_path: Base path for algorithm files (default: algorithms/<name>/)
            portfolio: Optional Portfolio instance for live trading
            shared_holdings: Optional SharedHoldings instance for shared position tracking
        """
        self.name = name
        self.portfolio = portfolio
        self._shared_holdings = shared_holdings

        # Set up paths
        if base_path:
            self._base_path = Path(base_path)
        else:
            self._base_path = Path(__file__).parent / name

        self._instruments_file = self._base_path / "instruments.json"
        self._holdings_file = self._base_path / "holdings.json"

        # Data stores
        self._instruments: Dict[str, AlgorithmInstrument] = {}
        self._holdings: Optional[Holdings] = None  # Used when not using shared holdings
        self._market_data: Dict[str, List[Dict]] = {}  # symbol -> list of bars

        # State
        self._loaded = False
        self._last_run: Optional[datetime] = None

    # =========================================================================
    # Shared Holdings Support
    # =========================================================================

    @property
    def uses_shared_holdings(self) -> bool:
        """Whether this algorithm uses shared holdings"""
        return self._shared_holdings is not None

    @property
    def shared_holdings(self):
        """Get the shared holdings instance"""
        return self._shared_holdings

    def set_shared_holdings(self, shared_holdings) -> None:
        """Set the shared holdings instance"""
        self._shared_holdings = shared_holdings
        if shared_holdings and self.name not in shared_holdings.algorithms:
            shared_holdings.register_algorithm(self.name)

    def get_effective_holdings(self) -> Dict:
        """
        Get holdings from appropriate source (shared or per-algorithm).

        Returns:
            Dict with cash, positions, total_value
        """
        if self._shared_holdings:
            return self._shared_holdings.get_algorithm_holdings(self.name)
        elif self._holdings:
            return {
                "algorithm": self.name,
                "cash": self._holdings.current_cash,
                "positions": [
                    {
                        "symbol": p.symbol,
                        "quantity": p.quantity,
                        "current_price": p.current_price,
                        "market_value": p.market_value,
                        "cost_basis": p.cost_basis,
                    }
                    for p in self._holdings.current_positions
                ],
                "total_value": self._holdings.total_value,
            }
        else:
            return {
                "algorithm": self.name,
                "cash": 0.0,
                "positions": [],
                "total_value": 0.0,
            }

    def get_effective_cash(self) -> float:
        """Get cash from appropriate source"""
        if self._shared_holdings:
            return self._shared_holdings.get_algorithm_cash(self.name)
        elif self._holdings:
            return self._holdings.current_cash
        return 0.0

    def get_effective_position(self, symbol: str) -> Tuple[float, float]:
        """
        Get position quantity and value from appropriate source.

        Returns:
            Tuple of (quantity, market_value)
        """
        if self._shared_holdings:
            return self._shared_holdings.get_algorithm_position(self.name, symbol)
        elif self._holdings:
            pos = self._holdings.get_position(symbol)
            if pos:
                return (pos.quantity, pos.market_value)
        return (0.0, 0.0)

    def get_effective_total_value(self) -> float:
        """Get total portfolio value from appropriate source"""
        holdings = self.get_effective_holdings()
        return holdings.get("total_value", 0.0)

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def instruments(self) -> List[AlgorithmInstrument]:
        """Get list of all instruments"""
        return list(self._instruments.values())

    @property
    def enabled_instruments(self) -> List[AlgorithmInstrument]:
        """Get list of enabled instruments"""
        return [i for i in self._instruments.values() if i.enabled]

    @property
    def holdings(self) -> Optional[Holdings]:
        """Get current holdings"""
        return self._holdings

    @property
    def is_loaded(self) -> bool:
        """Whether algorithm data has been loaded"""
        return self._loaded

    # =========================================================================
    # Abstract Methods - Must be implemented by subclasses
    # =========================================================================

    @abstractmethod
    def calculate_signals(
        self,
        market_data: Dict[str, List[Dict]],
    ) -> List[TradeSignal]:
        """
        Calculate trading signals based on market data.

        Args:
            market_data: Dict mapping symbol to list of bar data
                        Each bar: {"date", "open", "high", "low", "close", "volume"}

        Returns:
            List of TradeSignal objects
        """
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable description of the algorithm"""
        pass

    @property
    def required_bars(self) -> int:
        """Number of historical bars required for calculation"""
        return 1

    # =========================================================================
    # Load/Save Methods
    # =========================================================================

    def load(self) -> bool:
        """
        Load instruments and holdings from files.

        Returns:
            True if loaded successfully
        """
        try:
            self._load_instruments()
            self._load_holdings()
            self._loaded = True
            logger.info(f"Algorithm '{self.name}' loaded: {len(self._instruments)} instruments")
            return True
        except Exception as e:
            logger.error(f"Failed to load algorithm '{self.name}': {e}")
            return False

    def _load_instruments(self):
        """Load instruments from file"""
        if not self._instruments_file.exists():
            logger.warning(f"Instruments file not found: {self._instruments_file}")
            return

        with open(self._instruments_file) as f:
            data = json.load(f)

        self._instruments.clear()
        for inst_data in data.get("instruments", []):
            inst = AlgorithmInstrument.from_dict(inst_data)
            self._instruments[inst.symbol] = inst

    def _load_holdings(self):
        """Load holdings from file"""
        if not self._holdings_file.exists():
            # Create default holdings
            self._holdings = Holdings(
                algorithm_name=self.name,
                created_at=datetime.now(),
            )
            return

        with open(self._holdings_file) as f:
            data = json.load(f)

        self._holdings = Holdings.from_dict(data)

    def save_holdings(self):
        """Save current holdings to file"""
        if self._holdings is None:
            return

        self._holdings.last_updated = datetime.now()

        # Ensure directory exists
        self._base_path.mkdir(parents=True, exist_ok=True)

        with open(self._holdings_file, "w") as f:
            json.dump(self._holdings.to_dict(), f, indent=2)

        logger.info(f"Saved holdings for '{self.name}'")

    def save_instruments(self):
        """Save instruments to file"""
        # Ensure directory exists
        self._base_path.mkdir(parents=True, exist_ok=True)

        data = {
            "algorithm": self.name,
            "description": self.description,
            "instruments": [i.to_dict() for i in self._instruments.values()],
        }

        with open(self._instruments_file, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Saved instruments for '{self.name}'")

    # =========================================================================
    # Instrument Management
    # =========================================================================

    def get_instrument(self, symbol: str) -> Optional[AlgorithmInstrument]:
        """Get an instrument by symbol"""
        return self._instruments.get(symbol.upper())

    def add_instrument(self, instrument: AlgorithmInstrument) -> bool:
        """Add an instrument to the algorithm"""
        if instrument.symbol in self._instruments:
            return False
        self._instruments[instrument.symbol] = instrument
        return True

    def remove_instrument(self, symbol: str) -> bool:
        """Remove an instrument from the algorithm"""
        if symbol.upper() not in self._instruments:
            return False
        del self._instruments[symbol.upper()]
        return True

    def get_contracts(self) -> List[Contract]:
        """Get IB contracts for all enabled instruments"""
        return [i.to_contract() for i in self.enabled_instruments]

    # =========================================================================
    # Market Data Management
    # =========================================================================

    def set_market_data(self, symbol: str, bars: List[Dict]):
        """
        Set market data for a symbol.

        Args:
            symbol: Trading symbol
            bars: List of bar data, each with date, open, high, low, close, volume
        """
        self._market_data[symbol.upper()] = bars

    def get_market_data(self, symbol: str) -> List[Dict]:
        """Get market data for a symbol"""
        return self._market_data.get(symbol.upper(), [])

    def clear_market_data(self):
        """Clear all market data"""
        self._market_data.clear()

    # =========================================================================
    # Execution
    # =========================================================================

    def run(self, market_data: Optional[Dict[str, List[Dict]]] = None) -> AlgorithmResult:
        """
        Run the algorithm and generate signals.

        Args:
            market_data: Optional market data (uses stored data if not provided)

        Returns:
            AlgorithmResult with signals and metrics
        """
        if not self._loaded:
            return AlgorithmResult(
                algorithm_name=self.name,
                timestamp=datetime.now(),
                success=False,
                error="Algorithm not loaded",
            )

        # Use provided data or stored data
        data = market_data or self._market_data

        # Validate we have enough data
        for symbol in [i.symbol for i in self.enabled_instruments]:
            bars = data.get(symbol, [])
            if len(bars) < self.required_bars:
                logger.warning(
                    f"Insufficient data for {symbol}: {len(bars)} bars, "
                    f"need {self.required_bars}"
                )

        try:
            # Calculate signals
            signals = self.calculate_signals(data)

            self._last_run = datetime.now()

            return AlgorithmResult(
                algorithm_name=self.name,
                timestamp=self._last_run,
                signals=signals,
                success=True,
            )

        except Exception as e:
            logger.error(f"Algorithm '{self.name}' failed: {e}")
            return AlgorithmResult(
                algorithm_name=self.name,
                timestamp=datetime.now(),
                success=False,
                error=str(e),
            )

    def execute(
        self,
        signals: Optional[List[TradeSignal]] = None,
        dry_run: bool = True,
    ) -> AlgorithmResult:
        """
        Execute trading signals.

        Args:
            signals: Signals to execute (runs algorithm if not provided)
            dry_run: If True, don't actually place trades

        Returns:
            AlgorithmResult with execution details
        """
        if signals is None:
            result = self.run()
            if not result.success:
                return result
            signals = result.signals

        actionable = [s for s in signals if s.is_actionable]

        if not actionable:
            return AlgorithmResult(
                algorithm_name=self.name,
                timestamp=datetime.now(),
                signals=signals,
                notes="No actionable signals",
                success=True,
            )

        executed = []

        for signal in actionable:
            if dry_run:
                executed.append({
                    "symbol": signal.symbol,
                    "action": signal.action,
                    "quantity": signal.quantity,
                    "dry_run": True,
                })
                logger.info(
                    f"[DRY RUN] {signal.action} {signal.quantity} {signal.symbol} "
                    f"(reason: {signal.reason})"
                )
            else:
                # Live execution would go here
                if self.portfolio and self.portfolio.connected:
                    # Place actual orders
                    pass
                executed.append({
                    "symbol": signal.symbol,
                    "action": signal.action,
                    "quantity": signal.quantity,
                    "dry_run": False,
                })

        return AlgorithmResult(
            algorithm_name=self.name,
            timestamp=datetime.now(),
            signals=signals,
            executed_trades=executed,
            success=True,
        )

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def calculate_target_quantities(
        self,
        total_value: float,
        prices: Dict[str, float],
    ) -> Dict[str, int]:
        """
        Calculate target quantities for each instrument based on weights.

        Args:
            total_value: Total portfolio value to allocate
            prices: Current prices for each symbol

        Returns:
            Dict mapping symbol to target quantity
        """
        targets = {}

        for inst in self.enabled_instruments:
            if inst.weight <= 0:
                continue

            price = prices.get(inst.symbol, 0)
            if price <= 0:
                continue

            target_value = total_value * (inst.weight / 100.0)
            target_qty = int(target_value / price)
            targets[inst.symbol] = target_qty

        return targets

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(name='{self.name}', "
            f"instruments={len(self._instruments)}, loaded={self._loaded})"
        )
