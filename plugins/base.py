"""
plugins/base.py - Base class for trading plugins

Provides the foundation for implementing trading plugins with:
- Standardized lifecycle commands (start, stop, freeze, resume)
- Custom request handling
- Pub/Sub MessageBus integration for indicator feeds
- File-based state persistence — each plugin owns its directory
- Instrument management and market data subscriptions
- Holdings tracking and order execution
"""

import json
import logging
import os
import threading
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, List, Dict, Optional, Any, Set, Tuple, Callable

from ibapi.contract import Contract

if TYPE_CHECKING:
    from ib.models import PnLData

logger = logging.getLogger(__name__)


# =============================================================================
# Plugin State Enum
# =============================================================================

class PluginState(Enum):
    """Plugin lifecycle states"""
    UNLOADED = "unloaded"
    LOADED = "loaded"
    STARTED = "started"
    FROZEN = "frozen"
    STOPPED = "stopped"
    ERROR = "error"


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
    Tracks plugin holdings including cash and positions.

    Manages initial funding, current holdings, and historical snapshots.
    """
    plugin_name: str
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

    def add_cash(self, amount: float) -> None:
        """Add cash to holdings (can be negative to subtract)"""
        self.current_cash += amount
        self.last_updated = datetime.now()

    def add_position(
        self,
        symbol: str,
        quantity: float,
        cost_basis: float = 0.0,
        current_price: float = 0.0,
    ) -> None:
        """
        Add to a position (or create new one).

        If position exists, adds quantity and averages cost basis.
        """
        existing = self.get_position(symbol)
        if existing:
            # Average the cost basis
            total_qty = existing.quantity + quantity
            if total_qty > 0:
                existing.cost_basis = (
                    (existing.quantity * existing.cost_basis + quantity * cost_basis)
                    / total_qty
                )
            existing.quantity = total_qty
            existing.current_price = current_price or existing.current_price
        else:
            self.current_positions.append(HoldingPosition(
                symbol=symbol,
                quantity=quantity,
                cost_basis=cost_basis,
                current_price=current_price,
            ))
        self.last_updated = datetime.now()

    def remove_position(self, symbol: str, quantity: float) -> bool:
        """
        Remove quantity from a position.

        Returns True if successful, False if insufficient quantity.
        """
        pos = self.get_position(symbol)
        if not pos:
            return False
        if pos.quantity < quantity:
            return False

        pos.quantity -= quantity

        # Remove position entirely if zero
        if pos.quantity <= 0:
            self.current_positions = [p for p in self.current_positions if p.symbol != symbol]

        self.last_updated = datetime.now()
        return True

    def to_dict(self) -> Dict:
        return {
            "plugin": self.plugin_name,
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

        # Support both "plugin" and "algorithm" keys for backward compatibility with old state files
        plugin_name = data.get("plugin", data.get("algorithm", "unknown"))

        return cls(
            plugin_name=plugin_name,
            initial_cash=initial.get("cash", 0.0),
            initial_positions=[HoldingPosition.from_dict(p) for p in initial.get("positions", [])],
            current_cash=current.get("cash", 0.0),
            current_positions=[HoldingPosition.from_dict(p) for p in current.get("positions", [])],
            last_updated=last_updated,
            created_at=created_at,
        )


@dataclass
class PluginInstrument:
    """An instrument approved for trading by a plugin"""
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
    def from_dict(cls, data: Dict) -> "PluginInstrument":
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
    """A signal to trade from a plugin"""
    symbol: str
    action: str  # BUY, SELL, HOLD
    quantity: Decimal = Decimal("0")
    target_weight: float = 0.0
    current_weight: float = 0.0
    reason: str = ""
    confidence: float = 1.0  # 0.0 to 1.0
    urgency: str = "Normal"  # Patient, Normal, Urgent

    @property
    def is_actionable(self) -> bool:
        return self.action in ("BUY", "SELL") and self.quantity > 0


@dataclass
class PluginResult:
    """Result of plugin execution"""
    plugin_name: str
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


# Default path to the shared BarStore database
_DEFAULT_HIST_DB = Path(__file__).parent.parent / "historical" / "bars.db"

# =============================================================================
# Base Plugin Class
# =============================================================================

class PluginBase(ABC):
    """
    Abstract base class for trading plugins.

    Provides:
    - Standardized lifecycle (start, stop, freeze, resume)
    - Custom request handling (handle_request)
    - Pub/Sub MessageBus integration
    - File-based state persistence (save_state/load_state → {plugin_dir}/state.json)
    - Instruments, holdings, and trading execution

    Each plugin manages its own directory (self.plugin_dir):
    - instruments.json  — approved securities with target weights
    - holdings.json     — persistent position tracking
    - state.json        — written/read by save_state / load_state
    - Trading logic (implemented in subclasses)

    Lifecycle States:
        UNLOADED -> LOADED -> STARTED -> FROZEN -> STARTED
                              STARTED -> STOPPED -> UNLOADED
                              Any state -> ERROR

    Usage:
        class MyPlugin(PluginBase):
            def __init__(self):
                super().__init__("my_plugin")

            def start(self) -> bool:
                state = self.load_state()
                self.subscribe("momentum_signals", self._on_signal)
                return True

            def stop(self) -> bool:
                self.save_state({"my_data": self._data})
                self.unsubscribe_all()
                return True

            def freeze(self) -> bool:
                self.save_state({"my_data": self._data})
                return True

            def resume(self) -> bool:
                return True

            def handle_request(self, request_type, payload):
                if request_type == "get_metrics":
                    return {"success": True, "metrics": self._metrics}
                return {"success": False, "message": "Unknown request"}

            def calculate_signals(self, market_data):
                signals = [...]
                self.publish("my_plugin_signals", {"symbol": "SPY", "value": 0.8})
                return signals
    """

    # Plugin version (override in subclasses)
    VERSION = "1.0.0"

    # System plugin marker - if True, plugin is managed by the system
    # and cannot be unloaded/deleted by user commands
    IS_SYSTEM_PLUGIN = False

    # When True, the executive enforces that signals only reference symbols
    # present in this plugin's instrument set.  Set to True in your subclass
    # to opt in to instrument-set compliance checking.
    INSTRUMENT_COMPLIANCE = False

    def __init__(
        self,
        name: str,
        base_path: Optional[Path] = None,
        portfolio=None,
        shared_holdings=None,
        message_bus=None,
    ):
        """
        Initialize the plugin.

        Args:
            name: Unique plugin name (used for file paths and MessageBus)
            base_path: Base path for plugin files (default: plugins/<name>/)
            portfolio: Optional Portfolio instance for live trading
            shared_holdings: Optional SharedHoldings instance for shared position tracking
            message_bus: Optional MessageBus instance for pub/sub communication
        """
        self.name = name
        # slot: stable instance storage key, defaults to name.
        # Overridden at load time when path=slot syntax is used, allowing
        # multiple instances of the same plugin class with independent state.
        self.slot = name
        self.instance_id = str(uuid.uuid4())
        self.descriptor = None  # Opaque data set at load time
        self.portfolio = portfolio
        self._shared_holdings = shared_holdings
        self._message_bus = message_bus

        # Set up paths
        if base_path:
            self._base_path = Path(base_path)
        else:
            plugin_dir = Path(os.environ.get("IB_PLUGIN_DIR", Path(__file__).parent))
            self._base_path = plugin_dir / name

        self._instruments_file = self._base_path / "instruments.json"
        self._holdings_file = self._base_path / "holdings.json"
        self._state_file = self._base_path / "state.json"
        self._account_id: str = ""

        # Data stores
        self._instruments: Dict[str, PluginInstrument] = {}
        self._holdings: Optional[Holdings] = None

        # Plugin state
        self._state = PluginState.UNLOADED
        self._loaded = False
        self._last_run: Optional[datetime] = None

        # MessageBus subscriptions tracking
        self._subscriptions: List[str] = []

        # PluginExecutive reference (set on registration)
        self._executive = None

    def set_account(self, account_id: str) -> None:
        """Re-root _base_path to include account subfolder. Call before load()."""
        self._account_id = account_id
        plugin_dir = Path(os.environ.get("IB_PLUGIN_DIR", Path(__file__).parent))
        self._base_path        = plugin_dir / self.slot / account_id
        self._instruments_file = self._base_path / "instruments.json"
        self._holdings_file    = self._base_path / "holdings.json"
        self._state_file       = self._base_path / "state.json"

    @property
    def _bar_store(self):
        if not hasattr(self, "_bar_store_instance"):
            try:
                from ib.bar_store import BarStore
                db = Path(os.environ.get("IB_HIST_DB", str(_DEFAULT_HIST_DB)))
                self._bar_store_instance = BarStore(db)
            except Exception:
                self._bar_store_instance = None
        return self._bar_store_instance

    # =========================================================================
    # Lifecycle State Property
    # =========================================================================

    @property
    def state(self) -> PluginState:
        """Current plugin state"""
        return self._state

    @state.setter
    def state(self, value: PluginState):
        """Set plugin state with logging"""
        old_state = self._state
        self._state = value
        logger.info(f"Plugin '{self.name}' state: {old_state.value} -> {value.value}")

    # =========================================================================
    # MANDATORY LIFECYCLE INTERFACE - Must be implemented
    # =========================================================================

    @abstractmethod
    def start(self) -> bool:
        """
        Start the plugin - initialize and begin processing.

        Called when transitioning from LOADED to STARTED state.
        Should:
        - Load any saved state via load_state()
        - Set up MessageBus subscriptions
        - Initialize any required resources

        Returns:
            True if started successfully
        """
        pass

    @abstractmethod
    def stop(self) -> bool:
        """
        Stop the plugin - cleanup and shutdown.

        Called when transitioning from STARTED/FROZEN to STOPPED state.
        Should:
        - Save state via save_state()
        - Unsubscribe from all MessageBus channels
        - Release any resources

        Returns:
            True if stopped successfully
        """
        pass

    @abstractmethod
    def freeze(self) -> bool:
        """
        Freeze the plugin - pause processing, maintain state.

        Called when transitioning from STARTED to FROZEN state.
        Should:
        - Save current state via save_state()
        - Pause any ongoing processing
        - Keep resources allocated for quick resume

        Returns:
            True if frozen successfully
        """
        pass

    @abstractmethod
    def resume(self) -> bool:
        """
        Resume the plugin - continue from frozen state.

        Called when transitioning from FROZEN to STARTED state.
        Should:
        - Resume processing where it left off
        - State should already be in memory

        Returns:
            True if resumed successfully
        """
        pass

    @abstractmethod
    def handle_request(self, request_type: str, payload: Dict) -> Dict:
        """
        Handle a custom request.

        Allows external systems to send custom commands to the plugin.

        Args:
            request_type: Type of request (e.g., "get_metrics", "set_config")
            payload: Request payload data

        Returns:
            Dict with at least "success" key (True/False) and optionally
            "message", "data", or other response fields

        Example:
            def handle_request(self, request_type, payload):
                if request_type == "get_metrics":
                    return {"success": True, "data": self._metrics}
                elif request_type == "reset_state":
                    self._reset()
                    return {"success": True, "message": "State reset"}
                return {"success": False, "message": f"Unknown request: {request_type}"}
        """
        pass

    def on_unload(self) -> str:
        """
        Called when the plugin is about to be removed from the executive.

        Override to provide a meaningful departure message — final metrics,
        summary of results, goodbye, etc.

        Returns:
            Human-readable departure status string
        """
        return f"Plugin '{self.name}' unloaded"

    def cli_help(self) -> str:
        """
        Return CLI help text for this plugin's custom handle_request commands.

        Override in subclasses to document the request types your plugin
        accepts via ``ibctl plugin request <name> <type> [json]``.

        Returns:
            Multi-line help string describing available request types.

        Example::

            def cli_help(self) -> str:
                return (
                    "my_plugin custom commands:\\n"
                    "  plugin request my_plugin get_metrics {}\\n"
                    "  plugin request my_plugin set_threshold {\"value\": 0.5}\\n"
                    "  plugin request my_plugin reset_state {}\\n"
                )
        """
        return f"Plugin '{self.name}' defines no custom CLI commands."

    # =========================================================================
    # TRADING INTERFACE - Must be implemented
    # =========================================================================

    @abstractmethod
    def calculate_signals(self) -> List[TradeSignal]:
        """
        Calculate trading signals.

        Returns:
            List of TradeSignal objects
        """
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable description of the plugin"""
        pass

    # =========================================================================
    # State Persistence
    # =========================================================================

    def save_state(self, state: Dict[str, Any]) -> bool:
        """
        Save plugin state to {plugin_dir}/state.json.

        Automatically called on freeze() and stop().
        Can also be called manually for periodic saves.

        Args:
            state: State data to save (must be JSON-serializable)

        Returns:
            True if saved successfully
        """
        try:
            self._base_path.mkdir(parents=True, exist_ok=True)
            data = {
                "plugin_version": self.VERSION,
                "state": state,
                "saved_at": datetime.now().isoformat(),
            }
            self._state_file.write_text(json.dumps(data, default=str))
            return True
        except Exception as e:
            logger.error(f"Failed to save state for '{self.slot}': {e}")
            return False

    def load_state(self) -> Dict[str, Any]:
        """
        Load plugin state from {plugin_dir}/state.json.

        Should be called during start() to restore previous state.

        Returns:
            State dict, or empty dict if no file exists
        """
        try:
            if not self._state_file.exists():
                return {}
            data = json.loads(self._state_file.read_text())
            if "state" in data:
                return data["state"]
            return data
        except Exception as e:
            logger.error(f"Failed to load state for '{self.slot}': {e}")
            return {}

    def clear_state(self) -> bool:
        """
        Clear saved state by deleting state.json.

        Returns:
            True if cleared (or didn't exist)
        """
        if self._state_file.exists():
            self._state_file.unlink()
        return True

    # =========================================================================
    # MessageBus Integration
    # =========================================================================

    def set_message_bus(self, message_bus) -> None:
        """Set the MessageBus instance for pub/sub communication"""
        self._message_bus = message_bus

    def set_executive(self, executive) -> None:
        """Set the PluginExecutive reference for stream management"""
        self._executive = executive

    # =========================================================================
    # Order Fill / Status Callbacks
    # =========================================================================

    def register_order(self, order_id: int) -> None:
        """
        Register a directly-placed order for fill/status callbacks.

        Call this after placing an order via portfolio.place_order_custom() so
        that on_order_fill() and on_order_status() are invoked when IB reports
        status changes for that order.
        """
        if self._executive:
            self._executive.register_order_for_plugin(order_id, self.name)

    def on_order_fill(self, order_record) -> None:
        """
        Called when an order attributed to this plugin is fully filled.

        Override to react to fills (update holdings, wake waiting threads, etc.).

        Args:
            order_record: ib.models.OrderRecord with fill details
        """

    def on_order_status(self, order_record) -> None:
        """
        Called on every status change for an order attributed to this plugin.

        Override to detect rejections, partial fills, INACTIVE orders, etc.

        Args:
            order_record: ib.models.OrderRecord with current status
        """

    def on_commission(
        self,
        exec_id: str,
        commission: float,
        realized_pnl: float,
        currency: str,
    ) -> None:
        """
        Called when a commission report arrives for an order placed by this plugin.

        Override to track trading costs and realized P&L on a per-execution basis.

        Args:
            exec_id:      IB execution ID linking this report to an execDetails callback
            commission:   Commission charged for this execution
            realized_pnl: Realized P&L for closing trades (UNSET_DOUBLE if opening)
            currency:     Currency of the commission (e.g., "USD")
        """

    def on_pnl(self, pnl_data: "PnLData") -> None:
        """
        Called with live P&L updates from IB's reqPnL / reqPnLSingle subscriptions.

        Override to react to real-time P&L changes.

        Args:
            pnl_data: ib.models.PnLData; pnl_data.symbol is None for account-level
                      updates and set to the position symbol for single-position updates.
        """

    def on_ib_error(self, req_id: int, error_code: int, error_string: str) -> None:
        """
        Called when IB reports an error for a request attributed to this plugin.

        Covers order errors (req_id == order_id) and market data errors
        (req_id == subscription request ID).  Informational codes (2104,
        2106, 2119, 2158, 10167) and system messages (req_id == -1) are
        filtered out before this method is called.

        Args:
            req_id:       IB request/order ID that errored
            error_code:   IB error code
            error_string: Human-readable IB error description
        """

    # =========================================================================
    # Stream Management (via PluginExecutive's StreamManager)
    # =========================================================================

    def request_stream(
        self,
        symbol: str,
        contract: Contract,
        data_types: Optional[Set] = None,
        on_tick: Optional[Callable] = None,
        on_bar: Optional[Callable] = None,
        on_tick_by_tick: Optional[Callable] = None,
        on_depth: Optional[Callable] = None,
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> bool:
        """
        Request a data stream through the executive's stream manager.

        Args:
            symbol: Symbol to stream
            contract: IB Contract
            data_types: Set of DataType values (defaults to TICK + BAR_5SEC)
            on_tick: Callback(tick: TickData) for tick data (price or size tick)
            on_bar: Callback(bar) for bar data
            on_tick_by_tick: Callback(tbt: TickByTickData) for tick-by-tick events
            on_depth: Callback(depth: MarketDepth) for market depth updates
            what_to_show: TRADES, MIDPOINT, BID, ASK
            use_rth: Regular trading hours only

        Returns:
            True if stream requested successfully
        """
        if not self._executive:
            logger.warning(f"Plugin '{self.name}' has no executive - cannot request stream")
            return False
        return self._executive.stream_manager.request_stream(
            self.name, symbol, contract, data_types,
            on_tick, on_bar, on_tick_by_tick, on_depth,
            what_to_show, use_rth,
        )

    def cancel_stream(self, symbol: str) -> bool:
        """Cancel a stream previously requested by this plugin."""
        if not self._executive:
            return False
        return self._executive.stream_manager.cancel_stream(self.name, symbol)

    def get_historical_data(
        self,
        contract: Contract,
        end_date_time: str = "",
        duration_str: str = "1 W",
        bar_size_setting: str = "1 day",
        what_to_show: str = "TRADES",
        use_rth: bool = True,
        timeout: float = 60.0,
    ) -> Optional[List]:
        """
        Fetch historical bar data and block until complete or timeout.

        Data is private to this plugin — each call allocates its own
        request ID so concurrent calls from different plugins never mix.

        Args:
            contract:         IB Contract (use ContractBuilder helpers)
            end_date_time:    End of period as "YYYYMMDD-HH:MM:SS" in UTC
                              (e.g. "20260318-21:00:00" for 4 PM ET),
                              or "" for now
            duration_str:     How far back: "1 D", "1 W", "1 M", "1 Y", etc.
            bar_size_setting: Bar width: "1 day", "1 hour", "5 mins", etc.
            what_to_show:     TRADES, MIDPOINT, BID, ASK, ADJUSTED_LAST, etc.
            use_rth:          Regular trading hours only
            timeout:          Seconds to wait before giving up (default 60)

        Returns:
            List of ibapi BarData objects (attributes: date, open, high,
            low, close, volume, wap, barCount), or None on timeout/error.

        Example:
            bars = self.get_historical_data(
                contract=ContractBuilder.us_stock("AAPL"),
                duration_str="1 W",
                bar_size_setting="1 day",
            )
            if bars:
                for b in bars:
                    print(b.date, b.close)
        """
        if not self.portfolio:
            logger.warning(f"Plugin '{self.name}': no portfolio for historical data")
            return None

        done = threading.Event()
        result: Dict[str, Any] = {}

        def on_end(bars: list, start: str, end: str) -> None:
            result["bars"] = bars
            done.set()

        req_id = self.portfolio.request_historical_data(
            contract=contract,
            end_date_time=end_date_time,
            duration_str=duration_str,
            bar_size_setting=bar_size_setting,
            what_to_show=what_to_show,
            use_rth=use_rth,
            on_end=on_end,
        )

        if not done.wait(timeout=timeout):
            logger.warning(
                f"Plugin '{self.name}': historical data timeout "
                f"after {timeout}s (req_id={req_id})"
            )
            self.portfolio.cancel_historical_data(req_id)
            return None

        return result.get("bars", [])

    def subscribe_live_bars(
        self,
        contract,
        on_bar: Callable,
        duration_str: str = "1 D",
        bar_size_setting: str = "5 mins",
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> Optional[int]:
        """
        Subscribe to live bar updates using reqHistoricalData(keepUpToDate=True).

        Unlike reqRealTimeBars this works without a live market data subscription
        — it uses the historical data feed which is available on paper accounts.

        IB delivers bars via:
          historicalData     — initial backfill bars (already-completed bars)
          historicalDataUpdate — each new bar as it completes going forward

        Both call on_bar(bar) with ibapi BarData objects (date, open, high,
        low, close, volume, wap, barCount).

        Args:
            contract:         IB Contract (use ContractBuilder helpers)
            on_bar:           Callback(bar) called for each bar (backfill + live)
            duration_str:     How far back to seed: "1 D", "2 D", etc.
            bar_size_setting: Bar width: "5 mins", "1 hour", "1 day", etc.
            what_to_show:     TRADES, MIDPOINT, BID, ASK
            use_rth:          Regular trading hours only

        Returns:
            req_id to pass to cancel_live_bars(), or None on error.
        """
        if not self.portfolio:
            logger.warning(f"Plugin '{self.name}': no portfolio for live bars")
            return None

        symbol = contract.symbol
        store  = self._bar_store

        def _caching_on_bar(bar):
            on_bar(bar)          # user callback first — never block live trading
            if store:
                try:
                    store.insert_bar(symbol, bar_size_setting, what_to_show, use_rth, bar)
                except Exception:
                    pass

        req_id = self.portfolio.request_historical_data(
            contract=contract,
            duration_str=duration_str,
            bar_size_setting=bar_size_setting,
            what_to_show=what_to_show,
            use_rth=use_rth,
            on_bar=_caching_on_bar,
            on_end=None,          # no on_end — keeps subscription alive
            keep_up_to_date=True,
        )
        logger.info(
            f"Plugin '{self.name}': live bar subscription req_id={req_id} "
            f"({bar_size_setting}, keepUpToDate=True)"
        )
        return req_id

    def cancel_live_bars(self, req_id: int) -> None:
        """Cancel a live bar subscription started with subscribe_live_bars."""
        if self.portfolio and req_id is not None:
            self.portfolio.cancel_historical_data(req_id)
            logger.info(f"Plugin '{self.name}': cancelled live bars req_id={req_id}")

    def get_bars_cached(
        self,
        contract,
        start_dt: datetime,
        end_dt: datetime,
        bar_size_setting: str = "5 mins",
        what_to_show: str = "TRADES",
        use_rth: bool = True,
        force: bool = False,
    ) -> list:
        """Like get_historical_data but backed by BarStore (no re-fetch if cached)."""
        from ib.bar_store import duration_str as _dur_str
        store = self._bar_store
        if store is None:
            return self.get_historical_data(
                contract=contract,
                end_date_time=end_dt.strftime("%Y%m%d-%H:%M:%S"),
                duration_str=_dur_str(start_dt, end_dt),
                bar_size_setting=bar_size_setting,
                what_to_show=what_to_show,
                use_rth=use_rth,
            ) or []

        def fetch_fn(s, e):
            return self.get_historical_data(
                contract=contract,
                end_date_time=e.strftime("%Y%m%d-%H:%M:%S"),
                duration_str=_dur_str(s, e),
                bar_size_setting=bar_size_setting,
                what_to_show=what_to_show,
                use_rth=use_rth,
            ) or []

        return store.get_bars(
            symbol=contract.symbol,
            bar_size=bar_size_setting,
            what_to_show=what_to_show,
            use_rth=use_rth,
            start_dt=start_dt,
            end_dt=end_dt,
            fetch_fn=fetch_fn,
            force=force,
        )

    def get_contract_details(
        self,
        contract,
        timeout: float = 15.0,
    ) -> Optional[List]:
        """
        Fetch contract details and block until complete or timeout.

        Each call allocates its own request ID so concurrent calls from
        different plugins never interfere.

        Args:
            contract: IB Contract to look up
            timeout:  Seconds to wait (default 15)

        Returns:
            List of ContractDetails objects, or None on timeout/error.
            Useful fields include:
              cd.tradingHours  — e.g. "20260303:0930-20260303:1600;..."
              cd.liquidHours   — same format, regular-hours only
              cd.timeZoneId    — e.g. "America/New_York"
        """
        if not self.portfolio:
            logger.warning(f"Plugin '{self.name}': no portfolio for contract details")
            return None

        done = threading.Event()
        result: Dict[str, Any] = {}

        def on_end(details: list) -> None:
            result["details"] = details
            done.set()

        self.portfolio.request_contract_details(
            contract=contract,
            on_end=on_end,
        )

        if not done.wait(timeout=timeout):
            logger.warning(
                f"Plugin '{self.name}': contract details timeout after {timeout}s"
            )
            return None

        return result.get("details", [])

    def request_unload(self) -> bool:
        """
        Request that the executive unload this plugin.

        The unload happens asynchronously on a separate thread so it is
        safe to call from within handle_request() or any plugin callback.

        Returns:
            True if the request was accepted (executive is available)
        """
        if not self._executive:
            logger.warning(f"Plugin '{self.name}' has no executive - cannot request unload")
            return False
        self._executive.deferred_unload_plugin(self.instance_id)
        return True

    def publish(
        self,
        channel: str,
        payload: Any,
        message_type: str = "data",
    ) -> bool:
        """
        Publish a message to a MessageBus channel.

        Args:
            channel: Channel name (e.g., "my_plugin_signals")
            payload: Message payload (any JSON-serializable data)
            message_type: Type of message (data, signal, alert, metric, state)

        Returns:
            True if published (False if no MessageBus configured)
        """
        if self._message_bus is None:
            logger.warning(f"Plugin '{self.name}' has no MessageBus - cannot publish")
            return False

        return self._message_bus.publish(
            channel=channel,
            payload=payload,
            publisher=self.name,
            message_type=message_type,
        )

    def subscribe(
        self,
        channel: str,
        callback: Callable,
    ) -> bool:
        """
        Subscribe to a MessageBus channel.

        Args:
            channel: Channel name to subscribe to
            callback: Function called for each message (receives Message object)

        Returns:
            True if subscribed (False if no MessageBus configured)
        """
        if self._message_bus is None:
            logger.warning(f"Plugin '{self.name}' has no MessageBus - cannot subscribe")
            return False

        result = self._message_bus.subscribe(
            channel=channel,
            callback=callback,
            subscriber=self.name,
        )

        if result and channel not in self._subscriptions:
            self._subscriptions.append(channel)

        return result

    def unsubscribe(self, channel: str) -> bool:
        """
        Unsubscribe from a MessageBus channel.

        Args:
            channel: Channel name to unsubscribe from

        Returns:
            True if unsubscribed (False if no MessageBus or wasn't subscribed)
        """
        if self._message_bus is None:
            return False

        result = self._message_bus.unsubscribe(
            channel=channel,
            subscriber=self.name,
        )

        if channel in self._subscriptions:
            self._subscriptions.remove(channel)

        return result

    def unsubscribe_all(self) -> int:
        """
        Unsubscribe from all channels.

        Returns:
            Number of channels unsubscribed from
        """
        if self._message_bus is None:
            return 0

        count = self._message_bus.unsubscribe_all(self.name)
        self._subscriptions.clear()
        return count

    @property
    def subscribed_channels(self) -> List[str]:
        """Get list of currently subscribed channels"""
        return list(self._subscriptions)

    # =========================================================================
    # Shared Holdings Support
    # =========================================================================

    @property
    def uses_shared_holdings(self) -> bool:
        """Whether this plugin uses shared holdings"""
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
        Get holdings from appropriate source (shared or per-plugin).

        Returns:
            Dict with cash, positions, total_value
        """
        if self._shared_holdings:
            return self._shared_holdings.get_algorithm_holdings(self.name)
        elif self._holdings:
            return {
                "plugin": self.name,
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
                "plugin": self.name,
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
    def plugin_dir(self) -> Path:
        """Directory where this plugin stores its files (state.json, holdings.json, etc.)"""
        return self._base_path

    @property
    def instruments(self) -> List[PluginInstrument]:
        """Get list of all instruments"""
        return list(self._instruments.values())

    @property
    def enabled_instruments(self) -> List[PluginInstrument]:
        """Get list of enabled instruments"""
        return [i for i in self._instruments.values() if i.enabled]

    @property
    def holdings(self) -> Optional[Holdings]:
        """Get current holdings"""
        return self._holdings

    @property
    def is_loaded(self) -> bool:
        """Whether plugin data has been loaded"""
        return self._loaded

    @property
    def is_system_plugin(self) -> bool:
        """Whether this is a system-managed plugin (cannot be unloaded by user)"""
        return self.IS_SYSTEM_PLUGIN

    @property
    def config(self) -> Optional[Dict[str, Any]]:
        """Per-instance config (alias of self.descriptor when it is a dict)."""
        return self.descriptor if isinstance(self.descriptor, dict) else None

    # =========================================================================
    # Runtime Parameters Interface
    # =========================================================================

    def get_parameters(self) -> Dict[str, Any]:
        """
        Get configurable parameters and their current values.

        Override in subclasses to expose plugin-specific parameters
        that can be modified at runtime.

        Returns:
            Dict mapping parameter names to current values
        """
        return {}

    def set_parameter(self, key: str, value: Any) -> bool:
        """
        Set a parameter value at runtime.

        Override in subclasses to handle parameter updates.

        Args:
            key: Parameter name
            value: New parameter value

        Returns:
            True if parameter was set successfully
        """
        return False

    def get_parameter_schema(self) -> Dict[str, Dict[str, Any]]:
        """
        Get schema for configurable parameters.

        Override in subclasses to provide validation metadata.

        Returns:
            Dict mapping parameter names to schema dicts
        """
        return {}

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
            self.state = PluginState.LOADED
            logger.info(f"Plugin '{self.name}' loaded: {len(self._instruments)} instruments")
            return True
        except Exception as e:
            logger.error(f"Failed to load plugin '{self.name}': {e}")
            self.state = PluginState.ERROR
            return False

    def _load_instruments(self):
        """Load instruments from instruments.json."""
        instruments_file = self._base_path / "instruments.json"
        if not instruments_file.exists():
            return
        try:
            data = json.loads(instruments_file.read_text())
            self._instruments.clear()
            for d in data.get("instruments", []):
                inst = PluginInstrument.from_dict(d)
                self._instruments[inst.symbol] = inst
        except Exception as e:
            logger.error(f"Failed to load instruments for '{self.slot}': {e}")

    def _load_holdings(self):
        """Load holdings from holdings.json, falling back to fresh Holdings if absent."""
        holdings_file = self._base_path / "holdings.json"
        if holdings_file.exists():
            try:
                data = json.loads(holdings_file.read_text())
                self._holdings = Holdings.from_dict(data)
                return
            except Exception as e:
                logger.error(f"Failed to load holdings for '{self.slot}': {e}")
        self._holdings = Holdings(
            plugin_name=self.slot,
            created_at=datetime.now(),
        )

    def save_holdings(self):
        """Save current holdings to holdings.json."""
        if self._holdings is None:
            return

        self._base_path.mkdir(parents=True, exist_ok=True)
        self._holdings.last_updated = datetime.now()
        holdings_file = self._base_path / "holdings.json"
        holdings_file.write_text(json.dumps(self._holdings.to_dict(), default=str))
        logger.info(f"Saved holdings for '{self.slot}'")

    def save_instruments(self):
        """Save current instrument list to instruments.json."""
        self._base_path.mkdir(parents=True, exist_ok=True)
        instruments_file = self._base_path / "instruments.json"
        data = {"instruments": [i.to_dict() for i in self._instruments.values()]}
        instruments_file.write_text(json.dumps(data, default=str))
        logger.info(f"Saved {len(self._instruments)} instruments for '{self.slot}'")

    def reload_instruments(self):
        """Re-read instrument list from instruments.json into memory."""
        self._load_instruments()
        logger.info(f"Reloaded {len(self._instruments)} instruments for '{self.slot}'")

    # =========================================================================
    # Instrument Management
    # =========================================================================

    def get_instrument(self, symbol: str) -> Optional[PluginInstrument]:
        """Get an instrument by symbol"""
        return self._instruments.get(symbol.upper())

    def add_instrument(self, instrument: PluginInstrument) -> bool:
        """Add an instrument and persist to instruments.json. Returns False if already present."""
        if instrument.symbol in self._instruments:
            return False
        self._instruments[instrument.symbol] = instrument
        self.save_instruments()
        return True

    def remove_instrument(self, symbol: str) -> bool:
        """Remove an instrument and persist to instruments.json."""
        sym = symbol.upper()
        if sym not in self._instruments:
            return False
        del self._instruments[sym]
        self.save_instruments()
        return True

    def get_contracts(self) -> List[Contract]:
        """Get IB contracts for all enabled instruments"""
        return [i.to_contract() for i in self.enabled_instruments]

    # =========================================================================
    # Execution
    # =========================================================================

    def run(self) -> PluginResult:
        """
        Run the plugin and generate signals.

        Returns:
            PluginResult with signals and metrics
        """
        if not self._loaded:
            return PluginResult(
                plugin_name=self.name,
                timestamp=datetime.now(),
                success=False,
                error="Plugin not loaded",
            )

        if self._state not in (PluginState.LOADED, PluginState.STARTED):
            return PluginResult(
                plugin_name=self.name,
                timestamp=datetime.now(),
                success=False,
                error=f"Plugin in {self._state.value} state, cannot run",
            )

        try:
            signals = self.calculate_signals()

            self._last_run = datetime.now()

            return PluginResult(
                plugin_name=self.name,
                timestamp=self._last_run,
                signals=signals,
                success=True,
            )

        except Exception as e:
            logger.error(f"Plugin '{self.name}' failed: {e}")
            return PluginResult(
                plugin_name=self.name,
                timestamp=datetime.now(),
                success=False,
                error=str(e),
            )

    def execute(
        self,
        signals: Optional[List[TradeSignal]] = None,
        dry_run: bool = True,
    ) -> PluginResult:
        """
        Execute trading signals.

        Args:
            signals: Signals to execute (runs plugin if not provided)
            dry_run: If True, don't actually place trades

        Returns:
            PluginResult with execution details
        """
        if signals is None:
            result = self.run()
            if not result.success:
                return result
            signals = result.signals

        actionable = [s for s in signals if s.is_actionable]

        if not actionable:
            return PluginResult(
                plugin_name=self.name,
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

        return PluginResult(
            plugin_name=self.name,
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

    def get_status(self) -> Dict[str, Any]:
        """
        Get plugin status information.

        Returns:
            Dict with plugin status details
        """
        return {
            "name": self.name,
            "slot": self.slot,
            "instance_id": self.instance_id,
            "version": self.VERSION,
            "state": self._state.value,
            "loaded": self._loaded,
            "descriptor": self.descriptor,
            "instrument_compliance": self.INSTRUMENT_COMPLIANCE,
            "instruments": len(self._instruments),
            "enabled_instruments": len(self.enabled_instruments),
            "subscribed_channels": self._subscriptions,
            "last_run": self._last_run.isoformat() if self._last_run else None,
        }

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(name='{self.name}', "
            f"instance_id='{self.instance_id[:8]}', "
            f"state={self._state.value}, instruments={len(self._instruments)})"
        )


# =============================================================================
# Backward Compatibility Aliases
# =============================================================================

# Allow existing code using AlgorithmInstrument to work
AlgorithmInstrument = PluginInstrument
AlgorithmResult = PluginResult
