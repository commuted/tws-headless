"""
plugins/test_plugin/plugin.py — Canonical test plugin for the TWS headless test suite.

This plugin exists solely as a stable, loadable target for unit and integration
tests.  It exercises every PluginBase feature so that tests can verify:

  - PluginLoader.load_from_file()  loads it without error
  - All lifecycle callbacks (start / stop / freeze / resume / on_unload)
  - State persistence via get_state_for_save() / load_state()
  - Instrument management and INSTRUMENT_COMPLIANCE enforcement
  - Signal generation (calculate_signals)
  - Fill/order/commission/error hooks
  - MessageBus subscription
  - Live-bar subscription wrapper (subscribe_live_bars)
  - CLI help and custom request handling

IMPORTANT: this file uses only *absolute* imports so PluginLoader can load it
with importlib.util.spec_from_file_location() without a known parent package.
Do NOT change these to relative imports.
"""

import logging
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Dict, List, Optional, Any

from plugins.base import (
    PluginBase,
    TradeSignal,
    PluginInstrument,
    Holdings,
    PluginState,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Supplementary dataclass — stored in plugin state between restarts
# ---------------------------------------------------------------------------

@dataclass
class TestPluginState:
    """Persisted state for TestPlugin."""
    signal_count: int = 0
    fill_count: int = 0
    last_signal_time: Optional[str] = None
    custom_value: str = ""
    alerts_suspended: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TestPluginState":
        return cls(
            signal_count=d.get("signal_count", 0),
            fill_count=d.get("fill_count", 0),
            last_signal_time=d.get("last_signal_time"),
            custom_value=d.get("custom_value", ""),
            alerts_suspended=d.get("alerts_suspended", False),
        )


# ---------------------------------------------------------------------------
# Plugin class
# ---------------------------------------------------------------------------

class TestPlugin(PluginBase):
    """
    Canonical test plugin.

    Instruments:  SPY, TLT  (pre-registered defaults; callers may override)
    Mode:         on_bar / manual
    Signals:      always HOLD — this plugin never places real orders
    Compliance:   INSTRUMENT_COMPLIANCE = True (fills for unknown symbols fail)
    """

    # ------------------------------------------------------------------
    # Class-level metadata
    # ------------------------------------------------------------------

    VERSION = "1.0.0"
    INSTRUMENT_COMPLIANCE = True  # reject fills for symbols not in instrument list

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(self, name: str = "test_plugin", **kwargs):
        """
        ``name`` defaults to ``"test_plugin"`` so PluginLoader can instantiate
        this class without any keyword arguments.
        """
        super().__init__(name, **kwargs)

        # Runtime state (not persisted)
        self._live_bar_req_ids: Dict[str, int] = {}
        self._bar_cache: Dict[str, List[Dict]] = {}

        # Persisted state (restored in start() via load_state())
        self._pstate = TestPluginState()

        # Track what happened — useful for assertions in tests
        self.lifecycle_log: List[str] = []

    # ------------------------------------------------------------------
    # PluginBase abstract properties
    # ------------------------------------------------------------------

    @property
    def description(self) -> str:
        return "Canonical test plugin — exercises every PluginBase feature"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> bool:
        """
        Restore persisted state, subscribe to live bars and the MessageBus,
        then mark running.
        """
        saved = self.load_state()
        if saved:
            self._pstate = TestPluginState.from_dict(saved)

        # Subscribe to a MessageBus channel to demonstrate the pattern.
        # (MessageBus may be None in unit tests — guard accordingly.)
        if self._message_bus:
            self._message_bus.subscribe("risk_alert", self._on_risk_alert)

        # Subscribe to live bars for each registered instrument.
        for symbol, inst in self._instruments.items():
            if inst.enabled:
                self._subscribe_bars(symbol)

        self.lifecycle_log.append("start")
        logger.info(f"TestPlugin '{self.name}' started: {len(self._instruments)} instruments")
        return True

    def stop(self) -> bool:
        self._cancel_bar_subscriptions()
        self._save_full_state()
        self.lifecycle_log.append("stop")
        return True

    def freeze(self) -> bool:
        self._cancel_bar_subscriptions()
        self.lifecycle_log.append("freeze")
        return True

    def resume(self) -> bool:
        for symbol, inst in self._instruments.items():
            if inst.enabled:
                self._subscribe_bars(symbol)
        if self._message_bus:
            self._message_bus.subscribe("risk_alert", self._on_risk_alert)
        self.lifecycle_log.append("resume")
        return True

    def on_unload(self) -> str:
        self.lifecycle_log.append("unload")
        return (
            f"TestPlugin '{self.name}' unloaded: "
            f"{self._pstate.signal_count} signals generated, "
            f"{self._pstate.fill_count} fills received"
        )

    # ------------------------------------------------------------------
    # Signal generation
    # ------------------------------------------------------------------

    def calculate_signals(self) -> List[TradeSignal]:
        """
        Always returns HOLD signals — this plugin never trades for real.
        Increments signal_count so tests can verify the method was called.
        """
        if self._pstate.alerts_suspended:
            return []

        signals = []
        for symbol, inst in self._instruments.items():
            if not inst.enabled:
                continue
            signals.append(
                TradeSignal(
                    symbol=symbol,
                    action="HOLD",
                    reason="test_plugin always holds",
                    target_weight=inst.weight,
                )
            )

        self._pstate.signal_count += len(signals)
        self._pstate.last_signal_time = datetime.now().isoformat()
        return signals

    # ------------------------------------------------------------------
    # Fill / order / error hooks
    # ------------------------------------------------------------------

    def on_order_fill(self, req_id: int, symbol: str, filled: float,
                      avg_price: float, remaining: float) -> None:
        """Called by the executive when a fill arrives for one of our orders."""
        self._pstate.fill_count += 1
        logger.info(
            f"TestPlugin fill: req_id={req_id} symbol={symbol} "
            f"filled={filled} avg_price={avg_price:.4f}"
        )

    def on_order_status(self, req_id: int, status: str, filled: float,
                        remaining: float, avg_fill_price: float) -> None:
        """Called when an order status update arrives."""
        logger.debug(f"TestPlugin order status: req_id={req_id} status={status}")

    def on_commission(self, req_id: int, commission: float, currency: str) -> None:
        """Called when a commission report arrives for one of our orders."""
        logger.debug(f"TestPlugin commission: req_id={req_id} {commission:.4f} {currency}")

    def on_ib_error(self, req_id: int, error_code: int, error_string: str) -> None:
        """Called when IB reports an error for a request owned by this plugin."""
        symbol = next(
            (s for s, rid in self._live_bar_req_ids.items() if rid == req_id),
            "unknown",
        )
        logger.warning(
            f"TestPlugin IB error {error_code} for req_id={req_id} "
            f"(symbol={symbol}): {error_string}"
        )

    # ------------------------------------------------------------------
    # Custom request handling  (ibctl plugin request <name> ...)
    # ------------------------------------------------------------------

    def handle_request(self, request_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatch custom requests from the CLI or other plugins."""
        if request_type == "get_stats":
            return {
                "success": True,
                "signal_count": self._pstate.signal_count,
                "fill_count": self._pstate.fill_count,
                "last_signal_time": self._pstate.last_signal_time,
                "live_subscriptions": len(self._live_bar_req_ids),
                "cached_symbols": list(self._bar_cache.keys()),
                "alerts_suspended": self._pstate.alerts_suspended,
                "custom_value": self._pstate.custom_value,
            }

        if request_type == "set_custom_value":
            self._pstate.custom_value = payload.get("value", "")
            return {"success": True, "custom_value": self._pstate.custom_value}

        if request_type == "reset":
            self._pstate = TestPluginState()
            return {"success": True}

        if request_type == "suspend_alerts":
            self._pstate.alerts_suspended = True
            return {"success": True}

        if request_type == "resume_alerts":
            self._pstate.alerts_suspended = False
            return {"success": True}

        return {"success": False, "error": f"Unknown request type: {request_type!r}"}

    # ------------------------------------------------------------------
    # CLI help
    # ------------------------------------------------------------------

    def cli_help(self) -> str:
        return (
            "test_plugin — canonical test / reference plugin\n\n"
            "Requests (via: ibctl plugin request test_plugin <type> [key=value ...]):\n"
            "  get_stats           Return signal/fill counts and subscription state\n"
            "  set_custom_value    Set an arbitrary string: value=<string>\n"
            "  reset               Zero all counters and clear custom_value\n"
            "  suspend_alerts      Stop generating signals (simulate risk halt)\n"
            "  resume_alerts       Re-enable signal generation\n"
        )

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def get_state_for_save(self) -> Dict[str, Any]:
        return self._pstate.to_dict()

    def _save_full_state(self) -> None:
        self.save_state(self.get_state_for_save())

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _subscribe_bars(self, symbol: str) -> None:
        """Open a keepUpToDate daily-bar subscription and cache incoming bars."""
        if not self.portfolio:
            return
        if symbol in self._live_bar_req_ids:
            return

        try:
            from ib.contract_builder import ContractBuilder
            contract = ContractBuilder.stock(symbol)
        except Exception:
            return

        def _on_bar(bar):
            entry = {
                "date": str(bar.date),
                "open": bar.open, "high": bar.high,
                "low": bar.low, "close": bar.close,
                "volume": bar.volume,
            }
            self._bar_cache.setdefault(symbol, []).append(entry)
            # Keep a rolling 20-bar window
            self._bar_cache[symbol] = self._bar_cache[symbol][-20:]

        req_id = self.subscribe_live_bars(
            contract=contract,
            on_bar=_on_bar,
            duration_str="5 D",
            bar_size_setting="1 day",
            what_to_show="TRADES",
            use_rth=True,
        )
        if req_id is not None:
            self._live_bar_req_ids[symbol] = req_id

    def _cancel_bar_subscriptions(self) -> None:
        for symbol, req_id in list(self._live_bar_req_ids.items()):
            try:
                if self.portfolio:
                    self.portfolio.cancel_historical_data(req_id)
            except Exception:
                pass
        self._live_bar_req_ids.clear()

    def _on_risk_alert(self, message: Dict[str, Any]) -> None:
        """MessageBus handler for 'risk_alert' channel."""
        level = message.get("level", "info")
        if level == "critical":
            self._pstate.alerts_suspended = True
            logger.warning(f"TestPlugin: critical risk alert received — signals suspended")
