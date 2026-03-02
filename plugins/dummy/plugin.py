"""
plugins/dummy/plugin.py - Dummy Placeholder Plugin

A simple placeholder plugin that:
- Maintains static target allocations
- Does not react to market data
- Useful for testing and as a template for new plugins
"""

import logging
from datetime import datetime
from decimal import Decimal
from typing import List, Dict, Optional, Any
from pathlib import Path

from ..base import (
    PluginBase,
    TradeSignal,
    PluginInstrument,
    Holdings,
    PluginState,
)

logger = logging.getLogger(__name__)


class DummyPlugin(PluginBase):
    """
    Dummy Placeholder Plugin.

    This plugin:
    - Simply returns HOLD signals for all instruments
    - Does not analyze market data
    - Maintains whatever positions exist

    Use this as:
    - A placeholder when developing other plugins
    - A template for creating new plugins
    - A baseline for comparing plugin performance

    Usage:
        plugin = DummyPlugin()
        plugin.load()
        plugin.start()

        result = plugin.run()
        # All signals will be HOLD
    """

    VERSION = "1.0.0"

    def __init__(
        self,
        base_path: Optional[Path] = None,
        portfolio=None,
        shared_holdings=None,
        message_bus=None,
    ):
        super().__init__(
            "dummy",
            base_path,
            portfolio,
            shared_holdings,
            message_bus,
        )

        # Plugin-specific state
        self._run_counter = 0
        self._last_signals: List[Dict] = []

    @property
    def description(self) -> str:
        return (
            "Dummy Plugin: A placeholder that maintains static allocations "
            "and always returns HOLD signals. Use as a template for new plugins."
        )


    # =========================================================================
    # MANDATORY LIFECYCLE METHODS
    # =========================================================================

    def start(self) -> bool:
        """
        Start the plugin.

        Loads saved state if available.
        """
        logger.info(f"Starting plugin '{self.name}'")

        # Load any saved state
        saved_state = self.load_state()
        if saved_state:
            self._run_counter = saved_state.get("run_counter", 0)
            self._last_signals = saved_state.get("last_signals", [])
            logger.info(f"Restored state: run_counter={self._run_counter}")

        # Subscribe to any channels of interest
        # (Dummy plugin doesn't need any external feeds)

        return True

    def stop(self) -> bool:
        """
        Stop the plugin.

        Saves state and cleans up.
        """
        logger.info(f"Stopping plugin '{self.name}'")

        # Save state
        self.save_state({
            "run_counter": self._run_counter,
            "last_signals": self._last_signals,
        })

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
        self.save_state({
            "run_counter": self._run_counter,
            "last_signals": self._last_signals,
        })

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
        - get_stats: Get plugin statistics
        - get_last_signals: Get last generated signals
        - reset_counter: Reset run counter
        """
        if request_type == "get_stats":
            return {
                "success": True,
                "data": {
                    "run_counter": self._run_counter,
                    "instruments": len(self._instruments),
                    "enabled_instruments": len(self.enabled_instruments),
                    "state": self._state.value,
                },
            }

        elif request_type == "get_last_signals":
            return {
                "success": True,
                "data": {
                    "signals": self._last_signals,
                },
            }

        elif request_type == "reset_counter":
            self._run_counter = 0
            return {
                "success": True,
                "message": "Run counter reset to 0",
            }

        else:
            return {
                "success": False,
                "message": f"Unknown request type: {request_type}",
            }

    # =========================================================================
    # TRADING INTERFACE
    # =========================================================================

    def calculate_signals(self) -> List[TradeSignal]:
        """
        Calculate trading signals - always returns HOLD.

        This is a placeholder implementation that doesn't analyze data.

        Returns:
            List of HOLD signals for all instruments
        """
        signals = []
        self._run_counter += 1

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
                quantity=Decimal("0"),
                target_weight=inst.weight,
                current_weight=current_weight,
                reason="Dummy plugin - no action taken",
                confidence=1.0,
            ))

        # Store last signals for request handling
        self._last_signals = [
            {
                "symbol": s.symbol,
                "action": s.action,
                "target_weight": s.target_weight,
                "reason": s.reason,
            }
            for s in signals
        ]

        # Publish signals to MessageBus
        self.publish(
            f"{self.name}_signals",
            {
                "run_number": self._run_counter,
                "signals": self._last_signals,
                "timestamp": datetime.now().isoformat(),
            },
            message_type="signal",
        )

        logger.info(f"Dummy plugin generated {len(signals)} HOLD signals (run #{self._run_counter})")
        return signals

    # =========================================================================
    # STATE HELPERS
    # =========================================================================

    def get_state_for_save(self) -> Dict[str, Any]:
        """Get current state for auto-save"""
        return {
            "run_counter": self._run_counter,
            "last_signals": self._last_signals,
        }


def create_default_dummy() -> DummyPlugin:
    """
    Create a DummyPlugin with default instruments.

    Uses a simple 60/40 equity/bond allocation.
    """
    plugin = DummyPlugin()

    # Add default instruments
    default_instruments = [
        PluginInstrument("SPY", "S&P 500 ETF", weight=60.0),
        PluginInstrument("BND", "Total Bond ETF", weight=40.0),
    ]

    for inst in default_instruments:
        plugin.add_instrument(inst)

    # Create default holdings with cash
    plugin._holdings = Holdings(
        plugin_name="dummy",
        initial_cash=100000.0,
        current_cash=100000.0,
        created_at=datetime.now(),
    )

    return plugin
