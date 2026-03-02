"""
plugins/panic/plugin.py - Panic Plugin for emergency position liquidation

Accepts positions deposited into its control and closes them all.
Each position carries an integer urgency coefficient (1-3) that determines
the order in which positions are closed:
    3 = highest urgency, closed first
    2 = medium urgency
    1 = lowest urgency, closed last

Positions are closed via SELL signals with urgency mapped to TradeSignal urgency:
    3 -> "Urgent"
    2 -> "Normal"
    1 -> "Patient"

Usage:
    plugin = PanicPlugin()
    plugin.load()
    plugin.start()

    # Deposit positions to close
    plugin.handle_request("deposit", {
        "positions": [
            {"symbol": "SPY", "quantity": 100, "urgency": 3},
            {"symbol": "AAPL", "quantity": 50, "urgency": 1},
        ]
    })

    # Run to generate SELL signals (ordered by urgency descending)
    result = plugin.run()
"""

import logging
from decimal import Decimal
from datetime import datetime
from typing import List, Dict, Optional, Any
from pathlib import Path

from ..base import (
    PluginBase,
    TradeSignal,
    Holdings,
)

logger = logging.getLogger(__name__)

URGENCY_MAP = {
    3: "Urgent",
    2: "Normal",
    1: "Patient",
}


class PanicPlugin(PluginBase):
    """
    Panic Plugin - Emergency position liquidation.

    Accepts multiple positions via handle_request("deposit", ...) and generates
    SELL signals to close them all. Each position has an urgency coefficient (1-3)
    that controls execution priority and order type aggressiveness.
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
            "panic",
            base_path,
            portfolio,
            shared_holdings,
            message_bus,
        )

        # Positions queued for liquidation: list of {symbol, quantity, urgency}
        self._queued_positions: List[Dict[str, Any]] = []
        # History of closed positions
        self._closed_history: List[Dict[str, Any]] = []

    @property
    def description(self) -> str:
        return (
            "Panic Plugin: Emergency position liquidation. Accepts positions "
            "with urgency coefficients (1-3) and generates SELL signals to "
            "close them, highest urgency first."
        )


    # =========================================================================
    # MANDATORY LIFECYCLE METHODS
    # =========================================================================

    def start(self) -> bool:
        logger.info(f"Starting plugin '{self.name}'")

        saved_state = self.load_state()
        if saved_state:
            self._queued_positions = saved_state.get("queued_positions", [])
            self._closed_history = saved_state.get("closed_history", [])
            if self._queued_positions:
                logger.warning(
                    f"Restored {len(self._queued_positions)} queued positions "
                    f"from previous session"
                )

        return True

    def stop(self) -> bool:
        logger.info(f"Stopping plugin '{self.name}'")

        if self._queued_positions:
            logger.warning(
                f"Stopping with {len(self._queued_positions)} unprocessed positions!"
            )

        self.save_state({
            "queued_positions": self._queued_positions,
            "closed_history": self._closed_history,
        })

        self.unsubscribe_all()
        return True

    def freeze(self) -> bool:
        logger.info(f"Freezing plugin '{self.name}'")

        self.save_state({
            "queued_positions": self._queued_positions,
            "closed_history": self._closed_history,
        })

        return True

    def resume(self) -> bool:
        logger.info(f"Resuming plugin '{self.name}'")
        return True

    def handle_request(self, request_type: str, payload: Dict) -> Dict:
        """
        Handle custom requests.

        Supported requests:
        - deposit: Add positions to liquidation queue
            payload: {"positions": [{"symbol": str, "quantity": int/float, "urgency": 1|2|3}, ...]}
        - get_queue: Get current liquidation queue
        - get_history: Get closed position history
        - clear_queue: Remove all queued positions without closing
        """
        if request_type == "deposit":
            return self._handle_deposit(payload)

        elif request_type == "get_queue":
            return {
                "success": True,
                "data": {
                    "queued_positions": list(self._queued_positions),
                    "count": len(self._queued_positions),
                },
            }

        elif request_type == "get_history":
            return {
                "success": True,
                "data": {
                    "closed_history": list(self._closed_history),
                    "count": len(self._closed_history),
                },
            }

        elif request_type == "clear_queue":
            cleared = len(self._queued_positions)
            self._queued_positions.clear()
            logger.info(f"Cleared {cleared} positions from panic queue")
            return {
                "success": True,
                "message": f"Cleared {cleared} positions from queue",
            }

        else:
            return {
                "success": False,
                "message": f"Unknown request type: {request_type}",
            }

    def _handle_deposit(self, payload: Dict) -> Dict:
        """Validate and add positions to the liquidation queue."""
        positions = payload.get("positions")
        if not positions or not isinstance(positions, list):
            return {
                "success": False,
                "message": "Payload must contain 'positions' list",
            }

        added = []
        errors = []

        for i, pos in enumerate(positions):
            symbol = pos.get("symbol")
            quantity = pos.get("quantity")
            urgency = pos.get("urgency", 2)

            if not symbol or not isinstance(symbol, str):
                errors.append(f"Position {i}: missing or invalid 'symbol'")
                continue

            if not quantity or quantity <= 0:
                errors.append(f"Position {i} ({symbol}): quantity must be > 0")
                continue

            if urgency not in (1, 2, 3):
                errors.append(
                    f"Position {i} ({symbol}): urgency must be 1, 2, or 3, got {urgency}"
                )
                continue

            entry = {
                "symbol": symbol.upper(),
                "quantity": quantity,
                "urgency": urgency,
                "deposited_at": datetime.now().isoformat(),
            }
            self._queued_positions.append(entry)
            added.append(entry)
            logger.info(
                f"Deposited {symbol.upper()} qty={quantity} urgency={urgency} into panic queue"
            )

        # Persist after deposit
        self.save_state({
            "queued_positions": self._queued_positions,
            "closed_history": self._closed_history,
        })

        result = {
            "success": len(added) > 0 or len(errors) == 0,
            "data": {
                "added": len(added),
                "positions": added,
            },
        }
        if errors:
            result["errors"] = errors

        return result

    # =========================================================================
    # TRADING INTERFACE
    # =========================================================================

    def calculate_signals(self) -> List[TradeSignal]:
        """
        Generate SELL signals for all queued positions.

        Positions are sorted by urgency (3 first, 1 last) so the execution
        engine processes the most urgent liquidations first.
        """
        if not self._queued_positions:
            logger.info("Panic plugin: no positions queued for liquidation")
            return []

        # Sort by urgency descending (3 -> 2 -> 1)
        sorted_positions = sorted(
            self._queued_positions, key=lambda p: p["urgency"], reverse=True
        )

        signals = []
        for pos in sorted_positions:
            urgency_label = URGENCY_MAP.get(pos["urgency"], "Normal")

            signals.append(TradeSignal(
                symbol=pos["symbol"],
                action="SELL",
                quantity=Decimal(str(pos["quantity"])),
                target_weight=0.0,
                current_weight=0.0,
                reason=f"Panic liquidation (urgency={pos['urgency']})",
                confidence=1.0,
                urgency=urgency_label,
            ))

        # Move to history and clear queue
        for pos in self._queued_positions:
            pos["closed_at"] = datetime.now().isoformat()
            self._closed_history.append(pos)
        self._queued_positions.clear()

        # Persist
        self.save_state({
            "queued_positions": self._queued_positions,
            "closed_history": self._closed_history,
        })

        # Publish signals to MessageBus
        self.publish(
            f"{self.name}_signals",
            {
                "action": "liquidate",
                "signals": [
                    {
                        "symbol": s.symbol,
                        "quantity": s.quantity,
                        "urgency": s.urgency,
                    }
                    for s in signals
                ],
                "timestamp": datetime.now().isoformat(),
            },
            message_type="signal",
        )

        logger.warning(
            f"Panic plugin generated {len(signals)} SELL signals for liquidation"
        )
        return signals
