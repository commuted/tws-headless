"""
demo/sma_subscriber/plugin.py

Subscribes to the "indicators_sma" MessageBus channel and reacts to
each published SMA value.  Logs every update and tracks a short history.
Demonstrates how a strategy plugin would consume an indicator feed.

Usage:
    plugin load plugins.demo.sma_subscriber
    plugin start sma_subscriber
    plugin request sma_subscriber get_status
"""

import logging
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional

from plugins.base import PluginBase, TradeSignal

logger = logging.getLogger(__name__)

CHANNEL = "indicators_sma"
HISTORY_SIZE = 50


class SMASubscriberPlugin(PluginBase):
    """
    Demo strategy plugin that consumes the SMA indicator feed.

    Subscribes to 'indicators_sma' on start, logs every update, and
    demonstrates a simple crossover signal: logs BUY when close crosses
    above SMA and SELL when it crosses below.
    """

    VERSION = "1.0.0"
    IS_SYSTEM_PLUGIN = False

    def __init__(
        self,
        base_path: Optional[Path] = None,
        portfolio=None,
        shared_holdings=None,
        message_bus=None,
    ):
        super().__init__(
            "sma_subscriber",
            base_path, portfolio, shared_holdings, message_bus,
        )
        # Ring buffer of the last HISTORY_SIZE indicator payloads received
        self._history: deque = deque(maxlen=HISTORY_SIZE)
        self._received: int = 0
        self._last_above: Optional[bool] = None  # True = close was above SMA

    @property
    def description(self) -> str:
        return (
            f"Demo SMA Subscriber: listens to '{CHANNEL}', logs each update, "
            f"and signals close/SMA crossovers."
        )

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def start(self) -> bool:
        logger.info(f"[{self.name}] Starting — subscribing to '{CHANNEL}'")
        self.subscribe(CHANNEL, self._on_indicator)
        return True

    def stop(self) -> bool:
        logger.info(f"[{self.name}] Stopping")
        self.unsubscribe_all()
        return True

    def freeze(self) -> bool:
        return True

    def resume(self) -> bool:
        return True

    def calculate_signals(self) -> List[TradeSignal]:
        return []

    # -------------------------------------------------------------------------
    # Indicator callback
    # -------------------------------------------------------------------------

    def _on_indicator(self, message) -> None:
        payload: Dict[str, Any] = message.payload
        symbol    = payload.get("symbol", "?")
        sma       = payload.get("sma", 0.0)
        close     = payload.get("close", 0.0)
        period    = payload.get("period", 0)
        bar_count = payload.get("bar_count", 0)

        self._received += 1
        self._history.append(payload)

        above = close > sma
        crossover = ""
        if self._last_above is not None and above != self._last_above:
            crossover = " *** CROSS ABOVE ***" if above else " *** CROSS BELOW ***"
        self._last_above = above

        logger.info(
            f"[{self.name}] {symbol} bar={bar_count} "
            f"SMA({period})={sma:.4f} close={close:.4f} "
            f"({'above' if above else 'below'}){crossover}"
        )

    # -------------------------------------------------------------------------
    # Request handling
    # -------------------------------------------------------------------------

    def handle_request(self, request_type: str, payload: Dict) -> Dict:
        if request_type == "get_status":
            last = self._history[-1] if self._history else None
            return {
                "success": True,
                "data": {
                    "channel": CHANNEL,
                    "received": self._received,
                    "history_size": len(self._history),
                    "last": last,
                },
            }
        if request_type == "get_history":
            return {
                "success": True,
                "data": list(self._history),
            }
        return {"success": False, "message": f"Unknown request: {request_type}"}
