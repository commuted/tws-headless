"""
demo/sma_publisher/plugin.py

Subscribes to 5-second bars for a configurable symbol, computes a
simple moving average over a rolling window, and publishes each new
value to the MessageBus channel "indicators_sma".

Channel: indicators_sma
Payload:
    {
        "symbol":  "SPY",
        "period":  20,          # number of bars in the SMA window
        "sma":     541.32,      # current SMA value
        "close":   541.87,      # bar close that triggered this publish
        "bar_count": 1042,      # total bars processed since start
    }

Usage:
    plugin load plugins.demo.sma_publisher
    plugin start sma_publisher
"""

import logging
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional

from ib.contract_builder import ContractBuilder
from ib.data_feed import DataType
from plugins.base import PluginBase, TradeSignal

logger = logging.getLogger(__name__)

CHANNEL = "indicators_sma"


class SMAPublisherPlugin(PluginBase):
    """
    Indicator publisher: Simple Moving Average over 5-second bars.

    Streams bars for SYMBOL, maintains a rolling window of SMA_PERIOD
    close prices, and publishes a new SMA value to the MessageBus on
    every bar once the window is full.
    """

    VERSION = "1.0.0"
    IS_SYSTEM_PLUGIN = False

    SYMBOL = "SPY"
    SMA_PERIOD = 20

    def __init__(
        self,
        base_path: Optional[Path] = None,
        portfolio=None,
        shared_holdings=None,
        message_bus=None,
    ):
        super().__init__(
            "sma_publisher",
            base_path, portfolio, shared_holdings, message_bus,
        )
        self._closes: deque = deque(maxlen=self.SMA_PERIOD)
        self._bar_count: int = 0
        self._last_sma: Optional[float] = None

    @property
    def description(self) -> str:
        return (
            f"Demo SMA Publisher: streams {self.SYMBOL} 5-sec bars, computes "
            f"{self.SMA_PERIOD}-bar SMA, publishes to '{CHANNEL}'."
        )

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def start(self) -> bool:
        logger.info(f"[{self.name}] Starting — subscribing to {self.SYMBOL} bars")
        contract = ContractBuilder.us_stock(self.SYMBOL, primary_exchange="ARCA")
        self.request_stream(
            symbol=self.SYMBOL,
            contract=contract,
            data_types={DataType.BAR_5SEC},
            on_bar=self._on_bar,
            what_to_show="TRADES",
            use_rth=True,
        )
        return True

    def stop(self) -> bool:
        logger.info(f"[{self.name}] Stopping")
        self.cancel_stream(self.SYMBOL)
        return True

    def freeze(self) -> bool:
        return True

    def resume(self) -> bool:
        return True

    def calculate_signals(self) -> List[TradeSignal]:
        return []

    # -------------------------------------------------------------------------
    # Bar callback
    # -------------------------------------------------------------------------

    def _on_bar(self, bar) -> None:
        self._closes.append(bar.close)
        self._bar_count += 1

        if len(self._closes) < self.SMA_PERIOD:
            # Window not yet full — log progress every 5 bars
            if self._bar_count % 5 == 0:
                logger.debug(
                    f"[{self.name}] Warming up: {len(self._closes)}/{self.SMA_PERIOD} bars"
                )
            return

        sma = sum(self._closes) / self.SMA_PERIOD
        self._last_sma = sma

        self.publish(
            channel=CHANNEL,
            payload={
                "symbol": self.SYMBOL,
                "period": self.SMA_PERIOD,
                "sma": round(sma, 4),
                "close": bar.close,
                "bar_count": self._bar_count,
            },
            message_type="data",
        )

        logger.debug(
            f"[{self.name}] Published SMA({self.SMA_PERIOD})={sma:.4f} "
            f"close={bar.close} bar={self._bar_count}"
        )

    # -------------------------------------------------------------------------
    # Request handling
    # -------------------------------------------------------------------------

    def handle_request(self, request_type: str, payload: Dict) -> Dict:
        if request_type == "get_status":
            return {
                "success": True,
                "data": {
                    "symbol": self.SYMBOL,
                    "period": self.SMA_PERIOD,
                    "channel": CHANNEL,
                    "bar_count": self._bar_count,
                    "window_size": len(self._closes),
                    "window_full": len(self._closes) >= self.SMA_PERIOD,
                    "last_sma": self._last_sma,
                },
            }
        return {"success": False, "message": f"Unknown request: {request_type}"}
