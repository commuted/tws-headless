"""
client.py - Interactive Brokers API connection wrapper

Provides a base client for connecting to IB TWS/Gateway with proper
threading and event handling.
"""

import logging
from threading import Thread, Event, Lock
from typing import Optional, Callable, Dict, Any

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.common import TickerId

# Configure module logger
logger = logging.getLogger(__name__)


class IBClient(EWrapper, EClient):
    """
    Base Interactive Brokers API client with connection management.

    Handles connection lifecycle, threading, and basic callbacks.
    Subclass this to add specific functionality.

    Usage:
        client = IBClient()
        if client.connect():
            # do work
            client.disconnect()
    """

    # Default ports for different IB configurations
    PORTS = {
        "tws_live": 7496,
        "tws_paper": 7497,
        "gateway_live": 4001,
        "gateway_paper": 4002,
    }

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 1,
        timeout: float = 10.0,
    ):
        """
        Initialize the IB client.

        Args:
            host: IB Gateway/TWS host address
            port: IB Gateway/TWS port
            client_id: Unique client identifier
            timeout: Default timeout for operations in seconds
        """
        EWrapper.__init__(self)
        EClient.__init__(self, self)

        # Stable copies: EClient.disconnect() resets its own host/port to
        # None, so we keep private copies for reconnect.
        self._host = host
        self._port = port
        self.host = host
        self.port = port
        self.client_id = client_id
        self.timeout = timeout

        # Connection state
        self._connected = Event()
        self._next_order_id: Optional[int] = None
        self._thread: Optional[Thread] = None
        self._lock = Lock()

        # Account info
        self.managed_accounts: list = []

        # Request ID management
        self._next_req_id = 1

        # Callback registry for custom handlers
        self._callbacks: Dict[str, Callable] = {}

    @property
    def connected(self) -> bool:
        """Check if client is connected to IB"""
        return self._connected.is_set()

    @property
    def next_order_id(self) -> Optional[int]:
        """Get the next valid order ID"""
        return self._next_order_id

    def get_next_req_id(self) -> int:
        """Get next unique request ID"""
        with self._lock:
            req_id = self._next_req_id
            self._next_req_id += 1
            return req_id

    def register_callback(self, event: str, callback: Callable):
        """
        Register a callback for an event.

        Args:
            event: Event name (e.g., 'position', 'error')
            callback: Callable to invoke when event occurs
        """
        self._callbacks[event] = callback

    def connect(self) -> bool:
        """
        Connect to Interactive Brokers TWS/Gateway.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Use saved host/port: EClient.disconnect() resets its own
            # host/port fields to None, so we preserve ours separately.
            host, port = self._host, self._port
            logger.info(f"Connecting to IB at {host}:{port}...")
            EClient.connect(self, host, port, self.client_id)

            # Start message processing thread
            self._thread = Thread(target=self.run, daemon=True)
            self._thread.start()

            # Wait for connection confirmation
            if self._connected.wait(timeout=self.timeout):
                logger.info("Successfully connected to IB")
                return True
            else:
                logger.error("Connection timeout - no response from IB")
                return False

        except Exception as e:
            logger.error(f"Connection error: {e}")
            return False

    def disconnect(self):
        """Disconnect from Interactive Brokers"""
        try:
            self._connected.clear()
            EClient.disconnect(self)
            logger.info("Disconnected from IB")
        except Exception as e:
            logger.error(f"Disconnect error: {e}")

    def reconnect(self) -> bool:
        """Disconnect and reconnect to IB"""
        self.disconnect()
        return self.connect()

    # =========================================================================
    # EWrapper Callbacks
    # =========================================================================

    def nextValidId(self, orderId: int):
        """Called when connection is established with next valid order ID"""
        self._next_order_id = orderId
        self._connected.set()
        logger.debug(f"Next valid order ID: {orderId}")

        if "nextValidId" in self._callbacks:
            self._callbacks["nextValidId"](orderId)

    def managedAccounts(self, accountsList: str):
        """Called with list of managed accounts"""
        self.managed_accounts = [a.strip() for a in accountsList.split(",") if a.strip()]
        logger.info(f"Managed accounts: {self.managed_accounts}")

        if "managedAccounts" in self._callbacks:
            self._callbacks["managedAccounts"](self.managed_accounts)

    def error(
        self,
        reqId: TickerId,
        errorTime: int,
        errorCode: int,
        errorString: str,
        advancedOrderRejectJson: str = "",
    ):
        """Handle error messages from IB"""
        # Categorize errors
        if errorCode in (2104, 2106, 2158, 2119):
            # Informational: market data farm connections
            logger.debug(f"IB Info [{errorCode}]: {errorString}")
        elif errorCode in (10167,):
            # Delayed market data notification
            logger.debug(f"IB Note [{errorCode}]: {errorString}")
        elif reqId == -1:
            # System message
            logger.warning(f"IB System [{errorCode}]: {errorString}")
        else:
            # Actual error
            logger.error(f"IB Error [{errorCode}] reqId={reqId}: {errorString}")
            if advancedOrderRejectJson:
                logger.error(f"Order reject details: {advancedOrderRejectJson}")

        if "error" in self._callbacks:
            self._callbacks["error"](reqId, errorCode, errorString)

    def connectionClosed(self):
        """Called when connection to IB is closed"""
        self._connected.clear()
        logger.warning("Connection to IB closed")

        if "connectionClosed" in self._callbacks:
            self._callbacks["connectionClosed"]()

    def currentTime(self, time: int):
        """Handle server time response"""
        logger.debug(f"IB server time: {time}")

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
        return False

    def __repr__(self):
        status = "connected" if self.connected else "disconnected"
        return f"IBClient({self.host}:{self.port}, {status})"
