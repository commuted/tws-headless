"""
client.py - Interactive Brokers async API client

Async-native IB client: composes AsyncIBTransport for connection/dispatch,
inherits EClient solely for its req* request methods (47 methods that build
and send field-encoded or Protobuf-encoded messages to TWS).

The threading model (EReader + queue.Queue + EClient.run()) is replaced by
AsyncIBTransport.run(), an asyncio coroutine that runs as a Task.
"""

import asyncio
import logging
from typing import Optional, Callable, Dict

from ibapi import comm
from ibapi.client import EClient
from ibapi.server_versions import MIN_SERVER_VER_PROTOBUF
from ibapi.wrapper import EWrapper

from ib.async_transport import AsyncIBTransport

logger = logging.getLogger(__name__)


class IBClient(EWrapper, EClient):
    """
    Async Interactive Brokers API client.

    Inherits EClient for all req* request methods; overrides the transport
    layer (connect, sendMsg, isConnected, serverVersion) to use asyncio.

    Usage:
        client = IBClient()
        if await client.connect():
            await client.reqCurrentTime()
            await client.disconnect()
    """

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
        EWrapper.__init__(self)
        EClient.__init__(self, self)  # passes self as wrapper; inits req* method state

        self._host = host
        self._port = port
        self.host = host
        self.port = port
        self.client_id = client_id
        self.timeout = timeout

        self._transport: Optional[AsyncIBTransport] = None
        self._connected = asyncio.Event()
        self._next_order_id: Optional[int] = None
        self._next_req_id = 1
        self.managed_accounts: list = []
        self._callbacks: Dict[str, Callable] = {}
        self._run_task: Optional[asyncio.Task] = None

    # =========================================================================
    # EClient overrides — transport layer
    # =========================================================================

    def serverVersion(self) -> int:
        """Return server version (overrides EClient.serverVersion)."""
        return self._transport.serverVersion if self._transport else 0

    def isConnected(self) -> bool:
        """Return True if the transport is connected (overrides EClient.isConnected)."""
        return self._transport is not None and self._transport.isConnected()

    def sendMsg(self, msgId: int, msg: str) -> None:
        """
        Build and write a field-encoded message (overrides EClient.sendMsg).

        Called by all inherited req* methods. Synchronous: asyncio.StreamWriter.write()
        buffers the bytes; the event loop drains when it next has I/O time.
        """
        use_raw = self.serverVersion() >= MIN_SERVER_VER_PROTOBUF
        full_msg = comm.make_msg(msgId, use_raw, msg)
        self._transport.send_msg(full_msg)

    def sendMsgProtoBuf(self, msgId: int, msg: bytes) -> None:
        """
        Build and write a Protobuf-encoded message (overrides EClient.sendMsgProtoBuf).

        Called by req* methods that use Protobuf serialization (placeOrder,
        cancelOrder, reqExecutions, reqGlobalCancel) when serverVersion >= 203.
        """
        full_msg = comm.make_msg_proto(msgId, msg)
        self._transport.send_msg(full_msg)

    # =========================================================================
    # Connection lifecycle
    # =========================================================================

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    @property
    def next_order_id(self) -> Optional[int]:
        return self._next_order_id

    def get_next_req_id(self) -> int:
        """Return next unique request ID. Safe to call from the event loop thread."""
        req_id = self._next_req_id
        self._next_req_id += 1
        return req_id

    def register_callback(self, event: str, callback: Callable) -> None:
        self._callbacks[event] = callback

    async def connect(self) -> bool:
        """
        Connect to IB TWS/Gateway and wait for nextValidId.

        Returns True if connection succeeds within timeout, False otherwise.
        """
        try:
            logger.info(f"Connecting to IB at {self._host}:{self._port}...")
            self._connected.clear()
            self._transport = AsyncIBTransport(wrapper=self)
            await self._transport.connect(self._host, self._port, self.client_id)
            self._run_task = asyncio.create_task(self._transport.run())
            await asyncio.wait_for(self._connected.wait(), timeout=self.timeout)
            logger.info("Successfully connected to IB")
            return True
        except asyncio.TimeoutError:
            logger.error("Connection timeout - no response from IB")
            return False
        except Exception as e:
            logger.error(f"Connection error: {e}")
            return False

    async def disconnect(self) -> None:
        """Disconnect from IB and cancel the transport task."""
        self._connected.clear()
        if self._transport:
            try:
                self._transport.disconnect()
            except Exception:
                pass
        if self._run_task:
            self._run_task.cancel()
            try:
                await self._run_task
            except asyncio.CancelledError:
                pass
        logger.info("Disconnected from IB")

    async def reconnect(self) -> bool:
        """Disconnect and reconnect."""
        await self.disconnect()
        return await self.connect()

    # =========================================================================
    # EWrapper callbacks
    # =========================================================================

    def nextValidId(self, orderId: int):
        """Called when connection is established; signals connect() to return."""
        self._next_order_id = orderId
        self._connected.set()
        logger.debug(f"Next valid order ID: {orderId}")
        if "nextValidId" in self._callbacks:
            self._callbacks["nextValidId"](orderId)

    def managedAccounts(self, accountsList: str):
        self.managed_accounts = [a.strip() for a in accountsList.split(",") if a.strip()]
        logger.info(f"Managed accounts: {self.managed_accounts}")
        if "managedAccounts" in self._callbacks:
            self._callbacks["managedAccounts"](self.managed_accounts)

    def error(self, reqId, errorTime: int, errorCode: int, errorString: str,
              advancedOrderRejectJson: str = ""):
        if errorCode in (2104, 2106, 2158, 2119):
            logger.debug(f"IB Info [{errorCode}]: {errorString}")
        elif errorCode in (10167,):
            logger.debug(f"IB Note [{errorCode}]: {errorString}")
        elif reqId == -1:
            logger.warning(f"IB System [{errorCode}]: {errorString}")
        else:
            logger.error(f"IB Error [{errorCode}] reqId={reqId}: {errorString}")
            if advancedOrderRejectJson:
                logger.error(f"Order reject details: {advancedOrderRejectJson}")
        if "error" in self._callbacks:
            self._callbacks["error"](reqId, errorCode, errorString)

    def connectionClosed(self):
        self._connected.clear()
        logger.warning("Connection to IB closed")
        if "connectionClosed" in self._callbacks:
            self._callbacks["connectionClosed"]()

    def currentTime(self, time: int):
        logger.debug(f"IB server time: {time}")

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
        return False

    def __repr__(self):
        status = "connected" if self.connected else "disconnected"
        return f"IBClient({self.host}:{self.port}, {status})"
