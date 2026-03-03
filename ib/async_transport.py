"""
async_transport.py - Asyncio-native IB socket transport

Replaces EClient + EReader + Connection with a single coroutine-based
transport that reads length-prefixed messages from the IB socket and
dispatches them to an EWrapper via the ibapi Decoder.
"""
import asyncio
import logging
import struct
from typing import Optional

from ibapi import comm
from ibapi.comm import make_field
from ibapi.common import PROTOBUF_MSG_ID
from ibapi.decoder import Decoder
from ibapi.message import OUT
from ibapi.server_versions import (
    MAX_CLIENT_VER,
    MIN_CLIENT_VER,
    MIN_SERVER_VER_OPTIONAL_CAPABILITIES,
    MIN_SERVER_VER_PROTOBUF,
)
from ibapi.wrapper import EWrapper

logger = logging.getLogger(__name__)

_MAX_MSG_LEN = 0xFFFFFF  # 16 MB safety cap


class AsyncIBTransport:
    """
    Asyncio socket transport for the IB TWS/Gateway API.

    Connects to TWS, sends API requests, reads length-framed responses,
    and dispatches them to an EWrapper via the ibapi Decoder.

    Usage:
        transport = AsyncIBTransport(wrapper=my_wrapper)
        await transport.connect("127.0.0.1", 7497, client_id=1)
        asyncio.create_task(transport.run())
    """

    API_SIGN = b"API\0"

    def __init__(self, wrapper: EWrapper):
        self.wrapper = wrapper
        self.serverVersion: Optional[int] = None
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected = False
        self._decoder: Optional[Decoder] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    async def connect(self, host: str, port: int, client_id: int) -> None:
        """Open TCP connection and perform the TWS API handshake."""
        self._loop = asyncio.get_event_loop()
        self._reader, self._writer = await asyncio.open_connection(host, port)

        # Send: b"API\0" + length-framed version range string
        version_str = f"v{MIN_CLIENT_VER}..{MAX_CLIENT_VER}"
        self._writer.write(self.API_SIGN + comm.make_initial_msg(version_str))

        # Receive server version + connection time (length-framed, null-delimited fields)
        msg = await self._recv_msg()
        fields = comm.read_fields(msg)
        self.serverVersion = int(fields[0])

        # Send startApi
        body = f"{make_field(2)}{make_field(client_id)}"
        if self.serverVersion >= MIN_SERVER_VER_OPTIONAL_CAPABILITIES:
            body += make_field("")  # optional capabilities
        use_raw = self.serverVersion >= MIN_SERVER_VER_PROTOBUF
        self._writer.write(comm.make_msg(OUT.START_API, use_raw, body))

        self._decoder = Decoder(self.wrapper, self.serverVersion)
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False
        if self._writer:
            self._writer.close()

    def isConnected(self) -> bool:
        return self._connected

    # ------------------------------------------------------------------
    # Message loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Main async message loop.

        Reads length-framed messages from the socket and dispatches each
        through the Decoder (which calls EWrapper methods synchronously).
        Run this as an asyncio Task: asyncio.create_task(transport.run())
        """
        try:
            while self._connected:
                try:
                    msg = await asyncio.wait_for(self._recv_msg(), timeout=5.0)
                except asyncio.TimeoutError:
                    continue
                if not msg:
                    continue
                self._dispatch(msg)
        except (asyncio.IncompleteReadError, ConnectionResetError):
            logger.warning("IB connection closed by remote")
        finally:
            self._connected = False
            self.wrapper.connectionClosed()

    def _dispatch(self, msg: bytes) -> None:
        """Decode one message payload and invoke the corresponding EWrapper callback."""
        if self.serverVersion >= MIN_SERVER_VER_PROTOBUF:
            # Modern format: first 4 bytes are msgId as big-endian int
            msgId = int.from_bytes(msg[:4], "big")
            body = msg[4:]
            if msgId > PROTOBUF_MSG_ID:
                # Protobuf-encoded message
                self._decoder.processProtoBuf(body, msgId - PROTOBUF_MSG_ID)
            else:
                fields = comm.read_fields(body)
                if fields:
                    self._decoder.interpret(fields, msgId)
        else:
            # Legacy format: msgId is a null-terminated decimal string
            null_pos = msg.index(b"\0")
            msgId = int(msg[:null_pos])
            body = msg[null_pos + 1:]
            fields = comm.read_fields(body)
            if fields:
                self._decoder.interpret(fields, msgId)

    # ------------------------------------------------------------------
    # Send helpers
    # ------------------------------------------------------------------

    def send_msg(self, msg: bytes) -> None:
        """
        Write a pre-framed message to the socket.

        msg must already include the 4-byte length prefix (as produced
        by comm.make_msg or comm.make_msg_proto).

        Thread-safe: if called from a non-event-loop thread, the write
        is scheduled on the event loop via call_soon_threadsafe so that
        plugins running in asyncio.to_thread() can send requests safely.
        """
        if not self._writer:
            raise ConnectionError("Not connected")
        try:
            asyncio.get_running_loop()
            # Called from within the event loop — write directly.
            self._writer.write(msg)
        except RuntimeError:
            # Called from a different thread — schedule on the event loop.
            if self._loop is None:
                raise ConnectionError("Transport not connected (no event loop)")
            self._loop.call_soon_threadsafe(self._writer.write, msg)

    # ------------------------------------------------------------------
    # Receive helpers
    # ------------------------------------------------------------------

    async def _recv_msg(self) -> bytes:
        """Read one length-framed message from the socket (payload only, no length prefix)."""
        header = await self._reader.readexactly(4)
        size = struct.unpack("!I", header)[0]
        if size > _MAX_MSG_LEN:
            raise ValueError(f"Oversized IB message: {size} bytes")
        return await self._reader.readexactly(size)
