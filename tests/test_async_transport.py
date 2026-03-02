"""
Unit tests for async_transport.py

Tests AsyncIBTransport: TCP connection, API handshake, message loop,
message dispatch, and send/receive helpers.

The ibapi constants imported by async_transport are MagicMocks at module load
time (set by conftest.py).  The autouse fixture below replaces them with real
integers so comparison operators work correctly in every test.
"""

import asyncio
import struct

import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call

from ib.async_transport import AsyncIBTransport, _MAX_MSG_LEN


# ---------------------------------------------------------------------------
# Concrete integer values used across the test suite
# ---------------------------------------------------------------------------

_MIN_CLIENT_VER = 163
_MAX_CLIENT_VER = 176
_MIN_SERVER_VER_OPTIONAL_CAPABILITIES = 120   # threshold for optional-capabilities field
_MIN_SERVER_VER_PROTOBUF = 170                # threshold for modern framing
_PROTOBUF_MSG_ID = 1000                       # threshold for protobuf vs field-encoded

_VER_OLD = 100      # below both thresholds
_VER_MID = 150      # >= optional capabilities, < protobuf
_VER_MODERN = 175   # >= protobuf threshold


# ---------------------------------------------------------------------------
# Autouse fixture: replace MagicMock constants with real integers
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def patch_transport_constants():
    """Ensure server-version constants in ib.async_transport are real integers."""
    with patch.multiple(
        'ib.async_transport',
        MIN_CLIENT_VER=_MIN_CLIENT_VER,
        MAX_CLIENT_VER=_MAX_CLIENT_VER,
        MIN_SERVER_VER_OPTIONAL_CAPABILITIES=_MIN_SERVER_VER_OPTIONAL_CAPABILITIES,
        MIN_SERVER_VER_PROTOBUF=_MIN_SERVER_VER_PROTOBUF,
        PROTOBUF_MSG_ID=_PROTOBUF_MSG_ID,
    ):
        yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _framed(payload: bytes) -> tuple[bytes, bytes]:
    """Return (4-byte length header, payload) for _recv_msg simulation."""
    return struct.pack("!I", len(payload)), payload


def _make_reader_for_version(server_version: int) -> tuple[AsyncMock, bytes]:
    """
    Simulate the server sending a version-handshake response.
    Returns (reader mock, raw payload bytes).
    """
    payload = f"{server_version}\x00connection_time\x00".encode()
    header, body = _framed(payload)
    reader = AsyncMock()
    reader.readexactly = AsyncMock(side_effect=[header, body])
    return reader, payload


def _connect_patches(server_version: int, reader, writer):
    """
    Context manager stack for a successful connect() call:
    patches open_connection, comm, and Decoder.
    """
    import contextlib

    @contextlib.asynccontextmanager
    async def _ctx():
        with patch('asyncio.open_connection', return_value=(reader, writer)), \
             patch('ib.async_transport.comm') as mock_comm, \
             patch('ib.async_transport.Decoder') as MockDecoder:
            mock_comm.make_initial_msg.return_value = b""
            mock_comm.read_fields.return_value = [str(server_version), "time"]
            mock_comm.make_msg.return_value = b""
            yield mock_comm, MockDecoder

    return _ctx()


# ===========================================================================
# Init
# ===========================================================================

class TestAsyncIBTransportInit:
    """AsyncIBTransport initialises all attributes to safe defaults."""

    def test_wrapper_stored(self):
        wrapper = MagicMock()
        t = AsyncIBTransport(wrapper=wrapper)
        assert t.wrapper is wrapper

    def test_server_version_is_none(self):
        t = AsyncIBTransport(wrapper=MagicMock())
        assert t.serverVersion is None

    def test_not_connected_by_default(self):
        t = AsyncIBTransport(wrapper=MagicMock())
        assert t._connected is False
        assert t.isConnected() is False

    def test_reader_writer_decoder_are_none(self):
        t = AsyncIBTransport(wrapper=MagicMock())
        assert t._reader is None
        assert t._writer is None
        assert t._decoder is None


# ===========================================================================
# connect()
# ===========================================================================

class TestAsyncIBTransportConnect:
    """TCP connection and API handshake."""

    async def test_success_sets_connected_true(self):
        reader, _ = _make_reader_for_version(_VER_OLD)
        writer = MagicMock()
        async with _connect_patches(_VER_OLD, reader, writer):
            t = AsyncIBTransport(wrapper=MagicMock())
            await t.connect("127.0.0.1", 7497, 1)
        assert t._connected is True

    async def test_success_stores_server_version(self):
        reader, _ = _make_reader_for_version(_VER_MID)
        writer = MagicMock()
        async with _connect_patches(_VER_MID, reader, writer):
            t = AsyncIBTransport(wrapper=MagicMock())
            await t.connect("127.0.0.1", 7497, 1)
        assert t.serverVersion == _VER_MID

    async def test_success_creates_decoder(self):
        reader, _ = _make_reader_for_version(_VER_OLD)
        writer = MagicMock()
        async with _connect_patches(_VER_OLD, reader, writer) as (_, MockDecoder):
            t = AsyncIBTransport(wrapper=MagicMock())
            await t.connect("127.0.0.1", 7497, 1)
        MockDecoder.assert_called_once()
        assert t._decoder is not None

    async def test_first_write_starts_with_api_sign(self):
        """The handshake begins with the literal b'API\\0' marker."""
        reader, _ = _make_reader_for_version(_VER_OLD)
        writer = MagicMock()
        async with _connect_patches(_VER_OLD, reader, writer):
            t = AsyncIBTransport(wrapper=MagicMock())
            await t.connect("127.0.0.1", 7497, 1)
        first_arg = writer.write.call_args_list[0][0][0]
        assert first_arg.startswith(AsyncIBTransport.API_SIGN)

    async def test_old_server_omits_optional_capabilities_field(self):
        """Server below MIN_SERVER_VER_OPTIONAL_CAPABILITIES: 2 make_field calls only."""
        reader, _ = _make_reader_for_version(_VER_OLD)
        writer = MagicMock()
        with patch('asyncio.open_connection', return_value=(reader, writer)), \
             patch('ib.async_transport.comm') as mock_comm, \
             patch('ib.async_transport.make_field') as mock_make_field, \
             patch('ib.async_transport.Decoder'):
            mock_comm.make_initial_msg.return_value = b""
            mock_comm.read_fields.return_value = [str(_VER_OLD), "time"]
            mock_comm.make_msg.return_value = b""
            mock_make_field.return_value = ""

            t = AsyncIBTransport(wrapper=MagicMock())
            await t.connect("127.0.0.1", 7497, 1)

        # Only version=2 and client_id — no optional-capabilities ""
        assert mock_make_field.call_count == 2
        assert call("") not in mock_make_field.call_args_list

    async def test_mid_server_appends_optional_capabilities_field(self):
        """Server >= MIN_SERVER_VER_OPTIONAL_CAPABILITIES: 3 make_field calls."""
        reader, _ = _make_reader_for_version(_VER_MID)
        writer = MagicMock()
        with patch('asyncio.open_connection', return_value=(reader, writer)), \
             patch('ib.async_transport.comm') as mock_comm, \
             patch('ib.async_transport.make_field') as mock_make_field, \
             patch('ib.async_transport.Decoder'):
            mock_comm.make_initial_msg.return_value = b""
            mock_comm.read_fields.return_value = [str(_VER_MID), "time"]
            mock_comm.make_msg.return_value = b""
            mock_make_field.return_value = ""

            t = AsyncIBTransport(wrapper=MagicMock())
            await t.connect("127.0.0.1", 7497, 1)

        assert mock_make_field.call_count == 3
        mock_make_field.assert_any_call("")  # the empty optional-capabilities field

    async def test_connection_error_propagates(self):
        """OSError from open_connection is not swallowed."""
        with patch('asyncio.open_connection', side_effect=OSError("refused")):
            t = AsyncIBTransport(wrapper=MagicMock())
            with pytest.raises(OSError):
                await t.connect("127.0.0.1", 7497, 1)
        assert t._connected is False


# ===========================================================================
# disconnect()
# ===========================================================================

class TestAsyncIBTransportDisconnect:
    """disconnect() clears state and closes the writer."""

    def test_sets_connected_false(self):
        t = AsyncIBTransport(wrapper=MagicMock())
        t._connected = True
        t._writer = MagicMock()
        t.disconnect()
        assert t._connected is False

    def test_closes_writer(self):
        writer = MagicMock()
        t = AsyncIBTransport(wrapper=MagicMock())
        t._connected = True
        t._writer = writer
        t.disconnect()
        writer.close.assert_called_once()

    def test_no_writer_does_not_raise(self):
        t = AsyncIBTransport(wrapper=MagicMock())
        t._connected = True
        t._writer = None
        t.disconnect()          # must not raise
        assert t._connected is False


# ===========================================================================
# isConnected()
# ===========================================================================

class TestAsyncIBTransportIsConnected:
    """isConnected() reflects the _connected flag."""

    def test_false_by_default(self):
        assert AsyncIBTransport(wrapper=MagicMock()).isConnected() is False

    def test_true_after_set(self):
        t = AsyncIBTransport(wrapper=MagicMock())
        t._connected = True
        assert t.isConnected() is True

    def test_false_after_disconnect(self):
        t = AsyncIBTransport(wrapper=MagicMock())
        t._connected = True
        t._writer = MagicMock()
        t.disconnect()
        assert t.isConnected() is False


# ===========================================================================
# run()
# ===========================================================================

class TestAsyncIBTransportRun:
    """Async message loop: dispatch, error recovery, loop termination."""

    def _transport(self, server_version=_VER_OLD) -> AsyncIBTransport:
        t = AsyncIBTransport(wrapper=MagicMock())
        t._connected = True
        t._decoder = MagicMock()
        t.serverVersion = server_version
        return t

    async def test_dispatches_received_message(self):
        t = self._transport()
        msg = b"4\x00some_payload"
        # First call returns the message; second raises to terminate the loop.
        with patch.object(t, '_recv_msg', side_effect=[msg, asyncio.IncompleteReadError(b"", 4)]), \
             patch.object(t, '_dispatch') as mock_dispatch:
            await t.run()
        mock_dispatch.assert_called_once_with(msg)

    async def test_calls_connection_closed_on_normal_exit(self):
        """connectionClosed is always called when the loop exits."""
        t = self._transport()
        t._connected = False   # loop body never executes
        await t.run()
        t.wrapper.connectionClosed.assert_called_once()

    async def test_incomplete_read_sets_disconnected(self):
        t = self._transport()
        with patch.object(t, '_recv_msg', side_effect=asyncio.IncompleteReadError(b"", 4)):
            await t.run()
        assert t._connected is False

    async def test_incomplete_read_calls_connection_closed(self):
        t = self._transport()
        with patch.object(t, '_recv_msg', side_effect=asyncio.IncompleteReadError(b"", 4)):
            await t.run()
        t.wrapper.connectionClosed.assert_called_once()

    async def test_connection_reset_sets_disconnected(self):
        t = self._transport()
        with patch.object(t, '_recv_msg', side_effect=ConnectionResetError("reset")):
            await t.run()
        assert t._connected is False

    async def test_timeout_is_silently_ignored(self):
        """TimeoutError from wait_for is caught; the loop keeps running."""
        t = self._transport()
        # First call times out; second terminates via IncompleteReadError.
        with patch.object(t, '_recv_msg',
                          side_effect=[asyncio.TimeoutError(),
                                       asyncio.IncompleteReadError(b"", 4)]), \
             patch.object(t, '_dispatch') as mock_dispatch:
            await t.run()
        # No dispatch happened (timeout on first message, then error on second)
        mock_dispatch.assert_not_called()

    async def test_empty_message_not_dispatched(self):
        t = self._transport()
        with patch.object(t, '_recv_msg',
                          side_effect=[b"", asyncio.IncompleteReadError(b"", 4)]), \
             patch.object(t, '_dispatch') as mock_dispatch:
            await t.run()
        mock_dispatch.assert_not_called()

    async def test_does_not_read_when_already_disconnected(self):
        """If _connected is False before run() is called, recv is never awaited."""
        t = self._transport()
        t._connected = False
        with patch.object(t, '_recv_msg') as mock_recv:
            await t.run()
        mock_recv.assert_not_called()


# ===========================================================================
# _dispatch()
# ===========================================================================

class TestAsyncIBTransportDispatch:
    """Message decoding: legacy field-encoded, modern field-encoded, protobuf."""

    def _transport(self, server_version: int) -> AsyncIBTransport:
        t = AsyncIBTransport(wrapper=MagicMock())
        t.serverVersion = server_version
        t._decoder = MagicMock()
        return t

    def test_legacy_calls_interpret_with_fields_and_msgid(self):
        """Legacy format: msgId is a null-terminated decimal string."""
        t = self._transport(_VER_OLD)
        fields = ["f1", "f2"]
        msg = b"4\x00f1\x00f2\x00"
        with patch('ib.async_transport.comm') as mock_comm:
            mock_comm.read_fields.return_value = fields
            t._dispatch(msg)
        t._decoder.interpret.assert_called_once_with(fields, 4)

    def test_legacy_empty_fields_skips_interpret(self):
        t = self._transport(_VER_OLD)
        msg = b"4\x00"
        with patch('ib.async_transport.comm') as mock_comm:
            mock_comm.read_fields.return_value = []
            t._dispatch(msg)
        t._decoder.interpret.assert_not_called()

    def test_modern_field_encoded_calls_interpret(self):
        """Modern format, msgId <= PROTOBUF_MSG_ID → field-encoded path."""
        t = self._transport(_VER_MODERN)
        msgId = 4
        body = b"f1\x00f2\x00"
        msg = msgId.to_bytes(4, "big") + body
        fields = ["f1", "f2"]
        with patch('ib.async_transport.comm') as mock_comm:
            mock_comm.read_fields.return_value = fields
            t._dispatch(msg)
        t._decoder.interpret.assert_called_once_with(fields, msgId)

    def test_modern_protobuf_calls_process_proto_buf(self):
        """Modern format, msgId > PROTOBUF_MSG_ID → protobuf path."""
        t = self._transport(_VER_MODERN)
        relative_id = 5
        msgId = _PROTOBUF_MSG_ID + relative_id
        body = b"\x0a\x04data"
        msg = msgId.to_bytes(4, "big") + body
        t._dispatch(msg)
        t._decoder.processProtoBuf.assert_called_once_with(body, relative_id)
        t._decoder.interpret.assert_not_called()

    def test_modern_empty_fields_skips_interpret(self):
        """Modern field-encoded: if read_fields returns empty, interpret not called."""
        t = self._transport(_VER_MODERN)
        msgId = 4
        msg = msgId.to_bytes(4, "big")   # no body
        with patch('ib.async_transport.comm') as mock_comm:
            mock_comm.read_fields.return_value = []
            t._dispatch(msg)
        t._decoder.interpret.assert_not_called()


# ===========================================================================
# send_msg()
# ===========================================================================

class TestAsyncIBTransportSendMsg:
    """send_msg() writes pre-framed bytes to the writer or raises."""

    def test_writes_bytes_to_writer(self):
        writer = MagicMock()
        t = AsyncIBTransport(wrapper=MagicMock())
        t._writer = writer
        payload = b"\x00\x00\x00\x04data"
        t.send_msg(payload)
        writer.write.assert_called_once_with(payload)

    def test_raises_connection_error_when_no_writer(self):
        t = AsyncIBTransport(wrapper=MagicMock())
        t._writer = None
        with pytest.raises(ConnectionError):
            t.send_msg(b"data")


# ===========================================================================
# _recv_msg()
# ===========================================================================

class TestAsyncIBTransportRecvMsg:
    """Length-framed message reception."""

    async def test_reads_header_then_payload(self):
        payload = b"hello world"
        header = struct.pack("!I", len(payload))
        reader = AsyncMock()
        reader.readexactly = AsyncMock(side_effect=[header, payload])

        t = AsyncIBTransport(wrapper=MagicMock())
        t._reader = reader

        result = await t._recv_msg()

        assert result == payload
        reader.readexactly.assert_any_call(4)
        reader.readexactly.assert_any_call(len(payload))

    async def test_oversized_message_raises_value_error(self):
        header = struct.pack("!I", _MAX_MSG_LEN + 1)
        reader = AsyncMock()
        reader.readexactly = AsyncMock(side_effect=[header])

        t = AsyncIBTransport(wrapper=MagicMock())
        t._reader = reader

        with pytest.raises(ValueError, match="Oversized"):
            await t._recv_msg()

    async def test_exact_max_size_is_accepted(self):
        """_MAX_MSG_LEN itself is within the safety cap."""
        payload = b"x" * _MAX_MSG_LEN
        header = struct.pack("!I", _MAX_MSG_LEN)
        reader = AsyncMock()
        reader.readexactly = AsyncMock(side_effect=[header, payload])

        t = AsyncIBTransport(wrapper=MagicMock())
        t._reader = reader

        result = await t._recv_msg()
        assert len(result) == _MAX_MSG_LEN

    async def test_zero_length_returns_empty_bytes(self):
        header = struct.pack("!I", 0)
        reader = AsyncMock()
        reader.readexactly = AsyncMock(side_effect=[header, b""])

        t = AsyncIBTransport(wrapper=MagicMock())
        t._reader = reader

        assert await t._recv_msg() == b""
