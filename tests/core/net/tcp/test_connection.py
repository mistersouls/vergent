# Copyright 2026 Tourillon Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for Connection framing and MemoryConnection test double."""

import asyncio
import struct

import pytest

from tourillon.core.dispatch import Dispatcher
from tourillon.core.net.tcp.connection import MAX_FRAME_BYTES, Connection
from tourillon.core.net.tcp.testing import MemoryConnection
from tourillon.core.ports.transport import (
    ConnectionClosedError,
    FrameTooLargeError,
    InvalidEnvelopeError,
    SendCallback,
)
from tourillon.core.structure.envelope import Envelope


class _DummyWriter:
    """Minimal asyncio.StreamWriter stand-in for framing unit tests."""

    def __init__(self, peername: tuple[str, int] | None = ("127.0.0.1", 9999)) -> None:
        self._buffer = bytearray()
        self._closed = False
        self._peername = peername

    def write(self, data: bytes) -> None:
        """Append data to the internal capture buffer."""
        self._buffer.extend(data)

    async def drain(self) -> None:
        """No-op drain — yield control to the event loop."""
        await asyncio.sleep(0)

    def close(self) -> None:
        """Mark writer as closed."""
        self._closed = True

    async def wait_closed(self) -> None:
        """No-op wait_closed."""
        await asyncio.sleep(0)

    def get_extra_info(self, name: str, default: object = None) -> object:
        """Return peername or default for any other key."""
        if name == "peername":
            return self._peername
        return default

    @property
    def captured(self) -> bytes:
        """Return all data written to this writer."""
        return bytes(self._buffer)


def _framed(envelope: Envelope) -> bytes:
    """Encode envelope into a length-prefixed frame for feeding into a StreamReader."""
    body = envelope.encode()
    return struct.pack("!I", len(body)) + body


def _make_connection(
    data: bytes,
    max_frame_bytes: int = MAX_FRAME_BYTES,
    peername: tuple[str, int] | None = ("127.0.0.1", 9999),
) -> tuple[Connection, _DummyWriter]:
    """Build a Connection whose reader is pre-loaded with data."""
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    writer = _DummyWriter(peername=peername)
    return Connection(reader, writer, max_frame_bytes=max_frame_bytes), writer  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_receive_decodes_valid_frame() -> None:
    """receive() returns the correct Envelope for a well-formed frame."""
    env = Envelope.create(b"payload", kind="test.kind")
    conn, _ = _make_connection(_framed(env))
    received = await conn.receive()
    assert received is not None
    assert received.kind == env.kind
    assert received.payload == env.payload


@pytest.mark.asyncio
async def test_receive_returns_none_on_eof() -> None:
    """receive() returns None when the stream is at EOF immediately."""
    conn, _ = _make_connection(b"")
    assert await conn.receive() is None


@pytest.mark.asyncio
async def test_receive_returns_none_on_incomplete_header() -> None:
    """receive() returns None when fewer than 4 header bytes arrive before EOF."""
    conn, _ = _make_connection(b"\x00\x01")
    assert await conn.receive() is None


@pytest.mark.asyncio
async def test_receive_returns_none_on_incomplete_body() -> None:
    """receive() returns None when the frame body is truncated at EOF."""
    header = struct.pack("!I", 5)
    conn, _ = _make_connection(header + b"ab")
    assert await conn.receive() is None


@pytest.mark.asyncio
async def test_receive_raises_frame_too_large() -> None:
    """receive() raises FrameTooLargeError when the declared length exceeds the limit."""
    header = struct.pack("!I", MAX_FRAME_BYTES + 1)
    conn, _ = _make_connection(header)
    with pytest.raises(FrameTooLargeError):
        await conn.receive()


@pytest.mark.asyncio
async def test_receive_raises_invalid_envelope() -> None:
    """receive() raises InvalidEnvelopeError when the body is not a valid Envelope."""
    body = b"not-an-envelope"
    conn, _ = _make_connection(struct.pack("!I", len(body)) + body)
    with pytest.raises(InvalidEnvelopeError):
        await conn.receive()


@pytest.mark.asyncio
async def test_send_writes_framed_envelope() -> None:
    """send() writes a correctly framed envelope that round-trips through decode()."""
    reader = asyncio.StreamReader()
    writer = _DummyWriter()
    conn = Connection(reader, writer)  # type: ignore[arg-type]
    env = Envelope.create(b"rsp", kind="reply")
    await conn.send(env)
    data = writer.captured
    assert len(data) >= 4
    (body_len,) = struct.unpack("!I", data[:4])
    decoded = Envelope.decode(data[4 : 4 + body_len])
    assert decoded.kind == env.kind
    assert decoded.payload == env.payload


@pytest.mark.asyncio
async def test_send_raises_on_closed_connection() -> None:
    """send() raises ConnectionClosedError after close() has been called."""
    conn, _ = _make_connection(b"")
    await conn.close()
    with pytest.raises(ConnectionClosedError):
        await conn.send(Envelope.create(b"x", kind="k"))


@pytest.mark.asyncio
async def test_close_marks_writer_and_allows_double_close() -> None:
    """close() closes the writer; a second call is a safe no-op."""
    conn, writer = _make_connection(b"")
    await conn.close()
    assert writer._closed is True
    await conn.close()


@pytest.mark.asyncio
async def test_close_ignores_drain_and_wait_closed_exceptions() -> None:
    """close() swallows errors from drain() and wait_closed()."""

    class _BadWriter(_DummyWriter):
        async def drain(self) -> None:
            raise RuntimeError("drain failed")

        async def wait_closed(self) -> None:
            raise RuntimeError("wait failed")

    conn = Connection(asyncio.StreamReader(), _BadWriter())  # type: ignore[arg-type]
    await conn.close()
    assert conn._closed is True


@pytest.mark.asyncio
async def test_peer_address_returns_tuple() -> None:
    """peer_address returns (host, port) when the writer reports a valid peername."""
    conn, _ = _make_connection(b"", peername=("10.0.0.1", 4242))
    assert conn.peer_address == ("10.0.0.1", 4242)


@pytest.mark.asyncio
async def test_peer_address_returns_none_when_unavailable() -> None:
    """peer_address returns None when get_extra_info cannot provide a peername."""
    conn, _ = _make_connection(b"", peername=None)
    assert conn.peer_address is None


@pytest.mark.asyncio
async def test_peer_address_returns_none_for_short_tuple() -> None:
    """peer_address returns None when the peername tuple has fewer than 2 elements."""

    class _ShortPeer(_DummyWriter):
        def get_extra_info(self, name: str, default: object = None) -> object:
            return ("only_host",) if name == "peername" else default

    conn = Connection(asyncio.StreamReader(), _ShortPeer())  # type: ignore[arg-type]
    assert conn.peer_address is None


@pytest.mark.asyncio
async def test_memory_connection_round_trip() -> None:
    """push() feeds an envelope that receive() returns unchanged."""
    conn = MemoryConnection()
    env = Envelope.create(b"ping", kind="test.ping")
    await conn.push(env)
    assert await conn.receive() is env


@pytest.mark.asyncio
async def test_memory_connection_send_captures_outbound() -> None:
    """send() appends to sent; sent returns a snapshot of all sent envelopes."""
    conn = MemoryConnection()
    env = Envelope.create(b"out", kind="test.out")
    await conn.send(env)
    assert len(conn.sent) == 1
    assert conn.sent[0] is env


@pytest.mark.asyncio
async def test_memory_connection_close_inbound_returns_none() -> None:
    """close_inbound() causes receive() to return None (EOF signal)."""
    conn = MemoryConnection()
    await conn.close_inbound()
    assert await conn.receive() is None


@pytest.mark.asyncio
async def test_memory_connection_close_raises_on_send() -> None:
    """send() raises ConnectionClosedError after close()."""
    conn = MemoryConnection()
    await conn.close()
    with pytest.raises(ConnectionClosedError):
        await conn.send(Envelope.create(b"x", kind="k"))


@pytest.mark.asyncio
async def test_memory_connection_double_close_is_safe() -> None:
    """Calling close() twice on a MemoryConnection must not raise."""
    conn = MemoryConnection()
    await conn.close()
    await conn.close()


@pytest.mark.asyncio
async def test_memory_connection_clear_sent() -> None:
    """clear_sent() discards previously captured envelopes."""
    conn = MemoryConnection()
    await conn.send(Envelope.create(b"a", kind="k"))
    conn.clear_sent()
    assert conn.sent == []


@pytest.mark.asyncio
async def test_memory_connection_drives_dispatcher() -> None:
    """MemoryConnection integrates cleanly with Dispatcher for handler unit tests."""
    received: list[Envelope] = []

    async def echo(envelope: Envelope, send: SendCallback) -> None:
        received.append(envelope)
        await send(Envelope.create(envelope.payload, kind="echo.reply"))

    dispatcher = Dispatcher()
    dispatcher.register("echo", echo)
    conn = MemoryConnection()
    env = Envelope.create(b"hello", kind="echo")
    await conn.push(env)
    await conn.close_inbound()
    await dispatcher(conn.receive, conn.send)
    assert len(received) == 1
    assert received[0] is env
    assert len(conn.sent) == 1
    assert conn.sent[0].kind == "echo.reply"
    assert conn.sent[0].payload == b"hello"
