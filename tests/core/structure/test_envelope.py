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
"""Tests for Envelope, Dispatcher, framing, and correlation_id — scenarios 7-11."""

from __future__ import annotations

import uuid

import pytest

from tourillon.core.structure.envelope import KIND_MAX_LEN, Envelope
from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter


@pytest.mark.bootstrap
async def test_7_dispatcher_unknown_kind_returns_none() -> None:
    """An in-memory dispatcher returns None for an unknown kind (connection close signal)."""
    dispatcher = Dispatcher()
    assert dispatcher.lookup("unknown.kind") is None


@pytest.mark.bootstrap
async def test_8_envelope_kind_too_long_raises_valueerror() -> None:
    """kind of KIND_MAX_LEN+1 UTF-8 bytes raises ValueError at construction time."""
    long_kind = "k" * (KIND_MAX_LEN + 1)
    with pytest.raises(ValueError, match=str(KIND_MAX_LEN)):
        Envelope(kind=long_kind, payload=b"")


@pytest.mark.bootstrap
async def test_9_envelope_msgpack_round_trip_preserves_fields() -> None:
    """encode → decode via MsgpackSerializerAdapter preserves the original value."""
    serializer = MsgpackSerializerAdapter()
    original = {"key": "value", "count": 42, "nested": [1, 2, 3]}
    encoded = serializer.encode(original)
    decoded = serializer.decode(encoded)
    assert decoded == original


@pytest.mark.bootstrap
async def test_9b_envelope_framing_round_trip_preserves_all_fields() -> None:
    """Envelope encode → wire → decode round-trip preserves kind, payload, correlation_id, schema_id."""
    import asyncio

    from tourillon.core.transport.framing import read_envelope

    corr_id = uuid.uuid4()
    env = Envelope(
        kind="kv.put",
        payload=b"\x01\x02\x03",
        correlation_id=corr_id,
        schema_id=1,
    )
    wire = env.encode()
    reader = asyncio.StreamReader()
    reader.feed_data(wire)
    reader.feed_eof()
    decoded = await read_envelope(reader)
    assert decoded.kind == env.kind
    assert decoded.payload == env.payload
    assert decoded.correlation_id == corr_id
    assert decoded.schema_id == env.schema_id


@pytest.mark.bootstrap
async def test_10_proto_version_unsupported_sends_error_envelope() -> None:
    """Server sends kind=error.proto_version_unsupported with matching correlation_id, then closes."""
    import asyncio
    import struct

    from tourillon.core.transport.dispatcher import Dispatcher
    from tourillon.core.transport.server import TcpServer

    dispatcher = Dispatcher()
    server = TcpServer(dispatcher, ssl_context=None)

    host, port = "127.0.0.1", 0
    srv = await asyncio.start_server(server._handle_connection, host, port)
    addr = srv.sockets[0].getsockname()

    try:
        reader, writer = await asyncio.open_connection(addr[0], addr[1])

        corr_id = uuid.uuid4()
        # Build a header with proto_version=99
        bad_header = struct.pack("!BH16sIB", 99, 1, corr_id.bytes, 0, 3)
        writer.write(bad_header + b"kv.")
        await writer.drain()

        # Read response header (24 bytes) + kind
        resp_wire = await asyncio.wait_for(reader.read(256), timeout=5.0)
        # Parse minimal response: first byte is proto_version, bytes 3-19 are correlation_id
        assert len(resp_wire) >= 24
        resp_corr_id = uuid.UUID(bytes=resp_wire[3:19])
        assert resp_corr_id == corr_id

        kind_len = resp_wire[23]
        resp_kind = resp_wire[24 : 24 + kind_len].decode("utf-8")
        assert resp_kind == "error.proto_version_unsupported"
    finally:
        srv.close()
        await srv.wait_closed()


@pytest.mark.bootstrap
async def test_11_handler_response_carries_identical_correlation_id() -> None:
    """Handler sends response Envelope with identical correlation_id."""
    import asyncio

    from tourillon.core.transport.client import TcpClient
    from tourillon.core.transport.dispatcher import Dispatcher
    from tourillon.core.transport.server import TcpServer

    async def echo_handler(receive, send) -> None:  # noqa: ANN001
        env = await receive()
        await send(
            Envelope(kind="kv.put.ok", payload=b"", correlation_id=env.correlation_id)
        )

    dispatcher = Dispatcher()
    dispatcher.register("kv.put", echo_handler)
    server = TcpServer(dispatcher, ssl_context=None)

    srv = await asyncio.start_server(server._handle_connection, "127.0.0.1", 0)
    addr = srv.sockets[0].getsockname()

    client = TcpClient()
    try:
        await client.connect(f"{addr[0]}:{addr[1]}", tls_ctx=None)  # type: ignore[arg-type]
        request_env = Envelope(kind="kv.put", payload=b"hello")
        response = await client.request(request_env)
        assert response.correlation_id == request_env.correlation_id
    finally:
        await client.close()
        srv.close()
        await srv.wait_closed()


@pytest.mark.bootstrap
def test_envelope_make_with_explicit_correlation_id() -> None:
    """Envelope.create() preserves an explicitly supplied correlation_id."""
    cid = uuid.uuid4()
    env = Envelope.create(b"x", kind="kv.get", correlation_id=cid)
    assert env.correlation_id == cid
    assert env.kind == "kv.get"
    assert env.payload == b"x"


@pytest.mark.bootstrap
def test_envelope_make_generates_correlation_id_when_none() -> None:
    """Envelope.create() generates a fresh correlation_id when None is passed."""
    env = Envelope.create(b"", kind="kv.get")
    assert isinstance(env.correlation_id, uuid.UUID)


@pytest.mark.bootstrap
def test_envelope_decode_round_trip() -> None:
    """Envelope.decode(env.encode()) returns an Envelope with identical fields."""
    cid = uuid.uuid4()
    original = Envelope(kind="kv.put", payload=b"\x00\x01\x02", correlation_id=cid)
    decoded = Envelope.decode(original.encode())
    assert decoded.kind == original.kind
    assert decoded.payload == original.payload
    assert decoded.correlation_id == cid
    assert decoded.schema_id == original.schema_id


@pytest.mark.bootstrap
def test_envelope_decode_frame_too_short_raises() -> None:
    """Envelope.decode() raises ValueError when the byte buffer is shorter than the minimum header."""
    with pytest.raises(ValueError, match="frame too short"):
        Envelope.decode(b"\x01\x00")


@pytest.mark.bootstrap
def test_envelope_decode_kind_len_zero_raises() -> None:
    """Envelope.decode() raises ValueError when kind_len is zero."""
    import struct

    # Build a 25-byte buffer: valid header + kind_len=0
    header = struct.pack("!BH16sIB", 1, 1, uuid.uuid4().bytes, 0, 0)
    with pytest.raises(ValueError, match="kind_len is zero"):
        Envelope.decode(header)


@pytest.mark.bootstrap
def test_envelope_decode_kind_len_too_large_raises() -> None:
    """Envelope.decode() raises ValueError when kind_len exceeds KIND_MAX_LEN."""
    import struct

    header = struct.pack("!BH16sIB", 1, 1, uuid.uuid4().bytes, 0, KIND_MAX_LEN + 1)
    with pytest.raises(ValueError, match="kind_len"):
        Envelope.decode(header)


@pytest.mark.bootstrap
def test_envelope_decode_truncated_frame_raises() -> None:
    """Envelope.decode() raises ValueError when declared lengths exceed buffer size."""
    import struct

    # payload_len=100 but no payload bytes follow
    header = struct.pack("!BH16sIB", 1, 1, uuid.uuid4().bytes, 100, 6)
    buf = header + b"kv.put"  # missing 100-byte payload
    with pytest.raises(ValueError, match="truncated"):
        Envelope.decode(buf)
