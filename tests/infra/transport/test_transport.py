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
"""Transport layer tests — scenarios 12, 15-20."""

from __future__ import annotations

import asyncio
import contextlib
import uuid

import pytest

from tourillon.core.ports.transport import (
    MAX_IN_FLIGHT_PER_CONN,
    ConnectionClosedError,
    ResponseTimeoutError,
)
from tourillon.core.structure.envelope import Envelope
from tourillon.core.transport.client import TcpClient
from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.core.transport.server import TcpServer


async def _start_server(dispatcher: Dispatcher) -> tuple[asyncio.Server, str]:
    """Start a plain-TCP server and return (server, addr_str)."""
    server = TcpServer(dispatcher, ssl_context=None)
    srv = await asyncio.start_server(server._handle_connection, "127.0.0.1", 0)
    addr = srv.sockets[0].getsockname()
    return srv, f"{addr[0]}:{addr[1]}"


@pytest.mark.bootstrap
async def test_12_tcp_connect_refused_when_server_not_running() -> None:
    """TCP connect to a port with no server raises ConnectionRefusedError at OS level."""
    client = TcpClient()
    with pytest.raises((ConnectionRefusedError, OSError)):
        await client.connect("127.0.0.1:19999", tls_ctx=None)  # type: ignore[arg-type]


@pytest.mark.bootstrap
async def test_15_tcpclient_request_returns_matching_correlation_id() -> None:
    """TcpClient.request() returns response with matching correlation_id."""

    async def echo(receive, send) -> None:  # noqa: ANN001
        env = await receive()
        await send(
            Envelope(kind="pong", payload=b"", correlation_id=env.correlation_id)
        )

    dispatcher = Dispatcher()
    dispatcher.register("ping", echo)
    srv, addr = await _start_server(dispatcher)

    client = TcpClient()
    try:
        await client.connect(addr, tls_ctx=None)  # type: ignore[arg-type]
        req = Envelope(kind="ping", payload=b"")
        resp = await client.request(req)
        assert resp.correlation_id == req.correlation_id
    finally:
        await client.close()
        srv.close()
        await srv.wait_closed()


@pytest.mark.bootstrap
async def test_16_server_closes_when_max_in_flight_reached() -> None:
    """Server closes connection when MAX_IN_FLIGHT_PER_CONN is reached; client disconnects."""
    hang_event: asyncio.Event = asyncio.Event()

    async def hang(receive, send) -> None:  # noqa: ANN001
        await receive()
        await hang_event.wait()

    dispatcher = Dispatcher()
    dispatcher.register("hang", hang)
    srv, addr = await _start_server(dispatcher)

    client = TcpClient()
    futures: list[asyncio.Task[Envelope]] = []
    try:
        await client.connect(addr, tls_ctx=None)  # type: ignore[arg-type]
        # Flood the connection with MAX_IN_FLIGHT_PER_CONN + 1 requests
        for _ in range(MAX_IN_FLIGHT_PER_CONN + 1):
            env = Envelope(kind="hang", payload=b"")
            fut = asyncio.get_running_loop().create_task(
                client.request(env, timeout=15.0)
            )
            futures.append(fut)
        # Give the event loop enough time to flush all writes and let the
        # server read loop process them and close the connection.
        await asyncio.sleep(0.5)

        # The server should have closed the connection after hitting the limit.
        assert not client.is_connected
    finally:
        hang_event.set()
        for fut in futures:
            fut.cancel()
        await asyncio.gather(*futures, return_exceptions=True)
        await client.close()
        srv.close()


@pytest.mark.bootstrap
async def test_17_server_sends_payload_too_large_error() -> None:
    """Server sends error.payload_too_large with matching correlation_id then closes."""
    import struct

    dispatcher = Dispatcher()
    srv, addr = await _start_server(dispatcher)

    host, port_str = addr.rsplit(":", 1)
    reader, writer = await asyncio.open_connection(host, int(port_str))

    try:
        corr_id = uuid.uuid4()
        oversized_len = 5 * 1024 * 1024  # 5 MiB > 4 MiB limit
        header = struct.pack("!BH16sIB", 1, 1, corr_id.bytes, oversized_len, 3)
        writer.write(header + b"kv.")
        await writer.drain()

        resp = await asyncio.wait_for(reader.read(256), timeout=5.0)
        assert len(resp) >= 24
        resp_corr_id = uuid.UUID(bytes=resp[3:19])
        assert resp_corr_id == corr_id
        kind_len = resp[23]
        resp_kind = resp[24 : 24 + kind_len].decode("utf-8")
        assert resp_kind == "error.payload_too_large"
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        srv.close()
        await srv.wait_closed()


@pytest.mark.bootstrap
async def test_18_connection_lost_mid_flight_raises_connection_closed_error() -> None:
    """Outstanding request() fails with ConnectionClosedError when connection drops."""
    hang_event: asyncio.Event = asyncio.Event()
    server_writer_ref: list[asyncio.StreamWriter] = []

    async def hang(receive, send) -> None:  # noqa: ANN001
        await receive()
        await hang_event.wait()

    dispatcher = Dispatcher()
    dispatcher.register("hang", hang)
    server = TcpServer(dispatcher, ssl_context=None)

    # Wrap _handle_connection to capture the server-side writer reference.
    original = server._handle_connection

    async def tracked(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        server_writer_ref.append(w)
        await original(r, w)

    srv = await asyncio.start_server(tracked, "127.0.0.1", 0)
    addr = srv.sockets[0].getsockname()

    client = TcpClient()
    await client.connect(f"{addr[0]}:{addr[1]}", tls_ctx=None)  # type: ignore[arg-type]
    req = Envelope(kind="hang", payload=b"")
    fut = asyncio.create_task(client.request(req, timeout=10.0))
    # Wait until the handler is running and the server writer is captured.
    for _ in range(20):
        if server_writer_ref:
            break
        await asyncio.sleep(0.05)

    # Force-close the server-side writer → client reader gets EOF → _fail_pending.
    server_writer_ref[0].close()
    hang_event.set()

    with pytest.raises(ConnectionClosedError):
        await asyncio.wait_for(fut, timeout=5.0)

    await client.close()
    srv.close()


@pytest.mark.bootstrap
async def test_19_tcpclient_stream_delivers_all_envelopes_in_order() -> None:
    """TcpClient.stream() yields 3 progress envelopes then a .done; caller terminates on .done."""

    async def streamer(receive, send) -> None:  # noqa: ANN001
        env = await receive()
        for i in range(3):
            await send(
                Envelope(
                    kind="node.join.progress",
                    payload=str(i).encode(),
                    correlation_id=env.correlation_id,
                )
            )
        await send(
            Envelope(
                kind="node.join.done",
                payload=b"",
                correlation_id=env.correlation_id,
            )
        )

    dispatcher = Dispatcher()
    dispatcher.register("node.join", streamer)
    srv, addr = await _start_server(dispatcher)

    client = TcpClient()
    received = []
    try:
        await client.connect(addr, tls_ctx=None)  # type: ignore[arg-type]
        req = Envelope(kind="node.join", payload=b"")
        async for env in client.stream(req, timeout=5.0):
            received.append(env)
            if env.kind.endswith(".done"):
                break
    finally:
        await client.close()
        srv.close()
        await srv.wait_closed()

    assert len(received) == 4
    assert received[-1].kind == "node.join.done"
    for i, env in enumerate(received[:3]):
        assert env.payload == str(i).encode()


@pytest.mark.bootstrap
async def test_20_response_timeout_raises_and_connection_stays_open() -> None:
    """ResponseTimeoutError raised when server stays silent; connection remains open."""
    hang_event: asyncio.Event = asyncio.Event()

    async def hang(receive, send) -> None:  # noqa: ANN001
        await receive()
        await hang_event.wait()

    dispatcher = Dispatcher()
    dispatcher.register("hang", hang)
    srv, addr = await _start_server(dispatcher)

    client = TcpClient()
    try:
        await client.connect(addr, tls_ctx=None)  # type: ignore[arg-type]
        req = Envelope(kind="hang", payload=b"")
        with pytest.raises(ResponseTimeoutError):
            await client.request(req, timeout=0.1)

        # Connection must stay open — a second request on a different kind should work
        # (server may have closed the first task, but client connection is still up)
        assert client.is_connected
    finally:
        hang_event.set()
        await client.close()
        srv.close()
        await srv.wait_closed()


@pytest.mark.bootstrap
async def test_21_server_side_streaming_receive_routes_same_cid_to_handler() -> None:
    """Server routes same-cid follow-up envelopes to the running handler's queue.

    A streaming handler calls receive() twice: the first call returns the initial
    envelope; the second call should return a follow-up envelope sent by the client
    on the same correlation_id via TcpClient.send(). Server-side streaming receive
    must route it to the handler's queue rather than spawning a new handler.
    """
    second_received: list[bytes] = []

    async def streaming_handler(receive, send) -> None:  # noqa: ANN001
        env = await receive()
        # Send an intermediate ack so the client knows it can send the second envelope.
        await send(
            Envelope(kind="ping.ack", payload=b"", correlation_id=env.correlation_id)
        )
        # Now await the follow-up envelope on the same correlation_id.
        second = await receive()
        second_received.append(second.payload)
        await send(
            Envelope(
                kind="ping.done", payload=b"done", correlation_id=env.correlation_id
            )
        )

    dispatcher = Dispatcher()
    dispatcher.register("ping", streaming_handler)
    srv, addr = await _start_server(dispatcher)

    client = TcpClient()
    received = []
    try:
        await client.connect(addr, tls_ctx=None)  # type: ignore[arg-type]
        req = Envelope(kind="ping", payload=b"first")
        async for env in client.stream(req, timeout=5.0):
            received.append(env.kind)
            if env.kind == "ping.ack":
                # Fire-and-forget follow-up on the same correlation_id.
                follow = Envelope(
                    kind="ping",
                    payload=b"second",
                    correlation_id=req.correlation_id,
                )
                await client.send(follow)
            if env.kind == "ping.done":
                break
    finally:
        await client.close()
        srv.close()
        await srv.wait_closed()

    assert "ping.ack" in received
    assert "ping.done" in received
    assert second_received == [b"second"]


@pytest.mark.bootstrap
async def test_22_client_send_fire_and_forget_no_response_expected() -> None:
    """TcpClient.send() sends an envelope without registering any future or queue.

    The server receives the envelope; the client does not hang waiting for a
    response since no correlation_id is registered.
    """
    received_kinds: list[str] = []
    done_event = asyncio.Event()

    async def receiver(receive, send) -> None:  # noqa: ANN001
        env = await receive()
        received_kinds.append(env.kind)
        done_event.set()

    dispatcher = Dispatcher()
    dispatcher.register("ff.msg", receiver)
    srv, addr = await _start_server(dispatcher)

    client = TcpClient()
    try:
        await client.connect(addr, tls_ctx=None)  # type: ignore[arg-type]
        env = Envelope(kind="ff.msg", payload=b"fire")
        await client.send(env)
        await asyncio.wait_for(done_event.wait(), timeout=5.0)
    finally:
        await client.close()
        srv.close()
        await srv.wait_closed()

    assert received_kinds == ["ff.msg"]
