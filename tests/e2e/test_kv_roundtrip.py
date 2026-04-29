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
"""End-to-end KV round-trip tests against a live TcpServer and TcpClient.

Each test in this module spins up a real TcpServer bound to an ephemeral port
on 127.0.0.1, issues genuine X.509 mTLS certificates via the project's own PKI
adapter, then exercises the full request/response path through the network stack
— TLS handshake, Connection framing, Dispatcher routing, KvHandlers, MemoryStore
— and asserts on the decoded response payloads.

The fixture is function-scoped: every test gets an isolated server with a
fresh MemoryStore, so test cases are independent and can be run in any order.
The fixture tears the server down after each test, draining any in-flight
connections before returning control to pytest.
"""

import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import NamedTuple

import pytest

from tourctl.core.client import TcpClient
from tourillon.bootstrap.handlers import (
    KIND_KV_DELETE_OK,
    KIND_KV_GET_OK,
    KIND_KV_PUT_OK,
)
from tourillon.bootstrap.node import create_tcp_node
from tourillon.core.net.tcp.server import TcpServer
from tourillon.core.net.tcp.tls import build_ssl_context
from tourillon.core.ports.pki import CaRequest, CertRequest
from tourillon.core.structure.envelope import Envelope
from tourillon.infra.msgpack.serializer import MsgPackSerializer
from tourillon.infra.pki.x509 import X509CertificateAuthority


class _LiveNode(NamedTuple):
    """Holds the active client and serializer for a running test node.

    The TcpServer itself is managed exclusively by the fixture; tests interact
    only with the client and the serializer so they never need to touch
    lifecycle primitives directly.
    """

    client: TcpClient
    serializer: MsgPackSerializer


@pytest.fixture
async def live_node(tmp_path: Path) -> AsyncGenerator[_LiveNode]:
    """Start a fully wired TcpServer and yield a connected TcpClient.

    The fixture generates a self-signed CA, issues a server certificate with
    SAN IP 127.0.0.1 (required because the client verifies the server hostname),
    and issues a client certificate for mTLS. The server is bound to port 0 so
    the OS assigns an ephemeral port; the actual port is retrieved from the
    listening socket after start() returns.

    Certificate generation is offloaded to a thread via asyncio.to_thread
    because the X509CertificateAuthority methods are synchronous and CPU-bound.

    The server is stopped in the finally block so it is torn down even if a
    test raises, preventing port leaks between test runs.
    """
    pki = X509CertificateAuthority()

    ca_cert = tmp_path / "ca.pem"
    ca_key = tmp_path / "ca-key.pem"
    await asyncio.to_thread(
        pki.generate_ca,
        CaRequest(
            common_name="Tourillon E2E CA",
            valid_days=1,
            key_size=2048,
            out_cert=ca_cert,
            out_key=ca_key,
        ),
    )

    srv_cert = tmp_path / "srv.pem"
    srv_key = tmp_path / "srv-key.pem"
    await asyncio.to_thread(
        pki.issue_cert,
        CertRequest(
            common_name="127.0.0.1",
            san_dns=(),
            san_ip=("127.0.0.1",),
            valid_days=1,
            ca_cert=ca_cert,
            ca_key=ca_key,
            out_cert=srv_cert,
            out_key=srv_key,
            key_size=2048,
        ),
    )

    cli_cert = tmp_path / "cli.pem"
    cli_key = tmp_path / "cli-key.pem"
    await asyncio.to_thread(
        pki.issue_cert,
        CertRequest(
            common_name="e2e-client",
            san_dns=(),
            san_ip=(),
            valid_days=1,
            ca_cert=ca_cert,
            ca_key=ca_key,
            out_cert=cli_cert,
            out_key=cli_key,
            key_size=2048,
        ),
    )

    server: TcpServer = await create_tcp_node(
        "node-e2e", "127.0.0.1", 0, srv_cert, srv_key, ca_cert
    )
    await server.start()

    # Retrieve the ephemeral port assigned by the OS after the socket is bound.
    assert server._server is not None
    port: int = server._server.sockets[0].getsockname()[1]

    client_ssl = build_ssl_context(cli_cert, cli_key, ca_cert, server_side=False)
    serializer = MsgPackSerializer()
    client = TcpClient("127.0.0.1", port, client_ssl, serializer, timeout=10.0)

    try:
        yield _LiveNode(client=client, serializer=serializer)
    finally:
        await server.stop()


def _put_envelope(
    serializer: MsgPackSerializer,
    ks: bytes,
    key: bytes,
    value: bytes,
    *,
    now_ms: int | None = None,
) -> Envelope:
    """Build a kv.put request Envelope."""
    payload: dict[str, bytes | int] = {"keyspace": ks, "key": key, "value": value}
    if now_ms is not None:
        payload["now_ms"] = now_ms
    return Envelope.create(serializer.encode(payload), kind="kv.put")


def _get_envelope(serializer: MsgPackSerializer, ks: bytes, key: bytes) -> Envelope:
    """Build a kv.get request Envelope."""
    return Envelope.create(
        serializer.encode({"keyspace": ks, "key": key}), kind="kv.get"
    )


def _delete_envelope(serializer: MsgPackSerializer, ks: bytes, key: bytes) -> Envelope:
    """Build a kv.delete request Envelope."""
    return Envelope.create(
        serializer.encode({"keyspace": ks, "key": key}), kind="kv.delete"
    )


async def test_kv_put_and_get_returns_stored_value(live_node: _LiveNode) -> None:
    """Put a key then get it back; the response must carry the original value."""
    client, serializer = live_node
    ks, key, value = b"ns", b"greeting", b"hello world"

    put_resp = await client.request(_put_envelope(serializer, ks, key, value))

    assert put_resp.kind == KIND_KV_PUT_OK
    meta = serializer.decode(put_resp.payload)
    assert isinstance(meta["wall"], int)
    assert isinstance(meta["counter"], int)
    assert isinstance(meta["node_id"], str)

    get_resp = await client.request(_get_envelope(serializer, ks, key))

    assert get_resp.kind == KIND_KV_GET_OK
    versions = serializer.decode(get_resp.payload)["versions"]
    assert len(versions) == 1
    assert versions[0]["value"] == value


async def test_kv_get_missing_key_returns_empty_versions(live_node: _LiveNode) -> None:
    """Get a key that was never written; the server must return an empty versions list."""
    client, serializer = live_node

    get_resp = await client.request(_get_envelope(serializer, b"ns", b"absent"))

    assert get_resp.kind == KIND_KV_GET_OK
    assert serializer.decode(get_resp.payload)["versions"] == []


async def test_kv_delete_then_get_returns_empty_versions(live_node: _LiveNode) -> None:
    """Put a key, delete it, then get it; the store must return an empty versions list.

    MemoryStore applies last-write-wins on HLCTimestamp order: the Tombstone
    recorded by delete carries a timestamp greater than the preceding write, so
    get returns an empty list rather than the stale value.
    """
    client, serializer = live_node
    ks, key = b"ns", b"ephemeral"

    await client.request(_put_envelope(serializer, ks, key, b"transient"))

    del_resp = await client.request(_delete_envelope(serializer, ks, key))
    assert del_resp.kind == KIND_KV_DELETE_OK

    get_resp = await client.request(_get_envelope(serializer, ks, key))
    assert get_resp.kind == KIND_KV_GET_OK
    assert serializer.decode(get_resp.payload)["versions"] == []


async def test_kv_put_twice_same_key_returns_latest_value(live_node: _LiveNode) -> None:
    """Two successive puts on the same key must converge to the latest write.

    The second put carries a higher now_ms hint, guaranteeing that its
    HLCTimestamp is strictly greater than the first. MemoryStore's
    last-write-wins selection must therefore return the second value.
    """
    client, serializer = live_node
    ks, key = b"ns", b"counter"

    await client.request(_put_envelope(serializer, ks, key, b"v1", now_ms=1_000))
    await client.request(_put_envelope(serializer, ks, key, b"v2", now_ms=2_000))

    get_resp = await client.request(_get_envelope(serializer, ks, key))
    assert get_resp.kind == KIND_KV_GET_OK
    versions = serializer.decode(get_resp.payload)["versions"]
    assert len(versions) == 1
    assert versions[0]["value"] == b"v2"


async def test_kv_independent_keys_do_not_interfere(live_node: _LiveNode) -> None:
    """Writes to distinct keys in the same keyspace must not affect each other."""
    client, serializer = live_node
    ks = b"ns"

    await client.request(_put_envelope(serializer, ks, b"key-a", b"alpha"))
    await client.request(_put_envelope(serializer, ks, b"key-b", b"beta"))

    resp_a = await client.request(_get_envelope(serializer, ks, b"key-a"))
    resp_b = await client.request(_get_envelope(serializer, ks, b"key-b"))

    assert serializer.decode(resp_a.payload)["versions"][0]["value"] == b"alpha"
    assert serializer.decode(resp_b.payload)["versions"][0]["value"] == b"beta"
