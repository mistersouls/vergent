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
"""Tests for bootstrap node assembly factories."""

import base64
import ssl
from pathlib import Path

import pytest

from tourillon.bootstrap import create_memory_node, create_tcp_node
from tourillon.bootstrap.node import create_tcp_node_from_config
from tourillon.core.config import (
    ClusterSection,
    ServerEndpointConfig,
    TlsSection,
    TourillonConfig,
)
from tourillon.core.dispatch import Dispatcher
from tourillon.core.net.tcp.server import TcpServer
from tourillon.core.ports.storage import ReadOp, WriteOp
from tourillon.core.structure.version import StoreKey


def _noop_load_cert_chain(self, certfile, keyfile) -> None:  # type: ignore[override]
    return None


def _noop_load_verify_locations(  # type: ignore[override]
    self, cafile=None, capath=None, cadata=None
) -> None:
    return None


@pytest.mark.asyncio
async def test_create_memory_node_returns_storage_port() -> None:
    node = create_memory_node("node-1")

    # duck-type check for the LocalStoragePort protocol
    assert hasattr(node, "put") and callable(node.put)
    assert hasattr(node, "get") and callable(node.get)
    assert hasattr(node, "delete") and callable(node.delete)


@pytest.mark.asyncio
async def test_create_memory_node_different_ids_are_independent() -> None:
    node1 = create_memory_node("node-1")
    node2 = create_memory_node("node-2")

    key = StoreKey(keyspace=b"ks", key=b"k")

    await node1.put(WriteOp(address=key, value=b"v", now_ms=1))

    res = await node2.get(ReadOp(address=key))
    assert res == []


@pytest.mark.asyncio
async def test_create_tcp_node_returns_tcp_server(tmp_path: Path, monkeypatch) -> None:
    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    ca = tmp_path / "ca.pem"
    cert.write_bytes(b"dummy")
    key.write_bytes(b"dummy")
    ca.write_bytes(b"dummy")

    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", _noop_load_cert_chain)
    monkeypatch.setattr(
        ssl.SSLContext, "load_verify_locations", _noop_load_verify_locations
    )

    server = await create_tcp_node(
        "node-1", "127.0.0.1", 0, str(cert), str(key), str(ca)
    )
    assert isinstance(server, TcpServer)


@pytest.mark.asyncio
async def test_create_tcp_node_uses_provided_dispatcher(
    tmp_path: Path, monkeypatch
) -> None:
    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    ca = tmp_path / "ca.pem"
    cert.write_bytes(b"dummy")
    key.write_bytes(b"dummy")
    ca.write_bytes(b"dummy")

    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", _noop_load_cert_chain)
    monkeypatch.setattr(
        ssl.SSLContext, "load_verify_locations", _noop_load_verify_locations
    )

    dispatcher = Dispatcher()
    server = await create_tcp_node(
        "node-1", "127.0.0.1", 0, str(cert), str(key), str(ca), dispatcher=dispatcher
    )
    assert server._handler is dispatcher


@pytest.mark.asyncio
async def test_create_tcp_node_creates_dispatcher_when_none(
    tmp_path: Path, monkeypatch
) -> None:
    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    ca = tmp_path / "ca.pem"
    cert.write_bytes(b"dummy")
    key.write_bytes(b"dummy")
    ca.write_bytes(b"dummy")

    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", _noop_load_cert_chain)
    monkeypatch.setattr(
        ssl.SSLContext, "load_verify_locations", _noop_load_verify_locations
    )

    server = await create_tcp_node(
        "node-1", "127.0.0.1", 0, str(cert), str(key), str(ca)
    )
    assert isinstance(server._handler, Dispatcher)


def _make_config(bind: str = "0.0.0.0:7000") -> TourillonConfig:
    """Build a minimal TourillonConfig with placeholder base64 TLS data."""
    return TourillonConfig(
        node_id="test-node",
        data_dir="/tmp/data",
        log_level="info",
        tls=TlsSection(
            cert_data=base64.b64encode(b"cert").decode(),
            key_data=base64.b64encode(b"key").decode(),
            ca_data=base64.b64encode(b"ca").decode(),
        ),
        servers_kv=ServerEndpointConfig(bind=bind, advertise=bind),
        servers_peer=None,
        cluster=ClusterSection(seeds=(), replication_factor=3),
    )


@pytest.mark.asyncio
async def test_create_tcp_node_from_config_returns_tcp_server(
    monkeypatch,
) -> None:
    """create_tcp_node_from_config wires a TcpServer from a TourillonConfig."""
    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", _noop_load_cert_chain)
    monkeypatch.setattr(
        ssl.SSLContext, "load_verify_locations", _noop_load_verify_locations
    )

    cfg = _make_config()
    server = await create_tcp_node_from_config(cfg)
    assert isinstance(server, TcpServer)


@pytest.mark.asyncio
async def test_create_tcp_node_from_config_uses_kv_bind_port(
    monkeypatch,
) -> None:
    """create_tcp_node_from_config extracts host:port from servers_kv.bind."""
    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", _noop_load_cert_chain)
    monkeypatch.setattr(
        ssl.SSLContext, "load_verify_locations", _noop_load_verify_locations
    )

    cfg = _make_config(bind="127.0.0.1:9999")
    server = await create_tcp_node_from_config(cfg)
    # _port is the internal attribute on TcpServer
    assert server._port == 9999


@pytest.mark.asyncio
async def test_create_tcp_node_from_config_accepts_dispatcher(
    monkeypatch,
) -> None:
    """create_tcp_node_from_config uses a provided Dispatcher."""
    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", _noop_load_cert_chain)
    monkeypatch.setattr(
        ssl.SSLContext, "load_verify_locations", _noop_load_verify_locations
    )

    cfg = _make_config()
    dispatcher = Dispatcher()
    server = await create_tcp_node_from_config(cfg, dispatcher=dispatcher)
    assert server._handler is dispatcher
