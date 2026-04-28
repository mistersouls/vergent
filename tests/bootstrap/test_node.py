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

from pathlib import Path

import pytest

from tourillon.bootstrap import create_memory_node, create_tcp_node
from tourillon.core.dispatch import Dispatcher
from tourillon.core.net.tcp.server import TcpServer
from tourillon.core.ports.storage import ReadOp, WriteOp
from tourillon.core.structure.version import StoreKey


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

    # monkeypatch SSLContext methods so build_ssl_context succeeds without real certs
    import ssl

    def _noop_load_cert_chain(self, certfile, keyfile):
        return None

    def _noop_load_verify_locations(self, cafile=None, capath=None, cadata=None):
        return None

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

    import ssl

    def _noop_load_cert_chain(self, certfile, keyfile):
        return None

    def _noop_load_verify_locations(self, cafile=None, capath=None, cadata=None):
        return None

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

    import ssl

    def _noop_load_cert_chain(self, certfile, keyfile):
        return None

    def _noop_load_verify_locations(self, cafile=None, capath=None, cadata=None):
        return None

    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", _noop_load_cert_chain)
    monkeypatch.setattr(
        ssl.SSLContext, "load_verify_locations", _noop_load_verify_locations
    )

    server = await create_tcp_node(
        "node-1", "127.0.0.1", 0, str(cert), str(key), str(ca)
    )
    assert isinstance(server._handler, Dispatcher)
