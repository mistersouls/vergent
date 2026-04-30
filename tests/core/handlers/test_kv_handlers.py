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
"""Tests for KvHandlers — handle_put, handle_get, handle_delete."""

from unittest.mock import AsyncMock

import pytest

from tourillon.bootstrap.handlers import (
    KIND_KV_DELETE_OK,
    KIND_KV_ERR,
    KIND_KV_GET_OK,
    KIND_KV_PUT_OK,
    KvHandlers,
)
from tourillon.core.structure.envelope import Envelope
from tourillon.infra.memory.store import MemoryStore
from tourillon.infra.msgpack.serializer import MsgPackSerializer


def _make_handler() -> tuple[KvHandlers, MemoryStore, MsgPackSerializer]:
    store = MemoryStore(node_id="test-node")
    serializer = MsgPackSerializer()
    return KvHandlers(store, serializer), store, serializer


@pytest.mark.asyncio
async def test_handle_put_sends_put_ok_response() -> None:
    handler, _, serializer = _make_handler()
    payload = serializer.encode({"keyspace": b"ks", "key": b"k1", "value": b"v1"})
    envelope = Envelope.create(payload, kind="kv.put")
    send = AsyncMock()
    await handler.handle_put(envelope, send)
    send.assert_called_once()
    response: Envelope = send.call_args[0][0]
    assert response.kind == KIND_KV_PUT_OK
    data = serializer.decode(response.payload)
    assert "wall" in data and "counter" in data and "node_id" in data


@pytest.mark.asyncio
async def test_handle_get_sends_get_ok_with_version_after_put() -> None:
    handler, store, serializer = _make_handler()
    from tourillon.core.ports.storage import WriteOp
    from tourillon.core.structure.version import StoreKey

    await store.put(WriteOp(address=StoreKey(b"ks", b"k1"), value=b"v1", now_ms=1000))
    payload = serializer.encode({"keyspace": b"ks", "key": b"k1"})
    envelope = Envelope.create(payload, kind="kv.get")
    send = AsyncMock()
    await handler.handle_get(envelope, send)
    send.assert_called_once()
    response: Envelope = send.call_args[0][0]
    assert response.kind == KIND_KV_GET_OK
    data = serializer.decode(response.payload)
    assert len(data["versions"]) == 1
    assert data["versions"][0]["value"] == b"v1"


@pytest.mark.asyncio
async def test_handle_get_sends_empty_versions_for_unknown_key() -> None:
    handler, _, serializer = _make_handler()
    payload = serializer.encode({"keyspace": b"ks", "key": b"missing"})
    envelope = Envelope.create(payload, kind="kv.get")
    send = AsyncMock()
    await handler.handle_get(envelope, send)
    send.assert_called_once()
    response: Envelope = send.call_args[0][0]
    assert response.kind == KIND_KV_GET_OK
    data = serializer.decode(response.payload)
    assert data["versions"] == []


@pytest.mark.asyncio
async def test_handle_delete_sends_delete_ok() -> None:
    handler, _, serializer = _make_handler()
    payload = serializer.encode({"keyspace": b"ks", "key": b"k1"})
    envelope = Envelope.create(payload, kind="kv.delete")
    send = AsyncMock()
    await handler.handle_delete(envelope, send)
    send.assert_called_once()
    response: Envelope = send.call_args[0][0]
    assert response.kind == KIND_KV_DELETE_OK
    data = serializer.decode(response.payload)
    assert "wall" in data and "counter" in data


@pytest.mark.asyncio
async def test_handle_put_sends_err_on_missing_key_field() -> None:
    handler, _, serializer = _make_handler()
    # payload missing "key" field → should trigger except and send kv.err
    payload = serializer.encode({"keyspace": b"ks", "value": b"v1"})
    envelope = Envelope.create(payload, kind="kv.put")
    send = AsyncMock()
    await handler.handle_put(envelope, send)
    send.assert_called_once()
    response: Envelope = send.call_args[0][0]
    assert response.kind == KIND_KV_ERR


@pytest.mark.asyncio
async def test_handle_put_uses_injected_now_ms_provider() -> None:
    """KvHandlers uses the injected now_ms_provider for missing now_ms fields."""
    store = MemoryStore(node_id="test-node")
    serializer = MsgPackSerializer()
    captured: list[int] = []

    def provider() -> int:
        captured.append(42000)
        return 42000

    handler = KvHandlers(store, serializer, now_ms_provider=provider)
    payload = serializer.encode({"keyspace": b"ks", "key": b"prov", "value": b"v"})
    envelope = Envelope.create(payload, kind="kv.put")
    send = AsyncMock()
    await handler.handle_put(envelope, send)
    assert len(captured) == 1
    assert captured[0] == 42000


@pytest.mark.asyncio
async def test_handle_delete_uses_injected_now_ms_provider() -> None:
    """KvHandlers uses the injected now_ms_provider for missing now_ms on delete."""
    store = MemoryStore(node_id="test-node")
    serializer = MsgPackSerializer()
    captured: list[int] = []

    def provider() -> int:
        captured.append(99000)
        return 99000

    handler = KvHandlers(store, serializer, now_ms_provider=provider)
    payload = serializer.encode({"keyspace": b"ks", "key": b"d1"})
    envelope = Envelope.create(payload, kind="kv.delete")
    send = AsyncMock()
    await handler.handle_delete(envelope, send)
    assert len(captured) == 1
    assert captured[0] == 99000
