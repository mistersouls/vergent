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
"""Tests for ReplicationHandlers server-side replica behaviour."""

from unittest.mock import AsyncMock

import pytest

from tourillon.core.handlers.replication import (
    KIND_KV_ERR,
    KIND_KV_REPLICATE,
    KIND_KV_REPLICATE_OK,
    ReplicationHandlers,
)
from tourillon.core.ports.storage import ReadOp
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.version import StoreKey
from tourillon.infra.memory.store import MemoryStore
from tourillon.infra.msgpack.serializer import MsgPackSerializer


def _make_handler():
    store = MemoryStore(node_id="n1")
    serializer = MsgPackSerializer()
    return ReplicationHandlers(store, serializer), store, serializer


def _replicate_envelope(
    serializer, op_kind="put", wall=1000, counter=0, node_id="coord", value=b"v1"
):
    payload = {
        "op_kind": op_kind,
        "keyspace": b"ks",
        "key": b"k1",
        "wall": wall,
        "counter": counter,
        "node_id": node_id,
    }
    if op_kind == "put":
        payload["value"] = value
    return Envelope.create(serializer.encode(payload), kind=KIND_KV_REPLICATE)


@pytest.mark.asyncio
async def test_handle_replicate_put_applies_version_and_responds_ok() -> None:
    handler, store, serializer = _make_handler()
    send = AsyncMock()
    env = _replicate_envelope(
        serializer, op_kind="put", wall=1000, counter=1, node_id="coord", value=b"v1"
    )
    await handler.handle_replicate(env, send)
    send.assert_called_once()
    response = send.call_args[0][0]
    assert response.kind == KIND_KV_REPLICATE_OK
    data = serializer.decode(response.payload)
    assert data["wall"] == 1000
    assert data["counter"] == 1
    assert data["node_id"] == "coord"
    # stored value visible
    result = await store.get(ReadOp(address=StoreKey(keyspace=b"ks", key=b"k1")))
    assert result[0].value == b"v1"


@pytest.mark.asyncio
async def test_handle_replicate_delete_applies_tombstone_and_responds_ok() -> None:
    handler, store, serializer = _make_handler()
    send = AsyncMock()
    env = _replicate_envelope(
        serializer, op_kind="delete", wall=2000, counter=2, node_id="coord"
    )
    await handler.handle_replicate(env, send)
    send.assert_called_once()
    response = send.call_args[0][0]
    assert response.kind == KIND_KV_REPLICATE_OK
    data = serializer.decode(response.payload)
    assert data["wall"] == 2000
    assert data["counter"] == 2
    assert data["node_id"] == "coord"
    # deleted address returns empty
    result = await store.get(ReadOp(address=StoreKey(keyspace=b"ks", key=b"k1")))
    assert result == []


@pytest.mark.asyncio
async def test_handle_replicate_idempotent_second_application() -> None:
    handler, store, serializer = _make_handler()
    send = AsyncMock()
    env = _replicate_envelope(
        serializer, op_kind="put", wall=3000, counter=3, node_id="coord", value=b"dup"
    )
    await handler.handle_replicate(env, send)
    await handler.handle_replicate(env, send)
    # both calls responded
    assert send.call_count == 2
    result = await store.get(ReadOp(address=StoreKey(keyspace=b"ks", key=b"k1")))
    assert len(result) == 1
    assert result[0].value == b"dup"


@pytest.mark.asyncio
async def test_handle_replicate_unknown_op_kind_responds_kv_err() -> None:
    handler, store, serializer = _make_handler()
    send = AsyncMock()
    payload = {
        "op_kind": "update",
        "keyspace": b"ks",
        "key": b"k1",
        "wall": 1,
        "counter": 0,
        "node_id": "n",
    }
    env = Envelope.create(serializer.encode(payload), kind=KIND_KV_REPLICATE)
    await handler.handle_replicate(env, send)
    send.assert_called_once()
    response = send.call_args[0][0]
    assert response.kind == KIND_KV_ERR


@pytest.mark.asyncio
async def test_handle_replicate_missing_field_responds_kv_err() -> None:
    handler, store, serializer = _make_handler()
    send = AsyncMock()
    payload = {
        "op_kind": "put",
        "key": b"k1",
        "wall": 1,
        "counter": 0,
        "node_id": "n",
        "value": b"v",
    }
    env = Envelope.create(serializer.encode(payload), kind=KIND_KV_REPLICATE)
    await handler.handle_replicate(env, send)
    send.assert_called_once()
    response = send.call_args[0][0]
    assert response.kind == KIND_KV_ERR


@pytest.mark.asyncio
async def test_handle_replicate_response_echoes_coordinator_ts() -> None:
    handler, store, serializer = _make_handler()
    send = AsyncMock()
    env = _replicate_envelope(
        serializer, op_kind="put", wall=9999, counter=7, node_id="far-node", value=b"x"
    )
    await handler.handle_replicate(env, send)
    response = send.call_args[0][0]
    data = serializer.decode(response.payload)
    assert data["wall"] == 9999
    assert data["counter"] == 7
    assert data["node_id"] == "far-node"
