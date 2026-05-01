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
"""Tests for HandoffHandlers behaviour around push and delegate."""

from unittest.mock import AsyncMock

import pytest

from tourillon.core.handlers.handoff import (
    KIND_HANDOFF_ACK,
    KIND_HANDOFF_DELEGATE,
    KIND_HANDOFF_DELEGATE_OK,
    KIND_HANDOFF_PUSH,
    KIND_KV_ERR,
    HandoffHandlers,
)
from tourillon.core.ports.storage import ReadOp
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.handoff import HintedHandoffQueue
from tourillon.core.structure.version import StoreKey
from tourillon.infra.memory.handoff import InMemoryHandoffStore
from tourillon.infra.memory.store import MemoryStore
from tourillon.infra.msgpack.serializer import MsgPackSerializer


def _make_handler():
    store = MemoryStore(node_id="keeper")
    serializer = MsgPackSerializer()
    queue = HintedHandoffQueue(InMemoryHandoffStore())
    return HandoffHandlers(store, serializer, queue), store, serializer, queue


def _push_envelope(
    serializer, op_kind="put", wall=2000, counter=1, node_id="coord", value=b"val"
):
    payload = {
        "op_kind": op_kind,
        "keyspace": b"ks",
        "key": b"k2",
        "wall": wall,
        "counter": counter,
        "node_id": node_id,
    }
    if op_kind == "put":
        payload["value"] = value
    return Envelope.create(serializer.encode(payload), kind=KIND_HANDOFF_PUSH)


def _delegate_envelope(
    serializer,
    target_node_id="n3",
    op_kind="put",
    wall=3000,
    counter=0,
    node_id="coord",
    value=b"hint-val",
):
    payload = {
        "op_kind": op_kind,
        "keyspace": b"ks2",
        "key": b"k3",
        "wall": wall,
        "counter": counter,
        "node_id": node_id,
        "target_node_id": target_node_id,
    }
    if op_kind == "put":
        payload["value"] = value
    return Envelope.create(serializer.encode(payload), kind=KIND_HANDOFF_DELEGATE)


@pytest.mark.asyncio
async def test_handle_push_put_applies_version_and_responds_ack() -> None:
    handler, store, serializer, queue = _make_handler()
    send = AsyncMock()
    env = _push_envelope(
        serializer, op_kind="put", wall=2000, counter=1, node_id="coord", value=b"val"
    )
    await handler.handle_push(env, send)
    send.assert_called_once()
    response = send.call_args[0][0]
    assert response.kind == KIND_HANDOFF_ACK
    data = serializer.decode(response.payload)
    assert data["wall"] == 2000
    # stored value visible
    result = await store.get(ReadOp(address=StoreKey(keyspace=b"ks", key=b"k2")))
    assert result[0].value == b"val"


@pytest.mark.asyncio
async def test_handle_push_delete_applies_tombstone_and_responds_ack() -> None:
    handler, store, serializer, queue = _make_handler()
    send = AsyncMock()
    env = _push_envelope(
        serializer, op_kind="delete", wall=2100, counter=2, node_id="coord"
    )
    await handler.handle_push(env, send)
    send.assert_called_once()
    response = send.call_args[0][0]
    assert response.kind == KIND_HANDOFF_ACK
    # deleted address returns empty
    result = await store.get(ReadOp(address=StoreKey(keyspace=b"ks", key=b"k2")))
    assert result == []


@pytest.mark.asyncio
async def test_handle_push_unknown_op_kind_responds_kv_err() -> None:
    handler, store, serializer, queue = _make_handler()
    send = AsyncMock()
    payload = {
        "op_kind": "update",
        "keyspace": b"ks",
        "key": b"k2",
        "wall": 1,
        "counter": 0,
        "node_id": "n",
    }
    env = Envelope.create(serializer.encode(payload), kind=KIND_HANDOFF_PUSH)
    await handler.handle_push(env, send)
    send.assert_called_once()
    response = send.call_args[0][0]
    assert response.kind == KIND_KV_ERR


@pytest.mark.asyncio
async def test_handle_ack_unexpected_logs_warning_no_response() -> None:
    handler, store, serializer, queue = _make_handler()
    send = AsyncMock()
    env = Envelope.create(b"{}", kind=KIND_HANDOFF_ACK)
    await handler.handle_ack(env, send)
    send.assert_not_called()


@pytest.mark.asyncio
async def test_handle_delegate_enqueues_hint_and_responds_delegate_ok() -> None:
    handler, store, serializer, queue = _make_handler()
    send = AsyncMock()
    env = _delegate_envelope(
        serializer,
        target_node_id="n3",
        op_kind="put",
        wall=3000,
        counter=0,
        node_id="coord",
        value=b"hint-val",
    )
    await handler.handle_delegate(env, send)
    send.assert_called_once()
    response = send.call_args[0][0]
    assert response.kind == KIND_HANDOFF_DELEGATE_OK
    data = serializer.decode(response.payload)
    assert data["wall"] == 3000
    # queue should have one pending hint for n3
    pending = await queue.pending_for("n3")
    assert len(pending) == 1
    assert pending[0].ts.wall == 3000


@pytest.mark.asyncio
async def test_handle_delegate_delete_enqueues_hint_and_responds_delegate_ok() -> None:
    handler, store, serializer, queue = _make_handler()
    send = AsyncMock()
    env = _delegate_envelope(
        serializer,
        target_node_id="n4",
        op_kind="delete",
        wall=4000,
        counter=1,
        node_id="coord",
    )
    await handler.handle_delegate(env, send)
    send.assert_called_once()
    response = send.call_args[0][0]
    assert response.kind == KIND_HANDOFF_DELEGATE_OK
    pending = await queue.pending_for("n4")
    assert len(pending) == 1
    assert pending[0].kind == "delete"


@pytest.mark.asyncio
async def test_handle_delegate_ok_unexpected_no_response() -> None:
    handler, store, serializer, queue = _make_handler()
    send = AsyncMock()
    env = Envelope.create(b"{}", kind=KIND_HANDOFF_DELEGATE_OK)
    await handler.handle_delegate_ok(env, send)
    send.assert_not_called()


@pytest.mark.asyncio
async def test_handle_delegate_missing_target_node_id_responds_kv_err() -> None:
    handler, store, serializer, queue = _make_handler()
    send = AsyncMock()
    payload = {
        "op_kind": "put",
        "keyspace": b"ks2",
        "key": b"k3",
        "wall": 1,
        "counter": 0,
        "node_id": "n",
        "value": b"v",
    }
    env = Envelope.create(serializer.encode(payload), kind=KIND_HANDOFF_DELEGATE)
    await handler.handle_delegate(env, send)
    send.assert_called_once()
    response = send.call_args[0][0]
    assert response.kind == KIND_KV_ERR
