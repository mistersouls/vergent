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
"""Tests for ReplicaCoordinator fan-out and hint delegation behaviour."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from tourillon.core.ports.replication import QuorumNotReachedError
from tourillon.core.ports.storage import DeleteOp, WriteOp
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.replication import (
    ReplicaAck,
    ReplicaCoordinator,
)
from tourillon.core.structure.version import StoreKey, Tombstone, Version


def _ts(wall: int = 1000, counter: int = 0, node: str = "coord") -> HLCTimestamp:
    return HLCTimestamp(wall=wall, counter=counter, node_id=node)


def _addr() -> StoreKey:
    return StoreKey(keyspace=b"ks", key=b"k")


def _version(ts: HLCTimestamp | None = None) -> Version:
    return Version(address=_addr(), metadata=ts or _ts(), value=b"v")


def _tombstone(ts: HLCTimestamp | None = None) -> Tombstone:
    return Tombstone(address=_addr(), metadata=ts or _ts())


def _make_coordinator(
    local_node_id: str = "coord",
    write_quorum: int = 2,
    replicate_one: AsyncMock | None = None,
    delegate_hint: AsyncMock | None = None,
    max_in_flight: int = 10,
    request_budget_ms: int = 500,
) -> tuple[ReplicaCoordinator, AsyncMock, AsyncMock]:
    async def _ok_replica(target_node_id: str, request):
        return ReplicaAck(node_id=target_node_id, ts=request.ts)

    if replicate_one is None:
        replicate_one = AsyncMock(side_effect=_ok_replica)
    if delegate_hint is None:
        delegate_hint = AsyncMock()
    coord = ReplicaCoordinator(
        local_node_id=local_node_id,
        replicate_one=replicate_one,
        delegate_hint=delegate_hint,
        write_quorum=write_quorum,
        max_in_flight=max_in_flight,
        request_budget_ms=request_budget_ms,
    )
    return coord, replicate_one, delegate_hint


@pytest.mark.asyncio
async def test_replicate_write_coordinator_in_pl_quorum_success_returns_version() -> (
    None
):
    coord, replicate_one, delegate_hint = _make_coordinator(
        local_node_id="coord", write_quorum=2
    )
    version = _version()
    op = WriteOp(address=_addr(), value=b"v", now_ms=version.metadata.wall)
    out = await coord.replicate_write(op, version, ["coord", "n1", "n2"])
    assert out is version
    # replicate_one called for non-local nodes only
    assert replicate_one.call_count == 2
    delegate_hint.assert_not_called()


@pytest.mark.asyncio
async def test_replicate_write_coordinator_not_in_pl_fans_out_to_all() -> None:
    coord, replicate_one, delegate_hint = _make_coordinator(
        local_node_id="coord-proxy", write_quorum=2
    )
    version = _version()
    op = WriteOp(address=_addr(), value=b"v", now_ms=version.metadata.wall)
    out = await coord.replicate_write(op, version, ["n1", "n2", "n3"])
    assert out is version
    # coordinator not in pl -> replicate_one should be invoked for all three
    assert replicate_one.call_count == 3


@pytest.mark.asyncio
async def test_replicate_write_quorum_not_reached_raises() -> None:
    async def _down(*_args):
        raise Exception("offline")

    replicate_one = AsyncMock(side_effect=_down)
    coord, _, delegate_hint = _make_coordinator(
        local_node_id="coord-proxy", write_quorum=2, replicate_one=replicate_one
    )
    version = _version()
    op = WriteOp(address=_addr(), value=b"v", now_ms=version.metadata.wall)
    with pytest.raises(QuorumNotReachedError):
        await coord.replicate_write(op, version, ["n1", "n2", "n3"])
    delegate_hint.assert_not_called()


@pytest.mark.asyncio
async def test_replicate_write_non_acking_replica_hint_delegated_to_keeper() -> None:
    async def _side(target: str, request):
        if target == "n3":
            raise Exception("offline")
        return ReplicaAck(node_id=target, ts=request.ts)

    replicate_one = AsyncMock(side_effect=_side)
    delegate_hint = AsyncMock()
    coord = ReplicaCoordinator(
        local_node_id="n1",
        replicate_one=replicate_one,
        delegate_hint=delegate_hint,
        write_quorum=2,
        max_in_flight=10,
        request_budget_ms=500,
    )
    version = _version()
    op = WriteOp(address=_addr(), value=b"v", now_ms=version.metadata.wall)
    out = await coord.replicate_write(op, version, ["n1", "n2", "n3"])
    assert out is version
    # delegate_hint should be called once for the non-acking target n3
    delegate_hint.assert_called_once()
    keeper, target, request = delegate_hint.call_args[0]
    assert target == "n3"
    assert keeper == "n1"
    assert request.ts == version.metadata


@pytest.mark.asyncio
async def test_replicate_write_no_hint_delegation_when_quorum_not_reached() -> None:
    async def _down(*_args):
        raise Exception("offline")

    replicate_one = AsyncMock(side_effect=_down)
    delegate_hint = AsyncMock()
    coord = ReplicaCoordinator(
        local_node_id="n1",
        replicate_one=replicate_one,
        delegate_hint=delegate_hint,
        write_quorum=2,
        max_in_flight=10,
        request_budget_ms=500,
    )
    version = _version()
    op = WriteOp(address=_addr(), value=b"v", now_ms=version.metadata.wall)
    with pytest.raises(QuorumNotReachedError):
        await coord.replicate_write(op, version, ["n1", "n2", "n3"])
    delegate_hint.assert_not_called()


@pytest.mark.asyncio
async def test_replicate_write_w1_local_ack_sufficient_returns_version() -> None:
    coord, replicate_one, delegate_hint = _make_coordinator(
        local_node_id="coord", write_quorum=1
    )
    version = _version()
    op = WriteOp(address=_addr(), value=b"v", now_ms=version.metadata.wall)
    out = await coord.replicate_write(op, version, ["coord", "n1", "n2"])
    assert out is version


@pytest.mark.asyncio
async def test_replicate_delete_quorum_success_returns_tombstone() -> None:
    coord, replicate_one, delegate_hint = _make_coordinator(local_node_id="coord")
    tomb = _tombstone()
    op = DeleteOp(address=_addr(), now_ms=tomb.metadata.wall)
    out = await coord.replicate_delete(op, tomb, ["coord", "n1", "n2"])
    assert out is tomb


@pytest.mark.asyncio
async def test_replicate_delete_quorum_not_reached_raises() -> None:
    async def _down(*_args):
        raise Exception("offline")

    replicate_one = AsyncMock(side_effect=_down)
    coord = ReplicaCoordinator(
        local_node_id="coord-proxy",
        replicate_one=replicate_one,
        delegate_hint=AsyncMock(),
        write_quorum=2,
        max_in_flight=10,
        request_budget_ms=500,
    )
    tomb = _tombstone()
    op = DeleteOp(address=_addr(), now_ms=tomb.metadata.wall)
    with pytest.raises(QuorumNotReachedError):
        await coord.replicate_delete(op, tomb, ["n1", "n2", "n3"])


@pytest.mark.asyncio
async def test_replicate_write_timeout_causes_quorum_not_reached() -> None:
    async def _slow(target: str, request):
        await asyncio.sleep(0.1)
        return ReplicaAck(node_id=target, ts=request.ts)

    replicate_one = AsyncMock(side_effect=_slow)
    coord = ReplicaCoordinator(
        local_node_id="coord-proxy",
        replicate_one=replicate_one,
        delegate_hint=AsyncMock(),
        write_quorum=2,
        max_in_flight=10,
        request_budget_ms=10,
    )
    version = _version()
    op = WriteOp(address=_addr(), value=b"v", now_ms=version.metadata.wall)
    with pytest.raises(QuorumNotReachedError):
        await coord.replicate_write(op, version, ["n1", "n2", "n3"])
