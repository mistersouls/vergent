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
"""Tests for the HintedHandoffQueue domain coordinator."""

import asyncio

import pytest

from tourillon.core.ports.storage import WriteOp
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.handoff import Hint, HintedHandoffQueue
from tourillon.core.structure.version import StoreKey
from tourillon.infra.memory.handoff import InMemoryHandoffStore


def _ts(wall: int, node: str = "n1") -> HLCTimestamp:
    return HLCTimestamp(wall=wall, counter=0, node_id=node)


def _op() -> WriteOp:
    return WriteOp(address=StoreKey(keyspace=b"ks", key=b"k"), value=b"v", now_ms=1)


def _queue() -> HintedHandoffQueue:
    return HintedHandoffQueue(InMemoryHandoffStore())


async def test_enqueue_then_pending_for_returns_hint() -> None:
    q = _queue()
    await q.enqueue("n1", _ts(100), _op(), "put")
    pending = await q.pending_for("n1")
    assert len(pending) == 1
    assert pending[0].ts == _ts(100)


async def test_pending_for_returns_hints_sorted_by_ts_ascending() -> None:
    q = _queue()
    await q.enqueue("n1", _ts(300), _op(), "put")
    await q.enqueue("n1", _ts(100), _op(), "put")
    await q.enqueue("n1", _ts(200), _op(), "put")
    pending = await q.pending_for("n1")
    assert [h.ts for h in pending] == [_ts(100), _ts(200), _ts(300)]


async def test_replay_all_delivered_removes_all_hints() -> None:
    q = _queue()
    for w in (100, 200, 300):
        await q.enqueue("n1", _ts(w), _op(), "put")

    async def push_one(_: Hint) -> bool:
        return True

    delivered = await q.replay("n1", push_one)
    assert delivered == 3
    assert await q.pending_for("n1") == []


async def test_replay_stops_on_push_failure() -> None:
    q = _queue()
    for w in (1, 2, 3):
        await q.enqueue("n1", _ts(w), _op(), "put")

    async def push_one(h: Hint) -> bool:
        return h.ts.wall == 1

    delivered = await q.replay("n1", push_one)
    assert delivered == 1
    pending = await q.pending_for("n1")
    assert [h.ts.wall for h in pending] == [2, 3]


async def test_replay_returns_zero_when_no_hints() -> None:
    q = _queue()

    async def push_one(_: Hint) -> bool:
        return True

    assert await q.replay("n1", push_one) == 0


async def test_replay_removes_hint_only_after_ack() -> None:
    q = _queue()
    for w in (1, 2, 3):
        await q.enqueue("n1", _ts(w), _op(), "put")

    async def push_one(h: Hint) -> bool:
        return h.ts.wall == 1

    await q.replay("n1", push_one)
    pending = await q.pending_for("n1")
    assert [h.ts.wall for h in pending] == [2, 3]


async def test_concurrent_replay_same_target_serialised() -> None:
    q = _queue()
    for w in (1, 2, 3, 4, 5):
        await q.enqueue("n1", _ts(w), _op(), "put")

    seen: list[int] = []

    async def push_one(h: Hint) -> bool:
        # Yield control to encourage interleaving if lock missing.
        await asyncio.sleep(0)
        seen.append(h.ts.wall)
        return True

    results = await asyncio.gather(
        q.replay("n1", push_one),
        q.replay("n1", push_one),
    )
    assert sum(results) == 5
    assert sorted(seen) == [1, 2, 3, 4, 5]
    assert await q.pending_for("n1") == []


async def test_concurrent_replay_distinct_targets_parallel() -> None:
    q = _queue()
    await q.enqueue("n1", _ts(1, "n1"), _op(), "put")
    await q.enqueue("n2", _ts(2, "n2"), _op(), "put")

    async def push_one(_: Hint) -> bool:
        return True

    r1, r2 = await asyncio.gather(
        q.replay("n1", push_one),
        q.replay("n2", push_one),
    )
    assert r1 == 1
    assert r2 == 1
    assert await q.known_targets() == frozenset()


async def test_known_targets_reflects_enqueued_hints() -> None:
    q = _queue()
    await q.enqueue("n1", _ts(1, "n1"), _op(), "put")
    await q.enqueue("n2", _ts(2, "n2"), _op(), "put")
    assert await q.known_targets() == frozenset({"n1", "n2"})


async def test_known_targets_empty_after_full_replay() -> None:
    q = _queue()
    await q.enqueue("n1", _ts(1), _op(), "put")

    async def push_one(_: Hint) -> bool:
        return True

    await q.replay("n1", push_one)
    assert await q.known_targets() == frozenset()


async def test_enqueue_invalid_kind_raises() -> None:
    q = _queue()
    with pytest.raises(ValueError):
        await q.enqueue("n1", _ts(1), _op(), "update")


async def test_enqueue_empty_target_raises() -> None:
    q = _queue()
    with pytest.raises(ValueError):
        await q.enqueue("", _ts(1), _op(), "put")
