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
"""Tests for tourillon.infra.memory.handoff — InMemoryHandoffStore behaviour."""

import asyncio

from tourillon.core.ports.handoff import HandoffPort
from tourillon.core.ports.storage import WriteOp
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.handoff import Hint
from tourillon.core.structure.version import StoreKey
from tourillon.infra.memory.handoff import InMemoryHandoffStore


def _key() -> StoreKey:
    return StoreKey(keyspace=b"ks", key=b"k")


def _ts(wall: int, counter: int = 0, node: str = "n") -> HLCTimestamp:
    return HLCTimestamp(wall=wall, counter=counter, node_id=node)


def _hint(target: str, wall: int) -> Hint:
    return Hint(
        target_node_id=target,
        ts=_ts(wall),
        op=WriteOp(address=_key(), value=b"v", now_ms=wall),
        kind="put",
    )


async def test_append_then_pending_returns_sorted_by_ts() -> None:
    store = InMemoryHandoffStore()
    h3 = _hint("peer", 3)
    h1 = _hint("peer", 1)
    h2 = _hint("peer", 2)
    await store.append(h3)
    await store.append(h1)
    await store.append(h2)
    pending = await store.pending("peer")
    assert [h.ts.wall for h in pending] == [1, 2, 3]


async def test_remove_existing_hint() -> None:
    store = InMemoryHandoffStore()
    h1 = _hint("peer", 1)
    h2 = _hint("peer", 2)
    await store.append(h1)
    await store.append(h2)
    await store.remove("peer", h1.ts)
    pending = await store.pending("peer")
    assert [h.ts.wall for h in pending] == [2]


async def test_remove_absent_hint_is_noop() -> None:
    store = InMemoryHandoffStore()
    h1 = _hint("peer", 1)
    await store.append(h1)
    await store.remove("peer", _ts(99))
    await store.remove("ghost", _ts(1))
    pending = await store.pending("peer")
    assert [h.ts.wall for h in pending] == [1]


async def test_targets_reflects_pending_hints() -> None:
    store = InMemoryHandoffStore()
    await store.append(_hint("peer-a", 1))
    await store.append(_hint("peer-b", 2))
    assert await store.targets() == frozenset({"peer-a", "peer-b"})


async def test_targets_empty_after_all_removed() -> None:
    store = InMemoryHandoffStore()
    h = _hint("peer", 1)
    await store.append(h)
    await store.remove("peer", h.ts)
    assert await store.targets() == frozenset()


def test_isinstance_handoff_port() -> None:
    assert isinstance(InMemoryHandoffStore(), HandoffPort)


async def test_pending_returns_copy_not_reference() -> None:
    store = InMemoryHandoffStore()
    await store.append(_hint("peer", 1))
    pending = await store.pending("peer")
    pending.clear()
    again = await store.pending("peer")
    assert len(again) == 1


async def test_concurrent_append_for_same_target_is_safe() -> None:
    store = InMemoryHandoffStore()
    walls = list(range(20))
    await asyncio.gather(*(store.append(_hint("peer", w)) for w in walls))
    pending = await store.pending("peer")
    assert [h.ts.wall for h in pending] == walls
