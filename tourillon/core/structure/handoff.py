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
"""Hinted handoff data structures shared by core and infrastructure."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from tourillon.core.ports.handoff import HandoffPort
from tourillon.core.ports.storage import DeleteOp, WriteOp
from tourillon.core.structure.clock import HLCTimestamp


@dataclass(frozen=True)
class Hint:
    """Immutable record describing a write deferred for an unreachable peer.

    A Hint captures the original write or delete operation that could not be
    delivered to its intended replica, together with the HLC timestamp that
    was assigned when the operation was first accepted locally. Preserving
    the original timestamp is what allows hinted handoff to remain causally
    correct: when the target node eventually comes back online and the hint
    is replayed, the receiving store sees the exact same ordering metadata
    that would have been observed had the write succeeded immediately.

    The kind discriminator records whether the deferred operation was a put
    or a delete so that the replay path can dispatch to the correct method
    on LocalStoragePort without inspecting the operation payload itself.
    Only the literal strings ``"put"`` and ``"delete"`` are accepted, and the
    target node identifier must be a non-empty string so that hints are
    always associated with a well-defined recipient.
    """

    target_node_id: str
    ts: HLCTimestamp
    op: WriteOp | DeleteOp
    kind: str

    def __post_init__(self) -> None:
        """Validate the structural invariants of the hint at construction time."""
        if not self.target_node_id:
            raise ValueError("target_node_id must be a non-empty string")
        if self.kind not in ("put", "delete"):
            raise ValueError("kind must be either 'put' or 'delete'")
        if not isinstance(self.ts, HLCTimestamp):
            raise TypeError("ts must be an HLCTimestamp instance")


PushOne = Callable[[Hint], Awaitable[bool]]


class HintedHandoffQueue:
    """Ordered queue of pending hints per target replica.

    HintedHandoffQueue is the domain coordinator that sits between the
    ReplicaCoordinator (which enqueues hints when a replica does not ack)
    and the HandoffPort (which durably stores those hints). It provides:

    - enqueue: persist a new Hint via HandoffPort.append, preserving the
      original HLCTimestamp of the deferred write.
    - pending_for: snapshot of all hints for a target, sorted ascending by
      HLCTimestamp.
    - replay: deliver hints to a recovered target in strict HLC ascending
      order, removing each hint from the store only after the push_one
      callable confirms delivery (returns True). If push_one returns False
      for a hint, replay stops immediately, leaving that hint and all
      subsequent hints in place for the next replay attempt.
    - known_targets: pass-through to HandoffPort.targets().

    Per-target replay is serialised by an asyncio.Lock to prevent two
    concurrent replay coroutines for the same target from interleaving
    deliveries and breaking HLC ordering. Concurrent replays for distinct
    targets proceed in parallel.
    """

    def __init__(self, store: HandoffPort) -> None:
        """Bind the persistent store and initialise the per-target lock registry."""
        self._store: HandoffPort = store
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock_for(self, target_node_id: str) -> asyncio.Lock:
        """Return (creating if absent) the asyncio.Lock for target_node_id."""
        if target_node_id not in self._locks:
            self._locks[target_node_id] = asyncio.Lock()
        return self._locks[target_node_id]

    async def enqueue(
        self,
        target_node_id: str,
        ts: HLCTimestamp,
        op: WriteOp | DeleteOp,
        kind: str,
    ) -> None:
        """Build a Hint and persist it via HandoffPort.append.

        Precondition: ts is the original HLCTimestamp of the deferred write,
        never a freshly generated one. Constructing Hint validates
        target_node_id and kind, raising ValueError / TypeError on invalid
        input.
        """
        hint = Hint(target_node_id=target_node_id, ts=ts, op=op, kind=kind)
        await self._store.append(hint)

    async def pending_for(self, target_node_id: str) -> list[Hint]:
        """Return a snapshot of hints for target_node_id sorted ascending by ts."""
        return await self._store.pending(target_node_id)

    async def replay(self, target_node_id: str, push_one: PushOne) -> int:
        """Replay every pending hint for target_node_id in HLC ascending order.

        For each hint h in ascending HLCTimestamp order:
            ok = await push_one(h)
            if ok: await self._store.remove(target_node_id, h.ts)
            else:  break

        The replay is serialised per target via asyncio.Lock so that
        concurrent callers for the same target wait rather than interleave.

        Return the number of hints successfully delivered.
        """
        async with self._lock_for(target_node_id):
            hints = await self._store.pending(target_node_id)
            delivered = 0
            for hint in hints:
                ok = await push_one(hint)
                if not ok:
                    break
                await self._store.remove(target_node_id, hint.ts)
                delivered += 1
            return delivered

    async def known_targets(self) -> frozenset[str]:
        """Return the set of node_ids that currently have at least one pending hint."""
        return await self._store.targets()
