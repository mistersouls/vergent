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
"""Driven port for the hinted handoff store."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from tourillon.core.structure.clock import HLCTimestamp
    from tourillon.core.structure.handoff import Hint


@runtime_checkable
class HandoffPort(Protocol):
    """Driven port through which deferred writes are buffered for absent peers.

    HandoffPort is the boundary the replication path uses whenever a write
    must be retained on behalf of a replica that is currently unreachable.
    Each hint is appended against the identifier of the peer that should
    eventually receive it, and the store guarantees that pending hints for
    a given target are observed in strict HLC timestamp order. This ordering
    is essential for hinted handoff correctness: when the target reappears
    and replays its backlog, applying the hints in order reproduces the
    exact causal sequence that would have been observed had the writes been
    delivered live.

    All methods are asynchronous because realistic backends — durable
    queues, embedded databases, or remote services — perform I/O. The
    in-memory adapter used for tests adopts the same async signature so
    that production code can swap implementations without modification.
    Implementations must serialise concurrent operations on the same target
    so that ``append`` and ``remove`` never observe a partially mutated
    state for that target.
    """

    async def append(self, hint: Hint) -> None:
        """Persist a new hint for its target replica.

        The hint is inserted into the per-target backlog at the position
        determined by its HLC timestamp, so that subsequent calls to
        :meth:`pending` always observe a strictly ordered sequence.
        """
        ...

    async def pending(self, target_node_id: str) -> list[Hint]:
        """Return the ordered backlog of hints awaiting delivery to a target.

        The returned list is a snapshot copy ordered by ascending HLC
        timestamp. Mutating the result must not affect the underlying
        store, allowing callers to iterate without holding any lock.
        """
        ...

    async def remove(self, target_node_id: str, ts: HLCTimestamp) -> None:
        """Drop the hint identified by its target and HLC timestamp.

        The call is a no-op when no hint matches, which makes replay paths
        idempotent: a peer that acknowledges the same hint twice does not
        cause an error.
        """
        ...

    async def targets(self) -> frozenset[str]:
        """Return the set of target node identifiers with pending hints.

        Only targets whose backlog is currently non-empty are included,
        allowing the handoff scheduler to enumerate work without filtering
        out empty queues.
        """
        ...
