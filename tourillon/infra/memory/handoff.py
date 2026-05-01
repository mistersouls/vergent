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
"""In-memory adapter implementing :class:`HandoffPort` for tests and bring-up."""

import asyncio
import bisect

from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.handoff import Hint


class InMemoryHandoffStore:
    """Volatile, process-local backlog of pending hints per target replica.

    The store keeps one ordered list of hints per target node identifier.
    Hints are inserted using a binary search keyed on their HLC timestamp,
    so that the backlog remains sorted at all times and queries observe a
    strict causal sequence without an additional sort step. A dedicated
    :class:`asyncio.Lock` is created lazily for each target the first time
    it is referenced; this serialises concurrent ``append`` and ``remove``
    calls for the same target while still allowing operations on different
    targets to proceed in parallel.

    The adapter is intentionally non-durable: when the process exits, the
    backlog is lost. It exists to support unit tests and early bring-up,
    not as a production-grade handoff substrate.
    """

    def __init__(self) -> None:
        """Initialise an empty store with no targets registered."""
        self._hints: dict[str, list[Hint]] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock_for(self, target_node_id: str) -> asyncio.Lock:
        """Return the lock guarding the backlog of a given target.

        The lock is created on first access. Because lock creation occurs
        outside of any ``await`` point it is safe under asyncio's
        single-threaded cooperative scheduling.
        """
        lock = self._locks.get(target_node_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[target_node_id] = lock
        return lock

    async def append(self, hint: Hint) -> None:
        """Insert a hint at the position dictated by its HLC timestamp."""
        async with self._lock_for(hint.target_node_id):
            backlog = self._hints.setdefault(hint.target_node_id, [])
            bisect.insort(backlog, hint, key=lambda h: h.ts)

    async def pending(self, target_node_id: str) -> list[Hint]:
        """Return a sorted copy of the backlog for the given target."""
        async with self._lock_for(target_node_id):
            return list(self._hints.get(target_node_id, ()))

    async def remove(self, target_node_id: str, ts: HLCTimestamp) -> None:
        """Drop the hint matching the given timestamp; no-op when absent."""
        async with self._lock_for(target_node_id):
            backlog = self._hints.get(target_node_id)
            if backlog is None:
                return
            for index, hint in enumerate(backlog):
                if hint.ts == ts:
                    del backlog[index]
                    return

    async def targets(self) -> frozenset[str]:
        """Return the set of targets whose backlog is currently non-empty."""
        return frozenset(t for t, hints in self._hints.items() if hints)
