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
"""In-memory implementation of LocalStoragePort."""

from tourillon.core.ports.storage import DeleteOp, ReadOp, WriteOp
from tourillon.core.structure.clock import HLCClock
from tourillon.core.structure.version import Tombstone, Version
from tourillon.infra.memory.log import MemoryLog


class MemoryStore:
    """In-memory adapter that satisfies LocalStoragePort.

    MemoryStore composes a MemoryLog for event recording and an HLCClock for
    deterministic timestamp generation. Every write operation first advances
    the clock to obtain a causally ordered timestamp, then records the
    resulting Version or Tombstone in the log. Reads reconstruct the current
    visible state by scanning the log entries for the requested address and
    applying last-write-wins semantics: the entry whose metadata is greatest
    according to the HLC total order wins, regardless of insertion order.

    This last-write-wins resolution is deterministic across all replicas
    because HLCTimestamp defines a strict total order that includes the
    originating node_id as a tiebreaker. Two replicas that have applied the
    same set of events will always converge to the same visible value for
    every address.

    MemoryStore holds no persistence guarantee. It is the reference adapter
    for Milestone 1, intended to validate the ordering model and serve as the
    baseline against which future persistent adapters are tested.
    """

    def __init__(self, node_id: str) -> None:
        """Initialise the store for the given node with an empty log and clock."""
        self._clock: HLCClock = HLCClock(node_id)
        self._log: MemoryLog = MemoryLog()

    async def put(self, op: WriteOp) -> Version:
        """Record a new value and return the stamped Version.

        The clock is ticked with op.now_ms before the Version is created, so
        the returned timestamp is always strictly greater than every previous
        timestamp issued by this node.
        """
        ts = self._clock.tick(op.now_ms)
        version = Version(address=op.address, metadata=ts, value=op.value)
        await self._log.append(version)
        return version

    async def get(self, op: ReadOp) -> list[Version]:
        """Return the current visible value, or an empty list if absent.

        All log entries for the address are retrieved and the one with the
        greatest HLC metadata is selected as the winner. If the winning entry
        is a Tombstone the address is considered deleted and an empty list is
        returned. The list contains at most one element when the winning entry
        is a Version.
        """
        entries = await self._log.entries_for_address(op.address)
        if not entries:
            return []
        winner = max(entries, key=lambda le: le.entry.metadata)
        if isinstance(winner.entry, Tombstone):
            return []
        return [winner.entry]

    async def delete(self, op: DeleteOp) -> Tombstone:
        """Record a deletion and return the stamped Tombstone.

        The clock is ticked before the Tombstone is created, guaranteeing that
        the deletion causally follows every prior write issued by this node.
        """
        ts = self._clock.tick(op.now_ms)
        tombstone = Tombstone(address=op.address, metadata=ts)
        await self._log.append(tombstone)
        return tombstone
