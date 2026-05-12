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
"""Storage, PartitionStore, PartitionStaging — hexagonal storage port protocols.

The core domain depends only on these Protocol interfaces. Storage-engine
specifics (transaction semantics, cursor positioning, named data spaces) are
confined to tourillon/infra/store/ and never leak into core/.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

from tourillon.core.structure.record import Record


class PartitionStaging(Protocol):
    """Rebalance staging context scoped to one (pid, epoch) pair.

    stage() writes records to an invisible staging area, tagged with the
    current epoch so they remain hidden from read operations. commit()
    atomically promotes all staged entries to the committed (visible) state
    in a single durable write transaction. cleanup() removes all staging
    entries for this (pid, epoch) without promoting them, used on cancellation
    or when the applicator detects that the staging epoch is stale.

    Callers must ensure the pid appears in state.toml staging_pids BEFORE
    the first stage() call (invariant §2). commit() must be called BEFORE
    state.toml is updated to move the pid to committed_pids (invariant §1).
    """

    async def stage(self, record: Record) -> None:
        """Write one record to the staging area for this (pid, epoch)."""

    async def commit(self) -> None:
        """Atomically promote all staged entries to committed visibility.

        Must be called before updating the node state file (storage-first
        invariant §1): a crash between the storage commit and the state
        update is recoverable on restart; the reverse order is not.
        """

    async def cleanup(self) -> None:
        """Delete all staging entries for this (pid, epoch).

        Called on cancellation (superseded plan) or on startup when the
        stored epoch is older than the gossip epoch (stale staging entries).
        Does not affect any committed records in the partition.
        """

    async def exists(self) -> bool:
        """Return True if staging entries exist for this (pid, epoch).

        Used during crash recovery to distinguish "transfer incomplete —
        restart from cursor" from "storage committed but state file not yet
        updated — auto-heal on the committed path".
        """

    async def last_staged_index_key(self) -> bytes | None:
        """Return the cursor key of the highest-HLC staging entry.

        Layout: pid(4B BE) | hlc(12B) | keyspace | key.
        Pass verbatim as resume_from to scan() to position the cursor
        strictly after the last staged record, yielding only the delta.
        Returns None when no staging entries exist for this (pid, epoch).
        """


class PartitionStore(Protocol):
    """Per-partition handle for scan and staging access.

    pid is bound once at open_partition(); PartitionStore itself is
    pid-scoped. KV read/write operations (get/put/delete) will be added in
    a future KV proposal.
    """

    def scan(self, resume_from: bytes | None = None) -> AsyncIterator[Record]:
        """Yield all committed records in HLC order for this partition.

        When resume_from is None, yields all committed entries from the
        beginning of the partition. When resume_from is a cursor key
        (as returned by last_staged_index_key()), positions the read cursor
        strictly after that key, yielding only records not yet seen by the
        caller. Never re-sends the record at the cursor position.
        """

    def staging(self, epoch: int) -> PartitionStaging:
        """Return a PartitionStaging context scoped to (self.pid, epoch)."""


class Storage(Protocol):
    """Factory for per-partition storage handles.

    The pid is supplied once at open_partition(); all subsequent operations
    on the returned PartitionStore are scoped to that pid.
    """

    def open_partition(self, pid: int) -> PartitionStore:
        """Return the PartitionStore for *pid*."""
