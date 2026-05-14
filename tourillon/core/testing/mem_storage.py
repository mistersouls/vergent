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
"""In-memory implementations of Storage, PartitionStore, and PartitionStaging.

These classes stand in for the real storage backend in unit tests. They
implement the same port protocols with full in-memory semantics: staging
records are stored in a list and are invisible to scan() until commit()
promotes them, mirroring the staging-visibility invariant of the real adapter.

No durable transactions or cursor engines are involved; persistence is limited
to the lifetime of the object.
"""

from __future__ import annotations

import struct
from collections.abc import AsyncIterator

from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.record import Record


class InMemoryPartitionStaging:
    """In-memory PartitionStaging for one (pid, epoch) pair.

    Staged records are kept in an append-only list. commit() is a no-op
    beyond clearing the staging list (the real adapter would flip visibility
    tags in a write transaction). cleanup() removes all staged records.
    """

    def __init__(self, pid: int, epoch: int) -> None:
        self._pid = pid
        self._epoch = epoch
        self._staged: list[Record] = []

    async def stage(self, record: Record) -> None:
        """Append record to the in-memory staging list."""
        self._staged.append(record)

    async def commit(self) -> None:
        """Promote all staged records: clear the staging list (no-op here)."""

    async def cleanup(self) -> None:
        """Discard all staged records for this (pid, epoch)."""
        self._staged.clear()

    async def exists(self) -> bool:
        """Return True when at least one staged record is present."""
        return bool(self._staged)

    async def last_staged_index_key(self) -> bytes | None:
        """Return a synthetic cursor key for the highest-HLC staged record.

        Layout matches the port contract: pid(4B BE) | wall_ms(8B BE) |
        counter(2B BE) | node_id_prefix(2B) | keyspace | key. Callers may
        pass this verbatim as resume_from to scan().

        Returns None when no records are staged.
        """
        if not self._staged:
            return None
        last = self._staged[-1]
        hlc = last.metadata
        node_id_bytes = hlc.node_id.encode("utf-8")[:4].ljust(4, b"\x00")
        hlc_bytes = (
            struct.pack(">Q", hlc.wall_ms)
            + struct.pack(">H", hlc.counter)
            + node_id_bytes[:2]
        )
        return (
            struct.pack(">I", self._pid)
            + hlc_bytes
            + last.address.keyspace
            + last.address.key
        )

    def staged_records(self) -> list[Record]:
        """Return a snapshot copy of all staged records."""
        return list(self._staged)


class InMemoryPartitionStore:
    """In-memory PartitionStore for one pid.

    Committed records (added via add_record()) are always visible to scan().
    Staging contexts are keyed by epoch and returned by staging().
    """

    def __init__(self, pid: int) -> None:
        self._pid = pid
        self._records: list[Record] = []
        self._staging: dict[int, InMemoryPartitionStaging] = {}

    def add_record(self, record: Record) -> None:
        """Pre-populate committed records (test helper; no staging involved)."""
        self._records.append(record)

    async def scan(self, resume_from: bytes | None = None) -> AsyncIterator[Record]:
        """Yield committed records in HLC order.

        When resume_from is None, yields all records from the start.
        When resume_from is a cursor key (as returned by last_staged_index_key()),
        positions strictly after the cursor: the cursor record itself is never
        re-yielded. Only records with an HLC strictly greater than the cursor's
        (wall_ms, counter) pair are emitted.
        """
        sorted_records = sorted(self._records, key=_hlc_key)
        if resume_from is None:
            for rec in sorted_records:
                yield rec
            return

        cursor_wall, cursor_counter = _parse_cursor_hlc(resume_from)
        for rec in sorted_records:
            rwall, rcounter, _ = _hlc_key(rec)
            if (rwall, rcounter) > (cursor_wall, cursor_counter):
                yield rec

    def staging(self, epoch: int) -> InMemoryPartitionStaging:
        """Return or create the staging context for the given epoch."""
        if epoch not in self._staging:
            self._staging[epoch] = InMemoryPartitionStaging(self._pid, epoch)
        return self._staging[epoch]


class InMemoryStorage:
    """In-memory Storage: factory for per-partition partition stores."""

    def __init__(self) -> None:
        self._partitions: dict[int, InMemoryPartitionStore] = {}

    def open_partition(self, pid: int) -> InMemoryPartitionStore:
        """Return or create the PartitionStore for pid."""
        if pid not in self._partitions:
            self._partitions[pid] = InMemoryPartitionStore(pid)
        return self._partitions[pid]


def _hlc_key(rec: Record) -> tuple[int, int, str]:
    """Return a sortable (wall_ms, counter, node_id) tuple from a record."""
    hlc: HLCTimestamp = rec.metadata
    return hlc.wall_ms, hlc.counter, hlc.node_id


def _parse_cursor_hlc(cursor: bytes) -> tuple[int, int]:
    """Extract (wall_ms, counter) from a cursor bytes key.

    Cursor layout: pid(4B BE) | wall_ms(8B BE) | counter(2B BE) | …
    Returns (wall_ms, counter) for strict-greater-than comparison.
    Records whose (wall_ms, counter) is strictly greater than this value
    are considered to be after the cursor position.
    """
    wall_ms = struct.unpack(">Q", cursor[4:12])[0]
    counter = struct.unpack(">H", cursor[12:14])[0]
    return (wall_ms, counter)
