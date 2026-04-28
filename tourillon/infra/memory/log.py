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
"""In-memory implementation of LogPort."""

from tourillon.core.ports.log import LogEntry
from tourillon.core.structure.version import Tombstone, Version

_Identity: type = tuple[str, int, int, str]


def _identity(entry: Version | Tombstone) -> _Identity:
    """Derive the deduplication key for a log entry.

    The identity of an entry is fully determined by its key and the three
    components of its HLC metadata. Two entries that share the same identity
    are semantically identical regardless of the Python object that carries
    them, which is the basis for the idempotency guarantee of MemoryLog.append.
    """
    m = entry.metadata
    return entry.key, m.wall, m.counter, m.node_id


class MemoryLog:
    """In-memory adapter that satisfies LogPort.

    MemoryLog stores all entries in a plain Python list protected by a set of
    seen identities for O(1) duplicate detection. It is intentionally simple:
    there is no persistence, no compaction, and no concurrent-access protection.
    All durability and concurrency guarantees are deferred to the backend that
    will replace this adapter in a future milestone.

    Because the underlying data structures are not thread-safe, a MemoryLog
    instance must never be shared between concurrent coroutines without
    external synchronisation. In practice the store that owns the log is the
    only writer, and asyncio's single-threaded cooperative model ensures that
    no two coroutines can mutate the log simultaneously within a single event
    loop iteration.
    """

    def __init__(self) -> None:
        """Initialise an empty log with no entries."""
        self._entries: list[LogEntry] = []
        self._seen: set[_Identity] = set()

    @property
    def size(self) -> int:
        """Return the number of entries currently held in the log."""
        return len(self._entries)

    async def append(self, entry: Version | Tombstone) -> LogEntry | None:
        """Persist a new entry and return its LogEntry, or None if already seen.

        Duplicate detection is performed against the full entry identity before
        any mutation occurs, so the method is safe to call speculatively — for
        example during a recovery replay — without risking double-counting.
        """
        identity = _identity(entry)
        if identity in self._seen:
            return None
        self._seen.add(identity)
        log_entry = LogEntry(sequence=len(self._entries), entry=entry)
        self._entries.append(log_entry)
        return log_entry

    async def entries_for_key(self, key: str) -> list[LogEntry]:
        """Return all log entries whose key matches, in insertion order."""
        return [le for le in self._entries if le.entry.key == key]
