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
"""Driven port for the append-only local log and its entry value object."""

from dataclasses import dataclass
from typing import Protocol

from tourillon.core.structure.version import StoreKey, Tombstone, Version


@dataclass(frozen=True)
class LogEntry:
    """An immutable wrapper that pairs a log record with its position in the log.

    sequence is the zero-based insertion index assigned by the log at the moment
    the entry was accepted. It is stable for the lifetime of the log and grows
    monotonically with each new entry, giving consumers a reliable ordering
    handle that is independent of HLC timestamps. The entry field holds either
    a Version or a Tombstone, reflecting the fact that the log treats both
    writes and deletions as first-class events so that a store can reconstruct
    its full causal history from the log alone.
    """

    sequence: int
    entry: Version | Tombstone


class LogPort(Protocol):
    """Driven port that the core uses to durably record and query its local event log.

    The log is the source of truth for every state transition on a node. All
    writes and deletions pass through it before they affect the visible store
    state, so that any crash or restart can be recovered by re-reading the log
    from the beginning. Implementations are free to choose any storage backend
    provided they honour the two invariants below.

    The first invariant is monotonic sequencing: every accepted entry receives
    a unique, strictly increasing sequence number starting at zero. The second
    is idempotency: submitting the same Version or Tombstone a second time,
    identified by its address and full HLC metadata, must be a no-op that
    returns None without altering the log, so that callers can safely
    re-submit entries during recovery without producing duplicates.
    """

    @property
    def size(self) -> int:
        """Return the number of entries currently held in the log."""
        ...

    async def append(self, entry: Version | Tombstone) -> LogEntry | None:
        """Persist a new entry and return its LogEntry, or return None if already seen.

        The entry is considered a duplicate when another entry with the same
        address and identical HLC metadata already exists in the log. Callers
        must treat a None return as a confirmed no-op rather than an error.
        """
        ...

    async def entries_for_address(self, address: StoreKey) -> list[LogEntry]:
        """Return all log entries whose address matches, in insertion order."""
        ...
