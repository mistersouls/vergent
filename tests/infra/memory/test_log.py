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
"""Tests for tourillon.infra.memory.log — MemoryLog idempotent append behaviour."""

from tourillon.core.ports.log import LogEntry
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.version import Tombstone, Version
from tourillon.infra.memory.log import MemoryLog


def _ts(wall: int, counter: int = 0, node: str = "n") -> HLCTimestamp:
    return HLCTimestamp(wall=wall, counter=counter, node_id=node)


def _version(key: str = "k", wall: int = 1, value: bytes = b"v") -> Version:
    return Version(key=key, metadata=_ts(wall), value=value)


def _tombstone(key: str = "k", wall: int = 2) -> Tombstone:
    return Tombstone(key=key, metadata=_ts(wall))


async def test_memorylog_append_version_increases_size() -> None:
    """Appending a new Version must increase log size by one."""
    log = MemoryLog()
    await log.append(_version())
    assert log.size == 1


async def test_memorylog_append_tombstone_increases_size() -> None:
    """Appending a new Tombstone must increase log size by one."""
    log = MemoryLog()
    await log.append(_tombstone())
    assert log.size == 1


async def test_memorylog_append_returns_log_entry_with_correct_sequence() -> None:
    """append must return a LogEntry whose sequence starts at zero."""
    log = MemoryLog()
    entry = await log.append(_version(wall=10))
    assert isinstance(entry, LogEntry)
    assert entry.sequence == 0


async def test_memorylog_append_sequence_increments() -> None:
    """Sequence numbers must be strictly increasing across appends."""
    log = MemoryLog()
    e0 = await log.append(_version(wall=1))
    e1 = await log.append(_version(wall=2))
    assert e0 is not None
    assert e1 is not None
    assert e1.sequence == e0.sequence + 1


async def test_memorylog_append_idempotent_returns_none_on_duplicate() -> None:
    """Appending the same entry twice must return None on the second call."""
    log = MemoryLog()
    v = _version()
    first = await log.append(v)
    second = await log.append(v)
    assert first is not None
    assert second is None
    assert log.size == 1


async def test_memorylog_append_idempotent_does_not_increase_size() -> None:
    """Duplicate append must not increase the log size."""
    log = MemoryLog()
    v = _version()
    await log.append(v)
    await log.append(v)
    assert log.size == 1


async def test_memorylog_append_different_metadata_is_not_duplicate() -> None:
    """Two entries with the same key but different metadata are distinct."""
    log = MemoryLog()
    await log.append(_version(key="k", wall=1))
    await log.append(_version(key="k", wall=2))
    assert log.size == 2


async def test_memorylog_entries_for_key_returns_only_matching_key() -> None:
    """entries_for_key must return only entries whose key matches."""
    log = MemoryLog()
    await log.append(_version(key="a", wall=1))
    await log.append(_version(key="b", wall=2))
    await log.append(_version(key="a", wall=3))
    result = await log.entries_for_key("a")
    assert len(result) == 2
    assert all(e.entry.key == "a" for e in result)


async def test_memorylog_entries_for_key_includes_tombstones() -> None:
    """entries_for_key must include Tombstone entries for the key."""
    log = MemoryLog()
    await log.append(_version(key="x", wall=1))
    await log.append(_tombstone(key="x", wall=2))
    result = await log.entries_for_key("x")
    assert len(result) == 2


async def test_memorylog_entries_for_key_empty_for_unknown_key() -> None:
    """entries_for_key must return an empty list for a key never written."""
    log = MemoryLog()
    await log.append(_version(key="a"))
    assert await log.entries_for_key("z") == []


async def test_memorylog_size_is_zero_on_empty_log() -> None:
    """A freshly created log must report size zero."""
    assert MemoryLog().size == 0
