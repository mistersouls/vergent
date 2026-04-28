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
from tourillon.core.structure.version import StoreKey, Tombstone, Version
from tourillon.infra.memory.log import MemoryLog


def _ts(wall: int, counter: int = 0, node: str = "n") -> HLCTimestamp:
    return HLCTimestamp(wall=wall, counter=counter, node_id=node)


def _key(keyspace: bytes = b"ks", key: bytes = b"k") -> StoreKey:
    return StoreKey(keyspace=keyspace, key=key)


def _version(
    keyspace: bytes = b"ks", key: bytes = b"k", wall: int = 1, value: bytes = b"v"
) -> Version:
    return Version(address=_key(keyspace, key), metadata=_ts(wall), value=value)


def _tombstone(keyspace: bytes = b"ks", key: bytes = b"k", wall: int = 2) -> Tombstone:
    return Tombstone(address=_key(keyspace, key), metadata=_ts(wall))


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
    """Two entries with the same address but different metadata are distinct."""
    log = MemoryLog()
    await log.append(_version(wall=1))
    await log.append(_version(wall=2))
    assert log.size == 2


async def test_memorylog_entries_for_address_returns_only_matching_address() -> None:
    """entries_for_address must return only entries whose address matches."""
    log = MemoryLog()
    addr_a = _key(key=b"a")
    await log.append(_version(key=b"a", wall=1))
    await log.append(_version(key=b"b", wall=2))
    await log.append(_version(key=b"a", wall=3))
    result = await log.entries_for_address(addr_a)
    assert len(result) == 2
    assert all(e.entry.address == addr_a for e in result)


async def test_memorylog_entries_for_address_different_keyspace_not_returned() -> None:
    """entries_for_address must not return entries from a different keyspace."""
    log = MemoryLog()
    await log.append(_version(keyspace=b"ns1", key=b"k", wall=1))
    await log.append(_version(keyspace=b"ns2", key=b"k", wall=2))
    result = await log.entries_for_address(_key(keyspace=b"ns1", key=b"k"))
    assert len(result) == 1
    assert result[0].entry.address.keyspace == b"ns1"


async def test_memorylog_entries_for_address_includes_tombstones() -> None:
    """entries_for_address must include Tombstone entries for the address."""
    log = MemoryLog()
    addr = _key(key=b"x")
    await log.append(_version(key=b"x", wall=1))
    await log.append(_tombstone(key=b"x", wall=2))
    result = await log.entries_for_address(addr)
    assert len(result) == 2


async def test_memorylog_entries_for_address_empty_for_unknown_address() -> None:
    """entries_for_address must return an empty list for an address never written."""
    log = MemoryLog()
    await log.append(_version(key=b"a"))
    assert await log.entries_for_address(_key(key=b"z")) == []


async def test_memorylog_size_is_zero_on_empty_log() -> None:
    """A freshly created log must report size zero."""
    assert MemoryLog().size == 0
