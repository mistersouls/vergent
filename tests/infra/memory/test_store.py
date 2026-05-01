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
"""Tests for tourillon.infra.memory.store — MemoryStore deterministic versioned state."""

from tourillon.core.ports.storage import DeleteOp, ReadOp, WriteOp
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.version import StoreKey, Tombstone, Version
from tourillon.infra.memory.store import MemoryStore


def _key(keyspace: bytes = b"ks", key: bytes = b"k") -> StoreKey:
    return StoreKey(keyspace=keyspace, key=key)


async def test_memorystore_put_returns_version_with_correct_address_and_value() -> None:
    """put must return a Version with the supplied address and value."""
    store = MemoryStore("n1")
    addr = _key(key=b"foo")
    v = await store.put(WriteOp(address=addr, value=b"bar", now_ms=100))
    assert isinstance(v, Version)
    assert v.address == addr
    assert v.value == b"bar"


async def test_memorystore_put_assigns_correct_wall_timestamp() -> None:
    """put must assign a wall timestamp matching now_ms when no prior tick exists."""
    store = MemoryStore("n1")
    v = await store.put(WriteOp(address=_key(), value=b"v", now_ms=500))
    assert v.metadata.wall == 500


async def test_memorystore_get_returns_latest_version() -> None:
    """get must return the version written last for an address."""
    store = MemoryStore("n1")
    addr = _key()
    await store.put(WriteOp(address=addr, value=b"first", now_ms=1))
    v2 = await store.put(WriteOp(address=addr, value=b"second", now_ms=2))
    result = await store.get(ReadOp(address=addr))
    assert len(result) == 1
    assert result[0].value == b"second"
    assert result[0].metadata == v2.metadata


async def test_memorystore_get_returns_empty_for_unknown_address() -> None:
    """get must return an empty list for an address that has never been written."""
    store = MemoryStore("n1")
    assert await store.get(ReadOp(address=_key(key=b"missing"))) == []


async def test_memorystore_put_binary_safe_value() -> None:
    """put must store and return arbitrary binary payloads unchanged."""
    payload = bytes(range(256))
    store = MemoryStore("n1")
    addr = _key(key=b"bin")
    await store.put(WriteOp(address=addr, value=payload, now_ms=1))
    assert (await store.get(ReadOp(address=addr)))[0].value == payload


async def test_memorystore_put_empty_value() -> None:
    """put must accept an empty byte string as a valid value."""
    store = MemoryStore("n1")
    addr = _key(key=b"empty")
    await store.put(WriteOp(address=addr, value=b"", now_ms=1))
    assert (await store.get(ReadOp(address=addr)))[0].value == b""


async def test_memorystore_put_multiple_addresses_independently() -> None:
    """put on different addresses must not interfere with each other."""
    store = MemoryStore("n1")
    addr_a = _key(key=b"a")
    addr_b = _key(key=b"b")
    await store.put(WriteOp(address=addr_a, value=b"A", now_ms=1))
    await store.put(WriteOp(address=addr_b, value=b"B", now_ms=2))
    assert (await store.get(ReadOp(address=addr_a)))[0].value == b"A"
    assert (await store.get(ReadOp(address=addr_b)))[0].value == b"B"


async def test_memorystore_put_different_keyspaces_independently() -> None:
    """Same key in different keyspaces must be stored independently."""
    store = MemoryStore("n1")
    addr1 = _key(keyspace=b"ns1", key=b"k")
    addr2 = _key(keyspace=b"ns2", key=b"k")
    await store.put(WriteOp(address=addr1, value=b"v1", now_ms=1))
    await store.put(WriteOp(address=addr2, value=b"v2", now_ms=2))
    assert (await store.get(ReadOp(address=addr1)))[0].value == b"v1"
    assert (await store.get(ReadOp(address=addr2)))[0].value == b"v2"


async def test_memorystore_timestamps_are_monotonically_increasing() -> None:
    """Each successive put must produce a strictly increasing timestamp."""
    store = MemoryStore("n1")
    addr = _key()
    v1 = await store.put(WriteOp(address=addr, value=b"1", now_ms=100))
    v2 = await store.put(WriteOp(address=addr, value=b"2", now_ms=100))
    v3 = await store.put(WriteOp(address=addr, value=b"3", now_ms=200))
    assert v2.metadata > v1.metadata
    assert v3.metadata > v2.metadata


async def test_memorystore_delete_returns_tombstone() -> None:
    """delete must return a Tombstone with the correct address."""
    store = MemoryStore("n1")
    addr = _key()
    await store.put(WriteOp(address=addr, value=b"v", now_ms=1))
    t = await store.delete(DeleteOp(address=addr, now_ms=2))
    assert isinstance(t, Tombstone)
    assert t.address == addr


async def test_memorystore_delete_hides_existing_versions() -> None:
    """get must return an empty list after the address is deleted."""
    store = MemoryStore("n1")
    addr = _key()
    await store.put(WriteOp(address=addr, value=b"v", now_ms=1))
    await store.delete(DeleteOp(address=addr, now_ms=2))
    assert await store.get(ReadOp(address=addr)) == []


async def test_memorystore_put_after_delete_is_visible() -> None:
    """A put whose timestamp is after a tombstone must be visible via get."""
    store = MemoryStore("n1")
    addr = _key()
    await store.put(WriteOp(address=addr, value=b"old", now_ms=1))
    await store.delete(DeleteOp(address=addr, now_ms=2))
    v = await store.put(WriteOp(address=addr, value=b"new", now_ms=3))
    result = await store.get(ReadOp(address=addr))
    assert len(result) == 1
    assert result[0].value == b"new"
    assert result[0].metadata == v.metadata


async def test_memorystore_delete_nonexistent_address_is_safe() -> None:
    """delete on an address that was never written must not raise."""
    store = MemoryStore("n1")
    addr = _key(key=b"ghost")
    t = await store.delete(DeleteOp(address=addr, now_ms=1))
    assert t.address == addr
    assert await store.get(ReadOp(address=addr)) == []


async def test_memorystore_delete_twice_keeps_address_invisible() -> None:
    """A second delete with a later timestamp must keep the address invisible."""
    store = MemoryStore("n1")
    addr = _key()
    await store.put(WriteOp(address=addr, value=b"v", now_ms=1))
    await store.delete(DeleteOp(address=addr, now_ms=2))
    await store.put(WriteOp(address=addr, value=b"revived", now_ms=3))
    await store.delete(DeleteOp(address=addr, now_ms=4))
    assert await store.get(ReadOp(address=addr)) == []


async def test_apply_version_stores_value_retrievable_by_get() -> None:
    store = MemoryStore("n1")
    version = Version(
        address=StoreKey(keyspace=b"ks", key=b"k"),
        metadata=HLCTimestamp(wall=5000, counter=0, node_id="remote"),
        value=b"rep-val",
    )
    await store.apply_version(version)
    result = await store.get(ReadOp(address=version.address))
    assert len(result) == 1
    assert result[0].value == b"rep-val"


async def test_apply_tombstone_marks_address_deleted() -> None:
    store = MemoryStore("n1")
    addr = StoreKey(keyspace=b"ks", key=b"kdel")
    await store.put(WriteOp(address=addr, value=b"v", now_ms=1))
    tomb = Tombstone(
        address=addr, metadata=HLCTimestamp(wall=9999, counter=0, node_id="remote")
    )
    await store.apply_tombstone(tomb)
    assert await store.get(ReadOp(address=addr)) == []


async def test_apply_version_advances_hlc_clock() -> None:
    store = MemoryStore("n1")
    version = Version(
        address=StoreKey(keyspace=b"ks", key=b"kclk"),
        metadata=HLCTimestamp(wall=99999, counter=0, node_id="remote"),
        value=b"v",
    )
    await store.apply_version(version)
    # subsequent local put with now_ms=0 must produce a wall >= 99999
    v = await store.put(WriteOp(address=version.address, value=b"later", now_ms=0))
    assert v.metadata.wall >= 99999


async def test_apply_version_idempotent_second_application() -> None:
    store = MemoryStore("n1")
    version = Version(
        address=StoreKey(keyspace=b"ks", key=b"kidemp"),
        metadata=HLCTimestamp(wall=6000, counter=0, node_id="remote"),
        value=b"val",
    )
    await store.apply_version(version)
    await store.apply_version(version)
    result = await store.get(ReadOp(address=version.address))
    assert len(result) == 1
    assert result[0].value == b"val"


async def test_apply_version_lww_remote_beats_local() -> None:
    store = MemoryStore("n1")
    addr = StoreKey(keyspace=b"ks", key=b"kcmp")
    # local put at ts 100
    await store.put(WriteOp(address=addr, value=b"local", now_ms=100))
    # remote version with higher wall should win
    remote = Version(
        address=addr,
        metadata=HLCTimestamp(wall=200, counter=0, node_id="remote"),
        value=b"remote",
    )
    await store.apply_version(remote)
    result = await store.get(ReadOp(address=addr))
    assert result[0].value == b"remote"


async def test_apply_version_lww_local_beats_remote() -> None:
    store = MemoryStore("n1")
    addr = StoreKey(keyspace=b"ks", key=b"kcmp2")
    # local put at ts 500
    await store.put(WriteOp(address=addr, value=b"local", now_ms=500))
    # remote version with lower wall should not override
    remote = Version(
        address=addr,
        metadata=HLCTimestamp(wall=200, counter=0, node_id="remote"),
        value=b"remote",
    )
    await store.apply_version(remote)
    result = await store.get(ReadOp(address=addr))
    assert result[0].value == b"local"
