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

from tourillon.core.structure.version import Tombstone, Version
from tourillon.infra.memory.store import MemoryStore


async def test_memorystore_put_returns_version_with_correct_key_and_value() -> None:
    """put must return a Version with the supplied key and value."""
    store = MemoryStore("n1")
    v = await store.put("foo", b"bar", now_ms=100)
    assert isinstance(v, Version)
    assert v.key == "foo"
    assert v.value == b"bar"


async def test_memorystore_put_assigns_correct_wall_timestamp() -> None:
    """put must assign a wall timestamp matching now_ms when no prior tick exists."""
    store = MemoryStore("n1")
    v = await store.put("k", b"v", now_ms=500)
    assert v.metadata.wall == 500


async def test_memorystore_get_returns_latest_version() -> None:
    """get must return the version written last for a key."""
    store = MemoryStore("n1")
    await store.put("k", b"first", now_ms=1)
    v2 = await store.put("k", b"second", now_ms=2)
    result = await store.get("k")
    assert len(result) == 1
    assert result[0].value == b"second"
    assert result[0].metadata == v2.metadata


async def test_memorystore_get_returns_empty_for_unknown_key() -> None:
    """get must return an empty list for a key that has never been written."""
    store = MemoryStore("n1")
    assert await store.get("missing") == []


async def test_memorystore_put_binary_safe_value() -> None:
    """put must store and return arbitrary binary payloads unchanged."""
    payload = bytes(range(256))
    store = MemoryStore("n1")
    await store.put("bin", payload, now_ms=1)
    assert (await store.get("bin"))[0].value == payload


async def test_memorystore_put_empty_value() -> None:
    """put must accept an empty byte string as a valid value."""
    store = MemoryStore("n1")
    await store.put("empty", b"", now_ms=1)
    assert (await store.get("empty"))[0].value == b""


async def test_memorystore_put_multiple_keys_independently() -> None:
    """put on different keys must not interfere with each other."""
    store = MemoryStore("n1")
    await store.put("a", b"A", now_ms=1)
    await store.put("b", b"B", now_ms=2)
    assert (await store.get("a"))[0].value == b"A"
    assert (await store.get("b"))[0].value == b"B"


async def test_memorystore_timestamps_are_monotonically_increasing() -> None:
    """Each successive put must produce a strictly increasing timestamp."""
    store = MemoryStore("n1")
    v1 = await store.put("k", b"1", now_ms=100)
    v2 = await store.put("k", b"2", now_ms=100)
    v3 = await store.put("k", b"3", now_ms=200)
    assert v2.metadata > v1.metadata
    assert v3.metadata > v2.metadata


async def test_memorystore_delete_returns_tombstone() -> None:
    """delete must return a Tombstone with the correct key."""
    store = MemoryStore("n1")
    await store.put("k", b"v", now_ms=1)
    t = await store.delete("k", now_ms=2)
    assert isinstance(t, Tombstone)
    assert t.key == "k"


async def test_memorystore_delete_hides_existing_versions() -> None:
    """get must return an empty list after the key is deleted."""
    store = MemoryStore("n1")
    await store.put("k", b"v", now_ms=1)
    await store.delete("k", now_ms=2)
    assert await store.get("k") == []


async def test_memorystore_put_after_delete_is_visible() -> None:
    """A put whose timestamp is after a tombstone must be visible via get."""
    store = MemoryStore("n1")
    await store.put("k", b"old", now_ms=1)
    await store.delete("k", now_ms=2)
    v = await store.put("k", b"new", now_ms=3)
    result = await store.get("k")
    assert len(result) == 1
    assert result[0].value == b"new"
    assert result[0].metadata == v.metadata


async def test_memorystore_delete_nonexistent_key_is_safe() -> None:
    """delete on a key that was never written must not raise."""
    store = MemoryStore("n1")
    t = await store.delete("ghost", now_ms=1)
    assert t.key == "ghost"
    assert await store.get("ghost") == []


async def test_memorystore_delete_twice_keeps_key_invisible() -> None:
    """A second delete with a later timestamp must keep the key invisible."""
    store = MemoryStore("n1")
    await store.put("k", b"v", now_ms=1)
    await store.delete("k", now_ms=2)
    await store.put("k", b"revived", now_ms=3)
    await store.delete("k", now_ms=4)
    assert await store.get("k") == []
