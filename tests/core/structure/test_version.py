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
"""Tests for tourillon.core.structure.version — StoreKey, Version, and Tombstone."""

import dataclasses

import pytest

from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.version import StoreKey, Tombstone, Version


def _ts(wall: int = 1, counter: int = 0, node_id: str = "n") -> HLCTimestamp:
    return HLCTimestamp(wall=wall, counter=counter, node_id=node_id)


def _key(keyspace: bytes = b"ks", key: bytes = b"k") -> StoreKey:
    return StoreKey(keyspace=keyspace, key=key)


def test_storekey_fields_are_preserved() -> None:
    """StoreKey must store keyspace and key exactly as supplied."""
    sk = StoreKey(keyspace=b"accounts", key=b"user:1")
    assert sk.keyspace == b"accounts"
    assert sk.key == b"user:1"


def test_storekey_is_frozen() -> None:
    """StoreKey must be immutable — attribute assignment must raise."""
    sk = _key()
    with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
        sk.key = b"other"  # type: ignore[misc]


def test_storekey_equality_by_fields() -> None:
    """Two StoreKey instances with identical fields must compare equal."""
    assert StoreKey(keyspace=b"ks", key=b"k") == StoreKey(keyspace=b"ks", key=b"k")


def test_storekey_inequality_on_different_keyspace() -> None:
    """StoreKeys with differing keyspaces must not be equal."""
    assert StoreKey(keyspace=b"a", key=b"k") != StoreKey(keyspace=b"b", key=b"k")


def test_storekey_inequality_on_different_key() -> None:
    """StoreKeys with differing keys must not be equal."""
    assert StoreKey(keyspace=b"ks", key=b"a") != StoreKey(keyspace=b"ks", key=b"b")


def test_storekey_empty_keyspace_raises() -> None:
    """Constructing a StoreKey with an empty keyspace must raise ValueError."""
    with pytest.raises(ValueError, match="keyspace must not be empty"):
        StoreKey(keyspace=b"", key=b"k")


def test_storekey_empty_key_raises() -> None:
    """Constructing a StoreKey with an empty key must raise ValueError."""
    with pytest.raises(ValueError, match="key must not be empty"):
        StoreKey(keyspace=b"ks", key=b"")


def test_storekey_accepts_arbitrary_binary() -> None:
    """StoreKey must accept arbitrary binary content in both fields."""
    sk = StoreKey(keyspace=bytes(range(1, 10)), key=bytes(range(200, 210)))
    assert sk.keyspace == bytes(range(1, 10))
    assert sk.key == bytes(range(200, 210))


def test_storekey_encode_decode_roundtrip() -> None:
    """A StoreKey must survive an encode→decode cycle unchanged."""
    sk = StoreKey(keyspace=b"accounts", key=b"user:42")
    assert StoreKey.decode(sk.encode()) == sk


def test_storekey_encode_decode_roundtrip_binary_fields() -> None:
    """encode/decode must preserve arbitrary binary keyspace and key."""
    sk = StoreKey(keyspace=bytes(range(1, 20)), key=bytes(range(100, 110)))
    assert StoreKey.decode(sk.encode()) == sk


def test_storekey_decode_raises_on_too_short_frame() -> None:
    """decode must raise ValueError when the frame is shorter than the header."""
    with pytest.raises(ValueError, match="too short"):
        StoreKey.decode(b"\x00")


def test_storekey_decode_raises_on_zero_keyspace_len() -> None:
    """decode must raise ValueError when keyspace_len is zero."""
    import struct

    data = struct.pack("!HH", 0, 3) + b"key"
    with pytest.raises(ValueError, match="keyspace must not be empty"):
        StoreKey.decode(data)


def test_storekey_decode_raises_on_zero_key_len() -> None:
    """decode must raise ValueError when key_len is zero."""
    import struct

    data = struct.pack("!HH", 2, 0) + b"ks"
    with pytest.raises(ValueError, match="key must not be empty"):
        StoreKey.decode(data)


def test_storekey_decode_raises_on_truncated_frame() -> None:
    """decode must raise ValueError when declared lengths exceed available bytes."""
    sk = StoreKey(keyspace=b"ks", key=b"k")
    frame = sk.encode()
    with pytest.raises(ValueError, match="truncated"):
        StoreKey.decode(frame[:-1])


def test_storekey_encode_length_is_correct() -> None:
    """Encoded length must equal header (4 bytes) plus field lengths."""
    sk = StoreKey(keyspace=b"ns", key=b"abc")
    assert len(sk.encode()) == 4 + len(b"ns") + len(b"abc")


def test_version_fields_are_preserved() -> None:
    """Version must store address, metadata, and value exactly as supplied."""
    ts = _ts(wall=10)
    sk = _key()
    v = Version(address=sk, metadata=ts, value=b"bar")
    assert v.address == sk
    assert v.metadata == ts
    assert v.value == b"bar"


def test_version_is_frozen() -> None:
    """Version must be immutable — attribute assignment must raise."""
    v = Version(address=_key(), metadata=_ts(), value=b"v")
    with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
        v.value = b"other"  # type: ignore[misc]


def test_version_equality_by_fields() -> None:
    """Two Version instances with identical fields must compare equal."""
    ts = _ts(wall=5)
    sk = _key()
    assert Version(address=sk, metadata=ts, value=b"v") == Version(
        address=sk, metadata=ts, value=b"v"
    )


def test_version_inequality_on_different_value() -> None:
    """Versions with the same address and metadata but different values are not equal."""
    ts = _ts()
    sk = _key()
    assert Version(address=sk, metadata=ts, value=b"a") != Version(
        address=sk, metadata=ts, value=b"b"
    )


def test_version_inequality_on_different_keyspace() -> None:
    """Versions whose addresses differ on keyspace are not equal."""
    ts = _ts()
    assert Version(
        address=StoreKey(keyspace=b"a", key=b"k"), metadata=ts, value=b"v"
    ) != Version(address=StoreKey(keyspace=b"b", key=b"k"), metadata=ts, value=b"v")


def test_version_accepts_empty_bytes() -> None:
    """Version must accept an empty byte string as a valid value."""
    v = Version(address=_key(), metadata=_ts(), value=b"")
    assert v.value == b""


def test_version_accepts_arbitrary_binary() -> None:
    """Version must store and return arbitrary binary payloads unchanged."""
    payload = bytes(range(256))
    v = Version(address=_key(), metadata=_ts(), value=payload)
    assert v.value == payload


def test_tombstone_fields_are_preserved() -> None:
    """Tombstone must store address and metadata exactly as supplied."""
    ts = _ts(wall=99)
    sk = _key(key=b"gone")
    t = Tombstone(address=sk, metadata=ts)
    assert t.address == sk
    assert t.metadata == ts


def test_tombstone_is_frozen() -> None:
    """Tombstone must be immutable — attribute assignment must raise."""
    t = Tombstone(address=_key(), metadata=_ts())
    with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
        t.address = _key(key=b"other")  # type: ignore[misc]


def test_tombstone_equality_by_fields() -> None:
    """Two Tombstone instances with identical fields must compare equal."""
    ts = _ts(wall=7)
    sk = _key()
    assert Tombstone(address=sk, metadata=ts) == Tombstone(address=sk, metadata=ts)


def test_tombstone_inequality_on_different_key() -> None:
    """Tombstones with the same metadata but different keys are not equal."""
    ts = _ts()
    assert Tombstone(
        address=StoreKey(keyspace=b"ks", key=b"a"), metadata=ts
    ) != Tombstone(address=StoreKey(keyspace=b"ks", key=b"b"), metadata=ts)
