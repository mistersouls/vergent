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
"""Tests for tourillon.core.structure.version — Version and Tombstone."""

import dataclasses

import pytest

from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.version import Tombstone, Version


def _ts(wall: int = 1, counter: int = 0, node_id: str = "n") -> HLCTimestamp:
    return HLCTimestamp(wall=wall, counter=counter, node_id=node_id)


def test_version_fields_are_preserved() -> None:
    """Version must store key, metadata and value exactly as supplied."""
    ts = _ts(wall=10)
    v = Version(key="foo", metadata=ts, value=b"bar")
    assert v.key == "foo"
    assert v.metadata == ts
    assert v.value == b"bar"


def test_version_is_frozen() -> None:
    """Version must be immutable — attribute assignment must raise."""
    v = Version(key="k", metadata=_ts(), value=b"v")
    with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
        v.key = "other"  # type: ignore[misc]


def test_version_equality_by_fields() -> None:
    """Two Version instances with identical fields must compare equal."""
    ts = _ts(wall=5)
    assert Version(key="k", metadata=ts, value=b"v") == Version(
        key="k", metadata=ts, value=b"v"
    )


def test_version_inequality_on_different_value() -> None:
    """Versions with the same key and metadata but different values are not equal."""
    ts = _ts()
    assert Version(key="k", metadata=ts, value=b"a") != Version(
        key="k", metadata=ts, value=b"b"
    )


def test_version_accepts_empty_bytes() -> None:
    """Version must accept an empty byte string as a valid value."""
    v = Version(key="k", metadata=_ts(), value=b"")
    assert v.value == b""


def test_version_accepts_arbitrary_binary() -> None:
    """Version must store and return arbitrary binary payloads unchanged."""
    payload = bytes(range(256))
    v = Version(key="bin", metadata=_ts(), value=payload)
    assert v.value == payload


def test_tombstone_fields_are_preserved() -> None:
    """Tombstone must store key and metadata exactly as supplied."""
    ts = _ts(wall=99)
    t = Tombstone(key="gone", metadata=ts)
    assert t.key == "gone"
    assert t.metadata == ts


def test_tombstone_is_frozen() -> None:
    """Tombstone must be immutable — attribute assignment must raise."""
    t = Tombstone(key="k", metadata=_ts())
    with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
        t.key = "other"  # type: ignore[misc]


def test_tombstone_equality_by_fields() -> None:
    """Two Tombstone instances with identical fields must compare equal."""
    ts = _ts(wall=7)
    assert Tombstone(key="k", metadata=ts) == Tombstone(key="k", metadata=ts)


def test_tombstone_inequality_on_different_key() -> None:
    """Tombstones with the same metadata but different keys are not equal."""
    ts = _ts()
    assert Tombstone(key="a", metadata=ts) != Tombstone(key="b", metadata=ts)
