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
"""HashSpace unit tests — bits validation and hash truncation."""

from __future__ import annotations

import pytest

from tourillon.core.ring.hashspace import HashSpace

pytestmark = pytest.mark.ring


def test_hashspace_bits_zero_raises_value_error() -> None:
    """HashSpace(bits=0) raises ValueError because bits must be >= 1."""
    with pytest.raises(ValueError, match="bits must be >= 1"):
        HashSpace(bits=0)


def test_hashspace_bits_default_128_properties() -> None:
    """Default HashSpace has bits=128 and max=2**128."""
    hs = HashSpace()
    assert hs.bits == 128
    assert hs.max == 2**128


def test_hashspace_hash_with_bits_less_than_128_truncates() -> None:
    """hash() with bits<128 right-shifts MD5 digest to bits significant bits."""
    hs = HashSpace(bits=8)
    result = hs.hash(b"hello")
    assert 0 <= result < 256  # 2**8


def test_hashspace_hash_with_bits_128_returns_full_md5() -> None:
    """hash() with bits=128 returns the full 128-bit MD5 integer."""
    hs = HashSpace(bits=128)
    result = hs.hash(b"hello")
    assert 0 <= result < 2**128


def test_hashspace_hash_deterministic() -> None:
    """hash() produces the same result for the same input bytes."""
    hs = HashSpace(bits=16)
    assert hs.hash(b"key") == hs.hash(b"key")


def test_hashspace_hash_different_inputs_differ() -> None:
    """hash() produces different results for different inputs (collision-unlikely)."""
    hs = HashSpace(bits=64)
    assert hs.hash(b"key1") != hs.hash(b"key2")
