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
"""Tests for tourillon.core.ring.hashspace — HashSpace arithmetic and hash."""

import logging

import pytest

from tourillon.core.ring.hashspace import HashSpace


def test_hash_determinism_same_input_same_output() -> None:
    """Same bytes input must produce the same integer on successive calls."""
    hs = HashSpace()
    v1 = hs.hash(b"same-input")
    v2 = hs.hash(b"same-input")
    assert isinstance(v1, int)
    assert v1 == v2


def test_hash_bits128_result_in_range() -> None:
    """128-bit hash result must lie in [0, 2**128)."""
    hs = HashSpace(bits=128)
    v = hs.hash(b"anything")
    assert 0 <= v < (1 << 128)


def test_hash_bits8_result_in_range() -> None:
    """Reduced-width (8-bit) hash must lie in [0, 256)."""
    hs = HashSpace(bits=8)
    v = hs.hash(b"anything")
    assert 0 <= v < 256


def test_hash_different_inputs_different_outputs() -> None:
    """Different inputs should very likely map to different integers (MD5)."""
    hs = HashSpace()
    a = hs.hash(b"a-unique-value-1")
    b = hs.hash(b"a-unique-value-2")
    assert a != b


def test_hash_bits8_matches_bits128_truncated() -> None:
    """An 8-bit hash equals the 128-bit hash right-shifted by 120 bits."""
    data = b"truncate-test"
    hs128 = HashSpace(bits=128)
    hs8 = HashSpace(bits=8)
    assert hs8.hash(data) == (hs128.hash(data) >> (128 - 8))


def test_add_normal() -> None:
    """add must perform modular addition inside the configured max (256)."""
    hs = HashSpace(bits=8)
    assert hs.add(10, 5) == 15


def test_add_wraps_at_max() -> None:
    """add must wrap around the ring when the sum exceeds max."""
    hs = HashSpace(bits=8)
    assert hs.add(250, 10) == 4


def test_add_zero_offset() -> None:
    """Adding zero must return the original value unchanged."""
    hs = HashSpace(bits=32)
    assert hs.add(12345, 0) == 12345


def test_in_interval_normal_inside() -> None:
    """For a<b, x in (a, b] must be reported inside."""
    hs = HashSpace(bits=8)
    assert hs.in_interval(x=50, a=10, b=100)


def test_in_interval_normal_at_left_boundary() -> None:
    """Left boundary a is open: x==a must be outside."""
    hs = HashSpace(bits=8)
    assert not hs.in_interval(x=10, a=10, b=100)


def test_in_interval_normal_at_right_boundary() -> None:
    """Right boundary b is closed: x==b must be inside."""
    hs = HashSpace(bits=8)
    assert hs.in_interval(x=100, a=10, b=100)


def test_in_interval_normal_outside() -> None:
    """Points outside (a, b] must be reported outside in the normal case."""
    hs = HashSpace(bits=8)
    assert not hs.in_interval(x=5, a=10, b=100)


def test_in_interval_wrapped_inside() -> None:
    """When wrapped (a>b), values greater than a are inside."""
    hs = HashSpace(bits=8)
    assert hs.in_interval(x=250, a=200, b=50)


def test_in_interval_wrapped_inside_low() -> None:
    """When wrapped (a>b), values <= b are inside."""
    hs = HashSpace(bits=8)
    assert hs.in_interval(x=25, a=200, b=50)


def test_in_interval_wrapped_outside() -> None:
    """When wrapped (a>b), values between b+1 and a are outside."""
    hs = HashSpace(bits=8)
    # between 51 and 199 (exclusive of ends) pick 100
    assert not hs.in_interval(x=100, a=200, b=50)


def test_in_interval_degenerate_a_equals_b() -> None:
    """Degenerate single-vnode ring (a==b) must include every position."""
    hs = HashSpace(bits=8)
    for x in (0, 1, 128, 255):
        assert hs.in_interval(x=x, a=42, b=42)


def test_init_bits_below_minimum_raises() -> None:
    """bits < 1 must raise ValueError."""
    with pytest.raises(ValueError):
        HashSpace(bits=0)


def test_init_bits_above_maximum_raises() -> None:
    """bits > 128 must raise ValueError."""
    with pytest.raises(ValueError):
        HashSpace(bits=129)


def test_init_bits_128_no_warning(caplog: pytest.LogCaptureFixture) -> None:
    """Using full-width 128 bits must not emit a WARNING log."""
    caplog.set_level(logging.WARNING)
    HashSpace(bits=128)
    # No warning should have been emitted for full width
    assert not any(r.levelno >= logging.WARNING for r in caplog.records)


def test_init_bits_8_logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    """Using reduced width must log a WARNING to alert about reduced security."""
    caplog.set_level(logging.WARNING)
    HashSpace(bits=8)
    assert any(r.levelno == logging.WARNING for r in caplog.records)
