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
"""Circular hash space arithmetic backing the consistent-hash ring."""

import hashlib
import logging

_logger: logging.Logger = logging.getLogger(__name__)

_FULL_BITS: int = 128
_DIGEST_BYTES: int = 16


class HashSpace:
    """Circular integer space [0, 2**bits) used by the consistent-hash ring.

    A HashSpace encapsulates the modular arithmetic and the hashing function
    that the ring uses to map keys and vnode tokens onto a one-dimensional
    cycle. The full space uses 128 bits, matching the natural width of MD5,
    so that hash(value) is a direct big-endian interpretation of the digest.
    Reduced widths are accepted for tests and benchmarks: the digest is
    right-shifted by (128 - bits) so the resulting value still falls inside
    [0, 2**bits) while preserving the high-order distribution of the hash.

    HashSpace is intentionally narrow. It exposes only the primitives the
    ring layer needs — hashing, modular addition, and circular interval
    membership — and deliberately does not generate vnode tokens. Token
    generation is the responsibility of the bootstrap layer per docs/ring.md
    §1.4 so that this class remains a pure mathematical helper free of any
    naming or identity policy.
    """

    __slots__ = ("bits", "max")

    def __init__(self, bits: int = 128) -> None:
        """Build a HashSpace of the given width in bits.

        The bits argument must lie in the closed range [1, 128]. Values
        outside this range raise ValueError. When bits is strictly less
        than 128 a WARNING is logged because reduced widths are intended
        only for testing and benchmarking, never for production rings
        where the reduced collision resistance would degrade load balance.
        """
        if bits < 1 or bits > _FULL_BITS:
            raise ValueError(f"bits must be in [1, {_FULL_BITS}], got {bits}")
        self.bits: int = bits
        self.max: int = 1 << bits
        if bits < _FULL_BITS:
            _logger.warning(
                "HashSpace using reduced width bits=%d (full width is %d);"
                " intended for tests only",
                bits,
                _FULL_BITS,
            )

    def hash(self, value: bytes) -> int:
        """Hash a bytes value to an integer position in [0, self.max).

        The digest is computed with MD5 and interpreted as a big-endian
        128-bit unsigned integer. When the configured width is below 128
        bits the high-order (128 - bits) bits are kept by right-shifting
        the digest, which preserves the avalanche properties of the upper
        bytes while compressing the value into the configured range.
        """
        digest = hashlib.md5(value, usedforsecurity=False).digest()
        full = int.from_bytes(digest, byteorder="big", signed=False)
        if self.bits < _FULL_BITS:
            full >>= _FULL_BITS - self.bits
        return full

    def add(self, x: int, d: int) -> int:
        """Return (x + d) reduced into [0, self.max) by modular arithmetic.

        Both x and d may be any Python ints, positive or negative. The
        result is always a non-negative position inside the ring, allowing
        callers to step forward or backward across the wrap-around without
        special-casing the boundary.
        """
        return (x + d) % self.max

    def in_interval(self, x: int, a: int, b: int) -> bool:
        """Return True when x lies in the half-open circular interval (a, b].

        The interval is left-open and right-closed, matching the
        canonical Dynamo-style ownership rule that places a key on the
        first vnode whose token is greater than or equal to the key. Two
        regimes are distinguished:

        * Normal (a < b): membership reduces to a < x <= b.
        * Wrapped (a >= b): the interval crosses the zero boundary, so
          membership becomes x > a or x <= b.

        The wrapped case correctly handles the degenerate single-vnode
        ring where a == b, in which case every position is considered
        inside the interval.
        """
        if a < b:
            return a < x <= b
        return x > a or x <= b
