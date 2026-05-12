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
"""Tests for compute_transfer_digest — proposal 005 digest scenarios."""

from __future__ import annotations

import pytest

from tourillon.core.rebalance.digest import compute_transfer_digest
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.record import StoreKey, Tombstone, Version

_KEY = StoreKey(keyspace=b"ks", key=b"k1")
_HLC_V = HLCTimestamp(wall_ms=1000, counter=0, node_id="n1")
_HLC_T = HLCTimestamp(wall_ms=2000, counter=0, node_id="n1")


def _version() -> Version:
    return Version(address=_KEY, metadata=_HLC_V, value=b"val")


def _tombstone() -> Tombstone:
    return Tombstone(address=_KEY, metadata=_HLC_T)


@pytest.mark.rebalance
async def test_17_digest_ordered_deterministic() -> None:
    """Feed one Version then one Tombstone → SHA-256 deterministic; same order → same digest."""
    records = [_version(), _tombstone()]
    d1 = compute_transfer_digest(iter(records))
    d2 = compute_transfer_digest(iter(records))
    assert d1 == d2
    assert len(d1) == 64  # hex SHA-256


@pytest.mark.rebalance
async def test_18_digest_reverse_order_different() -> None:
    """Same records in reverse order → different digest from #17."""
    forward = compute_transfer_digest(iter([_version(), _tombstone()]))
    reverse = compute_transfer_digest(iter([_tombstone(), _version()]))
    assert forward != reverse


@pytest.mark.rebalance
async def test_empty_records_digest() -> None:
    """Empty record list produces consistent digest."""
    d = compute_transfer_digest(iter([]))
    assert len(d) == 64
    assert d == compute_transfer_digest(iter([]))


@pytest.mark.rebalance
async def test_version_only_digest() -> None:
    """Version-only digest differs from tombstone-only digest."""
    dv = compute_transfer_digest(iter([_version()]))
    dt = compute_transfer_digest(iter([_tombstone()]))
    assert dv != dt
