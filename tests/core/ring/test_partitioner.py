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
"""Tests for tourillon.core.ring.partitioner — logical partitions and placement."""

import dataclasses
from collections.abc import Iterable

import pytest

from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import (
    LogicalPartition,
    Partitioner,
    PartitionPlacement,
)
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.vnode import VNode


def _vnodes_from(tokens: Iterable[int], node_prefix: str = "n") -> list[VNode]:
    return [VNode(node_id=f"{node_prefix}{i}", token=t) for i, t in enumerate(tokens)]


# Reference setup: bits=8, partition_shift=3 -> 8 partitions, step=32, max=256
hs8 = HashSpace(bits=8)
pt8 = Partitioner(hs8, partition_shift=3)


def test_logical_partition_valid_construction() -> None:
    LogicalPartition(pid=1, start=32, end=64)


def test_logical_partition_negative_pid_raises() -> None:
    with pytest.raises(ValueError):
        LogicalPartition(pid=-1, start=0, end=10)


def test_logical_partition_negative_start_raises() -> None:
    with pytest.raises(ValueError):
        LogicalPartition(pid=0, start=-1, end=10)


def test_logical_partition_is_frozen() -> None:
    p = LogicalPartition(pid=0, start=0, end=32)
    with pytest.raises(dataclasses.FrozenInstanceError):
        p.pid = 1  # type: ignore[assignment]


def test_contains_normal_inside() -> None:
    p = LogicalPartition(pid=1, start=32, end=64)
    assert p.contains(50)


def test_contains_normal_at_left_boundary() -> None:
    p = LogicalPartition(pid=1, start=32, end=64)
    assert not p.contains(32)


def test_contains_normal_at_right_boundary() -> None:
    p = LogicalPartition(pid=1, start=32, end=64)
    assert p.contains(64)


def test_contains_normal_outside() -> None:
    p = LogicalPartition(pid=1, start=32, end=64)
    assert not p.contains(10)


def test_contains_wrapped_inside_high() -> None:
    # wrapped arc: start >= end
    p = LogicalPartition(pid=7, start=224, end=32)
    assert p.contains(240)


def test_contains_wrapped_inside_low() -> None:
    p = LogicalPartition(pid=7, start=224, end=32)
    assert p.contains(16)


def test_contains_wrapped_outside() -> None:
    p = LogicalPartition(pid=7, start=224, end=32)
    # number in the excluded middle
    assert not p.contains(100)


def test_contains_token_zero_in_first_partition() -> None:
    p = LogicalPartition(pid=0, start=0, end=32)
    assert not p.contains(0)


def test_partition_placement_address_is_pid_str() -> None:
    p = LogicalPartition(pid=2, start=64, end=96)
    vnode = VNode("n", 70)
    pp = PartitionPlacement(partition=p, vnode=vnode)
    assert pp.address == "2"


def test_partition_placement_is_frozen() -> None:
    p = LogicalPartition(pid=2, start=64, end=96)
    vnode = VNode("n", 70)
    pp = PartitionPlacement(partition=p, vnode=vnode)
    with pytest.raises(dataclasses.FrozenInstanceError):
        pp.vnode = VNode("x", 1)  # type: ignore[assignment]


def test_partitioner_partition_shift_zero_raises() -> None:
    with pytest.raises(ValueError):
        Partitioner(hs8, partition_shift=0)


def test_partitioner_partition_shift_equals_bits_raises() -> None:
    with pytest.raises(ValueError):
        Partitioner(hs8, partition_shift=8)


def test_partitioner_partition_shift_above_bits_raises() -> None:
    with pytest.raises(ValueError):
        Partitioner(hs8, partition_shift=9)


def test_partitioner_total_partitions() -> None:
    assert pt8.total_partitions == 8


def test_partitioner_total_partitions_shift4() -> None:
    p = Partitioner(hs8, partition_shift=4)
    assert p.total_partitions == 16


def test_pid_for_hash_zero() -> None:
    assert pt8.pid_for_hash(0) == 0


def test_pid_for_hash_step_boundary() -> None:
    assert pt8.pid_for_hash(32) == 1


def test_pid_for_hash_just_before_step() -> None:
    assert pt8.pid_for_hash(31) == 0


def test_pid_for_hash_max_minus_one() -> None:
    assert pt8.pid_for_hash(255) == 7


def test_pid_for_hash_all_in_range() -> None:
    got = {pt8.pid_for_hash(h) for h in range(hs8.max)}
    assert got.issubset(set(range(pt8.total_partitions)))


def test_segment_for_pid_zero() -> None:
    seg = pt8.segment_for_pid(0)
    assert seg.start == 0 and seg.end == 32


def test_segment_for_pid_middle() -> None:
    seg = pt8.segment_for_pid(4)
    assert seg.start == 128 and seg.end == 160


def test_segment_for_pid_last() -> None:
    seg = pt8.segment_for_pid(7)
    assert seg.start == 224 and seg.end == 256


def test_segment_for_pid_out_of_range_raises() -> None:
    with pytest.raises(ValueError):
        pt8.segment_for_pid(8)


def test_segment_for_pid_negative_raises() -> None:
    with pytest.raises(ValueError):
        pt8.segment_for_pid(-1)


def test_segment_last_partition_end_is_max() -> None:
    assert pt8.segment_for_pid(pt8.total_partitions - 1).end == hs8.max


def test_placement_for_token_empty_ring_raises() -> None:
    with pytest.raises(IndexError):
        pt8.placement_for_token(10, Ring.empty())


def test_placement_for_token_correct_pid() -> None:
    r = Ring.from_vnodes(_vnodes_from([10, 90, 200]))
    placement = pt8.placement_for_token(40, r)
    assert placement.partition.pid == 1


def test_placement_for_token_address_is_pid_str() -> None:
    r = Ring.from_vnodes(_vnodes_from([10, 90, 200]))
    placement = pt8.placement_for_token(40, r)
    assert placement.address == str(placement.partition.pid)


def test_placement_vnode_is_successor() -> None:
    r = Ring.from_vnodes(_vnodes_from([10, 90, 200]))
    token = 40
    placement = pt8.placement_for_token(token, r)
    assert placement.vnode == r.successor(token)


def test_determinism_same_inputs_same_output() -> None:
    r = Ring.from_vnodes(_vnodes_from([10, 90, 200]))
    p1 = Partitioner(hs8, partition_shift=3)
    p2 = Partitioner(hs8, partition_shift=3)
    a = p1.placement_for_token(40, r)
    b = p2.placement_for_token(40, r)
    assert a == b
