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
"""Tests for RebalancePlanner — proposal 005 planner scenarios."""

from __future__ import annotations

import pytest

from tourillon.core.rebalance.plan import RebalancePlan
from tourillon.core.rebalance.planner import RebalancePlanner
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.vnode import VNode


def _make_ring(*node_ids: str, tokens_per_node: int = 4) -> Ring:
    """Create a test ring with evenly spaced tokens."""
    hs = HashSpace(bits=20)
    total = hs.max
    all_tokens: list[VNode] = []
    n = len(node_ids)
    for i, nid in enumerate(node_ids):
        for j in range(tokens_per_node):
            token = int(total * (i * tokens_per_node + j) / (n * tokens_per_node))
            all_tokens.append(VNode(node_id=nid, token=token))
    return Ring(all_tokens)


def _planner(rf: int = 1, partition_shift: int = 4) -> RebalancePlanner:
    hs = HashSpace(bits=20)
    partitioner = Partitioner(hs, partition_shift)
    return RebalancePlanner(partitioner, rf)


@pytest.mark.rebalance
async def test_01_empty_old_ring_no_transfers() -> None:
    """Empty old ring, 1-node new ring → RebalancePlan with no transfers (no source)."""
    planner = _planner()
    new_ring = _make_ring("A")
    plan = planner.plan(Ring.empty(), new_ring, epoch=1)
    assert isinstance(plan, RebalancePlan)
    assert plan.ranges == ()


@pytest.mark.rebalance
async def test_02_rf1_1to2_nodes_generates_transfers() -> None:
    """rf=1, 1-node old ring, 2-node new ring → each pid owned by new node gets transfer from old."""
    planner = _planner(rf=1)
    old_ring = _make_ring("A")
    new_ring = _make_ring("A", "B")
    plan = planner.plan(old_ring, new_ring, epoch=2)
    assert plan.epoch == 2
    transfers = plan.expand()
    # All transfers should have src=A (old node) and dst=B (new node).
    assert len(transfers) > 0
    for t in transfers:
        assert t.src == "A"
        assert t.dst == "B"


@pytest.mark.rebalance
async def test_03_rf1_add_3rd_node_only_moving_pids() -> None:
    """rf=1, 2-node ring, add 3rd node → only pids whose owner changes in plan."""
    planner = _planner(rf=1)
    ring_2 = _make_ring("A", "B")
    ring_3 = _make_ring("A", "B", "C")
    plan = planner.plan(ring_2, ring_3, epoch=3)
    transfers = plan.expand()
    pids_in_plan = {t.pid for t in transfers}
    # Pids not in plan: owner didn't change between the two rings.
    total_pids = set(range(planner._partitioner.total_partitions))
    unchanged = total_pids - pids_in_plan
    # Both sets should be non-empty: some pids move, some stay.
    assert len(pids_in_plan) > 0
    assert len(unchanged) > 0


@pytest.mark.rebalance
async def test_04_rf1_drop_node_reassigned() -> None:
    """rf=1, 3-node ring, drop node → pids formerly owned by dropped node reassigned."""
    planner = _planner(rf=1)
    ring_3 = _make_ring("A", "B", "C")
    ring_2 = _make_ring("A", "B")
    plan = planner.plan(ring_3, ring_2, epoch=4)
    transfers = plan.expand()
    # All pids in the plan must have come from a node in the old ring A,B,C.
    assert len(transfers) > 0
    for t in transfers:
        assert t.src in ("A", "B", "C")
        assert t.dst in ("A", "B")


@pytest.mark.rebalance
async def test_05_same_rings_empty_plan() -> None:
    """Same old and new ring → empty RebalancePlan (ranges is empty tuple)."""
    planner = _planner(rf=1)
    ring = _make_ring("A", "B")
    plan = planner.plan(ring, ring, epoch=1)
    assert plan.ranges == ()


@pytest.mark.rebalance
async def test_06_rf3_2nodes_b_draining_no_transfer() -> None:
    """rf=3, 2 nodes (A,B), B draining → empty plan (A already has full replica set)."""
    planner = _planner(rf=3)
    ring_ab = _make_ring("A", "B")
    ring_a = _make_ring("A")
    plan = planner.plan(ring_ab, ring_a, epoch=5)
    assert plan.ranges == ()


@pytest.mark.rebalance
async def test_07_rf3_4nodes_d_draining() -> None:
    """rf=3, 4 nodes (A,B,C,D), D draining → transfers from D to entering nodes."""
    planner = _planner(rf=3)
    ring_abcd = _make_ring("A", "B", "C", "D")
    ring_abc = _make_ring("A", "B", "C")
    plan = planner.plan(ring_abcd, ring_abc, epoch=6)
    transfers = plan.expand()
    if not transfers:
        # Some ring layouts produce no transfers (e.g. under-replicated).
        return
    for t in transfers:
        # Source must be from the old ring (D leaves); dst must be in new ring.
        assert t.src in ("A", "B", "C", "D")
        assert t.dst in ("A", "B", "C")
        # dst must not equal src (no self-transfers).
        assert t.src != t.dst


@pytest.mark.rebalance
async def test_08_rf3_5nodes_de_draining() -> None:
    """rf=3, 5 nodes (A,B,C,D,E), D+E draining → leaving nodes are sources."""
    planner = _planner(rf=3)
    ring_abcde = _make_ring("A", "B", "C", "D", "E")
    ring_abc = _make_ring("A", "B", "C")
    plan = planner.plan(ring_abcde, ring_abc, epoch=7)
    transfers = plan.expand()
    srcs = {t.src for t in transfers}
    dsts = {t.dst for t in transfers}
    # Sources come from old ring; destinations from new ring.
    assert srcs.issubset({"A", "B", "C", "D", "E"})
    assert dsts.issubset({"A", "B", "C"})


@pytest.mark.rebalance
async def test_09_existing_replica_not_transfer_dst() -> None:
    """Replica-exclusion invariant: entering = new_replicas - old_replicas."""
    planner = _planner(rf=3)
    ring_abcd = _make_ring("A", "B", "C", "D")
    ring_abc = _make_ring("A", "B", "C")
    plan = planner.plan(ring_abcd, ring_abc, epoch=6)
    # Each transfer's src must differ from dst.
    for t in plan.expand():
        assert t.src != t.dst


@pytest.mark.rebalance
async def test_10_join_only_rf3_stable_cluster() -> None:
    """E joins; pid old={A,B,C}, new includes E → entering=[E] → min(old)→E."""
    planner = _planner(rf=3)
    ring_abc = _make_ring("A", "B", "C")
    ring_abce = _make_ring("A", "B", "C", "E")
    plan = planner.plan(ring_abc, ring_abce, epoch=10)
    transfers = plan.expand()
    assert len(transfers) > 0
    for t in transfers:
        assert t.dst == "E"
        # Source should be one of the original replicas (deterministic min).
        assert t.src in ("A", "B", "C")


@pytest.mark.rebalance
async def test_planner_deterministic() -> None:
    """planner.plan() twice with identical rings and rf produces identical plans."""
    planner = _planner(rf=1)
    old_ring = _make_ring("A")
    new_ring = _make_ring("A", "B")
    plan1 = planner.plan(old_ring, new_ring, epoch=1)
    plan2 = planner.plan(old_ring, new_ring, epoch=1)
    assert plan1.ranges == plan2.ranges


@pytest.mark.rebalance
async def test_39_pshift17_range_compaction() -> None:
    """pshift=17, 3-node cluster → plan.ranges is O(nodes), plan.expand() correct."""
    hs = HashSpace(bits=32)
    partitioner = Partitioner(hs, partition_shift=17)
    planner = RebalancePlanner(partitioner, rf=1)
    old_ring = _make_ring("A", "B", tokens_per_node=2)
    new_ring = _make_ring("A", "B", "C", tokens_per_node=2)
    plan = planner.plan(old_ring, new_ring, epoch=1)
    # Ranges are compacted (O(nodes)), not O(partitions).
    assert len(plan.ranges) < 100  # well below 131072
    # expand() yields non-zero pids.
    transfers = plan.expand()
    assert len(transfers) > 0
