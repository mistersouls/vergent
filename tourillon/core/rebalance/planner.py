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
"""RebalancePlanner — deterministic partition rebalance plan generator."""

from __future__ import annotations

from tourillon.core.rebalance.plan import (
    PartitionRangeTransfer,
    PartitionTransfer,
    RebalancePlan,
)
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.vnode import VNode


class RebalancePlanner:
    """Deterministic replica-aware rebalance plan generator.

    Given two ring snapshots (old_ring, new_ring) and the replication factor,
    computes the minimal set of partition transfers needed to bring new_ring
    into the correct replication state. The planner is topology-only: it never
    consults a registry, never checks probe state, and includes all phases
    (FAILED, PAUSED, JOINING, READY, DRAINING) in replica-set computation,
    guaranteeing identical plans on every node that observes the same rings.

    plan() is a pure synchronous method; it is safe to call from any context
    without awaiting. The returned RebalancePlan's ranges are compacted using
    the mandatory-merge invariant.
    """

    def __init__(self, partitioner: Partitioner, rf: int) -> None:
        self._partitioner = partitioner
        self._rf = rf

    def plan(self, old_ring: Ring, new_ring: Ring, epoch: int) -> RebalancePlan:
        """Return the RebalancePlan for transitioning from old_ring to new_ring.

        Iterates over all partition IDs in [0, total_partitions). For each pid
        computes the old and new replica sets and derives transfers needed.
        Adjacent transfers sharing the same (src, dst) are merged into
        PartitionRangeTransfer entries (mandatory-merge invariant).
        """
        total = self._partitioner.total_partitions
        raw_transfers: list[PartitionTransfer] = []
        for pid in range(total):
            transfers = self._transfers_for_pid(pid, old_ring, new_ring)
            raw_transfers.extend(transfers)
        ranges = _compact_transfers(raw_transfers)
        return RebalancePlan(epoch=epoch, ranges=tuple(ranges))

    def _transfers_for_pid(
        self, pid: int, old_ring: Ring, new_ring: Ring
    ) -> list[PartitionTransfer]:
        """Return the transfers needed for one pid."""
        token = self._partitioner.segment_for_pid(pid).end
        old_replicas = self._replica_set_for_token(old_ring, token)
        new_replicas = self._replica_set_for_token(new_ring, token)

        if not old_replicas:
            return []

        leaving = sorted(old_replicas - new_replicas)
        entering = sorted(new_replicas - old_replicas)
        if not entering:
            return []

        return self._pair_transfers(pid, leaving, entering, old_replicas)

    def _pair_transfers(
        self,
        pid: int,
        leaving: list[str],
        entering: list[str],
        old_replicas: frozenset[str],
    ) -> list[PartitionTransfer]:
        """Pair leaving nodes with entering nodes; use min(old_replicas) for unpaired."""
        result: list[PartitionTransfer] = []
        for src, dst in zip(leaving, entering, strict=False):
            result.append(PartitionTransfer(pid=pid, src=src, dst=dst))
        unpaired = entering[len(leaving) :]
        if not unpaired:
            return result
        src = _min_eligible_source(old_replicas)
        if src is None:
            return result
        for dst in unpaired:
            result.append(PartitionTransfer(pid=pid, src=src, dst=dst))
        return result

    def _replica_set_for_token(self, ring: Ring, token: int) -> frozenset[str]:
        """Return the first rf distinct node_ids clockwise from token in ring."""
        if len(ring) == 0:
            return frozenset()
        # Use a dummy VNode to find the start position.
        dummy = VNode(node_id="", token=token)
        return self._replica_set(ring, dummy, self._rf)

    @staticmethod
    def _replica_set(ring: Ring, vnode: VNode, rf: int) -> frozenset[str]:
        """Walk ring clockwise from vnode; return first rf distinct node_ids.

        Topology-only: includes all phases (READY, DRAINING, JOINING, FAILED,
        PAUSED) that have vnodes in the ring. IDLE nodes are absent from the
        ring by design and never encountered. No registry consulted — pure and
        deterministic given the ring snapshot alone.
        """
        seen: set[str] = set()
        for v in ring.iter_from(vnode):
            if v.node_id not in seen:
                seen.add(v.node_id)
                if len(seen) == rf:
                    break
        return frozenset(seen)


def _compact_transfers(
    transfers: list[PartitionTransfer],
) -> list[PartitionRangeTransfer]:
    """Merge adjacent transfers sharing the same (src, dst) into ranges.

    Enforces the mandatory-merge invariant: two adjacent entries with identical
    (src, dst) are merged when pid_end + 1 == pid_start of the next entry.
    """
    if not transfers:
        return []
    result: list[PartitionRangeTransfer] = []
    current = transfers[0]
    lo = current.pid
    hi = current.pid
    for t in transfers[1:]:
        if t.src == current.src and t.dst == current.dst and t.pid == hi + 1:
            hi = t.pid
        else:
            result.append(
                PartitionRangeTransfer(
                    pid_start=lo, pid_end=hi, src=current.src, dst=current.dst
                )
            )
            current = t
            lo = t.pid
            hi = t.pid
    result.append(
        PartitionRangeTransfer(
            pid_start=lo, pid_end=hi, src=current.src, dst=current.dst
        )
    )
    return result


def _min_eligible_source(old_replicas: frozenset[str]) -> str | None:
    """Return the lexicographically smallest non-FAILED non-PAUSED node_id.

    Since the planner has no registry access, it cannot inspect phases. The
    caller passes old_replicas as node_ids only. The planner trusts that the
    topology is correct and returns min(old_replicas) as the deterministic
    source. In practice, FAILED/PAUSED filtering is done by the applicator
    when it knows the registry; here we return min unconditionally for
    topology-only determinism.

    Returns None when old_replicas is empty.
    """
    if not old_replicas:
        return None
    return min(old_replicas)
