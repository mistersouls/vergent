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
"""Partition grid, logical partitions, and placement resolution for the ring."""

from __future__ import annotations

import dataclasses

from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.vnode import VNode


@dataclasses.dataclass(frozen=True)
class LogicalPartition:
    """Contiguous half-open arc ``(start, end]`` of the circular hash space.

    A LogicalPartition is a static, identity-bearing slice of the ring's
    hash space. Partitions are produced by a Partitioner which imposes a
    fixed grid of ``2 ** partition_shift`` arcs over ``[0, hash_space.max)``;
    each arc is identified by its integer ``pid`` and stable across ring
    mutations. Ownership of a partition is computed at routing time by
    looking up the successor VNode of any token inside the arc, but the
    arc itself never moves — this is what allows the upper layers to use
    ``pid`` as a stable storage key prefix.

    The interval is left-open and right-closed to align with the canonical
    Dynamo-style ownership rule used by ``HashSpace.in_interval`` and by
    ``Ring.successor``. The membership test handles both the normal regime
    (``start < end``) and the wrapped regime (``start >= end``) so that the
    partition crossing the zero boundary remains usable without callers
    special-casing the wrap-around.
    """

    pid: int
    start: int
    end: int

    def __post_init__(self) -> None:
        """Validate that the partition fields satisfy the grid invariants.

        Raise ValueError when ``pid`` is negative or when either bound is
        negative, since such a partition could not represent a valid arc
        of a non-negative hash space.
        """
        if self.pid < 0:
            raise ValueError(f"pid must be non-negative, got {self.pid}")
        if self.start < 0:
            raise ValueError(f"start must be non-negative, got {self.start}")
        if self.end < 0:
            raise ValueError(f"end must be non-negative, got {self.end}")

    def contains(self, h: int) -> bool:
        """Return True when ``h`` lies inside the half-open arc ``(start, end]``.

        The membership rule mirrors ``HashSpace.in_interval``: in the
        normal regime where ``start < end`` the test reduces to
        ``start < h <= end``; in the wrapped regime where ``start >= end``
        the arc crosses the zero boundary and the test becomes
        ``h > start or h <= end``.

        Note: the interval is left-open. A token equal to ``start`` is NOT
        considered inside the partition. For example, token ``0`` maps to
        ``pid == 0`` via ``Partitioner.pid_for_hash``, but
        ``LogicalPartition(pid=0, start=0, ...).contains(0)`` returns ``False``.
        This behaviour is intentional and consistent with the ring successor
        semantics used for ownership resolution.
        """
        if self.start < self.end:
            return self.start < h <= self.end
        return h > self.start or h <= self.end


@dataclasses.dataclass(frozen=True)
class PartitionPlacement:
    """Ephemeral binding between a LogicalPartition and its current owner VNode.

    A PartitionPlacement is the value object returned by routing: it pairs
    a stable LogicalPartition (identified by its ``pid``) with the VNode
    that currently owns it on the ring. The binding is recomputed after
    every ring mutation and is never persisted — only the partition's
    ``pid`` enters durable state via the ``address`` property, which is
    used as a stable key prefix in the storage layer.
    """

    partition: LogicalPartition
    vnode: VNode

    @property
    def address(self) -> str:
        """Return the storage-key prefix for this placement's partition.

        The address is the partition's ``pid`` rendered as a decimal
        string. It is deliberately independent of the current owner VNode
        so that data written under this prefix survives ring mutations
        without rewriting keys.
        """
        return str(self.partition.pid)


class Partitioner:
    """Static grid of ``2 ** partition_shift`` logical partitions over the ring.

    The Partitioner imposes a fixed, evenly-spaced partition grid on a
    HashSpace and resolves placements by composing the grid with a Ring.
    The grid is statically derived from ``partition_shift`` and never
    changes at runtime; only the binding from a partition to its owner
    VNode evolves as the ring is rebuilt. This separation is what lets
    ``LogicalPartition.pid`` serve as a stable storage-key prefix while
    ownership tracks ring topology.

    Routing is O(1) for the partition lookup (a single right-shift) plus
    O(log n) for the ring successor lookup. The Partitioner itself is
    stateless beyond its construction parameters and is safe to share
    across threads.
    """

    __slots__ = ("_hash_space", "_partition_shift", "_step")

    def __init__(self, hash_space: HashSpace, partition_shift: int) -> None:
        """Build a Partitioner over ``hash_space`` with ``2 ** partition_shift`` arcs.

        ``partition_shift`` must be at least 1 and strictly less than
        ``hash_space.bits``; otherwise the grid would either be trivial
        or coarser than the hash space itself, both of which are
        rejected with a ValueError. The per-arc step is precomputed as
        ``hash_space.max >> partition_shift`` so that subsequent
        segment lookups are pure arithmetic.
        """
        if partition_shift < 1:
            raise ValueError(f"partition_shift must be >= 1, got {partition_shift}")
        if partition_shift >= hash_space.bits:
            raise ValueError(
                "partition_shift must be strictly less than hash_space.bits"
            )
        self._hash_space: HashSpace = hash_space
        self._partition_shift: int = partition_shift
        self._step: int = hash_space.max >> partition_shift

    @property
    def total_partitions(self) -> int:
        """Return the total number of partitions in the grid.

        Equal to ``2 ** partition_shift``.
        """
        return 1 << self._partition_shift

    def pid_for_hash(self, h: int) -> int:
        """Return the partition identifier owning hash position ``h`` in O(1).

        The mapping is a single right-shift by
        ``hash_space.bits - partition_shift`` bits, which is equivalent
        to integer division by the per-arc step but avoids the division.
        The result lies in ``[0, total_partitions)`` for any
        ``h`` in ``[0, hash_space.max)``.
        """
        return h >> (self._hash_space.bits - self._partition_shift)

    def segment_for_pid(self, pid: int) -> LogicalPartition:
        """Return the LogicalPartition arc for ``pid``.

        Raise ValueError when ``pid`` is outside ``[0, total_partitions)``.
        The last partition's upper bound is pinned to ``hash_space.max``
        so the union of all arcs covers the full hash space exactly.

        Note: the final partition's ``end`` may be equal to ``hash_space.max``
        which is numerically equal to ``2**bits`` and therefore lies just
        outside the canonical ``[0, hash_space.max)`` domain. This choice
        is deliberate: interval membership tests are right-closed (``<=``)
        so using ``hash_space.max`` as the last upper bound yields a simple
        non-wrapping representation for the final arc.
        """
        total = self.total_partitions
        if pid < 0 or pid >= total:
            raise ValueError(f"pid {pid} out of range [0, {total})")
        start = pid * self._step
        end = (pid + 1) * self._step if pid < total - 1 else self._hash_space.max
        return LogicalPartition(pid=pid, start=start, end=end)

    def placement_for_token(self, token: int, ring: Ring) -> PartitionPlacement:
        """Resolve the PartitionPlacement that owns ``token`` on ``ring``.

        Composes the static partition grid with the ring's successor
        lookup: the partition is derived from ``token`` alone, while the
        owner VNode is the ring successor of ``token``. ``IndexError``
        from ``Ring.successor`` is propagated unchanged so that callers
        observe the empty-ring condition explicitly.
        """
        pid = self.pid_for_hash(token)
        partition = self.segment_for_pid(pid)
        vnode = ring.successor(token)
        return PartitionPlacement(partition=partition, vnode=vnode)
