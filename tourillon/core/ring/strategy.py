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
"""Placement strategies turning a PartitionPlacement into a preference list."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol, runtime_checkable

from tourillon.core.ring.partitioner import PartitionPlacement
from tourillon.core.ring.ring import Ring
from tourillon.core.structure.membership import MemberPhase

_ELIGIBLE_PHASES: frozenset[MemberPhase] = frozenset(
    {MemberPhase.READY, MemberPhase.DRAINING}
)


@runtime_checkable
class PlacementStrategy(Protocol):
    """Pure function turning a placement and a ring into a preference list.

    A PlacementStrategy maps the (partition, owner-vnode) binding produced
    by routing into the ordered list of physical node identifiers that
    should hold a replica of the partition. The contract is intentionally
    narrow:

    * The result is **deterministic** — for identical inputs (same
      placement, same ring snapshot, same external membership view) the
      strategy must return the same list.
    * The result length is **at most** the configured replication factor,
      but **may be shorter**. A shorter list is a normal outcome during
      bootstrap, drain, or on small clusters where fewer than ``rf``
      eligible nodes exist; it is not an error condition.
    * The strategy is a **pure function** of its arguments. Implementations
      must not mutate state, perform I/O, sleep, read clocks, or use
      randomness.

    Marked ``runtime_checkable`` so that test suites can use
    ``isinstance(obj, PlacementStrategy)`` to assert protocol conformance.
    """

    def preference_list(
        self,
        placement: PartitionPlacement,
        ring: Ring,
    ) -> list[str]: ...


class SimplePreferenceStrategy:
    """Walk the ring clockwise from the placement, picking eligible distinct nodes.

    Implements the canonical Dynamo-style preference walk described in
    ``docs/ring.md`` §5.1: starting from the placement's owner vnode, the
    ring is traversed in clockwise order; each physical ``node_id`` is
    considered exactly once (subsequent vnodes belonging to the same node
    are skipped) and is appended to the result only when its membership
    phase is eligible — that is ``READY`` or ``DRAINING``. Phases
    ``JOINING`` and ``IDLE`` and unknown nodes (``phase_for`` returns
    ``None``) are skipped but still consumed once so that the algorithm
    terminates cleanly even on adversarial layouts.

    Eligibility rationale (``docs/ring.md`` §5.2):

    * ``READY``    — fully synced replica, eligible for reads and writes.
    * ``DRAINING`` — still holds the full data set, eligible until removal.
    * ``JOINING``  — partial data only, ineligible.
    * ``IDLE``     — no data, ineligible.
    * ``None``     — gossip view has not yet observed the node, ineligible.

    The strategy carries no mutable state: ``rf`` and the membership
    accessor are bound at construction and ``preference_list`` is a pure
    function of its arguments.
    """

    __slots__ = ("_phase_for", "_rf")

    def __init__(
        self,
        rf: int,
        phase_for: Callable[[str], MemberPhase | None],
    ) -> None:
        """Bind the replication factor and the membership phase accessor.

        ``rf`` must be at least one; a non-positive replication factor has
        no operational meaning and is rejected with ``ValueError``.
        ``phase_for`` is the read-only projection of the membership view
        used to test node eligibility; it must be safe to call repeatedly
        and return ``None`` for unknown node identifiers.
        """
        if rf < 1:
            raise ValueError(f"rf must be >= 1, got {rf}")
        self._rf: int = rf
        self._phase_for: Callable[[str], MemberPhase | None] = phase_for

    def preference_list(
        self,
        placement: PartitionPlacement,
        ring: Ring,
    ) -> list[str]:
        """Return up to ``rf`` distinct eligible node identifiers.

        The walk starts at ``placement.vnode`` via ``ring.iter_from`` and
        proceeds clockwise. Each ``node_id`` encountered for the first
        time is recorded in a ``seen`` set so that further vnodes of the
        same physical node are ignored regardless of eligibility. Only
        nodes whose phase is in ``_ELIGIBLE_PHASES`` are appended to the
        result. Iteration stops as soon as ``rf`` eligible nodes have
        been collected, or when the walk is exhausted — in which case a
        list strictly shorter than ``rf`` is returned without error.
        """
        result: list[str] = []
        seen: set[str] = set()
        for vnode in ring.iter_from(placement.vnode):
            node_id = vnode.node_id
            if node_id in seen:
                continue
            seen.add(node_id)
            phase = self._phase_for(node_id)
            if phase is None or phase not in _ELIGIBLE_PHASES:
                continue
            result.append(node_id)
            if len(result) == self._rf:
                break
        return result
