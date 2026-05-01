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
"""Membership phase enumeration and gossip-exchange Member value object."""

import enum
from dataclasses import dataclass


class MemberPhase(enum.Enum):
    """Self-declared operational phase that a node propagates via gossip.

    The phase is the node's own statement of what it is currently doing in the
    cluster lifecycle. It is orthogonal to local reachability classifications
    such as REACHABLE, SUSPECT, or DEAD, which are private per-peer
    observations and are never gossiped. See docs/membership.md for the
    normative finite-state machine and transition rules.
    """

    IDLE = "idle"
    JOINING = "joining"
    READY = "ready"
    DRAINING = "draining"


@dataclass(frozen=True)
class Member:
    """Immutable gossip-exchange record describing a single cluster member.

    A Member is the unit of membership data exchanged between nodes during
    gossip reconciliation. It carries the stable logical identity of the
    node, its current gossip endpoint, the merge-precedence keys generation
    and seq, and the self-declared operational phase.

    The generation field is a monotonically increasing restart counter that
    advances each time the node re-joins from IDLE. It guarantees that
    records from a previous incarnation of the same node_id are silently
    discarded when the node comes back. The seq field is a monotonically
    increasing version counter that advances on every gossip emission so
    that concurrent records within the same generation can be totally
    ordered for merge purposes.

    Local reachability is deliberately absent from this record. Routing
    decisions that combine ring membership with reachability obtain the
    latter from the gossip engine's private state, never from a Member.
    """

    node_id: str
    peer_address: str
    generation: int
    seq: int
    phase: MemberPhase

    def __post_init__(self) -> None:
        """Validate that all field invariants hold for this Member.

        Raise ValueError with a descriptive message for whichever field
        violates its constraint so that callers can identify the problem
        without inspecting the full object. node_id and peer_address must
        be non-empty strings; generation and seq must be non-negative
        integers.
        """
        if not self.node_id:
            raise ValueError("node_id must not be empty")
        if not self.peer_address:
            raise ValueError("peer_address must not be empty")
        if self.generation < 0:
            raise ValueError(f"generation must be non-negative, got {self.generation}")
        if self.seq < 0:
            raise ValueError(f"seq must be non-negative, got {self.seq}")

    def supersedes(self, other: "Member") -> bool:
        """Return True when this Member should win merge against other.

        Merge precedence is determined by the lexicographic pair
        (generation, seq): a strictly greater generation wins
        unconditionally, and ties on generation defer to a strictly greater
        seq. Equal pairs do not supersede each other and the method returns
        False, leaving the existing record in place.

        This method does not inspect node_id; callers are responsible for
        only invoking it on records that share the same logical identity.
        """
        if self.generation != other.generation:
            return self.generation > other.generation
        return self.seq > other.seq
