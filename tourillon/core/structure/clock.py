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
"""Hybrid Logical Clock timestamp and stateful clock."""

from dataclasses import dataclass
from typing import Self


@dataclass(frozen=True, order=True)
class HLCTimestamp:
    """Immutable hybrid logical clock timestamp providing a strict total order.

    A HLCTimestamp combines a physical wall-clock reading in milliseconds, a
    logical monotonic counter, and the originating node identifier. Together
    these three fields produce a total order over all events in the distributed
    system: events are first compared by wall time, then by counter when wall
    times are equal, and finally by node_id as a deterministic tiebreaker when
    both wall and counter match. This guarantees that no two distinct timestamps
    are ever considered equal unless they share all three fields, which is
    structurally impossible for events produced by different nodes at the same
    logical instant.

    The dataclass field declaration order (wall, counter, node_id) participates
    directly in the generated comparison operators, so callers must never
    reorder the fields.
    """

    wall: int
    counter: int
    node_id: str

    def tick(self, now_ms: int) -> "HLCTimestamp":
        """Produce a new timestamp strictly greater than this one.

        The new wall value is max(self.wall, now_ms), guaranteeing monotonicity
        even when the physical clock moves backwards due to clock skew. When
        now_ms equals the current wall, the counter increments to preserve
        strict ordering within the same millisecond. When now_ms advances the
        wall, the counter resets to zero.
        """
        new_wall = max(self.wall, now_ms)
        if new_wall == self.wall:
            return HLCTimestamp(
                wall=new_wall, counter=self.counter + 1, node_id=self.node_id
            )
        return HLCTimestamp(wall=new_wall, counter=0, node_id=self.node_id)

    def update(self, remote: "HLCTimestamp", now_ms: int) -> "HLCTimestamp":
        """Merge this timestamp with a remote one, preserving causal ordering.

        The resulting timestamp is strictly greater than both this timestamp and
        the remote one, ensuring that any subsequent event on this node causally
        follows both inputs. The new wall is max(self.wall, remote.wall, now_ms).
        When the winning wall matches both local and remote walls simultaneously,
        the counter advances past max(local.counter, remote.counter). When only
        one side wins the wall comparison, the counter advances past that side's
        counter. When now_ms wins outright, the counter resets to zero.
        """
        new_wall = max(self.wall, remote.wall, now_ms)
        if new_wall == self.wall == remote.wall:
            new_counter = max(self.counter, remote.counter) + 1
        elif new_wall == self.wall:
            new_counter = self.counter + 1
        elif new_wall == remote.wall:
            new_counter = remote.counter + 1
        else:
            new_counter = 0
        return HLCTimestamp(wall=new_wall, counter=new_counter, node_id=self.node_id)

    def to_dict(self) -> dict[str, int | str]:
        """Serialise this timestamp into a plain dict suitable for wire encoding.

        The returned dict carries three keys — wall, counter, node_id — that are
        sufficient to reconstruct an identical HLCTimestamp via from_dict. The
        representation is binary-transparent when encoded with msgpack, and
        round-trips losslessly through any format that preserves int and str.
        """
        return {"wall": self.wall, "counter": self.counter, "node_id": self.node_id}

    @classmethod
    def from_dict(cls, data: dict[str, int | str]) -> Self:
        """Reconstruct an HLCTimestamp from its dict representation.

        The dict must carry integer wall and counter values and a string node_id,
        as produced by to_dict. Passing a dict with missing or wrongly typed keys
        raises KeyError or ValueError at the point of the failing conversion.
        """
        return cls(
            wall=int(data["wall"]),
            counter=int(data["counter"]),
            node_id=str(data["node_id"]),
        )


class HLCClock:
    """Mutable stateful hybrid logical clock bound to a single node.

    HLCClock maintains the current highest-known timestamp for its node and
    advances it monotonically through two operations. tick advances the state
    for a locally generated event, and update advances the state after receiving
    a message from a remote node. Both operations mutate the internal state and
    return the new timestamp so that callers can stamp their event or message
    with the precise value that was recorded.

    A single HLCClock instance must not be shared across concurrent coroutines
    without external synchronisation, as concurrent mutations to state would
    violate the monotonicity guarantee.
    """

    def __init__(self, node_id: str, initial_wall: int = 0) -> None:
        self.node_id: str = node_id
        self.state: HLCTimestamp = HLCTimestamp(
            wall=initial_wall, counter=0, node_id=node_id
        )

    def tick(self, now_ms: int) -> HLCTimestamp:
        """Advance the clock for a locally generated event and return the new timestamp."""
        self.state = self.state.tick(now_ms)
        return self.state

    def update(self, remote: HLCTimestamp, now_ms: int) -> HLCTimestamp:
        """Advance the clock after receiving a remote timestamp and return the merged result."""
        self.state = self.state.update(remote, now_ms)
        return self.state
