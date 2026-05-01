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
"""Virtual-node value object placed on the consistent-hash ring."""

from dataclasses import dataclass


@dataclass(frozen=True)
class VNode:
    """Immutable association between a node identifier and a ring token.

    A VNode is the unit of placement on the consistent-hash ring: each
    physical node owns several VNodes, one per token assigned to it during
    bootstrap. The token is a non-negative integer position inside the
    HashSpace that backs the ring; ownership of a key is resolved by
    locating the smallest token greater than or equal to the key's hash
    along the circular order.

    VNode deliberately exposes no comparison operators. Ordering on the
    ring is a property of the surrounding ring structure — it depends on
    both token value and tie-breaking rules that involve other VNodes —
    and must therefore be expressed by the Ring class rather than by
    pairwise dunder methods on VNode itself. Callers that need to order
    a list of VNodes pass them through the Ring's sorting logic.
    """

    node_id: str
    token: int

    def __post_init__(self) -> None:
        """Validate that the VNode fields satisfy the ring invariants.

        Raise ValueError when node_id is empty or when token is negative,
        since both conditions would make the VNode unusable as a ring
        placement.
        """
        if not self.node_id:
            raise ValueError("node_id must not be empty")
        if self.token < 0:
            raise ValueError(f"token must be non-negative, got {self.token}")
