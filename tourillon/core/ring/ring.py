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
"""Immutable sorted virtual-node ring with O(log n) successor lookup."""

from __future__ import annotations

import bisect
from collections.abc import Iterable, Iterator, Sequence

from tourillon.core.ring.vnode import VNode


class Ring:
    """Immutable, token-sorted sequence of VNodes forming a consistent-hash ring.

    The ring maintains its VNodes in ascending token order. Successor lookups
    use bisect.bisect_right with a key function over the vnode tuple directly,
    keeping the implementation simple without a parallel token structure. The
    ring is logically circular: when a lookup would fall past the last token it
    wraps around to index zero, and clockwise traversal via iter_from continues
    past the end of the sequence back to the beginning.

    All mutation operations are pure: add_vnodes and drop_nodes return a new
    Ring instance and never modify the original. Ring carries no versioning
    metadata; topology epoch tracking is the responsibility of the component
    that manages ring transitions (e.g. the membership or gossip layer).
    """

    __slots__ = ("_vnodes",)

    def __init__(self, vnodes: Iterable[VNode], *, _sorted: bool = False) -> None:
        """Initialise a Ring from an iterable of VNodes.

        When _sorted is False (the default) the iterable is sorted by ascending
        token before storage. Pass _sorted=True only when the caller has already
        produced a sorted sequence — all internal factory paths set this flag to
        avoid redundant work. External callers should prefer Ring.empty or
        Ring.from_vnodes.
        """
        self._vnodes: tuple[VNode, ...] = (
            tuple(vnodes) if _sorted else tuple(sorted(vnodes, key=lambda v: v.token))
        )

    @classmethod
    def empty(cls) -> Ring:
        """Return an empty Ring.

        Calling successor on an empty Ring raises IndexError.
        """
        return cls([], _sorted=True)

    @classmethod
    def from_vnodes(cls, vnodes: Iterable[VNode]) -> Ring:
        """Build a Ring by sorting vnodes by ascending token.

        The input iterable is materialised once and sorted in O(n log n).
        Duplicate tokens or duplicate (node_id, token) pairs are accepted
        silently because deduplication is the responsibility of the
        bootstrap layer that knows the token-generation policy.
        """
        return cls(list(vnodes))

    @property
    def vnodes(self) -> tuple[VNode, ...]:
        """Return the VNodes in ascending token order.

        The returned tuple is the Ring's internal, immutable snapshot. Callers
        that require a mutable copy may call list(ring.vnodes).
        """
        return self._vnodes

    def __len__(self) -> int:
        """Return the number of VNodes currently placed on the Ring."""
        return len(self._vnodes)

    def __getitem__(self, i: int) -> VNode:
        """Return the VNode at position i in ascending token order."""
        return self._vnodes[i]

    def __iter__(self) -> Iterator[VNode]:
        """Iterate over all VNodes in ascending token order."""
        return iter(self._vnodes)

    def __repr__(self) -> str:
        """Return a concise developer representation of this Ring."""
        return f"Ring(vnodes={self._vnodes!r})"

    def add_vnodes(self, new_vnodes: Iterable[VNode]) -> Ring:
        """Return a new Ring with new_vnodes merged into the existing sequence.

        The incoming VNodes are first sorted by token in O(k log k); the
        merge with the existing sorted tuple is then a single linear pass for
        an overall cost of O(k log k + n). The original Ring is untouched.
        Duplicates are accepted silently; the merge is stable, preserving the
        relative order of equal-token VNodes between the existing sequence and
        the incoming batch.
        """
        incoming = sorted(new_vnodes, key=lambda v: v.token)
        if not incoming:
            return Ring(self._vnodes, _sorted=True)
        merged = Ring._merge_sorted(self._vnodes, incoming)
        return Ring(merged, _sorted=True)

    def drop_nodes(self, node_ids: frozenset[str]) -> Ring:
        """Return a new Ring with every VNode of node_ids removed.

        The filter runs in a single linear pass over the sorted vnode tuple,
        preserving relative order so the result remains sorted without a
        re-sort. node_ids absent from the Ring are silently ignored, keeping
        the operation idempotent from the caller's perspective.
        """
        if not node_ids:
            return Ring(self._vnodes, _sorted=True)
        kept = [v for v in self._vnodes if v.node_id not in node_ids]
        return Ring(kept, _sorted=True)

    def successor(self, token: int) -> VNode:
        """Return the VNode responsible for token using O(log n) bisect_right.

        The lookup finds the first VNode whose token is strictly greater than
        the search token. When the index falls past the end of the sequence it
        wraps around to index zero, expressing the ring's circular topology.
        Raises IndexError when the Ring is empty.
        """
        if not self._vnodes:
            raise IndexError("Ring.successor called on an empty ring")
        idx = bisect.bisect_right(self._vnodes, token, key=lambda v: v.token)
        if idx == len(self._vnodes):
            idx = 0
        return self._vnodes[idx]

    def iter_from(self, vnode: VNode) -> Iterator[VNode]:
        """Yield every VNode in clockwise ring order starting from vnode.

        The starting VNode is located by value equality; a ValueError is
        raised if it is not present so that callers detect stale references
        early. Iteration wraps around past the last index and ends after
        every VNode has been produced exactly once. The ring layer has no
        notion of replication factor or eligibility — the caller decides
        when to stop consuming the iterator.
        """
        try:
            start = self._vnodes.index(vnode)
        except ValueError as exc:
            raise ValueError(f"vnode {vnode!r} is not present in the ring") from exc
        n = len(self._vnodes)
        for offset in range(n):
            yield self._vnodes[(start + offset) % n]

    @staticmethod
    def _merge_sorted(a: Sequence[VNode], b: Sequence[VNode]) -> list[VNode]:
        """Merge two token-sorted VNode sequences into a single sorted list.

        Performs a standard two-pointer merge in O(n + k). Both input sequences
        must already be sorted by ascending token; the result preserves that
        order with equal-token entries from a preceding entries from b.
        """
        merged: list[VNode] = []
        i = j = 0
        len_a, len_b = len(a), len(b)
        while i < len_a and j < len_b:
            if a[i].token <= b[j].token:
                merged.append(a[i])
                i += 1
            else:
                merged.append(b[j])
                j += 1
        if i < len_a:
            merged.extend(a[i:])
        if j < len_b:
            merged.extend(b[j:])
        return merged
