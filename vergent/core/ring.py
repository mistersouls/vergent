import bisect
from typing import Iterable, Generator, cast

from vergent.core.model.token_ import Token
from vergent.core.model.vnode import VNode


class Ring:
    """
    Ring represents the ordered set of VNodes on the 128-bit hash ring.

    - VNodes are sorted by their token.
    - find_successor() locates the vnode responsible for a given token.
    - preference_list() returns N distinct physical nodes in ring order.
    """

    def __init__(self, vnodes: list[VNode] = None, *, _sorted: bool = False) -> None:
        vnodes = vnodes or []
        self._vnodes = vnodes if _sorted else sorted(vnodes)

    def find_successor(self, token: int) -> VNode:
        """
        Return the vnode responsible for this token.

        We bisect using the tuple (token, "") as a search key.
        VNode inherits from tuple, so lexicographical comparison works:
        (token, "") compares correctly against (vnode.token, vnode.node_id).
        The empty string ensures node_id is never consulted unless tokens match,
        which should never happen in a well-formed ring.
        """
        idx = bisect.bisect_right(self._vnodes, (token, ""))  # type: ignore[arg-type]
        if idx == len(self._vnodes):
            idx = 0  # wrap-around
        return self._vnodes[idx]

    def update_ring(self, vnodes: list[VNode]) -> Ring:
        """
        Add multiple vnodes efficiently by sorting locally and merging.

        Complexity:
            - local sort: O(k log k)
            - merge: O(n + k)
        This is significantly faster than inserting one-by-one.
        """
        if not vnodes:
            return self

        new_vnodes = sorted(vnodes)
        sorted_vnodes = self._merge_sorted(self._vnodes, new_vnodes)
        return Ring(sorted_vnodes, _sorted=False)

    def index_of(self, vnode: VNode) -> int:
        return self._vnodes.index(vnode)

    def __getitem__(self, i: int) -> VNode:
        return self._vnodes[i]

    def __len__(self) -> int:
        return len(self._vnodes)

    @staticmethod
    def _merge_sorted(a: list[VNode], b: list[VNode]) -> list[VNode]:
        """
        Merge two sorted lists of (token, node_id).
        Equivalent to a manual merge step in merge sort.
        """
        i = j = 0
        merged: list[VNode] = []
        len_a, len_b = len(a), len(b)

        while i < len_a and j < len_b:
            if a[i].token <= b[j].token:
                merged.append(a[i])
                i += 1
            else:
                merged.append(b[j])
                j += 1

        # Append remaining items
        if i < len_a:
            merged.extend(a[i:])
        if j < len_b:
            merged.extend(b[j:])

        return merged

    def iter_from(self, vnode: VNode) -> Generator[VNode, None, None]:
        """Yield vnodes in ring order starting from vnode."""
        start = self._vnodes.index(vnode)
        for i in range(len(self._vnodes)):
            yield self._vnodes[(start + i) % len(self._vnodes)]

    def preference_list(self, vnode: VNode, replication_factor: int) -> list[VNode]:
        """
        Return N distinct node_ids in ring order starting from the successor.

        Skips vnodes belonging to the same physical node.
        """
        result: list[VNode] = [vnode]
        seen = {vnode.node_id}

        for successor in self.iter_from(vnode):
            if successor.node_id not in seen:
                result.append(successor)
                seen.add(successor.node_id)
            if len(result) == replication_factor:
                break

        return result
