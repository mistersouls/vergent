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
"""Tests for tourillon.core.ring.ring — Ring immutable topology."""

from collections.abc import Iterable

import pytest

from tourillon.core.ring.ring import Ring
from tourillon.core.ring.vnode import VNode


def _vnodes_from(tokens: Iterable[int], node_prefix: str = "n") -> list[VNode]:
    """Helper: produce VNode list with given tokens and node ids."""
    return [VNode(node_id=f"{node_prefix}{i}", token=t) for i, t in enumerate(tokens)]


def test_ring_empty_has_zero_len() -> None:
    """Ring.empty() must produce a ring with no vnodes."""
    assert len(Ring.empty()) == 0


def test_ring_from_vnodes_sorts_by_token() -> None:
    """from_vnodes must sort vnodes by ascending token regardless of input order."""
    unsorted = [VNode("n1", 30), VNode("n2", 10), VNode("n3", 20)]
    r = Ring.from_vnodes(unsorted)
    assert tuple(v.token for v in r.vnodes) == (10, 20, 30)


def test_ring_len_matches_vnode_count() -> None:
    """len(ring) must equal the number of vnodes."""
    r = Ring.from_vnodes(_vnodes_from([1, 2, 3]))
    assert len(r) == 3


def test_ring_getitem_returns_smallest_token_first() -> None:
    """ring[0] must be the vnode with the smallest token."""
    r = Ring.from_vnodes(_vnodes_from([50, 10, 20]))
    assert r[0].token == 10


def test_ring_iter_yields_all_vnodes_in_order() -> None:
    """Iterating a ring must yield all vnodes in ascending token order."""
    r = Ring.from_vnodes(_vnodes_from([5, 1, 3]))
    assert tuple(v.token for v in r) == (1, 3, 5)


def test_ring_repr_contains_vnodes() -> None:
    """repr(ring) must reference vnodes."""
    r = Ring.empty()
    assert "vnodes" in repr(r)


def test_successor_empty_raises() -> None:
    """successor on an empty ring must raise IndexError."""
    with pytest.raises(IndexError):
        Ring.empty().successor(10)


def test_successor_normal() -> None:
    """successor must return the first vnode whose token is strictly greater."""
    r = Ring.from_vnodes(_vnodes_from([10, 20, 30]))
    assert r.successor(5).token == 10


def test_successor_exact_token_goes_to_next() -> None:
    """A token equal to a vnode's token is owned by the next vnode (bisect_right)."""
    r = Ring.from_vnodes(_vnodes_from([10, 20, 30]))
    assert r.successor(10).token == 20


def test_successor_wraps_past_last_token() -> None:
    """A token greater than the last vnode token wraps to the first vnode."""
    r = Ring.from_vnodes(_vnodes_from([10, 20, 30]))
    assert r.successor(40).token == 10


def test_successor_equal_to_last_token_wraps() -> None:
    """A token equal to the last vnode token wraps to the first vnode."""
    r = Ring.from_vnodes(_vnodes_from([10, 20, 30]))
    assert r.successor(30).token == 10


def test_add_vnodes_original_unchanged() -> None:
    """add_vnodes must not modify the original ring."""
    base = Ring.from_vnodes(_vnodes_from([10, 30]))
    base.add_vnodes([VNode("nX", 20)])
    assert tuple(v.token for v in base.vnodes) == (10, 30)


def test_add_vnodes_result_contains_merged_vnodes() -> None:
    """add_vnodes must return a ring containing both old and new vnodes sorted."""
    base = Ring.from_vnodes(_vnodes_from([10, 30]))
    new = base.add_vnodes([VNode("nX", 20)])
    assert tuple(v.token for v in new.vnodes) == (10, 20, 30)


def test_add_vnodes_result_sorted() -> None:
    """add_vnodes result must be sorted by token regardless of insertion order."""
    base = Ring.from_vnodes(_vnodes_from([10, 40]))
    new = base.add_vnodes([VNode("a", 30), VNode("b", 20)])
    assert tuple(v.token for v in new.vnodes) == (10, 20, 30, 40)


def test_add_vnodes_empty_input_returns_equivalent_ring() -> None:
    """add_vnodes with empty input must return a ring with the same vnodes."""
    base = Ring.from_vnodes(_vnodes_from([1, 2]))
    new = base.add_vnodes([])
    assert tuple(v.token for v in new.vnodes) == tuple(v.token for v in base.vnodes)


def test_drop_nodes_original_unchanged() -> None:
    """drop_nodes must not modify the original ring."""
    vns = [VNode("a", 1), VNode("b", 2), VNode("a", 3)]
    base = Ring.from_vnodes(vns)
    base.drop_nodes(frozenset({"a"}))
    assert tuple(v.token for v in base.vnodes) == (1, 2, 3)


def test_drop_nodes_removes_correct_vnodes() -> None:
    """drop_nodes must remove all vnodes belonging to the given node_ids."""
    vns = [VNode("n1", 5), VNode("n2", 10), VNode("n1", 15)]
    r = Ring.from_vnodes(vns)
    dropped = r.drop_nodes(frozenset({"n1"}))
    assert all(v.node_id != "n1" for v in dropped.vnodes)
    assert tuple(v.token for v in dropped.vnodes) == (10,)


def test_drop_nodes_empty_set_returns_equivalent_ring() -> None:
    """drop_nodes with empty set must return a ring with the same vnodes."""
    base = Ring.from_vnodes(_vnodes_from([1, 2, 3]))
    new = base.drop_nodes(frozenset())
    assert tuple(v.token for v in new.vnodes) == tuple(v.token for v in base.vnodes)


def test_drop_nodes_absent_node_id_ignored() -> None:
    """drop_nodes must silently ignore node_ids not present in the ring."""
    base = Ring.from_vnodes(_vnodes_from([1, 2]))
    new = base.drop_nodes(frozenset({"nope"}))
    assert tuple(v.token for v in new.vnodes) == tuple(v.token for v in base.vnodes)


def test_drop_nodes_all_nodes_produces_empty_ring() -> None:
    """Dropping all node_ids must produce an empty ring."""
    r = Ring.from_vnodes([VNode("a", 1), VNode("b", 2)])
    assert len(r.drop_nodes(frozenset({"a", "b"}))) == 0


def test_iter_from_missing_vnode_raises() -> None:
    """iter_from must raise ValueError when the vnode is not in the ring."""
    r = Ring.from_vnodes(_vnodes_from([1, 2, 3]))
    with pytest.raises(ValueError):
        list(r.iter_from(VNode("x", 999)))


def test_iter_from_yields_all_vnodes_exactly_once() -> None:
    """iter_from must yield every vnode exactly once."""
    vns = [VNode("a", 1), VNode("b", 2), VNode("c", 3)]
    r = Ring.from_vnodes(vns)
    out = list(r.iter_from(vns[0]))
    assert len(out) == 3
    assert set(out) == set(vns)


def test_iter_from_clockwise_order() -> None:
    """iter_from(C) on [A,B,C,D] must yield [C,D,A,B]."""
    vns = [VNode("A", 10), VNode("B", 20), VNode("C", 30), VNode("D", 40)]
    r = Ring.from_vnodes(vns)
    got = list(r.iter_from(vns[2]))
    assert [v.node_id for v in got] == ["C", "D", "A", "B"]


def test_iter_from_single_vnode_ring() -> None:
    """iter_from on a single-vnode ring must yield that vnode once."""
    v = VNode("solo", 7)
    r = Ring.from_vnodes([v])
    assert list(r.iter_from(v)) == [v]


def test_merge_sorted_empty_both() -> None:
    """Merging two empty sequences must return an empty list."""
    assert Ring._merge_sorted([], []) == []


def test_merge_sorted_empty_a() -> None:
    """Merging an empty sequence with a non-empty one must return the non-empty."""
    b = [VNode("b", 5)]
    assert Ring._merge_sorted([], b) == b


def test_merge_sorted_empty_b() -> None:
    """Merging a non-empty sequence with an empty one must return the non-empty."""
    a = [VNode("a", 1)]
    assert Ring._merge_sorted(a, []) == a


def test_merge_sorted_equal_tokens_a_first() -> None:
    """On equal tokens the merge must be stable: a's entry precedes b's."""
    a = [VNode("a1", 5)]
    b = [VNode("b1", 5)]
    assert Ring._merge_sorted(a, b) == [a[0], b[0]]
