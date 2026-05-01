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
"""Tests for tourillon.core.ring.strategy — preference list construction."""

import pytest

from tourillon.core.ring import (
    HashSpace,
    Partitioner,
    PartitionPlacement,
    PlacementStrategy,
    Ring,
    SimplePreferenceStrategy,
    VNode,
)
from tourillon.core.structure.membership import MemberPhase

# HashSpace bits=8, 8 partitions
hs = HashSpace(bits=8)
pt = Partitioner(hs, partition_shift=3)


def _ring(*tokens: int) -> Ring:
    """Build a Ring from positional token integers, node_ids n0, n1, ..."""
    return Ring.from_vnodes([VNode(f"n{i}", t) for i, t in enumerate(tokens)])


def _phase_all_ready(node_id: str) -> MemberPhase:
    return MemberPhase.READY


def _placement(token: int, ring: Ring) -> PartitionPlacement:
    return pt.placement_for_token(token, ring)


def test_rf_zero_raises() -> None:
    with pytest.raises(ValueError):
        SimplePreferenceStrategy(rf=0, phase_for=_phase_all_ready)


def test_rf_negative_raises() -> None:
    with pytest.raises(ValueError):
        SimplePreferenceStrategy(rf=-1, phase_for=_phase_all_ready)


def test_rf_one_valid() -> None:
    SimplePreferenceStrategy(rf=1, phase_for=_phase_all_ready)


def test_isinstance_protocol() -> None:
    strat = SimplePreferenceStrategy(rf=1, phase_for=lambda _: MemberPhase.READY)
    assert isinstance(strat, PlacementStrategy)


def test_preference_list_all_ready_returns_rf_nodes() -> None:
    r = _ring(10, 90, 170, 240)
    placement = _placement(5, r)
    strat = SimplePreferenceStrategy(rf=2, phase_for=_phase_all_ready)
    got = strat.preference_list(placement, r)
    assert len(got) == 2


def test_preference_list_result_starts_from_successor() -> None:
    r = _ring(10, 90, 170, 240)
    placement = _placement(5, r)
    strat = SimplePreferenceStrategy(rf=1, phase_for=_phase_all_ready)
    got = strat.preference_list(placement, r)
    # successor of token 5 is vnode with token 10 -> node_id 'n0'
    assert got[0] == "n0"


def test_preference_list_ordering_follows_ring() -> None:
    r = _ring(10, 90, 170, 240)
    placement = _placement(5, r)
    strat = SimplePreferenceStrategy(rf=3, phase_for=_phase_all_ready)
    got = strat.preference_list(placement, r)
    assert got == ["n0", "n1", "n2"]


def test_draining_is_eligible() -> None:
    r = _ring(10, 90, 170)

    def phase_for(nid: str) -> MemberPhase | None:
        if nid == "n1":
            return MemberPhase.DRAINING
        return MemberPhase.READY

    placement = _placement(5, r)
    strat = SimplePreferenceStrategy(rf=3, phase_for=phase_for)
    got = strat.preference_list(placement, r)
    assert "n1" in got


def test_joining_is_ineligible() -> None:
    r = _ring(10, 90, 170)

    def phase_for(nid: str) -> MemberPhase | None:
        if nid == "n1":
            return MemberPhase.JOINING
        return MemberPhase.READY

    placement = _placement(5, r)
    strat = SimplePreferenceStrategy(rf=3, phase_for=phase_for)
    got = strat.preference_list(placement, r)
    assert "n1" not in got


def test_idle_is_ineligible() -> None:
    r = _ring(10, 90, 170)

    def phase_for(nid: str) -> MemberPhase | None:
        if nid == "n1":
            return MemberPhase.IDLE
        return MemberPhase.READY

    placement = _placement(5, r)
    strat = SimplePreferenceStrategy(rf=3, phase_for=phase_for)
    got = strat.preference_list(placement, r)
    assert "n1" not in got


def test_none_phase_is_ineligible() -> None:
    r = _ring(10, 90, 170)

    def phase_for(nid: str) -> MemberPhase | None:
        if nid == "n1":
            return None
        return MemberPhase.READY

    placement = _placement(5, r)
    strat = SimplePreferenceStrategy(rf=3, phase_for=phase_for)
    got = strat.preference_list(placement, r)
    assert "n1" not in got


def test_multi_vnodes_same_node_counted_once() -> None:
    # create a ring where 'n0' has two vnodes
    r = Ring.from_vnodes([VNode("n0", 10), VNode("n1", 90), VNode("n0", 150)])
    placement = pt.placement_for_token(5, r)
    strat = SimplePreferenceStrategy(rf=2, phase_for=_phase_all_ready)
    got = strat.preference_list(placement, r)
    # should contain two distinct node ids
    assert len(got) == 2
    assert got.count("n0") == 1


def test_seen_prevents_recount_of_ineligible_node() -> None:
    # 'bad' has two vnodes but is IDLE -> must not appear even via second vnode
    r = Ring.from_vnodes(
        [VNode("good", 10), VNode("bad", 50), VNode("bad", 90), VNode("g2", 140)]
    )

    def phase_for(nid: str) -> MemberPhase | None:
        if nid == "bad":
            return MemberPhase.IDLE
        return MemberPhase.READY

    placement = pt.placement_for_token(5, r)
    strat = SimplePreferenceStrategy(rf=3, phase_for=phase_for)
    got = strat.preference_list(placement, r)
    assert "bad" not in got


def test_fewer_eligible_than_rf_returns_shorter_list() -> None:
    # only two READY nodes on ring, rf=3
    r = _ring(10, 90)
    placement = _placement(5, r)
    strat = SimplePreferenceStrategy(rf=3, phase_for=_phase_all_ready)
    got = strat.preference_list(placement, r)
    assert len(got) == 2


def test_empty_ring_placement_vnode_not_in_ring_raises() -> None:
    r = _ring(10, 90)
    # build a placement whose vnode is not present in r
    seg = pt.segment_for_pid(0)
    fake_vnode = VNode("missing", 1)
    placement = PartitionPlacement(partition=seg, vnode=fake_vnode)
    strat = SimplePreferenceStrategy(rf=1, phase_for=_phase_all_ready)
    with pytest.raises(ValueError):
        strat.preference_list(placement, r)


def test_preference_list_same_inputs_idempotent() -> None:
    r = _ring(10, 90, 170)
    placement = _placement(5, r)
    strat = SimplePreferenceStrategy(rf=2, phase_for=_phase_all_ready)
    a = strat.preference_list(placement, r)
    b = strat.preference_list(placement, r)
    assert a == b


def test_preference_list_does_not_mutate_strategy() -> None:
    r = _ring(10, 90, 170)
    placement = _placement(5, r)
    strat = SimplePreferenceStrategy(rf=2, phase_for=_phase_all_ready)
    before_rf = strat._rf
    strat.preference_list(placement, r)
    after_rf = strat._rf
    assert before_rf == after_rf


def test_determinism_two_instances_same_result() -> None:
    r = _ring(10, 90, 170)
    placement = _placement(5, r)

    def phase_for(nid: str) -> MemberPhase | None:
        return MemberPhase.READY

    s1 = SimplePreferenceStrategy(rf=2, phase_for=phase_for)
    s2 = SimplePreferenceStrategy(rf=2, phase_for=phase_for)
    assert s1.preference_list(placement, r) == s2.preference_list(placement, r)


def test_public_surface_all_symbols() -> None:
    import tourillon.core.ring as ring_mod

    for name in ring_mod.__all__:
        getattr(ring_mod, name)
