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
"""TopologyManager tests — scenarios 7, 8, 15."""

from __future__ import annotations

import pytest

from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.lifecycle.registry import MemberRegistry
from tourillon.core.ring.topology import TopologyManager

pytestmark = pytest.mark.ring


def _member(
    node_id: str,
    phase: MemberPhase,
    tokens: tuple[int, ...] = (10, 50),
    generation: int = 1,
    seq: int = 0,
) -> Member:
    return Member(
        node_id=node_id,
        peer_address=f"{node_id}:7701",
        generation=generation,
        seq=seq,
        phase=phase,
        tokens=tokens,
        partition_shift=10,
    )


@pytest.mark.ring
async def test_7_apply_member_joining_to_ready_adds_vnodes_and_advances_epoch() -> None:
    """Returns True; snapshot ring includes B's vnodes; epoch advanced by 1."""
    mgr = TopologyManager()

    # B starts JOINING
    b_joining = _member("node-b", MemberPhase.JOINING, tokens=(30, 90))
    await mgr.apply_member(b_joining)
    snap_before = await mgr.snapshot()
    assert snap_before.epoch == 0  # JOINING does not advance epoch
    assert len(snap_before.ring) == 0  # not in ring yet

    # B transitions to READY
    b_ready = _member("node-b", MemberPhase.READY, tokens=(30, 90), seq=1)
    modified = await mgr.apply_member(b_ready)

    assert modified is True
    snap = await mgr.snapshot()
    assert snap.epoch == 1
    ring_ids = {v.node_id for v in snap.ring}
    assert "node-b" in ring_ids
    assert len(snap.ring) == 2  # two tokens


@pytest.mark.ring
async def test_8_apply_member_same_record_twice_returns_false() -> None:
    """Returns False; snapshot unchanged."""
    mgr = TopologyManager()
    member = _member("node-b", MemberPhase.READY)
    await mgr.apply_member(member)
    snap_before = await mgr.snapshot()

    modified = await mgr.apply_member(member)

    assert modified is False
    snap_after = await mgr.snapshot()
    assert snap_after.epoch == snap_before.epoch


@pytest.mark.ring
async def test_15_merge_registry_is_atomic() -> None:
    """A snapshot taken immediately after contains all 5 new members; no partial merge."""
    mgr = TopologyManager()

    registry = MemberRegistry()
    for i in range(5):
        m = _member(f"node-{i}", MemberPhase.READY, tokens=(i * 10,))
        registry.upsert(m)

    await mgr.merge_registry(registry)

    snap = await mgr.snapshot()
    assert len(list(snap.registry)) == 5
    for i in range(5):
        assert snap.registry.get(f"node-{i}") is not None


@pytest.mark.ring
async def test_topology_members_in_phase_returns_matching_members() -> None:
    """members_in_phase() on a Topology snapshot returns all matching members."""
    mgr = TopologyManager()
    await mgr.apply_member(_member("node-r", MemberPhase.READY))
    await mgr.apply_member(_member("node-j", MemberPhase.JOINING))

    snap = await mgr.snapshot()
    ready = snap.members_in_phase(MemberPhase.READY)
    assert "node-r" in ready
    assert "node-j" not in ready


@pytest.mark.ring
async def test_topology_active_node_ids_includes_ready_draining_paused() -> None:
    """active_node_ids returns READY, DRAINING, and PAUSED node_ids only."""
    mgr = TopologyManager()
    await mgr.apply_member(_member("r", MemberPhase.READY, tokens=(10, 50)))
    # D needs to go through READY first to get vnodes in ring
    await mgr.apply_member(_member("d", MemberPhase.READY, tokens=(20, 60), seq=0))
    await mgr.apply_member(_member("d", MemberPhase.DRAINING, tokens=(20, 60), seq=1))
    await mgr.apply_member(_member("j", MemberPhase.JOINING))

    snap = await mgr.snapshot()
    active = snap.active_node_ids
    assert "r" in active
    assert "d" in active
    assert "j" not in active


@pytest.mark.ring
async def test_topology_draining_to_idle_drops_vnodes_from_ring() -> None:
    """DRAINING → IDLE transition removes node vnodes from ring and increments epoch."""
    mgr = TopologyManager()
    # Put node into ring via READY
    await mgr.apply_member(
        _member("node-d", MemberPhase.READY, tokens=(50, 150), seq=0)
    )
    snap1 = await mgr.snapshot()
    assert len(snap1.ring) == 2
    epoch_after_ready = snap1.epoch

    # Transition to DRAINING (vnodes stay)
    await mgr.apply_member(
        _member("node-d", MemberPhase.DRAINING, tokens=(50, 150), seq=1)
    )

    # Transition to IDLE (vnodes removed)
    await mgr.apply_member(_member("node-d", MemberPhase.IDLE, tokens=(), seq=2))
    snap2 = await mgr.snapshot()
    assert len(snap2.ring) == 0
    assert snap2.epoch > epoch_after_ready
