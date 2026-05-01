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
"""Tests for tourillon.core.structure.membership — Member and MemberPhase."""

import dataclasses

import pytest

from tourillon.core.structure.membership import Member, MemberPhase


def test_member_phase_values() -> None:
    """MemberPhase must expose the four expected string values."""
    assert MemberPhase.IDLE.value == "idle"
    assert MemberPhase.JOINING.value == "joining"
    assert MemberPhase.READY.value == "ready"
    assert MemberPhase.DRAINING.value == "draining"


def test_member_valid_construction() -> None:
    """Constructing a Member with valid fields must succeed."""
    m = Member(
        node_id="n1",
        peer_address="127.0.0.1:5000",
        generation=1,
        seq=0,
        phase=MemberPhase.READY,
    )
    assert m.node_id == "n1"


def test_member_empty_node_id_raises() -> None:
    """Empty node_id must raise ValueError."""
    with pytest.raises(ValueError, match="node_id must not be empty"):
        Member(
            node_id="", peer_address="p", generation=0, seq=0, phase=MemberPhase.IDLE
        )


def test_member_empty_peer_address_raises() -> None:
    """Empty peer_address must raise ValueError."""
    with pytest.raises(ValueError, match="peer_address must not be empty"):
        Member(
            node_id="n", peer_address="", generation=0, seq=0, phase=MemberPhase.IDLE
        )


@pytest.mark.parametrize("generation", [-1])
def test_member_negative_generation_raises(generation: int) -> None:
    """Negative generation must raise ValueError."""
    with pytest.raises(ValueError, match="generation must be non-negative"):
        Member(
            node_id="n",
            peer_address="p",
            generation=generation,
            seq=0,
            phase=MemberPhase.IDLE,
        )


@pytest.mark.parametrize("seq", [-1])
def test_member_negative_seq_raises(seq: int) -> None:
    """Negative seq must raise ValueError."""
    with pytest.raises(ValueError, match="seq must be non-negative"):
        Member(
            node_id="n", peer_address="p", generation=0, seq=seq, phase=MemberPhase.IDLE
        )


def test_member_zero_generation_allowed() -> None:
    """Zero generation must be accepted."""
    m = Member(
        node_id="n", peer_address="p", generation=0, seq=1, phase=MemberPhase.IDLE
    )
    assert m.generation == 0


def test_member_zero_seq_allowed() -> None:
    """Zero seq must be accepted."""
    m = Member(
        node_id="n", peer_address="p", generation=1, seq=0, phase=MemberPhase.IDLE
    )
    assert m.seq == 0


def test_member_is_frozen() -> None:
    """Member must be immutable — attribute assignment must raise."""
    m = Member(
        node_id="n", peer_address="p", generation=1, seq=1, phase=MemberPhase.IDLE
    )
    with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
        m.node_id = "other"  # type: ignore[misc]


def test_supersedes_higher_generation_wins() -> None:
    """Higher generation must supersede a lower generation regardless of seq."""
    newer = Member(
        node_id="n", peer_address="p", generation=2, seq=0, phase=MemberPhase.IDLE
    )
    older = Member(
        node_id="n", peer_address="p", generation=1, seq=9999, phase=MemberPhase.IDLE
    )
    assert newer.supersedes(older)


def test_supersedes_lower_generation_loses() -> None:
    """Lower generation must not supersede a higher generation."""
    older = Member(
        node_id="n", peer_address="p", generation=1, seq=0, phase=MemberPhase.IDLE
    )
    newer = Member(
        node_id="n", peer_address="p", generation=2, seq=0, phase=MemberPhase.IDLE
    )
    assert not older.supersedes(newer)


def test_supersedes_same_generation_higher_seq_wins() -> None:
    """Within the same generation, higher seq must win."""
    a = Member(
        node_id="n", peer_address="p", generation=1, seq=5, phase=MemberPhase.IDLE
    )
    b = Member(
        node_id="n", peer_address="p", generation=1, seq=3, phase=MemberPhase.IDLE
    )
    assert a.supersedes(b)


def test_supersedes_same_generation_lower_seq_loses() -> None:
    """Within the same generation, lower seq must not win."""
    a = Member(
        node_id="n", peer_address="p", generation=1, seq=1, phase=MemberPhase.IDLE
    )
    b = Member(
        node_id="n", peer_address="p", generation=1, seq=5, phase=MemberPhase.IDLE
    )
    assert not a.supersedes(b)


def test_supersedes_equal_pair_returns_false() -> None:
    """Equal (generation, seq) pairs must not supersede each other."""
    a = Member(
        node_id="n", peer_address="p", generation=1, seq=1, phase=MemberPhase.IDLE
    )
    b = Member(
        node_id="n", peer_address="p", generation=1, seq=1, phase=MemberPhase.IDLE
    )
    assert not a.supersedes(b)


def test_supersedes_generation_dominates_seq() -> None:
    """A higher generation with low seq must still beat a lower generation with high seq."""
    a = Member(
        node_id="n", peer_address="p", generation=2, seq=0, phase=MemberPhase.IDLE
    )
    b = Member(
        node_id="n", peer_address="p", generation=1, seq=9999, phase=MemberPhase.IDLE
    )
    assert a.supersedes(b)
