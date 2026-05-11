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
"""Tests for GossipBootstrapper retry loop and seed exchange logic."""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, patch

import pytest

from tourillon.core.gossip.bootstrapper import (
    BootstrapAttemptError,
    BootstrapError,
    BootstrapPartitionShiftError,
    GossipBootstrapper,
)
from tourillon.core.gossip.config import GossipBootstrapConfig
from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.envelope import Envelope
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter

LOCAL_SHIFT = 12


def _make_member(node_id: str, gen: int = 1, seq: int = 0, shift: int = 12) -> Member:
    """Build a test Member with the given fields."""
    return Member(
        node_id=node_id,
        peer_address="10.0.0.1:7701",
        generation=gen,
        seq=seq,
        phase=MemberPhase.READY,
        tokens=(1024, 5120),
        partition_shift=shift,
    )


def _make_gossip_delta_envelope(
    serializer: MsgpackSerializerAdapter,
    members: list[dict],  # type: ignore[type-arg]
    has_more: bool = False,
) -> Envelope:
    """Build a gossip.delta Envelope with serialized member list."""
    payload = serializer.encode(
        {"sender": "node-seed", "has_more": has_more, "members": members}
    )
    return Envelope.create(payload, kind="gossip.delta", schema_id=serializer.schema_id)


class FakeGossipBootstrapper(GossipBootstrapper):
    """Bootstrapper that exposes _attempt via a controllable mock."""

    def __init__(self, attempt_results: list, config: GossipBootstrapConfig) -> None:
        topology_manager = TopologyManager()
        super().__init__(topology_manager, config, LOCAL_SHIFT)
        self._attempt_results = attempt_results
        self._attempt_calls = 0

    async def _attempt(self, seeds: list[str]) -> int:
        idx = min(self._attempt_calls, len(self._attempt_results) - 1)
        result = self._attempt_results[idx]
        self._attempt_calls += 1
        if isinstance(result, Exception):
            raise result
        return result


@pytest.mark.gossip
async def test_12_bootstrap_3_seeds_1_unreachable_seeds_ok_2() -> None:
    """Unreachable seed logged as WARNING; 2 succeed; seeds_ok=2; no retry needed; returns 2."""
    topology_mgr = TopologyManager()
    config = GossipBootstrapConfig(max_retries=3)
    bootstrapper = GossipBootstrapper(topology_mgr, config, LOCAL_SHIFT)

    members_to_merge = [_make_member("node-3", shift=12)]

    call_count = [0]

    async def fake_attempt(seeds: list[str]) -> int:
        call_count[0] += 1
        # 3 seeds, 1 unreachable → seeds_ok=2
        await topology_mgr.merge_registry(members_to_merge)
        return 2

    with patch.object(bootstrapper, "_attempt", side_effect=fake_attempt):
        result = await bootstrapper.run(
            ["10.0.0.1:7701", "10.0.0.2:7701", "10.0.0.3:7701"]
        )

    assert result == 2
    assert call_count[0] == 1  # no retry needed


@pytest.mark.gossip
async def test_13_bootstrap_all_seeds_unreachable_max_retries_3_raises() -> None:
    """3 attempts each logged as WARNING with increasing delay; BootstrapError raised after attempt 3."""
    config = GossipBootstrapConfig(
        max_retries=3, initial_delay_s=0.01, multiplier=2.0, jitter=0.0
    )
    bootstrapper = FakeGossipBootstrapper(
        attempt_results=[
            BootstrapAttemptError("no seed"),
            BootstrapAttemptError("no seed"),
            BootstrapAttemptError("no seed"),
        ],
        config=config,
    )

    with pytest.raises(BootstrapError):
        await bootstrapper.run(["10.0.0.1:7701"])


@pytest.mark.gossip
async def test_14_all_fail_attempt_1_one_responds_attempt_2_success() -> None:
    """Attempt 1 → WARNING + backoff sleep; attempt 2 → seeds_ok=1; success; no BootstrapError."""
    config = GossipBootstrapConfig(
        max_retries=3, initial_delay_s=0.01, multiplier=2.0, jitter=0.0
    )
    bootstrapper = FakeGossipBootstrapper(
        attempt_results=[
            BootstrapAttemptError("no seed"),
            1,  # success on attempt 2
        ],
        config=config,
    )

    result = await bootstrapper.run(["10.0.0.1:7701"])
    assert result == 1


@pytest.mark.gossip
async def test_15_bootstrap_backoff_timing() -> None:
    """Delays ≈ 1s (±10%), 2s (±10%) before final failure; total elapsed ≈ 3s."""
    config = GossipBootstrapConfig(
        initial_delay_s=0.05,
        multiplier=2.0,
        jitter=0.0,
        max_retries=3,
    )
    bootstrapper = FakeGossipBootstrapper(
        attempt_results=[
            BootstrapAttemptError("no seed"),
            BootstrapAttemptError("no seed"),
            BootstrapAttemptError("no seed"),
        ],
        config=config,
    )

    start = time.monotonic()
    with pytest.raises(BootstrapError):
        await bootstrapper.run(["10.0.0.1:7701"])
    elapsed = time.monotonic() - start
    # 0.05 + 0.10 ≈ 0.15 s total sleep; total must be > 0.1 s
    assert elapsed > 0.08


@pytest.mark.gossip
async def test_16_bootstrap_seed_5_members_empty_registry_merges_all() -> None:
    """Full delta received; merge_registry() applies 5 members; known_members=5."""
    topology_mgr = TopologyManager()
    config = GossipBootstrapConfig(max_retries=1)
    bootstrapper = GossipBootstrapper(topology_mgr, config, LOCAL_SHIFT)

    members = [_make_member(f"node-{i}", shift=12) for i in range(5)]

    async def fake_attempt(seeds: list[str]) -> int:
        await topology_mgr.merge_registry(members)
        return 1

    with patch.object(bootstrapper, "_attempt", side_effect=fake_attempt):
        await bootstrapper.run(["10.0.0.1:7701"])

    topo = await topology_mgr.snapshot()
    assert len(list(topo.registry)) == 5


@pytest.mark.gossip
async def test_17_bootstrap_stale_local_record_superseded_by_seed() -> None:
    """Stale record superseded; updated record applied; generation unchanged."""
    topology_mgr = TopologyManager()
    config = GossipBootstrapConfig(max_retries=1)
    bootstrapper = GossipBootstrapper(topology_mgr, config, LOCAL_SHIFT)

    # Simulate local stale state (seq=5)
    stale = _make_member("node-1", gen=1, seq=5, shift=12)
    await topology_mgr.apply_member(stale)

    # Seed has newer record (seq=8)
    fresh = _make_member("node-1", gen=1, seq=8, shift=12)

    async def fake_attempt(seeds: list[str]) -> int:
        await topology_mgr.merge_registry([fresh])
        return 1

    with patch.object(bootstrapper, "_attempt", side_effect=fake_attempt):
        await bootstrapper.run(["10.0.0.1:7701"])

    topo = await topology_mgr.snapshot()
    m = topo.registry.get("node-1")
    assert m is not None
    assert m.seq == 8
    assert m.generation == 1  # generation unchanged


@pytest.mark.gossip
async def test_46_bootstrap_partition_shift_mismatch_raises_immediately() -> None:
    """BootstrapPartitionShiftError raised immediately; no registry write; no retry."""
    topology_mgr = TopologyManager()
    config = GossipBootstrapConfig(max_retries=5)
    bootstrapper = GossipBootstrapper(topology_mgr, config, LOCAL_SHIFT)

    call_count = [0]

    async def fake_attempt(seeds: list[str]) -> int:
        call_count[0] += 1
        raise BootstrapPartitionShiftError(
            "10.0.0.1:7701", seed_shift=10, local_shift=12
        )

    with (
        patch.object(bootstrapper, "_attempt", side_effect=fake_attempt),
        pytest.raises(BootstrapPartitionShiftError),
    ):
        await bootstrapper.run(["10.0.0.1:7701"])

    assert call_count[0] == 1  # never retried


@pytest.mark.gossip
async def test_47_bootstrap_matching_partition_shift_succeeds() -> None:
    """No mismatch; delta applied normally; seeds_ok=1; bootstrap succeeds."""
    topology_mgr = TopologyManager()
    config = GossipBootstrapConfig(max_retries=1)
    bootstrapper = GossipBootstrapper(topology_mgr, config, LOCAL_SHIFT)

    member = _make_member("node-seed", shift=12)

    async def fake_attempt(seeds: list[str]) -> int:
        await topology_mgr.merge_registry([member])
        return 1

    with patch.object(bootstrapper, "_attempt", side_effect=fake_attempt):
        result = await bootstrapper.run(["10.0.0.1:7701"])

    assert result == 1


@pytest.mark.gossip
async def test_50_bootstrap_from_seed_unreachable_raises_attempt_error() -> None:
    """_attempt() with unreachable seed (OSError on connect) → seeds_ok=0 → BootstrapAttemptError."""
    from unittest.mock import MagicMock

    topology_mgr = TopologyManager()
    config = GossipBootstrapConfig(max_retries=1)
    bootstrapper = GossipBootstrapper(topology_mgr, config, LOCAL_SHIFT)

    mock_client = MagicMock()
    mock_client.connect = AsyncMock(side_effect=OSError("connection refused"))
    mock_client.close = AsyncMock()

    with (
        patch("tourillon.core.gossip.bootstrapper.TcpClient", return_value=mock_client),
        pytest.raises(BootstrapAttemptError),
    ):
        await bootstrapper._attempt(["10.0.0.1:7701"])


@pytest.mark.gossip
async def test_51_bootstrap_from_seed_success_merges_delta() -> None:
    """_attempt() with a responsive seed merges members via serialized delta; seeds_ok=1."""
    from unittest.mock import MagicMock

    topology_mgr = TopologyManager()
    config = GossipBootstrapConfig(max_retries=1)
    serializer = MsgpackSerializerAdapter()
    bootstrapper = GossipBootstrapper(
        topology_mgr, config, LOCAL_SHIFT, serializer=serializer
    )

    member_dict = {
        "node_id": "node-seed",
        "peer_address": "10.0.0.1:7701",
        "generation": 1,
        "seq": 1,
        "phase": "ready",
        "tokens": [1024, 5120],
        "partition_shift": LOCAL_SHIFT,
    }
    delta_env = _make_gossip_delta_envelope(serializer, [member_dict])

    async def fake_stream(env: object, timeout: float = 30.0):  # type: ignore[misc]
        yield delta_env

    mock_client = MagicMock()
    mock_client.connect = AsyncMock()
    mock_client.close = AsyncMock()
    mock_client.stream = fake_stream

    with patch(
        "tourillon.core.gossip.bootstrapper.TcpClient", return_value=mock_client
    ):
        result = await bootstrapper._attempt(["10.0.0.1:7701"])

    assert result == 1
    topo = await topology_mgr.snapshot()
    assert "node-seed" in [m.node_id for m in topo.registry]


@pytest.mark.gossip
async def test_52_bootstrap_from_seed_partition_shift_mismatch_raises() -> None:
    """_attempt() with a seed returning wrong partition_shift raises BootstrapPartitionShiftError."""
    from unittest.mock import MagicMock

    topology_mgr = TopologyManager()
    config = GossipBootstrapConfig(max_retries=1)
    serializer = MsgpackSerializerAdapter()
    bootstrapper = GossipBootstrapper(
        topology_mgr, config, LOCAL_SHIFT, serializer=serializer
    )

    member_dict = {
        "node_id": "node-seed",
        "peer_address": "10.0.0.1:7701",
        "generation": 1,
        "seq": 1,
        "phase": "ready",
        "tokens": [1024, 5120],
        "partition_shift": LOCAL_SHIFT + 1,  # wrong shift
    }
    delta_env = _make_gossip_delta_envelope(serializer, [member_dict])

    async def fake_stream(env: object, timeout: float = 30.0):  # type: ignore[misc]
        yield delta_env

    mock_client = MagicMock()
    mock_client.connect = AsyncMock()
    mock_client.close = AsyncMock()
    mock_client.stream = fake_stream

    with (
        patch("tourillon.core.gossip.bootstrapper.TcpClient", return_value=mock_client),
        pytest.raises(BaseExceptionGroup),
    ):
        await bootstrapper._attempt(["10.0.0.1:7701"])


@pytest.mark.gossip
async def test_53_exchange_digest_gossip_error_response_breaks() -> None:
    """_exchange_digest() stops when seed returns gossip.error; members empty but seed reachable."""
    from unittest.mock import MagicMock

    topology_mgr = TopologyManager()
    config = GossipBootstrapConfig(max_retries=1)
    bootstrapper = GossipBootstrapper(topology_mgr, config, LOCAL_SHIFT)

    error_env = Envelope.create(b"\x80", kind="gossip.error", schema_id=0)

    async def fake_stream(env: object, timeout: float = 30.0):  # type: ignore[misc]
        yield error_env

    mock_client = MagicMock()
    mock_client.connect = AsyncMock()
    mock_client.close = AsyncMock()
    mock_client.stream = fake_stream

    with patch(
        "tourillon.core.gossip.bootstrapper.TcpClient", return_value=mock_client
    ):
        # gossip.error breaks digest loop → empty members → merge([]) → seeds_ok=1
        result = await bootstrapper._attempt(["10.0.0.1:7701"])

    assert result == 1


@pytest.mark.gossip
async def test_54_decode_delta_without_serializer_returns_empty_list() -> None:
    """_decode_delta() with no serializer always returns empty list."""
    topology_mgr = TopologyManager()
    config = GossipBootstrapConfig()
    bootstrapper = GossipBootstrapper(topology_mgr, config, LOCAL_SHIFT)

    fake_env = Envelope.create(b"\x80", kind="gossip.delta", schema_id=0)
    result = bootstrapper._decode_delta(fake_env)
    assert result == []


@pytest.mark.gossip
async def test_55_decode_delta_with_serializer_builds_member_objects() -> None:
    """_decode_delta() with serializer decodes delta into Member objects."""
    from tourillon.core.lifecycle.member import Member

    topology_mgr = TopologyManager()
    config = GossipBootstrapConfig()
    serializer = MsgpackSerializerAdapter()
    bootstrapper = GossipBootstrapper(
        topology_mgr, config, LOCAL_SHIFT, serializer=serializer
    )

    member_dict = {
        "node_id": "node-1",
        "peer_address": "10.0.0.1:7701",
        "generation": 1,
        "seq": 3,
        "phase": "ready",
        "tokens": [1024, 5120],
        "partition_shift": LOCAL_SHIFT,
    }
    env = _make_gossip_delta_envelope(serializer, [member_dict])
    members = bootstrapper._decode_delta(env)

    assert members is not None
    assert len(members) == 1
    assert isinstance(members[0], Member)
    assert members[0].node_id == "node-1"


@pytest.mark.gossip
async def test_56_decode_delta_bad_payload_returns_none_and_warns() -> None:
    """_decode_delta() with corrupt payload handles exception gracefully (returns None)."""
    topology_mgr = TopologyManager()
    config = GossipBootstrapConfig()
    serializer = MsgpackSerializerAdapter()
    bootstrapper = GossipBootstrapper(
        topology_mgr, config, LOCAL_SHIFT, serializer=serializer
    )

    # Not valid msgpack will cause decode to fail
    bad_env = Envelope.create(b"not msgpack!!!", kind="gossip.delta", schema_id=0)
    result = bootstrapper._decode_delta(bad_env)
    assert result == []  # exception caught, warning logged, returns empty list


@pytest.mark.gossip
async def test_57_exchange_digest_no_serializer_delta_breaks_immediately() -> None:
    """_exchange_digest with no serializer stops after first gossip.delta (line 222)."""
    from unittest.mock import MagicMock

    topology_mgr = TopologyManager()
    config = GossipBootstrapConfig(max_retries=1)
    bootstrapper = GossipBootstrapper(
        topology_mgr, config, LOCAL_SHIFT
    )  # no serializer

    delta_env = Envelope.create(b"\x80", kind="gossip.delta", schema_id=0)

    async def fake_stream(env: object, timeout: float = 30.0):  # type: ignore[misc]
        yield delta_env

    mock_client = MagicMock()
    mock_client.connect = AsyncMock()
    mock_client.close = AsyncMock()
    mock_client.stream = fake_stream

    with patch(
        "tourillon.core.gossip.bootstrapper.TcpClient", return_value=mock_client
    ):
        result = await bootstrapper._attempt(["10.0.0.1:7701"])

    assert result == 1  # seed reachable even though delta was empty


@pytest.mark.gossip
async def test_58_exchange_digest_unexpected_kind_breaks() -> None:
    """_exchange_digest stops when an unexpected envelope kind is received (line 225)."""
    from unittest.mock import MagicMock

    topology_mgr = TopologyManager()
    config = GossipBootstrapConfig(max_retries=1)
    bootstrapper = GossipBootstrapper(
        topology_mgr, config, LOCAL_SHIFT
    )  # no serializer

    unknown_env = Envelope.create(b"", kind="gossip.unknown", schema_id=0)

    async def fake_stream(env: object, timeout: float = 30.0):  # type: ignore[misc]
        yield unknown_env

    mock_client = MagicMock()
    mock_client.connect = AsyncMock()
    mock_client.close = AsyncMock()
    mock_client.stream = fake_stream

    with patch(
        "tourillon.core.gossip.bootstrapper.TcpClient", return_value=mock_client
    ):
        result = await bootstrapper._attempt(["10.0.0.1:7701"])

    assert result == 1  # seed reachable even with unknown kind
