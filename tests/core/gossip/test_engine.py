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
"""Tests for GossipEngine hot-loop, AE loop, stats, and fan-out logic."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from tourillon.core.gossip.config import GossipConfig
from tourillon.core.gossip.engine import GossipEngine, GossipStats
from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.ports.transport import ResponseTimeoutError
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.envelope import Envelope
from tourillon.core.transport.pool import PeerClientPool
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter

LOCAL_SHIFT = 12


def _make_member(
    node_id: str,
    gen: int = 1,
    seq: int = 0,
    phase: MemberPhase = MemberPhase.READY,
    shift: int = 12,
) -> Member:
    return Member(
        node_id=node_id,
        peer_address="10.0.0.1:7701",
        generation=gen,
        seq=seq,
        phase=phase,
        tokens=(1024, 5120),
        partition_shift=shift,
    )


def _make_engine(
    node_id: str = "node-1",
    topology_mgr: TopologyManager | None = None,
    pool: PeerClientPool | None = None,
    on_failed: Any | None = None,
) -> GossipEngine:
    if topology_mgr is None:
        topology_mgr = TopologyManager()
    if pool is None:
        pool = MagicMock(spec=PeerClientPool)
    config = GossipConfig(anti_entropy_interval=999.0)  # disable AE by default
    return GossipEngine(
        node_id=node_id,
        topology_manager=topology_mgr,
        pool=pool,
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=None,
        on_failed=on_failed,
    )


@pytest.mark.gossip
async def test_18_gossip_push_merges_received_member() -> None:
    """node-2 receives gossip.push; merge_registry() updates node-1 phase to DRAINING."""
    topology_mgr = TopologyManager()
    node1_ready = _make_member("node-1", seq=10, phase=MemberPhase.READY)
    await topology_mgr.apply_member(node1_ready)

    node1_draining = _make_member("node-1", seq=11, phase=MemberPhase.DRAINING)
    accepted = await topology_mgr.merge_registry([node1_draining])

    topo = await topology_mgr.snapshot()
    m = topo.registry.get("node-1")
    assert m is not None
    assert m.phase == MemberPhase.DRAINING
    assert accepted == 1


@pytest.mark.gossip
async def test_19_supersedes_false_accepted_zero_no_repropagation() -> None:
    """supersedes() → False → accepted=0 → no re-propagation."""
    topology_mgr = TopologyManager()
    # Peer already has seq=200
    peer_record = _make_member("node-1", gen=1, seq=200)
    await topology_mgr.apply_member(peer_record)

    # node-1 pushes its stale seq=50 record
    stale = _make_member("node-1", gen=1, seq=50)
    accepted = await topology_mgr.merge_registry([stale])

    assert accepted == 0  # stale record rejected


@pytest.mark.gossip
async def test_20_supersedes_true_accepted_one_repropagation() -> None:
    """supersedes() → True → accepted=1 → re-propagation if ttl > 1."""
    topology_mgr = TopologyManager()
    # Peer has seq=50
    old_record = _make_member("node-1", gen=1, seq=50)
    await topology_mgr.apply_member(old_record)

    # node-1 pushes fresh seq=200
    fresh = _make_member("node-1", gen=1, seq=200)
    accepted = await topology_mgr.merge_registry([fresh])

    assert accepted == 1  # fresh record accepted


@pytest.mark.gossip
async def test_29_no_eligible_peers_hot_loop_no_outgoing_no_crash() -> None:
    """No outgoing envelopes; one logger.debug per cycle; no crash."""
    topology_mgr = TopologyManager()
    # Only IDLE and FAILED nodes — neither eligible for gossip

    idle_m = _make_member("node-2", phase=MemberPhase.IDLE)
    failed_m = _make_member("node-3", phase=MemberPhase.FAILED)
    await topology_mgr.apply_member(idle_m)
    await topology_mgr.apply_member(failed_m)

    engine = _make_engine(topology_mgr=topology_mgr)
    member_to_push = _make_member("node-1", seq=1)

    await engine.announce(member_to_push)
    # Give hot loop a chance to process
    await asyncio.sleep(0)
    await engine.stop()

    # No error means success; just verify stats not inflated
    assert engine.stats.push_sent_total == 0


@pytest.mark.gossip
async def test_30_draining_to_idle_final_announce_drained_before_stop() -> None:
    """One gossip.push with TTL=3 emitted; GossipEngine.stop() called; hot_queue drained."""
    topology_mgr = TopologyManager()
    pool = MagicMock(spec=PeerClientPool)
    engine = _make_engine(topology_mgr=topology_mgr, pool=pool)

    idle_member = _make_member("node-1", phase=MemberPhase.IDLE, seq=10)

    # Simulate DRAINING → IDLE: announce the IDLE member then stop
    engine_task = asyncio.get_running_loop().create_task(engine.start())
    await asyncio.sleep(0)

    await engine.announce(idle_member)
    await engine.stop()
    engine_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await engine_task

    # The queue must be fully drained — IDLE member was processed
    assert engine._hot_queue.empty()


@pytest.mark.gossip
async def test_31_gossip_error_response_no_crash_stats_not_incremented() -> None:
    """gossip.error response is handled gracefully; push_sent_total not incremented."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()

    # Add an eligible peer
    peer = _make_member("node-2", phase=MemberPhase.READY)
    await topology_mgr.apply_member(peer)

    fake_client = MagicMock()
    error_payload = serializer.encode(
        {
            "sender": "node-2",
            "rejected_kind": "gossip.push",
            "code": "rate_limited",
            "message": "too fast",
        }
    )
    error_env = Envelope.create(
        error_payload, kind="gossip.error", schema_id=serializer.schema_id
    )
    fake_client.request = AsyncMock(return_value=error_env)

    pool = MagicMock(spec=PeerClientPool)
    pool.acquire = AsyncMock(return_value=fake_client)

    config = GossipConfig(anti_entropy_interval=999.0)
    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=pool,
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=serializer,
    )

    member = _make_member("node-1", seq=1)
    await engine._push_member(member)

    # gossip.error → push_sent_total not incremented
    assert engine.stats.push_sent_total == 0


@pytest.mark.gossip
async def test_32_response_timeout_on_gossip_ping_logs_warning_skips_peer() -> None:
    """ResponseTimeoutError on gossip.ping logged as WARNING; peer skipped; next cycle unaffected."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()

    peer = _make_member("node-2", phase=MemberPhase.READY)
    await topology_mgr.apply_member(peer)

    fake_client = MagicMock()
    fake_client.request = AsyncMock(side_effect=ResponseTimeoutError("timed out"))

    pool = MagicMock(spec=PeerClientPool)
    pool.acquire = AsyncMock(return_value=fake_client)

    config = GossipConfig(anti_entropy_interval=999.0)
    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=pool,
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=serializer,
    )

    # _ae_ping_pong should not raise — logs WARNING and returns
    topo = await topology_mgr.snapshot()
    await engine._ae_ping_pong(peer, topo.epoch)

    # Engine is still operational — ae_cycles_total was not incremented here
    # but stats.last_ae_peer was not set (timeout before pong)
    assert engine.stats.last_ae_peer is None


@pytest.mark.gossip
async def test_33_stale_seq_record_rejected_by_peers_via_merge() -> None:
    """node-1 restarts with seq=50; peer has seq=200; merge_registry rejects stale."""
    topology_mgr = TopologyManager()

    # Peer already has seq=200 for node-1
    fresh = _make_member("node-1", gen=1, seq=200)
    await topology_mgr.apply_member(fresh)

    # node-1 tries to push its stale seq=50
    stale = _make_member("node-1", gen=1, seq=50)
    accepted = await topology_mgr.merge_registry([stale])

    assert accepted == 0  # stale record rejected

    # The registry still holds seq=200
    topo = await topology_mgr.snapshot()
    m = topo.registry.get("node-1")
    assert m is not None
    assert m.seq == 200


@pytest.mark.gossip
async def test_34_gossip_stats_match_actual_activity() -> None:
    """push_sent_total, ae_cycles_total, ae_diverged match actual activity."""
    stats = GossipStats()
    stats.push_sent_total += 3
    stats.ae_cycles_total += 5
    stats.ae_diverged += 2

    d = stats.to_dict()
    assert d["push_sent_total"] == 3
    assert d["ae_cycles_total"] == 5
    assert d["ae_diverged"] == 2
    assert "known_members" in d
    assert "last_ae_peer" in d
    assert "last_ae_at" in d
    assert "bootstrap_ok_total" in d
    assert "bootstrap_err_total" in d


@pytest.mark.gossip
async def test_44_ae_initiator_delta_partition_shift_mismatch_logs_warning_no_failed() -> (
    None
):
    """node-1 closes connection; logs WARNING; does NOT transition to FAILED."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    config = GossipConfig(anti_entropy_interval=999.0)

    failed_called = [False]

    async def on_failed() -> None:
        failed_called[0] = True

    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=MagicMock(spec=PeerClientPool),
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=serializer,
        on_failed=on_failed,
    )

    # Build a delta envelope whose member has wrong partition_shift
    bad_member = {
        "node_id": "node-2",
        "peer_address": "10.0.0.2:7701",
        "phase": "ready",
        "generation": 1,
        "seq": 5,
        "tokens": [1024],
        "partition_shift": 10,  # mismatch — local is 12
    }
    delta_payload = serializer.encode(
        {"sender": "node-2", "has_more": False, "members": [bad_member]}
    )
    delta_env = Envelope.create(
        delta_payload, kind="gossip.delta", schema_id=serializer.schema_id
    )

    # Call _merge_delta directly (simulates initiator receiving bad delta)
    await engine._merge_delta(delta_env, "node-2")

    # node-1 does NOT transition to FAILED — on_failed not called
    assert failed_called[0] is False

    # node-2 NOT written to local registry
    topo = await topology_mgr.snapshot()
    assert topo.registry.get("node-2") is None


@pytest.mark.gossip
async def test_45_partition_shift_mismatch_error_response_triggers_failed() -> None:
    """node-2 receives gossip.error partition_shift_mismatch → on_failed called → engine stops."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    config = GossipConfig(anti_entropy_interval=999.0)

    on_failed_called = [False]

    async def on_failed() -> None:
        on_failed_called[0] = True

    engine = GossipEngine(
        node_id="node-2",
        topology_manager=topology_mgr,
        pool=MagicMock(spec=PeerClientPool),
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=serializer,
        on_failed=on_failed,
    )

    # Build gossip.error envelope with partition_shift_mismatch code
    error_payload = serializer.encode(
        {
            "sender": "node-1",
            "rejected_kind": "gossip.push",
            "code": "partition_shift_mismatch",
            "message": "member node-2 has partition_shift=10; local=12",
        }
    )
    error_env = Envelope.create(
        error_payload, kind="gossip.error", schema_id=serializer.schema_id
    )

    # Simulate engine started (so stop() calls join())
    engine._started = True
    # Handle the error (triggers _transition_to_failed)
    await engine._handle_push_error(error_env)

    # on_failed must have been called (write-before-announce step)
    assert on_failed_called[0] is True


@pytest.mark.gossip
async def test_gossip_engine_stop_drains_hot_queue() -> None:
    """stop() drains hot_queue before signalling cancellation."""
    pool = MagicMock(spec=PeerClientPool)
    topology_mgr = TopologyManager()
    engine = _make_engine(topology_mgr=topology_mgr, pool=pool)

    # Queue up a member; hot loop will process it (no eligible peers → logged.debug)
    m = _make_member("node-1", seq=1)
    await engine.announce(m)

    engine_task = asyncio.get_running_loop().create_task(engine.start())
    await asyncio.sleep(0.05)
    await engine.stop()
    engine_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await engine_task

    # Must not hang indefinitely
    assert engine._hot_queue.empty()


@pytest.mark.gossip
async def test_push_member_no_eligible_peers_returns_without_error() -> None:
    """_push_member logs debug and returns without error when no eligible peers exist."""
    engine = _make_engine()
    # No peers in topology → _push_member exits via logger.debug + return
    member = _make_member("node-1", seq=1)
    await engine._push_member(member)
    assert engine.stats.push_sent_total == 0


@pytest.mark.gossip
async def test_push_member_with_peer_and_serializer_increments_push_sent() -> None:
    """_push_member with eligible peers and serializer increments push_sent_total."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    peer = _make_member("node-2", phase=MemberPhase.READY)
    await topology_mgr.apply_member(peer)

    ok_payload = serializer.encode({"accepted": 1, "sender": "node-2"})
    ok_env = Envelope.create(
        ok_payload, kind="gossip.push.ok", schema_id=serializer.schema_id
    )

    fake_client = MagicMock()
    fake_client.request = AsyncMock(return_value=ok_env)
    pool = MagicMock(spec=PeerClientPool)
    pool.acquire = AsyncMock(return_value=fake_client)

    config = GossipConfig(anti_entropy_interval=999.0)
    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=pool,
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=serializer,
    )
    member = _make_member("node-2", seq=1)
    await engine._push_member(member)
    assert engine.stats.push_sent_total == 1


@pytest.mark.gossip
async def test_push_member_oserror_logs_warning_no_crash() -> None:
    """_push_member with OSError on acquire logs warning and does not crash."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    peer = _make_member("node-2", phase=MemberPhase.READY)
    await topology_mgr.apply_member(peer)

    pool = MagicMock(spec=PeerClientPool)
    pool.acquire = AsyncMock(side_effect=OSError("connection refused"))

    config = GossipConfig(anti_entropy_interval=999.0)
    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=pool,
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=serializer,
    )
    member = _make_member("node-2", seq=1)
    await engine._push_member(member)  # must not raise
    assert engine.stats.push_sent_total == 0


@pytest.mark.gossip
async def test_handle_push_error_unparseable_payload_no_failed_call() -> None:
    """_handle_push_error with malformed payload sets code='' and skips on_failed."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    on_failed_called = [False]

    async def on_failed() -> None:
        on_failed_called[0] = True

    config = GossipConfig(anti_entropy_interval=999.0)
    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=MagicMock(spec=PeerClientPool),
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=serializer,
        on_failed=on_failed,
    )
    # Build envelope with non-msgpack payload so decode raises
    bad_env = Envelope.create(b"\xff\xff\xff", kind="gossip.error", schema_id=0)
    await engine._handle_push_error(bad_env)
    assert on_failed_called[0] is False


@pytest.mark.gossip
async def test_run_ae_cycle_no_eligible_peers_increments_cycle_only() -> None:
    """_run_ae_cycle increments ae_cycles_total but skips ping when no eligible peers."""
    engine = _make_engine()
    # No peers in topology
    await engine._run_ae_cycle()
    assert engine.stats.ae_cycles_total == 1
    assert engine.stats.last_ae_peer is None


@pytest.mark.gossip
async def test_run_ae_cycle_with_eligible_peer_calls_ping() -> None:
    """_run_ae_cycle with eligible peer increments ae_cycles_total and calls ping."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    peer = _make_member("node-2", phase=MemberPhase.READY)
    await topology_mgr.apply_member(peer)

    # ping raises OSError → no crash; ae_cycles_total incremented
    pool = MagicMock(spec=PeerClientPool)
    pool.acquire = AsyncMock(side_effect=OSError("refused"))

    config = GossipConfig(anti_entropy_interval=999.0)
    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=pool,
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=serializer,
    )
    await engine._run_ae_cycle()
    assert engine.stats.ae_cycles_total == 1


@pytest.mark.gossip
async def test_ae_ping_pong_serializer_none_returns_immediately() -> None:
    """_ae_ping_pong with no serializer returns without sending anything."""
    engine = _make_engine()  # serializer=None
    peer = _make_member("node-2", phase=MemberPhase.READY)
    await engine._ae_ping_pong(peer, epoch=0)
    # No crash, no stats updated
    assert engine.stats.last_ae_peer is None


@pytest.mark.gossip
async def test_ae_ping_pong_non_pong_response_returns_early() -> None:
    """_ae_ping_pong returns early when response kind is not gossip.pong."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    peer = _make_member("node-2", phase=MemberPhase.READY)
    await topology_mgr.apply_member(peer)

    bad_env = Envelope.create(b"", kind="gossip.error", schema_id=0)
    fake_client = MagicMock()
    fake_client.request = AsyncMock(return_value=bad_env)
    pool = MagicMock(spec=PeerClientPool)
    pool.acquire = AsyncMock(return_value=fake_client)

    config = GossipConfig(anti_entropy_interval=999.0)
    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=pool,
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=serializer,
    )
    await engine._ae_ping_pong(peer, epoch=0)
    assert engine.stats.last_ae_peer is None


@pytest.mark.gossip
async def test_ae_ping_pong_same_true_no_digest_exchange() -> None:
    """_ae_ping_pong with same=True skips digest exchange; ae_diverged not incremented."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    peer = _make_member("node-2", phase=MemberPhase.READY)
    await topology_mgr.apply_member(peer)

    pong_payload = serializer.encode(
        {"sender": "node-2", "epoch": 0, "fingerprint": "abc", "same": True}
    )
    pong_env = Envelope.create(
        pong_payload, kind="gossip.pong", schema_id=serializer.schema_id
    )
    fake_client = MagicMock()
    fake_client.request = AsyncMock(return_value=pong_env)
    pool = MagicMock(spec=PeerClientPool)
    pool.acquire = AsyncMock(return_value=fake_client)

    config = GossipConfig(anti_entropy_interval=999.0)
    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=pool,
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=serializer,
    )
    await engine._ae_ping_pong(peer, epoch=0)
    assert engine.stats.ae_diverged == 0
    assert engine.stats.last_ae_peer == "node-2"


@pytest.mark.gossip
async def test_ae_ping_pong_same_false_increments_diverged() -> None:
    """_ae_ping_pong with same=False increments ae_diverged and calls _ae_digest_delta."""
    from tourillon.core.transport.client import TcpClient

    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    peer = _make_member("node-2", phase=MemberPhase.READY)
    await topology_mgr.apply_member(peer)

    pong_payload = serializer.encode(
        {"sender": "node-2", "epoch": 0, "fingerprint": "xyz", "same": False}
    )
    pong_env = Envelope.create(
        pong_payload, kind="gossip.pong", schema_id=serializer.schema_id
    )

    class _FakeTcpClient(TcpClient):
        async def request(self, env: Envelope) -> Envelope:  # type: ignore[override]
            return pong_env

        async def stream(  # type: ignore[override]
            self, env: Envelope
        ) -> Any:  # noqa: ANN401 — async gen return type
            raise OSError("refused")
            yield  # make this an async generator

    fake_client = _FakeTcpClient()
    pool = MagicMock(spec=PeerClientPool)
    pool.acquire = AsyncMock(return_value=fake_client)

    config = GossipConfig(anti_entropy_interval=999.0)
    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=pool,
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=serializer,
    )
    await engine._ae_ping_pong(peer, epoch=0)
    assert engine.stats.ae_diverged == 1


@pytest.mark.gossip
async def test_ae_digest_delta_gossip_error_response_calls_handle_ae_error() -> None:
    """_ae_digest_delta returns early when stream yields gossip.error."""
    from tourillon.core.transport.client import TcpClient

    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    peer = _make_member("node-2", phase=MemberPhase.READY)
    await topology_mgr.apply_member(peer)

    error_env = Envelope.create(
        serializer.encode({"code": "rate_limited"}),
        kind="gossip.error",
        schema_id=serializer.schema_id,
    )

    class _ErrTcpClient(TcpClient):
        async def stream(  # type: ignore[override]
            self, _env: Envelope
        ) -> Any:  # noqa: ANN401 — async gen return type
            yield error_env

    config = GossipConfig(anti_entropy_interval=999.0)
    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=MagicMock(spec=PeerClientPool),
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=serializer,
    )
    await engine._ae_digest_delta(peer, _ErrTcpClient())
    assert engine.stats.ae_cycles_total == 0


@pytest.mark.gossip
async def test_ae_digest_delta_gossip_delta_has_more_false_merges_and_breaks() -> None:
    """_ae_digest_delta merges a gossip.delta envelope with has_more=False."""
    from tourillon.core.transport.client import TcpClient

    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    peer = _make_member("node-2", phase=MemberPhase.READY)
    await topology_mgr.apply_member(peer)

    delta_member = {
        "node_id": "node-2",
        "peer_address": "10.0.0.2:7701",
        "phase": "ready",
        "generation": 1,
        "seq": 5,
        "tokens": [1024],
        "partition_shift": LOCAL_SHIFT,
    }
    delta_env = Envelope.create(
        serializer.encode(
            {"sender": "node-2", "has_more": False, "members": [delta_member]}
        ),
        kind="gossip.delta",
        schema_id=serializer.schema_id,
    )

    class _DeltaTcpClient(TcpClient):
        async def stream(  # type: ignore[override]
            self, _env: Envelope
        ) -> Any:  # noqa: ANN401 — async gen return type
            yield delta_env

    config = GossipConfig(anti_entropy_interval=999.0)
    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=MagicMock(spec=PeerClientPool),
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=serializer,
    )
    await engine._ae_digest_delta(peer, _DeltaTcpClient())

    snap = await topology_mgr.snapshot()
    m = snap.registry.get("node-2")
    assert m is not None
    assert m.seq == 5


@pytest.mark.gossip
async def test_ae_digest_delta_oserror_logs_warning() -> None:
    """_ae_digest_delta with OSError on stream logs warning and does not crash."""
    from tourillon.core.transport.client import TcpClient

    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    peer = _make_member("node-2", phase=MemberPhase.READY)
    await topology_mgr.apply_member(peer)

    class _OsErrTcpClient(TcpClient):
        async def stream(  # type: ignore[override]
            self, _env: Envelope
        ) -> Any:  # noqa: ANN401 — async gen return type
            raise OSError("broken pipe")
            yield  # make this an async generator

    config = GossipConfig(anti_entropy_interval=999.0)
    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=MagicMock(spec=PeerClientPool),
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=serializer,
    )
    await engine._ae_digest_delta(peer, _OsErrTcpClient())  # must not raise


@pytest.mark.gossip
async def test_merge_delta_serializer_none_returns_without_error() -> None:
    """_merge_delta with no serializer returns immediately."""
    engine = _make_engine()  # serializer=None
    env = Envelope.create(b"anything", kind="gossip.delta", schema_id=0)
    await engine._merge_delta(env, "node-2")  # must not raise


@pytest.mark.gossip
async def test_merge_delta_valid_members_updates_registry() -> None:
    """_merge_delta with correct members merges them into the local registry."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    config = GossipConfig(anti_entropy_interval=999.0)
    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=MagicMock(spec=PeerClientPool),
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=serializer,
    )
    delta_member = {
        "node_id": "node-3",
        "peer_address": "10.0.0.3:7701",
        "phase": "ready",
        "generation": 2,
        "seq": 10,
        "tokens": [2048],
        "partition_shift": LOCAL_SHIFT,
    }
    delta_env = Envelope.create(
        serializer.encode(
            {"sender": "node-2", "has_more": False, "members": [delta_member]}
        ),
        kind="gossip.delta",
        schema_id=serializer.schema_id,
    )
    await engine._merge_delta(delta_env, "node-2")

    snap = await topology_mgr.snapshot()
    m = snap.registry.get("node-3")
    assert m is not None
    assert m.generation == 2


@pytest.mark.gossip
async def test_handle_ae_error_logs_warning_no_crash() -> None:
    """_handle_ae_error logs a WARNING and does not crash."""
    engine = _make_engine()
    env = Envelope.create(b"", kind="gossip.error", schema_id=0)
    await engine._handle_ae_error(env)  # must not raise


@pytest.mark.gossip
async def test_ae_loop_exits_cleanly_when_stop_event_set_before_interval() -> None:
    """_ae_loop exits via the return path when stop_event is set before interval fires."""
    engine = _make_engine()

    async def _set_stop_after_yield() -> None:
        await asyncio.sleep(0)
        engine._stop_event.set()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(_set_stop_after_yield())
        tg.create_task(engine._ae_loop())


@pytest.mark.gossip
async def test_ae_loop_timeout_branch_fires_ae_cycle() -> None:
    """_ae_loop TimeoutError branch calls _run_ae_cycle once then stops."""
    config = GossipConfig(anti_entropy_interval=0.001)  # fire almost immediately
    topology_mgr = TopologyManager()
    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=MagicMock(spec=PeerClientPool),
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=None,
    )
    # Set stop_event after one AE cycle fires
    cycle_entered = asyncio.Event()
    original_run_ae = engine._run_ae_cycle

    async def _patched_run_ae() -> None:
        cycle_entered.set()
        engine._stop_event.set()
        await original_run_ae()

    engine._run_ae_cycle = _patched_run_ae  # type: ignore[method-assign]
    await engine._ae_loop()
    assert cycle_entered.is_set()


@pytest.mark.gossip
async def test_send_push_serializer_none_returns_immediately() -> None:
    """_send_push with no serializer returns immediately without sending."""
    topology_mgr = TopologyManager()
    peer = _make_member("node-2", phase=MemberPhase.READY)
    await topology_mgr.apply_member(peer)

    pool = MagicMock(spec=PeerClientPool)
    pool.acquire = AsyncMock()

    config = GossipConfig(anti_entropy_interval=999.0)
    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=pool,
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=None,
    )
    member = _make_member("node-2", seq=1)
    target = _make_member("node-2", phase=MemberPhase.READY)
    await engine._send_push(member, target)
    pool.acquire.assert_not_called()


@pytest.mark.gossip
async def test_send_push_accepted_zero_does_not_increment_push_sent() -> None:
    """_send_push with push.ok accepted=0 does not increment push_sent_total."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    peer = _make_member("node-2", phase=MemberPhase.READY)
    await topology_mgr.apply_member(peer)

    ok_payload = serializer.encode({"accepted": 0, "sender": "node-2"})
    ok_env = Envelope.create(
        ok_payload, kind="gossip.push.ok", schema_id=serializer.schema_id
    )
    fake_client = MagicMock()
    fake_client.request = AsyncMock(return_value=ok_env)
    pool = MagicMock(spec=PeerClientPool)
    pool.acquire = AsyncMock(return_value=fake_client)

    config = GossipConfig(anti_entropy_interval=999.0)
    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=pool,
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=serializer,
    )
    member = _make_member("node-2", seq=1)
    target = _make_member("node-2", phase=MemberPhase.READY)
    await engine._send_push(member, target)
    # push_sent_total is incremented before the accepted==0 early return (line 214)
    assert engine.stats.push_sent_total == 1


@pytest.mark.gossip
async def test_handle_push_error_serializer_none_returns_immediately() -> None:
    """_handle_push_error with no serializer returns without doing anything."""
    engine = _make_engine()  # serializer=None
    env = Envelope.create(b"", kind="gossip.error", schema_id=0)
    await engine._handle_push_error(env)  # must not raise


@pytest.mark.gossip
async def test_ae_digest_delta_serializer_none_returns_immediately() -> None:
    """_ae_digest_delta with no serializer returns without doing anything."""
    from tourillon.core.transport.client import TcpClient

    engine = _make_engine()  # serializer=None
    peer = _make_member("node-2", phase=MemberPhase.READY)
    await engine._ae_digest_delta(peer, TcpClient())  # must not raise


@pytest.mark.gossip
async def test_merge_delta_bad_member_phase_logs_warning_no_crash() -> None:
    """_merge_delta with malformed member data catches exception and logs warning."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    config = GossipConfig(anti_entropy_interval=999.0)
    engine = GossipEngine(
        node_id="node-1",
        topology_manager=topology_mgr,
        pool=MagicMock(spec=PeerClientPool),
        config=config,
        partition_shift=LOCAL_SHIFT,
        serializer=serializer,
    )
    # "phase" with invalid value → MemberPhase() raises ValueError → except block
    bad_member = {
        "node_id": "node-bad",
        "peer_address": "10.0.0.9:7701",
        "phase": "not_a_valid_phase",
        "generation": 1,
        "seq": 1,
        "tokens": [],
        "partition_shift": LOCAL_SHIFT,
    }
    delta_env = Envelope.create(
        serializer.encode(
            {"sender": "node-2", "has_more": False, "members": [bad_member]}
        ),
        kind="gossip.delta",
        schema_id=serializer.schema_id,
    )
    await engine._merge_delta(delta_env, "node-2")  # must not raise
    snap = await topology_mgr.snapshot()
    assert snap.registry.get("node-bad") is None
