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
"""Wiring smoke-tests for _build_peer_dispatcher.

These tests verify that every handler constructor is called with valid
arguments — they would have caught the tls_ctx / attempt_timeout regression
immediately (TypeError from a mismatched __init__ keyword argument).

No real TLS credentials or sockets are required; the ssl_ctx parameters are
accepted as plain objects by the handlers at construction time.
"""

from __future__ import annotations

import asyncio

import pytest

from tourillon.core.gossip.config import GossipConfig
from tourillon.core.gossip.engine import GossipEngine
from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.lifecycle.probe import ProbeManager
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ports.state import StatePort
from tourillon.core.rebalance.applicator import RebalanceApplicator
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.config import (
    JoinConfig,
    NodeSize,
    ServerConfig,
    TlsConfig,
    TourillonConfig,
)
from tourillon.core.testing.mem_storage import InMemoryStorage
from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.core.transport.pool import PeerClientPool
from tourillon.infra.cli.node import (
    _await_rebalance_or_stop,
    _build_peer_dispatcher,
    _startup_phase_logic,
    _transition_draining_to_idle,
    _transition_joining_to_ready,
)
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter


class _FakeStatePort(StatePort):
    """In-memory StatePort for wiring tests."""

    async def load(self) -> NodeState | None:
        return None

    async def save(self, state: NodeState) -> None:
        pass


def _make_cfg(seeds: list[str] | None = None) -> TourillonConfig:
    """Return a minimal TourillonConfig suitable for wiring tests."""
    return TourillonConfig(
        node_id="node-1",
        node_size=NodeSize.M,
        data_dir=".data/node-1",
        tls=TlsConfig(cert_data="", key_data="", ca_data=""),
        kv_server=ServerConfig(bind="127.0.0.1:7700"),
        peer_server=ServerConfig(bind="127.0.0.1:7701"),
        seeds=seeds or [],
        partition_shift=12,
        join=JoinConfig(),
    )


def _make_deps(
    cfg: TourillonConfig,
) -> tuple[
    list[NodeState],
    _FakeStatePort,
    TopologyManager,
    ProbeManager,
    Partitioner,
    MsgpackSerializerAdapter,
    GossipConfig,
    GossipEngine,
]:
    """Build all domain objects needed by _build_peer_dispatcher."""
    state_ref: list[NodeState] = [
        NodeState(
            node_id=cfg.node_id,
            phase=MemberPhase.IDLE,
            generation=0,
            seq=0,
            tokens=(),
            epoch=0,
        )
    ]
    state_port = _FakeStatePort()
    topology_mgr = TopologyManager()
    probe_mgr = ProbeManager()
    hash_space = HashSpace()
    partitioner = Partitioner(hash_space, cfg.partition_shift)
    serializer = MsgpackSerializerAdapter()
    gossip_config = GossipConfig()
    # PeerClientPool accepts any ssl_ctx object at construction time.
    pool = PeerClientPool(ssl_ctx=object())  # type: ignore[arg-type]
    engine = GossipEngine(
        node_id=cfg.node_id,
        topology_manager=topology_mgr,
        pool=pool,
        config=gossip_config,
        partition_shift=cfg.partition_shift,
        serializer=serializer,
    )
    return (
        state_ref,
        state_port,
        topology_mgr,
        probe_mgr,
        partitioner,
        serializer,
        gossip_config,
        engine,
    )


@pytest.mark.bootstrap
def test_build_peer_dispatcher_returns_dispatcher_instance() -> None:
    """_build_peer_dispatcher returns a Dispatcher without raising TypeError.

    This test would have caught the tls_ctx regression: passing a removed
    keyword argument to NodeJoinHandler.__init__ raises TypeError immediately.
    """
    cfg = _make_cfg()
    (
        state_ref,
        state_port,
        topology_mgr,
        probe_mgr,
        partitioner,
        serializer,
        gossip_config,
        engine,
    ) = _make_deps(cfg)

    dispatcher = _build_peer_dispatcher(
        cfg=cfg,
        state_ref=state_ref,
        state_port=state_port,  # type: ignore[arg-type]
        topology_mgr=topology_mgr,
        probe_mgr=probe_mgr,
        partitioner=partitioner,
        client_ssl_ctx=object(),
        serializer=serializer,
        engine=engine,
        gossip_config=gossip_config,
        launch_bootstrap=None,
        peer_address="127.0.0.1:7701",
        kv_address="127.0.0.1:7700",
    )

    assert isinstance(dispatcher, Dispatcher)


@pytest.mark.bootstrap
def test_build_peer_dispatcher_registers_node_inspect() -> None:
    """node.inspect handler is registered in the returned Dispatcher."""
    cfg = _make_cfg()
    (
        state_ref,
        state_port,
        topology_mgr,
        probe_mgr,
        partitioner,
        serializer,
        gossip_config,
        engine,
    ) = _make_deps(cfg)

    dispatcher = _build_peer_dispatcher(
        cfg=cfg,
        state_ref=state_ref,
        state_port=state_port,  # type: ignore[arg-type]
        topology_mgr=topology_mgr,
        probe_mgr=probe_mgr,
        partitioner=partitioner,
        client_ssl_ctx=object(),
        serializer=serializer,
        engine=engine,
        gossip_config=gossip_config,
        launch_bootstrap=None,
        peer_address="127.0.0.1:7701",
        kv_address="127.0.0.1:7700",
    )

    assert dispatcher.lookup("node.inspect") is not None


@pytest.mark.bootstrap
def test_build_peer_dispatcher_registers_node_join() -> None:
    """node.join handler is registered in the returned Dispatcher."""
    cfg = _make_cfg()
    (
        state_ref,
        state_port,
        topology_mgr,
        probe_mgr,
        partitioner,
        serializer,
        gossip_config,
        engine,
    ) = _make_deps(cfg)

    dispatcher = _build_peer_dispatcher(
        cfg=cfg,
        state_ref=state_ref,
        state_port=state_port,  # type: ignore[arg-type]
        topology_mgr=topology_mgr,
        probe_mgr=probe_mgr,
        partitioner=partitioner,
        client_ssl_ctx=object(),
        serializer=serializer,
        engine=engine,
        gossip_config=gossip_config,
        launch_bootstrap=None,
        peer_address="127.0.0.1:7701",
        kv_address="127.0.0.1:7700",
    )

    assert dispatcher.lookup("node.join") is not None


@pytest.mark.bootstrap
def test_build_peer_dispatcher_registers_gossip_handlers() -> None:
    """Gossip protocol handlers (gossip.digest, gossip.push, etc.) are registered."""
    cfg = _make_cfg()
    (
        state_ref,
        state_port,
        topology_mgr,
        probe_mgr,
        partitioner,
        serializer,
        gossip_config,
        engine,
    ) = _make_deps(cfg)

    dispatcher = _build_peer_dispatcher(
        cfg=cfg,
        state_ref=state_ref,
        state_port=state_port,  # type: ignore[arg-type]
        topology_mgr=topology_mgr,
        probe_mgr=probe_mgr,
        partitioner=partitioner,
        client_ssl_ctx=object(),
        serializer=serializer,
        engine=engine,
        gossip_config=gossip_config,
        launch_bootstrap=None,
        peer_address="127.0.0.1:7701",
        kv_address="127.0.0.1:7700",
    )

    # Gossip register_gossip_handlers registers at least these two kinds.
    assert dispatcher.lookup("gossip.digest") is not None
    assert dispatcher.lookup("gossip.push") is not None


@pytest.mark.bootstrap
@pytest.mark.rebalance
def test_build_peer_dispatcher_registers_rebalance_handlers_for_idle_node() -> None:
    """Rebalance handlers are registered even when the node starts in IDLE.

    Regression test for the operator-session bug where a freshly-started IDLE
    node accepted `tourctl node join` (phase → JOINING) but the peer
    dispatcher — built once at startup with `applicator=None` — was missing
    the `rebalance.plan` / `rebalance.transfer` / `rebalance.status` handlers.
    Subsequent `tourctl rebalance status` calls hit `Unknown envelope kind
    'rebalance.status'`, closed the connection, and surfaced
    `ConnectionClosedError` to the operator as a raw traceback.
    """
    cfg = _make_cfg()
    (
        state_ref,
        state_port,
        topology_mgr,
        probe_mgr,
        partitioner,
        serializer,
        gossip_config,
        engine,
    ) = _make_deps(cfg)

    # state_ref[0].phase == IDLE — production passes applicator + storage
    # regardless of phase.
    assert state_ref[0].phase == MemberPhase.IDLE

    storage = InMemoryStorage()
    applicator = RebalanceApplicator(
        node_id=cfg.node_id,
        pool=PeerClientPool(ssl_ctx=object()),  # type: ignore[arg-type]
        state_port=state_port,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        serializer=serializer,
        peer_addresses={},
        max_concurrent_transfers=1,
        max_chunk_bytes=1024,
        total_partitions=partitioner.total_partitions,
    )

    dispatcher = _build_peer_dispatcher(
        cfg=cfg,
        state_ref=state_ref,
        state_port=state_port,  # type: ignore[arg-type]
        topology_mgr=topology_mgr,
        probe_mgr=probe_mgr,
        partitioner=partitioner,
        client_ssl_ctx=object(),
        serializer=serializer,
        engine=engine,
        gossip_config=gossip_config,
        launch_bootstrap=None,
        peer_address="127.0.0.1:7701",
        kv_address="127.0.0.1:7700",
        applicator=applicator,
        storage=storage,
    )

    assert dispatcher.lookup("rebalance.plan") is not None
    assert dispatcher.lookup("rebalance.transfer") is not None
    assert dispatcher.lookup("rebalance.status") is not None


@pytest.mark.bootstrap
def test_build_peer_dispatcher_join_handler_token_count_from_config() -> None:
    """join_handler._token_count is set to cfg.node_size.token_count."""
    cfg = _make_cfg()
    (
        state_ref,
        state_port,
        topology_mgr,
        probe_mgr,
        partitioner,
        serializer,
        gossip_config,
        engine,
    ) = _make_deps(cfg)

    dispatcher = _build_peer_dispatcher(
        cfg=cfg,
        state_ref=state_ref,
        state_port=state_port,  # type: ignore[arg-type]
        topology_mgr=topology_mgr,
        probe_mgr=probe_mgr,
        partitioner=partitioner,
        client_ssl_ctx=object(),
        serializer=serializer,
        engine=engine,
        gossip_config=gossip_config,
        launch_bootstrap=None,
        peer_address="127.0.0.1:7701",
        kv_address="127.0.0.1:7700",
    )

    handler = dispatcher.lookup("node.join")
    # NodeSize.M -> 4 tokens.
    assert getattr(handler, "_token_count", None) == cfg.node_size.token_count


# ---------------------------------------------------------------------------
# Helpers shared by the new transition / phase tests below
# ---------------------------------------------------------------------------


class _StoringStatePort(StatePort):
    """StatePort that stores the last saved NodeState."""

    def __init__(self) -> None:
        self._saved: NodeState | None = None

    async def load(self) -> NodeState | None:
        return self._saved

    async def save(self, state: NodeState) -> None:
        self._saved = state


class _MockPool:
    """Minimal PeerClientPool that never actually connects."""

    async def acquire(self, node_id: str, address: str) -> object:
        raise ConnectionError("no connection in mock")

    async def close_all(self) -> None:
        pass


class _FakeKvServer:
    """Ersatz TcpServer whose start() / stop() are no-ops."""

    async def start(self, host: str, port: int) -> None:
        pass

    async def stop(self) -> None:
        pass


def _make_joining_state(node_id: str = "node-1") -> NodeState:
    return NodeState(
        node_id=node_id,
        phase=MemberPhase.JOINING,
        generation=1,
        seq=2,
        tokens=(100, 200, 300, 400),
        epoch=3,
        committed_pids=(10, 20),
        staging_pids=(),
    )


def _make_draining_state(node_id: str = "node-1") -> NodeState:
    return NodeState(
        node_id=node_id,
        phase=MemberPhase.DRAINING,
        generation=1,
        seq=5,
        tokens=(100, 200, 300, 400),
        epoch=3,
        committed_pids=(10, 20),
        staging_pids=(),
    )


# ---------------------------------------------------------------------------
# _build_peer_dispatcher — with applicator + storage (lines 347–358)
# ---------------------------------------------------------------------------


@pytest.mark.bootstrap
def test_build_peer_dispatcher_with_rebalance_handlers_registers_plan_kind() -> None:
    """rebalance.plan handler is registered when applicator and storage are supplied."""
    cfg = _make_cfg()
    (
        state_ref,
        state_port,
        topology_mgr,
        probe_mgr,
        partitioner,
        serializer,
        gossip_config,
        engine,
    ) = _make_deps(cfg)

    pool = _MockPool()
    applicator = RebalanceApplicator(
        node_id=cfg.node_id,
        pool=pool,  # type: ignore[arg-type]
        state_port=state_port,
        storage=InMemoryStorage(),  # type: ignore[arg-type]
        serializer=serializer,
        peer_addresses={},
    )
    storage = InMemoryStorage()

    dispatcher = _build_peer_dispatcher(
        cfg=cfg,
        state_ref=state_ref,
        state_port=state_port,  # type: ignore[arg-type]
        topology_mgr=topology_mgr,
        probe_mgr=probe_mgr,
        partitioner=partitioner,
        client_ssl_ctx=object(),
        serializer=serializer,
        engine=engine,
        gossip_config=gossip_config,
        launch_bootstrap=None,
        peer_address="127.0.0.1:7701",
        kv_address="127.0.0.1:7700",
        applicator=applicator,
        storage=storage,  # type: ignore[arg-type]
    )

    assert dispatcher.lookup("rebalance.plan") is not None
    assert dispatcher.lookup("rebalance.status") is not None


# ---------------------------------------------------------------------------
# _transition_joining_to_ready (lines 636–679)
# ---------------------------------------------------------------------------


@pytest.mark.bootstrap
async def test_transition_joining_to_ready_persists_ready_phase() -> None:
    """JOINING → READY persists phase=READY to state_port before gossip."""
    cfg = _make_cfg()
    state = _make_joining_state(cfg.node_id)
    state_ref: list[NodeState] = [state]
    state_port = _StoringStatePort()

    topology_mgr = TopologyManager()
    pool = PeerClientPool(ssl_ctx=object())  # type: ignore[arg-type]
    serializer = MsgpackSerializerAdapter()
    gossip_config = GossipConfig()
    engine = GossipEngine(
        node_id=cfg.node_id,
        topology_manager=topology_mgr,
        pool=pool,
        config=gossip_config,
        partition_shift=cfg.partition_shift,
        serializer=serializer,
    )
    kv_server = _FakeKvServer()

    await _transition_joining_to_ready(
        cfg,
        state_ref,
        state_port,  # type: ignore[arg-type]
        topology_mgr,
        engine,
        kv_server,  # type: ignore[arg-type]
        "127.0.0.1",
        17700,
        "127.0.0.1:7701",
    )

    assert state_ref[0].phase == MemberPhase.READY
    assert state_port._saved is not None
    assert state_port._saved.phase == MemberPhase.READY
    assert state_port._saved.staging_pids == ()


@pytest.mark.bootstrap
async def test_transition_joining_to_ready_aborts_when_save_raises() -> None:
    """JOINING → READY aborts cleanly if state_port.save() raises."""

    class _FailingStatePort(StatePort):
        async def load(self) -> NodeState | None:
            return None

        async def save(self, state: NodeState) -> None:
            raise OSError("disk full")

    cfg = _make_cfg()
    state = _make_joining_state(cfg.node_id)
    state_ref: list[NodeState] = [state]
    topology_mgr = TopologyManager()
    pool = PeerClientPool(ssl_ctx=object())  # type: ignore[arg-type]
    serializer = MsgpackSerializerAdapter()
    gossip_config = GossipConfig()
    engine = GossipEngine(
        node_id=cfg.node_id,
        topology_manager=topology_mgr,
        pool=pool,
        config=gossip_config,
        partition_shift=cfg.partition_shift,
        serializer=serializer,
    )
    kv_server = _FakeKvServer()

    # Should not raise — the function catches the error and returns early.
    await _transition_joining_to_ready(
        cfg,
        state_ref,
        _FailingStatePort(),  # type: ignore[arg-type]
        topology_mgr,
        engine,
        kv_server,  # type: ignore[arg-type]
        "127.0.0.1",
        17701,
        "127.0.0.1:7701",
    )

    # state_ref is unchanged because the save failed before the in-place update.
    assert state_ref[0].phase == MemberPhase.JOINING


# ---------------------------------------------------------------------------
# _transition_draining_to_idle (lines 698–737)
# ---------------------------------------------------------------------------


@pytest.mark.bootstrap
async def test_transition_draining_to_idle_persists_idle_and_sets_stop() -> None:
    """DRAINING → IDLE persists phase=IDLE and sets the stop event."""
    cfg = _make_cfg()
    state = _make_draining_state(cfg.node_id)
    state_ref: list[NodeState] = [state]
    state_port = _StoringStatePort()

    topology_mgr = TopologyManager()
    pool = PeerClientPool(ssl_ctx=object())  # type: ignore[arg-type]
    serializer = MsgpackSerializerAdapter()
    gossip_config = GossipConfig()
    engine = GossipEngine(
        node_id=cfg.node_id,
        topology_manager=topology_mgr,
        pool=pool,
        config=gossip_config,
        partition_shift=cfg.partition_shift,
        serializer=serializer,
    )
    stop = asyncio.Event()

    await _transition_draining_to_idle(
        cfg,
        state_ref,
        state_port,  # type: ignore[arg-type]
        topology_mgr,
        engine,
        "127.0.0.1:7701",
        stop,
    )

    assert state_ref[0].phase == MemberPhase.IDLE
    assert state_port._saved is not None
    assert state_port._saved.phase == MemberPhase.IDLE
    assert stop.is_set()


@pytest.mark.bootstrap
async def test_transition_draining_to_idle_aborts_when_save_raises() -> None:
    """DRAINING → IDLE aborts cleanly if state_port.save() raises."""

    class _FailingStatePort(StatePort):
        async def load(self) -> NodeState | None:
            return None

        async def save(self, state: NodeState) -> None:
            raise OSError("disk full")

    cfg = _make_cfg()
    state = _make_draining_state(cfg.node_id)
    state_ref: list[NodeState] = [state]

    topology_mgr = TopologyManager()
    pool = PeerClientPool(ssl_ctx=object())  # type: ignore[arg-type]
    serializer = MsgpackSerializerAdapter()
    gossip_config = GossipConfig()
    engine = GossipEngine(
        node_id=cfg.node_id,
        topology_manager=topology_mgr,
        pool=pool,
        config=gossip_config,
        partition_shift=cfg.partition_shift,
        serializer=serializer,
    )
    stop = asyncio.Event()

    # Should not raise — the function catches the error and returns early.
    await _transition_draining_to_idle(
        cfg,
        state_ref,
        _FailingStatePort(),  # type: ignore[arg-type]
        topology_mgr,
        engine,
        "127.0.0.1:7701",
        stop,
    )

    # stop must NOT be set because the transition aborted before setting it.
    assert not stop.is_set()
    # state_ref unchanged because the save failed before the in-place update.
    assert state_ref[0].phase == MemberPhase.DRAINING


# ---------------------------------------------------------------------------
# _await_rebalance_or_stop (lines 592–616)
# ---------------------------------------------------------------------------


@pytest.mark.bootstrap
async def test_await_rebalance_or_stop_returns_none_when_stop_fires_first() -> None:
    """_await_rebalance_or_stop returns None when stop fires before completion."""

    class _SlowApplicator:
        async def wait_for_completion(self) -> tuple[list[int], list[int]]:
            await asyncio.sleep(9999)
            return ([], [])

    stop = asyncio.Event()
    stop.set()  # fire immediately

    result = await _await_rebalance_or_stop(_SlowApplicator(), stop)  # type: ignore[arg-type]

    assert result is None


@pytest.mark.bootstrap
async def test_await_rebalance_or_stop_returns_failed_list_on_completion() -> None:
    """_await_rebalance_or_stop returns failed_pids when completion fires first."""

    class _InstantApplicator:
        async def wait_for_completion(self) -> tuple[list[int], list[int]]:
            return ([42], [99])

    stop = asyncio.Event()

    result = await _await_rebalance_or_stop(_InstantApplicator(), stop)  # type: ignore[arg-type]

    # result is the failed_pids list (second element of the tuple).
    assert result == [99]


# ---------------------------------------------------------------------------
# _startup_phase_logic — PAUSED and FAILED branches (lines 1017–1028)
# ---------------------------------------------------------------------------


@pytest.mark.bootstrap
async def test_startup_phase_logic_paused_returns_false() -> None:
    """_startup_phase_logic returns False for PAUSED phase without starting anything."""
    cfg = _make_cfg()
    (
        state_ref,
        state_port,
        topology_mgr,
        _probe_mgr,
        _partitioner,
        serializer,
        gossip_config,
        engine,
    ) = _make_deps(cfg)

    state = NodeState(
        node_id=cfg.node_id,
        phase=MemberPhase.PAUSED,
        generation=1,
        seq=0,
        tokens=(100,),
        epoch=1,
    )

    result = await _startup_phase_logic(
        cfg=cfg,
        phase=MemberPhase.PAUSED,
        state=state,
        state_port=state_port,  # type: ignore[arg-type]
        topology_mgr=topology_mgr,
        client_ssl_ctx=object(),
        serializer=serializer,
        state_ref=state_ref,
        gossip_config=gossip_config,
        engine=engine,
    )

    assert result is False


@pytest.mark.bootstrap
async def test_startup_phase_logic_failed_returns_false() -> None:
    """_startup_phase_logic returns False for FAILED phase without starting anything."""
    cfg = _make_cfg()
    (
        state_ref,
        state_port,
        topology_mgr,
        _probe_mgr,
        _partitioner,
        serializer,
        gossip_config,
        engine,
    ) = _make_deps(cfg)

    state = NodeState(
        node_id=cfg.node_id,
        phase=MemberPhase.FAILED,
        generation=1,
        seq=0,
        tokens=(100,),
        epoch=1,
    )

    result = await _startup_phase_logic(
        cfg=cfg,
        phase=MemberPhase.FAILED,
        state=state,
        state_port=state_port,  # type: ignore[arg-type]
        topology_mgr=topology_mgr,
        client_ssl_ctx=object(),
        serializer=serializer,
        state_ref=state_ref,
        gossip_config=gossip_config,
        engine=engine,
    )

    assert result is False
