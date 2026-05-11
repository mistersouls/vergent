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

import pytest

from tourillon.core.gossip.config import GossipConfig
from tourillon.core.gossip.engine import GossipEngine
from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.lifecycle.probe import ProbeManager
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ports.state import StatePort
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
from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.core.transport.pool import PeerClientPool
from tourillon.infra.cli.node import _build_peer_dispatcher
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
def test_build_peer_dispatcher_registers_node_inspect_peer_view() -> None:
    """node.inspect.peer_view handler is registered in the returned Dispatcher."""
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

    assert dispatcher.lookup("node.inspect.peer_view") is not None


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
