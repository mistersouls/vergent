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
"""E2E tests for proposal 004 — seeded node join and gossip bootstrap discovery.

Every test goes through the same code paths an operator would exercise:

  load_config()               — validates config as the daemon does on startup
  _build_peer_dispatcher()    — wires all handlers exactly as _run_phase does
  TcpServer                   — real mTLS socket on an ephemeral loopback port
  tourctl._run_join()         — the actual tourctl coroutine, not a raw TcpClient

This means a handler constructor change (wrong keyword argument, missing
parameter) is caught here at the same wiring point as in production.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Any

import pytest

from tourillon.bootstrap.config import load_config
from tourillon.core.gossip.bootstrapper import GossipBootstrapper
from tourillon.core.gossip.config import GossipBootstrapConfig, GossipConfig
from tourillon.core.gossip.engine import GossipEngine
from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.lifecycle.probe import ProbeManager
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ports.state import StatePort
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.config import TourillonConfig
from tourillon.core.transport.pool import PeerClientPool
from tourillon.core.transport.server import TcpServer
from tourillon.infra.cli.node import _build_peer_dispatcher
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter
from tourillon.infra.tls.context import (
    build_client_ssl_context,
    build_server_ssl_context,
)

_SHIFT = 12


# ---------------------------------------------------------------------------
# In-memory StatePort
# ---------------------------------------------------------------------------


class _InMemoryStatePort(StatePort):
    """Lightweight in-memory StatePort for e2e fixtures."""

    def __init__(self, initial: NodeState) -> None:
        self._state = initial
        self.saved: list[NodeState] = []

    async def load(self) -> NodeState | None:
        return self._state

    async def save(self, state: NodeState) -> None:
        self._state = state
        self.saved.append(state)


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------


def _idle_state(node_id: str) -> NodeState:
    """Return a fresh IDLE NodeState."""
    return NodeState(
        node_id=node_id,
        phase=MemberPhase.IDLE,
        generation=0,
        seq=0,
        tokens=(),
        epoch=0,
    )


def _joining_state(node_id: str) -> NodeState:
    """Return a NodeState already in JOINING phase."""
    return NodeState(
        node_id=node_id,
        phase=MemberPhase.JOINING,
        generation=1,
        seq=1,
        tokens=(1, 2, 3, 4),
        epoch=0,
    )


def _ready_member(node_id: str, peer_address: str) -> Member:
    """Return a READY Member for seeding topology fixtures."""
    return Member(
        node_id=node_id,
        peer_address=peer_address,
        generation=1,
        seq=1,
        phase=MemberPhase.READY,
        tokens=(1024, 5120),
        partition_shift=_SHIFT,
    )


# ---------------------------------------------------------------------------
# Operator-realistic node fixture
#
# _make_config()  — builds TourillonConfig via load_config() from raw PEM bytes,
#                   exactly as `tourillon config generate` + `tourillon node start`
#                   would do.
#
# _running_node() — wires all handlers via _build_peer_dispatcher() and starts a
#                   real TcpServer; any constructor mismatch raises TypeError here,
#                   the same as it would for a real operator running the daemon.
# ---------------------------------------------------------------------------


def _make_config(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
    node_id: str,
    seeds: list[str] | None = None,
) -> TourillonConfig:
    """Build a TourillonConfig via load_config() as the daemon does on startup.

    Uses port 0 so the caller can bind an ephemeral server without reserving
    a fixed port.  The advertise address mirrors the bind address; tests that
    need the real port update topology after the server is bound.
    """
    ca_cert_pem, _ = ca_material
    leaf_cert_pem, leaf_key_pem = leaf_material
    ca_b64 = base64.b64encode(ca_cert_pem).decode()
    cert_b64 = base64.b64encode(leaf_cert_pem).decode()
    key_b64 = base64.b64encode(leaf_key_pem).decode()
    raw: dict[str, Any] = {
        "schema_version": 1,
        "node": {"id": node_id, "size": "M", "data_dir": "./node-data"},
        "tls": {"cert_data": cert_b64, "key_data": key_b64, "ca_data": ca_b64},
        "servers": {
            "kv": {"bind": "127.0.0.1:17700", "advertise": "127.0.0.1:17700"},
            "peer": {"bind": "127.0.0.1:17701", "advertise": "127.0.0.1:17701"},
        },
        "cluster": {"seeds": seeds or [], "rf": 3, "partition_shift": _SHIFT},
    }
    return load_config(raw)


@contextlib.asynccontextmanager
async def _running_node(
    cfg: TourillonConfig,
    initial_state: NodeState,
    launch_bootstrap: Callable[[list[str]], Coroutine[Any, Any, None]] | None = None,
) -> AsyncIterator[tuple[int, _InMemoryStatePort, TopologyManager]]:
    """Wire a node via _build_peer_dispatcher() and start a real mTLS TcpServer.

    Yields (bound_port, state_port, topology_manager).  Any TypeError from a
    mismatched handler constructor is raised here — the same failure point as
    in _run_phase() for a real daemon.
    """
    server_ssl = build_server_ssl_context(
        cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
    )
    client_ssl = build_client_ssl_context(
        cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
    )

    state_port = _InMemoryStatePort(initial_state)
    state_ref: list[NodeState] = [initial_state]
    topology_mgr = TopologyManager()
    probe_mgr = ProbeManager()
    hash_space = HashSpace()
    partitioner = Partitioner(hash_space, cfg.partition_shift)
    serializer = MsgpackSerializerAdapter()
    gossip_config = GossipConfig()
    pool = PeerClientPool(ssl_ctx=client_ssl)  # type: ignore[arg-type]
    engine = GossipEngine(
        node_id=cfg.node_id,
        topology_manager=topology_mgr,
        pool=pool,
        config=gossip_config,
        partition_shift=cfg.partition_shift,
        serializer=serializer,
    )

    # Full handler wiring — same code path as _run_phase in production.
    dispatcher = _build_peer_dispatcher(
        cfg=cfg,
        state_ref=state_ref,
        state_port=state_port,  # type: ignore[arg-type]
        topology_mgr=topology_mgr,
        probe_mgr=probe_mgr,
        partitioner=partitioner,
        client_ssl_ctx=client_ssl,
        serializer=serializer,
        engine=engine,
        gossip_config=gossip_config,
        launch_bootstrap=launch_bootstrap,
        peer_address=cfg.peer_server.advertise or cfg.peer_server.bind,
        kv_address=cfg.kv_server.advertise or cfg.kv_server.bind,
    )

    server = TcpServer(  # type: ignore[arg-type]
        dispatcher, ssl_context=server_ssl, name=f"e2e-{cfg.node_id}"
    )
    await server.start("127.0.0.1", 0)
    port: int = server._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    try:
        yield port, state_port, topology_mgr
    finally:
        await server.stop()
        await pool.close_all()


# ---------------------------------------------------------------------------
# tourctl helper — runs the actual tourctl coroutine against a real server
# ---------------------------------------------------------------------------


async def _tourctl_join(
    cfg: TourillonConfig,
    peer_address: str,
    seeds: list[str],
    timeout: float = 5.0,
) -> int:
    """Invoke tourctl._run_join() using TLS credentials from *cfg*.

    This is the exact coroutine tourctl calls after loading its context —
    same TLS path, same envelope construction, same error handling.
    """
    from tourctl.infra.cli.node import _run_join

    return await _run_join(
        cfg.tls.cert_data,
        cfg.tls.key_data,
        cfg.tls.ca_data,
        peer_address,
        seeds=seeds,
        timeout=timeout,
    )


# ---------------------------------------------------------------------------
# E2E scenario 2 — direct join, success
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.gossip
async def test_e2e_join_direct_ok(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """tourctl connects directly; node handles node.join; state saved as JOINING, generation=1."""
    cfg = _make_config(ca_material, leaf_material, "node-2", seeds=["127.0.0.1:9999"])

    async with _running_node(cfg, _idle_state("node-2")) as (port, state_port, _):
        exit_code = await _tourctl_join(
            cfg, f"127.0.0.1:{port}", seeds=["127.0.0.1:9999"]
        )

    assert exit_code == 0
    assert len(state_port.saved) == 1
    saved = state_port.saved[0]
    assert saved.phase == MemberPhase.JOINING
    assert saved.generation == 1
    assert len(saved.tokens) == 4


# ---------------------------------------------------------------------------
# E2E scenario 3 — wrong phase
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.gossip
async def test_e2e_join_wrong_phase(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """Node already JOINING; tourctl receives node.join.error code: wrong_phase."""
    cfg = _make_config(ca_material, leaf_material, "node-2")

    async with _running_node(cfg, _joining_state("node-2")) as (port, state_port, _):
        exit_code = await _tourctl_join(cfg, f"127.0.0.1:{port}", seeds=[])

    assert exit_code == 1
    assert len(state_port.saved) == 0


# ---------------------------------------------------------------------------
# E2E scenario 4 — no seeds
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.gossip
async def test_e2e_join_no_seeds(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """IDLE node with no config seeds; tourctl receives node.join.error code: no_seeds."""
    cfg = _make_config(ca_material, leaf_material, "node-2", seeds=[])

    async with _running_node(cfg, _idle_state("node-2")) as (port, state_port, _):
        exit_code = await _tourctl_join(cfg, f"127.0.0.1:{port}", seeds=[])

    assert exit_code == 1
    assert len(state_port.saved) == 0


# ---------------------------------------------------------------------------
# E2E scenario 35 — two-node gossip bootstrap discovery
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.gossip
async def test_e2e_35_two_node_join_gossip_bootstrap(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """Scenario 35: tourctl joins node-2; gossip bootstrap discovers node-1.

    node-1 is started via _running_node — all peer handlers including
    gossip.digest are wired by _build_peer_dispatcher.  node-2 is IDLE.
    tourctl sends node.join to node-2.  After bootstrap, node-2's registry
    contains node-1.  node-2 remains JOINING (KV server guard invariant).
    """
    cfg1 = _make_config(ca_material, leaf_material, "node-1")
    cfg2 = _make_config(ca_material, leaf_material, "node-2")

    bootstrap_done = asyncio.Event()

    async with _running_node(cfg1, _idle_state("node-1")) as (
        node1_port,
        _,
        node1_topology,
    ):
        seed_addr = f"127.0.0.1:{node1_port}"
        # Seed node-1's topology so gossip.digest can serve it.
        await node1_topology.apply_member(_ready_member("node-1", seed_addr))

        client_ssl_2 = build_client_ssl_context(
            cfg2.tls.cert_data, cfg2.tls.key_data, cfg2.tls.ca_data
        )
        serializer = MsgpackSerializerAdapter()

        # Late-binding list so the closure can capture node-2's topology after
        # _running_node creates it.
        node2_topology_ref: list[TopologyManager] = []

        async def launch_bootstrap(seeds: list[str]) -> None:
            gossip_cfg = GossipBootstrapConfig(
                max_retries=5, initial_delay_s=0.05, jitter=0.0
            )
            bootstrapper = GossipBootstrapper(
                topology_manager=node2_topology_ref[0],
                config=gossip_cfg,
                partition_shift=_SHIFT,
                ssl_ctx=client_ssl_2,  # type: ignore[arg-type]
                serializer=serializer,
            )
            await bootstrapper.run(seeds)
            bootstrap_done.set()

        async with _running_node(
            cfg2, _idle_state("node-2"), launch_bootstrap=launch_bootstrap
        ) as (node2_port, node2_state_port, node2_topology):
            node2_topology_ref.append(node2_topology)

            exit_code = await _tourctl_join(
                cfg2, f"127.0.0.1:{node2_port}", seeds=[seed_addr]
            )
            assert exit_code == 0, "tourctl node join must return 0"

            async with asyncio.timeout(5.0):
                await bootstrap_done.wait()

    # node-2 is JOINING — KV server must not be bound (phase guard invariant).
    assert node2_state_port.saved[-1].phase == MemberPhase.JOINING
    assert node2_state_port._state.phase not in (
        MemberPhase.READY,
        MemberPhase.DRAINING,
    )

    # node-2 discovered node-1 via gossip bootstrap.
    snap2 = await node2_topology.snapshot()
    node1_record = snap2.registry.get("node-1")
    assert (
        node1_record is not None
    ), "node-2 registry must contain node-1 after bootstrap"
    assert node1_record.phase == MemberPhase.READY


# ---------------------------------------------------------------------------
# E2E tourctl — unreachable target
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.gossip
async def test_e2e_tourctl_run_join_unreachable(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """tourctl._run_join() to an unreachable address returns exit code 1."""
    cfg = _make_config(ca_material, leaf_material, "node-2")
    # Port 1 is always refused on loopback.
    exit_code = await _tourctl_join(cfg, "127.0.0.1:1", seeds=[], timeout=2.0)
    assert exit_code == 1


# ---------------------------------------------------------------------------
# E2E tourctl CLI — argument validation (no server needed)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.gossip
def test_e2e_tourctl_cli_join_no_context(tmp_path: Any) -> None:
    """tourctl node join exits 1 with a clear message when no active context is set."""
    from pathlib import Path

    from typer.testing import CliRunner

    from tourctl.bootstrap.main import app as tourctl_app

    runner = CliRunner()
    empty_contexts = Path(str(tmp_path)) / "contexts.toml"
    empty_contexts.write_text("schema_version = 1\n")

    result = runner.invoke(
        tourctl_app,
        ["node", "join", "127.0.0.1:7701", "--contexts", str(empty_contexts)],
    )
    assert result.exit_code == 1


@pytest.mark.e2e
@pytest.mark.gossip
def test_e2e_tourctl_cli_join_help() -> None:
    """tourctl node join --help shows the peer-address positional argument."""
    from typer.testing import CliRunner

    from tourctl.bootstrap.main import app as tourctl_app

    runner = CliRunner()
    result = runner.invoke(tourctl_app, ["node", "join", "--help"])
    assert result.exit_code == 0
    assert "peer-address" in result.output.lower() or "PEER_ADDRESS" in result.output
