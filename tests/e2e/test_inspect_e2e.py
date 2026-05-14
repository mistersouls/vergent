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
"""E2E tests for proposal 003 — direct node inspect over mTLS.

Mirrors what an operator does manually:

  load_config()             — validates config exactly as the daemon
  _build_peer_dispatcher()  — wires NodeInspectHandler exactly as _run_phase
  TcpServer                 — real mTLS socket on an ephemeral loopback port
  tourctl._run_inspect()    — the real tourctl coroutine, not a raw TcpClient

A handler-constructor mismatch or a CLI option mismatch is caught here at
the same wiring point as in production — closing the divergence between
e2e and manual operator runs.
"""

from __future__ import annotations

import base64
import contextlib
from collections.abc import AsyncIterator
from typing import Any

import pytest

from tourillon.bootstrap.config import load_config
from tourillon.core.gossip.config import GossipConfig
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


def _ready_state(node_id: str) -> NodeState:
    return NodeState(
        node_id=node_id,
        phase=MemberPhase.READY,
        generation=1,
        seq=1,
        tokens=(100, 200, 300, 400),
        epoch=1,
    )


def _idle_state(node_id: str) -> NodeState:
    return NodeState(
        node_id=node_id,
        phase=MemberPhase.IDLE,
        generation=0,
        seq=0,
        tokens=(),
        epoch=0,
    )


def _ready_member(node_id: str, peer_address: str, tokens: tuple[int, ...]) -> Member:
    return Member(
        node_id=node_id,
        peer_address=peer_address,
        generation=1,
        seq=1,
        phase=MemberPhase.READY,
        tokens=tokens,
        partition_shift=_SHIFT,
    )


def _make_config(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
    node_id: str,
) -> TourillonConfig:
    """Build a TourillonConfig via load_config() — same path as daemon startup."""
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
        "cluster": {"seeds": [], "rf": 3, "partition_shift": _SHIFT},
    }
    return load_config(raw)


@contextlib.asynccontextmanager
async def _running_inspect_node(
    cfg: TourillonConfig,
    initial_state: NodeState,
    extra_members: list[Member] | None = None,
) -> AsyncIterator[tuple[int, TopologyManager]]:
    """Wire a node via _build_peer_dispatcher() and start a real mTLS TcpServer."""
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
    partitioner = Partitioner(HashSpace(), cfg.partition_shift)
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

    # Seed registry so members appear in inspect response.
    self_member = _ready_member(
        cfg.node_id,
        cfg.peer_server.advertise or cfg.peer_server.bind,
        initial_state.tokens,
    )
    await topology_mgr.apply_member(self_member)
    for m in extra_members or []:
        await topology_mgr.apply_member(m)

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
        launch_bootstrap=None,
        peer_address=cfg.peer_server.advertise or cfg.peer_server.bind,
        kv_address=cfg.kv_server.advertise or cfg.kv_server.bind,
    )

    server = TcpServer(  # type: ignore[arg-type]
        dispatcher, ssl_context=server_ssl, name=f"e2e-inspect-{cfg.node_id}"
    )
    await server.start("127.0.0.1", 0)
    port: int = server._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    try:
        yield port, topology_mgr
    finally:
        await server.stop()
        await pool.close_all()


async def _tourctl_inspect(
    cfg: TourillonConfig,
    peer_address: str,
    *,
    show_all_partitions: bool = False,
    json_output: bool = False,
    timeout: float = 5.0,
) -> int:
    """Invoke tourctl._run_inspect() — the exact coroutine the CLI runs."""
    from tourctl.infra.cli.node import _run_inspect

    return await _run_inspect(
        cfg.tls.cert_data,
        cfg.tls.key_data,
        cfg.tls.ca_data,
        peer_address,
        show_all_partitions=show_all_partitions,
        json_output=json_output,
        timeout=timeout,
    )


# ---------------------------------------------------------------------------
# Scenario 17: direct connection to a running READY node returns full snapshot
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.inspect
async def test_e2e_inspect_direct_ready_node_ok(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """tourctl node inspect <addr> connects directly; READY node self-reports."""
    cfg = _make_config(ca_material, leaf_material, "node-2")
    async with _running_inspect_node(cfg, _ready_state("node-2")) as (port, _):
        exit_code = await _tourctl_inspect(cfg, f"127.0.0.1:{port}")
    assert exit_code == 0


# ---------------------------------------------------------------------------
# Scenario 18: target unreachable — connection refused
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.inspect
async def test_e2e_inspect_target_unreachable(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """Server not running at <addr>; tourctl prints unreachable; exit code 1."""
    cfg = _make_config(ca_material, leaf_material, "node-2")
    # Port 1 is privileged and reliably refused on loopback.
    exit_code = await _tourctl_inspect(cfg, "127.0.0.1:1")
    assert exit_code == 1


# ---------------------------------------------------------------------------
# Scenario 19: --json emits parseable JSON to stdout
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.inspect
async def test_e2e_inspect_json_output_is_valid(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--json emits valid JSON; all payload fields defined in the schema are present."""
    import json

    cfg = _make_config(ca_material, leaf_material, "node-2")
    async with _running_inspect_node(cfg, _ready_state("node-2")) as (port, _):
        exit_code = await _tourctl_inspect(cfg, f"127.0.0.1:{port}", json_output=True)
    assert exit_code == 0

    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    for field in (
        "node_id",
        "phase",
        "peer_address",
        "kv_address",
        "size",
        "generation",
        "seq",
        "epoch",
        "tokens",
        "total_partitions",
        "partition_shift",
        "owned_partitions",
        "partition_ranges",
        "members",
        "members_truncated",
        "members_total",
        "probe_states",
        "probe_states_truncated",
    ):
        assert field in parsed, f"missing {field} in JSON response"
    assert (
        "forwarded_by" not in parsed
    ), "Proposal 003 rev 2 removes forwarded_by from the response"


# ---------------------------------------------------------------------------
# Extra: IDLE node returns minimal response (proposal 003 rev 2 invariant)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.inspect
async def test_e2e_inspect_idle_node_returns_minimal_response(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An IDLE node returns node.inspect.response with phase='idle'; not an error."""
    import json

    cfg = _make_config(ca_material, leaf_material, "node-2")
    async with _running_inspect_node(cfg, _idle_state("node-2")) as (port, _):
        exit_code = await _tourctl_inspect(cfg, f"127.0.0.1:{port}", json_output=True)
    assert exit_code == 0

    parsed = json.loads(capsys.readouterr().out)
    assert parsed["phase"] == "idle"
