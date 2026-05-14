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
"""End-to-end regression test for the operator session in ``note-2.md``.

The bug captured here is the exact sequence the operator observed against a
freshly-started node-2 (phase ``IDLE``, no data yet):

  1. ``tourctl node join localhost:7801``   → phase ``IDLE`` → ``JOINING``
  2. ``tourctl node inspect localhost:7801`` → returns successfully
  3. ``tourctl rebalance status localhost:7801`` → **closed mid-flight**:

         Unknown envelope kind 'rebalance.status' from ('127.0.0.1', 53439);
         closing connection.

     surfaced to the operator as a raw ``ConnectionClosedError`` traceback.

The root cause was that ``_build_peer_dispatcher`` ran once at startup, and
gated rebalance-handler registration on the **startup** phase.  An ``IDLE``
node therefore never registered ``rebalance.status``, even after it later
transitioned to ``JOINING``.  After the fix, the rebalance applicator and
storage are constructed unconditionally and the three rebalance handlers are
always registered on the peer dispatcher.

This file exercises the full path the operator actually walks — real mTLS
TcpServer, real ``TcpClient``, real Typer command coroutine — so the
regression cannot reappear silently.  Unlike the existing e2e files, the
fixture starts the node in ``IDLE``.
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
from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.lifecycle.probe import ProbeManager
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ports.state import StatePort
from tourillon.core.rebalance.applicator import RebalanceApplicator
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.config import TourillonConfig
from tourillon.core.testing.mem_storage import InMemoryStorage
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
    """Lightweight in-memory StatePort that captures every save() call."""

    def __init__(self, initial: NodeState) -> None:
        self._state = initial
        self.saved: list[NodeState] = []

    async def load(self) -> NodeState | None:
        return self._state

    async def save(self, state: NodeState) -> None:
        self._state = state
        self.saved.append(state)


def _idle_state(node_id: str) -> NodeState:
    return NodeState(
        node_id=node_id,
        phase=MemberPhase.IDLE,
        generation=0,
        seq=0,
        tokens=(),
        epoch=0,
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
async def _running_idle_node(
    cfg: TourillonConfig,
) -> AsyncIterator[tuple[int, _InMemoryStatePort]]:
    """Start a real mTLS TcpServer whose peer dispatcher is built for IDLE.

    Mirrors production: rebalance_storage and rebalance_applicator are
    constructed unconditionally and passed to ``_build_peer_dispatcher`` so
    the three rebalance handlers are registered even for an IDLE startup
    phase — this is the post-fix wiring.
    """
    server_ssl = build_server_ssl_context(
        cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
    )
    client_ssl = build_client_ssl_context(
        cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
    )

    initial = _idle_state(cfg.node_id)
    state_port = _InMemoryStatePort(initial)
    state_ref: list[NodeState] = [initial]
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

    storage = InMemoryStorage()
    applicator = RebalanceApplicator(
        node_id=cfg.node_id,
        pool=pool,
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
        client_ssl_ctx=client_ssl,
        serializer=serializer,
        engine=engine,
        gossip_config=gossip_config,
        launch_bootstrap=None,
        peer_address=cfg.peer_server.advertise or cfg.peer_server.bind,
        kv_address=cfg.kv_server.advertise or cfg.kv_server.bind,
        applicator=applicator,
        storage=storage,
    )

    # Pre-flight invariant: the very thing the operator session broke on.
    assert dispatcher.lookup("rebalance.status") is not None
    assert dispatcher.lookup("rebalance.plan") is not None
    assert dispatcher.lookup("rebalance.transfer") is not None

    server = TcpServer(  # type: ignore[arg-type]
        dispatcher,
        ssl_context=server_ssl,
        name=f"e2e-idle-rebalance-{cfg.node_id}",
    )
    await server.start("127.0.0.1", 0)
    port: int = server._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    try:
        yield port, state_port
    finally:
        await server.stop()
        await pool.close_all()


async def _tourctl_rebalance_status(
    cfg: TourillonConfig,
    peer_address: str,
    *,
    timeout: float = 5.0,
) -> int:
    """Invoke the exact tourctl coroutine the CLI executes for `rebalance status`."""
    from tourctl.infra.cli.rebalance import _run_status

    return await _run_status(
        cfg.tls.cert_data,
        cfg.tls.key_data,
        cfg.tls.ca_data,
        peer_address,
        after_pid=0,
        limit=500,
        blocked_only=False,
        json_output=False,
        timeout=timeout,
    )


# ---------------------------------------------------------------------------
# Regression scenario from note-2.md
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.rebalance
async def test_e2e_idle_node_rebalance_status_does_not_close_connection(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An IDLE node answers `tourctl rebalance status` instead of closing the connection.

    Pre-fix behaviour (captured verbatim in ``note-2.md``):

      ``Unknown envelope kind 'rebalance.status' from ...; closing connection.``
      → ``ConnectionClosedError: Connection closed`` surfaced as a Rich traceback.

    Post-fix expectation:

      * The peer dispatcher registers ``rebalance.status`` even for an IDLE
        startup phase (verified inside the fixture).
      * ``tourctl rebalance status`` exits cleanly (exit code 0).
      * The operator sees a friendly "No active rebalance" message rather
        than a ``ConnectionClosedError`` traceback or any line starting
        with ``✗``.
    """
    cfg = _make_config(ca_material, leaf_material, "node-2")
    async with _running_idle_node(cfg) as (port, _state_port):
        exit_code = await _tourctl_rebalance_status(cfg, f"127.0.0.1:{port}")

    assert exit_code == 0

    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "No active rebalance" in combined, (
        "Expected the friendly 'No active rebalance' message, "
        f"got stdout={captured.out!r} stderr={captured.err!r}"
    )
    assert "ConnectionClosedError" not in combined, (
        "Operator must never see ConnectionClosedError: "
        "regression of note-2.md operator session"
    )
    assert "✗" not in combined, (
        "An error glyph indicates the command treated this as a failure; "
        "an IDLE node with no active rebalance is a success case."
    )


@pytest.mark.e2e
@pytest.mark.rebalance
async def test_e2e_rebalance_status_connection_closed_is_handled_gracefully(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """If the daemon ever drops the connection on rebalance.status, tourctl prints a message instead of crashing.

    This guards the operator-UX fix in
    ``tourctl/core/commands/rebalance.py``: ``ConnectionClosedError`` is
    converted into a single-line ``✗`` message and a non-zero exit code.

    The simulated condition uses an empty Dispatcher (no handlers
    registered) which is the wire-level equivalent of the broken note-2
    daemon — the server closes the connection without a response envelope.
    """
    from tourillon.core.transport.dispatcher import Dispatcher

    cfg = _make_config(ca_material, leaf_material, "node-2")
    server_ssl = build_server_ssl_context(
        cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
    )

    empty_dispatcher = Dispatcher()
    server = TcpServer(  # type: ignore[arg-type]
        empty_dispatcher,
        ssl_context=server_ssl,
        name="e2e-rebalance-empty-dispatcher",
    )
    await server.start("127.0.0.1", 0)
    try:
        port: int = server._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
        exit_code = await _tourctl_rebalance_status(
            cfg, f"127.0.0.1:{port}", timeout=2.0
        )
    finally:
        await server.stop()

    assert exit_code == 1

    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert (
        "ConnectionClosedError" not in combined
    ), "Raw transport exception name must not leak to the operator"
    assert "Peer closed the connection" in combined or "✗" in combined, (
        "Expected the friendly closed-connection error message; "
        f"got stdout={captured.out!r} stderr={captured.err!r}"
    )
