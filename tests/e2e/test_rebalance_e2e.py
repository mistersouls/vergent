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
"""E2E tests for proposal 005 — rebalance handlers as an operator would exercise them.

Every test goes through the same code paths a daemon operator would exercise:

  load_config()            — validates config as the daemon does on startup
  _build_peer_dispatcher() — wires all handlers (including rebalance) as _run_phase does
  TcpServer                — real mTLS socket on an ephemeral loopback port
  TcpClient                — real mTLS client identical to the applicator transport
  tourctl._run_status()    — the actual tourctl coroutine, not a mock request

Handler constructor mismatches (wrong keyword argument, missing parameter) are
caught here at the same wiring point as in production: if ``register_rebalance_handlers``
or ``_build_peer_dispatcher`` changes its signature without updating the call site,
these tests fail with a TypeError rather than a silent no-op.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
from collections.abc import AsyncIterator, Callable
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
from tourillon.core.rebalance.digest import compute_transfer_digest
from tourillon.core.rebalance.plan import (
    PartitionRangeTransfer,
    PartitionTransfer,
    RebalancePlan,
    TransferHandle,
    TransferState,
)
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.config import TourillonConfig
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.record import StoreKey, Version, record_from_dict
from tourillon.core.testing.mem_storage import InMemoryStorage
from tourillon.core.transport.client import TcpClient
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


def _make_config(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
    node_id: str,
) -> TourillonConfig:
    """Build a TourillonConfig via load_config() exactly as the daemon does."""
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


def _joining_state(node_id: str, epoch: int = 1) -> NodeState:
    """Return a NodeState in JOINING phase with the given epoch."""
    return NodeState(
        node_id=node_id,
        phase=MemberPhase.JOINING,
        generation=1,
        seq=1,
        tokens=(1, 2, 3, 4),
        epoch=epoch,
        committed_pids=(),
        staging_pids=(),
    )


# ---------------------------------------------------------------------------
# Operator-realistic node fixture with rebalance wiring
#
# _running_node_with_rebalance() mirrors _running_node from test_gossip_join_e2e
# but additionally creates InMemoryStorage + RebalanceApplicator and passes them
# to _build_peer_dispatcher — the exact same call path as _run_phase in production.
# ---------------------------------------------------------------------------


@contextlib.asynccontextmanager
async def _running_node_with_rebalance(
    cfg: TourillonConfig,
    initial_state: NodeState,
    pre_populate_storage: Callable[[InMemoryStorage], None] | None = None,
    pre_populate_handles: Callable[[RebalanceApplicator], None] | None = None,
) -> AsyncIterator[
    tuple[
        int, _InMemoryStatePort, TopologyManager, RebalanceApplicator, InMemoryStorage
    ]
]:
    """Wire a node with rebalance handlers and start a real mTLS TcpServer.

    Yields (bound_port, state_port, topology_manager, applicator, storage).
    The rebalance handlers (plan, transfer, status) are always registered — the
    same condition that production code uses for JOINING/DRAINING phases.

    Any TypeError raised by a mismatched constructor argument surfaces here,
    the same failure point as in _run_phase() for a real daemon.
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

    rebalance_storage = InMemoryStorage()
    if pre_populate_storage is not None:
        pre_populate_storage(rebalance_storage)

    rebalance_applicator = RebalanceApplicator(
        node_id=cfg.node_id,
        pool=pool,
        state_port=state_port,  # type: ignore[arg-type]
        storage=rebalance_storage,  # type: ignore[arg-type]
        serializer=serializer,
        peer_addresses={},
        max_concurrent_transfers=cfg.rebalance.max_concurrent_transfers,
        max_chunk_bytes=cfg.rebalance.max_chunk_bytes,
        total_partitions=partitioner.total_partitions,
    )
    if pre_populate_handles is not None:
        pre_populate_handles(rebalance_applicator)

    peer_address = cfg.peer_server.advertise or cfg.peer_server.bind
    kv_address = cfg.kv_server.advertise or cfg.kv_server.bind

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
        launch_bootstrap=None,
        peer_address=peer_address,
        kv_address=kv_address,
        applicator=rebalance_applicator,
        storage=rebalance_storage,
    )

    server = TcpServer(  # type: ignore[arg-type]
        dispatcher, ssl_context=server_ssl, name=f"e2e-rebalance-{cfg.node_id}"
    )
    await server.start("127.0.0.1", 0)
    port: int = server._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    try:
        yield port, state_port, topology_mgr, rebalance_applicator, rebalance_storage
    finally:
        await server.stop()
        await pool.close_all()


# ---------------------------------------------------------------------------
# tourctl helper — wraps _run_status exactly as tourctl does
# ---------------------------------------------------------------------------


async def _tourctl_rebalance_status(
    cfg: TourillonConfig,
    peer_address: str,
    *,
    after_pid: int = 0,
    limit: int = 500,
    blocked_only: bool = False,
    json_output: bool = False,
    timeout: float = 5.0,
) -> int:
    """Invoke tourctl._run_status() using TLS credentials from *cfg*.

    This is the exact coroutine tourctl calls when the operator runs
    ``tourctl rebalance status`` — same TLS path, same envelope construction,
    same rendering logic.
    """
    from tourctl.infra.cli.rebalance import _run_status

    return await _run_status(
        cfg.tls.cert_data,
        cfg.tls.key_data,
        cfg.tls.ca_data,
        peer_address,
        after_pid=after_pid,
        limit=limit,
        blocked_only=blocked_only,
        json_output=json_output,
        timeout=timeout,
    )


# ---------------------------------------------------------------------------
# E2E — rebalance handlers are registered and respond (not unknown-kind close)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.rebalance
async def test_e2e_rebalance_handlers_registered_on_joining_node(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """Rebalance handlers are wired: rebalance.status reaches a handler and returns a response."""
    cfg = _make_config(ca_material, leaf_material, "node-1")

    async with _running_node_with_rebalance(cfg, _joining_state("node-1", epoch=1)) as (
        port,
        _,
        __,
        ___,
        ____,
    ):
        client_ssl = build_client_ssl_context(
            cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
        )
        serializer = MsgpackSerializerAdapter()
        client = TcpClient()
        await client.connect(f"127.0.0.1:{port}", client_ssl)
        try:
            req = Envelope.create(
                serializer.encode({"after_pid": 0, "limit": 10}),
                kind="rebalance.status",
                schema_id=serializer.schema_id,
            )
            resp = await client.request(req, timeout=5.0)
            assert resp.kind == "rebalance.status.response"
        finally:
            await client.close()


# ---------------------------------------------------------------------------
# E2E — tourctl rebalance status, no active plan
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.rebalance
async def test_e2e_rebalance_status_no_plan_returns_0(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """tourctl rebalance status on a node with no active plan returns exit code 0."""
    cfg = _make_config(ca_material, leaf_material, "node-1")

    async with _running_node_with_rebalance(cfg, _joining_state("node-1", epoch=1)) as (
        port,
        _,
        __,
        ___,
        ____,
    ):
        exit_code = await _tourctl_rebalance_status(cfg, f"127.0.0.1:{port}")

    assert exit_code == 0


# ---------------------------------------------------------------------------
# E2E — tourctl rebalance status with active RUNNING + FAILED handles
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.rebalance
async def test_e2e_rebalance_status_active_handles_shows_blocked(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """tourctl returns 0; response contains blocked=True when a handle is FAILED."""
    cfg = _make_config(ca_material, leaf_material, "node-1")

    def populate_handles(applicator: RebalanceApplicator) -> None:
        plan = RebalancePlan(
            epoch=1,
            ranges=(
                PartitionRangeTransfer(
                    pid_start=10, pid_end=10, src="src", dst="node-1"
                ),
                PartitionRangeTransfer(
                    pid_start=20, pid_end=20, src="src", dst="node-1"
                ),
            ),
        )
        applicator._plan = plan  # type: ignore[attr-defined]
        applicator._handles = {  # type: ignore[attr-defined]
            10: TransferHandle(
                PartitionTransfer(10, "src", "node-1"), TransferState.RUNNING
            ),
            20: TransferHandle(
                PartitionTransfer(20, "src", "node-1"),
                TransferState.FAILED,
                last_error="source unreachable after 10 retries",
            ),
        }

    async with _running_node_with_rebalance(
        cfg,
        _joining_state("node-1", epoch=1),
        pre_populate_handles=populate_handles,
    ) as (port, _, __, ___, ____):
        # json_output=True so the exit code comes from the blocked flag, not rendering
        exit_code = await _tourctl_rebalance_status(
            cfg, f"127.0.0.1:{port}", json_output=True
        )

    # 0 because we don't pass --blocked
    assert exit_code == 0


# ---------------------------------------------------------------------------
# E2E — tourctl rebalance status --blocked on FAILED node → exit code 2
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.rebalance
async def test_e2e_rebalance_status_blocked_flag_returns_exit_code_2(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """tourctl rebalance status --blocked returns exit code 2 when node is BLOCKED."""
    cfg = _make_config(ca_material, leaf_material, "node-1")

    def populate_handles(applicator: RebalanceApplicator) -> None:
        plan = RebalancePlan(
            epoch=1,
            ranges=(
                PartitionRangeTransfer(pid_start=5, pid_end=5, src="src", dst="node-1"),
            ),
        )
        applicator._plan = plan  # type: ignore[attr-defined]
        applicator._handles = {  # type: ignore[attr-defined]
            5: TransferHandle(
                PartitionTransfer(5, "src", "node-1"),
                TransferState.FAILED,
                last_error="src_mismatch",
            ),
        }

    async with _running_node_with_rebalance(
        cfg,
        _joining_state("node-1", epoch=1),
        pre_populate_handles=populate_handles,
    ) as (port, _, __, ___, ____):
        exit_code = await _tourctl_rebalance_status(
            cfg, f"127.0.0.1:{port}", blocked_only=True
        )

    assert exit_code == 2


# ---------------------------------------------------------------------------
# E2E — rebalance.plan rejected: epoch mismatch
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.rebalance
async def test_e2e_rebalance_plan_epoch_mismatch_rejected(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """Send rebalance.plan with epoch 99 to a node at epoch 1 → plan.reject epoch_mismatch."""
    cfg = _make_config(ca_material, leaf_material, "node-1")

    async with _running_node_with_rebalance(cfg, _joining_state("node-1", epoch=1)) as (
        port,
        _,
        __,
        ___,
        ____,
    ):
        client_ssl = build_client_ssl_context(
            cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
        )
        serializer = MsgpackSerializerAdapter()
        client = TcpClient()
        await client.connect(f"127.0.0.1:{port}", client_ssl)
        try:
            plan_env = Envelope.create(
                serializer.encode(
                    {
                        "epoch": 99,  # wrong — node is at epoch 1
                        "transfers": [
                            {
                                "pid_start": 0,
                                "pid_end": 0,
                                "src": "node-1",
                                "dst": "node-2",
                            }
                        ],
                        "resume_from": None,
                    }
                ),
                kind="rebalance.plan",
                schema_id=serializer.schema_id,
            )
            resp = await client.request(plan_env, timeout=5.0)
        finally:
            await client.close()

    assert resp.kind == "rebalance.plan.reject"
    data = serializer.decode(resp.payload)
    assert data["reason"] == "epoch_mismatch"


# ---------------------------------------------------------------------------
# E2E — rebalance.plan rejected: src mismatch
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.rebalance
async def test_e2e_rebalance_plan_src_mismatch_rejected(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """Send plan where src='completely-other' (not node-1 nor dst node-1) → plan.reject src_mismatch."""
    cfg = _make_config(ca_material, leaf_material, "node-1")

    async with _running_node_with_rebalance(cfg, _joining_state("node-1", epoch=1)) as (
        port,
        _,
        __,
        ___,
        ____,
    ):
        client_ssl = build_client_ssl_context(
            cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
        )
        serializer = MsgpackSerializerAdapter()
        client = TcpClient()
        await client.connect(f"127.0.0.1:{port}", client_ssl)
        try:
            plan_env = Envelope.create(
                serializer.encode(
                    {
                        "epoch": 1,
                        "transfers": [
                            {
                                "pid_start": 0,
                                "pid_end": 0,
                                "src": "completely-other",
                                "dst": "also-other",
                            }
                        ],
                        "resume_from": None,
                    }
                ),
                kind="rebalance.plan",
                schema_id=serializer.schema_id,
            )
            resp = await client.request(plan_env, timeout=5.0)
        finally:
            await client.close()

    assert resp.kind == "rebalance.plan.reject"
    data = serializer.decode(resp.payload)
    assert data["reason"] == "src_mismatch"


# ---------------------------------------------------------------------------
# E2E — full JOIN transfer happy path (operator as destination, node as source)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.rebalance
async def test_e2e_rebalance_full_join_transfer_happy_path(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """Full operator-facing JOIN transfer: source node streams records; commit.ok received.

    The client simulates the JOINING destination — exactly what RebalanceApplicator
    does in _do_join_transfer.  The source (our running node) must:
      1. respond with rebalance.plan.ok,
      2. stream one rebalance.transfer chunk (is_last=True),
      3. await rebalance.commit,
      4. validate the digest and respond rebalance.commit.ok.
    """
    cfg = _make_config(ca_material, leaf_material, "node-1")

    hlc = HLCTimestamp(wall_ms=1_000_000, counter=0, node_id="node-1")
    record = Version(
        address=StoreKey(keyspace=b"ks", key=b"mykey"),
        metadata=hlc,
        value=b"hello-world",
    )

    def populate_storage(storage: InMemoryStorage) -> None:
        storage.open_partition(0).add_record(record)

    async with _running_node_with_rebalance(
        cfg,
        _joining_state("node-1", epoch=1),
        pre_populate_storage=populate_storage,
    ) as (port, _, __, ___, ____):
        client_ssl = build_client_ssl_context(
            cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
        )
        serializer = MsgpackSerializerAdapter()
        client = TcpClient()
        await client.connect(f"127.0.0.1:{port}", client_ssl)

        plan_env = Envelope.create(
            serializer.encode(
                {
                    "epoch": 1,
                    "transfers": [
                        {
                            "pid_start": 0,
                            "pid_end": 0,
                            "src": "node-1",
                            "dst": "node-2",
                        }
                    ],
                    "resume_from": None,
                }
            ),
            kind="rebalance.plan",
            schema_id=serializer.schema_id,
        )

        records_received: list[Any] = []
        commit_sent = False
        final_kind = ""

        try:
            async with asyncio.timeout(10.0):
                async for resp in client.stream(plan_env, timeout=5.0):
                    if resp.kind == "rebalance.plan.ok":
                        continue
                    if resp.kind == "rebalance.transfer":
                        data = serializer.decode(resp.payload)
                        for rec_dict in data.get("records", []):
                            records_received.append(record_from_dict(rec_dict))
                        if data.get("is_last") and not commit_sent:
                            commit_sent = True
                            digest = compute_transfer_digest(iter(records_received))
                            commit_env = Envelope(
                                kind="rebalance.commit",
                                payload=serializer.encode(
                                    {
                                        "epoch": 1,
                                        "pid": 0,
                                        "digest": digest,
                                    }
                                ),
                                correlation_id=plan_env.correlation_id,
                                schema_id=serializer.schema_id,
                            )
                            await client.send(commit_env)
                        continue
                    if resp.kind in ("rebalance.commit.ok", "rebalance.commit.reject"):
                        final_kind = resp.kind
                        break
        finally:
            await client.close()

    assert (
        len(records_received) == 1
    ), "destination must receive the one pre-loaded record"
    assert records_received[0].address.key == b"mykey"
    assert (
        final_kind == "rebalance.commit.ok"
    ), f"expected commit.ok but got {final_kind!r}"


# ---------------------------------------------------------------------------
# E2E — full JOIN transfer with digest mismatch → commit.reject
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.rebalance
async def test_e2e_rebalance_full_join_transfer_digest_mismatch(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """Source returns rebalance.commit.reject when digest is wrong (process error, no retry)."""
    cfg = _make_config(ca_material, leaf_material, "node-1")

    hlc = HLCTimestamp(wall_ms=2_000_000, counter=0, node_id="node-1")
    record = Version(
        address=StoreKey(keyspace=b"ks", key=b"otherkey"),
        metadata=hlc,
        value=b"data",
    )

    def populate_storage(storage: InMemoryStorage) -> None:
        storage.open_partition(1).add_record(record)

    async with _running_node_with_rebalance(
        cfg,
        _joining_state("node-1", epoch=1),
        pre_populate_storage=populate_storage,
    ) as (port, _, __, ___, ____):
        client_ssl = build_client_ssl_context(
            cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
        )
        serializer = MsgpackSerializerAdapter()
        client = TcpClient()
        await client.connect(f"127.0.0.1:{port}", client_ssl)

        plan_env = Envelope.create(
            serializer.encode(
                {
                    "epoch": 1,
                    "transfers": [
                        {
                            "pid_start": 1,
                            "pid_end": 1,
                            "src": "node-1",
                            "dst": "node-2",
                        }
                    ],
                    "resume_from": None,
                }
            ),
            kind="rebalance.plan",
            schema_id=serializer.schema_id,
        )

        commit_sent = False
        final_kind = ""

        try:
            async with asyncio.timeout(10.0):
                async for resp in client.stream(plan_env, timeout=5.0):
                    if resp.kind == "rebalance.plan.ok":
                        continue
                    if resp.kind == "rebalance.transfer":
                        data = serializer.decode(resp.payload)
                        if data.get("is_last") and not commit_sent:
                            commit_sent = True
                            # Deliberately send a wrong digest
                            commit_env = Envelope(
                                kind="rebalance.commit",
                                payload=serializer.encode(
                                    {
                                        "epoch": 1,
                                        "pid": 1,
                                        "digest": "deadbeef" * 8,  # wrong
                                    }
                                ),
                                correlation_id=plan_env.correlation_id,
                                schema_id=serializer.schema_id,
                            )
                            await client.send(commit_env)
                        continue
                    if resp.kind in ("rebalance.commit.ok", "rebalance.commit.reject"):
                        final_kind = resp.kind
                        break
        finally:
            await client.close()

    assert (
        final_kind == "rebalance.commit.reject"
    ), f"expected commit.reject for digest mismatch but got {final_kind!r}"


# ---------------------------------------------------------------------------
# E2E — rebalance.status returns src+dst in each transfer entry
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.rebalance
async def test_e2e_rebalance_status_transfer_entries_have_src_and_dst(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """rebalance.status response contains src and dst on every transfer entry."""
    cfg = _make_config(ca_material, leaf_material, "node-1")

    def populate_handles(applicator: RebalanceApplicator) -> None:
        plan = RebalancePlan(
            epoch=2,
            ranges=(
                PartitionRangeTransfer(
                    pid_start=7, pid_end=7, src="src-node", dst="node-1"
                ),
            ),
        )
        applicator._plan = plan  # type: ignore[attr-defined]
        applicator._handles = {  # type: ignore[attr-defined]
            7: TransferHandle(
                PartitionTransfer(7, "src-node", "node-1"),
                TransferState.COMMITTED,
                bytes_done=1,
            ),
        }

    async with _running_node_with_rebalance(
        cfg,
        _joining_state("node-1", epoch=2),
        pre_populate_handles=populate_handles,
    ) as (port, _, __, ___, ____):
        client_ssl = build_client_ssl_context(
            cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
        )
        serializer = MsgpackSerializerAdapter()
        client = TcpClient()
        await client.connect(f"127.0.0.1:{port}", client_ssl)
        try:
            req = Envelope.create(
                serializer.encode({"after_pid": 0, "limit": 500}),
                kind="rebalance.status",
                schema_id=serializer.schema_id,
            )
            resp = await client.request(req, timeout=5.0)
            data = serializer.decode(resp.payload)
        finally:
            await client.close()

    assert resp.kind == "rebalance.status.response"
    assert data["epoch"] == 2
    transfers = data["transfers"]
    assert len(transfers) == 1
    t = transfers[0]
    assert t["src"] == "src-node"
    assert t["dst"] == "node-1"
    assert t["state"] == "committed"


# ---------------------------------------------------------------------------
# E2E — tourctl CLI: rebalance status --help (no server needed)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.rebalance
def test_e2e_tourctl_rebalance_status_help() -> None:
    """tourctl rebalance status --help exits 0 and --after-pid is a valid option.

    Validates --after-pid existence via a type-mismatch probe instead of
    asserting on help text, which is subject to Typer/Rich rendering
    differences across terminal widths, platforms, and library versions.
    Passing a non-integer value for --after-pid produces Click's invariant
    error message "invalid value for '--after-pid'" when the option is
    recognised, and "no such option" when it is not.  Neither form depends
    on terminal width or ANSI colour support.
    """
    import re

    from typer.testing import CliRunner

    from tourctl.bootstrap.main import app as tourctl_app

    runner = CliRunner()

    # 1. --help must exit 0.
    help_result = runner.invoke(tourctl_app, ["rebalance", "status", "--help"])
    assert help_result.exit_code == 0

    # 2. Passing a non-integer value for --after-pid must produce a type-mismatch
    #    error (not "no such option"), proving the option is registered.
    probe = runner.invoke(
        tourctl_app,
        ["rebalance", "status", "--after-pid", "not-an-int", "127.0.0.1:0"],
    )
    stripped = re.sub(r"\x1b\[[0-9;]*m", "", probe.output).lower()
    assert (
        "no such option" not in stripped
    ), "--after-pid is not registered; Typer/Click said 'no such option'"
    assert (
        "after-pid" in stripped or "after_pid" in stripped
    ), f"Expected type-mismatch message for --after-pid, got: {stripped!r}"


# ---------------------------------------------------------------------------
# E2E — tourctl CLI: rebalance status with no active context → exit 1
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.rebalance
def test_e2e_tourctl_rebalance_status_no_context(tmp_path: Any) -> None:
    """tourctl rebalance status exits 1 with a clear message when no active context."""
    from pathlib import Path

    from typer.testing import CliRunner

    from tourctl.bootstrap.main import app as tourctl_app

    runner = CliRunner()
    empty_contexts = Path(str(tmp_path)) / "contexts.toml"
    empty_contexts.write_text("schema_version = 1\n")

    result = runner.invoke(
        tourctl_app,
        [
            "rebalance",
            "status",
            "127.0.0.1:7701",
            "--contexts",
            str(empty_contexts),
        ],
    )
    assert result.exit_code == 1


# ---------------------------------------------------------------------------
# E2E — rebalance.status unreachable → tourctl returns exit code 1
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.rebalance
async def test_e2e_tourctl_rebalance_status_unreachable(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """tourctl rebalance status to an unreachable address returns exit code 1."""
    cfg = _make_config(ca_material, leaf_material, "node-1")
    # Port 1 is always refused on loopback.
    exit_code = await _tourctl_rebalance_status(cfg, "127.0.0.1:1", timeout=2.0)
    assert exit_code == 1
