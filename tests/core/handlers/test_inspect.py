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
"""Test suite for NodeInspectHandler (direct self-inspect only).

Scenarios follow proposal-node-inspect-05102026-003.md revision 2 - no
forwarding, no peer_view, no contact-node proxy. All tests use in-memory
adapters; no real sockets or disk I/O occurs.
"""

from __future__ import annotations

import json
import uuid
from io import StringIO
from typing import Any

import pytest
from rich.console import Console

from tourctl.core.commands.inspect import (
    PARTITION_DISPLAY_THRESHOLD,
    InspectCommand,
    _short_token,
)
from tourillon.core.handlers.inspect import (
    INSPECT_MEMBER_LIMIT,
    NodeInspectHandler,
    _partition_ranges_for_tokens,
    _truncate_to_budget,
)
from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.lifecycle.probe import ProbeManager
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ports.transport import (
    MAX_PAYLOAD_DEFAULT,
    ResponseTimeoutError,
)
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.ring.vnode import VNode
from tourillon.core.structure.envelope import Envelope
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter

pytestmark = pytest.mark.inspect

SERIALIZER = MsgpackSerializerAdapter()
HS = HashSpace(bits=128)
SHIFT = 10
PARTITIONER = Partitioner(HS, SHIFT)
TOTAL_PARTITIONS = PARTITIONER.total_partitions


def _make_state(
    phase: MemberPhase = MemberPhase.READY,
    generation: int = 1,
    seq: int = 0,
    tokens: tuple[int, ...] = (),
    epoch: int = 1,
    node_id: str = "node-1",
) -> NodeState:
    return NodeState(
        node_id=node_id,
        phase=phase,
        generation=generation,
        seq=seq,
        tokens=tokens,
        epoch=epoch,
    )


def _make_member(
    node_id: str,
    phase: MemberPhase = MemberPhase.READY,
    tokens: tuple[int, ...] = (),
    seq: int = 0,
) -> Member:
    return Member(
        node_id=node_id,
        peer_address=f"{node_id}:7701",
        generation=1,
        seq=seq,
        phase=phase,
        tokens=tokens,
        partition_shift=10,
    )


def _make_tokens(n: int) -> tuple[int, ...]:
    step = HS.max // n
    return tuple(step * i % HS.max for i in range(1, n + 1))


async def _make_topology(members: list[Member]) -> TopologyManager:
    tm = TopologyManager()
    for m in members:
        await tm.apply_member(m)
    return tm


async def _call_handler(
    handler: NodeInspectHandler,
    payload: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any] | None]:
    body = SERIALIZER.encode(payload if payload is not None else {})
    cid = uuid.uuid4()
    req = Envelope(kind="node.inspect", payload=body, correlation_id=cid)
    responses: list[Envelope] = []

    async def receive() -> Envelope:
        return req

    async def send(env: Envelope) -> None:
        responses.append(env)

    await handler(receive, send)
    if not responses:
        return "", None
    resp = responses[0]
    assert resp.correlation_id == cid
    if resp.payload:
        return resp.kind, SERIALIZER.decode(resp.payload)
    return resp.kind, None


def _make_handler(
    node_id: str = "node-1",
    state: NodeState | None = None,
    topology_manager: TopologyManager | None = None,
    probe_manager: ProbeManager | None = None,
    partitioner: Partitioner = PARTITIONER,
    get_gossip_stats: Any = None,
) -> NodeInspectHandler:
    if state is None:
        state = _make_state(tokens=_make_tokens(4), node_id=node_id)
    if topology_manager is None:
        topology_manager = TopologyManager()
    if probe_manager is None:
        probe_manager = ProbeManager()
    return NodeInspectHandler(
        node_id=node_id,
        get_state=lambda: state,
        topology_manager=topology_manager,
        probe_manager=probe_manager,
        partitioner=partitioner,
        peer_address=f"{node_id}:7701",
        kv_address=f"{node_id}:7700",
        size="M",
        serializer=SERIALIZER,
        get_gossip_stats=get_gossip_stats,
    )


async def test_1_self_inspect_ready_node_owned_partitions_and_ranges() -> None:
    """node_id='node-1', owned_partitions=1024, len(partition_ranges)==4."""
    tokens = _make_tokens(4)
    state = _make_state(tokens=tokens)
    member = _make_member("node-1", tokens=tokens)
    tm = await _make_topology([member])
    handler = _make_handler(node_id="node-1", state=state, topology_manager=tm)

    kind, data = await _call_handler(handler)

    assert kind == "node.inspect.response"
    assert data is not None
    assert data["node_id"] == "node-1"
    assert data["owned_partitions"] == TOTAL_PARTITIONS
    assert len(data["partition_ranges"]) == 4


async def test_2_direct_inspect_returns_self_node_id() -> None:
    """node.inspect.response with node_id='node-2'; no intermediary involved."""
    tokens = _make_tokens(4)
    state = _make_state(tokens=tokens, node_id="node-2")
    member = _make_member("node-2", tokens=tokens)
    tm = await _make_topology([member])
    handler = _make_handler(node_id="node-2", state=state, topology_manager=tm)

    kind, data = await _call_handler(handler)

    assert kind == "node.inspect.response"
    assert data is not None
    assert data["node_id"] == "node-2"


async def test_5_idle_node_returns_minimal_response() -> None:
    """Returns node.inspect.response with phase='idle' and empty fields."""
    state = _make_state(phase=MemberPhase.IDLE, tokens=())
    handler = _make_handler(state=state, topology_manager=TopologyManager())

    kind, data = await _call_handler(handler)

    assert kind == "node.inspect.response"
    assert data is not None
    assert data["phase"] == "idle"
    assert data["tokens"] == []
    assert data["partition_ranges"] == []
    assert data["members"] == []
    assert data["probe_states"] == []


async def test_6_serialisation_round_trip_preserves_all_fields() -> None:
    """All fields preserved exactly including nested partition_ranges."""
    tokens = _make_tokens(4)
    state = _make_state(tokens=tokens)
    member = _make_member("node-1", tokens=tokens)
    tm = await _make_topology([member])
    handler = _make_handler(state=state, topology_manager=tm)

    kind, data = await _call_handler(handler)

    assert kind == "node.inspect.response"
    assert data is not None
    assert data["total_partitions"] == TOTAL_PARTITIONS
    assert data["partition_shift"] == SHIFT
    assert isinstance(data["partition_ranges"], list)
    assert all(
        {"start_pid", "end_pid", "count", "token_hex"} <= set(r.keys())
        for r in data["partition_ranges"]
    )
    assert data["members_truncated"] is False
    assert data["probe_states_truncated"] is False
    assert "forwarded_by" not in data


async def test_8_partition_ranges_8_vnodes_shift4_correct_count() -> None:
    """Returns 8 entries; sum(count)==16; [start_pid,end_pid] non-overlapping."""
    part = Partitioner(HS, partition_shift=4)
    total = part.total_partitions
    tokens = _make_tokens(8)
    ring = Ring([VNode("node-1", t) for t in tokens])

    ranges = _partition_ranges_for_tokens(tokens, ring, part)

    assert len(ranges) == 8
    assert sum(r["count"] for r in ranges) == total

    all_pids: set[int] = set()
    for r in ranges:
        s, e = r["start_pid"], r["end_pid"]
        if s <= e:
            pids = set(range(s, e + 1))
        else:
            pids = set(range(s, total)) | set(range(0, e + 1))
        assert not (all_pids & pids), "overlap"
        all_pids |= pids
    assert all_pids == set(range(total))


def test_8b_partition_ranges_non_aligned_tokens_exact_total() -> None:
    """sum(count)==total; ownership matches Ring.successor for every pid."""
    part = Partitioner(HS, partition_shift=4)
    total = part.total_partitions
    step = HS.max // 16
    tokens: tuple[int, ...] = (
        step // 3,
        step + step // 3,
        3 * step + step // 2,
        7 * step + step // 7,
    )
    ring = Ring([VNode("node-1", t) for t in tokens])

    ranges = _partition_ranges_for_tokens(tokens, ring, part)
    assert sum(r["count"] for r in ranges) == total

    canonical: dict[int, int] = {}
    for pid in range(total):
        pid_start = pid * (HS.max >> 4)
        canonical[pid] = ring.successor(pid_start).token

    for r in ranges:
        token_val = int(r["token_hex"], 16)
        s, e = r["start_pid"], r["end_pid"]
        if s <= e:
            pids_in_range = range(s, e + 1)
        else:
            pids_in_range = list(range(s, total)) + list(range(0, e + 1))
        for pid in pids_in_range:
            assert canonical[pid] == token_val


def _build_resp(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "node_id": "node-1",
        "phase": "ready",
        "peer_address": "node-1:7701",
        "kv_address": "node-1:7700",
        "size": "M",
        "generation": 1,
        "seq": 0,
        "epoch": 1,
        "tokens": [],
        "total_partitions": TOTAL_PARTITIONS,
        "partition_shift": SHIFT,
        "owned_partitions": 0,
        "partition_ranges": [],
        "members": [],
        "members_truncated": False,
        "members_total": 0,
        "probe_states": [],
        "probe_states_truncated": False,
    }
    base.update(overrides)
    return base


class _StaticClient:
    def __init__(self, env: Envelope) -> None:
        self._env = env

    async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
        return Envelope(
            kind=self._env.kind,
            payload=self._env.payload,
            correlation_id=env.correlation_id,
        )

    async def close(self) -> None:
        pass

    @property
    def is_connected(self) -> bool:
        return True


def _make_inspect_cmd(
    env: Envelope, target_address: str = "node-1:7701"
) -> tuple[InspectCommand, StringIO, StringIO]:
    buf = StringIO()
    err_buf = StringIO()
    cmd = InspectCommand(
        client=_StaticClient(env),  # type: ignore[arg-type]
        serializer=SERIALIZER,
        console=Console(file=buf, highlight=False, force_terminal=False),
        err_console=Console(file=err_buf, stderr=True, highlight=False),
        target_address=target_address,
    )
    return cmd, buf, err_buf


async def test_9_cli_collapses_ranges_above_threshold_without_flag() -> None:
    """CLI prints summary; no individual range lines when above threshold."""
    many = [
        {"start_pid": i, "end_pid": i, "count": 1, "token_hex": f"0x{i:032x}"}
        for i in range(PARTITION_DISPLAY_THRESHOLD + 1)
    ]
    resp = _build_resp(partition_ranges=many, owned_partitions=len(many))
    env = Envelope(kind="node.inspect.response", payload=SERIALIZER.encode(resp))
    cmd, buf, _ = _make_inspect_cmd(env)
    exit_code = await cmd.run(show_all_partitions=False, json_output=False, timeout=5.0)

    assert exit_code == 0
    out = buf.getvalue()
    assert "--partitions" in out
    assert "→  pids" not in out


async def test_10_cli_shows_all_ranges_with_partitions_flag() -> None:
    """CLI prints all individual range lines when show_all_partitions=True."""
    many = [
        {"start_pid": i, "end_pid": i, "count": 1, "token_hex": f"0x{i:032x}"}
        for i in range(5)
    ]
    resp = _build_resp(partition_ranges=many, owned_partitions=5)
    env = Envelope(kind="node.inspect.response", payload=SERIALIZER.encode(resp))
    cmd, buf, _ = _make_inspect_cmd(env)
    exit_code = await cmd.run(show_all_partitions=True, json_output=False, timeout=5.0)

    assert exit_code == 0
    assert "→  pids" in buf.getvalue()


async def test_11_json_output_emits_valid_json() -> None:
    """stdout contains valid JSON; members_truncated=false; numeric fields present."""
    resp = _build_resp(owned_partitions=TOTAL_PARTITIONS, tokens=list(_make_tokens(4)))
    env = Envelope(kind="node.inspect.response", payload=SERIALIZER.encode(resp))
    cmd, buf, _ = _make_inspect_cmd(env)
    exit_code = await cmd.run(json_output=True, timeout=5.0)

    assert exit_code == 0
    parsed = json.loads(buf.getvalue())
    assert parsed["members_truncated"] is False
    assert parsed["probe_states_truncated"] is False
    assert "total_partitions" in parsed
    assert "partition_shift" in parsed


async def test_12_members_truncated_at_inspect_member_limit() -> None:
    """members contains exactly INSPECT_MEMBER_LIMIT; members_truncated=True."""
    members = [_make_member(f"node-{i:06d}") for i in range(INSPECT_MEMBER_LIMIT + 1)]
    local = members[0]
    tm = await _make_topology(members)
    state = _make_state(tokens=local.tokens, node_id=local.node_id)

    handler = _make_handler(node_id=local.node_id, state=state, topology_manager=tm)
    kind, data = await _call_handler(handler)

    assert kind == "node.inspect.response"
    assert data is not None
    assert len(data["members"]) == INSPECT_MEMBER_LIMIT
    assert data["members_truncated"] is True
    assert data["members_total"] == INSPECT_MEMBER_LIMIT + 1


async def test_13_probe_states_truncated_at_inspect_member_limit() -> None:
    """probe_states truncated to INSPECT_MEMBER_LIMIT; truncated=True."""
    pm = ProbeManager()
    for i in range(INSPECT_MEMBER_LIMIT + 1):
        await pm.record_heartbeat(f"peer-{i:06d}")

    tm = await _make_topology([_make_member("node-1")])
    state = _make_state(tokens=())
    handler = _make_handler(state=state, topology_manager=tm, probe_manager=pm)

    kind, data = await _call_handler(handler)
    assert kind == "node.inspect.response"
    assert data is not None
    assert len(data["probe_states"]) == INSPECT_MEMBER_LIMIT
    assert data["probe_states_truncated"] is True


async def test_14_cli_warns_when_members_truncated() -> None:
    """CLI prints membership-truncated warning line."""
    resp = _build_resp(
        members=[
            {
                "node_id": f"n-{i}",
                "phase": "ready",
                "generation": 1,
                "seq": 0,
                "peer_address": f"n-{i}:7701",
            }
            for i in range(10)
        ],
        members_truncated=True,
        members_total=31427,
    )
    env = Envelope(kind="node.inspect.response", payload=SERIALIZER.encode(resp))
    cmd, buf, _ = _make_inspect_cmd(env)
    exit_code = await cmd.run(json_output=False, timeout=5.0)

    assert exit_code == 0
    assert "truncated" in buf.getvalue().lower()
    assert "31427" in buf.getvalue()


def test_15_max_realistic_payload_fits_within_budget() -> None:
    """Serialized response with INSPECT_MEMBER_LIMIT members <= MAX_PAYLOAD_DEFAULT."""
    members_list = [
        {
            "node_id": f"very-long-node-identifier-{i:06d}",
            "phase": "ready",
            "generation": 1,
            "seq": i,
            "peer_address": f"peer-{i:06d}.prod.verylongdomain.example.com:7701",
        }
        for i in range(INSPECT_MEMBER_LIMIT)
    ]
    resp = _build_resp(members=members_list, members_total=INSPECT_MEMBER_LIMIT)
    encoded = SERIALIZER.encode(resp)
    assert len(encoded) <= MAX_PAYLOAD_DEFAULT


async def test_16_step6_fallback_clears_probe_states_when_oversized() -> None:
    """Handler clears probe_states; final payload <= MAX_PAYLOAD_DEFAULT."""
    long_str = "x" * 400
    big_member = {
        "node_id": long_str,
        "phase": "ready",
        "generation": 1,
        "seq": 0,
        "peer_address": long_str,
    }
    big_probe = {"node_id": long_str, "state": "live", "phi": 0.0}
    n = INSPECT_MEMBER_LIMIT
    resp = _build_resp(
        members=[big_member] * n,
        members_total=n,
        probe_states=[big_probe] * n,
    )

    result = _truncate_to_budget(resp, SERIALIZER)
    encoded = SERIALIZER.encode(result)
    assert len(encoded) <= MAX_PAYLOAD_DEFAULT
    assert result["probe_states_truncated"] is True


def test_s1_structure_dataclasses_are_constructable() -> None:
    """PartitionRange, MemberSummary, ProbeSummary, NodeInspectResponse construct."""
    from tourillon.core.structure.inspect import (
        MemberSummary,
        NodeInspectResponse,
        PartitionRange,
        ProbeSummary,
    )

    pr = PartitionRange(start_pid=0, end_pid=63, count=64, token_hex="0x001a3cb4f200")
    assert pr.token_hex == "0x001a3cb4f200"

    ms = MemberSummary(
        node_id="n1", phase="ready", generation=1, seq=5, peer_address="n1:7701"
    )
    assert ms.phase == "ready"

    ps = ProbeSummary(node_id="n2", state="live", phi=1.23)
    assert ps.phi == 1.23

    resp = NodeInspectResponse(
        node_id="n1",
        phase="ready",
        peer_address="n1:7701",
        kv_address="n1:7700",
        size="M",
        generation=1,
        seq=0,
        epoch=1,
        tokens=(1, 2, 3, 4),
        total_partitions=1024,
        partition_shift=10,
        owned_partitions=1024,
        partition_ranges=(pr,),
        members=(ms,),
        members_truncated=False,
        members_total=1,
        probe_states=(ps,),
        probe_states_truncated=False,
    )
    assert len(resp.partition_ranges) == 1


class _TimeoutClient:
    async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
        raise ResponseTimeoutError("timeout")

    async def close(self) -> None:
        pass

    @property
    def is_connected(self) -> bool:
        return True


async def test_s2_inspect_command_timeout_returns_exit_1() -> None:
    """InspectCommand returns exit code 1 on ResponseTimeoutError."""
    err_buf = StringIO()
    cmd = InspectCommand(
        client=_TimeoutClient(),  # type: ignore[arg-type]
        serializer=SERIALIZER,
        console=Console(file=StringIO()),
        err_console=Console(file=err_buf, stderr=True, highlight=False),
        target_address="node-2:7701",
    )
    exit_code = await cmd.run(timeout=5.0)
    assert exit_code == 1
    assert "timed out" in err_buf.getvalue().lower()


async def test_s3_inspect_command_node_unreachable_error() -> None:
    """InspectCommand prints unreachable message on error.node_unreachable."""
    env = Envelope(kind="error.node_unreachable", payload=b"")
    cmd, _, err_buf = _make_inspect_cmd(env, target_address="node-2:7701")
    exit_code = await cmd.run(timeout=5.0)
    assert exit_code == 1
    assert "unreachable" in err_buf.getvalue().lower()


async def test_s4_inspect_command_generic_error_kind() -> None:
    """InspectCommand shows raw error kind for unknown error responses."""
    env = Envelope(kind="error.some_unknown_error", payload=b"")
    cmd, _, err_buf = _make_inspect_cmd(env)
    exit_code = await cmd.run(timeout=5.0)
    assert exit_code == 1
    assert "error.some_unknown_error" in err_buf.getvalue()


async def test_s5_inspect_command_bad_payload_returns_1() -> None:
    """InspectCommand returns exit code 1 when response payload cannot decode."""
    env = Envelope(kind="node.inspect.response", payload=b"\xff\xfe\xfd")
    cmd, _, _ = _make_inspect_cmd(env)
    exit_code = await cmd.run(timeout=5.0)
    assert exit_code == 1


async def test_s6_inspect_handler_malformed_payload_closes_connection() -> None:
    """Malformed node.inspect payload logs a warning and sends no response."""
    handler = _make_handler()
    req = Envelope(
        kind="node.inspect",
        payload=b"\xff\xfe",
        correlation_id=uuid.uuid4(),
    )
    responses: list[Envelope] = []

    async def receive() -> Envelope:
        return req

    async def send(env: Envelope) -> None:
        responses.append(env)

    await handler(receive, send)
    assert len(responses) == 0


async def test_s7_truncate_budget_probe_cleared_fits() -> None:
    """_truncate_to_budget returns after clearing probe_states when enough."""
    member_entry = {
        "node_id": "n1",
        "phase": "ready",
        "generation": 1,
        "seq": 0,
        "peer_address": "n1:7701",
    }
    members = [member_entry] * 3000
    probe_entry = {
        "node_id": "p",
        "state": "live",
        "phi": 1.0,
        "padding": "x" * 1500,
    }
    probe_states = [probe_entry] * 3000

    resp = _build_resp(members=members, probe_states=probe_states, members_total=3000)
    encoded = SERIALIZER.encode(resp)
    assert len(encoded) > MAX_PAYLOAD_DEFAULT

    result = _truncate_to_budget(resp, SERIALIZER)
    assert result["probe_states"] == []
    assert result["probe_states_truncated"] is True
    assert result["members_truncated"] is False


def test_s8_short_token_edge_cases() -> None:
    """_short_token returns expected values for various inputs."""
    full = "0x" + "a" * 32
    assert _short_token(full) == "0xaaaaaaaa…"
    assert _short_token("0x1234") == "0x1234"
    assert _short_token("1234567890abcdef") == "1234567890abcdef"


async def test_s9_empty_payload_is_self_inspect() -> None:
    """A request with empty bytes payload is handled as self-inspect."""
    tokens = _make_tokens(4)
    state = _make_state(tokens=tokens)
    tm = await _make_topology([_make_member("node-1", tokens=tokens)])
    handler = _make_handler(state=state, topology_manager=tm)

    cid = uuid.uuid4()
    req = Envelope(kind="node.inspect", payload=b"", correlation_id=cid)
    responses: list[Envelope] = []

    async def receive() -> Envelope:
        return req

    async def send(env: Envelope) -> None:
        responses.append(env)

    await handler(receive, send)
    assert len(responses) == 1
    assert responses[0].kind == "node.inspect.response"


async def test_s10_gossip_stats_included_when_provided() -> None:
    """gossip_stats from the supplied callback is included in the response."""
    stats = {"known_members": 3, "push_sent_total": 7}
    tm = await _make_topology([_make_member("node-1")])
    handler = _make_handler(topology_manager=tm, get_gossip_stats=lambda: stats)

    _, data = await _call_handler(handler)
    assert data is not None
    assert data["gossip_stats"] == stats
