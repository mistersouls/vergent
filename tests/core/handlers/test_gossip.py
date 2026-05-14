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
"""Tests for peer-server gossip handlers: push, ping, digest."""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from tourillon.core.handlers.gossip import (
    GossipDigestHandler,
    GossipPingHandler,
    GossipPushHandler,
)
from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.envelope import Envelope
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


def _member_dict(m: Member) -> dict[str, Any]:
    return {
        "node_id": m.node_id,
        "peer_address": m.peer_address,
        "phase": str(m.phase),
        "generation": m.generation,
        "seq": m.seq,
        "tokens": list(m.tokens),
        "partition_shift": m.partition_shift,
    }


async def _invoke_handler(
    handler: Any, req_payload: bytes, kind: str
) -> list[Envelope]:
    """Invoke handler and collect all sent envelopes."""
    cid = uuid.uuid4()
    req = Envelope.create(req_payload, kind=kind, correlation_id=cid)
    sent: list[Envelope] = []

    async def receive() -> Envelope:
        return req

    async def send(env: Envelope) -> None:
        sent.append(env)

    await handler(receive, send)
    return sent


@pytest.mark.gossip
async def test_27_invalid_member_generation_negative_responds_error() -> None:
    """Responder sends gossip.error code: invalid_member on generation=-1."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    handler = GossipPushHandler("node-1", topology_mgr, LOCAL_SHIFT, serializer)

    bad_member = {
        "node_id": "node-2",
        "peer_address": "10.0.0.2:7701",
        "phase": "ready",
        "generation": -1,
        "seq": 0,
        "tokens": [1024],
        "partition_shift": 12,
    }
    payload = serializer.encode({"sender": "node-2", "ttl": 3, "members": [bad_member]})
    sent = await _invoke_handler(handler, payload, "gossip.push")

    assert len(sent) == 1
    assert sent[0].kind == "gossip.error"
    data = serializer.decode(sent[0].payload)
    assert data["code"] == "invalid_member"


@pytest.mark.gossip
async def test_42_push_partition_shift_mismatch_responder_sends_error_no_failed() -> (
    None
):
    """node-1 sends gossip.error code: partition_shift_mismatch; node-1 does NOT transition to FAILED."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    handler = GossipPushHandler("node-1", topology_mgr, LOCAL_SHIFT, serializer)

    # node-2 has partition_shift=10 (wrong)
    mismatched_member = {
        "node_id": "node-2",
        "peer_address": "10.0.0.2:7701",
        "phase": "ready",
        "generation": 1,
        "seq": 5,
        "tokens": [1024],
        "partition_shift": 10,
    }
    payload = serializer.encode(
        {"sender": "node-2", "ttl": 3, "members": [mismatched_member]}
    )
    sent = await _invoke_handler(handler, payload, "gossip.push")

    assert len(sent) == 1
    assert sent[0].kind == "gossip.error"
    data = serializer.decode(sent[0].payload)
    assert data["code"] == "partition_shift_mismatch"
    # Verify node-2 was NOT written to local registry
    topo = await topology_mgr.snapshot()
    assert topo.registry.get("node-2") is None


@pytest.mark.gossip
async def test_43_matching_partition_shift_gossip_proceeds_normally() -> None:
    """Two nodes, identical partition_shift=12 — no mismatch detected; gossip proceeds normally."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    handler = GossipPushHandler("node-1", topology_mgr, LOCAL_SHIFT, serializer)

    member = _make_member("node-2", shift=12)
    payload = serializer.encode(
        {"sender": "node-2", "ttl": 3, "members": [_member_dict(member)]}
    )
    sent = await _invoke_handler(handler, payload, "gossip.push")

    assert len(sent) == 1
    assert sent[0].kind == "gossip.push.ok"
    data = serializer.decode(sent[0].payload)
    assert data["accepted"] == 1

    topo = await topology_mgr.snapshot()
    assert topo.registry.get("node-2") is not None


@pytest.mark.gossip
async def test_26_digest_page_too_large_responds_error() -> None:
    """Responder replies gossip.error code: payload_too_large; connection closed."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    handler = GossipDigestHandler(
        "node-1", topology_mgr, max_digest_entries=4, serializer=serializer
    )

    # 5 digest entries — exceeds limit of 4
    entries = [{"node_id": f"node-{i}", "generation": 1, "seq": i} for i in range(5)]
    payload = serializer.encode(
        {"sender": "node-2", "has_more": False, "after_node_id": "", "members": entries}
    )
    sent = await _invoke_handler(handler, payload, "gossip.digest")

    assert len(sent) == 1
    assert sent[0].kind == "gossip.error"
    data = serializer.decode(sent[0].payload)
    assert data["code"] == "payload_too_large"


@pytest.mark.gossip
async def test_21_ping_pong_same_true_when_registries_match() -> None:
    """gossip.pong has same=true; no digest/delta exchange; total bytes < 200."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    member = _make_member("node-1")
    await topology_mgr.apply_member(member)

    handler = GossipPingHandler("node-2", topology_mgr, serializer)

    fp = await topology_mgr.member_fingerprint()
    topo = await topology_mgr.snapshot()

    payload = serializer.encode(
        {
            "sender": "node-1",
            "epoch": topo.epoch,
            "fingerprint": fp,
            "member_count": len(list(topo.registry)),
        }
    )
    sent = await _invoke_handler(handler, payload, "gossip.ping")

    assert len(sent) == 1
    assert sent[0].kind == "gossip.pong"
    data = serializer.decode(sent[0].payload)
    assert data["same"] is True
    # Verify small payload
    assert len(sent[0].payload) < 200


@pytest.mark.gossip
async def test_22_ping_pong_same_false_when_seq_diverged() -> None:
    """gossip.pong has same=false when fingerprints differ (stale seq)."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    member = _make_member("node-1", seq=100)
    await topology_mgr.apply_member(member)

    handler = GossipPingHandler("node-2", topology_mgr, serializer)

    topo = await topology_mgr.snapshot()
    # Send a stale fingerprint (computed from seq=50 — different)
    import hashlib
    import struct

    digest = hashlib.sha256()
    digest.update(b"node-1")
    digest.update(struct.pack(">QQ", 1, 50))  # stale seq
    stale_fp = digest.hexdigest()

    payload = serializer.encode(
        {
            "sender": "node-1",
            "epoch": topo.epoch,
            "fingerprint": stale_fp,
            "member_count": 1,
        }
    )
    sent = await _invoke_handler(handler, payload, "gossip.ping")
    assert sent[0].kind == "gossip.pong"
    data = serializer.decode(sent[0].payload)
    assert data["same"] is False


@pytest.mark.gossip
async def test_23_ping_pong_same_false_epoch_divergence() -> None:
    """gossip.pong has same=false when epoch differs."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    member = _make_member("node-1", seq=5)
    await topology_mgr.apply_member(member)

    handler = GossipPingHandler("node-2", topology_mgr, serializer)

    topo = await topology_mgr.snapshot()
    fp = await topology_mgr.member_fingerprint()

    # Send wrong epoch
    payload = serializer.encode(
        {
            "sender": "node-1",
            "epoch": topo.epoch + 99,
            "fingerprint": fp,
            "member_count": len(list(topo.registry)),
        }
    )
    sent = await _invoke_handler(handler, payload, "gossip.ping")
    data = serializer.decode(sent[0].payload)
    assert data["same"] is False


@pytest.mark.gossip
async def test_digest_handler_returns_delta_with_ahead_members() -> None:
    """Digest handler computes delta containing members where local registry is ahead."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()

    m1 = _make_member("node-1", seq=100)
    m2 = _make_member("node-2", seq=50)
    await topology_mgr.apply_member(m1)
    await topology_mgr.apply_member(m2)

    handler = GossipDigestHandler(
        "node-3", topology_mgr, max_digest_entries=4096, serializer=serializer
    )

    # Peer knows node-1 at seq=80 and node-2 at seq=50 (same)
    entries = [
        {"node_id": "node-1", "generation": 1, "seq": 80},
        {"node_id": "node-2", "generation": 1, "seq": 50},
    ]
    payload = serializer.encode(
        {"sender": "node-1", "has_more": False, "after_node_id": "", "members": entries}
    )
    sent = await _invoke_handler(handler, payload, "gossip.digest")

    assert len(sent) == 1
    assert sent[0].kind == "gossip.delta"
    data = serializer.decode(sent[0].payload)
    member_ids = {m["node_id"] for m in data["members"]}
    assert "node-1" in member_ids  # ahead by seq=100 vs 80
    assert "node-2" not in member_ids  # same seq — not in delta


@pytest.mark.gossip
async def test_push_malformed_payload_returns_no_response() -> None:
    """GossipPushHandler with non-decodable payload logs WARNING and returns without sending."""
    topology_mgr = TopologyManager()
    serializer = MsgpackSerializerAdapter()
    handler = GossipPushHandler("node-1", topology_mgr, LOCAL_SHIFT, serializer)

    sent = await _invoke_handler(handler, b"not msgpack!!!", "gossip.push")
    assert sent == []  # nothing sent; malformed payload silently dropped


@pytest.mark.gossip
async def test_push_payload_too_large_sends_error() -> None:
    """GossipPushHandler with members > 4096 responds gossip.error payload_too_large."""
    topology_mgr = TopologyManager()
    serializer = MsgpackSerializerAdapter()
    handler = GossipPushHandler("node-1", topology_mgr, LOCAL_SHIFT, serializer)

    # Build a payload with 4097 member entries
    many_members = [_member_dict(_make_member(f"node-{i}")) for i in range(4097)]
    payload = serializer.encode(
        {"sender": "node-src", "ttl": 1, "members": many_members}
    )
    sent = await _invoke_handler(handler, payload, "gossip.push")

    assert len(sent) == 1
    assert sent[0].kind == "gossip.error"
    data = serializer.decode(sent[0].payload)
    assert data["code"] == "payload_too_large"


@pytest.mark.gossip
async def test_push_stale_member_increments_ignored_count() -> None:
    """GossipPushHandler with stale (lower seq) member: ignored incremented; accepted=0, ignored=1."""
    topology_mgr = TopologyManager()
    serializer = MsgpackSerializerAdapter()
    handler = GossipPushHandler("node-1", topology_mgr, LOCAL_SHIFT, serializer)

    # Pre-populate topology with a fresh record
    fresh = _make_member("node-2", gen=1, seq=10)
    await topology_mgr.apply_member(fresh)

    # Send a stale record (lower seq) that apply_member will reject
    stale = _make_member("node-2", gen=1, seq=5)  # lower seq → stale
    payload = serializer.encode(
        {"sender": "node-src", "ttl": 1, "members": [_member_dict(stale)]}
    )
    sent = await _invoke_handler(handler, payload, "gossip.push")

    assert len(sent) == 1
    assert sent[0].kind == "gossip.push.ok"
    data = serializer.decode(sent[0].payload)
    assert data["accepted"] == 0
    assert data["ignored"] == 1


@pytest.mark.gossip
async def test_ping_malformed_payload_returns_no_response() -> None:
    """GossipPingHandler with non-decodable payload logs WARNING and returns without sending."""
    topology_mgr = TopologyManager()
    serializer = MsgpackSerializerAdapter()
    handler = GossipPingHandler("node-1", topology_mgr, serializer)

    sent = await _invoke_handler(handler, b"not msgpack!!!", "gossip.ping")
    assert sent == []  # malformed payload silently dropped


@pytest.mark.gossip
async def test_digest_malformed_payload_returns_no_response() -> None:
    """GossipDigestHandler with non-decodable payload logs WARNING and returns without sending."""
    topology_mgr = TopologyManager()
    serializer = MsgpackSerializerAdapter()
    handler = GossipDigestHandler(
        "node-1", topology_mgr, max_digest_entries=4096, serializer=serializer
    )

    sent = await _invoke_handler(handler, b"not msgpack!!!", "gossip.digest")
    assert sent == []  # malformed payload silently dropped


@pytest.mark.gossip
async def test_register_gossip_handlers_registers_all_three() -> None:
    """register_gossip_handlers registers push, ping, and digest handlers."""
    from tourillon.core.handlers.gossip import register_gossip_handlers
    from tourillon.core.transport.dispatcher import Dispatcher

    topology_mgr = TopologyManager()
    serializer = MsgpackSerializerAdapter()
    dispatcher = Dispatcher()

    register_gossip_handlers(
        dispatcher, "node-1", topology_mgr, LOCAL_SHIFT, 4096, serializer
    )

    assert dispatcher._handlers.get("gossip.push") is not None  # type: ignore[attr-defined]
    assert dispatcher._handlers.get("gossip.ping") is not None  # type: ignore[attr-defined]
    assert dispatcher._handlers.get("gossip.digest") is not None  # type: ignore[attr-defined]


@pytest.mark.gossip
async def test_digest_handler_reports_wanted_for_missing_node_ids() -> None:
    """Digest handler advertises wanted=[node_ids] for entries it does not know about."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()

    # Local only knows node-self; peer's digest mentions node-A and node-B.
    await topology_mgr.apply_member(_make_member("node-self", seq=10))

    handler = GossipDigestHandler(
        "node-self", topology_mgr, max_digest_entries=4096, serializer=serializer
    )

    entries = [
        {"node_id": "node-self", "generation": 1, "seq": 10},
        {"node_id": "node-A", "generation": 1, "seq": 5},
        {"node_id": "node-B", "generation": 2, "seq": 0},
    ]
    payload = serializer.encode(
        {"sender": "node-A", "has_more": False, "after_node_id": "", "members": entries}
    )
    sent = await _invoke_handler(handler, payload, "gossip.digest")

    assert len(sent) == 1
    assert sent[0].kind == "gossip.delta"
    data = serializer.decode(sent[0].payload)
    assert data["members"] == []  # nothing local that peer is missing
    assert sorted(data["wanted"]) == ["node-A", "node-B"]


@pytest.mark.gossip
async def test_digest_handler_wanted_empty_when_registries_identical() -> None:
    """Digest handler returns wanted=[] when peer's digest mentions no unknown node_ids."""
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    await topology_mgr.apply_member(_make_member("node-1", seq=10))
    await topology_mgr.apply_member(_make_member("node-2", seq=20))

    handler = GossipDigestHandler(
        "node-1", topology_mgr, max_digest_entries=4096, serializer=serializer
    )
    entries = [
        {"node_id": "node-1", "generation": 1, "seq": 10},
        {"node_id": "node-2", "generation": 1, "seq": 20},
    ]
    payload = serializer.encode(
        {"sender": "node-2", "has_more": False, "after_node_id": "", "members": entries}
    )
    sent = await _invoke_handler(handler, payload, "gossip.digest")

    data = serializer.decode(sent[0].payload)
    assert data["wanted"] == []
