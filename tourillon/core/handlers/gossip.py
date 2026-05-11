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
"""Peer-server connection handlers for all gossip envelope kinds.

Handlers in this module implement the responder side of the gossip protocol:
- GossipPushHandler   : receives gossip.push; validates; merges; responds gossip.push.ok
- GossipPingHandler   : receives gossip.ping; builds gossip.pong
- GossipDigestHandler : receives gossip.digest; responds with gossip.delta pages

All handlers live in this module so that a single register(dispatcher) call
wires the full gossip responder side.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any

from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.envelope import Envelope

if TYPE_CHECKING:
    from tourillon.core.ports.serializer import SerializerPort
    from tourillon.core.ports.transport import ReceiveEnvelope, SendEnvelope

logger = logging.getLogger(__name__)


def _send_gossip_error(
    kind: str,
    code: str,
    message: str,
    sender: str,
    correlation_id: uuid.UUID,
    serializer: SerializerPort,
) -> Envelope:
    """Build a gossip.error envelope."""
    payload = serializer.encode(
        {
            "sender": sender,
            "rejected_kind": kind,
            "code": code,
            "message": message,
        }
    )
    return Envelope.create(
        payload,
        kind="gossip.error",
        correlation_id=correlation_id,
        schema_id=serializer.schema_id,
    )


def _member_from_dict(d: dict[str, Any]) -> Member:
    """Deserialise a Member from a wire dict; raise ValueError on bad fields."""
    gen = int(d["generation"])
    seq = int(d["seq"])
    if gen < 0 or seq < 0:
        raise ValueError(f"invalid member fields: generation={gen} seq={seq}")
    return Member(
        node_id=d["node_id"],
        peer_address=d.get("peer_address", ""),
        generation=gen,
        seq=seq,
        phase=MemberPhase(d["phase"]),
        tokens=tuple(d.get("tokens", [])),
        partition_shift=int(d["partition_shift"]),
    )


class GossipPushHandler:
    """Responder for gossip.push.

    Validates members (generation/seq >= 0, partition_shift). Merges valid
    members into TopologyManager. Responds gossip.push.ok with accepted count.
    Responds gossip.error and closes on any validation failure.
    """

    def __init__(
        self,
        node_id: str,
        topology_manager: TopologyManager,
        partition_shift: int,
        serializer: SerializerPort,
    ) -> None:
        self._node_id = node_id
        self._topology_manager = topology_manager
        self._partition_shift = partition_shift
        self._serializer = serializer

    async def __call__(
        self,
        receive: ReceiveEnvelope,
        send: SendEnvelope,
    ) -> None:
        """Handle one gossip.push request."""
        req = await receive()
        try:
            data = self._serializer.decode(req.payload)
        except Exception:
            logger.warning("gossip.push: malformed payload")
            return

        members_raw: list[dict[str, Any]] = data.get("members", [])
        if len(members_raw) > 4096:
            err = _send_gossip_error(
                "gossip.push",
                "payload_too_large",
                f"members count {len(members_raw)} exceeds limit 4096",
                self._node_id,
                req.correlation_id,
                self._serializer,
            )
            await send(err)
            return

        accepted = 0
        ignored = 0
        for raw in members_raw:
            try:
                member = _member_from_dict(raw)
            except (ValueError, KeyError) as exc:
                err = _send_gossip_error(
                    "gossip.push",
                    "invalid_member",
                    str(exc),
                    self._node_id,
                    req.correlation_id,
                    self._serializer,
                )
                await send(err)
                return

            if member.partition_shift != self._partition_shift:
                err = _send_gossip_error(
                    "gossip.push",
                    "partition_shift_mismatch",
                    f"member {member.node_id!r} has partition_shift="
                    f"{member.partition_shift}; local={self._partition_shift}",
                    self._node_id,
                    req.correlation_id,
                    self._serializer,
                )
                await send(err)
                return

            if await self._topology_manager.apply_member(member):
                accepted += 1
            else:
                ignored += 1

        ok_payload = self._serializer.encode(
            {"sender": self._node_id, "accepted": accepted, "ignored": ignored}
        )
        ok_env = Envelope.create(
            ok_payload,
            kind="gossip.push.ok",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(ok_env)


class GossipPingHandler:
    """Responder for gossip.ping.

    Compares epoch, fingerprint, and member_count. Responds gossip.pong
    with same=True when all three match; same=False otherwise.
    """

    def __init__(
        self,
        node_id: str,
        topology_manager: TopologyManager,
        serializer: SerializerPort,
    ) -> None:
        self._node_id = node_id
        self._topology_manager = topology_manager
        self._serializer = serializer

    async def __call__(
        self,
        receive: ReceiveEnvelope,
        send: SendEnvelope,
    ) -> None:
        """Handle one gossip.ping request."""
        req = await receive()
        try:
            data = self._serializer.decode(req.payload)
        except Exception:
            logger.warning("gossip.ping: malformed payload")
            return

        topo = await self._topology_manager.snapshot()
        fingerprint = await self._topology_manager.member_fingerprint()
        member_count = len(list(topo.registry))

        same = (
            data.get("epoch") == topo.epoch
            and data.get("fingerprint") == fingerprint
            and data.get("member_count") == member_count
        )

        logger.debug(
            "gossip.pong → %s: same=%s epoch=%d members=%d fp=%.16s…",
            data.get("sender", "?"),
            same,
            topo.epoch,
            member_count,
            fingerprint,
        )

        pong_payload = self._serializer.encode(
            {
                "sender": self._node_id,
                "epoch": topo.epoch,
                "fingerprint": fingerprint,
                "member_count": member_count,
                "same": same,
            }
        )
        pong_env = Envelope.create(
            pong_payload,
            kind="gossip.pong",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(pong_env)


class GossipDigestHandler:
    """Responder for gossip.digest.

    Reads the version vector from the request and sends back gossip.delta
    pages containing Members where the local registry is ahead. An empty
    digest list (bootstrap case) triggers a full delta response.
    """

    def __init__(
        self,
        node_id: str,
        topology_manager: TopologyManager,
        max_digest_entries: int,
        serializer: SerializerPort,
    ) -> None:
        self._node_id = node_id
        self._topology_manager = topology_manager
        self._max_digest_entries = max_digest_entries
        self._serializer = serializer

    async def __call__(
        self,
        receive: ReceiveEnvelope,
        send: SendEnvelope,
    ) -> None:
        """Handle one gossip.digest request; stream gossip.delta pages."""
        req = await receive()
        try:
            data = self._serializer.decode(req.payload)
        except Exception:
            logger.warning("gossip.digest: malformed payload")
            return

        digest_entries: list[dict[str, Any]] = data.get("members", [])
        if len(digest_entries) > self._max_digest_entries:
            err = _send_gossip_error(
                "gossip.digest",
                "payload_too_large",
                f"digest count {len(digest_entries)} exceeds limit {self._max_digest_entries}",
                self._node_id,
                req.correlation_id,
                self._serializer,
            )
            await send(err)
            return

        # Build version-vector lookup from digest
        peer_versions: dict[str, tuple[int, int]] = {}
        for entry in digest_entries:
            peer_versions[entry["node_id"]] = (
                int(entry["generation"]),
                int(entry["seq"]),
            )

        topo = await self._topology_manager.snapshot()
        ahead: list[dict[str, Any]] = []
        for member in sorted(topo.registry, key=lambda m: m.node_id):
            peer_ver = peer_versions.get(member.node_id)
            if peer_ver is None or (member.generation, member.seq) > peer_ver:
                ahead.append(_member_to_dict(member))

        # Send a single delta page (pagination left for future extension).
        delta_payload = self._serializer.encode(
            {
                "sender": self._node_id,
                "has_more": False,
                "members": ahead,
            }
        )
        delta_env = Envelope.create(
            delta_payload,
            kind="gossip.delta",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(delta_env)


def _member_to_dict(m: Member) -> dict[str, Any]:
    """Serialise a Member to a wire-compatible dict."""
    return {
        "node_id": m.node_id,
        "peer_address": m.peer_address,
        "phase": str(m.phase),
        "generation": m.generation,
        "seq": m.seq,
        "tokens": list(m.tokens),
        "partition_shift": m.partition_shift,
    }


def register_gossip_handlers(
    dispatcher: object,
    node_id: str,
    topology_manager: TopologyManager,
    partition_shift: int,
    max_digest_entries: int,
    serializer: SerializerPort,
) -> None:
    """Register all gossip responder handlers on *dispatcher*."""
    push = GossipPushHandler(node_id, topology_manager, partition_shift, serializer)
    ping = GossipPingHandler(node_id, topology_manager, serializer)
    digest = GossipDigestHandler(
        node_id, topology_manager, max_digest_entries, serializer
    )
    dispatcher.register("gossip.push", push)  # type: ignore[union-attr]
    dispatcher.register("gossip.ping", ping)  # type: ignore[union-attr]
    dispatcher.register("gossip.digest", digest)  # type: ignore[union-attr]
