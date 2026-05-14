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
"""ConnectionHandler for the node.inspect envelope.

The handler is registered on the peer Dispatcher only. tourctl connects
directly to the target's peer address, so the handler always builds the
response from its own in-memory state - no forwarding occurs.
"""

from __future__ import annotations

import logging
import uuid
from bisect import bisect_left
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from tourillon.core.lifecycle.probe import ProbeManager
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ports.transport import MAX_PAYLOAD_DEFAULT
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.envelope import Envelope

if TYPE_CHECKING:
    from tourillon.core.ports.serializer import SerializerPort
    from tourillon.core.ports.transport import ReceiveEnvelope, SendEnvelope

logger = logging.getLogger(__name__)

INSPECT_MEMBER_LIMIT: int = 10_000

type NodeStateService = Callable[[], NodeState]


def _partition_ranges_for_tokens(
    tokens: tuple[int, ...],
    ring: Ring,
    partitioner: Partitioner,
) -> list[dict[str, Any]]:
    """Compute partition ranges for this node's own vnode tokens."""
    all_ring_tokens = [v.token for v in ring]
    n = len(all_ring_tokens)
    if n == 0:
        return []

    max_pid = partitioner.total_partitions - 1
    ranges: list[dict[str, Any]] = []

    for t in sorted(tokens):
        idx = bisect_left(all_ring_tokens, t)
        pred_idx = (idx - 1) % n
        pred_token = all_ring_tokens[pred_idx]
        wraps = idx == 0

        start_pid = partitioner.pid_for_hash(pred_token) + 1
        end_pid = partitioner.pid_for_hash(t)

        if wraps:
            count = (max_pid - start_pid + 1) + (end_pid + 1)
        else:
            count = end_pid - start_pid + 1

        ranges.append(
            {
                "start_pid": start_pid,
                "end_pid": end_pid,
                "count": count,
                "token_hex": f"0x{t:032x}",
            }
        )

    return sorted(ranges, key=lambda r: r["start_pid"])


def _build_member_list(registry: Any) -> list[dict[str, Any]]:
    """Build a sorted-by-node_id list of member summary dicts from *registry*."""
    members = sorted(registry, key=lambda m: m.node_id)
    return [
        {
            "node_id": m.node_id,
            "phase": str(m.phase),
            "generation": m.generation,
            "seq": m.seq,
            "peer_address": m.peer_address,
        }
        for m in members
    ]


def _apply_member_limit(
    member_list: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], bool]:
    """Truncate *member_list* to INSPECT_MEMBER_LIMIT; return (list, truncated)."""
    if len(member_list) > INSPECT_MEMBER_LIMIT:
        return member_list[:INSPECT_MEMBER_LIMIT], True
    return member_list, False


def _truncate_to_budget(
    resp: dict[str, Any],
    serializer: SerializerPort,
) -> dict[str, Any]:
    """Ensure the serialised size of *resp* stays within MAX_PAYLOAD_DEFAULT."""
    encoded = serializer.encode(resp)
    if len(encoded) <= MAX_PAYLOAD_DEFAULT:
        return resp

    resp["probe_states"] = []
    resp["probe_states_truncated"] = True
    encoded = serializer.encode(resp)
    if len(encoded) <= MAX_PAYLOAD_DEFAULT:
        return resp

    lo, hi = 0, len(resp["members"])
    while lo < hi:
        mid = (lo + hi + 1) // 2
        resp["members"] = resp["members"][:mid]
        if len(serializer.encode(resp)) <= MAX_PAYLOAD_DEFAULT:
            lo = mid
        else:
            hi = mid - 1

    resp["members"] = resp["members"][:lo]
    resp["members_truncated"] = True
    return resp


class NodeInspectHandler:
    """ConnectionHandler for node.inspect on the peer endpoint.

    Always builds and returns NodeInspectResponse from this node's own
    in-memory state. No outbound connection is made and no request
    forwarding occurs - tourctl addressed the target directly. An IDLE
    node returns a minimal response (phase/generation/seq/epoch from
    NodeState with empty tokens, ranges, members, probe_states).
    """

    def __init__(  # noqa: PLR0913
        self,
        node_id: str,
        get_state: NodeStateService,
        topology_manager: TopologyManager,
        probe_manager: ProbeManager,
        partitioner: Partitioner,
        peer_address: str,
        kv_address: str,
        size: str,
        serializer: SerializerPort,
        get_gossip_stats: Callable[[], dict[str, Any]] | None = None,
    ) -> None:
        self._node_id = node_id
        self._get_state = get_state
        self._topology_manager = topology_manager
        self._probe_manager = probe_manager
        self._partitioner = partitioner
        self._peer_address = peer_address
        self._kv_address = kv_address
        self._size = size
        self._serializer = serializer
        self._get_gossip_stats = get_gossip_stats

    async def __call__(
        self,
        receive: ReceiveEnvelope,
        send: SendEnvelope,
    ) -> None:
        """Handle one node.inspect request (always self-inspect)."""
        req = await receive()
        if req.payload:
            try:
                self._serializer.decode(req.payload)
            except Exception:
                logger.warning("node.inspect: malformed payload; closing connection")
                return

        resp_dict = await self._build_self_response()
        await self._reply(resp_dict, req.correlation_id, send)

    async def _build_self_response(self) -> dict[str, Any]:
        """Build the full inspect response dict for this node."""
        state = self._get_state()
        topo = await self._topology_manager.snapshot()
        probe_entries = await self._probe_manager.all_states_with_phi()

        my_tokens = tuple(v.token for v in topo.ring if v.node_id == self._node_id)
        partition_ranges = _partition_ranges_for_tokens(
            my_tokens, topo.ring, self._partitioner
        )
        owned = sum(r["count"] for r in partition_ranges)

        members_list = _build_member_list(topo.registry)
        members_total = len(members_list)
        members_list, members_truncated = _apply_member_limit(members_list)

        probe_list = sorted(
            [
                {"node_id": nid, "state": str(state_val), "phi": phi}
                for nid, state_val, phi in probe_entries
            ],
            key=lambda e: e["node_id"],
        )
        probe_list, probe_truncated = _apply_member_limit(probe_list)

        resp: dict[str, Any] = {
            "node_id": self._node_id,
            "phase": str(state.phase),
            "peer_address": self._peer_address,
            "kv_address": self._kv_address,
            "size": self._size,
            "generation": state.generation,
            "seq": state.seq,
            "epoch": state.epoch,
            "tokens": list(my_tokens),
            "total_partitions": self._partitioner.total_partitions,
            "partition_shift": self._partitioner.partition_shift,
            "owned_partitions": owned,
            "partition_ranges": partition_ranges,
            "members": members_list,
            "members_truncated": members_truncated,
            "members_total": members_total,
            "probe_states": probe_list,
            "probe_states_truncated": probe_truncated,
            "gossip_stats": (
                self._get_gossip_stats() if self._get_gossip_stats is not None else {}
            ),
        }
        return _truncate_to_budget(resp, self._serializer)

    async def _reply(
        self,
        resp_dict: dict[str, Any],
        correlation_id: uuid.UUID,
        send: SendEnvelope,
    ) -> None:
        """Encode *resp_dict* and send it as a node.inspect.response envelope."""
        payload = self._serializer.encode(resp_dict)
        env = Envelope.create(
            payload,
            kind="node.inspect.response",
            correlation_id=correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(env)


__all__ = [
    "INSPECT_MEMBER_LIMIT",
    "NodeInspectHandler",
    "NodeStateService",
]
