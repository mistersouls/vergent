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
"""Dataclasses for node inspect request/response payloads."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PartitionRange:
    """A contiguous range of partition IDs owned by one vnode.

    start_pid and end_pid are both inclusive. For the vnode with the minimum
    token in the ring (the wrapping arc), start_pid may be greater than
    end_pid; in that case the vnode owns [start_pid, max_pid] ∪ [0, end_pid]
    and count already accounts for both sub-ranges. token_hex is the full
    lowercase hex representation of the vnode token, prefixed with "0x"
    (e.g. "0x001a3cb4f200000000000000000000001a" zero-padded to 32 hex digits
    for a 128-bit hash space). The CLI truncates it to the first 8 hex digits
    followed by the ellipsis character "…" when rendering the standard vnode
    line:

        token 0x1a3cb4f2…  →  pids  0–63  (64 partitions)

    The response payload always carries the untruncated token_hex; truncation
    is a CLI rendering decision only.
    """

    start_pid: int
    end_pid: int
    count: int
    token_hex: str  # "0x<32 hex digits>" for 128-bit space; CLI truncates to 8


@dataclass(frozen=True)
class MemberSummary:
    """Condensed Member record for inclusion in NodeInspectResponse.members."""

    node_id: str
    phase: str  # MemberPhase value
    generation: int
    seq: int
    peer_address: str


@dataclass(frozen=True)
class ProbeSummary:
    """Local probe state for one peer, as seen by the inspected node."""

    node_id: str
    state: str  # MemberState value: "live" | "suspect" | "unknown"
    phi: float  # current φ value; 0.0 when no observations yet


@dataclass(frozen=True)
class NodeInspectResponse:
    """Full live snapshot returned by the target node in response to node.inspect.

    partition_ranges contains one entry per owned vnode, sorted by start_pid.
    For the vnode with the minimum token (wrapping arc), start_pid may exceed
    end_pid; count still reflects the correct total for that vnode's arc.

    members contains entries from the target's MemberRegistry sorted by
    node_id, truncated to INSPECT_MEMBER_LIMIT when the registry is large.
    members_total always reflects the true registry size before truncation.
    probe_states contains entries from the target's ProbeManager sorted by
    node_id, subject to the same limit.

    forwarded_by is None when the contact node and the target node are the
    same; otherwise it is the node_id of the node that proxied the request.
    Both members_truncated and probe_states_truncated default to False and are
    always present in the serialised payload.
    """

    node_id: str
    phase: str  # MemberPhase value
    peer_address: str
    kv_address: str
    size: str  # NodeSize value (e.g. "M")
    generation: int
    seq: int
    epoch: int
    tokens: tuple[int, ...]
    total_partitions: int
    partition_shift: int
    owned_partitions: int
    partition_ranges: tuple[PartitionRange, ...]
    members: tuple[MemberSummary, ...]
    members_truncated: bool
    members_total: int
    probe_states: tuple[ProbeSummary, ...]
    probe_states_truncated: bool
    forwarded_by: str | None


@dataclass(frozen=True)
class NodePeerViewResponse:
    """Gossip record returned by the contact node for node.inspect.peer_view.

    This response is constructed from the contact node's own MemberRegistry
    and ProbeManager. It never involves the target node. phi is 0.0 when the
    contact has never recorded a probe observation for the target.
    """

    target_node_id: str
    observed_by: str
    phase: str  # MemberPhase value
    generation: int
    seq: int
    peer_address: str
    tokens: tuple[int, ...]
    probe_state: str  # MemberState value
    phi: float
