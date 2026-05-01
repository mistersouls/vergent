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
"""Replication value objects and leaderless write coordinator."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

from tourillon.core.ports.replication import QuorumNotReachedError
from tourillon.core.ports.storage import DeleteOp, WriteOp
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.version import Tombstone, Version

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReplicaAck:
    """Single replica's acknowledgement of a replicate request.

    The acknowledgement carries the identifier of the acknowledging replica
    and the HLC timestamp that the replica recorded for the write. The
    timestamp must equal the coordinator's timestamp because replicas never
    re-stamp incoming writes; this property is what allows hinted handoff
    replays to be a deterministic no-op when a write has already been
    applied through the live fan-out path.
    """

    node_id: str
    ts: HLCTimestamp

    def __post_init__(self) -> None:
        """Validate that the acknowledgement is structurally well formed."""
        if not self.node_id:
            raise ValueError("node_id must be a non-empty string")
        if not isinstance(self.ts, HLCTimestamp):
            raise TypeError("ts must be an HLCTimestamp instance")


@dataclass(frozen=True)
class QuorumOutcome:
    """Result of a single replicate_write or replicate_delete fan-out.

    ``success`` is true if and only if the number of collected
    acknowledgements is greater than or equal to the configured write
    quorum ``W``. ``acked`` lists the replicas that acknowledged, in the
    order in which their acks were observed. ``hinted`` lists the node
    identifiers whose acknowledgement did not arrive within the budget and
    whose deferred write has therefore been persisted as a hint by the time
    this outcome is constructed.
    """

    success: bool
    acked: tuple[ReplicaAck, ...]
    hinted: tuple[str, ...]


@dataclass(frozen=True)
class ReplicationRequest:
    """Frozen input snapshot for one replication fan-out.

    A ``ReplicationRequest`` carries the operation to replicate, a
    discriminator describing whether the operation is a put or a delete,
    the canonical HLC timestamp already assigned by the local store, and
    the preference list snapshot computed from the ring view that was
    current when the client request arrived. The preference list is held
    as a tuple to make the structural immutability of the request explicit
    and to defend against concurrent mutation by the upstream caller.

    The HLC timestamp is forwarded verbatim to every replica and to every
    hint produced from this request. The preference list is never
    recomputed mid-replication, which preserves the ring-epoch coupling
    invariant: a fan-out always targets the replica set that was visible
    at the moment the client request was accepted.
    """

    op: WriteOp | DeleteOp
    kind: Literal["put", "delete"]
    ts: HLCTimestamp
    preference_list: tuple[str, ...]

    def __post_init__(self) -> None:
        """Validate the request and coerce a list preference list to a tuple."""
        if self.kind not in ("put", "delete"):
            raise ValueError("kind must be either 'put' or 'delete'")
        if not isinstance(self.ts, HLCTimestamp):
            raise TypeError("ts must be an HLCTimestamp instance")
        if not isinstance(self.preference_list, tuple):
            if isinstance(self.preference_list, list):
                object.__setattr__(
                    self,
                    "preference_list",
                    tuple(self.preference_list),
                )
            else:
                raise TypeError("preference_list must be a tuple or list of node_ids")
        for node_id in self.preference_list:
            if not isinstance(node_id, str) or not node_id:
                raise ValueError("preference_list entries must be non-empty strings")


ReplicateOne = Callable[[str, "ReplicationRequest"], Awaitable[ReplicaAck]]
"""Outbound replicate adapter: call ``target_node_id`` with ``request``."""

DelegateHint = Callable[[str, str, "ReplicationRequest"], Awaitable[None]]
"""Hint delegation adapter: ``(keeper_node_id, target_node_id, request)``."""


def _drain_replica_acks(
    ack_queue: "asyncio.Queue[ReplicaAck | BaseException]",
    acks: list[ReplicaAck],
) -> None:
    """Drain any pending acks from the queue into ``acks`` without blocking.

    Called after quorum is reached to collect acknowledgements from tasks
    that completed before they could be cancelled. This prevents nodes that
    successfully replicated the write from being incorrectly classified as
    non-acking and receiving an unnecessary hinted-handoff delegation.
    """
    while True:
        try:
            leftover = ack_queue.get_nowait()
            if isinstance(leftover, ReplicaAck):
                acks.append(leftover)
        except asyncio.QueueEmpty:
            break


class _EarlyExit(BaseException):
    """Sentinel raised inside a TaskGroup body to stop fan-out at quorum.

    Subclassing :class:`BaseException` (not :class:`Exception`) ensures
    the sentinel propagates through ``except Exception`` guards in user
    code and through the TaskGroup's body unwrapped (not as part of an
    :class:`ExceptionGroup`).
    """


class ReplicaCoordinator:
    """Async leaderless write coordinator.

    The coordinator drives a single replicate_write or replicate_delete
    fan-out. It enforces three invariants:

    * Bounded concurrency: a semaphore caps the number of simultaneous
      fan-outs to ``max_in_flight`` so a coordinator cannot exhaust the
      transport's outbound capacity under load.
    * Bounded latency: an :func:`asyncio.timeout` caps the wall-clock
      duration of every fan-out at ``request_budget_ms`` milliseconds.
    * Quorum or fail: as soon as ``write_quorum`` acknowledgements (the
      local write counts as one ack when the local node is in the
      preference list) are observed, remaining replica calls are
      cancelled. If the budget elapses first and the quorum has not been
      reached, :class:`QuorumNotReachedError` is raised and no hints are
      delegated.

    Hint delegation only happens on success: when quorum is reached, the
    keeper (the highest-priority acking replica in preference-list order)
    is asked, via ``delegate_hint``, to persist a hint for every replica
    in the preference list that did not ack. This guarantees Dynamo-style
    eventual delivery without the coordinator itself owning hint storage.
    """

    def __init__(
        self,
        *,
        local_node_id: str,
        replicate_one: ReplicateOne,
        delegate_hint: DelegateHint,
        write_quorum: int,
        max_in_flight: int,
        request_budget_ms: int,
    ) -> None:
        """Build a coordinator with the supplied outbound adapters and limits."""
        self._local_node_id = local_node_id
        self._replicate_one = replicate_one
        self._delegate_hint = delegate_hint
        self._write_quorum = write_quorum
        self._max_in_flight = max_in_flight
        self._request_budget_ms = request_budget_ms
        self._semaphore = asyncio.Semaphore(max_in_flight)
        self._budget_secs = request_budget_ms / 1000.0

    async def replicate_write(
        self,
        op: WriteOp,
        version: Version,
        preference_list: list[str],
    ) -> Version:
        """Replicate a put to ``preference_list`` and return ``version`` on quorum.

        The local write is assumed to have already been performed by the
        caller; if ``local_node_id`` is in the preference list it is
        counted as one ack. On quorum success, hints are delegated to the
        keeper for any non-acking replica and ``version`` is returned. On
        quorum failure, :class:`QuorumNotReachedError` is raised and no
        hints are delegated.
        """
        request = ReplicationRequest(
            op=op,
            kind="put",
            ts=version.metadata,
            preference_list=tuple(preference_list),
        )
        local_ack: ReplicaAck | None = None
        if self._local_node_id in preference_list:
            local_ack = ReplicaAck(node_id=self._local_node_id, ts=request.ts)
        acks, non_acking = await self._fan_out(request, local_ack)
        if len(acks) >= self._write_quorum:
            await self._delegate_hints(acks, non_acking, request)
            return version
        raise QuorumNotReachedError(
            acked=len(acks),
            required=self._write_quorum,
            ack_node_ids=[a.node_id for a in acks],
        )

    async def replicate_delete(
        self,
        op: DeleteOp,
        tombstone: Tombstone,
        preference_list: list[str],
    ) -> Tombstone:
        """Replicate a delete to ``preference_list`` and return ``tombstone`` on quorum.

        Behaves identically to :meth:`replicate_write` but carries a
        ``DeleteOp`` and the tombstone's HLC timestamp.
        """
        request = ReplicationRequest(
            op=op,
            kind="delete",
            ts=tombstone.metadata,
            preference_list=tuple(preference_list),
        )
        local_ack: ReplicaAck | None = None
        if self._local_node_id in preference_list:
            local_ack = ReplicaAck(node_id=self._local_node_id, ts=request.ts)
        acks, non_acking = await self._fan_out(request, local_ack)
        if len(acks) >= self._write_quorum:
            await self._delegate_hints(acks, non_acking, request)
            return tombstone
        raise QuorumNotReachedError(
            acked=len(acks),
            required=self._write_quorum,
            ack_node_ids=[a.node_id for a in acks],
        )

    async def _fan_out(
        self,
        request: ReplicationRequest,
        local_ack: ReplicaAck | None,
    ) -> tuple[list[ReplicaAck], list[str]]:
        """Fan out to remote replicas and return (acked, non_acking) lists.

        Seeds ``acks`` with the local ack when the coordinator is in the
        preference list, then delegates concurrent outbound calls to
        :meth:`_collect_acks`. The non-acking set is derived from the
        preference list after all acknowledgements have been collected.
        """
        acks: list[ReplicaAck] = [local_ack] if local_ack is not None else []
        remote_targets = [
            nid for nid in request.preference_list if nid != self._local_node_id
        ]
        if remote_targets:
            async with self._semaphore:
                await self._collect_acks(remote_targets, request, acks)
        acked_ids = {a.node_id for a in acks}
        non_acking = [nid for nid in request.preference_list if nid not in acked_ids]
        return acks, non_acking

    async def _collect_acks(
        self,
        remote_targets: list[str],
        request: ReplicationRequest,
        acks: list[ReplicaAck],
    ) -> None:
        """Launch per-replica tasks and collect acks until quorum or timeout.

        Launches one :meth:`_call_one` task per target inside a
        :class:`asyncio.TaskGroup` bounded by :func:`asyncio.timeout`.
        As soon as the accumulated ack count reaches ``write_quorum``,
        :class:`_EarlyExit` is raised from the TaskGroup body, which
        causes Python 3.13+ to cancel remaining tasks and wrap the
        sentinel in a :class:`BaseExceptionGroup`. The ``except*``
        handler catches that group and drains any acks that arrived from
        tasks that completed before cancellation so that they are not
        incorrectly treated as non-acking. A :class:`TimeoutError` from
        the budget expiry is swallowed; the caller inspects ``acks`` to
        determine success or failure.

        ``acks`` is mutated in place.
        """
        ack_queue: asyncio.Queue[ReplicaAck | BaseException] = asyncio.Queue()
        try:
            async with asyncio.timeout(self._budget_secs):
                try:
                    async with asyncio.TaskGroup() as tg:
                        for target in remote_targets:
                            tg.create_task(
                                self._call_one(target, request, ack_queue),
                                name=f"replicate-{target}",
                            )
                        for _ in range(len(remote_targets)):
                            result = await ack_queue.get()
                            if isinstance(result, ReplicaAck):
                                acks.append(result)
                            if len(acks) >= self._write_quorum:
                                raise _EarlyExit
                except* _EarlyExit:
                    _drain_replica_acks(ack_queue, acks)
        except TimeoutError:
            pass

    async def _delegate_hints(
        self,
        acks: list[ReplicaAck],
        non_acking: list[str],
        request: ReplicationRequest,
    ) -> None:
        """Ask the highest-priority acking replica to keep hints for non-acking peers.

        Only invoked after the coordinator has confirmed quorum success;
        on quorum failure no hints are delegated, preserving the
        invariant that hints exist only for writes that the cluster has
        accepted as durable.
        """
        if not non_acking:
            return
        pl_order = {nid: i for i, nid in enumerate(request.preference_list)}
        keeper = min(
            acks,
            key=lambda a: pl_order.get(a.node_id, len(request.preference_list)),
        )
        for target in non_acking:
            try:
                await self._delegate_hint(keeper.node_id, target, request)
            except Exception:
                _logger.warning(
                    "failed to delegate hint for %r to keeper %r",
                    target,
                    keeper.node_id,
                )

    async def _call_one(
        self,
        target_node_id: str,
        request: ReplicationRequest,
        ack_queue: "asyncio.Queue[ReplicaAck | BaseException]",
    ) -> None:
        """Issue one outbound replica call and push its outcome on the queue.

        This coroutine never re-raises a regular exception: failures are
        forwarded to the queue as :class:`Exception` instances so the
        fan-out loop can count them as non-acks without tearing down the
        TaskGroup. :class:`asyncio.CancelledError` is a
        :class:`BaseException` and is therefore not caught here, which
        preserves correct cancellation semantics when the TaskGroup
        cancels remaining tasks after quorum is reached.
        """
        try:
            ack = await self._replicate_one(target_node_id, request)
            await ack_queue.put(ack)
        except Exception as exc:
            await ack_queue.put(exc)
