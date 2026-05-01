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
"""Driving port for the replication coordinator."""

from typing import Protocol, runtime_checkable

from tourillon.core.ports.storage import DeleteOp, WriteOp
from tourillon.core.structure.version import Tombstone, Version


class QuorumNotReachedError(Exception):
    """Raised when fewer than the required number of replicas acknowledged.

    The replication coordinator fans a write or delete operation out to a
    preference list of replicas and awaits acknowledgements until the write
    quorum ``W`` has been collected or the per-request budget elapses. When
    the budget elapses before the quorum has been reached, this exception is
    raised so that the caller can surface a degraded-mode response or
    propagate a failure to the client. By the time the exception is raised,
    every replica that did not acknowledge has already had a hint persisted
    by the coordinator, so the durability guarantees of hinted handoff are
    preserved independently of the outcome surfaced to the caller.

    The instance carries the partial acknowledgement count, the configured
    quorum requirement, and the list of node identifiers that did
    acknowledge, so the caller can build a precise diagnostic without having
    to inspect the coordinator's internal state.
    """

    def __init__(self, acked: int, required: int, ack_node_ids: list[str]) -> None:
        """Initialise the error with the partial acknowledgement state."""
        super().__init__(
            f"quorum not reached: {acked}/{required} acks from {ack_node_ids!r}"
        )
        self.acked: int = acked
        self.required: int = required
        self.ack_node_ids: list[str] = ack_node_ids


@runtime_checkable
class ReplicationPort(Protocol):
    """Driving port through which a coordinator replicates writes and deletes.

    The replication port is the single entry point through which a client
    facing handler asks the coordinator to fan a locally stamped write or
    delete out to the preference list, awaiting quorum. Hiding the fan-out,
    quorum, timeout, and hint-enqueue bookkeeping behind two methods keeps
    the call sites trivial: callers supply only the storage operation, the
    preference list already computed from the ring snapshot the request
    arrived on, and the local HLC timestamp that was assigned by the local
    store. The port returns the local ``Version`` or ``Tombstone`` exactly
    once quorum has been reached.

    Concrete implementations must be asynchronous, must never block the
    event loop, and must guarantee that on return the hint queue is
    consistent with the set of replicas that did and did not acknowledge.
    Implementations must also forward the supplied HLC timestamp verbatim
    to every replica and every hint, so that re-application on the replica
    side is a deterministic no-op when the same write is delivered twice.
    """

    async def replicate_write(
        self,
        op: WriteOp,
        version: Version,
        preference_list: list[str],
    ) -> Version:
        """Fan the locally stamped version out to every replica.

        The ``version.metadata`` field is the canonical HLC timestamp for
        this write. The exact same timestamp is carried verbatim to every
        replica and to every hint produced by this call, and implementations
        must not re-stamp the write on the replica side. The supplied
        preference list is the frozen snapshot computed from the ring view
        that was current when the client request arrived; it is not
        recomputed mid-replication.

        On success the method returns the original ``version`` once ``W``
        acknowledgements — including the local replica — have been
        collected. When the request budget elapses before ``W`` acks are
        gathered, the method raises :class:`QuorumNotReachedError`. In
        either case, every replica that failed to acknowledge has already
        been enqueued as a hint by the time this method returns or raises.
        """
        ...

    async def replicate_delete(
        self,
        op: DeleteOp,
        tombstone: Tombstone,
        preference_list: list[str],
    ) -> Tombstone:
        """Fan a locally stamped tombstone out to every replica.

        The contract mirrors :meth:`replicate_write` exactly: the
        ``tombstone.metadata`` field is the authoritative HLC timestamp
        for the deletion, and it is forwarded verbatim to every replica
        and every hint. The method returns the original ``tombstone``
        once quorum has been reached, raises :class:`QuorumNotReachedError`
        otherwise, and guarantees that hints have been enqueued for every
        replica that failed to acknowledge by the time it returns or
        raises.
        """
        ...
