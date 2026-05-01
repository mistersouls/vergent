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
"""Server-side KindHandlers for handoff.push and handoff.delegate protocols."""

import logging

from tourillon.core.dispatch import Dispatcher
from tourillon.core.ports.serializer import SerializerPort
from tourillon.core.ports.storage import DeleteOp, ReplicaStoragePort, WriteOp
from tourillon.core.ports.transport import SendCallback
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.handoff import HintedHandoffQueue
from tourillon.core.structure.version import StoreKey, Tombstone, Version

KIND_HANDOFF_PUSH: str = "handoff.push"
KIND_HANDOFF_ACK: str = "handoff.ack"
KIND_HANDOFF_DELEGATE: str = "handoff.delegate"
KIND_HANDOFF_DELEGATE_OK: str = "handoff.delegate.ok"
KIND_KV_ERR: str = "kv.err"

_logger = logging.getLogger(__name__)


def _err_envelope(
    serializer: SerializerPort, envelope: Envelope, message: str
) -> Envelope:
    """Build a kv.err response envelope preserving the request correlation_id."""
    return Envelope.create(
        serializer.encode({"error": message}),
        kind=KIND_KV_ERR,
        correlation_id=envelope.correlation_id,
    )


class HandoffHandlers:
    """Server-side handlers for the handoff.push and handoff.delegate protocols.

    handoff.push: a keeper/sender replays a previously deferred write to the
    target replica. The payload schema is identical to kv.replicate. The
    handler applies the pre-stamped Version or Tombstone to local storage via
    ReplicaStoragePort and responds with handoff.ack carrying the coordinator's
    HLCTimestamp verbatim.

    handoff.ack: a response kind that travels target → sender. A handoff.ack
    arriving inbound at a server is unexpected; the handler logs a warning and
    discards it. The correlation-based ack matching happens on the sender side
    outside the dispatcher.

    handoff.delegate: the coordinator asks this node to become a *keeper* for a
    hint targeting some other replica (target_node_id in the payload). The
    handler enqueues the hint in the local HintedHandoffQueue so that it will
    be replayed via handoff.push when target_node_id becomes reachable. The
    response is handoff.delegate.ok carrying the keeper's HLCTimestamp.

    handoff.delegate.ok: a response kind that travels keeper → coordinator. An
    inbound handoff.delegate.ok at a non-coordinator node is unexpected; the
    handler logs a warning and discards.

    All four kinds are registered by calling register() on a Dispatcher. The
    HintedHandoffQueue is only required for the keeper role (handoff.delegate);
    nodes that never act as keepers can still register all handlers safely.
    """

    def __init__(
        self,
        store: ReplicaStoragePort,
        serializer: SerializerPort,
        handoff_queue: HintedHandoffQueue,
    ) -> None:
        """Bind to local storage, serializer, and the local handoff queue."""
        self._store = store
        self._serializer = serializer
        self._queue = handoff_queue

    def register(self, dispatcher: Dispatcher) -> None:
        """Register all four handoff kind handlers on dispatcher."""
        dispatcher.register(KIND_HANDOFF_PUSH, self.handle_push)
        dispatcher.register(KIND_HANDOFF_ACK, self.handle_ack)
        dispatcher.register(KIND_HANDOFF_DELEGATE, self.handle_delegate)
        dispatcher.register(KIND_HANDOFF_DELEGATE_OK, self.handle_delegate_ok)

    async def handle_push(self, envelope: Envelope, send: SendCallback) -> None:
        """Apply a deferred write and respond handoff.ack.

        The payload schema is identical to kv.replicate:
            {op_kind: "put"|"delete", keyspace: bytes, key: bytes,
             value?: bytes, wall: int, counter: int, node_id: str}

        On success the response payload is {wall, counter, node_id} mirroring
        the carried HLCTimestamp. The response kind is handoff.ack.
        Idempotency is guaranteed by ReplicaStoragePort.
        """
        try:
            data = self._serializer.decode(envelope.payload)
            address = StoreKey(keyspace=data["keyspace"], key=data["key"])
            ts = HLCTimestamp(
                wall=int(data["wall"]),
                counter=int(data["counter"]),
                node_id=str(data["node_id"]),
            )
            op_kind = str(data["op_kind"])
            if op_kind == "put":
                version = Version(
                    address=address, metadata=ts, value=bytes(data["value"])
                )
                await self._store.apply_version(version)
            elif op_kind == "delete":
                tombstone = Tombstone(address=address, metadata=ts)
                await self._store.apply_tombstone(tombstone)
            else:
                raise ValueError(f"unknown op_kind: {op_kind!r}")
        except Exception as exc:
            _logger.warning(
                "handoff.push failed (correlation_id=%s): %s",
                envelope.correlation_id,
                exc,
            )
            await send(_err_envelope(self._serializer, envelope, str(exc)))
            return

        await send(
            Envelope.create(
                self._serializer.encode(
                    {"wall": ts.wall, "counter": ts.counter, "node_id": ts.node_id}
                ),
                kind=KIND_HANDOFF_ACK,
                correlation_id=envelope.correlation_id,
            )
        )

    async def handle_ack(self, envelope: Envelope, send: SendCallback) -> None:
        """Discard an unexpected inbound handoff.ack and log a warning.

        handoff.ack travels target → sender as a response to handoff.push.
        Receiving it as an inbound request on the server side indicates a
        protocol routing error. The envelope is discarded to avoid a tight
        response loop.
        """
        _logger.warning(
            "Unexpected inbound handoff.ack (correlation_id=%s) — discarding",
            envelope.correlation_id,
        )

    async def handle_delegate(self, envelope: Envelope, send: SendCallback) -> None:
        """Enqueue a hint for target_node_id and respond handoff.delegate.ok.

        The payload schema extends kv.replicate with an extra field:
            {op_kind: "put"|"delete", keyspace: bytes, key: bytes,
             value?: bytes, wall: int, counter: int, node_id: str,
             target_node_id: str}

        The handler enqueues the hint in the local HintedHandoffQueue under the
        supplied target_node_id. The keeper (this node) is responsible for
        replaying the hint via handoff.push once target_node_id joins the ring
        again. The response carries the coordinator's HLCTimestamp verbatim.
        """
        try:
            data = self._serializer.decode(envelope.payload)
            target_node_id = str(data["target_node_id"])
            ts = HLCTimestamp(
                wall=int(data["wall"]),
                counter=int(data["counter"]),
                node_id=str(data["node_id"]),
            )
            op_kind = str(data["op_kind"])
            address = StoreKey(keyspace=data["keyspace"], key=data["key"])
            op: WriteOp | DeleteOp
            if op_kind == "put":
                op = WriteOp(
                    address=address, value=bytes(data["value"]), now_ms=ts.wall
                )
            elif op_kind == "delete":
                op = DeleteOp(address=address, now_ms=ts.wall)
            else:
                raise ValueError(f"unknown op_kind: {op_kind!r}")
            await self._queue.enqueue(
                target_node_id=target_node_id,
                ts=ts,
                op=op,
                kind=op_kind,
            )
        except Exception as exc:
            _logger.warning(
                "handoff.delegate failed (correlation_id=%s): %s",
                envelope.correlation_id,
                exc,
            )
            await send(_err_envelope(self._serializer, envelope, str(exc)))
            return

        await send(
            Envelope.create(
                self._serializer.encode(
                    {"wall": ts.wall, "counter": ts.counter, "node_id": ts.node_id}
                ),
                kind=KIND_HANDOFF_DELEGATE_OK,
                correlation_id=envelope.correlation_id,
            )
        )

    async def handle_delegate_ok(self, envelope: Envelope, send: SendCallback) -> None:
        """Discard an unexpected inbound handoff.delegate.ok and log a warning.

        handoff.delegate.ok travels keeper → coordinator as a response to
        handoff.delegate. Receiving it inbound at a non-coordinator node is
        a protocol routing error and the envelope is discarded.
        """
        _logger.warning(
            "Unexpected inbound handoff.delegate.ok (correlation_id=%s) — discarding",
            envelope.correlation_id,
        )
