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
"""Server-side KindHandlers for the kv.replicate node-to-node protocol."""

import logging

from tourillon.core.dispatch import Dispatcher
from tourillon.core.ports.serializer import SerializerPort
from tourillon.core.ports.storage import ReplicaStoragePort
from tourillon.core.ports.transport import SendCallback
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.version import StoreKey, Tombstone, Version

KIND_KV_REPLICATE: str = "kv.replicate"
KIND_KV_REPLICATE_OK: str = "kv.replicate.ok"
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


class ReplicationHandlers:
    """Server-side handlers for the kv.replicate node-to-node protocol.

    A node registers ReplicationHandlers with its Dispatcher to act as a
    *replica* in another node's fan-out. When a kv.replicate envelope arrives,
    the handler decodes the coordinator's canonical HLCTimestamp and the
    operation payload, constructs a pre-stamped Version or Tombstone, and
    applies it to local storage via ReplicaStoragePort — which stores the
    value with the coordinator's exact timestamp rather than generating a new
    one. The response echoes the coordinator's timestamp verbatim so that the
    coordinator can confirm Invariant #4 (no re-stamping) is satisfied.

    Idempotency: ReplicaStoragePort.apply_version / apply_tombstone are
    idempotent by contract. Receiving the same (address, metadata) pair twice
    produces the same visible state as receiving it once, which makes repeated
    retries safe without tracking seen correlation_ids.
    """

    def __init__(
        self,
        store: ReplicaStoragePort,
        serializer: SerializerPort,
    ) -> None:
        """Bind to the replica storage port and message serializer."""
        self._store = store
        self._serializer = serializer

    def register(self, dispatcher: Dispatcher) -> None:
        """Register handle_replicate on dispatcher under KIND_KV_REPLICATE."""
        dispatcher.register(KIND_KV_REPLICATE, self.handle_replicate)

    async def handle_replicate(self, envelope: Envelope, send: SendCallback) -> None:
        """Decode a kv.replicate envelope, apply the write, and respond kv.replicate.ok.

        The request payload schema is:
            {op_kind: "put"|"delete", keyspace: bytes, key: bytes,
             value?: bytes, wall: int, counter: int, node_id: str}

        On success the response payload is {wall, counter, node_id} mirroring
        the coordinator's HLCTimestamp exactly. On failure a kv.err response
        is sent with the error message.
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
                "kv.replicate failed (correlation_id=%s): %s",
                envelope.correlation_id,
                exc,
            )
            await send(_err_envelope(self._serializer, envelope, str(exc)))
            return

        await send(
            Envelope.create(
                self._serializer.encode(
                    {
                        "wall": ts.wall,
                        "counter": ts.counter,
                        "node_id": ts.node_id,
                    }
                ),
                kind=KIND_KV_REPLICATE_OK,
                correlation_id=envelope.correlation_id,
            )
        )
