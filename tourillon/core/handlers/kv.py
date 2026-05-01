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
"""KV kind handlers for the Tourillon bootstrap layer.

This module registers the three primary key-value operations — ``kv.put``,
``kv.get``, and ``kv.delete`` — as KindHandlers on a Dispatcher. Each
operation is a dedicated async method that decodes the Envelope payload via a
SerializerPort, delegates to the LocalStoragePort, and sends a response
Envelope back through the same connection using the request's correlation_id.

Wire protocol (all payloads encoded/decoded via SerializerPort):

    kv.put  request  → {keyspace: bytes, key: bytes, value: bytes, now_ms: int}
    kv.put  response → kind "kv.put.ok":    {wall: int, counter: int, node_id: str}

    kv.get  request  → {keyspace: bytes, key: bytes}
    kv.get  response → kind "kv.get.ok":    {versions: [{wall: int, counter: int,
                                              node_id: str, value: bytes}]}

    kv.delete request  → {keyspace: bytes, key: bytes, now_ms: int}
    kv.delete response → kind "kv.delete.ok": {wall: int, counter: int, node_id: str}

    error response     → kind "kv.err":      {error: str}

Raw bytes payloads (keys, values) are transmitted as native binary fields by
the serializer, with no base64 encoding. The correlation_id carried by the
request Envelope is forwarded verbatim to the response so that clients can
match replies to outstanding requests.
"""

import logging
import time
from collections.abc import Callable

from tourillon.core.dispatch import Dispatcher
from tourillon.core.ports.serializer import SerializerPort
from tourillon.core.ports.storage import DeleteOp, LocalStoragePort, ReadOp, WriteOp
from tourillon.core.ports.transport import SendCallback
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.version import StoreKey

KIND_KV_PUT: str = "kv.put"
KIND_KV_GET: str = "kv.get"
KIND_KV_DELETE: str = "kv.delete"

KIND_KV_PUT_OK: str = "kv.put.ok"
KIND_KV_GET_OK: str = "kv.get.ok"
KIND_KV_DELETE_OK: str = "kv.delete.ok"
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


class KvHandlers:
    """Server-side handlers for the kv.put, kv.get, and kv.delete protocol operations.

    KvHandlers wraps a LocalStoragePort and a SerializerPort and exposes three
    async methods that satisfy the KindHandler protocol. Each method is
    registered by calling register() on a Dispatcher prior to starting the
    TcpServer. After registration a client can open an mTLS connection, send an
    Envelope whose payload was encoded with the same SerializerPort, and receive
    a response Envelope whose payload can be decoded with the same serializer.

    A single KvHandlers instance is safe to share across all connections because
    it holds no per-connection state; all state lives in the LocalStoragePort
    implementation and the SerializerPort is stateless.
    """

    def __init__(
        self,
        store: LocalStoragePort,
        serializer: SerializerPort,
        now_ms_provider: Callable[[], int] | None = None,
    ) -> None:
        """Attach to the given storage port and serializer for all KV operations.

        The optional now_ms_provider is called to obtain the current logical
        wall-clock hint (in milliseconds) when a client request omits the
        now_ms field. When None the provider defaults to a monotonic-based
        millisecond clock. Production node assembly should supply a node-level
        HLC provider; the default is safe for testing and single-node scenarios.
        """
        self._store = store
        self._serializer = serializer
        self._now_ms: Callable[[], int] = (
            now_ms_provider
            if now_ms_provider is not None
            else (lambda: int(time.monotonic() * 1000))
        )

    def register(self, dispatcher: Dispatcher) -> None:
        """Register all three KV handler methods on dispatcher.

        Call this once before the TcpServer is started. Subsequent calls to
        register() on the same dispatcher will overwrite the previous bindings.
        """
        dispatcher.register(KIND_KV_PUT, self.handle_put)
        dispatcher.register(KIND_KV_GET, self.handle_get)
        dispatcher.register(KIND_KV_DELETE, self.handle_delete)

    async def handle_put(self, envelope: Envelope, send: SendCallback) -> None:
        """Process a kv.put request and send back a kv.put.ok or kv.err response.

        The request payload must decode to a map with binary fields "keyspace",
        "key", "value", and an optional integer field "now_ms". When "now_ms" is
        absent the server derives it from the configured wall-clock provider. The response
        payload encodes the three fields of the HLCTimestamp assigned by the store.
        """
        try:
            data = self._serializer.decode(envelope.payload)
            address = StoreKey(keyspace=data["keyspace"], key=data["key"])
            now_ms: int = data.get("now_ms", self._now_ms())
            op = WriteOp(address=address, value=data["value"], now_ms=now_ms)
            version = await self._store.put(op)
        except Exception as exc:
            _logger.warning(
                "kv.put failed (correlation_id=%s): %s", envelope.correlation_id, exc
            )
            await send(_err_envelope(self._serializer, envelope, str(exc)))
            return

        await send(
            Envelope.create(
                self._serializer.encode(
                    {
                        "wall": version.metadata.wall,
                        "counter": version.metadata.counter,
                        "node_id": version.metadata.node_id,
                    }
                ),
                kind=KIND_KV_PUT_OK,
                correlation_id=envelope.correlation_id,
            )
        )

    async def handle_get(self, envelope: Envelope, send: SendCallback) -> None:
        """Process a kv.get request and send back a kv.get.ok or kv.err response.

        The request payload must decode to a map with binary fields "keyspace"
        and "key". The response payload encodes a "versions" list that is either
        empty (key not found or deleted) or contains a single map with "wall",
        "counter", "node_id", and "value" (raw bytes).
        """
        try:
            data = self._serializer.decode(envelope.payload)
            address = StoreKey(keyspace=data["keyspace"], key=data["key"])
            versions = await self._store.get(ReadOp(address=address))
        except Exception as exc:
            _logger.warning(
                "kv.get failed (correlation_id=%s): %s", envelope.correlation_id, exc
            )
            await send(_err_envelope(self._serializer, envelope, str(exc)))
            return

        await send(
            Envelope.create(
                self._serializer.encode(
                    {
                        "versions": [
                            {
                                "wall": v.metadata.wall,
                                "counter": v.metadata.counter,
                                "node_id": v.metadata.node_id,
                                "value": v.value,
                            }
                            for v in versions
                        ]
                    }
                ),
                kind=KIND_KV_GET_OK,
                correlation_id=envelope.correlation_id,
            )
        )

    async def handle_delete(self, envelope: Envelope, send: SendCallback) -> None:
        """Process a kv.delete request and send back a kv.delete.ok or kv.err response.

        The request payload must decode to a map with binary fields "keyspace",
        "key", and an optional integer field "now_ms". The response payload
        encodes the three fields of the HLCTimestamp assigned to the Tombstone.
        """
        try:
            data = self._serializer.decode(envelope.payload)
            address = StoreKey(keyspace=data["keyspace"], key=data["key"])
            now_ms = data.get("now_ms", self._now_ms())
            op = DeleteOp(address=address, now_ms=now_ms)
            tombstone = await self._store.delete(op)
        except Exception as exc:
            _logger.warning(
                "kv.delete failed (correlation_id=%s): %s", envelope.correlation_id, exc
            )
            await send(_err_envelope(self._serializer, envelope, str(exc)))
            return

        await send(
            Envelope.create(
                self._serializer.encode(
                    {
                        "wall": tombstone.metadata.wall,
                        "counter": tombstone.metadata.counter,
                        "node_id": tombstone.metadata.node_id,
                    }
                ),
                kind=KIND_KV_DELETE_OK,
                correlation_id=envelope.correlation_id,
            )
        )
