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
"""Binary framing envelope for Tourillon protocol messages."""

import struct
import uuid
from dataclasses import dataclass
from typing import Self

PROTO_VERSION: int = 1

_HEADER_FMT: str = "!BH16sI"
_HEADER_SIZE: int = struct.calcsize(_HEADER_FMT)


@dataclass(frozen=True)
class Envelope:
    """Framing container that wraps an opaque binary payload for transmission.

    An Envelope provides a minimal, version-aware binary header common to every
    message exchanged in the Tourillon protocol, regardless of the semantic
    operation being performed. The choice of which operation a message
    represents is deliberately absent here: routing and dispatch logic belongs
    to the transport layer, which adds its own verb framing on top of or around
    the envelope body.

    The fixed 23-byte header encodes the protocol version so that receivers can
    reject frames from incompatible peers before attempting to parse the
    payload, a 16-bit schema identifier that callers may use to version their
    serialisation format independently of the protocol version, a 128-bit UUID
    correlation identifier for matching responses to their originating requests,
    and a 32-bit payload length that bounds how many bytes the receiver reads.

    The binary layout is:

        offset  bytes  field
        0       1      proto_version  (uint8)
        1       2      schema_id      (uint16 big-endian)
        3       16     correlation_id (UUID as 16 raw bytes)
        19      4      payload_len    (uint32 big-endian)
        23      N      payload

    Callers should always construct envelopes with the create factory and
    decode incoming frames with decode. Direct instantiation is permitted but
    callers then bear responsibility for keeping proto_version consistent.
    """

    proto_version: int
    correlation_id: uuid.UUID
    schema_id: int
    payload: bytes

    @classmethod
    def create(
        cls,
        payload: bytes,
        *,
        correlation_id: uuid.UUID | None = None,
        schema_id: int = 0,
    ) -> Self:
        """Construct a new outgoing envelope stamped with the current protocol version.

        When correlation_id is omitted a random UUID v4 is generated. Callers
        that need to correlate a response with a prior request should supply
        the same correlation_id that was carried by the request envelope.
        """
        return cls(
            proto_version=PROTO_VERSION,
            correlation_id=(
                correlation_id if correlation_id is not None else uuid.uuid4()
            ),
            schema_id=schema_id,
            payload=payload,
        )

    def encode(self) -> bytes:
        """Serialise this envelope into its canonical wire representation."""
        header = struct.pack(
            _HEADER_FMT,
            self.proto_version,
            self.schema_id,
            self.correlation_id.bytes,
            len(self.payload),
        )
        return header + self.payload

    @classmethod
    def decode(cls, data: bytes) -> Self:
        """Deserialise a wire frame back into an Envelope.

        Raise ValueError with a descriptive message when the frame is shorter
        than the header or when the payload_len field claims more bytes than are
        available. Trailing bytes beyond payload_len are silently ignored so
        that forward-compatible extensions can append optional data without
        breaking older receivers.
        """
        if len(data) < _HEADER_SIZE:
            raise ValueError(
                f"frame too short: got {len(data)} bytes, need {_HEADER_SIZE}"
            )
        proto_version, schema_id, cid_bytes, payload_len = struct.unpack_from(
            _HEADER_FMT, data
        )
        if len(data) < _HEADER_SIZE + payload_len:
            raise ValueError(
                f"truncated payload: expected {payload_len} bytes,"
                f" got {len(data) - _HEADER_SIZE}"
            )
        payload = data[_HEADER_SIZE : _HEADER_SIZE + payload_len]
        return cls(
            proto_version=proto_version,
            correlation_id=uuid.UUID(bytes=cid_bytes),
            schema_id=schema_id,
            payload=payload,
        )
