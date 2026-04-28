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
KIND_MAX_LEN: int = 64

_HEADER_FMT: str = "!BH16sI"
_HEADER_SIZE: int = struct.calcsize(_HEADER_FMT)


@dataclass(frozen=True)
class Envelope:
    """Framing container that wraps an opaque binary payload for transmission.

    An Envelope provides a minimal, version-aware binary header common to every
    message exchanged in the Tourillon protocol, regardless of the semantic
    operation being performed.

    The fixed 23-byte header encodes the protocol version, a 16-bit schema
    identifier for independent payload versioning, a 128-bit UUID correlation
    identifier for matching responses to requests, and a 32-bit payload length.
    Following the fixed header, a 1-byte length prefix introduces a variable-
    length UTF-8 string field named kind, which callers use to identify the
    operation or message category. The kind field is an open string: no central
    registry exists and new values may be introduced freely, subject to the
    byte-length constraint enforced at construction and decode time.

    The binary layout is:

        offset      bytes   field
        0           1       proto_version  (uint8)
        1           2       schema_id      (uint16 big-endian)
        3           16      correlation_id (UUID as 16 raw bytes)
        19          4       payload_len    (uint32 big-endian)
        23          1       kind_len       (uint8, 1 … KIND_MAX_LEN)
        24          N       kind           (UTF-8 string, N = kind_len)
        24 + N      M       payload        (M = payload_len)

    kind must be non-empty and its UTF-8 encoding must not exceed KIND_MAX_LEN
    bytes. Both create and decode raise ValueError when these constraints are
    violated.

    Callers should always construct envelopes with the create factory and
    decode incoming frames with decode. Direct instantiation is permitted but
    callers bear responsibility for keeping proto_version consistent.
    """

    proto_version: int
    correlation_id: uuid.UUID
    schema_id: int
    kind: str
    payload: bytes

    def __post_init__(self) -> None:
        """Enforce kind constraints at construction time.

        Raise ValueError when kind is empty or when its UTF-8 encoding exceeds
        KIND_MAX_LEN bytes. This guard applies to both direct instantiation and
        to the create factory, since the factory delegates to the constructor.
        """
        if not self.kind:
            raise ValueError("kind must not be empty")
        encoded = self.kind.encode()
        if len(encoded) > KIND_MAX_LEN:
            raise ValueError(
                f"kind is {len(encoded)} bytes, exceeds maximum of {KIND_MAX_LEN}"
            )

    @classmethod
    def create(
        cls,
        payload: bytes,
        *,
        kind: str,
        correlation_id: uuid.UUID | None = None,
        schema_id: int = 0,
    ) -> Self:
        """Construct a new outgoing envelope stamped with the current protocol version.

        kind identifies the operation or message category and must satisfy the
        non-empty and KIND_MAX_LEN byte constraints. When correlation_id is
        omitted a random UUID v4 is generated. Callers that need to correlate a
        response with a prior request should supply the same correlation_id that
        was carried by the request envelope.
        """
        return cls(
            proto_version=PROTO_VERSION,
            correlation_id=(
                correlation_id if correlation_id is not None else uuid.uuid4()
            ),
            schema_id=schema_id,
            kind=kind,
            payload=payload,
        )

    def encode(self) -> bytes:
        """Serialise this envelope into its canonical wire representation."""
        kind_bytes = self.kind.encode()
        header = struct.pack(
            _HEADER_FMT,
            self.proto_version,
            self.schema_id,
            self.correlation_id.bytes,
            len(self.payload),
        )
        return header + bytes([len(kind_bytes)]) + kind_bytes + self.payload

    @classmethod
    def decode(cls, data: bytes) -> Self:
        """Deserialise a wire frame back into an Envelope.

        Raise ValueError with a descriptive message when the frame is shorter
        than the minimum header, when kind_len is zero or exceeds KIND_MAX_LEN,
        or when the declared field lengths claim more bytes than are available.
        Trailing bytes beyond the declared payload are silently ignored so that
        forward-compatible extensions can append optional data without breaking
        older receivers.
        """
        min_size = _HEADER_SIZE + 1
        if len(data) < min_size:
            raise ValueError(
                f"frame too short: got {len(data)} bytes, need at least {min_size}"
            )
        proto_version, schema_id, cid_bytes, payload_len = struct.unpack_from(
            _HEADER_FMT, data
        )
        kind_len = data[_HEADER_SIZE]
        if kind_len == 0:
            raise ValueError("kind_len is zero: kind must not be empty")
        if kind_len > KIND_MAX_LEN:
            raise ValueError(f"kind_len {kind_len} exceeds maximum of {KIND_MAX_LEN}")
        total_needed = _HEADER_SIZE + 1 + kind_len + payload_len
        if len(data) < total_needed:
            raise ValueError(
                f"truncated frame: need {total_needed} bytes, got {len(data)}"
            )
        kind_start = _HEADER_SIZE + 1
        kind = data[kind_start : kind_start + kind_len].decode()
        payload_start = kind_start + kind_len
        payload = data[payload_start : payload_start + payload_len]
        return cls(
            proto_version=proto_version,
            correlation_id=uuid.UUID(bytes=cid_bytes),
            schema_id=schema_id,
            kind=kind,
            payload=payload,
        )
