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
"""compute_transfer_digest — SHA-256 integrity digest for rebalance transfers."""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Iterable

from tourillon.core.structure.record import Record, Tombstone, Version


def compute_transfer_digest(records: Iterable[Record]) -> str:
    """Return the hex SHA-256 over records in their iteration order (HLC order).

    Fed with only the records received in the current transfer session (from
    resume_from onward). Caller passes records in the order they were streamed;
    order must match the source's scan order. The same sequence of identical
    records always produces the same digest regardless of the call site.

    Each record contributes a deterministic byte sequence:
      - kind byte (0x00 = Version, 0x01 = Tombstone)
      - length-prefixed keyspace bytes
      - length-prefixed key bytes
      - HLC components: wall_ms (8B BE) | counter (2B BE) | length-prefixed node_id
      - length-prefixed value (b"" for Tombstone)
    """
    h = hashlib.sha256()
    for record in records:
        h.update(_record_bytes(record))
    return h.hexdigest()


def _record_bytes(record: Record) -> bytes:
    """Serialise one record to a deterministic byte sequence for hashing."""
    parts: list[bytes] = []
    if isinstance(record, Version):
        parts.append(b"\x00")
        parts.extend(_store_key_parts(record.address))
        parts.extend(_hlc_parts(record.metadata))
        parts.append(_len_prefix(record.value))
    else:
        assert isinstance(record, Tombstone)
        parts.append(b"\x01")
        parts.extend(_store_key_parts(record.address))
        parts.extend(_hlc_parts(record.metadata))
        parts.append(_len_prefix(b""))
    return b"".join(parts)


def _store_key_parts(address: object) -> list[bytes]:
    """Return length-prefixed keyspace + key bytes."""
    from tourillon.core.structure.record import StoreKey

    assert isinstance(address, StoreKey)
    return [_len_prefix(address.keyspace), _len_prefix(address.key)]


def _hlc_parts(metadata: object) -> list[bytes]:
    """Return wall_ms (8B) | counter (2B) | length-prefixed node_id bytes."""
    from tourillon.core.structure.clock import HLCTimestamp

    assert isinstance(metadata, HLCTimestamp)
    node_id_bytes = metadata.node_id.encode("utf-8")
    return [
        struct.pack(">Q", metadata.wall_ms),
        struct.pack(">H", metadata.counter),
        _len_prefix(node_id_bytes),
    ]


def _len_prefix(data: bytes) -> bytes:
    """Prepend a 4-byte big-endian length to data."""
    return struct.pack(">I", len(data)) + data
