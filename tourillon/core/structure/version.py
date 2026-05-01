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
"""Addressing unit, versioned value, and deletion-marker value objects."""

import struct
from dataclasses import dataclass
from typing import Self, cast

from tourillon.core.structure.clock import HLCTimestamp

_STOREKEY_HDR_FMT: str = "!HH"
_STOREKEY_HDR_SIZE: int = struct.calcsize(_STOREKEY_HDR_FMT)


@dataclass(frozen=True)
class StoreKey:
    """Canonical addressing unit that pairs a keyspace with a record key.

    A StoreKey is the unit of identity used throughout every layer of
    Tourillon: storage operations, Version and Tombstone records, log entries,
    and routing decisions all carry a StoreKey rather than loose arguments.
    This prevents caller confusion about argument ordering and allows future
    fields — such as a partition token — to be added without changing method
    signatures.

    Both fields are raw bytes so that the storage and transport layers remain
    agnostic to any higher-level encoding chosen by callers. Callers that work
    with human-readable names are responsible for encoding them to bytes before
    constructing a StoreKey.

    The keyspace is a client-supplied logical scope analogous to a Kubernetes
    namespace. It provides isolation between datasets and participates in the
    consistent-hash computation that determines ring ownership. There is no
    implicit or default keyspace: callers must always supply one explicitly.

    Both keyspace and key must be non-empty. Attempting to construct a StoreKey
    with an empty field raises ValueError immediately, preventing invalid
    addresses from propagating into the log or store.

    The encode method serialises a StoreKey to a compact wire representation
    and decode reconstructs it. The binary layout is:

        offset  bytes  field
        0       2      keyspace_len  (uint16 big-endian)
        2       2      key_len       (uint16 big-endian)
        4       N      keyspace      (N = keyspace_len)
        4+N     M      key           (M = key_len)
    """

    keyspace: bytes
    key: bytes

    def __post_init__(self) -> None:
        """Enforce that both keyspace and key are non-empty.

        Raise ValueError with a descriptive message for whichever field
        violates the constraint so that callers can identify the problem
        without inspecting the full object.
        """
        if not self.keyspace:
            raise ValueError("keyspace must not be empty")
        if not self.key:
            raise ValueError("key must not be empty")

    def encode(self) -> bytes:
        """Serialise this StoreKey into its canonical wire representation."""
        header = struct.pack(_STOREKEY_HDR_FMT, len(self.keyspace), len(self.key))
        return header + self.keyspace + self.key

    @classmethod
    def decode(cls, data: bytes) -> Self:
        """Deserialise a wire frame back into a StoreKey.

        Raise ValueError when the frame is shorter than the fixed header, when
        either declared length is zero, or when the total declared length
        exceeds the available bytes.
        """
        if len(data) < _STOREKEY_HDR_SIZE:
            raise ValueError(
                f"StoreKey frame too short: got {len(data)} bytes,"
                f" need at least {_STOREKEY_HDR_SIZE}"
            )
        ks_len, k_len = struct.unpack_from(_STOREKEY_HDR_FMT, data)
        if ks_len == 0:
            raise ValueError("keyspace_len is zero: keyspace must not be empty")
        if k_len == 0:
            raise ValueError("key_len is zero: key must not be empty")
        total = _STOREKEY_HDR_SIZE + ks_len + k_len
        if len(data) < total:
            raise ValueError(
                f"truncated StoreKey frame: need {total} bytes, got {len(data)}"
            )
        keyspace = data[_STOREKEY_HDR_SIZE : _STOREKEY_HDR_SIZE + ks_len]
        key = data[_STOREKEY_HDR_SIZE + ks_len : total]
        return cls(keyspace=keyspace, key=key)

    def to_dict(self) -> dict[str, bytes]:
        """Serialise this key into a binary-transparent dict for wire encoding.

        The returned dict carries the raw bytes of keyspace and key without any
        re-encoding, so that binary-transparent serialisers such as msgpack can
        round-trip the fields losslessly. Callers that need a text-safe format
        are responsible for hex-encoding the fields before further processing.
        """
        return {"keyspace": self.keyspace, "key": self.key}

    @classmethod
    def from_dict(cls, data: dict[str, bytes]) -> Self:
        """Reconstruct a StoreKey from its dict representation.

        The dict must carry bytes values for both keyspace and key, as produced
        by to_dict. Passing string or other non-bytes values raises TypeError
        inside the dataclass __post_init__ validation.
        """
        return cls(keyspace=data["keyspace"], key=data["key"])


@dataclass(frozen=True)
class Version:
    """An immutable snapshot of a key's value at a specific causal instant.

    A Version records the full address of the record, the binary payload, and
    the HLC timestamp that orders this write relative to all other writes
    across the cluster. The metadata field is the authoritative ordering
    handle: when two nodes disagree about which write is newer for the same
    address, the Version with the greater metadata wins unconditionally.
    Callers must never compare Version instances by their value bytes; the
    metadata alone determines precedence. The value field is deliberately
    opaque binary so that the storage layer remains agnostic to the
    serialisation format chosen by callers.
    """

    address: StoreKey
    metadata: HLCTimestamp
    value: bytes

    def to_dict(self) -> dict[str, object]:
        """Serialise this version into a kind-discriminated dict for wire encoding.

        The kind field is always "version", distinguishing this dict from the
        output of Tombstone.to_dict when both are stored in a heterogeneous
        collection. Nested address and metadata fields are serialised by
        delegating to their own to_dict methods.
        """
        return {
            "kind": "version",
            "address": self.address.to_dict(),
            "metadata": self.metadata.to_dict(),
            "value": self.value,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> Self:
        """Reconstruct a Version from its kind-discriminated dict.

        The dict must have been produced by to_dict or an equivalent encoder.
        Nested address and metadata are reconstructed by delegating to their
        own from_dict class methods.
        """
        address = StoreKey.from_dict(cast(dict[str, bytes], data["address"]))
        metadata = HLCTimestamp.from_dict(cast(dict[str, int | str], data["metadata"]))
        return cls(address=address, metadata=metadata, value=cast(bytes, data["value"]))


@dataclass(frozen=True)
class Tombstone:
    """An immutable deletion marker that causally supersedes earlier Versions.

    A Tombstone is produced by a delete operation and carries the same HLC
    metadata as a Version so that it participates in the same total order. A
    Tombstone whose metadata is greater than all existing Versions for an
    address makes that address invisible to readers. Like Version, ordering is
    always resolved through metadata comparison. The absence of a value field
    is intentional: a Tombstone carries no payload, and its sole semantic
    purpose is to record that a deletion happened at a specific causal instant.
    """

    address: StoreKey
    metadata: HLCTimestamp

    def to_dict(self) -> dict[str, object]:
        """Serialise this tombstone into a kind-discriminated dict for wire encoding.

        The kind field is always "tombstone", distinguishing this dict from
        the output of Version.to_dict. There is no value field.
        """
        return {
            "kind": "tombstone",
            "address": self.address.to_dict(),
            "metadata": self.metadata.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> Self:
        """Reconstruct a Tombstone from its kind-discriminated dict."""
        address = StoreKey.from_dict(cast(dict[str, bytes], data["address"]))
        metadata = HLCTimestamp.from_dict(cast(dict[str, int | str], data["metadata"]))
        return cls(address=address, metadata=metadata)


def record_from_dict(data: dict[str, object]) -> Version | Tombstone:
    """Reconstruct a Version or Tombstone from its kind-discriminated dict.

    The kind field produced by Version.to_dict or Tombstone.to_dict determines
    which concrete type is instantiated. Raises ValueError when the kind is
    unrecognised. This free function is the single dispatch point for callers
    that hold a heterogeneous list of record dicts, such as TransferBatch.from_dict.
    """
    kind = str(data["kind"])
    if kind == "version":
        return Version.from_dict(data)
    if kind == "tombstone":
        return Tombstone.from_dict(data)
    raise ValueError(f"unknown record kind: {kind!r}")
