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
"""StoreKey, Version, Tombstone — typed storage record model."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from tourillon.core.structure.clock import HLCTimestamp

type Record = Version | Tombstone


@dataclass(frozen=True)
class StoreKey:
    """Canonical addressing unit: logical keyspace + record key, both raw bytes.

    keyspace and key are encoding-agnostic raw bytes. Callers responsible for
    human-readable names encode to bytes before constructing a StoreKey,
    keeping the encoding decision at the boundary.
    """

    keyspace: bytes
    key: bytes

    def to_dict(self) -> dict[str, object]:
        """Return a wire-compatible dict with base64-free bytes fields."""
        return {"keyspace": self.keyspace, "key": self.key}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StoreKey:
        """Reconstruct a StoreKey from a wire dict."""
        ks = data["keyspace"]
        k = data["key"]
        return cls(
            keyspace=bytes(ks) if not isinstance(ks, bytes) else ks,
            key=bytes(k) if not isinstance(k, bytes) else k,
        )


@dataclass(frozen=True)
class Version:
    """Immutable snapshot of a key's value at a specific causal instant.

    metadata carries the HLC ordering handle. Value is the raw record bytes;
    callers determine encoding. Never compare records by value bytes for
    ordering; always use metadata.
    """

    address: StoreKey
    metadata: HLCTimestamp
    value: bytes

    def to_dict(self) -> dict[str, object]:
        """Return a kind-discriminated wire dict for msgpack serialisation."""
        return {
            "kind": "version",
            "address": self.address.to_dict(),
            "metadata": self.metadata.to_dict(),
            "value": self.value,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Version:
        """Reconstruct a Version from its wire dict representation."""
        v = data["value"]
        return cls(
            address=StoreKey.from_dict(data["address"]),
            metadata=HLCTimestamp.from_dict(data["metadata"]),
            value=bytes(v) if not isinstance(v, bytes) else v,
        )


@dataclass(frozen=True)
class Tombstone:
    """Deletion marker that causally supersedes earlier Versions.

    A Tombstone has no value field. Its presence in DBI_DATA signals that the
    key was deleted at the HLC instant encoded in metadata. kv.get returns a
    Tombstone when the last write to a key was a delete.
    """

    address: StoreKey
    metadata: HLCTimestamp

    def to_dict(self) -> dict[str, object]:
        """Return a kind-discriminated wire dict for msgpack serialisation."""
        return {
            "kind": "tombstone",
            "address": self.address.to_dict(),
            "metadata": self.metadata.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Tombstone:
        """Reconstruct a Tombstone from its wire dict representation."""
        return cls(
            address=StoreKey.from_dict(data["address"]),
            metadata=HLCTimestamp.from_dict(data["metadata"]),
        )


def record_from_dict(data: dict[str, Any]) -> Record:
    """Reconstruct a Version or Tombstone from its kind-discriminated dict.

    Raise ValueError when the kind field is absent or unrecognised.
    """
    kind = data.get("kind")
    if kind == "version":
        return Version.from_dict(data)
    if kind == "tombstone":
        return Tombstone.from_dict(data)
    raise ValueError(f"Unknown record kind: {kind!r}")
