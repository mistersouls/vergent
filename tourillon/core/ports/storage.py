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
"""Driving port and operation objects for the key-value service contract."""

from dataclasses import dataclass
from typing import Protocol

from tourillon.core.structure.version import StoreKey, Tombstone, Version


@dataclass(frozen=True)
class WriteOp:
    """All inputs required to record a new value for an address.

    WriteOp bundles the target address, the binary payload, and the physical
    clock hint into a single immutable object. The store uses now_ms together
    with its internal HLC to derive a deterministic causal timestamp; callers
    must not assume that now_ms alone determines the ordering of the returned
    Version.

    Additional fields — such as a consistency level, a TTL, or a
    pre-resolved partition_id supplied by the router — can be added here in
    future milestones without changing the LocalStoragePort method signatures.
    """

    address: StoreKey
    value: bytes
    now_ms: int


@dataclass(frozen=True)
class ReadOp:
    """All inputs required to read the current visible value for an address.

    ReadOp carries only the target address because a read requires no clock
    hint and no payload. It serves as the stable, extensible argument type
    for LocalStoragePort.get, leaving room for future fields such as a
    consistency level or a read timestamp bound.
    """

    address: StoreKey


@dataclass(frozen=True)
class DeleteOp:
    """All inputs required to tombstone an address.

    DeleteOp bundles the target address and the physical clock hint. Like
    WriteOp, it accepts now_ms as a hint that the store advances through its
    HLC to guarantee that the resulting Tombstone causally follows all prior
    writes on this node. A partition_id field may be added here when the ring
    layer is introduced in Milestone 2.
    """

    address: StoreKey
    now_ms: int


class LocalStoragePort(Protocol):
    """Driving port through which all callers interact with the local key-value store.

    LocalStoragePort is the primary boundary of the core hexagon. Every read
    or write directed at this node must go through this interface, keeping
    callers completely decoupled from the underlying storage strategy. Each
    method receives a dedicated operation object — WriteOp, ReadOp, or
    DeleteOp — rather than a flat argument list, so that the contract
    remains stable as new fields are introduced on the operation types.

    All methods are async because persistence and, in future milestones, local
    durability guarantees will involve I/O. An asynchronous interface from the
    start prevents a breaking API change when the backing store evolves.

    The HLC timestamp assigned by the store is always strictly greater than
    any timestamp previously issued by this node. Callers must not assume
    that the now_ms hint they supply in a WriteOp or DeleteOp alone determines
    the ordering of the returned Version or Tombstone.
    """

    async def put(self, op: WriteOp) -> Version:
        """Record a new value and return the stamped Version.

        The returned Version carries the HLC timestamp assigned by the store,
        which is strictly greater than any timestamp previously issued by this
        node.
        """
        ...

    async def get(self, op: ReadOp) -> list[Version]:
        """Return the current visible value, or an empty list if absent.

        The list contains at most one element. It is empty when the address
        has never been written or when its most recent causal event is a
        Tombstone.
        """
        ...

    async def delete(self, op: DeleteOp) -> Tombstone:
        """Record a deletion and return the stamped Tombstone.

        The Tombstone's metadata is strictly greater than any previously
        issued timestamp on this node, guaranteeing that the deletion causally
        follows all earlier writes.
        """
        ...
