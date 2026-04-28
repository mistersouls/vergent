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
"""Driving port that defines the key-value service contract for the core hexagon."""

from typing import Protocol

from tourillon.core.structure.version import Tombstone, Version


class LocalStoragePort(Protocol):
    """Driving port through which all callers interact with the local key-value store.

    LocalStoragePort is the primary boundary of the core hexagon. Every read or
    write directed at this node must go through this interface, keeping callers
    completely decoupled from the underlying storage strategy. The contract is
    intentionally narrow: it exposes only the three operations that a node-local
    store needs to fulfil, deferring replication, routing, and ring membership
    to higher layers.

    All methods are async because persistence and, in future milestones, local
    durability guarantees will involve I/O. An asynchronous interface from the
    start prevents a breaking API change when the backing store evolves.

    Callers supply now_ms as a physical-clock hint that the implementation
    combines with its internal HLC to derive a deterministic causal timestamp.
    Callers must not assume that now_ms alone determines the ordering of the
    returned Version or Tombstone; the HLC may advance the timestamp further to
    preserve monotonicity.
    """

    async def put(self, key: str, value: bytes, now_ms: int) -> Version:
        """Record a new value for key and return the stamped Version.

        The returned Version carries the HLC timestamp assigned by the store,
        which is strictly greater than any timestamp previously issued by this
        node.
        """
        ...

    async def get(self, key: str) -> list[Version]:
        """Return the current visible value for key, or an empty list if absent.

        The list contains at most one element. It is empty when the key has
        never been written or when its most recent causal event is a Tombstone.
        """
        ...

    async def delete(self, key: str, now_ms: int) -> Tombstone:
        """Record a deletion for key and return the stamped Tombstone.

        The Tombstone's metadata is strictly greater than any previously issued
        timestamp on this node, guaranteeing that the deletion causally follows
        all earlier writes.
        """
        ...
