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
"""Versioned value and deletion-marker value objects."""

from dataclasses import dataclass

from tourillon.core.structure.clock import HLCTimestamp


@dataclass(frozen=True)
class Version:
    """An immutable snapshot of a key's value at a specific causal instant.

    A Version records the key, the binary payload, and the HLC timestamp that
    orders this write relative to all other writes across the cluster. The
    metadata field is the authoritative ordering handle: when two nodes disagree
    about which write is newer for the same key, the Version with the greater
    metadata wins unconditionally. Callers must never compare Version instances
    by their value bytes; the metadata alone determines precedence. The value
    field is deliberately opaque binary so that the storage layer remains
    agnostic to the serialisation format chosen by callers.
    """

    key: str
    metadata: HLCTimestamp
    value: bytes


@dataclass(frozen=True)
class Tombstone:
    """An immutable deletion marker that causally supersedes earlier Versions.

    A Tombstone is produced by a delete operation and carries the same HLC
    metadata as a Version so that it participates in the same total order. A
    Tombstone whose metadata is greater than all existing Versions for a key
    makes that key invisible to readers. Like Version, ordering is always
    resolved through metadata comparison. The absence of a value field is
    intentional: a Tombstone carries no payload, and its sole semantic purpose
    is to record that a deletion happened at a specific causal instant.
    """

    key: str
    metadata: HLCTimestamp
