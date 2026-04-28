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
"""Bootstrap — wires concrete adapters into a fully operational node."""

from tourillon.core.ports.storage import LocalStoragePort
from tourillon.infra.memory.store import MemoryStore


def create_memory_node(node_id: str) -> LocalStoragePort:
    """Assemble and return an in-memory node bound to the given identifier.

    This factory is the single point of composition for the in-memory adapter
    stack. It constructs a MemoryStore, which internally wires a MemoryLog and
    an HLCClock, and returns it typed as LocalStoragePort so that callers
    depend on the port contract rather than the concrete adapter. Replacing
    this function with one that wires a different adapter — for instance a
    persistent backend — is sufficient to switch the storage strategy without
    touching any caller.
    """
    return MemoryStore(node_id)
