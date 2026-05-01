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
"""Kind constants and handler classes for node-to-node replication and handoff protocols."""

from tourillon.core.handlers.handoff import (
    KIND_HANDOFF_ACK,
    KIND_HANDOFF_DELEGATE,
    KIND_HANDOFF_DELEGATE_OK,
    KIND_HANDOFF_PUSH,
    HandoffHandlers,
)
from tourillon.core.handlers.replication import (
    KIND_KV_REPLICATE,
    KIND_KV_REPLICATE_OK,
    ReplicationHandlers,
)

__all__ = [
    "KIND_HANDOFF_ACK",
    "KIND_HANDOFF_DELEGATE",
    "KIND_HANDOFF_DELEGATE_OK",
    "KIND_HANDOFF_PUSH",
    "KIND_KV_REPLICATE",
    "KIND_KV_REPLICATE_OK",
    "HandoffHandlers",
    "ReplicationHandlers",
]
