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
"""Tests for tourillon.core.ports.replication."""

from tourillon.core.ports.replication import (
    QuorumNotReachedError,
    ReplicationPort,
)
from tourillon.core.ports.storage import DeleteOp, WriteOp
from tourillon.core.structure.version import Tombstone, Version


def test_quorum_not_reached_error_carries_fields() -> None:
    err = QuorumNotReachedError(acked=1, required=2, ack_node_ids=["n1"])
    assert err.acked == 1
    assert err.required == 2
    assert err.ack_node_ids == ["n1"]


def test_quorum_not_reached_error_message_format() -> None:
    err = QuorumNotReachedError(acked=1, required=2, ack_node_ids=["n1"])
    assert str(err) == "quorum not reached: 1/2 acks from ['n1']"


def test_replication_port_is_runtime_checkable() -> None:
    class _Impl:
        async def replicate_write(
            self,
            op: WriteOp,
            version: Version,
            preference_list: list[str],
        ) -> Version:
            return version

        async def replicate_delete(
            self,
            op: DeleteOp,
            tombstone: Tombstone,
            preference_list: list[str],
        ) -> Tombstone:
            return tombstone

    assert isinstance(_Impl(), ReplicationPort)


def test_replication_port_rejects_missing_methods() -> None:
    class _Bad:
        async def replicate_write(
            self,
            op: WriteOp,
            version: Version,
            preference_list: list[str],
        ) -> Version:
            return version

    assert not isinstance(_Bad(), ReplicationPort)
