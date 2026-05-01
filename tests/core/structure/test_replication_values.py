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
"""Tests for tourillon.core.structure.replication value objects."""

import dataclasses

import pytest

from tourillon.core.ports.storage import WriteOp
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.replication import (
    QuorumOutcome,
    ReplicaAck,
    ReplicationRequest,
)
from tourillon.core.structure.version import StoreKey


def _ts(wall: int = 1, counter: int = 0, node: str = "n") -> HLCTimestamp:
    return HLCTimestamp(wall=wall, counter=counter, node_id=node)


def _write_op() -> WriteOp:
    return WriteOp(address=StoreKey(keyspace=b"ks", key=b"k"), value=b"v", now_ms=1)


def test_replica_ack_valid_construction() -> None:
    ack = ReplicaAck(node_id="n1", ts=_ts())
    assert ack.node_id == "n1"
    assert ack.ts == _ts()


def test_replica_ack_empty_node_id_raises() -> None:
    with pytest.raises(ValueError):
        ReplicaAck(node_id="", ts=_ts())


def test_replica_ack_invalid_ts_type_raises() -> None:
    with pytest.raises(TypeError):
        ReplicaAck(node_id="n1", ts="not-a-ts")  # type: ignore[arg-type]


def test_replica_ack_is_frozen() -> None:
    ack = ReplicaAck(node_id="n1", ts=_ts())
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(ack, "node_id", "n2")  # noqa: B010


def test_replica_ack_equality_by_value() -> None:
    a = ReplicaAck(node_id="n1", ts=_ts())
    b = ReplicaAck(node_id="n1", ts=_ts())
    assert a == b
    assert hash(a) == hash(b)


def test_quorum_outcome_valid_construction() -> None:
    ack = ReplicaAck(node_id="n1", ts=_ts())
    outcome = QuorumOutcome(success=True, acked=(ack,), hinted=("n3",))
    assert outcome.success is True
    assert outcome.acked == (ack,)
    assert outcome.hinted == ("n3",)


def test_quorum_outcome_is_frozen() -> None:
    outcome = QuorumOutcome(success=False, acked=(), hinted=())
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(outcome, "success", True)  # noqa: B010


def test_replication_request_valid_construction() -> None:
    req = ReplicationRequest(
        op=_write_op(),
        kind="put",
        ts=_ts(),
        preference_list=("n1", "n2", "n3"),
    )
    assert req.preference_list == ("n1", "n2", "n3")
    assert req.kind == "put"


def test_replication_request_coerces_list_to_tuple() -> None:
    req = ReplicationRequest(
        op=_write_op(),
        kind="put",
        ts=_ts(),
        preference_list=["n1", "n2"],  # type: ignore[arg-type]
    )
    assert isinstance(req.preference_list, tuple)
    assert req.preference_list == ("n1", "n2")


def test_replication_request_invalid_kind_raises() -> None:
    with pytest.raises(ValueError):
        ReplicationRequest(
            op=_write_op(),
            kind="update",  # type: ignore[arg-type]
            ts=_ts(),
            preference_list=("n1",),
        )


def test_replication_request_invalid_ts_type_raises() -> None:
    with pytest.raises(TypeError):
        ReplicationRequest(
            op=_write_op(),
            kind="put",
            ts="not-a-ts",  # type: ignore[arg-type]
            preference_list=("n1",),
        )


def test_replication_request_invalid_preference_list_type_raises() -> None:
    with pytest.raises(TypeError):
        ReplicationRequest(
            op=_write_op(),
            kind="put",
            ts=_ts(),
            preference_list="n1",  # type: ignore[arg-type]
        )


def test_replication_request_empty_node_id_in_preference_list_raises() -> None:
    with pytest.raises(ValueError):
        ReplicationRequest(
            op=_write_op(),
            kind="put",
            ts=_ts(),
            preference_list=("n1", ""),
        )


def test_replication_request_empty_preference_list_valid() -> None:
    req = ReplicationRequest(
        op=_write_op(),
        kind="put",
        ts=_ts(),
        preference_list=(),
    )
    assert req.preference_list == ()


def test_replication_request_is_frozen() -> None:
    req = ReplicationRequest(
        op=_write_op(),
        kind="put",
        ts=_ts(),
        preference_list=("n1",),
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(req, "kind", "delete")  # noqa: B010
