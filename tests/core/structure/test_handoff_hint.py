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
"""Tests for tourillon.core.structure.handoff — Hint dataclass invariants."""

import dataclasses

import pytest

from tourillon.core.ports.storage import DeleteOp, WriteOp
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.handoff import Hint
from tourillon.core.structure.version import StoreKey


def _key() -> StoreKey:
    return StoreKey(keyspace=b"ks", key=b"k")


def _ts(wall: int = 1, counter: int = 0, node: str = "n") -> HLCTimestamp:
    return HLCTimestamp(wall=wall, counter=counter, node_id=node)


def _write_op() -> WriteOp:
    return WriteOp(address=_key(), value=b"v", now_ms=1)


def _delete_op() -> DeleteOp:
    return DeleteOp(address=_key(), now_ms=1)


def test_hint_valid_construction() -> None:
    hint = Hint(target_node_id="peer-1", ts=_ts(), op=_write_op(), kind="put")
    assert hint.target_node_id == "peer-1"
    assert hint.kind == "put"
    assert hint.ts == _ts()


def test_hint_empty_target_node_id_raises() -> None:
    with pytest.raises(ValueError):
        Hint(target_node_id="", ts=_ts(), op=_write_op(), kind="put")


def test_hint_invalid_kind_raises() -> None:
    with pytest.raises(ValueError):
        Hint(target_node_id="peer-1", ts=_ts(), op=_write_op(), kind="update")


def test_hint_valid_kind_put() -> None:
    hint = Hint(target_node_id="peer-1", ts=_ts(), op=_write_op(), kind="put")
    assert hint.kind == "put"


def test_hint_valid_kind_delete() -> None:
    hint = Hint(target_node_id="peer-1", ts=_ts(), op=_delete_op(), kind="delete")
    assert hint.kind == "delete"


def test_hint_is_frozen() -> None:
    hint = Hint(target_node_id="peer-1", ts=_ts(), op=_write_op(), kind="put")
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(hint, "kind", "delete")  # noqa: B010


def test_hint_equality_by_value() -> None:
    a = Hint(target_node_id="peer-1", ts=_ts(), op=_write_op(), kind="put")
    b = Hint(target_node_id="peer-1", ts=_ts(), op=_write_op(), kind="put")
    assert a == b
