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
"""Tests for tourillon.core.ring.vnode — VNode value object."""

import dataclasses

import pytest

from tourillon.core.ring.vnode import VNode


def test_vnode_valid_construction() -> None:
    """Constructing a VNode with valid fields must succeed."""
    vn = VNode(node_id="n1", token=42)
    assert vn.node_id == "n1"
    assert vn.token == 42


def test_vnode_zero_token_allowed() -> None:
    """Zero is a valid token value (lower bound of the hash space)."""
    vn = VNode(node_id="n1", token=0)
    assert vn.token == 0


def test_vnode_empty_node_id_raises() -> None:
    """Empty node_id must raise ValueError."""
    with pytest.raises(ValueError, match="node_id must not be empty"):
        VNode(node_id="", token=10)


def test_vnode_negative_token_raises() -> None:
    """Negative token must raise ValueError."""
    with pytest.raises(ValueError, match="token must be non-negative"):
        VNode(node_id="n1", token=-1)


def test_vnode_is_frozen() -> None:
    """VNode must be immutable — attribute assignment must raise."""
    vn = VNode(node_id="n1", token=100)
    with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
        vn.token = 999  # type: ignore[misc]


def test_vnode_equality_by_value() -> None:
    """Two VNodes with identical fields must be equal."""
    assert VNode(node_id="n1", token=10) == VNode(node_id="n1", token=10)


def test_vnode_inequality_different_token() -> None:
    """VNodes with different tokens must not be equal."""
    assert VNode(node_id="n1", token=10) != VNode(node_id="n1", token=20)


def test_vnode_inequality_different_node_id() -> None:
    """VNodes with different node_ids must not be equal."""
    assert VNode(node_id="n1", token=10) != VNode(node_id="n2", token=10)


def test_vnode_no_ordering_operators() -> None:
    """VNode must not expose __lt__ — ordering belongs to Ring."""
    vn = VNode(node_id="n1", token=10)
    assert (
        not hasattr(vn, "__lt__")
        or not callable(getattr(type(vn), "__lt__", None))
        or type(vn).__lt__ is object.__lt__
    )
