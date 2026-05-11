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
"""Tests for startup integrity checks: node_id consistency and tokens/size coherence."""

from __future__ import annotations

import pytest

from tourillon.core.lifecycle.checks import (
    NodeIdMismatchError,
    check_node_id_consistency,
    check_tokens_coherence,
)
from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.structure.config import NodeSize


@pytest.mark.gossip
def test_36_node_id_mismatch_config_vs_state_raises() -> None:
    """Process logs ERROR with both ids; exits with code 1; no socket bound."""
    with pytest.raises(NodeIdMismatchError):
        check_node_id_consistency("node-2", "node-1")


@pytest.mark.gossip
def test_37_node_id_match_config_vs_state_passes() -> None:
    """node_id check passes; startup continues normally."""
    check_node_id_consistency("node-2", "node-2")  # must not raise


@pytest.mark.gossip
def test_38_tokens_size_mismatch_ready_returns_false() -> None:
    """ERROR logged; seq incremented; state written with phase=FAILED; peer server bound; KV NOT bound."""
    # NodeSize M expects 4 tokens; provide 8
    node_size = NodeSize("M")
    tokens = tuple(range(8))
    result = check_tokens_coherence(MemberPhase.READY, tokens, node_size)
    assert result is False


@pytest.mark.gossip
def test_38b_tokens_size_mismatch_joining_returns_false() -> None:
    """Tokens/size coherence check fails for JOINING phase."""
    node_size = NodeSize("M")
    tokens = tuple(range(8))  # wrong count
    result = check_tokens_coherence(MemberPhase.JOINING, tokens, node_size)
    assert result is False


@pytest.mark.gossip
def test_38c_tokens_size_mismatch_draining_returns_false() -> None:
    """Tokens/size coherence check fails for DRAINING phase."""
    node_size = NodeSize("M")
    tokens = tuple(range(8))
    result = check_tokens_coherence(MemberPhase.DRAINING, tokens, node_size)
    assert result is False


@pytest.mark.gossip
def test_38d_tokens_size_correct_ready_returns_true() -> None:
    """Correct token count for READY phase passes."""
    node_size = NodeSize("M")
    tokens = tuple(range(node_size.token_count))
    result = check_tokens_coherence(MemberPhase.READY, tokens, node_size)
    assert result is True


@pytest.mark.gossip
def test_39_tokens_size_mismatch_idle_skipped_returns_true() -> None:
    """Tokens/size check skipped for IDLE phase; node starts normally in IDLE."""
    node_size = NodeSize("M")
    tokens = ()  # IDLE has no tokens — exempt
    result = check_tokens_coherence(MemberPhase.IDLE, tokens, node_size)
    assert result is True


@pytest.mark.gossip
def test_40_tokens_size_mismatch_paused_skipped_returns_true() -> None:
    """Tokens/size check skipped; node starts inert in PAUSED."""
    node_size = NodeSize("M")
    tokens = tuple(range(8))  # wrong count but exempt
    result = check_tokens_coherence(MemberPhase.PAUSED, tokens, node_size)
    assert result is True


@pytest.mark.gossip
def test_41_tokens_size_mismatch_failed_skipped_returns_true() -> None:
    """Tokens/size check skipped; node remains FAILED."""
    node_size = NodeSize("M")
    tokens = tuple(range(8))
    result = check_tokens_coherence(MemberPhase.FAILED, tokens, node_size)
    assert result is True
