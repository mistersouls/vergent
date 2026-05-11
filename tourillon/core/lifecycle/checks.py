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
"""Startup integrity checks for node_id consistency and tokens/size coherence."""

from __future__ import annotations

from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.structure.config import NodeSize

# Phases that require the tokens/size coherence check.
_TOKEN_CHECK_PHASES = frozenset(
    {MemberPhase.JOINING, MemberPhase.READY, MemberPhase.DRAINING}
)


class NodeIdMismatchError(Exception):
    """Raised by check_node_id_consistency when config and state node_ids differ.

    The caller must exit with code 1 immediately. No socket is bound and no
    lock is held beyond this check.
    """


def check_node_id_consistency(config_node_id: str, state_node_id: str) -> None:
    """Raise NodeIdMismatchError if config and state node_ids differ.

    Called during startup before any socket is bound. Never returns normally
    on mismatch — the caller must exit with code 1.
    """
    if config_node_id != state_node_id:
        raise NodeIdMismatchError(
            f"node_id mismatch: config={config_node_id!r} state={state_node_id!r}. "
            "This data_dir belongs to a different node. Check your config.toml "
            "or point data_dir at the correct directory."
        )


def check_tokens_coherence(
    phase: MemberPhase,
    tokens: tuple[int, ...],
    node_size: NodeSize,
) -> bool:
    """Return True when the tokens/size invariant is satisfied for the phase.

    Returns False (caller must transition to FAILED) when all of the following
    hold: phase ∈ {JOINING, READY, DRAINING} and len(tokens) != node_size.token_count.
    Always returns True for IDLE, PAUSED, and FAILED phases (exempt).
    """
    if phase not in _TOKEN_CHECK_PHASES:
        return True
    return len(tokens) == node_size.token_count
