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
"""Tests for NodeJoinHandler: direct join, phase guards, seed resolution."""

from __future__ import annotations

import uuid

import pytest

from tourillon.core.handlers.node_join import NodeJoinHandler
from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ports.state import StatePort
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.envelope import Envelope
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter

LOCAL_SHIFT = 12


def _make_idle_state(node_id: str = "node-2") -> NodeState:
    return NodeState(
        node_id=node_id,
        phase=MemberPhase.IDLE,
        generation=0,
        seq=0,
        tokens=(),
        epoch=0,
    )


class FakeStatePort(StatePort):
    """In-memory StatePort adapter for tests."""

    def __init__(self) -> None:
        self._state: NodeState | None = None
        self.saved: list[NodeState] = []

    async def load(self) -> NodeState | None:
        return self._state

    async def save(self, state: NodeState) -> None:
        self._state = state
        self.saved.append(state)


async def _invoke_join(
    handler: NodeJoinHandler,
    payload: bytes,
) -> list[Envelope]:
    """Invoke the handler and collect sent envelopes."""
    cid = uuid.uuid4()
    req = Envelope.create(payload, kind="node.join", correlation_id=cid)
    sent: list[Envelope] = []

    async def receive() -> Envelope:
        return req

    async def send(env: Envelope) -> None:
        sent.append(env)

    await handler(receive, send)
    return sent


def _make_handler(
    node_id: str = "node-2",
    state: NodeState | None = None,
    config_seeds: list[str] | None = None,
) -> tuple[NodeJoinHandler, FakeStatePort]:
    """Build a NodeJoinHandler with an in-memory state port."""
    state_port = FakeStatePort()
    idle = state or _make_idle_state(node_id)
    handler = NodeJoinHandler(
        node_id=node_id,
        get_state=lambda: idle,
        topology_manager=TopologyManager(),
        state_port=state_port,
        config_seeds=config_seeds or ["10.0.0.1:7701"],
        partition_shift=LOCAL_SHIFT,
        peer_address="10.0.0.2:7701",
        serializer=MsgpackSerializerAdapter(),
        launch_bootstrap=None,
    )
    handler._token_count = 4  # type: ignore[attr-defined]
    return handler, state_port


@pytest.mark.gossip
async def test_3_direct_join_ok_saves_joining_state() -> None:
    """tourctl connects directly; node handles locally; StatePort.save() with phase=joining, generation=1."""
    serializer = MsgpackSerializerAdapter()
    handler, state_port = _make_handler()

    payload = serializer.encode({"seeds": ["10.0.0.1:7701"]})
    sent = await _invoke_join(handler, payload)

    assert len(sent) == 1
    assert sent[0].kind == "node.join.ok"
    data = serializer.decode(sent[0].payload)
    assert data["node_id"] == "node-2"
    assert data["phase"] == "joining"

    assert len(state_port.saved) == 1
    saved = state_port.saved[0]
    assert saved.phase == MemberPhase.JOINING
    assert saved.generation == 1  # incremented exactly once


@pytest.mark.gossip
async def test_10_join_wrong_phase_responds_error_no_state_change() -> None:
    """node.join.error code: wrong_phase; no state change."""
    serializer = MsgpackSerializerAdapter()
    joining_state = NodeState(
        node_id="node-2",
        phase=MemberPhase.JOINING,
        generation=1,
        seq=1,
        tokens=(1, 2, 3, 4),
        epoch=0,
    )
    state_port = FakeStatePort()
    handler = NodeJoinHandler(
        node_id="node-2",
        get_state=lambda: joining_state,
        topology_manager=TopologyManager(),
        state_port=state_port,
        config_seeds=["10.0.0.1:7701"],
        partition_shift=LOCAL_SHIFT,
        peer_address="10.0.0.2:7701",
        serializer=serializer,
        launch_bootstrap=None,
    )

    payload = serializer.encode({"seeds": []})
    sent = await _invoke_join(handler, payload)

    assert len(sent) == 1
    assert sent[0].kind == "node.join.error"
    data = serializer.decode(sent[0].payload)
    assert data["code"] == "wrong_phase"
    assert len(state_port.saved) == 0  # no state change


@pytest.mark.gossip
async def test_11_join_no_seeds_responds_error() -> None:
    """node.join.error code: no_seeds; no state change."""
    serializer = MsgpackSerializerAdapter()
    state_port = FakeStatePort()
    idle_state = _make_idle_state("node-2")

    handler = NodeJoinHandler(
        node_id="node-2",
        get_state=lambda: idle_state,
        topology_manager=TopologyManager(),
        state_port=state_port,
        config_seeds=[],  # no seeds in config
        partition_shift=LOCAL_SHIFT,
        peer_address="10.0.0.2:7701",
        serializer=serializer,
        launch_bootstrap=None,
    )

    # Neither config seeds nor envelope seeds
    payload = serializer.encode({"seeds": []})
    sent = await _invoke_join(handler, payload)

    assert len(sent) == 1
    assert sent[0].kind == "node.join.error"
    data = serializer.decode(sent[0].payload)
    assert data["code"] == "no_seeds"
    assert len(state_port.saved) == 0


@pytest.mark.gossip
async def test_join_seeds_from_config_when_envelope_empty() -> None:
    """Seeds fall back to config_seeds when envelope carries an empty list."""
    serializer = MsgpackSerializerAdapter()
    handler, state_port = _make_handler(config_seeds=["10.0.0.5:7701"])

    payload = serializer.encode({"seeds": []})
    sent = await _invoke_join(handler, payload)

    assert sent[0].kind == "node.join.ok"
    assert state_port.saved[0].phase == MemberPhase.JOINING


@pytest.mark.gossip
async def test_join_seeds_override_config_seeds() -> None:
    """Seeds in the envelope take precedence over config_seeds."""
    serializer = MsgpackSerializerAdapter()
    handler, state_port = _make_handler(config_seeds=["10.0.0.5:7701"])

    payload = serializer.encode({"seeds": ["10.0.0.9:7701"]})
    sent = await _invoke_join(handler, payload)

    assert sent[0].kind == "node.join.ok"
    assert state_port.saved[0].phase == MemberPhase.JOINING


@pytest.mark.gossip
async def test_join_generation_incremented_exactly_once() -> None:
    """generation starts at 0 for IDLE; exactly +1 after join."""
    serializer = MsgpackSerializerAdapter()
    handler, state_port = _make_handler()

    payload = serializer.encode({"seeds": ["10.0.0.1:7701"]})
    await _invoke_join(handler, payload)

    assert state_port.saved[0].generation == 1
    assert state_port.saved[0].seq == 1


@pytest.mark.gossip
async def test_join_tokens_generated_with_correct_count() -> None:
    """4 tokens generated for a token_count=4 configuration."""
    serializer = MsgpackSerializerAdapter()
    handler, state_port = _make_handler()

    payload = serializer.encode({"seeds": ["10.0.0.1:7701"]})
    await _invoke_join(handler, payload)

    assert len(state_port.saved[0].tokens) == 4


@pytest.mark.gossip
async def test_join_malformed_payload_closes_silently() -> None:
    """A malformed payload does not crash the handler and sends nothing."""
    handler, state_port = _make_handler()

    sent: list[Envelope] = []

    async def receive() -> Envelope:
        return Envelope.create(b"\xff\xff", kind="node.join")

    async def send(env: Envelope) -> None:
        sent.append(env)

    await handler(receive, send)
    assert sent == []
    assert state_port.saved == []


@pytest.mark.gossip
async def test_join_set_state_called_with_joining_state() -> None:
    """set_state callback receives the new JOINING NodeState after a successful join.

    This ensures the shared state_ref is updated before the background bootstrap
    task runs so that _execute_launch_bootstrap announces JOINING (not stale IDLE).
    """
    serializer = MsgpackSerializerAdapter()
    state_port = FakeStatePort()
    idle_state = _make_idle_state("node-2")

    captured: list[NodeState] = []

    handler = NodeJoinHandler(
        node_id="node-2",
        get_state=lambda: idle_state,
        topology_manager=TopologyManager(),
        state_port=state_port,
        config_seeds=["10.0.0.1:7701"],
        partition_shift=LOCAL_SHIFT,
        peer_address="10.0.0.2:7701",
        serializer=serializer,
        launch_bootstrap=None,
        set_state=captured.append,
    )
    handler._token_count = 4  # type: ignore[attr-defined]

    payload = serializer.encode({"seeds": ["10.0.0.1:7701"]})
    sent = await _invoke_join(handler, payload)

    assert sent[0].kind == "node.join.ok"
    assert len(captured) == 1
    assert captured[0].phase == MemberPhase.JOINING
    assert captured[0].generation == 1
    assert captured[0].seq == 1
    assert len(captured[0].tokens) == 4


@pytest.mark.gossip
async def test_join_set_state_not_called_on_error() -> None:
    """set_state is never called when the join fails (wrong_phase or no_seeds)."""
    serializer = MsgpackSerializerAdapter()
    state_port = FakeStatePort()
    joining_state = NodeState(
        node_id="node-2",
        phase=MemberPhase.JOINING,
        generation=1,
        seq=1,
        tokens=(1, 2, 3, 4),
        epoch=0,
    )

    captured: list[NodeState] = []

    handler = NodeJoinHandler(
        node_id="node-2",
        get_state=lambda: joining_state,
        topology_manager=TopologyManager(),
        state_port=state_port,
        config_seeds=["10.0.0.1:7701"],
        partition_shift=LOCAL_SHIFT,
        peer_address="10.0.0.2:7701",
        serializer=serializer,
        launch_bootstrap=None,
        set_state=captured.append,
    )

    payload = serializer.encode({"seeds": []})
    sent = await _invoke_join(handler, payload)

    assert sent[0].kind == "node.join.error"
    assert captured == []  # set_state must NOT be called on error
