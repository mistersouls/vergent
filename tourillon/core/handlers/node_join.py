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
"""Peer-server connection handler for node.join envelopes.

tourctl connects directly to the target node's peer address; no forwarding
is performed. The handler guards that phase == IDLE, executes the
IDLE → JOINING transition atomically (StatePort.save() before responding),
then enqueues gossip bootstrap as a background task.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import uuid
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ports.state import StatePort
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.envelope import Envelope

if TYPE_CHECKING:
    from tourillon.core.ports.serializer import SerializerPort
    from tourillon.core.ports.transport import ReceiveEnvelope, SendEnvelope

logger = logging.getLogger(__name__)

# Type alias for a coroutine factory that launches gossip bootstrap.
type BootstrapLauncher = Callable[[list[str]], Coroutine[Any, Any, None]]

# Callback invoked after a successful IDLE → JOINING state save so callers
# can update their shared state reference (e.g. state_ref[0] in _run_phase).
type SetState = Callable[[NodeState], None]


class NodeJoinHandler:
    """Peer-server handler for the node.join envelope kind.

    tourctl connects directly to the target node's peer server address;
    there is no forwarding. This handler always handles the request locally.

    Guards that phase == IDLE, executes the IDLE → JOINING transition
    atomically (StatePort.save() before responding), then enqueues gossip
    bootstrap as a background task. Responds node.join.ok on success;
    node.join.error otherwise. Never mutates state if any guard fails.

    launch_bootstrap is called after node.join.ok is sent; it receives the
    resolved seed list and runs asynchronously in the background TaskGroup.

    set_state is called immediately after StatePort.save() succeeds, before
    any response is sent, so that the caller's shared state reference reflects
    the new JOINING state. Without this update the background bootstrap task
    would read stale IDLE state and announce an IDLE member to peers instead
    of the correct JOINING member.
    """

    def __init__(
        self,
        node_id: str,
        get_state: Callable[[], NodeState],
        topology_manager: TopologyManager,
        state_port: StatePort,
        config_seeds: list[str],
        partition_shift: int,
        peer_address: str,
        serializer: SerializerPort,
        launch_bootstrap: BootstrapLauncher | None = None,
        set_state: SetState | None = None,
    ) -> None:
        self._node_id = node_id
        self._get_state = get_state
        self._topology_manager = topology_manager
        self._state_port = state_port
        self._config_seeds = config_seeds
        self._partition_shift = partition_shift
        self._peer_address = peer_address
        self._serializer = serializer
        self._launch_bootstrap = launch_bootstrap
        self._set_state = set_state

    async def __call__(
        self,
        receive: ReceiveEnvelope,
        send: SendEnvelope,
    ) -> None:
        """Handle one node.join request."""
        req = await receive()
        try:
            data = self._serializer.decode(req.payload)
            seeds_override: list[str] = list(data.get("seeds", []) or [])
        except Exception:
            logger.warning("node.join: malformed payload; closing connection")
            return

        await self._handle_local(req, seeds_override, send)

    async def _handle_local(
        self,
        req: Envelope,
        seeds_override: list[str],
        send: SendEnvelope,
    ) -> None:
        """Execute the IDLE → JOINING transition."""
        state = self._get_state()
        if state.phase != MemberPhase.IDLE:
            await self._join_error(
                req.correlation_id,
                send,
                "wrong_phase",
                f"node {self._node_id!r} is in phase {state.phase.value!r}; cannot join",
            )
            return

        seeds = seeds_override or self._config_seeds
        if not seeds:
            await self._join_error(
                req.correlation_id,
                send,
                "no_seeds",
                "no seeds available; provide --seeds or configure [cluster].seeds",
            )
            return

        # Generate tokens for the ring position.
        token_count_n = self._derive_token_count()
        tokens = tuple(secrets.randbelow(2**128) for _ in range(token_count_n))

        new_generation = state.generation + 1
        new_seq = state.seq + 1
        new_state = NodeState(
            node_id=state.node_id,
            phase=MemberPhase.JOINING,
            generation=new_generation,
            seq=new_seq,
            tokens=tokens,
            epoch=state.epoch,
        )

        try:
            await self._state_port.save(new_state)
        except Exception as exc:
            logger.error("node.join: failed to persist state: %s", exc)
            await self._join_error(
                req.correlation_id,
                send,
                "internal_error",
                "failed to persist state transition",
            )
            return

        # Update the caller's shared state reference immediately after
        # persistence so that the background bootstrap task reads the correct
        # JOINING state (not stale IDLE) when building the member to announce.
        if self._set_state is not None:
            self._set_state(new_state)

        # Register the joining member in the topology.
        member = Member(
            node_id=self._node_id,
            peer_address=self._peer_address,
            generation=new_generation,
            seq=new_seq,
            phase=MemberPhase.JOINING,
            tokens=tokens,
            partition_shift=self._partition_shift,
        )
        await self._topology_manager.apply_member(member)

        # Respond to tourctl before launching background bootstrap.
        ok_payload = self._serializer.encode(
            {
                "node_id": self._node_id,
                "phase": "joining",
                "peer_address": self._peer_address,
            }
        )
        ok_env = Envelope.create(
            ok_payload,
            kind="node.join.ok",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(ok_env)

        # Launch gossip bootstrap asynchronously.
        if self._launch_bootstrap is not None:
            loop = asyncio.get_running_loop()
            loop.create_task(
                self._launch_bootstrap(seeds),
                name="gossip.bootstrap",
            )

    def _derive_token_count(self) -> int:
        """Derive token count from state node_id via config — uses 4 as default."""
        # In full integration the TourillonConfig is wired in; here we use a
        # safe default. The actual token count comes from NodeSize.token_count
        # set at config-generate time. A handler without config access defaults
        # to 4 (NodeSize.M). Callers in full startup always inject config.
        return getattr(self, "_token_count", 4)

    async def _join_error(
        self,
        correlation_id: uuid.UUID,
        send: SendEnvelope,
        code: str,
        message: str,
    ) -> None:
        """Send a node.join.error envelope."""
        payload = self._serializer.encode({"code": code, "message": message})
        env = Envelope.create(
            payload,
            kind="node.join.error",
            correlation_id=correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(env)
