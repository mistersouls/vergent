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
"""Tests for startup phase dispatch logic and socket lifecycle per phase."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ring.topology import TopologyManager


def _make_state(
    phase: MemberPhase,
    node_id: str = "node-2",
    generation: int = 1,
    seq: int = 5,
    tokens: tuple[int, ...] | None = None,
    epoch: int = 1,
) -> NodeState:
    if tokens is None:
        tokens = () if phase == MemberPhase.IDLE else tuple(range(4))
    return NodeState(
        node_id=node_id,
        phase=phase,
        generation=generation,
        seq=seq,
        tokens=tokens,
        epoch=epoch,
    )


@pytest.mark.gossip
async def test_1_idle_with_seeds_binds_peer_only() -> None:
    """Peer server bound on 7701; KV server NOT bound; log 'waiting for tourctl node join'."""
    from tourillon.infra.cli.node import _KV_PHASES

    phase = MemberPhase.IDLE

    bind_kv = phase in _KV_PHASES
    assert bind_kv is False  # IDLE must NOT bind KV


@pytest.mark.gossip
async def test_5_joining_restart_peer_only_no_generation_increment() -> None:
    """Peer server bound; KV server NOT bound; gossip bootstrap starts immediately; generation NOT re-incremented."""
    from tourillon.infra.cli.node import _KV_PHASES

    phase = MemberPhase.JOINING
    bind_kv = phase in _KV_PHASES
    assert bind_kv is False  # JOINING must NOT bind KV


@pytest.mark.gossip
async def test_6_ready_restart_binds_peer_and_kv() -> None:
    """Peer server + KV server bound; topology rebuilt from state; GossipEngine started."""
    from tourillon.infra.cli.node import _KV_PHASES

    phase = MemberPhase.READY
    bind_kv = phase in _KV_PHASES
    assert bind_kv is True  # READY binds KV


@pytest.mark.gossip
async def test_7_draining_restart_binds_peer_and_kv() -> None:
    """Peer server + KV server bound; gossip bootstrap starts; phase remains draining."""
    from tourillon.infra.cli.node import _KV_PHASES

    phase = MemberPhase.DRAINING
    bind_kv = phase in _KV_PHASES
    assert bind_kv is True  # DRAINING binds KV


@pytest.mark.gossip
async def test_8_paused_peer_only_no_gossip_bootstrap() -> None:
    """Peer server bound; KV server NOT bound; no gossip bootstrap; log indicates behaviour out of scope."""
    from tourillon.infra.cli.node import (
        _BOOTSTRAP_PHASES,
        _KV_PHASES,
        _REBALANCE_WATCH_PHASES,
    )

    phase = MemberPhase.PAUSED
    assert phase not in _KV_PHASES
    assert phase not in _BOOTSTRAP_PHASES
    assert phase not in _REBALANCE_WATCH_PHASES


@pytest.mark.gossip
async def test_9_failed_peer_only_no_gossip_warning() -> None:
    """Peer server bound; KV server NOT bound; no gossip bootstrap; WARNING logged."""
    from tourillon.infra.cli.node import (
        _BOOTSTRAP_PHASES,
        _KV_PHASES,
        _REBALANCE_WATCH_PHASES,
    )

    phase = MemberPhase.FAILED
    assert phase not in _KV_PHASES
    assert phase not in _BOOTSTRAP_PHASES
    assert phase not in _REBALANCE_WATCH_PHASES


@pytest.mark.gossip
async def test_ready_node_not_in_rebalance_watch_phases() -> None:
    """READY does not drive a phase transition, so _watch_rebalance_and_transition is not started.

    Rebalance handlers are still registered on the peer dispatcher
    unconditionally (verified by `test_build_peer_dispatcher_*`), so a
    READY node still answers `rebalance.status` queries.
    """
    from tourillon.infra.cli.node import _REBALANCE_WATCH_PHASES

    assert MemberPhase.READY not in _REBALANCE_WATCH_PHASES


@pytest.mark.gossip
async def test_joining_node_in_rebalance_watch_phases() -> None:
    """JOINING starts the watch task that drives JOINING → READY."""
    from tourillon.infra.cli.node import _REBALANCE_WATCH_PHASES

    assert MemberPhase.JOINING in _REBALANCE_WATCH_PHASES


@pytest.mark.gossip
async def test_draining_node_in_rebalance_watch_phases() -> None:
    """DRAINING starts the watch task that drives DRAINING → IDLE."""
    from tourillon.infra.cli.node import _REBALANCE_WATCH_PHASES

    assert MemberPhase.DRAINING in _REBALANCE_WATCH_PHASES


@pytest.mark.gossip
async def test_idle_node_not_in_rebalance_watch_phases() -> None:
    """IDLE does not drive a phase transition itself; the watch task is not started.

    The three rebalance handlers (plan, transfer, status) are however
    registered on the peer dispatcher unconditionally so that an IDLE
    node can answer `rebalance.status` queries immediately after
    transitioning to JOINING via `tourctl node join` — without a daemon
    restart.
    """
    from tourillon.infra.cli.node import _REBALANCE_WATCH_PHASES

    assert MemberPhase.IDLE not in _REBALANCE_WATCH_PHASES


@pytest.mark.gossip
async def test_startup_phase_logic_idle_no_seeds_starts_first_node() -> None:
    """IDLE with no seeds triggers first-node bootstrap → engine started."""

    from tourillon.core.gossip.config import GossipConfig
    from tourillon.infra.cli.node import _startup_phase_logic
    from tourillon.infra.store.state import FileStateAdapter

    state = _make_state(MemberPhase.IDLE, tokens=())
    topology_mgr = TopologyManager()
    serializer = MagicMock()
    state_ref = [state]

    # We need a real-ish config and state_port; mock config and port
    mock_cfg = MagicMock()
    mock_cfg.node_id = "node-2"
    mock_cfg.seeds = []  # no seeds
    mock_cfg.partition_shift = 12

    mock_port = MagicMock(spec=FileStateAdapter)
    mock_port.save = AsyncMock()
    mock_engine = MagicMock()
    mock_engine.announce = AsyncMock()

    with patch(
        "tourillon.infra.cli.node.run_first_node_bootstrap",
        new=AsyncMock(return_value=state),
    ):
        result = await _startup_phase_logic(
            cfg=mock_cfg,
            phase=MemberPhase.IDLE,
            state=state,
            state_port=mock_port,
            topology_mgr=topology_mgr,
            client_ssl_ctx=None,
            serializer=serializer,
            state_ref=state_ref,
            gossip_config=GossipConfig(),
            engine=mock_engine,
        )

    assert result is True  # engine should start


@pytest.mark.gossip
async def test_startup_phase_logic_idle_with_seeds_no_engine() -> None:
    """IDLE with seeds configured — peer server bound, no engine started."""
    from tourillon.core.gossip.config import GossipConfig
    from tourillon.infra.cli.node import _startup_phase_logic
    from tourillon.infra.store.state import FileStateAdapter

    state = _make_state(MemberPhase.IDLE, tokens=())
    topology_mgr = TopologyManager()
    state_ref = [state]

    mock_cfg = MagicMock()
    mock_cfg.node_id = "node-2"
    mock_cfg.seeds = ["10.0.0.1:7701"]  # seeds present
    mock_cfg.partition_shift = 12

    mock_port = MagicMock(spec=FileStateAdapter)
    mock_engine = MagicMock()
    mock_engine.announce = AsyncMock()

    result = await _startup_phase_logic(
        cfg=mock_cfg,
        phase=MemberPhase.IDLE,
        state=state,
        state_port=mock_port,
        topology_mgr=topology_mgr,
        client_ssl_ctx=None,
        serializer=None,
        state_ref=state_ref,
        gossip_config=GossipConfig(),
        engine=mock_engine,
    )

    assert result is False  # engine should NOT start


@pytest.mark.gossip
async def test_startup_phase_logic_paused_no_engine() -> None:
    """PAUSED phase — peer server bound only; no gossip bootstrap; engine not started."""
    from tourillon.core.gossip.config import GossipConfig
    from tourillon.infra.cli.node import _startup_phase_logic
    from tourillon.infra.store.state import FileStateAdapter

    state = _make_state(MemberPhase.PAUSED)
    topology_mgr = TopologyManager()
    state_ref = [state]

    mock_cfg = MagicMock()
    mock_cfg.node_id = "node-2"
    mock_cfg.seeds = []
    mock_cfg.partition_shift = 12

    mock_port = MagicMock(spec=FileStateAdapter)
    mock_engine = MagicMock()
    mock_engine.announce = AsyncMock()

    result = await _startup_phase_logic(
        cfg=mock_cfg,
        phase=MemberPhase.PAUSED,
        state=state,
        state_port=mock_port,
        topology_mgr=topology_mgr,
        client_ssl_ctx=None,
        serializer=None,
        state_ref=state_ref,
        gossip_config=GossipConfig(),
        engine=mock_engine,
    )

    assert result is False


@pytest.mark.gossip
async def test_startup_phase_logic_failed_no_engine() -> None:
    """FAILED phase — peer server bound only; recovery out of scope; engine not started."""
    from tourillon.core.gossip.config import GossipConfig
    from tourillon.infra.cli.node import _startup_phase_logic
    from tourillon.infra.store.state import FileStateAdapter

    state = _make_state(MemberPhase.FAILED)
    topology_mgr = TopologyManager()
    state_ref = [state]

    mock_cfg = MagicMock()
    mock_cfg.node_id = "node-2"
    mock_cfg.seeds = []
    mock_cfg.partition_shift = 12

    mock_port = MagicMock(spec=FileStateAdapter)
    mock_engine = MagicMock()
    mock_engine.announce = AsyncMock()

    result = await _startup_phase_logic(
        cfg=mock_cfg,
        phase=MemberPhase.FAILED,
        state=state,
        state_port=mock_port,
        topology_mgr=topology_mgr,
        client_ssl_ctx=None,
        serializer=None,
        state_ref=state_ref,
        gossip_config=GossipConfig(),
        engine=mock_engine,
    )

    assert result is False


@pytest.mark.gossip
async def test_startup_phase_logic_ready_applies_member_and_starts_engine() -> None:
    """READY phase applies local member to topology and returns True (engine starts)."""
    from tourillon.core.gossip.config import GossipConfig
    from tourillon.infra.cli.node import _startup_phase_logic
    from tourillon.infra.store.state import FileStateAdapter

    state = _make_state(MemberPhase.READY)
    topology_mgr = TopologyManager()
    state_ref = [state]

    mock_cfg = MagicMock()
    mock_cfg.node_id = "node-2"
    mock_cfg.seeds = []
    mock_cfg.partition_shift = 12
    mock_cfg.peer_server.advertise = None
    mock_cfg.peer_server.bind = "10.0.0.2:7701"

    mock_port = MagicMock(spec=FileStateAdapter)
    mock_engine = MagicMock()
    mock_engine.announce = AsyncMock()

    result = await _startup_phase_logic(
        cfg=mock_cfg,
        phase=MemberPhase.READY,
        state=state,
        state_port=mock_port,
        topology_mgr=topology_mgr,
        client_ssl_ctx=None,
        serializer=None,
        state_ref=state_ref,
        gossip_config=GossipConfig(),
        engine=mock_engine,
    )

    assert result is True
    snap = await topology_mgr.snapshot()
    members = list(snap.registry)
    assert any(m.node_id == "node-2" for m in members)


@pytest.mark.gossip
async def test_execute_engine_failed_persists_and_announces() -> None:
    """_execute_engine_failed writes FAILED state then announces via engine."""
    from unittest.mock import AsyncMock, MagicMock

    from tourillon.core.lifecycle.member import MemberPhase
    from tourillon.core.lifecycle.state import NodeState
    from tourillon.infra.cli.node import _execute_engine_failed

    state = NodeState(
        node_id="node-1",
        phase=MemberPhase.READY,
        generation=1,
        seq=5,
        tokens=(1, 2, 3, 4),
        epoch=1,
    )
    state_ref = [state]

    mock_cfg = MagicMock()
    mock_cfg.node_id = "node-1"
    mock_cfg.partition_shift = 12

    mock_port = MagicMock()
    mock_port.save = AsyncMock()

    announced = []
    mock_engine = MagicMock()
    mock_engine.announce = AsyncMock(side_effect=lambda m: announced.append(m))

    await _execute_engine_failed(
        cfg=mock_cfg,
        state_port=mock_port,
        state_ref=state_ref,
        peer_address="10.0.0.1:7701",
        engine=mock_engine,
    )

    mock_port.save.assert_awaited_once()
    saved_state = mock_port.save.call_args[0][0]
    assert saved_state.phase == MemberPhase.FAILED
    assert saved_state.seq == 6

    assert len(announced) == 1
    assert announced[0].phase == MemberPhase.FAILED
    assert state_ref[0].phase == MemberPhase.FAILED


@pytest.mark.gossip
async def test_execute_engine_failed_save_error_still_announces() -> None:
    """_execute_engine_failed logs error if save fails but still announces."""
    from unittest.mock import AsyncMock, MagicMock

    from tourillon.core.lifecycle.member import MemberPhase
    from tourillon.core.lifecycle.state import NodeState
    from tourillon.infra.cli.node import _execute_engine_failed

    state = NodeState(
        node_id="node-1",
        phase=MemberPhase.READY,
        generation=1,
        seq=5,
        tokens=(1, 2, 3, 4),
        epoch=1,
    )
    state_ref = [state]

    mock_cfg = MagicMock()
    mock_cfg.node_id = "node-1"
    mock_cfg.partition_shift = 12

    mock_port = MagicMock()
    mock_port.save = AsyncMock(side_effect=OSError("disk full"))

    announced = []
    mock_engine = MagicMock()
    mock_engine.announce = AsyncMock(side_effect=lambda m: announced.append(m))

    await _execute_engine_failed(
        cfg=mock_cfg,
        state_port=mock_port,
        state_ref=state_ref,
        peer_address="10.0.0.1:7701",
        engine=mock_engine,
    )

    assert len(announced) == 1


@pytest.mark.gossip
async def test_bootstrap_first_node_success_returns_state() -> None:
    """_bootstrap_first_node calls run_first_node_bootstrap and returns new state."""
    from unittest.mock import AsyncMock, MagicMock

    from tourillon.core.lifecycle.member import MemberPhase
    from tourillon.core.lifecycle.state import NodeState
    from tourillon.infra.cli.node import _bootstrap_first_node

    expected = NodeState(
        node_id="node-1",
        phase=MemberPhase.READY,
        generation=1,
        seq=1,
        tokens=(0, 1, 2, 3),
        epoch=1,
    )

    mock_cfg = MagicMock()
    mock_cfg.node_id = "node-1"
    mock_port = MagicMock()
    mock_topo = MagicMock()

    with patch(
        "tourillon.infra.cli.node.run_first_node_bootstrap",
        new=AsyncMock(return_value=expected),
    ):
        result = await _bootstrap_first_node(mock_cfg, mock_port, mock_topo)

    assert result is expected


@pytest.mark.gossip
async def test_execute_launch_bootstrap_success_increments_stats() -> None:
    """_execute_launch_bootstrap runs bootstrapper and increments engine stats."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from tourillon.core.gossip.config import GossipConfig
    from tourillon.core.lifecycle.member import MemberPhase
    from tourillon.core.lifecycle.state import NodeState
    from tourillon.core.ring.topology import TopologyManager
    from tourillon.infra.cli.node import _execute_launch_bootstrap

    mock_cfg = MagicMock()
    mock_cfg.partition_shift = 12
    mock_cfg.node_id = "node-1"
    mock_cfg.peer_server.advertise = None
    mock_cfg.peer_server.bind = "10.0.0.1:7701"

    mock_engine = MagicMock()
    mock_engine.stats.bootstrap_ok_total = 0
    mock_engine.announce = AsyncMock()

    topology_mgr = TopologyManager()
    gossip_cfg = GossipConfig()
    stop = asyncio.Event()
    start_engine_event = asyncio.Event()
    state = NodeState(
        node_id="node-1",
        phase=MemberPhase.JOINING,
        generation=1,
        seq=0,
        tokens=(0, 1, 2, 3),
        epoch=0,
    )
    state_ref = [state]

    with patch("tourillon.infra.cli.node.GossipBootstrapper") as mock_bootstrapper:
        instance = MagicMock()
        instance.run = AsyncMock(return_value=3)
        mock_bootstrapper.return_value = instance

        await _execute_launch_bootstrap(
            cfg=mock_cfg,
            topology_mgr=topology_mgr,
            gossip_config=gossip_cfg,
            client_ssl_ctx=None,
            serializer=None,
            engine=mock_engine,
            phase=MemberPhase.JOINING,
            stop=stop,
            state_ref=state_ref,
            start_engine_event=start_engine_event,
            seeds=["10.0.0.1:7701"],
        )

    assert mock_engine.stats.bootstrap_ok_total == 3
    assert not stop.is_set()  # success — stop must NOT be triggered
    assert start_engine_event.is_set()  # engine must be signalled to start
    mock_engine.announce.assert_awaited_once()


@pytest.mark.gossip
async def test_run_seeded_bootstrap_success() -> None:
    """_run_seeded_bootstrap calls bootstrapper.run with cfg.seeds."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from tourillon.core.gossip.config import GossipConfig
    from tourillon.core.ring.topology import TopologyManager
    from tourillon.infra.cli.node import _run_seeded_bootstrap

    mock_cfg = MagicMock()
    mock_cfg.partition_shift = 12
    mock_cfg.seeds = ["10.0.0.1:7701"]

    topology_mgr = TopologyManager()
    gossip_cfg = GossipConfig()

    with patch("tourillon.infra.cli.node.GossipBootstrapper") as mock_bootstrapper:
        instance = MagicMock()
        instance.run = AsyncMock(return_value=1)
        mock_bootstrapper.return_value = instance

        await _run_seeded_bootstrap(
            cfg=mock_cfg,
            topology_mgr=topology_mgr,
            gossip_config=gossip_cfg,
            client_ssl_ctx=None,
            serializer=None,
        )

    instance.run.assert_awaited_once_with(["10.0.0.1:7701"])


@pytest.mark.gossip
async def test_startup_phase_logic_joining_calls_seeded_bootstrap() -> None:
    """JOINING phase calls _run_seeded_bootstrap and returns True."""
    from tourillon.core.gossip.config import GossipConfig
    from tourillon.infra.cli.node import _startup_phase_logic
    from tourillon.infra.store.state import FileStateAdapter

    state = _make_state(MemberPhase.JOINING)
    topology_mgr = TopologyManager()
    state_ref = [state]

    mock_cfg = MagicMock()
    mock_cfg.node_id = "node-2"
    mock_cfg.seeds = ["10.0.0.1:7701"]
    mock_cfg.partition_shift = 12
    mock_cfg.peer_server.advertise = None
    mock_cfg.peer_server.bind = "10.0.0.2:7701"

    mock_port = MagicMock(spec=FileStateAdapter)
    mock_engine = MagicMock()
    mock_engine.announce = AsyncMock()

    with patch(
        "tourillon.infra.cli.node._run_seeded_bootstrap", new=AsyncMock()
    ) as mock_bootstrap:
        result = await _startup_phase_logic(
            cfg=mock_cfg,
            phase=MemberPhase.JOINING,
            state=state,
            state_port=mock_port,
            topology_mgr=topology_mgr,
            client_ssl_ctx=None,
            serializer=None,
            state_ref=state_ref,
            gossip_config=GossipConfig(),
            engine=mock_engine,
        )

    assert result is True
    mock_bootstrap.assert_awaited_once()
    mock_engine.announce.assert_awaited_once()


@pytest.mark.gossip
async def test_run_seeded_bootstrap_partition_shift_error_calls_sys_exit() -> None:
    """_run_seeded_bootstrap exits on BootstrapPartitionShiftError."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from tourillon.core.gossip.bootstrapper import BootstrapPartitionShiftError
    from tourillon.core.gossip.config import GossipConfig
    from tourillon.core.ring.topology import TopologyManager
    from tourillon.infra.cli.node import _run_seeded_bootstrap

    mock_cfg = MagicMock()
    mock_cfg.partition_shift = 12
    mock_cfg.seeds = ["10.0.0.1:7701"]

    topology_mgr = TopologyManager()
    gossip_cfg = GossipConfig()

    exc = BootstrapPartitionShiftError("10.0.0.1:7701", seed_shift=10, local_shift=12)
    with patch("tourillon.infra.cli.node.GossipBootstrapper") as mock_bootstrapper:
        instance = MagicMock()
        instance.run = AsyncMock(side_effect=exc)
        mock_bootstrapper.return_value = instance

        with pytest.raises(SystemExit):
            await _run_seeded_bootstrap(
                cfg=mock_cfg,
                topology_mgr=topology_mgr,
                gossip_config=gossip_cfg,
                client_ssl_ctx=None,
                serializer=None,
            )


@pytest.mark.gossip
async def test_run_seeded_bootstrap_bootstrap_error_calls_sys_exit() -> None:
    """_run_seeded_bootstrap exits on BootstrapError."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from tourillon.core.gossip.bootstrapper import BootstrapError
    from tourillon.core.gossip.config import GossipConfig
    from tourillon.core.ring.topology import TopologyManager
    from tourillon.infra.cli.node import _run_seeded_bootstrap

    mock_cfg = MagicMock()
    mock_cfg.partition_shift = 12
    mock_cfg.seeds = ["10.0.0.1:7701"]

    topology_mgr = TopologyManager()
    gossip_cfg = GossipConfig()

    with patch("tourillon.infra.cli.node.GossipBootstrapper") as mock_bootstrapper:
        instance = MagicMock()
        instance.run = AsyncMock(side_effect=BootstrapError("no seeds"))
        mock_bootstrapper.return_value = instance

        with pytest.raises(SystemExit):
            await _run_seeded_bootstrap(
                cfg=mock_cfg,
                topology_mgr=topology_mgr,
                gossip_config=gossip_cfg,
                client_ssl_ctx=None,
                serializer=None,
            )


@pytest.mark.gossip
async def test_bootstrap_first_node_exception_calls_sys_exit() -> None:
    """_bootstrap_first_node exits on any exception from run_first_node_bootstrap."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from tourillon.infra.cli.node import _bootstrap_first_node

    mock_cfg = MagicMock()
    mock_cfg.node_id = "node-1"
    mock_port = MagicMock()
    mock_topo = MagicMock()

    with (
        patch(
            "tourillon.infra.cli.node.run_first_node_bootstrap",
            new=AsyncMock(side_effect=RuntimeError("disk full")),
        ),
        pytest.raises(SystemExit),
    ):
        await _bootstrap_first_node(mock_cfg, mock_port, mock_topo)


@pytest.mark.gossip
async def test_execute_launch_bootstrap_partition_shift_error_exits() -> None:
    """_execute_launch_bootstrap sets stop on BootstrapPartitionShiftError."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from tourillon.core.gossip.bootstrapper import BootstrapPartitionShiftError
    from tourillon.core.gossip.config import GossipConfig
    from tourillon.core.lifecycle.member import MemberPhase
    from tourillon.core.lifecycle.state import NodeState
    from tourillon.core.ring.topology import TopologyManager
    from tourillon.infra.cli.node import _execute_launch_bootstrap

    mock_cfg = MagicMock()
    mock_cfg.partition_shift = 12
    mock_cfg.node_id = "node-1"
    mock_cfg.peer_server.advertise = None
    mock_cfg.peer_server.bind = "10.0.0.1:7701"

    mock_engine = MagicMock()
    mock_engine.stats.bootstrap_ok_total = 0
    mock_engine.announce = AsyncMock()

    topology_mgr = TopologyManager()
    gossip_cfg = GossipConfig()
    stop = asyncio.Event()
    start_engine_event = asyncio.Event()
    state = NodeState(
        node_id="node-1",
        phase=MemberPhase.JOINING,
        generation=1,
        seq=0,
        tokens=(0, 1, 2, 3),
        epoch=0,
    )

    exc = BootstrapPartitionShiftError("10.0.0.1:7701", seed_shift=10, local_shift=12)
    with patch("tourillon.infra.cli.node.GossipBootstrapper") as mock_bootstrapper:
        instance = MagicMock()
        instance.run = AsyncMock(side_effect=exc)
        mock_bootstrapper.return_value = instance

        await _execute_launch_bootstrap(
            cfg=mock_cfg,
            topology_mgr=topology_mgr,
            gossip_config=gossip_cfg,
            client_ssl_ctx=None,
            serializer=None,
            engine=mock_engine,
            phase=MemberPhase.JOINING,
            stop=stop,
            state_ref=[state],
            start_engine_event=start_engine_event,
            seeds=["10.0.0.1:7701"],
        )

    assert stop.is_set()  # fatal error must trigger clean shutdown
    assert not start_engine_event.is_set()  # engine must NOT start on error


@pytest.mark.gossip
async def test_execute_launch_bootstrap_bootstrap_error_exits() -> None:
    """_execute_launch_bootstrap sets stop on BootstrapError."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from tourillon.core.gossip.bootstrapper import BootstrapError
    from tourillon.core.gossip.config import GossipConfig
    from tourillon.core.lifecycle.member import MemberPhase
    from tourillon.core.lifecycle.state import NodeState
    from tourillon.core.ring.topology import TopologyManager
    from tourillon.infra.cli.node import _execute_launch_bootstrap

    mock_cfg = MagicMock()
    mock_cfg.partition_shift = 12
    mock_cfg.node_id = "node-1"
    mock_cfg.peer_server.advertise = None
    mock_cfg.peer_server.bind = "10.0.0.1:7701"

    mock_engine = MagicMock()
    mock_engine.announce = AsyncMock()

    topology_mgr = TopologyManager()
    gossip_cfg = GossipConfig()
    stop = asyncio.Event()
    start_engine_event = asyncio.Event()
    state = NodeState(
        node_id="node-1",
        phase=MemberPhase.JOINING,
        generation=1,
        seq=0,
        tokens=(0, 1, 2, 3),
        epoch=0,
    )

    with patch("tourillon.infra.cli.node.GossipBootstrapper") as mock_bootstrapper:
        instance = MagicMock()
        instance.run = AsyncMock(side_effect=BootstrapError("all failed"))
        mock_bootstrapper.return_value = instance

        await _execute_launch_bootstrap(
            cfg=mock_cfg,
            topology_mgr=topology_mgr,
            gossip_config=gossip_cfg,
            client_ssl_ctx=None,
            serializer=None,
            engine=mock_engine,
            phase=MemberPhase.JOINING,
            stop=stop,
            state_ref=[state],
            start_engine_event=start_engine_event,
            seeds=["10.0.0.1:7701"],
        )

    assert stop.is_set()  # fatal error must trigger clean shutdown
    assert not start_engine_event.is_set()  # engine must NOT start on error


@pytest.mark.gossip
def test_log_startup_phase_joining_logs_resuming_bootstrap() -> None:
    """_log_startup_phase JOINING logs 'Resuming gossip bootstrap'."""
    from unittest.mock import MagicMock

    from tourillon.infra.cli.node import _log_startup_phase

    cfg = MagicMock()
    cfg.node_id = "node-1"
    cfg.seeds = []
    state = MagicMock()
    _log_startup_phase(cfg, MemberPhase.JOINING, state)  # no crash


@pytest.mark.gossip
def test_log_startup_phase_ready_logs_topology_rebuilt() -> None:
    """_log_startup_phase READY logs topology info with vnode count and epoch."""
    from unittest.mock import MagicMock

    from tourillon.infra.cli.node import _log_startup_phase

    cfg = MagicMock()
    cfg.node_id = "node-1"
    cfg.seeds = []
    state = _make_state(MemberPhase.READY)
    _log_startup_phase(cfg, MemberPhase.READY, state)  # no crash


@pytest.mark.gossip
def test_log_startup_phase_draining_logs_resume_drain() -> None:
    """_log_startup_phase DRAINING logs 'Resuming drain with gossip bootstrap'."""
    from unittest.mock import MagicMock

    from tourillon.infra.cli.node import _log_startup_phase

    cfg = MagicMock()
    cfg.node_id = "node-1"
    cfg.seeds = []
    state = _make_state(MemberPhase.DRAINING)
    _log_startup_phase(cfg, MemberPhase.DRAINING, state)  # no crash


@pytest.mark.gossip
def test_log_startup_phase_idle_with_seeds_logs_seed_count() -> None:
    """_log_startup_phase IDLE with seeds logs configured seed count."""
    from unittest.mock import MagicMock

    from tourillon.infra.cli.node import _log_startup_phase

    cfg = MagicMock()
    cfg.node_id = "node-1"
    cfg.seeds = ["10.0.0.1:7701", "10.0.0.2:7701"]
    state = _make_state(MemberPhase.IDLE, tokens=())
    _log_startup_phase(cfg, MemberPhase.IDLE, state)  # no crash


@pytest.mark.gossip
def test_log_startup_phase_else_branch_logs_generic_message() -> None:
    """_log_startup_phase else branch (IDLE+no seeds, PAUSED, FAILED) logs generic message."""
    from unittest.mock import MagicMock

    from tourillon.infra.cli.node import _log_startup_phase

    cfg = MagicMock()
    cfg.node_id = "node-1"
    cfg.seeds = []  # no seeds → else branch for IDLE without seeds
    state = _make_state(MemberPhase.IDLE, tokens=())
    _log_startup_phase(cfg, MemberPhase.IDLE, state)  # hits else branch

    _log_startup_phase(cfg, MemberPhase.PAUSED, state)  # hits else branch
    _log_startup_phase(cfg, MemberPhase.FAILED, state)  # hits else branch


@pytest.mark.gossip
def test_parse_bind_splits_host_and_port_correctly() -> None:
    """_parse_bind correctly splits a 'host:port' string."""
    from tourillon.infra.cli.node import _parse_bind

    host, port = _parse_bind("10.0.0.1:7701")
    assert host == "10.0.0.1"
    assert port == 7701


@pytest.mark.gossip
def test_parse_bind_empty_host_defaults_to_all_interfaces() -> None:
    """_parse_bind with port-only address defaults host to '0.0.0.0'."""
    from tourillon.infra.cli.node import _parse_bind

    host, port = _parse_bind(":7701")
    assert host == "0.0.0.0"
    assert port == 7701


@pytest.mark.gossip
async def test_install_signal_handlers_does_not_raise() -> None:
    """_install_signal_handlers registers SIGINT/SIGTERM without raising."""
    import asyncio

    from tourillon.infra.cli.node import _install_signal_handlers

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    _install_signal_handlers(loop, stop)  # should not raise on any platform


@pytest.mark.gossip
async def test_startup_phase_logic_joining_self_registers_before_bootstrap() -> None:
    """JOINING phase registers own member in topology BEFORE bootstrap runs."""
    from tourillon.core.gossip.config import GossipConfig
    from tourillon.infra.cli.node import _startup_phase_logic
    from tourillon.infra.store.state import FileStateAdapter

    state = _make_state(MemberPhase.JOINING)
    topology_mgr = TopologyManager()
    state_ref = [state]

    mock_cfg = MagicMock()
    mock_cfg.node_id = "node-2"
    mock_cfg.seeds = ["10.0.0.1:7701"]
    mock_cfg.partition_shift = 12
    mock_cfg.peer_server.advertise = None
    mock_cfg.peer_server.bind = "10.0.0.2:7701"

    mock_port = MagicMock(spec=FileStateAdapter)
    mock_engine = MagicMock()
    mock_engine.announce = AsyncMock()

    members_at_bootstrap: list[object] = []

    async def _capture_bootstrap(*_args: object, **_kw: object) -> None:
        snap = await topology_mgr.snapshot()
        members_at_bootstrap.extend(snap.registry)

    with patch(
        "tourillon.infra.cli.node._run_seeded_bootstrap",
        new=_capture_bootstrap,
    ):
        await _startup_phase_logic(
            cfg=mock_cfg,
            phase=MemberPhase.JOINING,
            state=state,
            state_port=mock_port,
            topology_mgr=topology_mgr,
            client_ssl_ctx=None,
            serializer=None,
            state_ref=state_ref,
            gossip_config=GossipConfig(),
            engine=mock_engine,
        )

    # Own member must already be visible to bootstrapper when it runs.
    assert any(getattr(m, "node_id", None) == "node-2" for m in members_at_bootstrap)


@pytest.mark.gossip
async def test_await_engine_start_or_stop_stop_signal_returns_immediately() -> None:
    """_await_engine_start_or_stop returns without starting engine when stop is set."""
    from tourillon.infra.cli.node import _await_engine_start_or_stop

    stop = asyncio.Event()
    start_engine_event = asyncio.Event()

    mock_engine = MagicMock()
    mock_engine.start = AsyncMock()
    mock_engine.stop = AsyncMock()

    stop.set()  # already stopped before entering

    # Stub out _setup_rebalance so it is never reached (stop fires first).
    with patch(
        "tourillon.infra.cli.node._setup_rebalance", new=AsyncMock()
    ) as mock_setup:
        await _await_engine_start_or_stop(
            stop=stop,
            start_engine_event=start_engine_event,
            engine=mock_engine,
            cfg=MagicMock(),
            state_ref=[MagicMock()],
            state_port=MagicMock(),
            topology_mgr=MagicMock(),
            partitioner=MagicMock(),
            applicator=MagicMock(),
            kv_server=MagicMock(),
            kv_host="127.0.0.1",
            kv_port=17700,
            peer_address="127.0.0.1:17701",
        )

    mock_engine.start.assert_not_awaited()
    mock_setup.assert_not_awaited()


@pytest.mark.gossip
async def test_await_engine_start_or_stop_starts_engine_and_watcher_after_join() -> (
    None
):
    """_await_engine_start_or_stop starts GossipEngine and rebalance watcher when start_engine_event fires.

    After the fix, the function also calls _setup_rebalance and spawns
    _watch_rebalance_and_transition — the root cause of JOINING nodes never
    transitioning to READY when the daemon started in IDLE phase.
    """
    from tourillon.infra.cli.node import _await_engine_start_or_stop

    stop = asyncio.Event()
    start_engine_event = asyncio.Event()

    engine_started = asyncio.Event()

    async def _fake_engine_start() -> None:
        engine_started.set()
        await stop.wait()

    mock_engine = MagicMock()
    mock_engine.start = _fake_engine_start
    mock_engine.stop = AsyncMock()

    setup_called = asyncio.Event()
    watch_called = asyncio.Event()

    async def _fake_setup(**_kwargs: object) -> None:
        setup_called.set()

    async def _fake_watch(**_kwargs: object) -> None:
        watch_called.set()
        await stop.wait()

    async def _trigger_then_stop() -> None:
        await asyncio.sleep(0.05)
        start_engine_event.set()
        await asyncio.sleep(0.1)
        stop.set()

    with (
        patch("tourillon.infra.cli.node._setup_rebalance", new=_fake_setup),
        patch(
            "tourillon.infra.cli.node._watch_rebalance_and_transition",
            new=_fake_watch,
        ),
    ):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                _await_engine_start_or_stop(
                    stop=stop,
                    start_engine_event=start_engine_event,
                    engine=mock_engine,
                    cfg=MagicMock(),
                    state_ref=[MagicMock()],
                    state_port=MagicMock(),
                    topology_mgr=MagicMock(),
                    partitioner=MagicMock(),
                    applicator=MagicMock(),
                    kv_server=MagicMock(),
                    kv_host="127.0.0.1",
                    kv_port=17700,
                    peer_address="127.0.0.1:17701",
                ),
                name="test.await_engine",
            )
            tg.create_task(_trigger_then_stop(), name="test.trigger")

    assert engine_started.is_set(), "GossipEngine must have been started"
    assert setup_called.is_set(), "_setup_rebalance must be called on IDLE→JOINING"
    assert watch_called.is_set(), "_watch_rebalance_and_transition must be spawned"


@pytest.mark.gossip
async def test_execute_launch_bootstrap_success_registers_own_member_in_topology() -> (
    None
):
    """_execute_launch_bootstrap registers local JOINING member in topology after bootstrap."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from tourillon.core.gossip.config import GossipConfig
    from tourillon.core.lifecycle.member import MemberPhase
    from tourillon.core.lifecycle.state import NodeState
    from tourillon.core.ring.topology import TopologyManager
    from tourillon.infra.cli.node import _execute_launch_bootstrap

    mock_cfg = MagicMock()
    mock_cfg.partition_shift = 12
    mock_cfg.node_id = "node-1"
    mock_cfg.peer_server.advertise = None
    mock_cfg.peer_server.bind = "10.0.0.1:7701"

    mock_engine = MagicMock()
    mock_engine.stats.bootstrap_ok_total = 0
    mock_engine.announce = AsyncMock()

    topology_mgr = TopologyManager()
    gossip_cfg = GossipConfig()
    stop = asyncio.Event()
    start_engine_event = asyncio.Event()
    state = NodeState(
        node_id="node-1",
        phase=MemberPhase.JOINING,
        generation=1,
        seq=0,
        tokens=(0, 1, 2, 3),
        epoch=0,
    )

    with patch("tourillon.infra.cli.node.GossipBootstrapper") as mock_bootstrapper:
        instance = MagicMock()
        instance.run = AsyncMock(return_value=1)
        mock_bootstrapper.return_value = instance

        await _execute_launch_bootstrap(
            cfg=mock_cfg,
            topology_mgr=topology_mgr,
            gossip_config=gossip_cfg,
            client_ssl_ctx=None,
            serializer=None,
            engine=mock_engine,
            phase=MemberPhase.JOINING,
            stop=stop,
            state_ref=[state],
            start_engine_event=start_engine_event,
            seeds=["10.0.0.1:7701"],
        )

    snap = await topology_mgr.snapshot()
    members = list(snap.registry)
    assert any(m.node_id == "node-1" for m in members)
