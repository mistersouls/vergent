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
"""Tests for ProbeManager — is_live, is_unknown, snapshot, record_miss, record_heartbeat."""

from __future__ import annotations

import asyncio

import pytest

from tourillon.core.lifecycle.probe import MemberState, ProbeManager


@pytest.mark.lifecycle
async def test_probe_manager_is_live_returns_true_after_heartbeat() -> None:
    """is_live() returns True when node has recent heartbeats below phi threshold."""
    pm = ProbeManager()
    await pm.record_heartbeat("node-1")
    # Record multiple heartbeats to establish intervals
    await asyncio.sleep(0.01)
    await pm.record_heartbeat("node-1")
    await asyncio.sleep(0.01)
    await pm.record_heartbeat("node-1")

    assert await pm.is_live("node-1") is True


@pytest.mark.lifecycle
async def test_probe_manager_is_unknown_returns_true_for_unseen_node() -> None:
    """is_unknown() returns True for a node with no observations."""
    pm = ProbeManager()
    assert await pm.is_unknown("node-never-seen") is True


@pytest.mark.lifecycle
async def test_probe_manager_is_live_returns_false_for_unseen_node() -> None:
    """is_live() returns False for a node with no observations (state is UNKNOWN)."""
    pm = ProbeManager()
    assert await pm.is_live("node-never-seen") is False


@pytest.mark.lifecycle
async def test_probe_manager_is_unknown_returns_false_after_heartbeat() -> None:
    """is_unknown() returns False after at least one heartbeat recorded."""
    pm = ProbeManager()
    await pm.record_heartbeat("node-1")
    assert await pm.is_unknown("node-1") is False


@pytest.mark.lifecycle
async def test_probe_manager_snapshot_returns_all_tracked_nodes() -> None:
    """snapshot() maps all known nodes to their current MemberState."""
    pm = ProbeManager()
    await pm.record_heartbeat("node-1")
    await pm.record_miss("node-2")  # creates detector without heartbeat

    snap = await pm.snapshot()

    assert "node-1" in snap
    assert "node-2" in snap


@pytest.mark.lifecycle
async def test_probe_manager_record_miss_creates_detector_for_unknown_node() -> None:
    """record_miss() creates a detector even when no heartbeat has been seen."""
    pm = ProbeManager()
    await pm.record_miss("node-new")

    # Node is now tracked but has no heartbeat history → phi=0.0
    phi = await pm.phi_of("node-new")
    assert phi == 0.0

    # State should be UNKNOWN (no heartbeats yet)
    state = await pm.state_of("node-new")
    assert state == MemberState.UNKNOWN


@pytest.mark.lifecycle
async def test_probe_manager_record_miss_does_not_overwrite_existing_detector() -> None:
    """record_miss() on an already-tracked node does not reset heartbeat history."""
    pm = ProbeManager()
    await pm.record_heartbeat("node-1")
    await asyncio.sleep(0.01)
    await pm.record_heartbeat("node-1")

    # record_miss should not reset the detector
    await pm.record_miss("node-1")
    phi = await pm.phi_of("node-1")
    # After two heartbeats + small sleep, phi should be small but >= 0
    assert phi >= 0.0

    # Node should still be LIVE (phi below threshold)
    assert await pm.is_live("node-1") is True


@pytest.mark.lifecycle
async def test_probe_manager_all_states_with_phi_includes_all_nodes() -> None:
    """all_states_with_phi() returns list of (node_id, state, phi) for all tracked nodes."""
    pm = ProbeManager()
    await pm.record_heartbeat("node-a")
    await pm.record_miss("node-b")

    states = await pm.all_states_with_phi()
    node_ids = {nid for nid, _, _ in states}
    assert "node-a" in node_ids
    assert "node-b" in node_ids


@pytest.mark.lifecycle
def test_failure_detector_mean_empty_returns_zero() -> None:
    """_mean() returns 0.0 for an empty deque (defensive branch)."""
    from collections import deque

    from tourillon.core.lifecycle.phi import _mean  # type: ignore[attr-defined]

    assert _mean(deque()) == 0.0


@pytest.mark.lifecycle
def test_failure_detector_phi_with_zero_interval_returns_zero() -> None:
    """phi() returns 0.0 when mean of intervals is zero (defensive branch)."""
    from tourillon.core.lifecycle.phi import FailureDetector

    fd = FailureDetector()
    # Manually inject a zero-length interval so mean==0 → return 0.0 guard fires.
    fd._last_arrival = 0.0  # noqa: SLF001
    fd._intervals.append(0.0)  # noqa: SLF001
    assert fd.phi() == 0.0
