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
"""Tests for HLCTimestamp and HLCClock — proposal 005 clock structures."""

from __future__ import annotations

import pytest

from tourillon.core.structure.clock import HLCClock, HLCTimestamp


@pytest.mark.rebalance
def test_hlctimestamp_total_order_wall_ms() -> None:
    """HLCTimestamp with higher wall_ms compares greater."""
    a = HLCTimestamp(wall_ms=100, counter=5, node_id="x")
    b = HLCTimestamp(wall_ms=200, counter=0, node_id="x")
    assert a < b
    assert b > a
    assert a != b


@pytest.mark.rebalance
def test_hlctimestamp_total_order_counter() -> None:
    """Same wall_ms: higher counter compares greater."""
    a = HLCTimestamp(wall_ms=100, counter=1, node_id="x")
    b = HLCTimestamp(wall_ms=100, counter=2, node_id="x")
    assert a < b


@pytest.mark.rebalance
def test_hlctimestamp_total_order_node_id() -> None:
    """Same wall_ms and counter: node_id tiebreak is lexicographic."""
    a = HLCTimestamp(wall_ms=100, counter=0, node_id="aaa")
    b = HLCTimestamp(wall_ms=100, counter=0, node_id="bbb")
    assert a < b


@pytest.mark.rebalance
def test_hlctimestamp_round_trip_dict() -> None:
    """to_dict() followed by from_dict() produces an equal HLCTimestamp."""
    ts = HLCTimestamp(wall_ms=999, counter=3, node_id="node-1")
    assert HLCTimestamp.from_dict(ts.to_dict()) == ts


@pytest.mark.rebalance
def test_hlcclock_tick_monotone() -> None:
    """Successive tick() calls return strictly increasing timestamps."""
    clk = HLCClock("node-1")
    prev = clk.tick()
    for _ in range(10):
        curr = clk.tick()
        assert curr > prev
        prev = curr


@pytest.mark.rebalance
def test_hlcclock_tick_counter_increments_when_wall_unchanged() -> None:
    """tick() increments counter when wall_ms does not advance."""
    clk = HLCClock("node-1")
    # Force clock to a fixed wall_ms so next tick stays in same ms bucket.
    clk._wall_ms = 10**18  # Far future; real time.time()*1000 is always smaller.
    t1 = clk.tick()
    t2 = clk.tick()
    assert t2.wall_ms == t1.wall_ms
    assert t2.counter == t1.counter + 1


@pytest.mark.rebalance
def test_hlcclock_update_advances_past_remote() -> None:
    """update() with a remote timestamp in the future advances the clock."""
    clk = HLCClock("node-1")
    future = HLCTimestamp(wall_ms=10**18, counter=5, node_id="node-2")
    result = clk.update(future)
    assert result.wall_ms == future.wall_ms
    assert result.counter == future.counter + 1


@pytest.mark.rebalance
def test_hlcclock_update_concurrent_same_wall() -> None:
    """update() when local and remote share the same wall_ms uses max(counter)+1."""
    clk = HLCClock("node-1")
    clk._wall_ms = 10**18
    clk._counter = 3
    remote = HLCTimestamp(wall_ms=10**18, counter=7, node_id="node-2")
    result = clk.update(remote)
    assert result.wall_ms == 10**18
    assert result.counter == 8  # max(3, 7) + 1


@pytest.mark.rebalance
def test_hlcclock_update_local_wall_ahead() -> None:
    """update() when local wall_ms is strictly ahead: counter increments by 1."""
    clk = HLCClock("node-1")
    clk._wall_ms = 10**18
    clk._counter = 2
    old_remote = HLCTimestamp(wall_ms=100, counter=99, node_id="node-2")
    result = clk.update(old_remote)
    assert result.wall_ms == 10**18
    assert result.counter == 3  # local_counter + 1
