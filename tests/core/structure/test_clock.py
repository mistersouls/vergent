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
"""Tests for tourillon.core.structure.clock — HLCTimestamp and HLCClock."""

import pytest

from tourillon.core.structure.clock import HLCClock, HLCTimestamp


def test_hlctimestamp_lt_by_wall() -> None:
    """Earlier wall time is strictly less than later wall time."""
    a = HLCTimestamp(wall=100, counter=0, node_id="n1")
    b = HLCTimestamp(wall=200, counter=0, node_id="n1")
    assert a < b
    assert not b < a


def test_hlctimestamp_lt_by_counter_same_wall() -> None:
    """Lower counter is strictly less when wall times are equal."""
    a = HLCTimestamp(wall=100, counter=0, node_id="n1")
    b = HLCTimestamp(wall=100, counter=1, node_id="n1")
    assert a < b


def test_hlctimestamp_lt_by_node_id_tiebreak() -> None:
    """node_id breaks ties when wall and counter are identical."""
    a = HLCTimestamp(wall=100, counter=0, node_id="node-a")
    b = HLCTimestamp(wall=100, counter=0, node_id="node-b")
    assert a < b
    assert not b < a


def test_hlctimestamp_total_order_is_consistent() -> None:
    """gt and le are consistent with lt."""
    a = HLCTimestamp(wall=1, counter=0, node_id="n")
    b = HLCTimestamp(wall=2, counter=0, node_id="n")
    assert b > a
    assert a <= b
    assert b >= a
    assert a <= a
    assert a >= a


def test_hlctimestamp_equality_reflexive() -> None:
    """A timestamp equals itself."""
    ts = HLCTimestamp(wall=42, counter=7, node_id="x")
    assert ts == ts
    assert not ts < ts
    assert ts <= ts


def test_hlctimestamp_tick_increments_counter_when_wall_unchanged() -> None:
    """tick must increment counter when now_ms equals current wall."""
    ts = HLCTimestamp(wall=100, counter=3, node_id="n")
    result = ts.tick(now_ms=100)
    assert result.wall == 100
    assert result.counter == 4
    assert result.node_id == "n"


def test_hlctimestamp_tick_advances_wall_and_resets_counter() -> None:
    """tick must advance wall and reset counter when now_ms is strictly ahead."""
    ts = HLCTimestamp(wall=100, counter=5, node_id="n")
    result = ts.tick(now_ms=200)
    assert result.wall == 200
    assert result.counter == 0


def test_hlctimestamp_tick_ignores_clock_skew_backwards() -> None:
    """tick must not allow wall to go backwards when now_ms is behind current wall."""
    ts = HLCTimestamp(wall=500, counter=0, node_id="n")
    result = ts.tick(now_ms=100)
    assert result.wall == 500
    assert result.counter == 1


def test_hlctimestamp_tick_result_is_strictly_greater() -> None:
    """tick result must be strictly greater than original timestamp."""
    ts = HLCTimestamp(wall=100, counter=0, node_id="n")
    assert ts.tick(now_ms=100) > ts
    assert ts.tick(now_ms=99) > ts
    assert ts.tick(now_ms=200) > ts


def test_hlctimestamp_update_takes_max_wall() -> None:
    """update must take the maximum of local wall, remote wall, and now_ms."""
    local = HLCTimestamp(wall=100, counter=0, node_id="n")
    remote = HLCTimestamp(wall=200, counter=0, node_id="m")
    result = local.update(remote, now_ms=50)
    assert result.wall == 200


def test_hlctimestamp_update_increments_counter_on_equal_walls() -> None:
    """update must increment past max(local, remote) counter when walls are equal."""
    local = HLCTimestamp(wall=100, counter=2, node_id="n")
    remote = HLCTimestamp(wall=100, counter=5, node_id="m")
    result = local.update(remote, now_ms=100)
    assert result.wall == 100
    assert result.counter == 6


def test_hlctimestamp_update_result_exceeds_both_inputs() -> None:
    """update result must be strictly greater than both local and remote."""
    local = HLCTimestamp(wall=100, counter=3, node_id="n")
    remote = HLCTimestamp(wall=100, counter=3, node_id="m")
    result = local.update(remote, now_ms=100)
    assert result > local
    assert result > remote


def test_hlctimestamp_update_now_ms_wins() -> None:
    """update must use now_ms as wall when it is strictly the largest."""
    local = HLCTimestamp(wall=100, counter=9, node_id="n")
    remote = HLCTimestamp(wall=100, counter=9, node_id="m")
    result = local.update(remote, now_ms=500)
    assert result.wall == 500
    assert result.counter == 0


def test_hlcclock_initial_state_has_zero_counter() -> None:
    """A freshly created clock must start at counter=0."""
    clock = HLCClock("node-1", initial_wall=0)
    assert clock.state.counter == 0
    assert clock.state.node_id == "node-1"


def test_hlcclock_tick_advances_state() -> None:
    """tick must advance the mutable clock state."""
    clock = HLCClock("n", initial_wall=0)
    ts1 = clock.tick(now_ms=100)
    ts2 = clock.tick(now_ms=100)
    assert ts2 > ts1
    assert clock.state == ts2


def test_hlcclock_update_advances_state_from_remote() -> None:
    """update must advance state past the remote timestamp."""
    clock = HLCClock("local", initial_wall=0)
    remote = HLCTimestamp(wall=999, counter=0, node_id="remote")
    result = clock.update(remote, now_ms=1)
    assert result > remote
    assert clock.state == result


def test_hlcclock_state_is_monotonic_across_ticks() -> None:
    """Consecutive ticks must produce strictly increasing timestamps."""
    clock = HLCClock("n")
    timestamps = [clock.tick(now_ms=1000) for _ in range(10)]
    for i in range(1, len(timestamps)):
        assert timestamps[i] > timestamps[i - 1]


def test_hlcclock_tick_raises_no_error_on_zero_now_ms() -> None:
    """tick must not raise for now_ms=0."""
    clock = HLCClock("n", initial_wall=0)
    ts = clock.tick(now_ms=0)
    assert ts.counter == 1


@pytest.mark.parametrize("node_a,node_b", [("a", "b"), ("alpha", "beta")])
def test_hlctimestamp_node_id_tiebreak_is_deterministic(
    node_a: str, node_b: str
) -> None:
    """For equal wall+counter, ordering is always determined by node_id lexicography."""
    ts_a = HLCTimestamp(wall=1, counter=0, node_id=node_a)
    ts_b = HLCTimestamp(wall=1, counter=0, node_id=node_b)
    assert ts_a < ts_b
