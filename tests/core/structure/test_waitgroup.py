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
"""Tests for WaitGroup[T] asyncio synchronization primitive."""

from __future__ import annotations

import asyncio

import pytest

from tourillon.core.structure.waitgroup import WaitGroup


@pytest.mark.gossip
async def test_waitgroup_add_and_done_resolves_wait() -> None:
    """Adding two tasks and signalling done() twice resolves wait() with results."""
    wg: WaitGroup[str] = WaitGroup()
    await wg.add(2)
    await wg.done("a", success=True)
    await wg.done("b", success=False)
    success, failed = await wg.wait()
    assert "a" in success
    assert "b" in failed


@pytest.mark.gossip
async def test_waitgroup_is_done_false_while_pending() -> None:
    """is_done() returns False while any tasks remain."""
    wg: WaitGroup[str] = WaitGroup()
    await wg.add(1)
    assert wg.is_done() is False


@pytest.mark.gossip
async def test_waitgroup_is_done_true_after_all_done() -> None:
    """is_done() returns True after all tasks are marked done."""
    wg: WaitGroup[str] = WaitGroup()
    await wg.add(1)
    await wg.done("x", success=True)
    assert wg.is_done() is True


@pytest.mark.gossip
async def test_waitgroup_new_cycle_clears_previous_results() -> None:
    """Starting a new cycle via add() when counter is 0 clears old results."""
    wg: WaitGroup[str] = WaitGroup()
    await wg.add(1)
    await wg.done("old", success=True)
    await wg.wait()

    await wg.add(1)  # new cycle
    await wg.done("new", success=False)
    success, failed = await wg.wait()
    assert "old" not in success
    assert "new" in failed


@pytest.mark.gossip
async def test_waitgroup_add_zero_is_noop() -> None:
    """add(0) is a no-op and does not start a new cycle or change state."""
    wg: WaitGroup[str] = WaitGroup()
    await wg.add(0)
    # Counter is 0, so wait() should return immediately
    success, failed = await wg.wait()
    assert success == []
    assert failed == []


@pytest.mark.gossip
async def test_waitgroup_add_negative_raises_valueerror() -> None:
    """add(-1) raises ValueError."""
    wg: WaitGroup[str] = WaitGroup()
    with pytest.raises(ValueError, match="negative"):
        await wg.add(-1)


@pytest.mark.gossip
async def test_waitgroup_extra_done_raises_runtimeerror() -> None:
    """Calling done() more times than add() raises RuntimeError."""
    wg: WaitGroup[str] = WaitGroup()
    await wg.add(1)
    await wg.done("a")
    with pytest.raises(RuntimeError, match="Too many calls"):
        await wg.done("b")


@pytest.mark.gossip
async def test_waitgroup_wait_on_empty_returns_immediately() -> None:
    """wait() on a fresh WaitGroup with no tasks returns immediately."""
    wg: WaitGroup[str] = WaitGroup()
    success, failed = await asyncio.wait_for(wg.wait(), timeout=1.0)
    assert success == []
    assert failed == []


@pytest.mark.gossip
async def test_waitgroup_done_with_none_sub_not_recorded() -> None:
    """done(sub=None) decrements counter but records nothing in results."""
    wg: WaitGroup[str] = WaitGroup()
    await wg.add(1)
    await wg.done(None, success=True)
    success, failed = await wg.wait()
    assert success == []
    assert failed == []


@pytest.mark.gossip
async def test_waitgroup_concurrent_tasks_resolve_correctly() -> None:
    """Multiple concurrent tasks all calling done() resolve wait() exactly once."""
    wg: WaitGroup[int] = WaitGroup()
    await wg.add(5)

    async def worker(i: int) -> None:
        await wg.done(i, success=(i % 2 == 0))

    async with asyncio.TaskGroup() as tg:
        for i in range(5):
            tg.create_task(worker(i))

    success, failed = await wg.wait()
    assert set(success) == {0, 2, 4}
    assert set(failed) == {1, 3}
