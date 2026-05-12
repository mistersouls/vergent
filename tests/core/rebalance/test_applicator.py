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
"""Tests for RebalanceApplicator — proposal 005 applicator scenarios."""

from __future__ import annotations

import asyncio

import pytest

from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.rebalance.applicator import RebalanceApplicator
from tourillon.core.rebalance.plan import (
    PartitionRangeTransfer,
    RebalancePlan,
    TransferState,
)
from tourillon.core.testing.mem_storage import InMemoryStorage


class _MockStatePort:
    """In-memory StatePort for tests."""

    def __init__(self, node_id: str = "self") -> None:
        self._state = NodeState(
            node_id=node_id,
            phase=MemberPhase.JOINING,
            generation=1,
            seq=0,
            tokens=(),
            epoch=1,
            committed_pids=(),
            staging_pids=(),
        )

    async def load(self) -> NodeState:
        return self._state

    async def save(self, state: NodeState) -> None:
        self._state = state


class _MockPool:
    """Mock PeerClientPool that always raises ConnectionError (network failure)."""

    async def acquire(self, node_id: str, address: str) -> object:
        raise ConnectionError(f"Cannot connect to {node_id} at {address}")


class _MockPoolSuccess:
    """Mock pool that returns a fake client able to handle one transfer."""

    def __init__(self, client: _FakeClient) -> None:
        self._client = client

    async def acquire(self, node_id: str, address: str) -> _FakeClient:
        return self._client


class _FakeStreamIter:
    def __init__(self, items: list[object]) -> None:
        self._items = iter(items)

    def __aiter__(self) -> _FakeStreamIter:
        return self

    async def __anext__(self) -> object:
        try:
            return next(self._items)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


class _FakeClient:
    """Fake TcpClient that returns predetermined transfer stream."""

    def __init__(self, stream_responses: list[object]) -> None:
        self._stream_responses = stream_responses
        self._sent: list[object] = []

    def stream(self, env: object) -> _FakeStreamIter:
        return _FakeStreamIter(self._stream_responses)

    async def send(self, env: object) -> None:
        self._sent.append(env)

    @property
    def is_connected(self) -> bool:
        return True

    async def close(self) -> None:
        pass


class _FakePool:
    """Mock pool returning a configurable _FakeClient."""

    def __init__(self, client: _FakeClient) -> None:
        self._client = client

    async def acquire(self, node_id: str, address: str) -> _FakeClient:
        return self._client


class _MockSerializer:
    schema_id = 1

    def encode(self, obj: object) -> bytes:
        import json

        return json.dumps(obj, default=str).encode()

    def decode(self, data: bytes) -> object:
        import json

        return json.loads(data)


def _plan(pids: list[int], src: str = "source", dst: str = "self") -> RebalancePlan:
    """Build a RebalancePlan from a list of pids."""
    if not pids:
        return RebalancePlan(epoch=1, ranges=())
    ranges = tuple(
        PartitionRangeTransfer(pid_start=p, pid_end=p, src=src, dst=dst) for p in pids
    )
    return RebalancePlan(epoch=1, ranges=ranges)


@pytest.mark.rebalance
async def test_12_second_apply_no_overlap_cancels_old() -> None:
    """Second apply() with no overlapping pids → old pids cancelled, new ones started."""
    storage = InMemoryStorage()
    state_port = _MockStatePort()
    pool = _MockPool()
    ser = _MockSerializer()

    applicator = RebalanceApplicator(
        node_id="self",
        pool=pool,  # type: ignore[arg-type]
        state_port=state_port,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        serializer=ser,  # type: ignore[arg-type]
        peer_addresses={"source": "source:7700"},
        max_concurrent_transfers=4,
    )

    plan1 = _plan([1, 2])
    await applicator.apply(plan1)
    handles_after_first = dict(applicator._handles)

    plan2 = _plan([3, 4])
    await applicator.apply(plan2)

    for pid in [1, 2]:
        assert handles_after_first[pid].cancel_event.is_set()
    assert 3 in applicator._handles
    assert 4 in applicator._handles


@pytest.mark.rebalance
async def test_13_committed_pid_skips_transfer() -> None:
    """apply() with a pid in committed_pids → transfer exits immediately (idempotent)."""
    storage = InMemoryStorage()
    state_port = _MockStatePort()
    state_port._state = NodeState(
        node_id="self",
        phase=MemberPhase.JOINING,
        generation=1,
        seq=0,
        tokens=(),
        epoch=1,
        committed_pids=(42,),
        staging_pids=(),
    )
    pool = _MockPool()
    ser = _MockSerializer()

    applicator = RebalanceApplicator(
        node_id="self",
        pool=pool,  # type: ignore[arg-type]
        state_port=state_port,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        serializer=ser,  # type: ignore[arg-type]
        peer_addresses={"source": "source:7700"},
    )

    plan = _plan([42])
    await applicator.apply(plan)
    await asyncio.sleep(0.05)  # let coroutine run

    h = applicator._handles.get(42)
    assert h is not None
    assert h.state == TransferState.COMMITTED


@pytest.mark.rebalance
async def test_14_cancel_event_cleanup() -> None:
    """Transfer cancel_event set → staging.cleanup() called; WaitGroup done(False)."""
    storage = InMemoryStorage()
    pid = 5
    store = storage.open_partition(pid)
    store.staging(1)  # Ensure staging partition exists.
    state_port = _MockStatePort()
    pool = _MockPool()
    ser = _MockSerializer()

    applicator = RebalanceApplicator(
        node_id="self",
        pool=pool,  # type: ignore[arg-type]
        state_port=state_port,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        serializer=ser,  # type: ignore[arg-type]
        peer_addresses={"source": "source:7700"},
    )

    plan = _plan([pid])
    await applicator.apply(plan)
    handle = applicator._handles[pid]
    handle.cancel_event.set()

    await asyncio.sleep(0.1)
    assert handle.state in (TransferState.CANCELLED, TransferState.FAILED)


@pytest.mark.rebalance
async def test_25_status_no_plan_empty_response() -> None:
    """Applicator with no handles → status returns empty transfers, epoch=None."""
    storage = InMemoryStorage()
    state_port = _MockStatePort()
    pool = _MockPool()
    ser = _MockSerializer()

    applicator = RebalanceApplicator(
        node_id="self",
        pool=pool,  # type: ignore[arg-type]
        state_port=state_port,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        serializer=ser,  # type: ignore[arg-type]
        peer_addresses={},
    )

    result = await applicator.status()
    assert result["epoch"] is None
    assert result["active_partitions"] == 0
    assert result["transfers"] == []
    assert result["blocked"] is False


@pytest.mark.rebalance
async def test_40_status_blocked_true_when_failed() -> None:
    """1 RUNNING + 1 FAILED handle → blocked=True; summary reflects counts."""
    storage = InMemoryStorage()
    state_port = _MockStatePort()
    pool = _MockPool()
    ser = _MockSerializer()

    applicator = RebalanceApplicator(
        node_id="self",
        pool=pool,  # type: ignore[arg-type]
        state_port=state_port,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        serializer=ser,  # type: ignore[arg-type]
        peer_addresses={"source": "source:7700"},
    )

    plan = _plan([1, 2])
    applicator._plan = plan
    from tourillon.core.rebalance.plan import PartitionTransfer, TransferHandle

    h1 = TransferHandle(
        transfer=PartitionTransfer(pid=1, src="source", dst="self"),
        state=TransferState.RUNNING,
    )
    h2 = TransferHandle(
        transfer=PartitionTransfer(pid=2, src="source", dst="self"),
        state=TransferState.FAILED,
        last_error="unreachable",
    )
    applicator._handles = {1: h1, 2: h2}

    result = await applicator.status()
    assert result["blocked"] is True
    assert result["summary"]["running"] == 1
    assert result["summary"]["failed"] == 1


def _build_transfer_stream(ser: _MockSerializer, pid: int) -> list[object]:
    """Return pre-canned stream responses for a successful single-pid transfer."""
    from tourillon.core.structure.envelope import Envelope

    plan_ok = Envelope(
        kind="rebalance.plan.ok",
        payload=ser.encode({"epoch": 1}),
    )
    transfer_payload = ser.encode(
        {
            "epoch": 1,
            "pid": pid,
            "chunk_seq": 0,
            "is_last": True,
            "records": [],
        }
    )
    transfer_env = Envelope(kind="rebalance.transfer", payload=transfer_payload)
    commit_ok = Envelope(
        kind="rebalance.commit.ok",
        payload=ser.encode({"epoch": 1}),
    )
    return [plan_ok, transfer_env, commit_ok]


@pytest.mark.rebalance
async def test_11_applicator_transfer_success_committed_pid() -> None:
    """applicator.apply(plan) succeeds → committed_pids includes pid; WaitGroup done."""
    pid = 42
    ser = _MockSerializer()
    fake_client = _FakeClient(_build_transfer_stream(ser, pid))
    pool = _FakePool(fake_client)
    storage = InMemoryStorage()
    state_port = _MockStatePort()

    applicator = RebalanceApplicator(
        node_id="self",
        pool=pool,  # type: ignore[arg-type]
        state_port=state_port,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        serializer=ser,  # type: ignore[arg-type]
        peer_addresses={"source": "source:7700"},
    )

    plan = _plan([pid])
    await applicator.apply(plan)
    await asyncio.sleep(0.2)

    h = applicator._handles.get(pid)
    assert h is not None
    assert h.state == TransferState.COMMITTED
    loaded = await state_port.load()
    assert pid in loaded.committed_pids


@pytest.mark.rebalance
async def test_15_digest_mismatch_marks_failed_immediately() -> None:
    """Source returns rebalance.commit.reject → FAILED immediately, no retry."""
    pid = 7
    ser = _MockSerializer()
    from tourillon.core.structure.envelope import Envelope

    plan_ok = Envelope(kind="rebalance.plan.ok", payload=ser.encode({"epoch": 1}))
    transfer_payload = ser.encode(
        {
            "epoch": 1,
            "pid": pid,
            "chunk_seq": 0,
            "is_last": True,
            "records": [],
        }
    )
    transfer_env = Envelope(kind="rebalance.transfer", payload=transfer_payload)
    commit_reject = Envelope(
        kind="rebalance.commit.reject", payload=ser.encode({"epoch": 1})
    )
    fake_client = _FakeClient([plan_ok, transfer_env, commit_reject])
    pool = _FakePool(fake_client)
    storage = InMemoryStorage()
    state_port = _MockStatePort()

    applicator = RebalanceApplicator(
        node_id="self",
        pool=pool,  # type: ignore[arg-type]
        state_port=state_port,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        serializer=ser,  # type: ignore[arg-type]
        peer_addresses={"source": "source:7700"},
    )

    plan = _plan([pid])
    await applicator.apply(plan)
    await asyncio.sleep(0.2)

    h = applicator._handles.get(pid)
    assert h is not None
    assert h.state == TransferState.FAILED
    assert h.last_error is not None


@pytest.mark.rebalance
async def test_16_plan_reject_epoch_mismatch_marks_failed() -> None:
    """Source returns rebalance.plan.reject epoch_mismatch → FAILED immediately, no retry."""
    pid = 9
    ser = _MockSerializer()
    from tourillon.core.structure.envelope import Envelope

    plan_reject = Envelope(
        kind="rebalance.plan.reject",
        payload=ser.encode({"reason": "epoch_mismatch"}),
    )
    fake_client = _FakeClient([plan_reject])
    pool = _FakePool(fake_client)
    storage = InMemoryStorage()
    state_port = _MockStatePort()

    applicator = RebalanceApplicator(
        node_id="self",
        pool=pool,  # type: ignore[arg-type]
        state_port=state_port,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        serializer=ser,  # type: ignore[arg-type]
        peer_addresses={"source": "source:7700"},
    )

    plan = _plan([pid])
    await applicator.apply(plan)
    await asyncio.sleep(0.2)

    h = applicator._handles.get(pid)
    assert h is not None
    assert h.state == TransferState.FAILED


@pytest.mark.rebalance
async def test_29_all_retries_exhausted_marks_failed() -> None:
    """All max_retries exhausted (connection error) → FAILED; staging cleanup; phase not advanced."""
    pid = 3
    storage = InMemoryStorage()
    state_port = _MockStatePort()
    pool = _MockPool()  # always raises ConnectionError
    ser = _MockSerializer()

    applicator = RebalanceApplicator(
        node_id="self",
        pool=pool,  # type: ignore[arg-type]
        state_port=state_port,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        serializer=ser,  # type: ignore[arg-type]
        peer_addresses={"source": "source:7700"},
        max_concurrent_transfers=4,
    )
    # Shorten backoff so the test completes quickly.
    import tourillon.core.rebalance.applicator as _app_mod

    _orig_max = _app_mod._MAX_RETRIES
    _orig_delay = _app_mod._BACKOFF_INITIAL
    _app_mod._MAX_RETRIES = 2
    _app_mod._BACKOFF_INITIAL = 0.01

    try:
        plan = _plan([pid])
        await applicator.apply(plan)
        await asyncio.sleep(0.5)
    finally:
        _app_mod._MAX_RETRIES = _orig_max
        _app_mod._BACKOFF_INITIAL = _orig_delay

    h = applicator._handles.get(pid)
    assert h is not None
    assert h.state == TransferState.FAILED


@pytest.mark.rebalance
async def test_42_commit_reject_process_error_no_retry() -> None:
    """rebalance.commit.reject (digest mismatch) → FAILED; last_error set; no retry attempted."""
    pid = 55
    ser = _MockSerializer()
    from tourillon.core.structure.envelope import Envelope

    plan_ok = Envelope(kind="rebalance.plan.ok", payload=ser.encode({"epoch": 1}))
    transfer_payload = ser.encode(
        {
            "epoch": 1,
            "pid": pid,
            "chunk_seq": 0,
            "is_last": True,
            "records": [],
        }
    )
    transfer_env = Envelope(kind="rebalance.transfer", payload=transfer_payload)
    commit_reject = Envelope(
        kind="rebalance.commit.reject", payload=ser.encode({"epoch": 1})
    )
    fake_client = _FakeClient([plan_ok, transfer_env, commit_reject])
    call_count = 0

    class _CountingPool:
        async def acquire(self, node_id: str, addr: str) -> _FakeClient:
            nonlocal call_count
            call_count += 1
            return fake_client

    storage = InMemoryStorage()
    state_port = _MockStatePort()

    applicator = RebalanceApplicator(
        node_id="self",
        pool=_CountingPool(),  # type: ignore[arg-type]
        state_port=state_port,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        serializer=ser,  # type: ignore[arg-type]
        peer_addresses={"source": "source:7700"},
    )

    plan = _plan([pid])
    await applicator.apply(plan)
    await asyncio.sleep(0.2)

    h = applicator._handles.get(pid)
    assert h is not None
    assert h.state == TransferState.FAILED
    assert h.last_error is not None
    # Process error → no retries; acquire called exactly once.
    assert call_count == 1


@pytest.mark.rebalance
async def test_50_drain_transfer_success_committed_pid() -> None:
    """DRAIN path: applicator sends plan, streams chunks, validates commit → committed."""
    pid = 77
    ser = _MockSerializer()
    from tourillon.core.rebalance.digest import compute_transfer_digest
    from tourillon.core.structure.envelope import Envelope

    # DST sends plan.ok with no resume_from (fresh transfer).
    plan_ok = Envelope(
        kind="rebalance.plan.ok", payload=ser.encode({"epoch": 1, "resume_from": None})
    )
    # DST sends commit after receiving all chunks.
    expected_digest = compute_transfer_digest(iter([]))  # empty store → empty digest
    commit = Envelope(
        kind="rebalance.commit",
        payload=ser.encode({"epoch": 1, "pid": pid, "digest": expected_digest}),
    )
    fake_client = _FakeClient([plan_ok, commit])
    pool = _FakePool(fake_client)
    storage = InMemoryStorage()
    state_port = _MockStatePort()

    applicator = RebalanceApplicator(
        node_id="self",
        pool=pool,  # type: ignore[arg-type]
        state_port=state_port,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        serializer=ser,  # type: ignore[arg-type]
        peer_addresses={"dst": "dst:7700"},
    )

    # DRAIN: src == self
    ranges = (
        PartitionRangeTransfer(pid_start=pid, pid_end=pid, src="self", dst="dst"),
    )
    plan = RebalancePlan(epoch=1, ranges=ranges)
    await applicator.apply(plan)
    await asyncio.sleep(0.2)

    h = applicator._handles.get(pid)
    assert h is not None
    assert h.state == TransferState.COMMITTED
    loaded = await state_port.load()
    assert pid in loaded.committed_pids
    # Source must have sent at least one transfer chunk + commit.ok via client.send()
    sent_kinds = [e.kind for e in fake_client._sent]  # type: ignore[union-attr]
    assert "rebalance.transfer" in sent_kinds
    assert "rebalance.commit.ok" in sent_kinds


@pytest.mark.rebalance
async def test_51_drain_transfer_digest_mismatch_marks_failed() -> None:
    """DRAIN path: destination sends wrong digest → source marks FAILED, sends commit.reject."""
    pid = 78
    ser = _MockSerializer()
    from tourillon.core.structure.envelope import Envelope

    plan_ok = Envelope(kind="rebalance.plan.ok", payload=ser.encode({"epoch": 1}))
    # Destination sends a bogus digest.
    commit = Envelope(
        kind="rebalance.commit",
        payload=ser.encode({"epoch": 1, "pid": pid, "digest": "badhash"}),
    )
    fake_client = _FakeClient([plan_ok, commit])
    pool = _FakePool(fake_client)
    storage = InMemoryStorage()
    state_port = _MockStatePort()

    applicator = RebalanceApplicator(
        node_id="self",
        pool=pool,  # type: ignore[arg-type]
        state_port=state_port,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        serializer=ser,  # type: ignore[arg-type]
        peer_addresses={"dst": "dst:7700"},
    )

    ranges = (
        PartitionRangeTransfer(pid_start=pid, pid_end=pid, src="self", dst="dst"),
    )
    plan = RebalancePlan(epoch=1, ranges=ranges)
    await applicator.apply(plan)
    await asyncio.sleep(0.2)

    h = applicator._handles.get(pid)
    assert h is not None
    assert h.state == TransferState.FAILED
    sent_kinds = [e.kind for e in fake_client._sent]  # type: ignore[union-attr]
    assert "rebalance.commit.reject" in sent_kinds


@pytest.mark.rebalance
async def test_52_do_transfer_wrong_direction_marks_failed() -> None:
    """Transfer where neither src nor dst matches node_id → FAILED immediately."""
    pid = 99
    ser = _MockSerializer()
    pool = _MockPool()
    storage = InMemoryStorage()
    state_port = _MockStatePort()

    applicator = RebalanceApplicator(
        node_id="self",
        pool=pool,  # type: ignore[arg-type]
        state_port=state_port,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        serializer=ser,  # type: ignore[arg-type]
        peer_addresses={},
    )

    # Create a plan where neither src nor dst is "self"
    ranges = (
        PartitionRangeTransfer(pid_start=pid, pid_end=pid, src="node-a", dst="node-b"),
    )
    plan = RebalancePlan(epoch=1, ranges=ranges)
    await applicator.apply(plan)
    await asyncio.sleep(0.1)

    h = applicator._handles.get(pid)
    assert h is not None
    assert h.state == TransferState.FAILED
