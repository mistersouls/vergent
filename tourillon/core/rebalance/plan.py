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
"""Rebalance plan types: PartitionTransfer, PartitionRangeTransfer, RebalancePlan."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


@dataclass(frozen=True)
class PartitionRangeTransfer:
    """Wire unit for rebalance.plan: a contiguous range of pids with same (src, dst).

    Invariant: pid_start <= pid_end. Using ranges instead of individual pids
    reduces the plan payload from O(partitions) to O(nodes) entries, keeping
    payloads well below MAX_PAYLOAD_DEFAULT even at pshift=17.
    """

    pid_start: int
    pid_end: int
    src: str
    dst: str

    def __len__(self) -> int:
        """Return the number of pids in this range (inclusive)."""
        return self.pid_end - self.pid_start + 1

    def __iter__(self):  # type: ignore[override]
        """Yield each pid in [pid_start, pid_end]."""
        return iter(range(self.pid_start, self.pid_end + 1))

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a wire-compatible dict."""
        return {
            "pid_start": self.pid_start,
            "pid_end": self.pid_end,
            "src": self.src,
            "dst": self.dst,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PartitionRangeTransfer:
        """Deserialise from wire dict; raise ValueError on invalid data."""
        ps = int(data["pid_start"])
        pe = int(data["pid_end"])
        if ps > pe:
            raise ValueError(f"pid_start ({ps}) must be <= pid_end ({pe})")
        return cls(
            pid_start=ps,
            pid_end=pe,
            src=str(data["src"]),
            dst=str(data["dst"]),
        )


def range_transfer_list_from_dict(
    raw: list[dict[str, Any]],
) -> list[PartitionRangeTransfer]:
    """Deserialise and validate a list of PartitionRangeTransfer dicts.

    Enforce the mandatory-merge invariant: two adjacent entries with identical
    (src, dst) MUST NOT satisfy entry_a.pid_end + 1 == entry_b.pid_start.
    Raise ValueError with reason "malformed_transfers" on any violation.
    """
    result: list[PartitionRangeTransfer] = []
    for raw_entry in raw:
        entry = PartitionRangeTransfer.from_dict(raw_entry)
        if result:
            prev = result[-1]
            if (
                prev.src == entry.src
                and prev.dst == entry.dst
                and prev.pid_end + 1 == entry.pid_start
            ):
                raise ValueError("malformed_transfers")
        result.append(entry)
    return result


@dataclass(frozen=True)
class PartitionTransfer:
    """Internal unit used by the applicator for per-pid tracking and staging.

    Never serialised on the wire. Produced by RebalancePlan.expand().
    """

    pid: int
    src: str
    dst: str


@dataclass(frozen=True)
class RebalancePlan:
    """Rebalance plan for one topology epoch.

    ranges is the compact wire form. Call expand() to get the per-pid list
    that the applicator uses for staging and tracking.
    """

    epoch: int
    ranges: tuple[PartitionRangeTransfer, ...]

    def expand(self) -> tuple[PartitionTransfer, ...]:
        """Expand all ranges into individual PartitionTransfer instances."""
        transfers: list[PartitionTransfer] = []
        for r in self.ranges:
            for pid in r:
                transfers.append(PartitionTransfer(pid=pid, src=r.src, dst=r.dst))
        return tuple(transfers)


class TransferState(StrEnum):
    """Lifecycle state of a single pid transfer."""

    PENDING = "pending"
    RUNNING = "running"
    COMMITTED = "committed"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass
class TransferHandle:
    """Mutable tracking record for one in-flight partition transfer.

    cancel_event.set() causes the transfer coroutine to abort cleanly:
    staging.cleanup() is called and WaitGroup.done(pid, False) is signalled.
    last_error holds the human-readable description of the most recent failure,
    or None when no error has occurred.
    """

    transfer: PartitionTransfer
    state: TransferState
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    chunks_done: int = 0
    chunks_total: int | None = None
    bytes_done: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    last_error: str | None = None
