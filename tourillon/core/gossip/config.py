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
"""GossipBootstrapConfig and GossipConfig — immutable engine configuration."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class GossipBootstrapConfig:
    """Exponential backoff parameters for the gossip bootstrap retry loop.

    Default parameters cover a window of approximately three minutes
    (1 s → 2 s → 4 s → … → 60 s, ten attempts), long enough for most rolling
    restarts to complete while still failing loudly for genuine outages.
    Setting max_retries=0 enables unlimited retries.
    """

    initial_delay_s: float = 1.0  # delay before first retry
    max_delay_s: float = 60.0  # ceiling for exponential growth
    multiplier: float = 2.0  # doubling factor per attempt
    jitter: float = 0.1  # ±10 % uniform jitter to avoid thundering herd
    max_retries: int = 10  # 0 = unlimited retries
    connect_timeout: float = 10.0  # per-seed connection timeout in seconds


@dataclass(frozen=True)
class GossipConfig:
    """Full configuration for the GossipEngine.

    bootstrap holds the retry parameters for the initial full-resync sequence.
    All other fields govern the steady-state hot-path and anti-entropy cycles.
    """

    bootstrap: GossipBootstrapConfig = field(default_factory=GossipBootstrapConfig)
    anti_entropy_interval: float = 30.0  # seconds between AE cycles
    max_fan_out: int = 6  # ceil(log2(N)) + 1, capped here
    max_payload_bytes: int = 1_048_576  # 1 MiB per gossip.push envelope
    max_digest_entries: int = 4_096  # entries per gossip.digest page
    max_gossip_per_peer_rps: float = 20.0  # rate limit per peer
