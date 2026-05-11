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
"""Tests for PeerClientPool — persistent mTLS connection pool."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tourillon.core.transport.pool import PeerClientPool


def _make_fake_client(connected: bool = True) -> MagicMock:
    """Build a mock TcpClient that appears connected."""
    c = MagicMock()
    c.is_connected = connected
    c.connect = AsyncMock()
    c.close = AsyncMock()
    return c


@pytest.mark.gossip
async def test_pool_acquire_creates_new_client_on_first_call() -> None:
    """acquire() creates and connects a fresh TcpClient on first call."""
    fake_client = _make_fake_client()

    with patch("tourillon.core.transport.pool.TcpClient", return_value=fake_client):
        pool = PeerClientPool(connect_timeout=1.0)
        client = await pool.acquire("node-1", "10.0.0.1:7701")

    assert client is fake_client
    fake_client.connect.assert_awaited_once_with("10.0.0.1:7701", None)


@pytest.mark.gossip
async def test_pool_acquire_reuses_existing_connected_client() -> None:
    """acquire() returns the same TcpClient when already connected."""
    fake_client = _make_fake_client(connected=True)

    with patch("tourillon.core.transport.pool.TcpClient", return_value=fake_client):
        pool = PeerClientPool(connect_timeout=1.0)
        c1 = await pool.acquire("node-1", "10.0.0.1:7701")
        c2 = await pool.acquire("node-1", "10.0.0.1:7701")

    assert c1 is c2
    # connect called only once
    assert fake_client.connect.await_count == 1


@pytest.mark.gossip
async def test_pool_acquire_reconnects_on_stale_client() -> None:
    """Stale client (is_connected=False) is closed; fresh one created."""
    stale = _make_fake_client(connected=False)
    fresh = _make_fake_client(connected=True)

    with patch("tourillon.core.transport.pool.TcpClient", return_value=fresh):
        pool = PeerClientPool(connect_timeout=1.0)
        pool._clients["node-1"] = stale  # inject stale client directly

        c = await pool.acquire("node-1", "10.0.0.1:7701")

    assert c is fresh
    stale.close.assert_awaited_once()


@pytest.mark.gossip
async def test_pool_release_closes_client() -> None:
    """release() closes and removes the TcpClient for node_id."""
    fake_client = _make_fake_client()

    with patch("tourillon.core.transport.pool.TcpClient", return_value=fake_client):
        pool = PeerClientPool(connect_timeout=1.0)
        await pool.acquire("node-1", "10.0.0.1:7701")
        await pool.release("node-1")

    fake_client.close.assert_awaited_once()
    assert "node-1" not in pool._clients


@pytest.mark.gossip
async def test_pool_close_all_closes_every_client() -> None:
    """close_all() closes all stored TcpClients."""
    c1 = _make_fake_client()
    c2 = _make_fake_client()

    with patch("tourillon.core.transport.pool.TcpClient", side_effect=[c1, c2]):
        pool = PeerClientPool(connect_timeout=1.0)
        await pool.acquire("node-1", "10.0.0.1:7701")
        await pool.acquire("node-2", "10.0.0.2:7701")
        await pool.close_all()

    c1.close.assert_awaited_once()
    c2.close.assert_awaited_once()
    assert len(pool._clients) == 0


@pytest.mark.gossip
async def test_pool_concurrent_acquire_same_node_id_creates_one_client() -> None:
    """N coroutines requesting the same node_id simultaneously produce exactly one TcpClient."""
    create_count = [0]

    class CountingClient:
        def __init__(self) -> None:
            create_count[0] += 1
            self.is_connected = True
            self.connect = AsyncMock()
            self.close = AsyncMock()

    with patch("tourillon.core.transport.pool.TcpClient", side_effect=CountingClient):
        pool = PeerClientPool(connect_timeout=1.0)
        tasks = [
            asyncio.create_task(pool.acquire("node-1", "10.0.0.1:7701"))
            for _ in range(10)
        ]
        results = await asyncio.gather(*tasks)

    # All 10 callers must receive the same client object
    assert len(set(id(r) for r in results)) == 1
    # Only one TcpClient created despite 10 concurrent calls
    assert create_count[0] == 1
