import asyncio
import logging
from collections import deque

from vergent.core.config import PeerConfig
from vergent.core.model.event import Event
from vergent.core.model.membership import Membership
from vergent.core.p2p.connection import PeerConnection
from vergent.core.p2p.view import View
from vergent.core.sub import Subscription


class SeedBootstrapper:
    def __init__(
        self,
        config: PeerConfig,
        checksums: dict[str, int],
        loop: asyncio.AbstractEventLoop,
        max_epoch_delta: int = 3    # todo(souls): put in config
    ) -> None:
        self._seeds = config.seeds
        self._advertised_listener = config.peer_listener
        self._node_id = config.node_id
        self._node_size = config.node_size
        self._loop = loop
        self._max_epoch_delta = max_epoch_delta
        self._checksums = checksums

        self._subscription = Subscription(loop)
        self._seed_clients = {
            seed: PeerConnection(seed, config.client_ssl_ctx, self._subscription, loop)
            for seed in self._seeds if seed != self._advertised_listener
        }
        self._logger = logging.getLogger("vergent.core.bootstrapper")

    @property
    def quorum_size(self) -> int:
        return (len(self._seeds) // 2) + 1

    async def bootstrap(self) -> list[Membership]:
        views: dict[str, View] = {}
        latest_view: View | None = None
        attempt = 0

        while True:
            attempt += 1
            self._logger.debug(f"[bootstrap] Attempt #{attempt}")
            start = self._loop.time()
            fetch_views = asyncio.create_task(self.fetch_views())

            try:
                self._logger.debug(f"[bootstrap] Sending sync checksums requests")
                await asyncio.wait_for(self.try_once(), timeout=5)
            except asyncio.TimeoutError:
                self._logger.warning(f"[bootstrap] Sending sync checksums timed out")

            elapsed = 5 - (self._loop.time() - start)
            if elapsed < 0:
                self._logger.debug(f"[bootstrap] try_once took too long, restarting attempt")
                continue

            try:
                self._logger.debug(f"[bootstrap] Waiting for fetch_views (timeout={elapsed:.3f}s)")
                views.update(await asyncio.wait_for(fetch_views, timeout=elapsed))
                self._logger.debug(f"[bootstrap] Views fetched from seeds")
                latest_view = self.majority_view(views)
            except asyncio.TimeoutError:
                self._logger.debug("[bootstrap] fetch_views timed out (task auto-cancelled)")

            if latest_view is not None:
                self._logger.info("[bootstrap] Majority view reached, bootstrap complete")
                break

        if not latest_view.buckets:
            self._logger.debug("[bootstrap] the list of checksums is empty from seeds")
            return []

        fetch_memberships = asyncio.create_task(self.fetch_memberships(latest_view))
        await self.trigger_sync_memberships(latest_view)
        self._logger.debug("[bootstrap] Triggered sync memberships")
        memberships = await fetch_memberships
        self._logger.debug(f"[bootstrap] Fetching memberships done with {len(memberships)} memberships")
        await self._close_connections()
        return memberships

    async def fetch_views(self) -> dict[str, View]:
        views: dict[str, View] = {}

        async for event in self._subscription:
            if event.type != "gossip":
                continue

            checksums = event.payload.get("checksums")
            epoch = event.payload.get("epoch")
            seed_id = event.payload.get("peer_id")
            address = event.payload.get("address")

            if (
                checksums is None or
                seed_id is None or
                epoch is None or
                address is None
            ):
                self._logger.warning(f"sync/checksums payload is invalid: {event.payload}")
                continue

            views[seed_id] = View(
                epoch=epoch,
                buckets=checksums,
                address=address,
                peer=seed_id
            )

            if len(views) >= self.quorum_size:
                break

        return views

    async def trigger_sync_memberships(self, view: View) -> None:
        buckets = deque(view.buckets.keys())

        while buckets:
            bucket_id = buckets.popleft()
            event = Event(
                type="sync",
                payload={
                    "kind": "membership",
                    "bucket_id": bucket_id,
                    "source": self._node_id,
                }
            )
            try:
                await self._send_to_seed(view.address, event)
            except Exception as ex:
                self._logger.warning(f"Failed to send sync membership event to seed {view.address}: {ex}")
                buckets.append(bucket_id)

            await asyncio.sleep(0.1)

    async def fetch_memberships(self, view: View) -> list[Membership]:
        memberships: list[Membership] = []
        buckets = view.buckets

        async for event in self._subscription:
            if event.type != "sync/memberships":
                continue

            remote_memberships = event.payload.get("memberships", {})
            remote_bucket_id = event.payload.get("bucket_id")
            if not remote_bucket_id:
                continue

            if remote_bucket_id in buckets:
                del buckets[remote_bucket_id]
                memberships.extend(
                    Membership.from_dict(c)
                    for c in remote_memberships.values()
                )

            if not buckets:
                break

        return memberships

    async def try_once(self) -> None:
        event = Event(
            type="gossip",
            payload={
                "address": self._advertised_listener,
                "peer_id": self._node_id,
                "epoch": 0,
                "checksums": self._checksums
            }
        )
        coros = [
            self._send_to_seed(address, event)
            for address in self._seed_clients
        ]
        await asyncio.gather(*coros, return_exceptions=True)

    def majority_view(self, views: dict[str, View]) -> View | None:
        # Quorum check
        if len(views) < self.quorum_size:
            return None

        # select dominant view
        dominant = max(views.values(), key=lambda v: v.epoch)

        # Filter views that are within acceptable delta
        coherent_views = [
            v for v in views.values()
            if abs(v.epoch - dominant.epoch) <= self._max_epoch_delta
        ]

        # Majority check
        if len(coherent_views) < self.quorum_size:
            return None

        return dominant

    async def _send_to_seed(self, address: str, event: Event) -> None:
        client = self._seed_clients[address]
        try:
            await client.send(event)
        except Exception as ex:
            self._logger.warning(f"Failed to send event to seed {address}: {ex}")

    async def _close_connections(self) -> None:
        for conn in self._seed_clients.values():
            await conn.close()
