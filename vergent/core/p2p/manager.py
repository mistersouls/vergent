import asyncio
import logging
import math
import random
import ssl
from typing import Mapping, Any

from vergent.core.model.event import Event
from vergent.core.p2p.client import PeerClientPool
from vergent.core.p2p.conflict import ValueVersion
from vergent.core.p2p.hlc import HLC
from vergent.core.p2p.phi import FailureDetector
from vergent.core.p2p.versionned import VersionedStorage
from vergent.core.p2p.view import MembershipView
from vergent.core.sub import Subscription


class PeerManager:
    def __init__(
        self,
        listen: str,
        peers: set[str],
        storage: VersionedStorage,
        ssl_ctx: ssl.SSLContext
    ) -> None:
        self._listen = listen
        self._peers = set(peers)
        self._storage = storage
        self._ssl_ctx = ssl_ctx
        self._loop = asyncio.get_event_loop()

        self._outgoing: Subscription[Event | None] = Subscription(self._loop)
        self._incoming: Subscription[Event | None] = Subscription(self._loop)
        self._clients = PeerClientPool(self._incoming, self._ssl_ctx, self._loop)

        self._detectors: dict[str, FailureDetector] = {
            peer: FailureDetector()
            for peer in self._peers
        }
        self._view = MembershipView(listen, self._peers | {listen})

        self._sync_locks: dict[str, asyncio.Lock] = {
            peer: asyncio.Lock() for peer in self._peers
        }

        self._logger = logging.getLogger("vergent.core.peering")

    async def manage(self, stop: asyncio.Event) -> None:
        self._logger.info(f"Starting with {len(self._peers)} peers: {sorted(self._peers)}")
        tasks = [
            asyncio.create_task(self.ping_for_ever(stop)),
            asyncio.create_task(self.listen_peers(stop)),
            asyncio.create_task(self.health_check_peers(stop)),
            asyncio.create_task(self.gossip_forever(stop)),
            asyncio.create_task(stop.wait()),
        ]
        self._logger.info(f"Started regular ping: {sorted(self._peers)}")
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        self._logger.info("Stop signal received, shutting down...")

        self._outgoing.publish(None)
        self._incoming.publish(None)
        await asyncio.sleep(0)

        for task in done:
            if exc := task.exception():
                self._logger.error("Exception occurred", exc_info=exc)

        for task in pending:
            task.cancel()

        if pending:
            await asyncio.gather(*pending)

        self._logger.info("Shutdown complete")

    async def ping_for_ever(self, stop: asyncio.Event) -> None:
        for peer in self._peers:
            asyncio.create_task(self._notify_one(stop, peer))

        event = Event(type="ping", payload={"source": self._listen})
        while not stop.is_set():
            await asyncio.sleep(2)  # to review
            if self._peers:
                self._outgoing.publish(event)

    async def listen_peers(self, stop: asyncio.Event) -> None:
        async for event in self._incoming:
            if stop.is_set() or event is None:
                break

            await self._handle_event(stop, event)

    async def health_check_peers(self, stop: asyncio.Event) -> None:
        interval = 5.0  # to review

        while not stop.is_set():
            await asyncio.sleep(interval)
            now = self._loop.time()

            for peer, detector in self._detectors.items():
                entry = self._view.get(peer)
                if entry is None:
                    continue

                phi = detector.compute_phi(now)

                # Optional: skip meaningless φ values (no data or infinite suspicion)
                if phi == 0.0 or math.isinf(phi):
                    continue

                suspect = phi >= detector.threshold

                # Transition alive -> suspect / dead
                if entry.status == "alive" and suspect:
                    self._view.set_suspect(peer)
                    self._logger.warning(f"Peer {peer} -> suspect (φ={phi:.2f})")

                # Transition: suspect/dead → alive
                elif entry.status in ("suspect", "dead") and not suspect:
                    self._view.set_alive(peer)
                    await self._trigger_sync(peer)
                    self._logger.info(f"Peer {peer} -> alive (φ={phi:.2f})")

    async def gossip_forever(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(3)  # to review

            peer = self._choose_random_alive_peer()
            if peer is None:
                continue

            payload = {"source": self._listen, "membership": self._view.snapshot()}
            event = Event(type="gossip", payload=payload)
            client = self._clients.get(peer)
            await client.send(event)
            self._logger.debug(f"Sent gossip membership to {peer}")

    def inject_incoming(self, event: Event) -> None:
        self._incoming.publish(event)

    def inject_outgoing(self, event: Event) -> None:
        self._outgoing.publish(event)

    def register_peer(self, stop: asyncio.Event, peer: str) -> None:
        self._logger.info(f"Discovered new peer via gossip: {peer}")

        self._peers.add(peer)
        self._detectors[peer] = FailureDetector()

        # Start ping loop for this peer
        asyncio.create_task(self._notify_one(stop, peer))

        # Sync with this peer
        self._sync_locks[peer] = asyncio.Lock()
        asyncio.create_task(self._trigger_sync(peer))

    def snapshot_view(self) -> Mapping[str, Mapping[str, Any]]:
        return self._view.snapshot()

    async def _handle_event(self, stop: asyncio.Event, event: Event) -> None:
        match event.type:
            case "pong":
                peer = event.payload.get("from")
                self._pong(peer)
            case "gossip":
                self._apply_gossip(stop, event.payload)
            case "sync/digest":
                await self._handle_sync_digest(event)
            case "sync/fetch":
                await self._handle_sync_fetch(event)

    def _apply_gossip(self, stop: asyncio.Event, payload: Mapping[str, Any]) -> None:
        """
        Apply a received gossip membership payload to the local view.
        """
        membership = payload.get("membership") or {}
        remote_node = payload.get("source")

        if not membership or remote_node is None:
            return

        # 1. Build a clean remote view
        remote_view = self._build_remote_view(remote_node, membership)

        # 2. Register new peers discovered in the gossip
        for peer in remote_view.all_peers():
            if peer != self._listen and peer not in self._peers:
                self.register_peer(stop, peer)

        # 3. Merge remote membership into local membership
        self._view.merge(remote_view)
        self._logger.debug(f"Merged membership from {remote_node}, {len(membership)} entries")

    @staticmethod
    def _build_remote_view(
        remote_node: str,
        membership: Mapping[str, Mapping[str, Any]]
    ) -> MembershipView:
        remote_peers = set(membership.keys())
        remote_view = MembershipView(node=remote_node, initial_peers=remote_peers)

        for peer, data in membership.items():
            entry = remote_view.get(peer)
            entry.status = data["status"]
            entry.epoch = data["epoch"]

        return remote_view

    def _choose_random_alive_peer(self) -> str | None:
        alive = [
            peer
            for peer in self._view.alive_peers()
            if peer != self._listen
        ]
        if not alive:
            return None
        return random.choice(alive)

    async def _handle_sync_digest(self, event: Event):
        remote_digest = event.payload["digest"]
        remote = event.payload["source"]

        local_digest = await self._storage.compute_digest()

        keys_to_fetch = []

        for key, remote_hlc_dict in remote_digest.items():
            remote_hlc = HLC.from_dict(remote_hlc_dict)

            local_hlc_dict = local_digest.get(key)
            if local_hlc_dict is None:
                keys_to_fetch.append(key)
                continue

            local_hlc = HLC.from_dict(dict(local_hlc_dict))

            if remote_hlc > local_hlc:
                keys_to_fetch.append(key)

        if not keys_to_fetch:
            self._logger.info(f"No divergence with {remote}")
            return

        fetch_event = Event(
            type="sync",
            payload={"kind": "fetch", "keys": keys_to_fetch},
        )
        client = self._clients.get(remote)
        await client.send(fetch_event)

        self._logger.info(f"Requested {len(keys_to_fetch)} keys from {remote}")

    async def _handle_sync_fetch(self, event: Event) -> None:
        remote = event.payload["source"]
        versions = event.payload["versions"]

        count = 0
        for key, vdict in versions.items():
            version = ValueVersion.from_dict(vdict)
            await self._storage.apply_remote_version(key, version)
            count += 1

        self._logger.info(f"Applied {count} versions from {remote}")

    async def _notify_one(self, stop: asyncio.Event, address: str) -> None:

        client = self._clients.get(address)
        pending: dict[str, Event] = {}

        async for event in self._outgoing:
            if stop.is_set() or event is None:
                self._logger.info(f"Stopping notify loop for peer {address}")
                await client.close()
                break

            pending[event.type] = event

            # Try to flush all pending events (one per type)
            to_remove = []

            for etype, ev in pending.items():
                try:
                    # to review timeout value
                    await asyncio.wait_for(client.send(ev), timeout=0.2)
                    self._logger.debug(f"Notified event type={etype} to {address}")
                    to_remove.append(etype)
                except asyncio.TimeoutError:
                    self._logger.warning(
                        f"Timeout sending {etype} to {address}; "
                        f"peer may be down, keeping event pending"
                    )

            # Remove successfully sent events
            for etype in to_remove:
                del pending[etype]

            # Yield point to avoid hot loop
            await asyncio.sleep(0)

    def _pong(self, peer: str | None) -> None:
        if peer not in self._detectors:
            self._logger.debug(f"Received pong from unknown peer {peer}")
            return

        self._logger.debug(f"Received pong from {peer}")
        now = self._loop.time()
        detector = self._detectors[peer]
        detector.record_heartbeat(now)

        # Optional: immediate transition dead/suspect -> alive on pong
        entry = self._view.get(peer)
        if entry is None:
            return

        # Transition dead -> alive
        if entry.status in ("suspect", "dead"):
            self._view.set_alive(peer)
            asyncio.create_task(self._trigger_sync(peer))
            phi = detector.compute_phi(now)
            self._logger.info(f"Peer {peer} is now alive (φ={phi:.2f})")

    async def _trigger_sync(self, peer: str) -> None:
        lock = self._sync_locks.get(peer)
        if lock is None:
            return

        if lock.locked():
            self._logger.debug(f"Sync already running for {peer}, skipping")
            return

        async with lock:
            self._logger.info(f"Starting sync with {peer}")
            event = Event(type="sync", payload={"kind": "digest"})
            client = self._clients.get(peer)
            await client.send(event)
            self._logger.info(f"Sync request sent to {peer}")
