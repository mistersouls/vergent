import asyncio
import logging
import math

from vergent.core.model.event import Event
from vergent.core.p2p.client import PeerClientPool
from vergent.core.p2p.phi import FailureDetector
from vergent.core.p2p.view import MembershipView
from vergent.core.sub import Subscription


class PeerManager:
    def __init__(
        self,
        listen: str,
        peers: set[str],
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._listen = listen
        self._peers = set(peers)
        self._loop = loop

        self._outgoing: Subscription[Event | None] = Subscription(loop)
        self._incoming: Subscription[Event | None] = Subscription(loop)
        self._clients = PeerClientPool(self._incoming, loop)
        self._tasks: list[asyncio.Task[None]] = []

        self._detectors: dict[str, FailureDetector] = {
            peer: FailureDetector()
            for peer in self._peers
        }

        self._view = MembershipView(listen, self._peers | {listen})

        self._logger = logging.getLogger("vergent.core.peering")

    async def manage(self, stop: asyncio.Event) -> None:
        self._logger.info(f"Starting with {len(self._peers)} peers: {sorted(self._peers)}")
        tasks = [
            asyncio.create_task(self.ping_for_ever(stop)),
            asyncio.create_task(self.listen_peers(stop)),
            asyncio.create_task(self.health_check_peers(stop)),
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
        tasks = [
            asyncio.create_task(self._ping_one(stop, peer))
            for peer in self._peers
        ]
        self._tasks.extend(tasks)

        event = Event(type="ping", payload={"source": self._listen})
        while not stop.is_set():
            await asyncio.sleep(2)
            self._outgoing.publish(event)

    async def listen_peers(self, stop: asyncio.Event) -> None:
        async for event in self._incoming:
            if stop.is_set() or event is None:
                break

            await self._handle_event(event)

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
                    self._logger.info(f"Peer {peer} -> alive (φ={phi:.2f})")

    async def _handle_event(self, event: Event) -> None:
        match event.type:
            case "pong":
                peer = event.payload.get("from")
                self._pong(peer)

    async def _ping_one(self, stop: asyncio.Event, address: str) -> None:
        async for event in self._outgoing:
            client = self._clients.get(address)

            if stop.is_set() or event is None:
                await client.close()
                break

            await client.send(event)
            self._logger.debug(f"Sent ping to {address}")

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
            phi = detector.compute_phi(now)
            self._logger.info(f"Peer {peer} is now alive (φ={phi:.2f})")
