import asyncio
import logging

from vergent.core.bootstrapper import SeedBootstrapper
from vergent.core.bucket import BucketTable
from vergent.core.config import PeerConfig
from vergent.core.model.event import Event
from vergent.core.model.membership import Membership
from vergent.core.model.partition import Partitioner
from vergent.core.model.state import PeerState
from vergent.core.model.vnode import VNode
from vergent.core.p2p.connection import PeerConnectionPool
from vergent.core.ring import Ring


class PeerManager:
    def __init__(
        self,
        config: PeerConfig,
        state: PeerState,
        conns: PeerConnectionPool,
        partitioner: Partitioner,
        view: BucketTable,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._logger = logging.getLogger("vergent.core.manager")
        self._config = config
        self._state = state
        self._conns = conns
        self._partitioner = partitioner
        self._loop = loop

        self._started = False
        self._joined = False
        self._join_event = asyncio.Event()
        self._leave_event = asyncio.Event()
        self._view = view
        self._ring = Ring(self._state.vnodes)
        self._owned_partitions: set[int] = set()

    @property
    def should_join(self) -> bool:
        seeds = self._config.seeds
        node = self._config.advertised_listener
        return seeds != set() and seeds != {node}

    async def start(self, stop_event: asyncio.Event) -> None:
        if self.should_join:
            await self._join_and_start(stop_event)
        else:
            await self._start_single_node(stop_event)

    async def shutdown(self) -> None:
        if self._state.tasks:
            self._logger.info("Waiting for background tasks to complete.")

        while self._state.tasks:
            await asyncio.sleep(0.1)

    async def _start(self, stop_event: asyncio.Event) -> None:
        gossip = self._loop.create_task(self._gossip_forever(stop_event))
        gossip.add_done_callback(self._state.tasks.discard)
        self._logger.info("Gossip started")

        listen_peers = self._loop.create_task(self._incoming_event(stop_event))
        listen_peers.add_done_callback(self._state.tasks.discard)
        self._logger.info("Listen incoming event")

    async def _start_single_node(self, stop_event: asyncio.Event) -> None:
        self._logger.info("Starting in single-node mode")
        self._view.add_or_update(self._state.membership)
        await self._start(stop_event)

    async def _join_and_start(self, stop_event: asyncio.Event) -> None:
        listen_peers = self._loop.create_task(self._incoming_event(stop_event))
        listen_peers.add_done_callback(self._state.tasks.discard)
        self._logger.info("Listen incoming event")
        self._logger.info("Waiting for JOIN request to start")

        await self._join_event.wait()
        self._logger.info(f"Joining seeds: {self._config.seeds}")

        gossip = self._loop.create_task(self._gossip_forever(stop_event))
        gossip.add_done_callback(self._state.tasks.discard)
        self._logger.info("Gossip started")

    async def _gossip_forever(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(1)

            membership = self._peek_random_member()
            if membership is None:
                continue

            source = self._config.advertised_listener
            checksums = self._view.get_checksums()
            payload = {"source": source, "checksums": checksums}
            event = Event(type="gossip", payload=payload)
            await self._send_event(membership, event)
            self._logger.debug(f"Gossip to {membership.node_id}")

    async def _incoming_event(self, stop_event: asyncio.Event) -> None:
        async for event in self._conns.subscribe():
            if event is None or stop_event.is_set():
                break

            await self._handle_event(event)

    def _peek_random_member(self) -> Membership | None:
        membership = self._view.peek_random_member()
        if membership.node_id != self._state.membership.node_id:
            return membership
        return None

    async def _send_event(self, membership: Membership, event: Event) -> None:
        peer = membership.node_id
        address = membership.address

        if not self._conns.has(peer):
            self._conns.register(peer, address)
        client = self._conns.get(membership.node_id)

        await client.send(event)

    async def _handle_event(self, event: Event) -> None:
        self._logger.debug(f"Received event: {event}")
        match event.type:
            case "join":
                await self._join()
            case "gossip":
                self._logger.debug(f"Received Gossip event: {event}")

    async def _join(self) -> None:
        if self._join_event.is_set():
            self._logger.warning("Node already joined peers")
            return

        bootstrapper = SeedBootstrapper(
            config=self._config,
            state=self._state,
            loop=self._loop,
            checksums=self._view.get_checksums()
        )
        memberships = await bootstrapper.bootstrap()
        while not memberships:
            self._logger.error("No memberships fetched from seeds, retrying in 5 sec...")
            await asyncio.sleep(5)
            memberships = await bootstrapper.bootstrap()

        partitions = await asyncio.to_thread(
            self._compute_partitions_to_fetch, memberships
        )
        self._join_event.set()
        # Start fetching partitions
        self._logger.info(f"Own partitions: {partitions}")

    def _compute_partitions_to_fetch(self, memberships: list[Membership]) -> dict[int, str]:
        vnodes: list[VNode] = []

        for membership in memberships:
            self._view.add_or_update(membership)
            vnodes.extend(VNode.generate_vnodes(membership.node_id, membership.size))

        old_ring = Ring(vnodes)
        local_vnodes = self._state.vnodes
        stolen = self._compute_stolen_partitions(old_ring, local_vnodes)
        self._owned_partitions = set(stolen.keys())
        self._ring = old_ring.update_ring(local_vnodes)
        return stolen

    def _compute_stolen_partitions(self, ring: Ring, vnodes: list[VNode]) -> dict[int, str]:
        stolen = {}

        for vnode in vnodes:
            token = vnode.token
            pid = self._partitioner.pid_for_hash(token)
            prev_pid = pid - 1

            if prev_pid < 0:
                continue

            end = self._partitioner.end_for_pid(prev_pid)
            old = ring.find_successor(end)

            if old.token > end:
                if end <= token < old.token:
                    stolen[prev_pid] = old.node_id
            else:
                if token >= end or token < old.token:
                    stolen[prev_pid] = old.node_id

        return stolen
