import asyncio
import logging
from typing import Callable, Awaitable

from vergent.core.bootstrapper import SeedBootstrapper
from vergent.core.bucket import BucketTable
from vergent.core.config import PeerConfig
from vergent.core.gossip import GossipBucketSync
from vergent.core.model.event import Event
from vergent.core.model.membership import Membership, MembershipDiff
from vergent.core.model.partition import Partitioner
from vergent.core.model.state import PeerState
from vergent.core.model.vnode import VNode
from vergent.core.p2p.connection import PeerConnectionPool
from vergent.core.ring import Ring
from vergent.core.sub import Subscription


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
        self._view = view

        self._started = False
        self._joined = False
        self._join_event = asyncio.Event()
        self._leave_event = asyncio.Event()
        self._owned_partitions: set[int] = set()
        self._internal_incoming: Subscription[Event | None] = Subscription(self._loop)
        self._bucket_syncer = GossipBucketSync(
            bucket_table=self._view,
            publisher=self._internal_incoming,
            loop=self._loop
        )

        # Event dispatcher
        self._handlers: dict[str, Callable[[Event], Awaitable[None]]] = {
            "join": self._handle_join,
            "gossip": self._handle_gossip,
            "_sync/memberships": self._handle_internal_membership,
            "sync/memberships": self._handle_membership
        }

    def _spawn(self, coro):
        """Spawn a task and track it in state.tasks."""
        task = self._loop.create_task(coro)
        self._state.tasks.add(task)
        task.add_done_callback(self._state.tasks.discard)
        return task

    @property
    def should_join(self) -> bool:
        seeds = self._config.seeds
        node = self._config.advertised_listener
        return seeds != set() and seeds != {node}

    async def start(self, stop_event: asyncio.Event) -> None:
        """Unified startup pipeline."""
        self._logger.info("Starting PeerManager")

        # Start incoming listeners first
        self._spawn(self._incoming_event(self._conns.subscription, stop_event))
        self._spawn(self._incoming_event(self._internal_incoming, stop_event))
        self._logger.info("Incoming event listeners started")

        # Start gissip syncer
        self._bucket_syncer.start()
        self._logger.info("Gossip bucket syncer started")

        # Wait for join event
        if self.should_join:
            self._logger.info("Waiting for JOIN event before starting gossip")
            await self._join_event.wait()

        # Add ourselves to the view
        self._logger.info("Starting in single-node mode")
        self._view.add_or_update(self._state.membership)

        # Start gossip after join or immediately in single-node mode
        self._spawn(self._gossip_forever(stop_event))
        self._logger.info("Gossip started")

    async def shutdown(self) -> None:
        await self._bucket_syncer.stop()

        if self._state.tasks:
            self._logger.info("Waiting for background tasks to complete.")

        while self._state.tasks:
            await asyncio.sleep(0.1)

    async def _gossip_forever(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(1)

            membership = self._peek_random_member()
            if membership is None:
                continue

            source = self._config.advertised_listener
            checksums = self._view.get_checksums()
            payload = {
                "peer_id": source,
                "address": self._state.membership.address,
                "checksums": checksums
            }

            event = Event(type="gossip", payload=payload)
            await self._send_event(membership, event)
            self._logger.debug(f"Gossip sent to {membership.node_id}")

    async def _handle_gossip(self, event: Event) -> None:
        """Placeholder for real anti-entropy logic."""
        checksums = event.payload.get("checksums")
        source = event.payload["peer_id"]
        address = event.payload["address"]
        if not self._conns.has(source):
            self._conns.register(source, address)

        await self._bucket_syncer.handle_gossip(checksums, source)

    async def _handle_internal_membership(self, event: Event) -> None:
        target = event.payload["target"]
        to_send = Event(
            type="sync",
            payload={
                "kind": "membership",
                "bucket_id": event.payload["bucket_id"],
                "source": self._state.membership.node_id
            }
        )
        client = self._conns.get(target)
        await client.send(to_send)

    async def _handle_membership(self, event: Event) -> None:
        remote_memberships = event.payload.get("memberships", {})
        bucket_id = event.payload.get("bucket_id")
        peer_id = event.payload.get("peer_id")
        memberships = [Membership(**m) for m in remote_memberships.values()]
        diff = self._bucket_syncer.sync_memberships(bucket_id, peer_id, memberships)
        if diff.changed:
            self._update_ring(diff)

    async def _incoming_event(
        self,
        subscription: Subscription[Event | None],
        stop_event: asyncio.Event
    ) -> None:
        async for event in subscription:
            if event is None or stop_event.is_set():
                break
            await self._handle_event(event)

    async def _handle_event(self, event: Event) -> None:
        self._logger.debug(f"Received event: {event.type}")
        handler = self._handlers.get(event.type)
        if handler:
            await handler(event)
        else:
            self._logger.warning(f"Unknown event type: {event.type}")

    async def _handle_join(self, _: Event) -> None:
        """Triggered when a JOIN event is received."""
        if self._join_event.is_set():
            self._logger.warning("Node already joined peers")
            return

        bootstrapper = SeedBootstrapper(
            config=self._config,
            state=self._state,
            loop=self._loop,
            checksums=self._view.get_checksums(),
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
        self._logger.info(f"Own partitions: {partitions}")

    def _compute_partitions_to_fetch(self, memberships: list[Membership]) -> dict[int, str]:
        vnodes: list[VNode] = []

        for membership in memberships:
            self._view.add_or_update(membership)
            vnodes.extend(VNode.generate_vnodes(membership.node_id, membership.size.value))

        old_ring = Ring(vnodes)
        local_vnodes = self._state.vnodes

        stolen = self._compute_stolen_partitions(old_ring, local_vnodes)
        self._owned_partitions = set(stolen.keys())

        self._state.ring = old_ring.add_vnodes(local_vnodes)
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

        client = self._conns.get(peer)
        await client.send(event)

    def _update_ring(self, diff: MembershipDiff) -> None:
        ring = self._state.ring

        removed_node_ids = {m.node_id for m in diff.removed}
        vnodes_to_add: list[VNode] = []
        # Collect trims (size decreases)
        trims: dict[str, int] = {}

        for change in diff.updated:
            before = change.before
            after = change.after

            if after.size > before.size:
                delta = after.size.value - before.size.value
                vnodes_to_add.extend(VNode.generate_vnodes(after.node_id, delta))

            elif after.size < before.size:
                delta = before.size.value - after.size.value
                trims[after.node_id] = trims.get(after.node_id, 0) + delta


        for membership in diff.added:
            vnodes_to_add.extend(
                VNode.generate_vnodes(membership.node_id, membership.size.value)
            )

        if removed_node_ids:
            ring = ring.drop_nodes(removed_node_ids)

        if trims:
            ring = ring.trim_nodes(trims)

        if vnodes_to_add:
            ring = ring.add_vnodes(vnodes_to_add)

        self._state.ring = ring

        self._logger.info(
            f"Ring updated from bucket {diff.bucket_id}: "
            f"+{len(diff.added)} added, "
            f"-{len(diff.removed)} removed, "
            f"{len(diff.updated)} updated"
        )
