import asyncio
import logging
from collections import defaultdict

from vergent.core.bootstrapper import SeedBootstrapper
from vergent.core.bucket import BucketTable
from vergent.core.config import PeerConfig
from vergent.core.model.event import Event
from vergent.core.model.membership import Membership
from vergent.core.model.partition import Partitioner
from vergent.core.model.state import PeerState, NodeMeta
from vergent.core.model.vnode import VNode
from vergent.core.p2p.connection import PeerConnectionPool
from vergent.core.ports.node import NodeMetaStore
from vergent.core.ring import Ring
from vergent.core.space import HashSpace


class PeerLifecycle:
    def __init__(
        self,
        meta_store: NodeMetaStore,
        state: PeerState,
        config: PeerConfig,
        view: BucketTable,
        conns: PeerConnectionPool,
        partitioner: Partitioner,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._meta_store = meta_store
        self._state = state
        self._config = config
        self._view = view
        self._conns = conns
        self._partitioner = partitioner
        self._loop = loop

        self._logger = logging.getLogger("vergent.core.lifecycle")

    async def join(self, gate: asyncio.Event) -> Membership:
        meta = self._meta_store.get()

        if meta.phase == "ready":
            self._logger.info("Node is already in ready phase.")
            membership = self._membership_for_join(meta)
            # todo(souls): race condition if join and drain in short window
            await self._build_ring(membership)
            return membership

        if meta.phase == "idle":
            await gate.wait()
            meta = self._meta_store.get()
            if meta.phase != "idle":
                raise RuntimeError(f"Phase changed unexpectedly during join: {meta.phase}")
            else:
                meta = self._meta_store.set_phase("joining")

        if meta.phase != "joining":
            raise RuntimeError(f"Node phase should be joining but {meta.phase}")

        local_membership = self._membership_for_join(meta)
        pmap = await self._compute_partitions_to_fetch(local_membership)
        partitions = {pid for pids in pmap.values() for pid in pids}
        partitions_task = asyncio.create_task(self._wait_partition(partitions))
        await self._request_partitions(pmap, local_membership)
        await partitions_task
        self._logger.info("Successful sync partitions")

        self._meta_store.set_phase("ready")

        return local_membership

    async def drain(self, gate: asyncio.Event) -> Membership:
        meta = self._meta_store.get()

        if meta.phase in ("ready", "idle"):
            await gate.wait()
            meta = self._meta_store.get()
            if meta.phase in ("ready", "idle"):
                meta = self._meta_store.set_phase("draining")
                membership = self._membership_for_drain(meta)
                await self._build_ring(membership)
                self._logger.info("Node is draining from ring")
                return membership

        if meta.phase == "joining":
            raise RuntimeError("Drain requested while joining is in progress")

        if meta.phase == "draining":
            self._logger.warning("Drain requested but node is already draining")
            membership = self._membership_for_drain(meta)
            await self._build_ring(membership)
            return membership

        raise RuntimeError(f"Unsupported phase for drain: {meta.phase}")

    async def _bootstrap(self) -> list[Membership]:
        bootstrapper = SeedBootstrapper(
            config=self._config,
            loop=self._loop,
            checksums=self._view.get_checksums()
        )
        memberships = await bootstrapper.bootstrap()
        while not memberships:
            self._logger.error("No memberships fetched from seeds, retrying in 5 sec...")
            await asyncio.sleep(5)
            memberships = await bootstrapper.bootstrap()

        return memberships

    async def _compute_partitions_to_fetch(self, local: Membership,) -> dict[str, list[int]]:
        old_ring = await self._discover_ring()
        local_vnodes = VNode.generate_vnodes(local.node_id, local.tokens)
        stolen = await self._compute_stolen_partitions(old_ring, local_vnodes, local)
        self._state.ring = old_ring.add_vnodes(local_vnodes)
        return stolen

    async def _compute_stolen_partitions(
        self,
        ring: Ring,
        vnodes: list[VNode],
        membership: Membership
    ) -> dict[str, list[int]]:
        """
        ring = old ring (before adding our vnodes)
        vnodes = our new vnodes
        """
        stolen = defaultdict(list)

        # Build the new ring by adding our vnodes
        new_ring = ring.add_vnodes(vnodes)

        # For every partition, check if ownership changed
        for pid in range(self._partitioner.total_partitions):
            end = self._partitioner.end_for_pid(pid)

            old_owner = ring.find_successor(end).node_id
            new_owner = new_ring.find_successor(end).node_id

            if new_owner == membership.node_id and old_owner != new_owner:
                stolen[old_owner].append(pid)

            await asyncio.sleep(0.001)  # yield loop control

        return stolen

    async def _discover_ring(self) -> Ring:
        vnodes: list[VNode] = []
        memberships = await self._bootstrap()

        for membership in memberships:
            self._view.add_or_update(membership)
            vnodes.extend(VNode.generate_vnodes(membership.node_id, membership.tokens))
            await asyncio.sleep(0.001)  # yield loop control
            if not self._conns.has(membership.node_id):
                self._conns.register(membership.node_id, membership.peer_address)

        return Ring(vnodes)

    async def _build_ring(self, membership: Membership) -> Ring:
        ring = await self._discover_ring()
        local_vnodes = VNode.generate_vnodes(membership.node_id, membership.tokens)
        self._state.ring = ring.add_vnodes(local_vnodes)
        return ring

    def _membership_for_drain(self, meta: NodeMeta) -> Membership:
        node_id = meta.node_id
        node_size = meta.size.value
        tokens = meta.tokens

        if tokens:
            tokens = []
            self._meta_store.set_tokens(tokens)

        return Membership(
            node_id=node_id,
            size=node_size,
            tokens=tokens,
            peer_address=self._config.peer_listener,
            replication_address=self._config.replication_listener
        )

    def _membership_for_join(self, meta: NodeMeta) -> Membership:
        node_id = meta.node_id
        node_size = meta.size
        tokens: list[int] = meta.tokens

        if not tokens:
            tokens = list(HashSpace.generate_tokens(node_id, node_size.value))
            self._meta_store.set_tokens(tokens)

        return Membership(
            node_id=node_id,
            size=node_size,
            tokens=tokens,
            peer_address=self._config.peer_listener,
            replication_address=self._config.replication_listener
        )

    async def _request_partitions(
        self,
        partitions: dict[str, list[int]],
        membership: Membership
    ) -> None:
        tasks: list[asyncio.Task] = []
        for peer, partitions in partitions.items():
            self._logger.debug(f"Requesting {len(partitions)} stolen partitions to {peer}")
            event = Event(
                type="sync",
                payload={
                    "kind": "partition",
                    "partitions": partitions,
                    "source": membership.node_id,
                    "replication_address": membership.replication_address
                }
            )
            client = self._conns.get(peer)
            tasks.append(asyncio.create_task(client.send(event)))

        if tasks:
            await asyncio.wait(tasks)

    async def _wait_partition(self, partitions: set[int]) -> None:
        pending = partitions.copy()
        if not pending:
            return

        # later to check maybe which peer partition come from
        async for event in self._conns.subscription:
            if event.type == "_sync/partition":
                pid = event.payload["pid"]
                pending.discard(pid)
                if not pending:
                    break
