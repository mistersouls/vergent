import asyncio
import logging
from typing import Callable, Awaitable

from vergent.core.bucket import BucketTable
from vergent.core.config import PeerConfig
from vergent.core.gossip import GossipBucketSync
from vergent.core.lifecycle import PeerLifecycle
from vergent.core.model.event import Event
from vergent.core.model.membership import Membership, MembershipDiff
from vergent.core.model.partition import Partitioner
from vergent.core.model.state import PeerState
from vergent.core.model.vnode import VNode
from vergent.core.p2p.connection import PeerConnectionPool
from vergent.core.ports.node import NodeMetaStore
from vergent.core.space import HashSpace
from vergent.core.sub import Subscription


class PeerManager:
    def __init__(
        self,
        config: PeerConfig,
        state: PeerState,
        meta_store: NodeMetaStore,
        conns: PeerConnectionPool,
        partitioner: Partitioner,
        view: BucketTable,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._logger = logging.getLogger("vergent.core.manager")
        self._config = config
        self._state = state
        self._meta_store = meta_store
        self._conns = conns
        self._partitioner = partitioner
        self._loop = loop
        self._view = view

        node_meta = self._meta_store.get()
        self._membership = Membership(
            node_id=node_meta.node_id,
            size=node_meta.size,
            peer_address=self._config.peer_listener,
            replication_address=self._config.replication_listener,
            tokens=[]
        )
        self._join_event = asyncio.Event()
        self._drain_event = asyncio.Event()
        self._internal_incoming: Subscription[Event | None] = Subscription(self._loop)
        self._bucket_syncer = GossipBucketSync(
            bucket_table=self._view,
            publisher=self._internal_incoming,
            loop=self._loop
        )

        # Event dispatcher
        self._handlers: dict[str, Callable[[Event], Awaitable[None]]] = {
            "join": self._handle_join,
            "drain": self._handle_drain,
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
    def standalone(self) -> bool:
        seeds = self._config.seeds
        node = self._config.peer_listener
        return seeds == set() or seeds == {node}

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
        if not self.standalone:
            lifecycle = PeerLifecycle(
                meta_store=self._meta_store,
                config=self._config,
                state=self._state,
                view=self._view,
                conns=self._conns,
                partitioner=self._partitioner,
                loop=self._loop
            )
            join_task = asyncio.create_task(lifecycle.join(self._join_event))
            drain_task =asyncio.create_task(lifecycle.drain(self._drain_event))
            stop_task = asyncio.create_task(stop_event.wait())
            await asyncio.wait(
                [join_task, drain_task, stop_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_event.is_set():
                self._logger.info("Stop event received, cancel waiting for JOIN/DRAIN")
                join_task.cancel()
                drain_task.cancel()
                return

            self._membership = self._membership_from_tasks([join_task, drain_task])
            stop_task.cancel()
            self._logger.info("Starting in ring mode.")
        else:
            self._logger.info("Starting in single-node mode")
            self._membership = self._standalone_membership()

        # Add ourselves to the view
        self._view.add_or_update(self._membership)

        # Start gossip after join or immediately in single-node mode
        self._spawn(self._gossip_forever(stop_event))
        self._logger.info("Gossip started")

    async def shutdown(self) -> None:
        await self._bucket_syncer.stop()
        self._internal_incoming.publish(None)
        self._conns.subscription.publish(None)

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

            source = self._config.peer_listener
            checksums = self._view.get_checksums()
            payload = {
                "peer_id": source,
                "address": self._membership.peer_address,
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
                "source": self._membership.node_id
            }
        )
        client = self._conns.get(target)
        await client.send(to_send)

    async def _handle_membership(self, event: Event) -> None:
        remote_memberships = event.payload.get("memberships", {})
        bucket_id = event.payload.get("bucket_id")
        peer_id = event.payload.get("peer_id")
        memberships = [
            Membership.from_dict(m)
            for m in remote_memberships.values()
        ]
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
            self._logger.warning("Join gate already enabled.")
            return

        meta = self._meta_store.get()
        if meta.phase == "draining":
            self._logger.info("Waiting draining phase to finish.")
            await self._drain_event.wait()

        self._join_event.set()
        self._drain_event.clear()
        self._logger.info(f"Join gate enabled.")

    async def _handle_drain(self, _: Event) -> None:
        """Triggered when a DRAIN event is received."""
        if self._drain_event.is_set():
            self._logger.warning("Drain gate already enabled.")
            return

        meta = self._meta_store.get()
        if meta.phase == "joining":
            self._logger.info("Waiting joining phase to finish.")
            await self._join_event.wait()

        self._drain_event.set()
        self._join_event.clear()
        self._logger.info(f"Drain gate enabled.")

    def _membership_from_tasks(self, tasks: list[asyncio.Task]) -> Membership:
        membership = None

        for task in tasks:
            if not task.done():
                task.cancel()
                continue

            if ex := task.exception():
                self._logger.warning(ex)
                continue

            if membership is None:
                membership = task.result()
            else:
                raise RuntimeError(
                    "Race condition for membership from different tasks."
                )

        if membership is None:
            raise RuntimeError("Could not determine local membership")

        return membership

    def _peek_random_member(self) -> Membership | None:
        membership = self._view.peek_random_member()
        if membership.node_id != self._membership.node_id:
            return membership
        return None

    async def _send_event(self, membership: Membership, event: Event) -> None:
        peer = membership.node_id
        address = membership.peer_address

        if not self._conns.has(peer):
            self._conns.register(peer, address)

        client = self._conns.get(peer)
        await client.send(event)

    def _standalone_membership(self) -> Membership:
        # to review: self._meta_store.get() is calling multiple
        # time there and in PeerLifecycle
        meta = self._meta_store.get()

        node_id = meta.node_id
        node_size = meta.size
        tokens = meta.tokens

        saved = False
        if not tokens:
            gen_tokens = HashSpace.generate_tokens(node_id, node_size.value)
            tokens = list(gen_tokens)
            saved = True
        if meta.phase != "ready":
            saved = True

        if saved:
            self._meta_store.save(meta)


        return Membership(
            node_id=node_id,
            size=node_size,
            tokens=tokens,
            peer_address=self._config.peer_listener,
            replication_address=self._config.replication_listener
        )

    def _update_ring(self, diff: MembershipDiff) -> None:
        ring = self._state.ring

        # 1. Remove nodes that disappeared
        removed_node_ids = {m.node_id for m in diff.removed}
        if removed_node_ids:
            ring = ring.drop_nodes(removed_node_ids)

        # 2. Apply updates (replace tokens)
        for change in diff.updated:
            before = change.before
            after = change.after

            # Remove old tokens
            ring = ring.drop_nodes({before.node_id})

            # Add new tokens
            ring = ring.add_vnodes(VNode.generate_vnodes(after.node_id, after.tokens))

        # 3. Add new nodes
        for membership in diff.added:
            ring = ring.add_vnodes(
                VNode.generate_vnodes(membership.node_id, membership.tokens)
            )

        self._state.ring = ring

        self._logger.info(
            f"Ring updated from bucket {diff.bucket_id}: "
            f"{len(diff.added)} added, "
            f"{len(diff.removed)} removed, "
            f"{len(diff.updated)} updated"
        )
