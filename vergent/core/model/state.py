import asyncio
from typing import TYPE_CHECKING, Literal
from dataclasses import dataclass, field

from vergent.core.model.membership import Membership
from vergent.core.model.vnode import VNode
from vergent.core.ring import Ring


if TYPE_CHECKING:
    from vergent.core.protocol import Protocol


PeerPhase = Literal["idle", "joining", "leaving", "normal"]


@dataclass
class ServerState:
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    connections: set[Protocol] = field(default_factory=set)
    tasks: set[asyncio.Task[None]] = field(default_factory=set)


@dataclass
class PeerState:
    membership: Membership
    vnodes: list[VNode]
    ring: Ring = field(default_factory=Ring)
    phase: PeerPhase = "idle"
    tasks: set[asyncio.Task[None]] = field(default_factory=set)
