import asyncio
from typing import TYPE_CHECKING, Literal
from dataclasses import dataclass, field

from vergent.core.model.vnode import SizeClass
from vergent.core.p2p.hlc import HLC
from vergent.core.ring import Ring


if TYPE_CHECKING:
    from vergent.core.protocol import Protocol


NodePhase = Literal["idle", "joining", "draining", "ready"]


@dataclass
class ServerState:
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    connections: set[Protocol] = field(default_factory=set)
    tasks: set[asyncio.Task[None]] = field(default_factory=set)


@dataclass
class PeerState:
    ring: Ring = field(default_factory=Ring)
    tasks: set[asyncio.Task[None]] = field(default_factory=set)


@dataclass
class NodeMeta:
    node_id: str
    size: SizeClass
    hlc: HLC
    phase: NodePhase = "idle"
    tokens: list[int] = field(default_factory=list)
