import asyncio
from dataclasses import dataclass, field


@dataclass
class ServerState:
    connections: set[asyncio.Protocol] = field(default_factory=set)
    tasks: set[asyncio.Task[None]] = field(default_factory=set)
