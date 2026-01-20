from typing import Protocol, Callable, Awaitable, AsyncIterator

from vergent.core.model.event import Event

ReceiveEvent = Callable[[], Awaitable[Event]]

SendEvent = Callable[[Event], Awaitable[None]]


class GatewayProtocol(Protocol):
    async def __call__(self, receive: ReceiveEvent, send: SendEvent) -> None:
        ...


class Storage(Protocol):
    async def get(self, namespace: bytes, key: bytes) -> bytes | None:
        ...

    async def put(self, namespace: bytes, key: bytes, value: bytes) -> None:
        ...

    async def put_many(
        self,
        namespace: bytes,
        items: list[tuple[bytes, bytes]]
    ) -> None:
        ...

    async def delete(self, namespace: bytes, key: bytes) -> None:
        ...

    def iter(
        self,
        namespace: bytes,
        limit: int = -1,
        batch_size: int = 1024
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        ...


class StorageFactory(Protocol):
    def create(self, sid: str) -> Storage:
        ...

    def get_max_namespaces(self) -> int:
        ...
