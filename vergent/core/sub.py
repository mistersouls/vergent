import asyncio


class Subscription[T]:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self.waiter = self._loop.create_future()

    def publish(self, value: T):
        waiter = self.waiter
        self.waiter = self._loop.create_future()
        waiter.set_result((value, self.waiter))

    async def subscribe(self):
        waiter = self.waiter
        while True:
            value, waiter = await waiter
            yield value

    __aiter__ = subscribe
