import asyncio


class FlowControl:
    def __init__(self) -> None:
        self._writable = asyncio.Event()
        self._writable.set()
        self.write_paused = False

    async def drain(self) -> None:
        await self._writable.wait()

    def pause_writing(self) -> None:
        self.write_paused = True
        self._writable.clear()

    def resume_writing(self) -> None:
        if self.write_paused:
            self.write_paused = False
            self._writable.set()
