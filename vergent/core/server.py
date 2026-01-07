import asyncio
import logging

from vergent.core.config import ServerConfig
from vergent.core.model.state import ServerState
from vergent.core.protocol import Protocol
from vergent.core.utils.sig import signal_handler


class Server:
    def __init__(
        self,
        config: ServerConfig,
        loop: asyncio.AbstractEventLoop,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        self._config = config
        self._loop = loop
        self._stop_event = stop_event or asyncio.Event()
        self.state = ServerState(stop_event=self._stop_event)
        self._logger = logging.getLogger("vergent.core.server")

    def run(self) -> None:
        loop = self._loop
        loop.run_until_complete(self.serve())

    async def start(self) -> asyncio.Server:
        config = self._config
        host = config.host
        port = config.port

        return await self._loop.create_server(
            self.create_protocol,
            host=host,
            port=port,
            backlog=config.backlog,
            ssl=config.ssl_ctx
        )

    async def serve(self) -> None:
        def graceful_exit(*_) -> None:
            self._stop_event.set()

        with signal_handler(graceful_exit):
            await self._serve()

    async def _serve(self) -> None:
        config = self._config
        host = config.host
        port = config.port

        server = await self.start()
        print(f"=========== Listening on '{host}:{port}' ==============")

        await self.loop_forever()
        await self.shutdown(server)

    def create_protocol(self) -> asyncio.Protocol:
        loop = self._loop
        return Protocol(config=self._config, server_state=self.state, loop=loop)

    async def loop_forever(self) -> None:
        await self._stop_event.wait()

    async def shutdown(self, server: asyncio.Server) -> None:
        server.close()

        for connection in self.state.connections.copy():
            connection.shutdown()

        try:
            await asyncio.wait_for(
                self._wait_task_complete(server),
                timeout=self._config.timeout_graceful_shutdown
            )
        except asyncio.TimeoutError:
            self._logger.error(
                f"Cancel {len(self.state.tasks)} running task(s), "
                f"timeout graceful shutdown: {self.state.tasks}"
            )
            for task in self.state.tasks:
                task.cancel("Task cancelled, timeout graceful shutdown exceeded")

    async def _wait_task_complete(self, server: asyncio.Server) -> None:
        if self.state.connections:
            self._logger.info("Waiting for client connections to close.")

        while self.state.connections:
            await asyncio.sleep(0.1)

        if self.state.tasks:
            self._logger.info("Waiting for background tasks to complete.")

        while self.state.tasks:
            await asyncio.sleep(0.1)

        await server.wait_closed()
