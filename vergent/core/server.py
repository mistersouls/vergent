import asyncio

from vergent.core.config import Config
from vergent.core.model.state import ServerState
from vergent.core.protocol import Protocol


class Server:
    def __init__(self, config: Config) -> None:
        self._config = config
        self.state = ServerState()

    def run(self) -> None:
        loop = self._config.loop
        loop.run_until_complete(self.serve())

    async def serve(self) -> None:
        config = self._config
        host = config.host
        port = config.port
        server = await config.loop.create_server(
            self.create_protocol,
            host=host,
            port=port,
            backlog=config.backlog
        )

        async with server:
            print(f"=========== Server started at '{host}:{port}' ==============")
            await server.serve_forever()

    def create_protocol(self) -> asyncio.Protocol:
        loop = self._config.loop
        return Protocol(config=self._config, server_state=self.state, loop=loop)

