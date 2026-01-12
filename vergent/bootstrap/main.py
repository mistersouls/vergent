import asyncio
import logging

from vergent.bootstrap.config.loader import get_cli_args
from vergent.bootstrap.deps import get_core
from vergent.bootstrap.pkg import scan
from vergent.core.facade import VergentCore
from vergent.core.manager import PeerManager
from vergent.core.server import Server
from vergent.core.utils.log import setup_logging
from vergent.core.utils.sig import signal_handler


async def async_run(
    core: VergentCore,
    stop_event: asyncio.Event,
) -> None:
    api_config = core.api_config
    peer_config = core.peer_config
    peer_state = core.peer_state
    loop = core.loop

    logger = logging.getLogger("vergent.bootstrap.main")
    api = Server(config=api_config, loop=loop, stop_event=stop_event)
    peer = Server(config=peer_config, loop=loop, stop_event=stop_event)
    peer_manager = PeerManager(
        config=peer_config,
        state=peer_state,
        conns=core.connection_pools,
        partitioner=core.partitioner,
        view=core.view,
        loop=loop,
        # view=initial_view,
        # partitioner=core.partitioner,
    )

    def graceful_exit(*_) -> None:
        stop_event.set()

    with signal_handler(graceful_exit):
        peer_server = await peer.start()
        await peer_manager.start(stop_event)
        api_server = await api.start()
        logger.info("[main] Node is now fully operational (P2P + API).")

        await stop_event.wait()

        await api.shutdown(api_server)
        await peer.shutdown(peer_server)
        await peer_manager.shutdown()
        await core.connection_pools.close()


def run(core: VergentCore) -> None:
    loop = core.loop
    stop_event = asyncio.Event()

    try:
        loop.run_until_complete(
            async_run(core, stop_event)
        )
    except KeyboardInterrupt:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
        finally:
            loop.close()


@scan("vergent.bootstrap.handlers")
def entrypoint() -> None:
    cli = get_cli_args()
    core = get_core()

    setup_logging(cli.log_level)
    run(core)
