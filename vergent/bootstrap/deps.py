import asyncio
import json
from functools import lru_cache

from pydantic import ValidationError

from vergent.bootstrap.config.loader import get_cli_args
from vergent.bootstrap.config.models import VergentConfig
from vergent.core.app import App
from vergent.core.config import Config
from vergent.core.facade import VergentCore
from vergent.core.p2p.manager import PeerManager
from vergent.infra.local_storage import LMDBStorage, LMDBStorageFactory


@lru_cache
def get_app() -> App:
    app = App()
    return app


@lru_cache
def get_advertise_address() -> str:
    args = get_cli_args()
    if args.advertised_listener:
        return args.advertised_listener
    return f"{args.host}:{args.port}"


@lru_cache
def get_peer_manager() -> PeerManager:
    args = get_cli_args()
    peers = set(args.peers or [])
    peer_manager = PeerManager(
        peers=peers,
        listen=get_advertise_address(),
        storage=get_versioned_storage(),
        ssl_ctx=get_client_ssl_ctx(),
    )
    return peer_manager


@lru_cache
def get_core() -> VergentCore:
    cli = get_cli_args()
    config = get_config()
    storage_factory = LMDBStorageFactory(
        path=config.storage.data_dir
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    host = config.server.host
    port = config.server.port
    advertised_listener = config.advertised.listener or f"{host}:{port}"

    app_config = Config(
        app=get_app(),
        node_id=config.node.id,
        node_size=config.node.size,
        host=host,
        port=port,
        tls_certfile=config.server.tls.certfile,
        tls_keyfile=config.server.tls.keyfile,
        tls_cafile=config.server.tls.cafile,
        backlog=config.server.backlog,
        timeout_graceful_shutdown=config.server.timeout_graceful_shutdown,
        advertised_listener=advertised_listener,
        limit_concurrency=config.server.limit_concurrency,
        max_buffer_size=config.server.max_buffer_size,
        max_message_size=config.server.max_message_size,
        partition_shift=config.placement.shift,
        replication_factor=config.placement.replication_factor
    )
    return VergentCore(
        config=app_config,
        storage_factory=storage_factory,
        log_level=cli.log_level,
    )


@lru_cache
def get_config() -> VergentConfig:
    try:
        return VergentConfig()  # type: ignore[call-arg]
    except FileNotFoundError as ex:
        raise SystemExit(f"Provide a correct configuration file path: {ex}")
    except ValidationError as ex:
        msg = ["Configuration validation failed:"]
        errs = json.loads(ex.json())
        for err in errs:
            msg.append(f"  {'.'.join(err['loc'])}: {err['msg']}")
        raise SystemExit("\n".join(msg))
