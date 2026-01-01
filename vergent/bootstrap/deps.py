import asyncio
import json
from functools import lru_cache

from pydantic import ValidationError

from vergent.bootstrap.builder import CoreBuilder
from vergent.bootstrap.config.loader import get_cli_args
from vergent.bootstrap.config.settings import VergentConfig
from vergent.core.app import App
from vergent.core.facade import VergentCore
from vergent.core.p2p.manager import PeerManager


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
    config = get_config()
    app = get_app()

    builder = CoreBuilder(app=app, config=config)
    return builder.build()


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
