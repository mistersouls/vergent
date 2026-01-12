import json
from functools import lru_cache

from pydantic import ValidationError

from vergent.bootstrap.builder import CoreBuilder
from vergent.bootstrap.config.settings import VergentConfig
from vergent.core.app import App
from vergent.core.facade import VergentCore


@lru_cache
def get_api_app() -> App:
    app = App()
    return app


@lru_cache
def get_peer_app() -> App:
    app = App()
    return app


@lru_cache
def get_core() -> VergentCore:
    config = get_config()
    api = get_api_app()
    peer = get_peer_app()

    builder = CoreBuilder(api=api, peer=peer, config=config)
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
