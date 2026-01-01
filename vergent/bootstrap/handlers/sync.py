from vergent.bootstrap.deps import get_app, get_core
from vergent.core.model.event import Event


app = get_app()


@app.request("sync")
async def sync(data: dict) -> Event:
    core = get_core()
    storage = core.storage
    kind = data["kind"]
    match kind:
        case "digest":
            digest = await storage.compute_digest()
            return Event(type="sync/digest", payload={
                "digest": digest,
                "source": core.config.advertised_listener,
            })
        case "fetch":
            keys = data.get("keys", [])
            versions = {}

            for key in keys:
                v = await storage.get_version(key)
                if v is not None:
                    versions[key] = v.to_dict()

            return Event(type="sync/fetch", payload={
                "versions": versions,
                "source": core.config.advertised_listener,
            })
        case _:
            return Event(type="error", payload={"message": f"Unknow kind sync {kind}"})
