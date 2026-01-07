from vergent.bootstrap.deps import get_peer_app, get_core
from vergent.core.model.event import Event
from vergent.core.p2p.conflict import ValueVersion


app = get_peer_app()


@app.request("join")
async def join(data: dict) -> Event:
    core = get_core()
    core.incoming.publish(Event(type="join", payload=data))
    return Event(type="ok", payload={"status": "accepted"})


@app.request("ping")
async def ping(data: dict) -> Event:
    core = get_core()
    return Event(type="pong", payload={"from": core.peer_config.advertised_listener})


@app.request("gossip")
async def gossip(data: dict) -> Event:
    core = get_core()
    return Event(
            type="gossip",
            payload={
                "address": core.peer_config.advertised_listener,
                "epoch": core.view.latest_epoch,
                "peer_id": core.peer_config.node_id,
                "checksums": core.view.get_checksums()
            }
        )


@app.request("replicate")
async def replicate(data: dict) -> Event:
    core = get_core()
    storage = core.storage
    key = data["key"]
    version = ValueVersion.from_dict(data["version"])
    await storage.apply_remote_version(key, version)
    return Event(type="ok", payload={"hlc": version.hlc.to_dict(), "source": core.peer_config.advertised_listener})


@app.request("sync")
async def sync(data: dict) -> Event:
    core = get_core()
    storage = core.storage
    kind = data["kind"]
    match kind:
        case "membership":
            bucket_id = data["bucket_id"]
            return Event(
                type="sync/memberships",
                payload={
                    "address": core.peer_config.advertised_listener,
                    "peer_id": core.peer_config.node_id,
                    "memberships": core.view.get_bucket_memberships(bucket_id),
                    "bucket_id": bucket_id,
                }
            )
        case "digest":
            digest = await storage.compute_digest()
            return Event(type="sync/digest", payload={
                "digest": digest,
                "source": core.peer_config.advertised_listener,
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
                "source": core.peer_config.advertised_listener,
            })
        case _:
            return Event(type="error", payload={"message": f"Unknow kind sync {kind}"})
