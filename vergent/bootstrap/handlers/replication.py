from vergent.bootstrap.deps import get_app, get_core
from vergent.core.model.event import Event
from vergent.core.p2p.conflict import ValueVersion

app = get_app()


@app.request("replicate")
async def replicate(data: dict) -> Event:
    core = get_core()
    storage = core.storage
    key = data["key"]
    version = ValueVersion.from_dict(data["version"])
    await storage.apply_remote_version(key, version)
    return Event(type="ok", payload={"hlc": version.hlc.to_dict(), "source": core.config.advertised_listener})
