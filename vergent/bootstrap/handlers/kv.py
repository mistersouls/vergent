import msgpack

from vergent.bootstrap.deps import get_api_app, get_core
from vergent.core.model.event import Event
from vergent.core.model.request import PutRequest

app = get_api_app()


@app.request("get")
async def get(data: dict) -> Event:
    core = get_core()
    versions = await core.coordinator.get(data["key"].encode(), 1, 0.05)
    return Event(type="ok", payload={"versions": versions})


@app.request("put")
async def put(data: dict) -> Event:
    core = get_core()
    value = msgpack.packb(data["value"], use_bin_type=True)
    request = PutRequest(
        request_id=data["request_id"],
        key=data["key"].encode(),
        value=value,
        quorum_write=1,
        timeout=0.05
    )
    return await core.coordinator.put(request)


@app.request("delete")
async def delete(data: dict) -> Event:
    core = get_core()
    key = data["key"].encode()
    version = await core.coordinator.delete(key, 1, 0.05)
    return Event(type="ok", payload={"version": version})

