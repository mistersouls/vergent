from vergent.bootstrap.deps import get_peer_app, get_core
from vergent.core.model.event import Event
from vergent.core.model.request import PutRequest, GetRequest, DeleteRequest
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
    return Event(type="pong", payload={"from": core.peer_config.peer_listener})


@app.request("gossip")
async def gossip(data: dict) -> Event:
    core = get_core()
    core.incoming.publish(Event(type="gossip", payload=data))
    return Event(
            type="gossip",
            payload={
                "address": core.peer_config.peer_listener,
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
    partition = core.partitioner.find_partition_by_key(key)
    new_version = await storage.apply(partition.pid_bytes, key, version)
    payload = {
        "version": new_version,
        "request_id": data.get("request_id"),
        "source": core.peer_config.peer_listener,
    }
    return Event(type="ok", payload=payload)


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
                    "address": core.peer_config.peer_listener,
                    "peer_id": core.peer_config.node_id,
                    "memberships": core.view.get_bucket_memberships(bucket_id),
                    "bucket_id": bucket_id,
                }
            )
        case "fetch":
            keys = data.get("keys", [])
            versions = {}

            for key in keys:
                partition = core.partitioner.find_partition_by_key(key)
                v = await storage.get_version(partition.pid_bytes, key)
                if v is not None:
                    versions[key] = v.to_dict()

            return Event(type="sync/fetch", payload={
                "versions": versions,
                "source": core.peer_config.peer_listener,
                "request_id": data.get("request_id"),
            })
        case "partition":
            partitions = data["partitions"]
            replication_address = data["replication_address"]
            core.pts.accept(replication_address, partitions)
            return Event(type="ok", payload={"sender": core.peer_config.replication_listener})
        case _:
            return Event(type="error", payload={"message": f"Unknow kind sync {kind}"})


@app.request("forward")
async def forward(data: dict) -> Event:
    core = get_core()
    kind = data["kind"]
    key = data["key"]
    request_id = data["request_id"]
    placement = core.coordinator.find_key_placement(key)
    owner = placement.vnode
    node_id = core.peer_state.membership.node_id
    if owner.node_id != node_id:
        return Event(
            type="ko",
            payload={
                "message": f"The node {node_id} is not owner of {key}",
                "request_id": request_id
            }
        )

    match kind:
        case "put":
            request = PutRequest(
                request_id=request_id,
                key=key,
                value=data["value"],
                quorum_write=data["W"],
                timeout=data["timeout"],
            )
            return await core.coordinator.coordinate_put(request, placement)
        case "get":
            request = GetRequest(
                request_id=request_id,
                key=key,
                quorum_read=data["R"],
                timeout=data["timeout"],
            )
            return await core.coordinator.coordinate_get(request, placement)
        case "delete":
            request = DeleteRequest(
                request_id=request_id,
                key=key,
                quorum_write=data["W"],
                timeout=data["timeout"],
            )
            return await core.coordinator.coordinate_delete(request, placement)
        case _:
            return Event(
                type="error",
                payload={
                    "message": f"Unknow kind {kind}",
                    "request_id": data.get("request_id"),
                }
            )
