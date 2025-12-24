import argparse
import asyncio
import struct
import msgpack
from vergent.core.model.event import Event


async def send_event(writer, event: Event) -> None:
    payload = msgpack.packb(event.to_dict(), use_bin_type=True)
    frame = struct.pack("!I", len(payload)) + payload
    writer.write(frame)
    await writer.drain()


async def read_event(reader) -> Event:
    header = await reader.readexactly(4)
    length = struct.unpack("!I", header)[0]
    payload = await reader.readexactly(length)
    data = msgpack.unpackb(payload, raw=False)
    return Event(**data)


async def rpc_call(reader, writer, event: Event) -> Event:
    await send_event(writer, event)
    return await read_event(reader)


async def repl(host: str, port: int) -> None:
    reader, writer = await asyncio.open_connection(host, port)

    print(f"Connected to vergent node at {host}:{port}")
    prompt = f"vergent({host}:{port})> "

    while True:
        try:
            cmd = input(prompt).strip()
        except EOFError:
            break

        if not cmd:
            continue

        parts = cmd.split()
        op = parts[0]

        if op in ("quit", "exit"):
            break

        try:
            if op == "get" and len(parts) == 2:
                key = parts[1]
                res = await rpc_call(reader, writer, Event("get", {"key": key}))
                print(res.payload["content"])

            elif op == "put" and len(parts) == 3:
                key, value = parts[1], parts[2]
                res = await rpc_call(reader, writer, Event("put", {"key": key, "value": value}))
                print("OK", res.payload["hlc"])

            elif op == "delete" and len(parts) == 2:
                key = parts[1]
                res = await rpc_call(reader, writer, Event("delete", {"key": key}))
                print("OK", res.payload["hlc"])

            else:
                print("Commands:")
                print("  get <key>")
                print("  put <key> <value>")
                print("  delete <key>")
                print("  exit")

        except (ConnectionResetError, BrokenPipeError):
            print("Connection lost, reconnecting...")
            reader, writer = await asyncio.open_connection(host, port)

    writer.close()
    await writer.wait_closed()


def parse_args():
    parser = argparse.ArgumentParser(
        prog="vsql",
        description="Minimal Vergent CLI client"
    )

    parser.add_argument(
        "address",
        nargs="?",
        help="Server address in the form host:port"
    )

    parser.add_argument(
        "--host",
        help="Server host"
    )

    parser.add_argument(
        "--port",
        type=int,
        help="Server port"
    )

    return parser.parse_args()


def entrypoint():
    args = parse_args()

    # Priority: explicit flags > address argument
    if args.host and args.port:
        host, port = args.host, args.port

    elif args.address:
        if ":" not in args.address:
            print("Error: address must be host:port")
            exit(1)
        host, port = args.address.split(":")
        port = int(port)

    else:
        print("Usage: vergentctl host:port OR vergentctl -h host -p port")
        exit(1)

    asyncio.run(repl(host, port))
