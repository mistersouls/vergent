import argparse
import asyncio
from .repl import repl

def parse_args():
    parser = argparse.ArgumentParser(
        prog="vergentctl",
        description="Vergent CLI client"
    )

    parser.add_argument("address", nargs="?", help="host:port")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)

    return parser.parse_args()

def entrypoint():
    args = parse_args()

    if args.host and args.port:
        host, port = args.host, args.port
    elif args.address:
        if ":" not in args.address:
            print("Error: address must be host:port")
            exit(1)
        host, port = args.address.split(":")
        port = int(port)
    else:
        print("Usage: vergentctl host:port OR vergentctl --host H --port P")
        exit(1)

    asyncio.run(repl(host, port))
