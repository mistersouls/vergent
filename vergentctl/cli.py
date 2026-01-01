import argparse
import asyncio

from vergentctl.repl import repl
from vergentctl.ssl_ import gen_root_ca, gen_node_cert, inspect_cert


def entrypoint() -> None:
    args = parse_args()

    commands = {
        "connect": cmd_connect,
        "gen-root-cert": cmd_gen_root,
        "gen-cert": cmd_gen_cert,
        "inspect-cert": cmd_inspect_cert,
    }

    handler = commands.get(args.command)
    if handler is None:
        print(f"Unknown command: {args.command}")
        exit(1)

    handler(args)


def parse_args():
    parser = argparse.ArgumentParser(
        prog="vergentctl",
        description="Vergent CLI client"
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # -------------------------
    # connect
    # -------------------------
    p_connect = sub.add_parser("connect", help="Connect to a Vergent node")
    p_connect.add_argument("address", nargs="?", help="host:port")
    p_connect.add_argument("--host")
    p_connect.add_argument("--port", type=int)
    p_connect.add_argument(
        "--tls-certfile",
        required=True,
        help="Client certificate (PEM) used for mTLS",
    )
    p_connect.add_argument(
        "--tls-keyfile",
        required=True,
        help="Client private key (PEM) used for mTLS",
    )
    p_connect.add_argument(
        "--tls-cafile",
        required=True,
        help="CA certificate (PEM) used to validate the node certificate",
    )

    # -------------------------
    # gen-root
    # -------------------------
    p_root = sub.add_parser("gen-root-cert", help="Generate a root CA certificate")
    p_root.add_argument("--out", required=True)
    p_root.add_argument("--key-out", required=True)
    p_root.add_argument("--days", type=int, default=3650)
    p_root.add_argument("--ask-passphrase", action="store_true",
                        help="Ask for a passphrase to encrypt the CA private key")

    # -------------------------
    # gen-cert
    # -------------------------
    p_cert = sub.add_parser("gen-cert", help="Generate a node certificate signed by the CA")
    p_cert.add_argument("--ca", required=True)
    p_cert.add_argument("--ca-key", required=True)
    p_cert.add_argument("--out", required=True)
    p_cert.add_argument("--key-out", required=True)
    p_cert.add_argument("--cn", required=True)
    p_cert.add_argument("--days", type=int, default=365)
    p_cert.add_argument("--ca-passphrase", help="Passphrase for the CA private key")
    p_cert.add_argument("--san-dns", action="append", default=[], help="Add a DNS SAN entry")
    p_cert.add_argument("--san-ip", action="append", default=[], help="Add an IP SAN entry")

    # -------------------------
    # inspect-cert
    # -------------------------
    p_inspect = sub.add_parser("inspect-cert", help="Display certificate information")
    p_inspect.add_argument("--cert", required=True, help="Certificate file (PEM)")

    return parser.parse_args()


def cmd_connect(args) -> None:
    if args.host and args.port:
        host, port = args.host, args.port
    elif args.address:
        if ":" not in args.address:
            print("Error: address must be host:port")
            exit(1)
        host, port = args.address.split(":")
        port = int(port)
    else:
        print("Usage: vergentctl connect host:port OR --host H --port P")
        exit(1)

    asyncio.run(repl(host, port, args.tls_certfile, args.tls_keyfile, args.tls_cafile))


def cmd_gen_root(args) -> None:
    gen_root_ca(args.out, args.key_out, args.days, args.ask_passphrase)


def cmd_gen_cert(args) -> None:
    gen_node_cert(
        ca=args.ca,
        ca_key=args.ca_key,
        out_cert=args.out,
        out_key=args.key_out,
        cn=args.cn,
        days=args.days,
        ca_passphrase=args.ca_passphrase,
        san_dns=args.san_dns,
        san_ip=args.san_ip,
    )


def cmd_inspect_cert(args):
    inspect_cert(args.cert)
