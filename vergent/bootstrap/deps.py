import argparse
import os
import ssl
from functools import lru_cache

from vergent.core.app import App
from vergent.core.p2p.manager import PeerManager
from vergent.core.p2p.versionned import VersionedStorage
from vergent.core.storage import LMDBStorage
from pathlib import Path

from vergent.core.types_ import Storage


@lru_cache
def get_storage() -> Storage:
    args = get_cli_args()
    data_dir = Path(args.data_dir)
    return LMDBStorage(str(data_dir))


@lru_cache
def get_versioned_storage() -> VersionedStorage:
    backend = get_storage()
    return VersionedStorage(backend, get_advertise_address())


@lru_cache
def get_app() -> App:
    app = App()
    return app


@lru_cache
def get_advertise_address() -> str:
    args = get_cli_args()
    if args.advertise_address:
        return args.advertise_address
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
def get_server_ssl_ctx() -> ssl.SSLContext:
    args = get_cli_args()

    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ctx.load_cert_chain(certfile=args.tls_certfile, keyfile=args.tls_keyfile)
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.load_verify_locations(cafile=args.tls_cafile)

    return ctx


@lru_cache
def get_client_ssl_ctx() -> ssl.SSLContext:
    args = get_cli_args()

    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.load_cert_chain(certfile=args.tls_certfile, keyfile=args.tls_keyfile)
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.load_verify_locations(cafile=args.tls_cafile)

    return ctx


@lru_cache
def get_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="Vergent",
        description=(
            "Start a VergentDB node.\n\n"
            "VergentDB is a distributed key-value engine designed for strong "
            "consistency, deterministic convergence, and modular P2P replication. "
            "This CLI allows you to configure how the node listens, how it "
            "advertises itself to peers, and how it participates in the cluster."
        ),
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help=(
            "Host/IP on which the server listens for incoming TCP connections.\n"
            "This is the *bind address* used locally by the OS.\n"
            "Examples:\n"
            " --host 0.0.0.0 (listen on all interfaces)\n"
            " --host 127.0.0.1 (listen only locally)\n"
            " --host 10.0.0.42 (listen on a specific interface)"
        ),
    )

    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help=(
            "TCP port on which the server listens.\n"
            "This is the port clients and peers connect to.\n"
            "Example: --port 9000"
        ),
    )

    parser.add_argument(
        "--timeout-graceful-shutdown",
        type=float,
        default=5.0,
        help=(
            "Maximum time (in seconds) allowed for a graceful shutdown.\n"
            "When the node receives SIGINT/SIGTERM/SIGBREAK, it begins a coordinated shutdown:\n"
            " - stop accepting new connections\n"
            " - finish in-flight requests\n"
            " - flush and close storage\n"
            " - notify peers of departure\n\n"
            "If this timeout is exceeded, the node forces termination.\n\n"
            "Examples:\n"
            " --timeout-graceful-shutdown 10\n"
            " --timeout-graceful-shutdown 30"
        )
    )

    parser.add_argument(
        "--data-dir",
        type=str,
        required=True,
        help=(
            "Directory where the node stores all persistent data.\n"
            "This directory is used by the storage backend.\n"
            "It must exist or be creatable, writable, and persistent across restarts.\n\n"
            "This option is REQUIRED (same behavior as etcd).\n\n"
            "Examples:\n"
            " --data-dir ./data\n"
            " --data-dir /var/lib/vergent/node1\n"
            " --data-dir /mnt/ssd/vergent"
        )
    )

    parser.add_argument(
        "--tls-certfile",
        type=str,
        required=True,
        help=(
            "Path to the node's TLS certificate (PEM).\n"
            "Vergent uses mandatory mutual TLS (mTLS): all traffic between nodes is "
            "encrypted and authenticated.\n"
        ),
    )

    parser.add_argument(
        "--tls-keyfile",
        type=str,
        required=True,
        help=(
            "Path to the node's TLS private key (PEM).\n"
            "Required for mTLS: the node proves its identity using this key.\n"
        ),
    )

    parser.add_argument(
        "--tls-cafile",
        type=str,
        required=True,
        help=(
            "Path to the CA certificate (PEM) used to verify peer node certificates.\n"
            "Vergent enforces mutual TLS (mTLS): nodes must present a certificate "
            "signed by this CA. Connections without valid certificates are rejected.\n"
        ),
    )

    parser.add_argument(
        "--advertise-address",
        type=str,
        default=None,
        help=(
            "Public address (host:port) advertised to other nodes.\n"
            "This is the address peers will use to contact this node.\n"
            "It may differ from --host/--port when running behind NAT, Docker,\n"
            "Kubernetes, reverse proxies, or load balancers.\n\n"
            "If omitted, defaults to '<host>:<port>'.\n"
            "Examples:\n"
            " --advertise-address mynode.example.com:443\n"
            " --advertise-address 192.168.1.10:9000"
        )
    )

    parser.add_argument(
        "--peers",
        nargs="*",
        default=[],
        help=(
            "List of peer nodes to connect to at startup.\n"
            "Each peer must be in the form host:port.\n"
            "These peers are used for cluster join, gossip, quorum operations,\n"
            "and anti-entropy synchronization.\n\n"
            "Examples:\n"
            " --peers 10.0.0.2:9000 10.0.0.3:9000\n"
            " --peers nodeA.cluster.local:7000"
        )
    )

    parser.add_argument(
        "--backlog",
        type=int,
        default=128,
        help=(
            "Maximum number of pending TCP connections allowed by the OS.\n"
            "This controls how many clients can be waiting to connect.\n"
            "Defaults to 128."
        )
    )

    parser.add_argument(
        "--limit-concurrency",
        type=int,
        default=1024,
        help=(
            "Maximum number of concurrent in-flight requests handled by the node.\n"
            "This protects the server from overload.\n"
            "Defaults to 1024."
        )
    )

    parser.add_argument(
        "--max-buffer-size",
        type=int,
        default=4 * 1024 * 1024,
        help=(
            "Maximum allowed buffer size for incoming data (in bytes).\n"
            "If exceeded, the connection is closed.\n"
            "Defaults to 4MB."
        )
    )

    parser.add_argument(
        "--max-message-size",
        type=int,
        default=1 * 1024 * 1024,
        help=(
            "Maximum allowed size for a single decoded message (in bytes).\n"
            "Defaults to 1MB."
        )
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help=(
            "Logging verbosity for the node.\n"
            "Choose among: DEBUG, INFO, WARNING, ERROR, CRITICAL.\n\n"
            "DEBUG    → verbose output, useful for development and tracing.\n"
            "INFO     → standard operational logs (default).\n"
            "WARNING  → only warnings and errors.\n"
            "ERROR    → only errors.\n"
            "CRITICAL → only critical failures.\n\n"
            "Example:\n"
            "  --log-level DEBUG"
        ),
    )

    args = parser.parse_args()

    for name, path in [
        ("--tls-certfile", args.tls_certfile),
        ("--tls-keyfile", args.tls_keyfile),
        ("--tls-cafile", args.tls_cafile)
    ]:
        if not os.path.isfile(path):
            print(f"{name} does not exist or is not a file: {path}")
            exit(1)

    return args
