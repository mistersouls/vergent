import argparse
import os
from functools import lru_cache
from pathlib import Path


@lru_cache
def get_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="vergent",
        description=(
            "Start a Vergent node.\n\n"
            "Vergent is a distributed key-value engine designed for strong "
            "consistency, deterministic convergence, and modular P2P replication. "
        ),
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "-c", "--config",
        type=str,
        help="Path to a Vergent configuration file"
    )

    # parser.add_argument(
    #     "--data-dir",
    #     type=str,
    #     required=True,
    #     help=(
    #         "Directory where the node stores all persistent data.\n"
    #         "This directory is used by the storage backend.\n"
    #         "It must exist or be creatable, writable, and persistent across restarts.\n\n"
    #         "This option is REQUIRED.\n\n"
    #         "Examples:\n"
    #         " --data-dir ./data\n"
    #         " --data-dir /var/lib/vergent/node1\n"
    #         " --data-dir /mnt/ssd/vergent"
    #     )
    # )
    #
    # parser.add_argument(
    #     "--peers",
    #     nargs="*",
    #     default=[],
    #     help=(
    #         "List of peer nodes to connect to at startup.\n"
    #         "Each peer must be in the form host:port.\n"
    #         "These peers are used for cluster join, gossip, quorum operations,\n"
    #         "and anti-entropy synchronization.\n\n"
    #         "Examples:\n"
    #         " --peers 10.0.0.2:9000 10.0.0.3:9000\n"
    #         " --peers nodeA.cluster.local:7000"
    #     )
    # )

    parser.add_argument(
        "-l", "--log-level",
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

    return parser.parse_args()


@lru_cache
def get_configfile() -> Path:
    args = get_cli_args()

    # Priority: CLI > ENV > default file in current working directory
    raw = args.config or os.getenv("VERCONFIG")

    if raw is None:
        file = Path.cwd() / "config.yaml"
    else:
        file = Path(raw)

    if not file.is_file():
        raise SystemExit(
            f"[config] Configuration file not found: '{file}'.\n"
            "  - Use --config <file.yaml>\n"
            "  - Or set the VERCONFIG environment variable\n"
            "  - Or place a 'config.yaml' file in the current working directory."
        )

    return file
