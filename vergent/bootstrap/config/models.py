from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, Field, field_validator, ValidationError
from pydantic_core.core_schema import ValidationInfo
from pydantic_settings import BaseSettings, SettingsConfigDict, PydanticBaseSettingsSource, YamlConfigSettingsSource

from vergent.bootstrap.config.loader import get_configfile
from vergent.core.model.vnode import SizeClass


class NodeSettings(BaseSettings):
    id: Annotated[
        str,
        Field(
            description=(
                "Unique and stable identifier for this node within the cluster.\n"
                "This value must be globally unique and must not change across restarts.\n"
                "It is used as the node's identity in gossip, vnode ownership, routing,\n"
                "rebalance planning, and all cluster‑wide coordination.\n\n"
                "The identifier should be explicit (e.g., 'node-1', 'db-a', 'shard-3') and\n"
                "must not depend on ephemeral properties such as IP address or hostname.\n"
            )
        )
    ]

    size: Annotated[
        SizeClass,
        Field(
            description=(
                "Size class defining how many virtual slots (vnodes) this node receives\n"
                "when generating its vnode assignment for the first time.\n\n"
                "This value is used ONLY if the storage backend does not already contain\n"
                "a persisted vnode list. Once a vnode list exists on disk, it becomes the\n"
                "authoritative source of truth and this field is ignored.\n\n"
                "Allowed values:\n"
                "  XS  → 1 slot\n"
                "  S   → 2 slots\n"
                "  M   → 4 slots\n"
                "  L   → 8 slots\n"
                "  XL  → 16 slots\n"
                "  XXL → 32 slots\n"
            ),
            default=SizeClass.L
        )
    ]

    @field_validator("size", mode="before")
    @classmethod
    def parse_size(cls, v):
        if isinstance(v, str):
            return SizeClass[v]  # convertit "L" → SizeClass.L
        return v


class AdvertisedSettings(BaseModel):
    listener: Annotated[
        str,
        Field(
            description=(
                "Public address (host:port) advertised to other nodes.\n"
                "This is the address peers will use to contact this node.\n"
                "It may differ from server.host/server.port when running behind NAT, Docker,\n"
                "Kubernetes, reverse proxies, or load balancers.\n\n"
                "If omitted, defaults to '<host>:<port>'.\n"
            ),
            default=None
        )
    ]

    seeds: Annotated[
        list[str],
        Field(
            description=(
                "List of seed node addresses (host:port) used for initial cluster discovery.\n"
                "At startup, this node will contact the listed seeds to obtain the current\n"
                "cluster membership, ring layout, and gossip state.\n\n"
                "Seeds are only used during bootstrap. Once gossip converges, the node no\n"
                "longer depends on them, and the cluster can continue operating even if all\n"
                "seed nodes go offline.\n\n"
                "An empty list is valid for single‑node deployments or when the node is\n"
                "manually bootstrapped. Each entry must be a reachable TCP address such as\n"
                "'10.0.0.5:2001' or 'db-node-1.internal:2001'."
            ),
            default_factory=list
        )
    ]


class TLSSettings(BaseModel):
    certfile: Annotated[
        Path,
        Field(
            description=(
                "Path to the node's TLS certificate (PEM).\n"
                "Vergent uses mandatory mutual TLS (mTLS): all traffic between nodes is "
                "encrypted and authenticated.\n"
            ),
        )
    ]

    keyfile: Annotated[
        Path,
        Field(
            description=(
                "Path to the node's TLS private key (PEM).\n"
                "Required for mTLS: the node proves its identity using this key.\n"
            )
        )
    ]

    cafile: Annotated[
        Path,
        Field(
            description=(
                "Path to the CA certificate (PEM) used to verify peer node certificates.\n"
                "Vergent enforces mutual TLS (mTLS): nodes must present a certificate "
                "signed by this CA. Connections without valid certificates are rejected.\n"
            )
        )
    ]

    @field_validator("certfile", "keyfile", "cafile")
    @classmethod
    def validate_path(cls, v: Path, info: ValidationInfo) -> Path:
        if not v.exists():
            raise ValidationError(f"Path {v} does not exist.")
        return v


class ServerSettings(BaseModel):
    host: Annotated[
        str,
        Field(
            description=(
                "Host/IP on which the server listens for incoming TCP connections.\n"
                "This is the *bind address* used locally by the OS."
            ),
            default="127.0.0.1"
        )
    ]

    port: Annotated[
        int,
        Field(
            description=(
                "TCP port on which the server listens.\n"
                "This is the port clients and peers connect to."
            ),
            default=2000
        )
    ]

    tls: Annotated[
        TLSSettings,
        Field(description="")
    ]

    backlog: Annotated[
        int,
        Field(
            description=(
                "Maximum number of pending TCP connections allowed by the OS.\n"
                "This controls how many clients can be waiting to connect.\n"
                "Defaults to 128."
            ),
            default=128
        )
    ]

    timeout_graceful_shutdown: Annotated[
        float,
        Field(
            description=(
                "Maximum time (in seconds) allowed for a graceful shutdown.\n"
                "When the node receives SIGINT/SIGTERM/SIGBREAK, it begins a coordinated shutdown:\n"
                " - stop accepting new connections\n"
                " - finish in-flight requests\n"
                " - flush and close storage\n"
                " - notify peers of departure\n\n"
                "If this timeout is exceeded, the node forces termination.\n\n"
            ),
            default=5.0
        )
    ]

    limit_concurrency: Annotated[
        int,
        Field(
            description=(
                "Maximum number of concurrent in-flight requests handled by the node.\n"
                "This protects the server from overload.\n"
                "Defaults to 1024."
            ),
            default=1024
        )
    ]

    max_buffer_size: Annotated[
        int,
        Field(
            description=(
                "Maximum allowed buffer size for incoming data (in bytes).\n"
                "If exceeded, the connection is closed.\n"
                "Defaults to 4MB."
            ),
            default=4 * 1024 * 1024
        )
    ]

    max_message_size: Annotated[
        int,
        Field(
            description=(
                "Maximum allowed size for a single decoded message (in bytes).\n"
                "Defaults to 1MB."
            ),
            default=1 * 1024 * 1024
        )
    ]


class StorageSettings(BaseModel):
    data_dir: Annotated[
        Path,
        Field(
            description=(
                "Directory where the node stores all persistent data.\n"
                "This directory is used by the storage backend.\n"
                "It must exist or be creatable, writable, and persistent across restarts.\n\n"
            )
        )
    ]


class PlacementSettings(BaseModel):
    shift: Annotated[
        int,
        Field(
            description=(
                "Shift factor used to compute the total number of logical partitions (Q).\n"
                "Q = 1 << shift\n\n"
                "This defines the granularity of the ring. A larger shift increases the\n"
                "number of partitions, improving distribution fairness but also increasing\n"
                "the number of storage directories and the cost of rebalancing.\n\n"
                "This value MUST remain stable for the lifetime of the cluster. Changing it\n"
                "alters every partition boundary and requires a full migration of all data."
            ),
            default=16
        )
    ]

    replication_factor: Annotated[
        int,
        Field(
            description=(
                "Number of distinct nodes that must store each partition.\n"
                "A replication_factor of N means each key is stored on N different nodes,\n"
                "following the ring's successor ordering.\n\n"
                "This value must be <= the number of nodes in the cluster. Increasing it\n"
                "adds replicas and triggers a controlled rebalance; decreasing it removes\n"
                "replicas and requires coordinated cleanup.\n\n"
                "This setting affects durability, availability, and read/write quorum rules."
            ),
            default=3
        )
    ]


class VergentConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="VERGENT_",
        yaml_file=get_configfile(),
        extra="allow"
    )

    node: Annotated[
        NodeSettings,
        Field(
            description=(
                "Node‑level configuration.\n"
                "Defines the node's identity and its size class for vnode generation.\n"
                "This section determines how the node participates in the ring and how\n"
                "it is identified by peers during gossip, routing, and replication."
            )
        )
    ]

    server: Annotated[
        ServerSettings,
        Field(
            description=(
                "Local server configuration.\n"
                "Controls how the node listens for incoming TCP connections, enforces\n"
                "TLS security, and applies runtime limits such as concurrency, buffer\n"
                "sizes, and graceful shutdown behavior."
            ),
            default_factory=ServerSettings
        )
    ]

    advertised: Annotated[
        AdvertisedSettings,
        Field(
            description=(
                "Public addresses advertised to other nodes.\n"
                "Defines how peers reach this node and which seed nodes it contacts\n"
                "during bootstrap. Useful when running behind NAT, Docker, Kubernetes,\n"
                "reverse proxies, or load balancers."
            ),
            default_factory=AdvertisedSettings
        )
    ]

    storage: Annotated[
        StorageSettings,
        Field(
            description=(
                "Storage backend configuration.\n"
                "Defines the directory where all persistent data is stored, including\n"
                "partitions, metadata, and vnode assignments. This directory must be\n"
                "stable and writable across restarts."
            )
        )
    ]

    placement: Annotated[
        PlacementSettings,
        Field(
            description=(
                "Ring and placement configuration.\n"
                "Defines the number of partitions (via shift), the replication factor,\n"
                "and the rules governing how data is distributed across the cluster.\n"
                "This section determines how keys map to partitions and how replicas\n"
                "are assigned to nodes."
            ),
            default_factory=PlacementSettings
        )
    ]

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (YamlConfigSettingsSource(settings_cls),)
