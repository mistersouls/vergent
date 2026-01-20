import ssl
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
            return SizeClass[v]  # converts "L" → SizeClass.L
        return v


class ApiServerSettings(BaseModel):
    host: Annotated[
        str,
        Field(
            description="Bind address for the client-facing API.",
            default="127.0.0.1"
        )
    ]

    port: Annotated[
        int,
        Field(
            description="TCP port for client requests (GET/PUT/DELETE).",
            default=2000
        )
    ]


class PeerServerSettings(BaseModel):
    host: Annotated[
        str,
        Field(
            description="Bind address for inter-node communication.",
            default="127.0.0.1"
        )
    ]

    port: Annotated[
        int,
        Field(
            description="TCP port for inter-node communication.",
            default=12000
        )
    ]

    listener: Annotated[
        str | None,
        Field(
            description=(
                "Reachable address (host:port) advertised to other nodes.\n"
                "Defaults to '<host>:<port>' if omitted."
            ),
            default=None
        )
    ]

    seeds: Annotated[
        list[str],
        Field(
            description=(
                "List of seed node addresses (host:port) used during bootstrap.\n"
                "Seeds are only used at startup to obtain membership and ring layout."
            ),
            default_factory=list
        )
    ]


class ReplicationServerSettings(BaseModel):
    host: Annotated[
        str,
        Field(
            description=(
                "Bind address for the replication server.\n"
                "Usually 0.0.0.0 so peers can reach it.\n"
                "This server handles binary partition transfers (FileReceiver)."
            ),
            default="127.0.0.1",
        ),
    ]

    port: Annotated[
        int,
        Field(
            description=(
                "TCP port dedicated to partition replication.\n"
                "Used exclusively for binary partition transfers and separate from "
                "the peer API (server.peer.port)."
            ),
            default=13000,
        ),
    ]

    listener: Annotated[
        str | None,
        Field(
            description=(
                "Public address advertised to other nodes for partition transfers.\n"
                "If omitted, defaults to '<host>:<port>'."
            ),
            default=None,
        ),
    ]

    max_concurrent_transfers: Annotated[
        int,
        Field(
            description=(
                "Maximum number of concurrent incoming partition transfers.\n"
                "Each transfer uses one TCP connection and one SHA‑256 streaming pipeline."
            ),
            default=4,
        ),
    ]

    timeout_transfer: Annotated[
        float,
        Field(
            description=(
                "Maximum allowed duration for a single file transfer.\n"
                "If exceeded, the transfer is aborted and the connection closed.\n"
                "0 means no timeout."
            ),
            default=30.0,
        ),
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
    api: Annotated[
        ApiServerSettings,
        Field(description="Client-facing server configuration.")
    ]

    peer: Annotated[
        PeerServerSettings,
        Field(description="Inter-node server configuration.")
    ]

    replication: Annotated[
        ReplicationServerSettings,
        Field(description="Replication server configuration.")
    ]

    tls: Annotated[
        TLSSettings,
        Field(description="TLS configuration shared by client and peer servers.")
    ]

    backlog: Annotated[
        int,
        Field(
            description="Maximum number of pending TCP connections.",
            default=128
        )
    ]

    timeout_graceful_shutdown: Annotated[
        float,
        Field(
            description="Maximum time allowed for graceful shutdown.",
            default=5.0
        )
    ]

    limit_concurrency: Annotated[
        int,
        Field(
            description="Maximum number of concurrent in-flight requests.",
            default=1024
        )
    ]

    max_buffer_size: Annotated[
        int,
        Field(
            description="Maximum allowed buffer size for incoming data.",
            default=4 * 1024 * 1024
        )
    ]

    max_message_size: Annotated[
        int,
        Field(
            description="Maximum allowed size for a single decoded message.",
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

    def get_server_ssl_ctx(self) -> ssl.SSLContext:
        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ctx.load_cert_chain(
            certfile=self.server.tls.certfile,
            keyfile=self.server.tls.keyfile
        )
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(cafile=self.server.tls.cafile)

        return ctx

    def get_client_ssl_ctx(self) -> ssl.SSLContext:
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.load_cert_chain(
            certfile=self.server.tls.certfile,
            keyfile=self.server.tls.keyfile
        )
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(cafile=self.server.tls.cafile)

        return ctx
