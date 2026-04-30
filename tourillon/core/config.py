# Copyright 2026 Tourillon Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Canonical in-memory configuration model for Tourillon nodes."""

from dataclasses import dataclass


class ConfigError(ValueError):
    """Raised when configuration loading or validation fails.

    ConfigError wraps all configuration problems — missing required fields,
    invalid values, malformed TOML, and missing files — into a single
    exception type so that callers can catch it without knowing which layer
    of the configuration resolution chain produced the failure.
    """


_VALID_LOG_LEVELS: frozenset[str] = frozenset({"debug", "info", "warning", "error"})


@dataclass(frozen=True)
class ServerEndpointConfig:
    """Bind and advertise addresses for a single TCP listener.

    The bind address is the interface and port the OS socket listens on.
    The advertise address is the host and port announced to peers and
    clients for routing. These differ when the node runs behind NAT, a
    load balancer, or a container port mapping. When advertise is not
    supplied by the caller it defaults to the bind value before construction.
    """

    bind: str
    advertise: str

    def __post_init__(self) -> None:
        """Validate that both addresses are non-empty."""
        if not self.bind:
            raise ConfigError("server endpoint bind address must not be empty")
        if not self.advertise:
            raise ConfigError("server endpoint advertise address must not be empty")


@dataclass(frozen=True)
class TlsSection:
    """Inline base64-encoded PEM material for mTLS identity and trust.

    All three fields are base64-encoded PEM strings. There are no *file path
    variants; this eliminates dual-representation complexity and ensures that
    every config file is fully self-contained. cert_data carries the server
    certificate, key_data carries the matching private key, and ca_data
    carries the CA certificate used to verify incoming connections. Both
    servers.kv and servers.peer inherit ca_data from this section unless they
    override it with their own ca_data field.
    """

    cert_data: str
    key_data: str
    ca_data: str

    def __post_init__(self) -> None:
        """Validate that all PEM fields are present."""
        for fname in ("cert_data", "key_data", "ca_data"):
            if not getattr(self, fname):
                raise ConfigError(f"tls.{fname} must not be empty")


@dataclass(frozen=True)
class ClusterSection:
    """Cluster membership hints read at startup; ignored in single-node mode.

    Both seeds and replication_factor are present in Milestone 1 TOML files
    to allow forward-compatible config generation, but the single-node runtime
    ignores them. The ring layer introduced in Milestone 2 activates these fields.
    """

    seeds: tuple[str, ...]
    replication_factor: int

    def __post_init__(self) -> None:
        """Validate that replication_factor is at least 1."""
        if self.replication_factor < 1:
            raise ConfigError(
                f"cluster.replication_factor must be >= 1,"
                f" got {self.replication_factor}"
            )


@dataclass(frozen=True)
class TourillonConfig:
    """Canonical in-memory representation of all resolved Tourillon configuration.

    TourillonConfig is constructed once at startup by
    tourillon.bootstrap.config.load_config, which applies the four-level
    precedence chain: CLI flag > environment variable > config file > built-in
    default. After construction no other module reads the config file or
    environment variables; every subsystem receives a TourillonConfig instance
    injected through its constructor.

    All TLS material is carried as inline base64-encoded PEM strings. There are
    no *file path fields. The config is fully validated before any socket is
    bound, any file is opened, or any TLS context is created. ConfigError is
    raised for any invalid field value.

    servers_peer is optional in Milestone 1; its presence enables the second TCP
    listener introduced in Milestone 2.
    """

    node_id: str
    data_dir: str
    log_level: str
    tls: TlsSection
    servers_kv: ServerEndpointConfig
    servers_peer: ServerEndpointConfig | None
    cluster: ClusterSection

    def __post_init__(self) -> None:
        """Validate top-level fields."""
        if not self.node_id:
            raise ConfigError("node.id must not be empty")
        if not self.data_dir:
            raise ConfigError("node.data_dir must not be empty")
        if self.log_level not in _VALID_LOG_LEVELS:
            raise ConfigError(
                f"node.log_level must be one of {sorted(_VALID_LOG_LEVELS)},"
                f" got {self.log_level!r}"
            )
