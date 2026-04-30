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
"""Config loading factory and config file write utility for the bootstrap layer.

load_config applies the four-level precedence chain — CLI flag > environment
variable > config file > built-in default — and returns a validated
TourillonConfig. It is the only code path that reads the TOML config file or
inspects environment variables; all other modules receive a pre-built
TourillonConfig through dependency injection.

write_config_file is the single utility allowed to write a config or contexts
file; callers must never invoke os.chmod directly.
"""

import os
import sys
import tempfile
import tomllib
from pathlib import Path

import tomli_w

from tourillon.core.config import (
    ClusterSection,
    ConfigError,
    ServerEndpointConfig,
    TlsSection,
    TourillonConfig,
)

_ENV_NODE_ID = "TOURILLON_NODE_ID"
_ENV_DATA_DIR = "TOURILLON_DATA_DIR"
_ENV_LOG_LEVEL = "TOURILLON_LOG_LEVEL"

_DEFAULT_DATA_DIR = str(Path.home() / ".local" / "share" / "tourillon")
_DEFAULT_LOG_LEVEL = "info"
_DEFAULT_REPLICATION_FACTOR = 3


def _require_int(value: object, field: str) -> int:
    """Return value as-is when it is an int, otherwise raise ConfigError.

    Booleans are a subclass of int in Python; TOML true/false must be rejected
    for numeric fields to avoid silent misuse (e.g. replication_factor = true).
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(
            f"{field} must be an integer in the config file,"
            f" got {type(value).__name__} {value!r} — did you forget to remove the quotes?"
        )
    return value


def _require_str(value: object, field: str) -> str:
    """Return value as-is when it is a str, otherwise raise ConfigError.

    TOML is a typed format: an unquoted integer like ``id = 1`` produces a
    Python int, not a str. Silently coercing non-string values with str()
    would hide authoring mistakes and make validation unreliable. This helper
    is the single place that enforces the str requirement for all string fields
    parsed from the TOML document.
    """
    if not isinstance(value, str):
        raise ConfigError(
            f"{field} must be a string in the config file,"
            f" got {type(value).__name__} {value!r} — did you forget the quotes?"
        )
    return value


def load_config(
    config_path: Path,
    *,
    node_id: str | None = None,
    log_level: str | None = None,
) -> TourillonConfig:
    """Load and validate TourillonConfig from a TOML file with overlay chain.

    Read the TOML file at config_path, apply environment variable overrides,
    then apply the keyword argument overrides (which represent CLI flags). The
    resulting values are validated and returned as an immutable TourillonConfig.

    Raise ConfigError if the file cannot be read, if required fields are missing,
    or if any field value fails validation.

    Parameters:
        config_path: Path to the TOML configuration file.
        node_id: CLI-level override for node.id (shadows env and file).
        log_level: CLI-level override for node.log_level (shadows env and file).
    """
    if not config_path.exists():
        raise ConfigError(f"config file not found: {config_path}")
    try:
        raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"config file parse error: {exc}") from exc

    node_section = raw.get("node", {})
    tls_raw = raw.get("tls", {})
    servers_raw = raw.get("servers", {})
    kv_raw = servers_raw.get("kv", {})
    peer_raw = servers_raw.get("peer", None)
    cluster_raw = raw.get("cluster", {})

    resolved_node_id = (
        node_id
        or os.environ.get(_ENV_NODE_ID)
        or _require_str(node_section.get("id", ""), "node.id")
    )
    if not resolved_node_id:
        raise ConfigError(
            "node.id is required: set it in the config file under [node] id = "
            f'"...", via the {_ENV_NODE_ID} environment variable, or --node-id'
        )

    resolved_log_level = (
        log_level
        or os.environ.get(_ENV_LOG_LEVEL)
        or _require_str(
            node_section.get("log_level", _DEFAULT_LOG_LEVEL), "node.log_level"
        )
    )

    resolved_data_dir = os.environ.get(_ENV_DATA_DIR) or _require_str(
        node_section.get("data_dir", _DEFAULT_DATA_DIR), "node.data_dir"
    )

    tls = _parse_tls(tls_raw)
    servers_kv = _parse_endpoint(kv_raw, section_name="servers.kv")
    servers_peer = (
        _parse_endpoint(peer_raw, section_name="servers.peer")
        if peer_raw is not None
        else None
    )

    seeds = tuple(cluster_raw.get("seeds", []))
    replication_factor = _require_int(
        cluster_raw.get("replication_factor", _DEFAULT_REPLICATION_FACTOR),
        "cluster.replication_factor",
    )

    return TourillonConfig(
        node_id=resolved_node_id,
        data_dir=resolved_data_dir,
        log_level=resolved_log_level.lower(),
        tls=tls,
        servers_kv=servers_kv,
        servers_peer=servers_peer,
        cluster=ClusterSection(seeds=seeds, replication_factor=replication_factor),
    )


def write_config_file(path: Path, payload: str, mode: int = 0o600) -> None:
    """Atomically write payload to path at the given file mode.

    Stage the payload to a sibling temporary file, set permissions, then
    replace atomically with os.replace. A crash between write and replace
    leaves the temp file behind but never corrupts the existing file.
    Callers must never invoke os.chmod directly; this is the single utility
    that writes config and contexts files.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent))
    tmp = Path(tmp_name)
    try:
        try:
            os.write(fd, payload.encode())
        finally:
            os.close(fd)
        if sys.platform != "win32":
            os.chmod(tmp, mode)
        tmp.replace(path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def build_node_toml(
    *,
    node_id: str,
    data_dir: str,
    log_level: str,
    cert_data: str,
    key_data: str,
    ca_data: str,
    kv_bind: str,
    kv_advertise: str,
    peer_bind: str | None = None,
    peer_advertise: str | None = None,
    seeds: list[str] | None = None,
    replication_factor: int = 3,
) -> str:
    """Serialise a complete node config.toml from the given parameters.

    Return the TOML document as a string. All TLS fields are inline
    base64-encoded PEM strings. The [servers.peer] section is only emitted
    when peer_bind is supplied. The [cluster] section is always present to
    allow forward-compatible config that the ring layer will activate in
    Milestone 2.
    """
    doc: dict = {
        "node": {
            "id": node_id,
            "data_dir": data_dir,
            "log_level": log_level,
        },
        "tls": {
            "cert_data": cert_data,
            "key_data": key_data,
            "ca_data": ca_data,
        },
        "servers": {
            "kv": {
                "bind": kv_bind,
                "advertise": kv_advertise,
            },
        },
        "cluster": {
            "seeds": seeds or [],
            "replication_factor": replication_factor,
        },
    }
    if peer_bind is not None:
        doc["servers"]["peer"] = {
            "bind": peer_bind,
            "advertise": peer_advertise if peer_advertise else peer_bind,
        }
    return tomli_w.dumps(doc)


def _parse_tls(section: dict) -> TlsSection:
    """Parse and validate the [tls] section from the raw TOML dict."""
    for key in ("cert_data", "key_data", "ca_data"):
        if key not in section:
            raise ConfigError(f"tls.{key} is required but not found in config")
    return TlsSection(
        cert_data=_require_str(section["cert_data"], "tls.cert_data"),
        key_data=_require_str(section["key_data"], "tls.key_data"),
        ca_data=_require_str(section["ca_data"], "tls.ca_data"),
    )


def _parse_endpoint(section: dict, *, section_name: str) -> ServerEndpointConfig:
    """Parse and validate a server endpoint sub-section."""
    bind_raw = section.get("bind", "")
    bind = _require_str(bind_raw, f"{section_name}.bind") if bind_raw != "" else ""
    if not bind:
        raise ConfigError(f"{section_name}.bind is required but not found in config")
    advertise_raw = section.get("advertise", bind)
    advertise = _require_str(advertise_raw, f"{section_name}.advertise")
    return ServerEndpointConfig(bind=bind, advertise=advertise)
