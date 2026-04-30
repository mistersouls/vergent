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
"""Context file model and atomic read/write utilities for tourctl contexts.toml.

The contexts file lives at ~/.config/tourillon/contexts.toml and stores named
cluster connections for tourctl. Each context bundles a cluster definition, an
optional endpoint address for the kv and peer listeners, and client credentials
(certificate and private key) used when connecting.

ContextsFile provides the in-memory representation with upsert, active-context
management, and atomic persistence. All writes go through ContextsFile.save(),
which uses write_config_file from tourillon.bootstrap.config so that the file is
always replaced atomically and never partially written.
"""

import tomllib
from dataclasses import dataclass
from dataclasses import field as dc_field
from pathlib import Path

from tourillon.bootstrap.config import write_config_file
from tourillon.core.config import ConfigError

DEFAULT_CONTEXTS_PATH: Path = Path.home() / ".config" / "tourillon" / "contexts.toml"


@dataclass(frozen=True)
class ClusterEntry:
    """Cluster identity and CA material embedded in a context entry.

    ca_data is the base64-encoded PEM CA certificate used to verify the
    cluster nodes' server certificates when tourctl opens an mTLS connection.
    """

    name: str
    ca_data: str


@dataclass(frozen=True)
class ContextEndpoints:
    """Optional endpoint addresses for a context.

    At least one of kv or peer must be present. A context with only kv is
    sufficient for tourctl kv operations. A context with only peer is
    sufficient for operator commands such as tourctl ring inspect and
    tourctl log tail.
    """

    kv: str | None = None
    peer: str | None = None


@dataclass(frozen=True)
class ContextCredentials:
    """Client certificate and private key embedded in a context entry.

    Both fields carry base64-encoded PEM strings. The private key is the
    reason the contexts file must be written with mode 0600 and must never
    be world-readable.
    """

    cert_data: str
    key_data: str


@dataclass(frozen=True)
class ContextEntry:
    """A named context bundling cluster identity, endpoints, and credentials.

    Each context allows tourctl to connect to a specific cluster without
    requiring explicit connection flags on every command. The active context
    is identified by the current_context field of the parent ContextsFile.
    At least one of endpoints.kv or endpoints.peer must be supplied; an entry
    with no endpoint addresses is invalid because it cannot be used to connect.
    """

    name: str
    cluster: ClusterEntry
    endpoints: ContextEndpoints
    credentials: ContextCredentials

    def __post_init__(self) -> None:
        """Validate that at least one endpoint is present."""
        if not self.endpoints.kv and not self.endpoints.peer:
            raise ConfigError(
                f"context {self.name!r}: at least one of endpoints.kv or"
                " endpoints.peer must be specified"
            )


@dataclass
class ContextsFile:
    """The parsed and editable contents of the tourctl contexts file.

    ContextsFile provides upsert (add or replace a named context), active
    context management, and atomic persistence. All mutations are in-memory
    until save() is called. save() replaces the file atomically via a temp
    file so a crash mid-write never corrupts the existing context list.
    """

    current_context: str | None
    contexts: list[ContextEntry] = dc_field(default_factory=list)

    def find_context(self, name: str) -> ContextEntry | None:
        """Return the context with the given name, or None if not found."""
        for ctx in self.contexts:
            if ctx.name == name:
                return ctx
        return None

    def upsert_context(self, entry: ContextEntry) -> None:
        """Add or replace the context with the same name as entry."""
        for i, ctx in enumerate(self.contexts):
            if ctx.name == entry.name:
                self.contexts[i] = entry
                return
        self.contexts.append(entry)

    def set_current(self, name: str) -> None:
        """Set the active context. Raise ConfigError if name is not known."""
        if not any(ctx.name == name for ctx in self.contexts):
            known = [ctx.name for ctx in self.contexts]
            raise ConfigError(
                f"context {name!r} not found; available contexts: {known}"
            )
        self.current_context = name

    def save(self, path: Path | None = None) -> None:
        """Atomically write the contexts file to path at mode 0600.

        Serialise to TOML and call write_config_file, which stages through a
        temp file and replaces atomically. A crash between write and replace
        leaves the temp file behind but never corrupts the existing list.
        """
        import tomli_w

        resolved = path or DEFAULT_CONTEXTS_PATH
        doc: dict = {}
        if self.current_context is not None:
            doc["current-context"] = self.current_context
        doc["contexts"] = [_entry_to_dict(ctx) for ctx in self.contexts]
        write_config_file(resolved, tomli_w.dumps(doc), mode=0o600)


def load_contexts(path: Path | None = None) -> ContextsFile:
    """Load contexts from path (defaults to ~/.config/tourillon/contexts.toml).

    Return an empty ContextsFile when the file does not exist. Raise
    ConfigError if the file exists but cannot be parsed.
    """
    resolved = path or DEFAULT_CONTEXTS_PATH
    if not resolved.exists():
        return ContextsFile(current_context=None, contexts=[])
    try:
        raw = tomllib.loads(resolved.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"contexts file parse error: {exc}") from exc
    return _parse_contexts_file(raw)


def _parse_contexts_file(raw: dict) -> ContextsFile:
    """Parse the raw TOML dict into a ContextsFile."""
    current_context: str | None = raw.get("current-context")
    entries: list[ContextEntry] = []
    for ctx_raw in raw.get("contexts", []):
        cluster_raw = ctx_raw.get("cluster", {})
        endpoints_raw = ctx_raw.get("endpoints", {})
        creds_raw = ctx_raw.get("credentials", {})
        entry = ContextEntry(
            name=ctx_raw["name"],
            cluster=ClusterEntry(
                name=cluster_raw.get("name", ctx_raw["name"]),
                ca_data=cluster_raw["ca_data"],
            ),
            endpoints=ContextEndpoints(
                kv=endpoints_raw.get("kv"),
                peer=endpoints_raw.get("peer"),
            ),
            credentials=ContextCredentials(
                cert_data=creds_raw["cert_data"],
                key_data=creds_raw["key_data"],
            ),
        )
        entries.append(entry)
    return ContextsFile(current_context=current_context, contexts=entries)


def _entry_to_dict(entry: ContextEntry) -> dict:
    """Convert a ContextEntry to a TOML-serialisable dict."""
    result: dict = {
        "name": entry.name,
        "cluster": {
            "name": entry.cluster.name,
            "ca_data": entry.cluster.ca_data,
        },
        "credentials": {
            "cert_data": entry.credentials.cert_data,
            "key_data": entry.credentials.key_data,
        },
    }
    endpoints: dict = {}
    if entry.endpoints.kv:
        endpoints["kv"] = entry.endpoints.kv
    if entry.endpoints.peer:
        endpoints["peer"] = entry.endpoints.peer
    result["endpoints"] = endpoints
    return result
