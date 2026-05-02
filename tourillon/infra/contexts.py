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
"""contexts.toml read/write operations.

Writing is always atomic: contents are serialised to a temp file in the same
directory, then os.replace() is used to swap it in. A mid-write crash cannot
corrupt an existing contexts.toml.
"""

from __future__ import annotations

import contextlib
import os
import stat
import tempfile
import tomllib
from pathlib import Path
from typing import Any

import tomli_w

from tourillon.core.structure.contexts import (
    ClusterRef,
    ContextEntry,
    ContextsFile,
    CredentialsConfig,
    EndpointsConfig,
)


class ContextsError(Exception):
    """Raised for any contexts.toml parsing or consistency error."""


def load_contexts(path: Path) -> ContextsFile:
    """Read *path* and return a ContextsFile.

    Return an empty ContextsFile if the file does not exist yet.
    Raise ContextsError for parse errors.
    """
    if not path.exists():
        return ContextsFile()

    try:
        raw: dict[str, Any] = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise ContextsError(f"Cannot parse {path}: {exc}") from exc

    current = raw.get("current-context")
    entries: list[ContextEntry] = []
    for ctx in raw.get("contexts", []):
        cluster_raw = ctx.get("cluster", {})
        ep_raw = ctx.get("endpoints", {})
        cred_raw = ctx.get("credentials", {})
        entries.append(
            ContextEntry(
                name=ctx["name"],
                cluster=ClusterRef(
                    name=cluster_raw.get("name", ctx["name"]),
                    ca_data=cluster_raw.get("ca_data", ""),
                ),
                endpoints=EndpointsConfig(
                    kv=ep_raw.get("kv"),
                    peer=ep_raw.get("peer"),
                ),
                credentials=CredentialsConfig(
                    cert_data=cred_raw.get("cert_data", ""),
                    key_data=cred_raw.get("key_data", ""),
                ),
            )
        )
    return ContextsFile(current_context=current, contexts=entries)


def save_contexts(path: Path, file: ContextsFile) -> None:
    """Atomically write *file* to *path* at mode 0600.

    Uses temp-file + os.replace() to prevent mid-write corruption.
    """
    raw: dict[str, Any] = {}
    if file.current_context is not None:
        raw["current-context"] = file.current_context

    contexts_list = []
    for ctx in file.contexts:
        ep: dict[str, str] = {}
        if ctx.endpoints.kv:
            ep["kv"] = ctx.endpoints.kv
        if ctx.endpoints.peer:
            ep["peer"] = ctx.endpoints.peer
        contexts_list.append(
            {
                "name": ctx.name,
                "cluster": {"name": ctx.cluster.name, "ca_data": ctx.cluster.ca_data},
                "endpoints": ep,
                "credentials": {
                    "cert_data": ctx.credentials.cert_data,
                    "key_data": ctx.credentials.key_data,
                },
            }
        )
    raw["contexts"] = contexts_list

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        os.write(fd, tomli_w.dumps(raw).encode())
        os.close(fd)
        os.chmod(tmp_path, stat.S_IRUSR | stat.S_IWUSR)
        os.replace(tmp_path, path)
    except Exception:
        os.close(fd)
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise
