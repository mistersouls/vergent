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
"""FileStateAdapter — StatePort implementation backed by state.toml.

Writes are atomic: new content is written to a sibling temp file,
os.replace() is called, then the containing directory is fsynced when the
platform supports it. A crash mid-write leaves either the old state or the
new state visible on disk; a partial file is never possible.

All load() and save() calls are serialised through a per-instance asyncio
Lock (_io_lock).  On Windows, Python's open() does not request the
FILE_SHARE_DELETE sharing flag, which prevents os.replace() from renaming
over a file that is currently open for reading by another thread pool
worker.  The _io_lock ensures that no read thread is holding state.toml
open while a save thread executes os.replace(state.tmp, state.toml).

Callers that need an atomic load-modify-save cycle must additionally hold
their own higher-level lock (e.g. RebalanceApplicator._state_lock) to
prevent interleaved modifications between the load and the save; that
higher-level lock is orthogonal to the I/O-serialisation role of _io_lock.
"""

from __future__ import annotations

import asyncio
import logging
import os
import tomllib
from pathlib import Path
from typing import Any

import tomli_w

from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ports.state import StateError

logger = logging.getLogger(__name__)

_TMP_SUFFIX = ".tmp"


class FileStateAdapter:
    """StatePort implementation that reads/writes state.toml in data_dir.

    state.toml canonical format:

        [node]
        phase      = "ready"
        generation = 1
        seq        = 0
        tokens     = [14, 87, 142, 201]

        [topology]
        epoch = 1

    Injected at startup by the bootstrap sequence. No other component may
    open or write state.toml directly.

    All I/O is serialised through _io_lock to prevent Windows
    FILE_SHARE_DELETE races between concurrent thread-pool read operations
    and the os.replace() rename inside save().
    """

    def __init__(self, path: Path) -> None:
        """Initialise with the absolute path to state.toml."""
        self._path = path
        self._tmp = path.with_suffix(_TMP_SUFFIX)
        # Serialises all I/O: prevents concurrent reads from holding state.toml
        # open (without FILE_SHARE_DELETE) while save() renames state.tmp over it.
        self._io_lock = asyncio.Lock()

    async def load(self) -> NodeState | None:
        """Read and parse state.toml.

        Return None if the file does not exist. Raise StateError on any
        parse or I/O failure.
        """
        async with self._io_lock:
            return await asyncio.to_thread(self._load_sync)

    async def save(self, state: NodeState) -> None:
        """Write state atomically via temp-file + os.replace() + fsync.

        Raise StateError on any I/O failure.
        """
        async with self._io_lock:
            await asyncio.to_thread(self._save_sync, state)
        logger.info(
            "State persisted to disk (phase: %s, epoch: %d, generation: %d).",
            state.phase.value,
            state.epoch,
            state.generation,
        )

    def _load_sync(self) -> NodeState | None:
        if not self._path.exists():
            return None
        try:
            raw: dict[str, Any] = tomllib.loads(self._path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise StateError(f"Cannot read state.toml: {exc}") from exc
        return _parse_state(raw)

    def _save_sync(self, state: NodeState) -> None:
        raw = _encode_state(state)
        try:
            self._tmp.write_text(tomli_w.dumps(raw), encoding="utf-8")
            os.replace(self._tmp, self._path)
        except OSError as exc:
            raise StateError(f"Cannot write state.toml: {exc}") from exc
        _fsync_dir(self._path.parent)


def _parse_state(raw: dict[str, Any]) -> NodeState:
    """Parse a raw TOML dict into a NodeState; raise StateError on failure."""
    try:
        node = raw["node"]
        topo: dict[str, Any] = raw.get("topology", {})
        reb: dict[str, Any] = raw.get("rebalance", {})
        return NodeState(
            node_id=str(node.get("node_id", "")),
            phase=MemberPhase(node["phase"]),
            generation=int(node["generation"]),
            seq=int(node["seq"]),
            tokens=tuple(int(t) for t in node.get("tokens", [])),
            epoch=int(topo.get("epoch", 0)),
            committed_pids=tuple(int(p) for p in reb.get("committed_pids", [])),
            staging_pids=tuple(int(p) for p in reb.get("staging_pids", [])),
        )
    except Exception as exc:
        raise StateError(f"Malformed state.toml: {exc}") from exc


def _encode_state(state: NodeState) -> dict[str, Any]:
    """Encode a NodeState into a TOML-serialisable dict."""
    return {
        "node": {
            "node_id": state.node_id,
            "phase": state.phase.value,
            "generation": state.generation,
            "seq": state.seq,
            "tokens": list(state.tokens),
        },
        "topology": {
            "epoch": state.epoch,
        },
        "rebalance": {
            "committed_pids": list(state.committed_pids),
            "staging_pids": list(state.staging_pids),
        },
    }


def _fsync_dir(directory: Path) -> None:
    """Best-effort fsync of the directory containing the state file."""
    try:
        fd = os.open(str(directory), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass  # not all platforms support directory fsync; ignore silently
