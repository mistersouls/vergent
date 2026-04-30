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
"""Tests for tourillon.bootstrap.contexts — ContextsFile, load_contexts, save."""

from pathlib import Path

import pytest

from tourillon.bootstrap.contexts import (
    ClusterEntry,
    ContextCredentials,
    ContextEndpoints,
    ContextEntry,
    ContextsFile,
    load_contexts,
)
from tourillon.core.config import ConfigError


def _make_entry(name: str, kv: str = "127.0.0.1:7000") -> ContextEntry:
    return ContextEntry(
        name=name,
        cluster=ClusterEntry(name=name, ca_data="Y2E="),
        endpoints=ContextEndpoints(kv=kv),
        credentials=ContextCredentials(cert_data="Y2VydA==", key_data="a2V5"),
    )


def test_context_entry_requires_at_least_one_endpoint() -> None:
    """ContextEntry rejects entries with neither kv nor peer endpoint."""
    with pytest.raises(ConfigError, match="at least one"):
        ContextEntry(
            name="empty",
            cluster=ClusterEntry(name="c", ca_data="a"),
            endpoints=ContextEndpoints(kv=None, peer=None),
            credentials=ContextCredentials(cert_data="c", key_data="k"),
        )


def test_context_entry_kv_only_is_valid() -> None:
    """ContextEntry with only kv endpoint is valid."""
    entry = _make_entry("prod")
    assert entry.endpoints.kv == "127.0.0.1:7000"
    assert entry.endpoints.peer is None


def test_context_entry_peer_only_is_valid() -> None:
    """ContextEntry with only peer endpoint is valid."""
    entry = ContextEntry(
        name="ops",
        cluster=ClusterEntry(name="ops", ca_data="a"),
        endpoints=ContextEndpoints(kv=None, peer="127.0.0.1:7001"),
        credentials=ContextCredentials(cert_data="c", key_data="k"),
    )
    assert entry.endpoints.peer == "127.0.0.1:7001"


def test_contexts_file_upsert_adds_new_entry() -> None:
    """upsert_context appends a new entry when name is not present."""
    cf = ContextsFile(current_context=None)
    cf.upsert_context(_make_entry("prod"))
    assert len(cf.contexts) == 1
    assert cf.contexts[0].name == "prod"


def test_contexts_file_upsert_replaces_existing_entry() -> None:
    """upsert_context replaces an existing entry with the same name."""
    cf = ContextsFile(current_context=None)
    cf.upsert_context(_make_entry("prod", kv="old:7000"))
    cf.upsert_context(_make_entry("prod", kv="new:7000"))
    assert len(cf.contexts) == 1
    assert cf.contexts[0].endpoints.kv == "new:7000"


def test_contexts_file_find_context_returns_correct_entry() -> None:
    """find_context returns the named entry or None."""
    cf = ContextsFile(current_context=None)
    cf.upsert_context(_make_entry("a"))
    cf.upsert_context(_make_entry("b"))
    assert cf.find_context("a") is not None
    assert cf.find_context("c") is None


def test_contexts_file_set_current_valid_name() -> None:
    """set_current updates current_context when name exists."""
    cf = ContextsFile(current_context=None)
    cf.upsert_context(_make_entry("prod"))
    cf.set_current("prod")
    assert cf.current_context == "prod"


def test_contexts_file_set_current_unknown_name_raises() -> None:
    """set_current raises ConfigError when the name is not known."""
    cf = ContextsFile(current_context=None)
    with pytest.raises(ConfigError, match="not found"):
        cf.set_current("nonexistent")


def test_contexts_file_save_and_load_round_trip(tmp_path: Path) -> None:
    """save followed by load_contexts reconstructs the identical contexts."""
    path = tmp_path / "contexts.toml"
    cf = ContextsFile(current_context="prod")
    cf.upsert_context(_make_entry("prod"))
    cf.save(path)

    loaded = load_contexts(path)
    assert loaded.current_context == "prod"
    assert len(loaded.contexts) == 1
    assert loaded.contexts[0].name == "prod"
    assert loaded.contexts[0].credentials.cert_data == "Y2VydA=="


def test_contexts_file_save_creates_parent_directory(tmp_path: Path) -> None:
    """save creates intermediate directories when they do not exist."""
    path = tmp_path / "sub" / "dir" / "contexts.toml"
    cf = ContextsFile(current_context=None)
    cf.upsert_context(_make_entry("x"))
    cf.save(path)
    assert path.exists()


def test_load_contexts_returns_empty_when_file_absent(tmp_path: Path) -> None:
    """load_contexts returns an empty ContextsFile when the file does not exist."""
    cf = load_contexts(tmp_path / "absent.toml")
    assert cf.current_context is None
    assert cf.contexts == []


def test_load_contexts_raises_on_invalid_toml(tmp_path: Path) -> None:
    """load_contexts raises ConfigError when the TOML is malformed."""
    bad = tmp_path / "bad.toml"
    bad.write_text("[[broken toml")
    with pytest.raises(ConfigError, match="parse error"):
        load_contexts(bad)


def test_contexts_file_save_and_load_with_peer_endpoint(tmp_path: Path) -> None:
    """Contexts with peer endpoint round-trip correctly."""
    path = tmp_path / "contexts.toml"
    entry = ContextEntry(
        name="ops",
        cluster=ClusterEntry(name="ops", ca_data="Y2E="),
        endpoints=ContextEndpoints(kv="n1:7000", peer="n1:7001"),
        credentials=ContextCredentials(cert_data="Y2VydA==", key_data="a2V5"),
    )
    cf = ContextsFile(current_context="ops")
    cf.upsert_context(entry)
    cf.save(path)

    loaded = load_contexts(path)
    ctx = loaded.find_context("ops")
    assert ctx is not None
    assert ctx.endpoints.kv == "n1:7000"
    assert ctx.endpoints.peer == "n1:7001"
