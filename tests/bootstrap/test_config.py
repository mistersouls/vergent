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
"""Tests for tourillon.bootstrap.config — load_config and write_config_file."""

import base64
from pathlib import Path

import pytest

from tourillon.bootstrap.config import build_node_toml, load_config, write_config_file
from tourillon.core.config import ConfigError

_MINIMAL_TOML = """\
[node]
id = "test-node"
data_dir = "/tmp/data"
log_level = "info"

[tls]
cert_data = "Y2VydA=="
key_data = "a2V5"
ca_data = "Y2E="

[servers.kv]
bind = "0.0.0.0:7000"
advertise = "127.0.0.1:7000"

[cluster]
seeds = []
replication_factor = 3
"""


def test_load_config_minimal_valid(tmp_path: Path) -> None:
    """load_config returns a valid TourillonConfig for a minimal TOML file."""
    cfg_file = tmp_path / "node.toml"
    cfg_file.write_text(_MINIMAL_TOML)
    cfg = load_config(cfg_file)
    assert cfg.node_id == "test-node"
    assert cfg.log_level == "info"
    assert cfg.servers_kv.bind == "0.0.0.0:7000"
    assert cfg.servers_peer is None


def test_load_config_missing_file_raises(tmp_path: Path) -> None:
    """load_config raises ConfigError when the file does not exist."""
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "absent.toml")


def test_load_config_invalid_toml_raises(tmp_path: Path) -> None:
    """load_config raises ConfigError on malformed TOML."""
    bad = tmp_path / "bad.toml"
    bad.write_text("[[invalid toml")
    with pytest.raises(ConfigError, match="parse error"):
        load_config(bad)


def test_load_config_cli_node_id_overrides_file(tmp_path: Path) -> None:
    """CLI node_id override shadows the value in the config file."""
    cfg_file = tmp_path / "node.toml"
    cfg_file.write_text(_MINIMAL_TOML)
    cfg = load_config(cfg_file, node_id="override-node")
    assert cfg.node_id == "override-node"


def test_load_config_cli_log_level_overrides_file(tmp_path: Path) -> None:
    """CLI log_level override shadows the value in the config file."""
    cfg_file = tmp_path / "node.toml"
    cfg_file.write_text(_MINIMAL_TOML)
    cfg = load_config(cfg_file, log_level="debug")
    assert cfg.log_level == "debug"


def test_load_config_env_node_id_overrides_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TOURILLON_NODE_ID env override shadows the file value."""
    cfg_file = tmp_path / "node.toml"
    cfg_file.write_text(_MINIMAL_TOML)
    monkeypatch.setenv("TOURILLON_NODE_ID", "env-node")
    cfg = load_config(cfg_file)
    assert cfg.node_id == "env-node"


def test_load_config_missing_node_id_raises(tmp_path: Path) -> None:
    """load_config raises ConfigError when node.id is absent and no override."""
    toml = _MINIMAL_TOML.replace('id = "test-node"\n', "")
    cfg_file = tmp_path / "node.toml"
    cfg_file.write_text(toml)
    with pytest.raises(ConfigError, match="node.id"):
        load_config(cfg_file)


def test_load_config_missing_tls_cert_raises(tmp_path: Path) -> None:
    """load_config raises ConfigError when tls.cert_data is absent."""
    toml = _MINIMAL_TOML.replace('cert_data = "Y2VydA=="\n', "")
    cfg_file = tmp_path / "node.toml"
    cfg_file.write_text(toml)
    with pytest.raises(ConfigError, match="cert_data"):
        load_config(cfg_file)


def test_load_config_missing_kv_bind_raises(tmp_path: Path) -> None:
    """load_config raises ConfigError when servers.kv.bind is absent."""
    toml = _MINIMAL_TOML.replace('bind = "0.0.0.0:7000"\n', "")
    cfg_file = tmp_path / "node.toml"
    cfg_file.write_text(toml)
    with pytest.raises(ConfigError, match="servers.kv.bind"):
        load_config(cfg_file)


def test_load_config_advertise_defaults_to_bind(tmp_path: Path) -> None:
    """When kv.advertise is absent, it defaults to kv.bind."""
    toml = _MINIMAL_TOML.replace('advertise = "127.0.0.1:7000"\n', "")
    cfg_file = tmp_path / "node.toml"
    cfg_file.write_text(toml)
    cfg = load_config(cfg_file)
    assert cfg.servers_kv.advertise == cfg.servers_kv.bind


def test_load_config_with_peer_section(tmp_path: Path) -> None:
    """load_config parses an optional [servers.peer] section."""
    toml = _MINIMAL_TOML + '\n[servers.peer]\nbind = "0.0.0.0:7001"\n'
    cfg_file = tmp_path / "node.toml"
    cfg_file.write_text(toml)
    cfg = load_config(cfg_file)
    assert cfg.servers_peer is not None
    assert cfg.servers_peer.bind == "0.0.0.0:7001"


def test_write_config_file_creates_file(tmp_path: Path) -> None:
    """write_config_file creates the file with the given content."""
    out = tmp_path / "out.toml"
    write_config_file(out, "hello = true\n")
    assert out.read_text() == "hello = true\n"


def test_write_config_file_is_atomic(tmp_path: Path) -> None:
    """write_config_file replaces atomically — no partial file left on failure."""
    existing = tmp_path / "out.toml"
    existing.write_text("original")
    write_config_file(existing, "updated")
    assert existing.read_text() == "updated"


def test_build_node_toml_round_trips(tmp_path: Path) -> None:
    """build_node_toml output loads back into a valid TourillonConfig."""
    cert_b64 = base64.b64encode(b"cert").decode()
    key_b64 = base64.b64encode(b"key").decode()
    ca_b64 = base64.b64encode(b"ca").decode()
    toml_str = build_node_toml(
        node_id="n1",
        data_dir="/data",
        log_level="info",
        cert_data=cert_b64,
        key_data=key_b64,
        ca_data=ca_b64,
        kv_bind="0.0.0.0:7000",
        kv_advertise="n1:7000",
    )
    cfg_file = tmp_path / "node.toml"
    cfg_file.write_text(toml_str)
    cfg = load_config(cfg_file)
    assert cfg.node_id == "n1"
    assert cfg.tls.cert_data == cert_b64


def test_build_node_toml_with_peer_section(tmp_path: Path) -> None:
    """build_node_toml includes [servers.peer] when peer_bind is supplied."""
    toml_str = build_node_toml(
        node_id="n1",
        data_dir="/data",
        log_level="info",
        cert_data="c",
        key_data="k",
        ca_data="a",
        kv_bind="0.0.0.0:7000",
        kv_advertise="n1:7000",
        peer_bind="0.0.0.0:7001",
        peer_advertise="n1:7001",
    )
    cfg_file = tmp_path / "node.toml"
    cfg_file.write_text(toml_str)
    cfg = load_config(cfg_file)
    assert cfg.servers_peer is not None
    assert cfg.servers_peer.bind == "0.0.0.0:7001"
