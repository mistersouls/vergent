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
"""Tests for tourillon.core.config — TourillonConfig and related dataclasses."""

import pytest

from tourillon.core.config import (
    ClusterSection,
    ConfigError,
    ServerEndpointConfig,
    TlsSection,
    TourillonConfig,
)


def test_server_endpoint_config_valid() -> None:
    """ServerEndpointConfig accepts non-empty bind and advertise."""
    ep = ServerEndpointConfig(bind="0.0.0.0:7000", advertise="node1:7000")
    assert ep.bind == "0.0.0.0:7000"
    assert ep.advertise == "node1:7000"


def test_server_endpoint_config_empty_bind_raises() -> None:
    """ServerEndpointConfig rejects an empty bind address."""
    with pytest.raises(ConfigError, match="bind"):
        ServerEndpointConfig(bind="", advertise="node1:7000")


def test_server_endpoint_config_empty_advertise_raises() -> None:
    """ServerEndpointConfig rejects an empty advertise address."""
    with pytest.raises(ConfigError, match="advertise"):
        ServerEndpointConfig(bind="0.0.0.0:7000", advertise="")


def test_tls_section_valid() -> None:
    """TlsSection accepts non-empty base64 PEM fields."""
    tls = TlsSection(cert_data="abc", key_data="def", ca_data="ghi")
    assert tls.cert_data == "abc"


def test_tls_section_empty_cert_raises() -> None:
    """TlsSection rejects empty cert_data."""
    with pytest.raises(ConfigError, match="cert_data"):
        TlsSection(cert_data="", key_data="def", ca_data="ghi")


def test_tls_section_empty_key_raises() -> None:
    """TlsSection rejects empty key_data."""
    with pytest.raises(ConfigError, match="key_data"):
        TlsSection(cert_data="abc", key_data="", ca_data="ghi")


def test_tls_section_empty_ca_raises() -> None:
    """TlsSection rejects empty ca_data."""
    with pytest.raises(ConfigError, match="ca_data"):
        TlsSection(cert_data="abc", key_data="def", ca_data="")


def test_cluster_section_valid() -> None:
    """ClusterSection accepts a valid replication_factor >= 1."""
    cs = ClusterSection(seeds=("n1:7000",), replication_factor=3)
    assert cs.replication_factor == 3


def test_cluster_section_zero_replication_factor_raises() -> None:
    """ClusterSection rejects replication_factor < 1."""
    with pytest.raises(ConfigError, match="replication_factor"):
        ClusterSection(seeds=(), replication_factor=0)


def _valid_config() -> TourillonConfig:
    return TourillonConfig(
        node_id="node-1",
        data_dir="/var/lib/tourillon",
        log_level="info",
        tls=TlsSection(cert_data="c", key_data="k", ca_data="a"),
        servers_kv=ServerEndpointConfig(bind="0.0.0.0:7000", advertise="n1:7000"),
        servers_peer=None,
        cluster=ClusterSection(seeds=(), replication_factor=3),
    )


def test_tourillon_config_valid() -> None:
    """TourillonConfig accepts all valid fields."""
    cfg = _valid_config()
    assert cfg.node_id == "node-1"
    assert cfg.log_level == "info"
    assert cfg.servers_peer is None


def test_tourillon_config_with_peer_endpoint() -> None:
    """TourillonConfig accepts an optional servers_peer endpoint."""
    cfg = TourillonConfig(
        node_id="node-1",
        data_dir="/data",
        log_level="debug",
        tls=TlsSection(cert_data="c", key_data="k", ca_data="a"),
        servers_kv=ServerEndpointConfig(bind="0.0.0.0:7000", advertise="n1:7000"),
        servers_peer=ServerEndpointConfig(bind="0.0.0.0:7001", advertise="n1:7001"),
        cluster=ClusterSection(seeds=(), replication_factor=1),
    )
    assert cfg.servers_peer is not None
    assert cfg.servers_peer.bind == "0.0.0.0:7001"


def test_tourillon_config_empty_node_id_raises() -> None:
    """TourillonConfig rejects an empty node_id."""
    with pytest.raises(ConfigError, match="node.id"):
        TourillonConfig(
            node_id="",
            data_dir="/data",
            log_level="info",
            tls=TlsSection(cert_data="c", key_data="k", ca_data="a"),
            servers_kv=ServerEndpointConfig(bind="0.0.0.0:7000", advertise="n1:7000"),
            servers_peer=None,
            cluster=ClusterSection(seeds=(), replication_factor=1),
        )


def test_tourillon_config_empty_data_dir_raises() -> None:
    """TourillonConfig rejects an empty data_dir."""
    with pytest.raises(ConfigError, match="data_dir"):
        TourillonConfig(
            node_id="n1",
            data_dir="",
            log_level="info",
            tls=TlsSection(cert_data="c", key_data="k", ca_data="a"),
            servers_kv=ServerEndpointConfig(bind="0.0.0.0:7000", advertise="n1:7000"),
            servers_peer=None,
            cluster=ClusterSection(seeds=(), replication_factor=1),
        )


def test_tourillon_config_invalid_log_level_raises() -> None:
    """TourillonConfig rejects an unrecognised log level."""
    with pytest.raises(ConfigError, match="log_level"):
        TourillonConfig(
            node_id="n1",
            data_dir="/data",
            log_level="verbose",
            tls=TlsSection(cert_data="c", key_data="k", ca_data="a"),
            servers_kv=ServerEndpointConfig(bind="0.0.0.0:7000", advertise="n1:7000"),
            servers_peer=None,
            cluster=ClusterSection(seeds=(), replication_factor=1),
        )


def test_tourillon_config_all_valid_log_levels() -> None:
    """TourillonConfig accepts all four valid log level strings."""
    for level in ("debug", "info", "warning", "error"):
        cfg = TourillonConfig(
            node_id="n1",
            data_dir="/data",
            log_level=level,
            tls=TlsSection(cert_data="c", key_data="k", ca_data="a"),
            servers_kv=ServerEndpointConfig(bind="0.0.0.0:7000", advertise="n1:7000"),
            servers_peer=None,
            cluster=ClusterSection(seeds=(), replication_factor=1),
        )
        assert cfg.log_level == level
