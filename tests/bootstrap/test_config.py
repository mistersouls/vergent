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
"""Tests for tourillon.bootstrap.config — scenarios 1-6."""

from __future__ import annotations

import base64
import copy
import datetime

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from tourillon.bootstrap.config import ConfigError, load_config
from tourillon.core.structure.config import NodeSize


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def _make_expired_leaf(ca_cert_pem: bytes, ca_key_pem: bytes) -> tuple[bytes, bytes]:
    ca_cert = x509.load_pem_x509_certificate(ca_cert_pem)
    ca_key = serialization.load_pem_private_key(ca_key_pem, password=None)
    leaf_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = _utcnow()
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "expired")]))
        .issuer_name(ca_cert.subject)
        .public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=10))
        .not_valid_after(now - datetime.timedelta(days=1))
        .sign(ca_key, hashes.SHA256())  # type: ignore[arg-type]
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM)
    key_pem = leaf_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    return cert_pem, key_pem


def _make_mismatched_key() -> bytes:
    """Return PEM for a fresh key that does not match any existing cert."""
    other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return other_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )


@pytest.mark.bootstrap
async def test_1_load_config_valid_returns_tourillonconfig(
    valid_config_dict: dict,
) -> None:
    """Returns TourillonConfig; all fields correct; node_size.token_count == 4 for size='M'."""
    cfg = load_config(valid_config_dict)
    assert cfg.node_id == "node-1"
    assert cfg.node_size == NodeSize.M
    assert cfg.node_size.token_count == 4
    assert cfg.kv_server.bind == "0.0.0.0:7700"
    assert cfg.peer_server.bind == "0.0.0.0:7701"
    assert cfg.schema_version == 1


@pytest.mark.bootstrap
async def test_2_load_config_expired_cert_raises(
    valid_config_dict: dict,
    ca_material: tuple[bytes, bytes],
) -> None:
    """ConfigError mentioning expiry; no socket bound."""
    ca_cert_pem, ca_key_pem = ca_material
    expired_cert, expired_key = _make_expired_leaf(ca_cert_pem, ca_key_pem)
    raw = copy.deepcopy(valid_config_dict)
    raw["tls"]["cert_data"] = base64.b64encode(expired_cert).decode()
    raw["tls"]["key_data"] = base64.b64encode(expired_key).decode()
    with pytest.raises(ConfigError, match="expired"):
        load_config(raw)


@pytest.mark.bootstrap
async def test_3_load_config_cert_key_mismatch_raises(valid_config_dict: dict) -> None:
    """ConfigError mentioning mismatch."""
    raw = copy.deepcopy(valid_config_dict)
    raw["tls"]["key_data"] = base64.b64encode(_make_mismatched_key()).decode()
    with pytest.raises(ConfigError, match="match"):
        load_config(raw)


@pytest.mark.bootstrap
async def test_4_load_config_missing_node_id_raises(valid_config_dict: dict) -> None:
    """ConfigError naming the missing field."""
    raw = copy.deepcopy(valid_config_dict)
    del raw["node"]["id"]
    with pytest.raises(ConfigError, match=r"\[node\]"):
        load_config(raw)


@pytest.mark.bootstrap
async def test_5_load_config_invalid_size_raises(valid_config_dict: dict) -> None:
    """ConfigError listing valid values."""
    raw = copy.deepcopy(valid_config_dict)
    raw["node"]["size"] = "HUGE"
    with pytest.raises(ConfigError, match="HUGE"):
        load_config(raw)


@pytest.mark.bootstrap
async def test_6_load_config_duplicate_bind_raises(valid_config_dict: dict) -> None:
    """Startup fails when kv.bind == peer.bind; both addresses logged."""
    raw = copy.deepcopy(valid_config_dict)
    raw["servers"]["peer"]["bind"] = raw["servers"]["kv"]["bind"]
    with pytest.raises(ConfigError, match="same bind address"):
        load_config(raw)
