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
"""Shared test fixtures for the Tourillon test suite."""

from __future__ import annotations

import base64
import datetime

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def _generate_ca() -> tuple[bytes, bytes]:
    """Return (ca_cert_pem, ca_key_pem) for a fresh self-signed CA."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Test CA")])
    now = _utcnow()
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM)
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    return cert_pem, key_pem


def _generate_leaf(
    ca_cert_pem: bytes,
    ca_key_pem: bytes,
    common_name: str = "test-node",
    valid_days: int = 365,
) -> tuple[bytes, bytes]:
    """Return (leaf_cert_pem, leaf_key_pem) signed by the given CA."""
    ca_cert = x509.load_pem_x509_certificate(ca_cert_pem)
    ca_key = serialization.load_pem_private_key(ca_key_pem, password=None)
    leaf_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = _utcnow()
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
        .issuer_name(ca_cert.subject)
        .public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=valid_days))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName(common_name)]), critical=False
        )
        .sign(ca_key, hashes.SHA256())  # type: ignore[arg-type]
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM)
    key_pem = leaf_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    return cert_pem, key_pem


@pytest.fixture(scope="session")
def ca_material() -> tuple[bytes, bytes]:
    """Session-scoped CA certificate and private key (PEM bytes)."""
    return _generate_ca()


@pytest.fixture(scope="session")
def leaf_material(ca_material: tuple[bytes, bytes]) -> tuple[bytes, bytes]:
    """Session-scoped leaf certificate and private key (PEM bytes) signed by ca_material."""
    ca_cert_pem, ca_key_pem = ca_material
    return _generate_leaf(ca_cert_pem, ca_key_pem)


@pytest.fixture(scope="session")
def valid_config_dict(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> dict:
    """A fully valid raw config dict suitable for load_config()."""
    ca_cert_pem, _ = ca_material
    leaf_cert_pem, leaf_key_pem = leaf_material
    return {
        "schema_version": 1,
        "node": {"id": "node-1", "size": "M", "data_dir": "./node-data"},
        "tls": {
            "cert_data": base64.b64encode(leaf_cert_pem).decode(),
            "key_data": base64.b64encode(leaf_key_pem).decode(),
            "ca_data": base64.b64encode(ca_cert_pem).decode(),
        },
        "servers": {
            "kv": {"bind": "0.0.0.0:7700", "advertise": ""},
            "peer": {"bind": "0.0.0.0:7701", "advertise": ""},
        },
        "cluster": {"seeds": [], "rf": 3, "partition_shift": 10},
    }
