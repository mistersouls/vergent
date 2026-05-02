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
"""SSLContext factory and TLS material validation helpers.

All TLS material is stored as base64-encoded PEM inline in TOML files.
No *_file path variants are permitted. Functions here decode the base64
wrapper and load the raw PEM bytes directly into ssl.SSLContext objects.
"""

from __future__ import annotations

import base64
import datetime
import ssl
import tempfile
from pathlib import Path


class TlsValidationError(Exception):
    """Raised when TLS material fails validation (expiry, mismatch, etc.)."""


def _decode_pem(b64_pem: str, label: str) -> bytes:
    """Decode a base64-encoded PEM string to raw PEM bytes."""
    try:
        return base64.b64decode(b64_pem)
    except Exception as exc:
        raise TlsValidationError(f"Invalid base64 for {label}: {exc}") from exc


def validate_cert_not_expired(b64_cert_pem: str) -> None:
    """Raise TlsValidationError if the certificate encoded in *b64_cert_pem* is expired."""
    from cryptography import x509

    pem = _decode_pem(b64_cert_pem, "cert_data")
    cert = x509.load_pem_x509_certificate(pem)
    now = datetime.datetime.now(datetime.UTC)
    if cert.not_valid_after_utc < now:
        raise TlsValidationError(
            f"Server certificate expired on {cert.not_valid_after_utc.date()}"
        )


def validate_cert_key_match(b64_cert_pem: str, b64_key_pem: str) -> None:
    """Raise TlsValidationError if the cert and key public keys differ."""
    from cryptography import x509
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.serialization import load_pem_private_key

    cert_pem = _decode_pem(b64_cert_pem, "cert_data")
    key_pem = _decode_pem(b64_key_pem, "key_data")

    cert = x509.load_pem_x509_certificate(cert_pem)
    try:
        key = load_pem_private_key(key_pem, password=None)
    except Exception as exc:
        raise TlsValidationError(f"Cannot load private key: {exc}") from exc

    cert_pub = cert.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    key_pub = key.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    if cert_pub != key_pub:
        raise TlsValidationError(
            "CA private key does not match CA certificate public key"
        )


def build_server_ssl_context(
    b64_cert_pem: str,
    b64_key_pem: str,
    b64_ca_pem: str,
) -> ssl.SSLContext:
    """Build a server-side mTLS SSLContext from inline base64-encoded PEM.

    The context enforces mutual TLS: clients must present a certificate signed
    by the cluster CA. There is no plaintext fallback.
    """
    cert_pem = _decode_pem(b64_cert_pem, "cert_data")
    key_pem = _decode_pem(b64_key_pem, "key_data")
    ca_pem = _decode_pem(b64_ca_pem, "ca_data")

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.check_hostname = False

    with tempfile.TemporaryDirectory() as td:
        t = Path(td)
        (t / "cert.pem").write_bytes(cert_pem)
        (t / "key.pem").write_bytes(key_pem)
        (t / "ca.pem").write_bytes(ca_pem)
        ctx.load_cert_chain(str(t / "cert.pem"), str(t / "key.pem"))
        ctx.load_verify_locations(str(t / "ca.pem"))

    return ctx


def build_client_ssl_context(
    b64_cert_pem: str,
    b64_key_pem: str,
    b64_ca_pem: str,
    server_hostname: str = "",
) -> ssl.SSLContext:
    """Build a client-side mTLS SSLContext from inline base64-encoded PEM.

    The context presents a client certificate and verifies the server against
    the cluster CA. There is no plaintext fallback.
    """
    cert_pem = _decode_pem(b64_cert_pem, "cert_data")
    key_pem = _decode_pem(b64_key_pem, "key_data")
    ca_pem = _decode_pem(b64_ca_pem, "ca_data")

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = bool(server_hostname)
    ctx.verify_mode = ssl.CERT_REQUIRED

    with tempfile.TemporaryDirectory() as td:
        t = Path(td)
        (t / "cert.pem").write_bytes(cert_pem)
        (t / "key.pem").write_bytes(key_pem)
        (t / "ca.pem").write_bytes(ca_pem)
        ctx.load_cert_chain(str(t / "cert.pem"), str(t / "key.pem"))
        ctx.load_verify_locations(str(t / "ca.pem"))

    return ctx
