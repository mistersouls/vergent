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
"""Tests for infra/tls/context.py validation helpers."""

from __future__ import annotations

import base64
import datetime

import pytest

from tourillon.infra.tls.context import (
    TlsValidationError,
    build_client_ssl_context,
    build_server_ssl_context,
    validate_cert_key_match,
    validate_cert_not_expired,
)


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


@pytest.mark.bootstrap
def test_validate_cert_not_expired_invalid_base64_raises() -> None:
    """TlsValidationError raised when cert_data is not valid base64."""
    with pytest.raises(TlsValidationError, match="Invalid base64"):
        validate_cert_not_expired("!!! not base64 !!!")


@pytest.mark.bootstrap
def test_validate_cert_key_match_corrupted_key_raises(
    leaf_material: tuple[bytes, bytes],
) -> None:
    """TlsValidationError raised when key PEM is corrupted (not valid key bytes)."""
    cert_pem, _ = leaf_material
    cert_b64 = base64.b64encode(cert_pem).decode()
    # Valid base64 but not a PEM private key
    bad_key_b64 = base64.b64encode(b"not a private key").decode()
    with pytest.raises(TlsValidationError, match="Cannot load private key"):
        validate_cert_key_match(cert_b64, bad_key_b64)


@pytest.mark.bootstrap
def test_validate_cert_not_expired_valid_cert_passes(
    leaf_material: tuple[bytes, bytes],
) -> None:
    """validate_cert_not_expired does not raise for a valid non-expired cert."""
    cert_pem, _ = leaf_material
    validate_cert_not_expired(base64.b64encode(cert_pem).decode())


@pytest.mark.bootstrap
def test_build_server_ssl_context_returns_context(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """build_server_ssl_context returns an ssl.SSLContext without raising."""
    import ssl

    ca_cert_pem, _ = ca_material
    leaf_cert_pem, leaf_key_pem = leaf_material
    ctx = build_server_ssl_context(
        base64.b64encode(leaf_cert_pem).decode(),
        base64.b64encode(leaf_key_pem).decode(),
        base64.b64encode(ca_cert_pem).decode(),
    )
    assert isinstance(ctx, ssl.SSLContext)


@pytest.mark.bootstrap
def test_build_client_ssl_context_returns_context(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """build_client_ssl_context returns an ssl.SSLContext without raising."""
    import ssl

    ca_cert_pem, _ = ca_material
    leaf_cert_pem, leaf_key_pem = leaf_material
    ctx = build_client_ssl_context(
        base64.b64encode(leaf_cert_pem).decode(),
        base64.b64encode(leaf_key_pem).decode(),
        base64.b64encode(ca_cert_pem).decode(),
    )
    assert isinstance(ctx, ssl.SSLContext)
