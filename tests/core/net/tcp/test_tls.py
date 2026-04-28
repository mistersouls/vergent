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

import ssl
from pathlib import Path

import pytest

from tourillon.core.net.tcp.tls import (
    MINIMUM_TLS_VERSION,
    TlsConfigurationError,
    build_ssl_context,
)


def test_build_ssl_context_missing_certfile_raises(tmp_path: Path) -> None:
    missing = tmp_path / "no_cert.pem"
    key = tmp_path / "key.pem"
    ca = tmp_path / "ca.pem"
    # create key and ca so only cert is missing
    key.write_bytes(b"dummy")
    ca.write_bytes(b"dummy")

    with pytest.raises(TlsConfigurationError):
        build_ssl_context(missing, key, ca)


def test_build_ssl_context_missing_keyfile_raises(tmp_path: Path) -> None:
    cert = tmp_path / "cert.pem"
    missing = tmp_path / "no_key.pem"
    ca = tmp_path / "ca.pem"
    cert.write_bytes(b"dummy")
    ca.write_bytes(b"dummy")

    with pytest.raises(TlsConfigurationError):
        build_ssl_context(cert, missing, ca)


def test_build_ssl_context_missing_cafile_raises(tmp_path: Path) -> None:
    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    missing = tmp_path / "no_ca.pem"
    cert.write_bytes(b"dummy")
    key.write_bytes(b"dummy")

    with pytest.raises(TlsConfigurationError):
        build_ssl_context(cert, key, missing)


def test_build_ssl_context_server_side_sets_cert_required(tmp_path: Path) -> None:
    # create files that exist but contain invalid material so load_cert_chain fails
    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    ca = tmp_path / "ca.pem"
    cert.write_bytes(b"not a cert")
    key.write_bytes(b"not a key")
    ca.write_bytes(b"not a ca")

    with pytest.raises(TlsConfigurationError):
        build_ssl_context(cert, key, ca, server_side=True)


def test_build_ssl_context_server_side_check_hostname_false(
    tmp_path: Path, monkeypatch
) -> None:
    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    ca = tmp_path / "ca.pem"
    cert.write_bytes(b"dummy")
    key.write_bytes(b"dummy")
    ca.write_bytes(b"dummy")

    # monkeypatch SSLContext methods so build_ssl_context returns a context
    def _noop_load_cert_chain(self, certfile, keyfile):
        return None

    def _noop_load_verify_locations(self, cafile=None, capath=None, cadata=None):
        return None

    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", _noop_load_cert_chain)
    monkeypatch.setattr(
        ssl.SSLContext, "load_verify_locations", _noop_load_verify_locations
    )

    ctx = build_ssl_context(cert, key, ca, server_side=True)
    assert ctx.check_hostname is False
    assert ctx.verify_mode == ssl.CERT_REQUIRED


def test_build_ssl_context_client_side_check_hostname_true(
    tmp_path: Path, monkeypatch
) -> None:
    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    ca = tmp_path / "ca.pem"
    cert.write_bytes(b"dummy")
    key.write_bytes(b"dummy")
    ca.write_bytes(b"dummy")

    def _noop_load_cert_chain(self, certfile, keyfile):
        return None

    def _noop_load_verify_locations(self, cafile=None, capath=None, cadata=None):
        return None

    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", _noop_load_cert_chain)
    monkeypatch.setattr(
        ssl.SSLContext, "load_verify_locations", _noop_load_verify_locations
    )

    ctx = build_ssl_context(cert, key, ca, server_side=False)
    assert ctx.check_hostname is True
    assert ctx.verify_mode == ssl.CERT_REQUIRED


def test_minimum_tls_version_is_1_3() -> None:
    assert ssl.TLSVersion.TLSv1_3 == MINIMUM_TLS_VERSION
