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
"""Tests for tourctl.bootstrap.deps — configure, get_client, get_serializer."""

import pytest

from tourctl.bootstrap import deps
from tourillon.core.net.tcp.tls import TlsConfigurationError


def _reset_deps(deps_module) -> None:
    # restore initial singleton state
    deps_module._host = None
    deps_module._port = None
    deps_module._ssl_ctx = None
    deps_module._serializer = None
    deps_module._timeout = 10.0
    deps_module._certfile = None
    deps_module._keyfile = None
    deps_module._cafile = None


def test_configure_raises_tls_error_on_missing_certfile(tmp_path) -> None:
    _reset_deps(deps)
    keyfile = tmp_path / "k.pem"
    cafile = tmp_path / "ca.pem"
    keyfile.write_text("x")
    cafile.write_text("x")

    with pytest.raises(TlsConfigurationError):
        deps.configure("127.0.0.1", 7000, tmp_path / "missing.pem", keyfile, cafile)


def test_configure_raises_tls_error_on_missing_keyfile(tmp_path) -> None:
    _reset_deps(deps)
    certfile = tmp_path / "cert.pem"
    cafile = tmp_path / "ca.pem"
    certfile.write_text("x")
    cafile.write_text("x")

    with pytest.raises(TlsConfigurationError):
        deps.configure("127.0.0.1", 7000, certfile, tmp_path / "missing.key", cafile)


def test_configure_raises_tls_error_on_missing_cafile(tmp_path) -> None:
    _reset_deps(deps)
    certfile = tmp_path / "cert.pem"
    keyfile = tmp_path / "k.pem"
    certfile.write_text("x")
    keyfile.write_text("x")

    with pytest.raises(TlsConfigurationError):
        deps.configure("127.0.0.1", 7000, certfile, keyfile, tmp_path / "missing.ca")


def test_get_client_raises_runtime_error_before_configure(monkeypatch) -> None:
    _reset_deps(deps)
    # ensure configure not called
    monkeypatch.setattr(deps, "_host", None)
    monkeypatch.setattr(deps, "_port", None)
    monkeypatch.setattr(deps, "_ssl_ctx", None)
    monkeypatch.setattr(deps, "_serializer", None)

    with pytest.raises(RuntimeError):
        deps.get_client()


def test_get_serializer_raises_runtime_error_before_configure(monkeypatch) -> None:
    _reset_deps(deps)
    monkeypatch.setattr(deps, "_serializer", None)

    with pytest.raises(RuntimeError):
        deps.get_serializer()


def test_configure_idempotent_with_identical_args(tmp_path, monkeypatch) -> None:
    _reset_deps(deps)
    certfile = tmp_path / "cert.pem"
    keyfile = tmp_path / "k.pem"
    cafile = tmp_path / "ca.pem"
    certfile.write_text("x")
    keyfile.write_text("x")
    cafile.write_text("x")

    # Avoid real SSL parsing — stub the build_ssl_context used by deps
    monkeypatch.setattr(
        deps, "build_ssl_context", lambda c, k, ca, server_side=False: object()
    )

    # Should not raise when called twice with same args
    deps.configure("127.0.0.1", 7000, certfile, keyfile, cafile, timeout=5.0)
    deps.configure("127.0.0.1", 7000, certfile, keyfile, cafile, timeout=5.0)


def test_get_client_returns_tcp_client_after_configure(tmp_path, monkeypatch) -> None:
    _reset_deps(deps)
    certfile = tmp_path / "cert.pem"
    keyfile = tmp_path / "k.pem"
    cafile = tmp_path / "ca.pem"
    certfile.write_text("x")
    keyfile.write_text("x")
    cafile.write_text("x")
    monkeypatch.setattr(
        deps, "build_ssl_context", lambda c, k, ca, server_side=False: object()
    )
    deps.configure("127.0.0.1", 7000, certfile, keyfile, cafile)
    from tourctl.core.client import TcpClient

    client = deps.get_client()
    assert isinstance(client, TcpClient)


def test_get_serializer_returns_serializer_after_configure(
    tmp_path, monkeypatch
) -> None:
    _reset_deps(deps)
    certfile = tmp_path / "cert.pem"
    keyfile = tmp_path / "k.pem"
    cafile = tmp_path / "ca.pem"
    certfile.write_text("x")
    keyfile.write_text("x")
    cafile.write_text("x")
    monkeypatch.setattr(
        deps, "build_ssl_context", lambda c, k, ca, server_side=False: object()
    )
    deps.configure("127.0.0.1", 7000, certfile, keyfile, cafile)

    s = deps.get_serializer()
    assert s is not None
