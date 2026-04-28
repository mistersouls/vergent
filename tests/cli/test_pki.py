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
"""Tests for tourillon pki CLI commands."""

from pathlib import Path

from typer.testing import CliRunner

from tourillon.infra.cli.main import app


def _run(args, catch_exceptions=True, **kwargs):
    runner = CliRunner()
    return runner.invoke(app, args, catch_exceptions=catch_exceptions)


def test_pki_ca_creates_cert_and_key(tmp_path: Path) -> None:
    out = tmp_path
    result = _run(["pki", "ca", "--common-name", "Test", "--out-dir", str(out)])
    assert result.exit_code == 0
    assert (out / "ca.crt").exists()
    assert (out / "ca.key").exists()


def test_pki_ca_refuses_overwrite_without_force(tmp_path: Path) -> None:
    out = tmp_path
    r1 = _run(["pki", "ca", "--common-name", "Test", "--out-dir", str(out)])
    assert r1.exit_code == 0
    r2 = _run(["pki", "ca", "--common-name", "Test", "--out-dir", str(out)])
    assert r2.exit_code != 0


def test_pki_ca_overwrites_with_force(tmp_path: Path) -> None:
    out = tmp_path
    r1 = _run(["pki", "ca", "--common-name", "Test", "--out-dir", str(out)])
    assert r1.exit_code == 0
    r2 = _run(["pki", "ca", "--common-name", "Test", "--out-dir", str(out), "--force"])
    assert r2.exit_code == 0


def test_pki_server_requires_san(tmp_path: Path) -> None:
    out = tmp_path
    # create CA first
    rca = _run(["pki", "ca", "--common-name", "Test", "--out-dir", str(out)])
    assert rca.exit_code == 0
    # server without SAN should fail
    r = _run(
        [
            "pki",
            "server",
            "--ca-cert",
            str(out / "ca.crt"),
            "--ca-key",
            str(out / "ca.key"),
            "--common-name",
            "node-1",
            "--out-dir",
            str(out / "node"),
        ]
    )
    assert r.exit_code != 0


def test_pki_server_creates_cert_and_key(tmp_path: Path) -> None:
    out = tmp_path
    rca = _run(["pki", "ca", "--common-name", "Test", "--out-dir", str(out)])
    assert rca.exit_code == 0
    node_dir = out / "node"
    r = _run(
        [
            "pki",
            "server",
            "--ca-cert",
            str(out / "ca.crt"),
            "--ca-key",
            str(out / "ca.key"),
            "--common-name",
            "node-1",
            "--san-dns",
            "localhost",
            "--out-dir",
            str(node_dir),
        ]
    )
    assert r.exit_code == 0
    assert (node_dir / "server.crt").exists()
    assert (node_dir / "server.key").exists()


def test_pki_client_creates_cert_and_key(tmp_path: Path) -> None:
    out = tmp_path
    rca = _run(["pki", "ca", "--common-name", "Test", "--out-dir", str(out)])
    assert rca.exit_code == 0
    client_dir = out / "client"
    r = _run(
        [
            "pki",
            "client",
            "--ca-cert",
            str(out / "ca.crt"),
            "--ca-key",
            str(out / "ca.key"),
            "--common-name",
            "myapp",
            "--out-dir",
            str(client_dir),
        ]
    )
    assert r.exit_code == 0
    assert (client_dir / "client.crt").exists()
    assert (client_dir / "client.key").exists()


def test_pki_ca_invalid_key_size_causes_error(tmp_path: Path) -> None:
    out = tmp_path
    r = _run(
        [
            "pki",
            "ca",
            "--common-name",
            "Test",
            "--out-dir",
            str(out),
            "--key-size",
            "1024",
        ]
    )
    assert r.exit_code != 0


def test_pki_server_missing_ca_cert_exits_1(tmp_path: Path) -> None:
    out = tmp_path
    node_dir = out / "node"
    r = _run(
        [
            "pki",
            "server",
            "--ca-cert",
            str(out / "nope.crt"),
            "--ca-key",
            str(out / "nope.key"),
            "--common-name",
            "node-1",
            "--san-dns",
            "localhost",
            "--out-dir",
            str(node_dir),
        ]
    )
    assert r.exit_code != 0
