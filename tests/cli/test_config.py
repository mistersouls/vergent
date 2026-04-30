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
"""Tests for tourillon CLI config sub-commands — generate and generate-context."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from tourillon.infra.cli.main import app

runner = CliRunner()


def test_config_generate_missing_ca_cert_exits_error(tmp_path: Path) -> None:
    """config generate exits non-zero when --ca-cert is missing."""
    with patch(
        "tourillon.infra.cli.commands.config.print_error",
        side_effect=SystemExit(1),
    ):
        result = runner.invoke(
            app,
            [
                "config",
                "generate",
                "--node-id",
                "n1",
                "--ca-cert",
                str(tmp_path / "absent.crt"),
                "--ca-key",
                str(tmp_path / "absent.key"),
                "--san-ip",
                "127.0.0.1",
                "--kv-bind",
                "0.0.0.0:7000",
                "--out",
                str(tmp_path / "out.toml"),
            ],
            catch_exceptions=False,
        )
    assert result.exit_code != 0


def test_config_generate_no_san_exits_error(tmp_path: Path) -> None:
    """config generate exits non-zero when no SAN is provided."""
    ca_cert = tmp_path / "ca.crt"
    ca_key = tmp_path / "ca.key"
    ca_cert.write_bytes(b"cert")
    ca_key.write_bytes(b"key")
    with patch(
        "tourillon.infra.cli.commands.config.print_error",
        side_effect=SystemExit(1),
    ):
        result = runner.invoke(
            app,
            [
                "config",
                "generate",
                "--node-id",
                "n1",
                "--ca-cert",
                str(ca_cert),
                "--ca-key",
                str(ca_key),
                "--kv-bind",
                "0.0.0.0:7000",
                "--out",
                str(tmp_path / "out.toml"),
            ],
            catch_exceptions=False,
        )
    assert result.exit_code != 0


def test_config_generate_writes_toml_on_success(tmp_path: Path) -> None:
    """config generate writes a config.toml when all arguments are valid."""
    ca_cert = tmp_path / "ca.crt"
    ca_key = tmp_path / "ca.key"
    ca_cert.write_bytes(b"CACERT")
    ca_key.write_bytes(b"CAKEY")
    out_path = tmp_path / "node.toml"

    with patch(
        "tourillon.infra.cli.commands.config._issue_cert_bytes",
        return_value=(b"CERT", b"KEY"),
    ):
        result = runner.invoke(
            app,
            [
                "config",
                "generate",
                "--node-id",
                "test-node",
                "--ca-cert",
                str(ca_cert),
                "--ca-key",
                str(ca_key),
                "--san-ip",
                "127.0.0.1",
                "--kv-bind",
                "127.0.0.1:7000",
                "--out",
                str(out_path),
            ],
        )

    assert result.exit_code == 0
    assert out_path.exists()
    content = out_path.read_text()
    assert "test-node" in content
    assert "cert_data" in content


def test_config_generate_context_writes_entry(tmp_path: Path) -> None:
    """config generate-context upserts a context entry in contexts.toml."""
    ca_cert = tmp_path / "ca.crt"
    ca_key = tmp_path / "ca.key"
    ca_cert.write_bytes(b"CACERT")
    ca_key.write_bytes(b"CAKEY")
    contexts_path = tmp_path / "contexts.toml"

    with patch(
        "tourillon.infra.cli.commands.config._issue_cert_bytes",
        return_value=(b"CERT", b"KEY"),
    ):
        result = runner.invoke(
            app,
            [
                "config",
                "generate-context",
                "prod",
                "--ca-cert",
                str(ca_cert),
                "--ca-key",
                str(ca_key),
                "--kv-endpoint",
                "127.0.0.1:7000",
                "--contexts-file",
                str(contexts_path),
            ],
        )

    assert result.exit_code == 0
    assert contexts_path.exists()
    from tourillon.bootstrap.contexts import load_contexts

    cf = load_contexts(contexts_path)
    assert cf.find_context("prod") is not None
    assert cf.current_context == "prod"


def test_config_generate_context_auto_sets_first_as_active(tmp_path: Path) -> None:
    """The first context created is automatically set as the active context."""
    ca_cert = tmp_path / "ca.crt"
    ca_key = tmp_path / "ca.key"
    ca_cert.write_bytes(b"CACERT")
    ca_key.write_bytes(b"CAKEY")
    contexts_path = tmp_path / "contexts.toml"

    with patch(
        "tourillon.infra.cli.commands.config._issue_cert_bytes",
        return_value=(b"CERT", b"KEY"),
    ):
        runner.invoke(
            app,
            [
                "config",
                "generate-context",
                "first",
                "--ca-cert",
                str(ca_cert),
                "--ca-key",
                str(ca_key),
                "--kv-endpoint",
                "node1:7000",
                "--contexts-file",
                str(contexts_path),
            ],
        )
        runner.invoke(
            app,
            [
                "config",
                "generate-context",
                "second",
                "--ca-cert",
                str(ca_cert),
                "--ca-key",
                str(ca_key),
                "--kv-endpoint",
                "node2:7000",
                "--contexts-file",
                str(contexts_path),
            ],
        )

    from tourillon.bootstrap.contexts import load_contexts

    cf = load_contexts(contexts_path)
    assert cf.current_context == "first"
    assert cf.find_context("second") is not None


def test_config_generate_context_missing_ca_cert_exits_error(tmp_path: Path) -> None:
    """config generate-context exits non-zero when --ca-cert is missing."""
    with patch(
        "tourillon.infra.cli.commands.config.print_error",
        side_effect=SystemExit(1),
    ):
        result = runner.invoke(
            app,
            [
                "config",
                "generate-context",
                "prod",
                "--ca-cert",
                str(tmp_path / "absent.crt"),
                "--ca-key",
                str(tmp_path / "absent.key"),
                "--kv-endpoint",
                "127.0.0.1:7000",
            ],
            catch_exceptions=False,
        )
    assert result.exit_code != 0
