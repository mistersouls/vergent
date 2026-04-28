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
"""Tests for tourillon CLI node commands."""

from typer.testing import CliRunner

from tourillon.infra.cli.main import app


def test_node_start_missing_certfile_exits_with_error() -> None:
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "node",
            "start",
            "--node-id",
            "n1",
            "--certfile",
            "C:\\nonexistent\\cert.crt",
            "--keyfile",
            "C:\\nonexistent\\key.key",
            "--cafile",
            "C:\\nonexistent\\ca.crt",
        ],
        catch_exceptions=True,
    )
    assert result.exit_code != 0


def test_node_start_help_renders_shows_node_id() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["node", "start", "--help"], catch_exceptions=False)
    assert result.exit_code == 0
    assert "--node-id" in result.output


def test_version_command_outputs_version() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["version"], catch_exceptions=False)
    assert result.exit_code == 0
    assert "tourillon" in result.output
