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
"""Tests for the tourctl bootstrap main CLI."""

import importlib.metadata
from unittest.mock import patch

from typer.testing import CliRunner

from tourctl.bootstrap.main import app

runner = CliRunner()


def test_version_command_exits_0() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0


def test_version_command_calls_print_info_with_version() -> None:
    with (
        patch("tourctl.bootstrap.main.print_info") as mock_info,
        patch("importlib.metadata.version", return_value="1.2.3"),
    ):
        result = runner.invoke(app, ["version"])

    assert result.exit_code == 0
    mock_info.assert_called_once()
    assert "tourctl 1.2.3" in mock_info.call_args[0][0]


def test_version_command_falls_back_to_dev() -> None:
    with (
        patch("tourctl.bootstrap.main.print_info") as mock_info,
        patch(
            "importlib.metadata.version",
            side_effect=importlib.metadata.PackageNotFoundError,
        ),
    ):
        result = runner.invoke(app, ["version"])

    assert result.exit_code == 0
    mock_info.assert_called_once()
    assert "tourctl dev" in mock_info.call_args[0][0]


def test_tourctl_help_renders() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "tourctl" in result.output


def test_kv_subcommand_visible_in_help() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "kv" in result.output
