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
"""Tests for NodeRunner — the asyncio event loop supervisor."""

from unittest.mock import MagicMock, patch

import pytest

from tourillon.infra.cli.error import ExitError
from tourillon.infra.cli.runner import NodeRunner


def test_node_runner_stores_server() -> None:
    server = MagicMock()
    runner = NodeRunner(server)
    assert runner._server is server


def test_node_runner_run_exits_cleanly() -> None:
    server = MagicMock()
    runner = NodeRunner(server)
    with patch("tourillon.infra.cli.runner.asyncio.run", return_value=None):
        runner.run()


def test_node_runner_run_wraps_runtime_errors_as_exit_error() -> None:
    server = MagicMock()
    runner = NodeRunner(server)
    with (
        patch(
            "tourillon.infra.cli.runner.asyncio.run",
            side_effect=ValueError("boom"),
        ),
        pytest.raises(ExitError) as exc_info,
    ):
        runner.run()
    assert exc_info.value.exit_code == 2
    assert "boom" in exc_info.value.message


def test_node_runner_run_reraises_exit_error() -> None:
    server = MagicMock()
    runner = NodeRunner(server)
    original = ExitError("intended", exit_code=1)
    with (
        patch(
            "tourillon.infra.cli.runner.asyncio.run",
            side_effect=original,
        ),
        pytest.raises(ExitError) as exc_info,
    ):
        runner.run()
    assert exc_info.value is original
