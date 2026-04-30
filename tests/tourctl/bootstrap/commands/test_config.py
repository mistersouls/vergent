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
"""Tests for tourctl.bootstrap.commands.config — use-context, list commands."""

from io import StringIO
from pathlib import Path
from unittest.mock import patch

import rich.console
from typer.testing import CliRunner

from tourctl.bootstrap.commands.config import app
from tourillon.bootstrap.contexts import (
    ClusterEntry,
    ContextCredentials,
    ContextEndpoints,
    ContextEntry,
    ContextsFile,
)
from tourillon.infra.cli import output as cli_output

runner = CliRunner()


def _write_contexts(tmp_path: Path, current: str | None = None) -> Path:
    """Write a contexts.toml with two entries and return its path."""
    cf = ContextsFile(current_context=current)
    cf.upsert_context(
        ContextEntry(
            name="prod",
            cluster=ClusterEntry(name="prod", ca_data="Y2E="),
            endpoints=ContextEndpoints(kv="prod-node:7000"),
            credentials=ContextCredentials(cert_data="Y2VydA==", key_data="a2V5"),
        )
    )
    cf.upsert_context(
        ContextEntry(
            name="staging",
            cluster=ClusterEntry(name="staging", ca_data="Y2E="),
            endpoints=ContextEndpoints(kv="staging-node:7000"),
            credentials=ContextCredentials(cert_data="Y2VydA==", key_data="a2V5"),
        )
    )
    path = tmp_path / "contexts.toml"
    cf.save(path)
    return path


def test_use_context_sets_active_context(tmp_path: Path) -> None:
    """use-context writes the named context as current-context."""
    path = _write_contexts(tmp_path)
    result = runner.invoke(
        app,
        ["use-context", "prod", "--contexts-file", str(path)],
    )
    assert result.exit_code == 0
    from tourillon.bootstrap.contexts import load_contexts

    loaded = load_contexts(path)
    assert loaded.current_context == "prod"


def test_use_context_unknown_name_exits_nonzero(tmp_path: Path) -> None:
    """use-context on an unknown context name exits non-zero."""
    path = _write_contexts(tmp_path)
    with patch(
        "tourctl.bootstrap.commands.config.print_error",
        side_effect=SystemExit(1),
    ):
        result = runner.invoke(
            app,
            ["use-context", "nonexistent", "--contexts-file", str(path)],
            catch_exceptions=False,
        )
    assert result.exit_code != 0


def test_list_contexts_shows_all_contexts(tmp_path: Path) -> None:
    """list prints all context names."""
    buf = StringIO()
    path = _write_contexts(tmp_path, current="prod")
    captured = rich.console.Console(file=buf, highlight=False, force_terminal=False)
    with patch.object(cli_output, "_console", captured):
        result = runner.invoke(
            app,
            ["list", "--contexts-file", str(path)],
        )
    assert result.exit_code == 0
    output_text = buf.getvalue()
    assert "prod" in output_text
    assert "staging" in output_text


def test_list_contexts_marks_active_context(tmp_path: Path) -> None:
    """list marks the active context with an asterisk."""
    buf = StringIO()
    path = _write_contexts(tmp_path, current="staging")
    captured = rich.console.Console(file=buf, highlight=False, force_terminal=False)
    with patch.object(cli_output, "_console", captured):
        runner.invoke(
            app,
            ["list", "--contexts-file", str(path)],
        )
    output_text = buf.getvalue()
    active_line = next(
        (line for line in output_text.splitlines() if "staging" in line), ""
    )
    assert "*" in active_line


def test_list_contexts_empty_prints_hint(tmp_path: Path) -> None:
    """list informs the operator when no contexts exist."""
    buf = StringIO()
    path = tmp_path / "empty.toml"
    captured = rich.console.Console(file=buf, highlight=False, force_terminal=False)
    with patch.object(cli_output, "_console", captured):
        result = runner.invoke(
            app,
            ["list", "--contexts-file", str(path)],
        )
    assert result.exit_code == 0
    assert "No contexts" in buf.getvalue()
