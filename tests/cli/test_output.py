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
"""Tests for CLI output helpers."""

from io import StringIO

import pytest
import rich.console

from tourillon.infra.cli import output


def _make_captured_console() -> rich.console.Console:
    return rich.console.Console(file=StringIO(), force_terminal=False)


def test_print_success_contains_checkmark_ok_message() -> None:
    console = _make_captured_console()
    output._console = console
    output.print_success("ok")
    text = console.file.getvalue()
    assert "ok" in text


def test_print_error_exits_with_code_1() -> None:
    console = _make_captured_console()
    output._console = console
    with pytest.raises(SystemExit) as excinfo:
        output.print_error("bad")
    assert excinfo.value.code == 1


def test_print_error_exits_with_custom_code_2() -> None:
    console = _make_captured_console()
    output._console = console
    with pytest.raises(SystemExit) as excinfo:
        output.print_error("bad", exit_code=2)
    assert excinfo.value.code == 2


def test_print_info_contains_message() -> None:
    console = _make_captured_console()
    output._console = console
    output.print_info("hello")
    text = console.file.getvalue()
    assert "hello" in text


def test_print_warning_contains_message() -> None:
    console = _make_captured_console()
    output._console = console
    output.print_warning("warn")
    text = console.file.getvalue()
    assert "warn" in text


def test_print_key_value_renders_rows() -> None:
    console = _make_captured_console()
    output._console = console
    output.print_key_value("Title", [("key", "val")])
    text = console.file.getvalue()
    assert "key" in text
    assert "val" in text
