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
"""Tests for CLI error handling utilities."""

import sys

import pytest
import rich.console

from tourillon.infra.cli.error import ExitError, configure_error_hook


def test_exit_error_has_default_exit_code_1() -> None:
    err = ExitError("msg")
    assert err.exit_code == 1


def test_exit_error_custom_code_2() -> None:
    err = ExitError("msg", exit_code=2)
    assert err.exit_code == 2


def test_configure_error_hook_exit_error_triggers_system_exit_1() -> None:
    console = rich.console.Console(file=None)
    configure_error_hook(console)
    exc = ExitError("fail", 1)
    with pytest.raises(SystemExit) as excinfo:
        sys.excepthook(type(exc), exc, None)  # type: ignore[arg-type]
    assert excinfo.value.code == 1


def test_configure_error_hook_keyboard_interrupt_exits_0() -> None:
    console = rich.console.Console(file=None)
    configure_error_hook(console)
    with pytest.raises(SystemExit) as excinfo:
        sys.excepthook(KeyboardInterrupt, KeyboardInterrupt(), None)  # type: ignore[arg-type]
    assert excinfo.value.code == 0


def test_configure_error_hook_unexpected_error_exits_2() -> None:
    console = rich.console.Console(file=None)
    configure_error_hook(console)
    with pytest.raises(SystemExit) as excinfo:
        sys.excepthook(RuntimeError, RuntimeError("boom"), None)  # type: ignore[arg-type]
    assert excinfo.value.code == 2
