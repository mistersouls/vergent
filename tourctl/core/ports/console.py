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
"""ConsolePort — output-console abstraction for tourctl's core layer.

The core layer must not import rich.Console or any other third-party rendering
library.  Command classes depend on this protocol; the actual rich.Console
adapter is injected at startup by tourctl/infra/cli/.
"""

from __future__ import annotations

from typing import Any, Protocol


class ConsolePort(Protocol):
    """Minimal console output port used by tourctl command classes.

    Structural subtyping: rich.Console satisfies this protocol without any
    explicit declaration.  The method signature uses *args / **kwargs to remain
    compatible with the full rich.Console.print signature while keeping the
    protocol simple enough for test doubles.
    """

    def print(self, *args: Any, **kwargs: Any) -> None:
        """Emit one line of output to this console."""
