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
"""Dispatcher — routes incoming Envelopes to registered ConnectionHandlers.

Handlers are registered at startup time and never modified at runtime. An
Envelope whose kind has no registered handler causes immediate connection
close without any response — the Dispatcher has no handler to produce one.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tourillon.core.ports.transport import ConnectionHandler

logger = logging.getLogger(__name__)


class Dispatcher:
    """Map Envelope kind strings to ConnectionHandler callables.

    Call register() for each kind during startup. The lookup() method returns
    the handler or None when the kind is unknown. Unknown kinds must result in
    immediate connection close without any application-level response.
    """

    def __init__(self) -> None:
        self._handlers: dict[str, ConnectionHandler] = {}

    def register(self, kind: str, handler: ConnectionHandler) -> None:
        """Register *handler* for *kind*.

        Raise ValueError if *kind* is already registered — duplicate
        registration is a programming error, not a runtime condition.
        """
        if kind in self._handlers:
            raise ValueError(f"Handler for kind {kind!r} is already registered")
        self._handlers[kind] = handler
        logger.debug("registered handler", extra={"kind": kind})

    def lookup(self, kind: str) -> ConnectionHandler | None:
        """Return the handler registered for *kind*, or None if unknown."""
        return self._handlers.get(kind)
