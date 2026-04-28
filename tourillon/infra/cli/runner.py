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
"""Asyncio event loop wrapper that supervises a TcpServer until a signal arrives.

NodeRunner bridges the synchronous Typer command layer with the asyncio
TcpServer lifecycle. It installs OS signal handlers for SIGINT and SIGTERM,
starts the server, blocks until a stop signal is received, and then shuts
the server down cleanly before returning. The cross-platform signal strategy
is:

- POSIX (Linux, macOS): loop.add_signal_handler is used; the handler sets an
  asyncio.Event from within the running loop.
- Windows: asyncio does not support add_signal_handler. signal.signal is used
  with a threading.Event bridge; the handler calls loop.call_soon_threadsafe to
  set the asyncio.Event from the signal thread.
"""

import asyncio
import signal
import sys
import threading
from types import FrameType

from tourillon.core.net.tcp.server import TcpServer
from tourillon.infra.cli.error import ExitError


class NodeRunner:
    """Supervise a TcpServer for its full operational lifetime.

    Instantiate with a pre-assembled TcpServer (e.g. from create_tcp_node),
    then call run(). The method blocks until SIGINT or SIGTERM is received,
    calls server.stop(), and returns. Any exception from server.start() is
    wrapped in ExitError and re-raised so that the CLI error hook can display
    it without a traceback.
    """

    def __init__(self, server: TcpServer) -> None:
        self._server = server

    def run(self) -> None:
        """Start the server and block until a termination signal is received."""
        try:
            asyncio.run(self._main())
        except ExitError:
            raise
        except Exception as exc:
            raise ExitError(str(exc), exit_code=2) from exc

    async def _main(self) -> None:
        """Async entry point: wire signals, start server, wait, stop."""
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        if sys.platform != "win32":
            loop.add_signal_handler(signal.SIGINT, stop_event.set)
            loop.add_signal_handler(signal.SIGTERM, stop_event.set)
        else:
            _thread_evt = threading.Event()

            def _win_handler(signum: int, _frame: FrameType | None) -> None:
                _thread_evt.set()
                loop.call_soon_threadsafe(stop_event.set)

            signal.signal(signal.SIGINT, _win_handler)
            signal.signal(signal.SIGTERM, _win_handler)

        try:
            await self._server.start()
        except Exception as exc:
            raise ExitError(f"Failed to start server: {exc}", exit_code=2) from exc

        try:
            await stop_event.wait()
        finally:
            await self._server.stop()
