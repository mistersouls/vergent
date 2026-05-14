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
"""NodeJoinCommand — sends node.join directly to the target node and renders the result."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from tourctl.core.ports.console import ConsolePort
from tourillon.core.ports.transport import RESPONSE_TIMEOUT, ResponseTimeoutError
from tourillon.core.structure.envelope import Envelope
from tourillon.core.transport.client import TcpClient

if TYPE_CHECKING:
    from tourillon.core.ports.serializer import SerializerPort


class NodeJoinCommand:
    """Sends node.join directly to the target node's peer address and renders the result.

    tourctl connects to the target peer address explicitly provided by the
    operator; no forwarding or registry lookup is involved.
    """

    def __init__(
        self,
        client: TcpClient,
        serializer: SerializerPort,
        console: ConsolePort,
        err_console: ConsolePort,
        peer_address: str = "",
    ) -> None:
        self._client = client
        self._serializer = serializer
        self._console = console
        self._err_console = err_console
        self._peer_address = peer_address

    async def run(
        self,
        seeds: list[str] | None = None,
        timeout: float = RESPONSE_TIMEOUT,
    ) -> int:
        """Send node.join and print the result. Return 0 on success, 1 on error."""
        self._console.print(f"Connecting to {self._peer_address} …\n")

        payload = self._serializer.encode({"seeds": seeds or []})
        req = Envelope.create(
            payload,
            kind="node.join",
            schema_id=self._serializer.schema_id,
        )

        try:
            resp = await self._client.request(req, timeout=timeout)
        except ResponseTimeoutError:
            self._err_console.print(f"✗ node.join timed out after {timeout:.0f}s.")
            return 1

        if resp.kind == "node.join.error":
            return self._handle_error(resp)

        if resp.kind == "node.join.ok":
            return self._render_ok(resp)

        self._err_console.print(f"✗ Unexpected response kind: {resp.kind}")
        return 1

    def _handle_error(self, resp: Envelope) -> int:
        """Decode and print node.join.error; return 1."""
        try:
            data: dict[str, Any] = self._serializer.decode(resp.payload)
            code = data.get("code", "unknown")
            message = data.get("message", "")
        except Exception:
            code = "decode_error"
            message = ""

        if code == "wrong_phase":
            self._err_console.print(f"Error: {message}")
        elif code == "no_seeds":
            self._err_console.print(
                "Error: no seeds available; provide --seeds or configure [cluster].seeds."
            )
        else:
            self._err_console.print(f"Error: {message or code}")
        return 1

    def _render_ok(self, resp: Envelope) -> int:
        """Decode and render node.join.ok; return 0."""
        try:
            data: dict[str, Any] = self._serializer.decode(resp.payload)
            node_id = data.get("node_id", "")
            phase = data.get("phase", "joining")
            peer_address = data.get("peer_address", "") or self._peer_address
        except Exception:
            node_id = ""
            phase = "joining"
            peer_address = self._peer_address

        self._console.print(f"Seeded join initiated for node {node_id!r}.")
        self._console.print(f"Phase → {phase}.")
        self._console.print(
            f"Monitor progress with: tourctl node inspect {peer_address}"
        )
        return 0
