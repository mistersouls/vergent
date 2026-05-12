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
"""RebalanceStatusCommand — sends rebalance.status and renders the output."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from tourctl.core.ports.console import ConsolePort
from tourillon.core.ports.transport import RESPONSE_TIMEOUT, ResponseTimeoutError
from tourillon.core.structure.envelope import Envelope
from tourillon.core.transport.client import TcpClient

if TYPE_CHECKING:
    from tourillon.core.ports.serializer import SerializerPort


class RebalanceStatusCommand:
    """Sends rebalance.status and renders the paginated response.

    Connects to the peer endpoint of the active context (or --node address),
    sends the request with after_pid and limit, and renders the response
    table. When --blocked is set, filters to FAILED transfers and prints the
    operator hint block. Returns exit code 0 on success, 1 on error, or 2
    when --blocked and the node is BLOCKED.
    """

    def __init__(
        self,
        client: TcpClient,
        serializer: SerializerPort,
        console: ConsolePort,
        err_console: ConsolePort,
    ) -> None:
        self._client = client
        self._serializer = serializer
        self._console = console
        self._err_console = err_console

    async def run(
        self,
        *,
        after_pid: int = 0,
        limit: int = 500,
        blocked_only: bool = False,
        json_output: bool = False,
        timeout: float = RESPONSE_TIMEOUT,
    ) -> int:
        """Execute the rebalance status request and render results.

        Return 0 on success, 1 on error, 2 when blocked_only=True and the
        node is BLOCKED.
        """
        payload = self._serializer.encode({"after_pid": after_pid, "limit": limit})
        req = Envelope.create(
            payload,
            kind="rebalance.status",
            schema_id=self._serializer.schema_id,
        )
        try:
            resp = await self._client.request(req, timeout=timeout)
        except ResponseTimeoutError:
            self._err_console.print(
                f"✗ Rebalance status timed out after {timeout:.0f}s."
            )
            return 1

        if resp.kind.startswith("error."):
            self._err_console.print(f"✗ Error response: {resp.kind}")
            return 1

        try:
            data = self._serializer.decode(resp.payload)
        except Exception:
            self._err_console.print("✗ Failed to decode response payload.")
            return 1

        if json_output:
            self._console.print(json.dumps(data, indent=2, default=str))
            return 0 if not (blocked_only and data.get("blocked")) else 2

        return self._render(data, blocked_only=blocked_only)

    def _render(self, data: dict[str, Any], *, blocked_only: bool) -> int:
        """Render human-readable Rich output."""
        epoch = data.get("epoch")
        if epoch is None:
            node_hint = data.get("node_id", "")
            self._console.print(
                f"No active rebalance{' on ' + node_hint if node_hint else ''}."
            )
            return 0

        trigger = data.get("trigger", "?")
        role = data.get("role", "?")
        blocked = data.get("blocked", False)
        summary = data.get("summary", {})
        active = data.get("active_partitions", 0)
        transfers = data.get("transfers", [])

        phase_hint = "JOINING → READY" if trigger == "joining" else "DRAINING → IDLE"
        self._console.print(f"\nRebalance status — (epoch {epoch}, {phase_hint})")
        self._console.print(f"Role: {role}")
        if blocked:
            self._console.print("State: BLOCKED\n")

        committed = summary.get("committed", 0)
        running = summary.get("running", 0)
        failed = summary.get("failed", 0)
        self._console.print(
            f"Summary: {active} active partitions "
            f"({committed} committed, {running} running, {failed} failed)\n"
        )

        if blocked_only:
            return self._render_blocked(transfers)

        self._render_table(transfers)
        return 0

    def _render_blocked(self, transfers: list[dict[str, Any]]) -> int:
        """Render blocked transfers and operator hint. Return exit code 2."""
        failed = [t for t in transfers if t.get("state") == "failed"]
        if not failed:
            self._console.print("No FAILED transfers found.")
            return 0

        self._console.print("Blocking transfers:")
        self._console.print(
            f" {'PID':<6} {'FROM':<8} {'TO':<8} {'STATE':<8} LAST_ERROR"
        )
        self._console.print("─" * 75)
        for t in failed:
            self._console.print(
                f"  {t['pid']:<5} {t['src']:<8} {t['dst']:<8} "
                f"{'FAILED':<8} {t.get('last_error') or '—'}"
            )

        self._console.print(
            "\nOperator hint:\n"
            "  - Check failing source/destination node states (FAILED/PAUSED?).\n"
            "  - Either:\n"
            "      recover them (FAILED → JOINING / READY), or\n"
            "      change topology to trigger a new epoch and plan."
        )
        return 2

    def _render_table(self, transfers: list[dict[str, Any]]) -> None:
        """Render the full transfer table."""
        self._console.print(
            f" {'PID':<5} {'FROM':<8} {'TO':<8} {'STATE':<11} "
            f"{'CHUNKS':<9} {'BYTES':<10} {'AGE':<6} LAST_ERROR"
        )
        self._console.print("─" * 75)
        for t in transfers:
            pid = t.get("pid", "")
            src = t.get("src", "")
            dst = t.get("dst", "")
            state = str(t.get("state", "")).upper()
            cd = t.get("chunks_done", 0)
            ct = t.get("chunks_total")
            chunks = f"{cd}/{ct}" if ct is not None else (str(cd) if cd else "—")
            bd = t.get("bytes_done", 0)
            size = _fmt_bytes(bd) if bd else "—"
            err = t.get("last_error") or "—"
            self._console.print(
                f"  {pid:<4} {src:<8} {dst:<8} {state:<11} "
                f"{chunks:<9} {size:<10} {'—':<6} {err}"
            )


def _fmt_bytes(n: int) -> str:
    """Format byte count as human-readable string."""
    if n >= 1_048_576:
        return f"{n / 1_048_576:.1f} MiB"
    if n >= 1024:
        return f"{n / 1024:.1f} KiB"
    return f"{n} B"
