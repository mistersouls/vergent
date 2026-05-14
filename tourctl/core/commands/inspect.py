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
"""InspectCommand - sends node.inspect and renders the response."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from tourctl.core.ports.console import ConsolePort
from tourillon.core.ports.transport import RESPONSE_TIMEOUT, ResponseTimeoutError
from tourillon.core.structure.envelope import Envelope
from tourillon.core.transport.client import TcpClient

if TYPE_CHECKING:
    from tourillon.core.ports.serializer import SerializerPort

PARTITION_DISPLAY_THRESHOLD: int = 64


class InspectCommand:
    """Opens a direct mTLS connection to the target address and renders the result.

    Per proposal 003 rev 2 the operator supplies the target node's peer
    address directly; tourctl connects, sends node.inspect with an empty
    payload, and renders the response. No forwarding occurs. Every
    partition range line uses the standard format:

        token 0x<8 hex digits>...  ->  pids  <start>-<end>  (<count> partitions)
    """

    def __init__(
        self,
        client: TcpClient,
        serializer: SerializerPort,
        console: ConsolePort,
        err_console: ConsolePort,
        target_address: str = "",
    ) -> None:
        self._client = client
        self._serializer = serializer
        self._console = console
        self._err_console = err_console
        self._target_address = target_address

    async def run(
        self,
        *,
        show_all_partitions: bool = False,
        json_output: bool = False,
        timeout: float = RESPONSE_TIMEOUT,
    ) -> int:
        """Execute the inspect request and render/print results."""
        self._console.print(f"Connecting to {self._target_address} …")
        req = Envelope.create(
            self._serializer.encode({}),
            kind="node.inspect",
            schema_id=self._serializer.schema_id,
        )

        try:
            resp = await self._client.request(req, timeout=timeout)
        except ResponseTimeoutError:
            self._err_console.print(f"✗ Inspect timed out after {timeout:.0f}s.")
            return 1

        if resp.kind.startswith("error."):
            return self._handle_error(resp.kind)

        try:
            data = self._serializer.decode(resp.payload)
        except Exception:
            self._err_console.print("✗ Failed to decode response payload.")
            return 1

        if json_output:
            self._console.print(json.dumps(data, indent=2))
            return 0

        return self._render(data, show_all_partitions)

    def _handle_error(self, error_kind: str) -> int:
        """Print the appropriate error and return exit code 1."""
        addr = self._target_address or "target"
        if error_kind == "error.node_unreachable":
            self._err_console.print(f"✗ {addr} is unreachable (connection refused).")
        else:
            self._err_console.print(f"✗ Received error response: {error_kind}")
        return 1

    def _render(self, data: dict[str, Any], show_all_partitions: bool) -> int:
        """Render NodeInspectResponse as Rich-formatted output."""
        header_id = data.get("node_id", "") or self._target_address
        self._console.print(f"Inspecting {self._target_address} ({header_id}) ...\n")

        phase = data.get("phase", "").upper()
        size = data.get("size", "")
        token_count = len(data.get("tokens", []))
        self._console.print("  [bold]Identity[/bold]")
        self._console.print(f"    Node ID:      {data.get('node_id', '')}")
        self._console.print(f"    Phase:        {phase}")
        self._console.print(f"    Size:         {size}  ({token_count} vnodes)")
        self._console.print(f"    Generation:   {data.get('generation', 0)}")
        self._console.print(f"    Seq:          {data.get('seq', 0)}")
        self._console.print(f"    Epoch:        {data.get('epoch', 0)}\n")

        self._console.print("  [bold]Addresses[/bold]")
        self._console.print(f"    Peer:   {data.get('peer_address', '')}")
        self._console.print(f"    KV:     {data.get('kv_address', '')}\n")

        self._render_ring_section(data, show_all_partitions)
        self._render_members_section(data)
        self._render_probe_section(data)
        self._render_gossip_stats_section(data)

        if data.get("members_truncated"):
            shown = len(data.get("members", []))
            total = data.get("members_total", shown)
            self._console.print(
                f"\n  [yellow]⚠ Membership list truncated: "
                f"showing {shown} of {total} members.[/yellow]"
            )
        return 0

    def _render_ring_section(
        self,
        data: dict[str, Any],
        show_all_partitions: bool,
    ) -> None:
        """Render the Ring position section."""
        ps = data.get("partition_shift", 0)
        total = data.get("total_partitions", 0)
        owned = data.get("owned_partitions", 0)
        pct = (owned / total * 100) if total else 0.0
        ranges: list[dict[str, Any]] = data.get("partition_ranges", [])
        n_ranges = len(ranges)

        self._console.print(
            f"  [bold]Ring position[/bold]  "
            f"(partition_shift={ps}, {total:,} total partitions)"
        )
        self._console.print(f"    Owned partitions:  {owned}  ({pct:.1f} % of total)")

        if n_ranges > PARTITION_DISPLAY_THRESHOLD and not show_all_partitions:
            self._console.print(
                f"    Vnode ranges:      {n_ranges}  "
                f"(use --partitions to list all ranges)"
            )
        else:
            self._console.print(f"    Vnode ranges:      {n_ranges}")
            for r in ranges:
                token_short = _short_token(r.get("token_hex", ""))
                start = r.get("start_pid", 0)
                end = r.get("end_pid", 0)
                count = r.get("count", 0)
                self._console.print(
                    f"      token {token_short}  →  "
                    f"pids  {start}–{end}  ({count} partitions)"
                )
        self._console.print()

    def _render_members_section(self, data: dict[str, Any]) -> None:
        """Render the Cluster membership section."""
        members: list[dict[str, Any]] = data.get("members", [])
        total = data.get("members_total", len(members))
        node_id = data.get("node_id", "")
        self._console.print(
            f"  [bold]Cluster membership[/bold]  (as seen by {node_id})"
        )
        self._console.print(f"    Members:  {total} total")
        for m in members:
            nid = m.get("node_id", "")
            phase = m.get("phase", "").upper()
            gen = m.get("generation", 0)
            seq = m.get("seq", 0)
            marker = "   ← this node" if nid == node_id else ""
            self._console.print(
                f"      {nid:<12} {phase:<12} gen={gen}  seq={seq}{marker}"
            )
        self._console.print()

    def _render_probe_section(self, data: dict[str, Any]) -> None:
        """Render the Probe state section."""
        probes: list[dict[str, Any]] = data.get("probe_states", [])
        node_id = data.get("node_id", "")
        self._console.print(
            f"  [bold]Probe state[/bold]  ({node_id}'s local failure detector)"
        )
        for p in probes:
            nid = p.get("node_id", "")
            state = p.get("state", "").upper()
            self._console.print(f"    {nid}   {state}")

    def _render_gossip_stats_section(self, data: dict[str, Any]) -> None:
        """Render the Gossip stats section when gossip_stats is present."""
        stats: dict[str, Any] = data.get("gossip_stats", {})
        if not stats:
            return
        self._console.print("\n  [bold]Gossip stats:[/bold]")
        self._console.print(f"    Known members:      {stats.get('known_members', 0)}")
        self._console.print(
            f"    Push sent total:    {stats.get('push_sent_total', 0)}"
        )
        self._console.print(
            f"    Push recv total:    {stats.get('push_recv_total', 0)}"
        )
        ae_total = stats.get("ae_cycles_total", 0)
        ae_div = stats.get("ae_diverged", 0)
        self._console.print(
            f"    AE cycles total:    {ae_total}     diverged: {ae_div}"
        )
        last_peer = stats.get("last_ae_peer") or "—"
        last_at = stats.get("last_ae_at") or "—"
        self._console.print(f"    Last AE peer:       {last_peer}   at {last_at}")
        ok = stats.get("bootstrap_ok_total", 0)
        err = stats.get("bootstrap_err_total", 0)
        self._console.print(f"    Bootstrap seeds ok: {ok}   err: {err}")


def _short_token(token_hex: str) -> str:
    """Return the first 8 hex digits of *token_hex* followed by '…'."""
    if token_hex.startswith("0x") and len(token_hex) > 10:
        return token_hex[:10] + "…"
    return token_hex
