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
"""Node assembly factories for the Tourillon bootstrap layer."""

from pathlib import Path

from tourillon.bootstrap.handlers import KvHandlers
from tourillon.core.dispatch import Dispatcher
from tourillon.core.net.tcp.server import TcpServer
from tourillon.core.net.tcp.tls import build_ssl_context
from tourillon.core.ports.storage import LocalStoragePort
from tourillon.infra.memory.store import MemoryStore
from tourillon.infra.msgpack.serializer import MsgPackSerializer


def create_memory_node(node_id: str) -> LocalStoragePort:
    """Assemble and return an in-memory node bound to the given identifier.

    This factory is the single point of composition for the in-memory adapter
    stack. It constructs a MemoryStore, which internally wires a MemoryLog and
    an HLCClock, and returns it typed as LocalStoragePort so that callers
    depend on the port contract rather than the concrete adapter. Replacing
    this function with one that wires a different adapter — for instance a
    persistent backend — is sufficient to switch the storage strategy without
    touching any caller.
    """
    return MemoryStore(node_id)


async def create_tcp_node(
    node_id: str,
    host: str,
    port: int,
    certfile: str | Path,
    keyfile: str | Path,
    cafile: str | Path,
    *,
    dispatcher: Dispatcher | None = None,
) -> TcpServer:
    """Assemble and return a TcpServer-backed node with mTLS and a Dispatcher.

    Build the SSL context from the given certificate paths, create a Dispatcher
    if none is supplied, and return a ready-to-start TcpServer. The server
    manages Connection framing and backpressure internally; callers interact only
    with the Dispatcher to register KindHandlers. The returned server is not yet
    running; callers must await server.start() and later server.stop(). The
    function is declared async for forward compatibility with future async setup
    steps such as ring registration and peer discovery.

    Parameters:
        node_id: Logical identifier for this node within the ring.
        host: Bind address for the TCP listener.
        port: Bind port for the TCP listener.
        certfile: Path to the PEM certificate file for mTLS.
        keyfile: Path to the PEM private key file for mTLS.
        cafile: Path to the CA certificate bundle used to verify peers.
        dispatcher: Optional pre-configured Dispatcher. When None a fresh
            empty Dispatcher is created.

    Returns:
        A TcpServer instance ready to be started.
    """
    # node_id is reserved for future ring registration and peer discovery.
    _node_id = node_id  # noqa: F841

    ssl_ctx = build_ssl_context(certfile, keyfile, cafile)
    active_dispatcher = dispatcher if dispatcher is not None else Dispatcher()
    store = MemoryStore(node_id)
    KvHandlers(store, MsgPackSerializer()).register(active_dispatcher)
    return TcpServer(host, port, ssl_ctx, active_dispatcher)
