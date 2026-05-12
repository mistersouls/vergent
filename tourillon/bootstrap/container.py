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
"""NodeContainer — infrastructure adapter facade for node startup.

All concrete adapter instances (serializer, TLS contexts, peer connection
pool) are constructed here and bundled into a single NodeContainer. Switching
an adapter (e.g. replacing the serializer or the TLS implementation) requires
changes only in build_node_container(), not across every call site in the
startup sequence.

The container is created once per _run_phase() invocation and passed down to
_build_peer_dispatcher() and the gossip engine rather than constructing
adapters in multiple places.
"""

from __future__ import annotations

import ssl
from dataclasses import dataclass

from tourillon.core.ports.serializer import SerializerPort
from tourillon.core.structure.config import TourillonConfig
from tourillon.core.transport.pool import PeerClientPool
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter
from tourillon.infra.tls.context import (
    build_client_ssl_context,
    build_server_ssl_context,
)


@dataclass(frozen=True)
class NodeContainer:
    """All infrastructure adapters needed to run a tourillon node.

    serializer:     the wire serialisation adapter (msgpack by default).
    server_ssl_ctx: mTLS context for the inbound peer server socket.
    client_ssl_ctx: mTLS context for outbound peer connections.
    pool:           re-usable outbound connection pool (keyed by node_id).

    To switch adapters cluster-wide, change build_node_container() only.
    """

    serializer: SerializerPort
    server_ssl_ctx: ssl.SSLContext
    client_ssl_ctx: ssl.SSLContext
    pool: PeerClientPool


def build_node_container(cfg: TourillonConfig) -> NodeContainer:
    """Construct and return a fully wired NodeContainer from *cfg*.

    Reads TLS material from cfg.tls (inline base64-encoded PEM) and builds
    both server and client ssl.SSLContext objects. Creates a fresh
    MsgpackSerializerAdapter and a PeerClientPool configured with the client
    context. Raise TlsValidationError if the certificate material is invalid
    (already validated by load_config, so this is a safety net only).
    """
    serializer = MsgpackSerializerAdapter()
    server_ssl_ctx = build_server_ssl_context(
        cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
    )
    client_ssl_ctx = build_client_ssl_context(
        cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
    )
    pool = PeerClientPool(ssl_ctx=client_ssl_ctx)
    return NodeContainer(
        serializer=serializer,
        server_ssl_ctx=server_ssl_ctx,
        client_ssl_ctx=client_ssl_ctx,
        pool=pool,
    )
