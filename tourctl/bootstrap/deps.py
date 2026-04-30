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
"""Dependency injection for the tourctl CLI.

``deps`` is a module-level singleton store. Each CLI command calls either
``configure()`` (explicit cert paths) or ``configure_from_context()`` (active
context from contexts.toml) once at startup to validate certificates and
initialise all shared objects. Subsequent calls to the factory accessors
``get_client()`` and ``get_serializer()`` return the already-constructed
instances.

This pattern keeps commands thin: they describe intent, call a configure
function, then delegate to single-purpose async functions. Swapping the
serializer or transport in tests is done by calling configure with different
arguments.
"""

import base64
import ssl
from pathlib import Path

from tourctl.core.client import TcpClient
from tourillon.bootstrap.contexts import ContextEntry, load_contexts
from tourillon.core.config import ConfigError
from tourillon.core.net.tcp.tls import (
    TlsConfigurationError,
    build_ssl_context,
    build_ssl_context_from_data,
)
from tourillon.core.ports.serializer import SerializerPort
from tourillon.infra.msgpack.serializer import MsgPackSerializer

_host: str | None = None
_port: int | None = None
_ssl_ctx: ssl.SSLContext | None = None
_serializer: SerializerPort | None = None
_timeout: float = 10.0
_certfile: Path | None = None
_keyfile: Path | None = None
_cafile: Path | None = None


def configure(
    host: str,
    port: int,
    certfile: Path,
    keyfile: Path,
    cafile: Path,
    *,
    timeout: float = 10.0,
) -> None:
    """Validate certificate paths and initialise all module-level singletons.

    Call this once at the top of every CLI command that supplies explicit
    certificate paths, before calling ``get_client()`` or
    ``get_serializer()``. If any certificate path is missing or the SSL
    context cannot be built, ``TlsConfigurationError`` is raised immediately
    so the command can surface a clear message before entering the event loop.

    For commands that use the active context from contexts.toml, call
    ``configure_from_active_context()`` instead.

    Raises:
        TlsConfigurationError: If a certificate path is missing or the SSL
            library rejects the certificate material.
    """
    global _host, _port, _ssl_ctx, _serializer, _timeout, _certfile, _keyfile, _cafile

    for label, path in [
        ("certfile", certfile),
        ("keyfile", keyfile),
        ("cafile", cafile),
    ]:
        if not path.exists() or not path.is_file():
            raise TlsConfigurationError(f"{label} not found: {path}")

    if (
        _host == host
        and _port == int(port)
        and _timeout == float(timeout)
        and _certfile == certfile
        and _keyfile == keyfile
        and _cafile == cafile
        and _ssl_ctx is not None
        and _serializer is not None
    ):
        return

    _ssl_ctx = build_ssl_context(certfile, keyfile, cafile, server_side=False)
    _host = host
    _port = int(port)
    _serializer = MsgPackSerializer()
    _timeout = float(timeout)
    _certfile = certfile
    _keyfile = keyfile
    _cafile = cafile


def configure_from_active_context(
    *,
    contexts_file: Path | None = None,
    timeout: float = 10.0,
) -> ContextEntry:
    """Load the active context from contexts.toml and initialise singletons.

    Read the contexts file, locate the active context (current-context field),
    decode the inline base64 TLS material, and build an SSLContext. Raise
    ConfigError if the contexts file is missing or malformed, and raise
    TlsConfigurationError if the PEM material is invalid.

    Returns the active ContextEntry so that the caller can access the kv or
    peer endpoint address.

    Raises:
        ConfigError: If no contexts file is found or no active context is set.
        TlsConfigurationError: If the TLS material in the context is invalid.
    """
    global _host, _port, _ssl_ctx, _serializer, _timeout, _certfile, _keyfile, _cafile

    contexts = load_contexts(contexts_file)
    if not contexts.current_context:
        raise ConfigError(
            "No active context set. Run: tourctl config use-context <NAME>"
        )
    entry = contexts.find_context(contexts.current_context)
    if entry is None:
        raise ConfigError(
            f"Active context {contexts.current_context!r} not found in contexts file."
            " Run: tourctl config use-context <NAME>"
        )
    if not entry.endpoints.kv:
        raise ConfigError(
            f"Context {entry.name!r} has no kv endpoint. "
            "Re-create it with tourillon config generate-context --kv-endpoint."
        )

    host, _, port_str = entry.endpoints.kv.rpartition(":")
    if not host:
        host = entry.endpoints.kv
        port = 7000
    else:
        port = int(port_str)

    cert_pem = base64.b64decode(entry.credentials.cert_data)
    key_pem = base64.b64decode(entry.credentials.key_data)
    ca_pem = base64.b64decode(entry.cluster.ca_data)

    ssl_ctx = build_ssl_context_from_data(cert_pem, key_pem, ca_pem, server_side=False)

    _ssl_ctx = ssl_ctx
    _host = host
    _port = port
    _serializer = MsgPackSerializer()
    _timeout = float(timeout)
    _certfile = None
    _keyfile = None
    _cafile = None

    return entry


def get_client() -> TcpClient:
    """Return a fresh ``TcpClient`` bound to the configured endpoint.

    A new ``TcpClient`` instance is returned on every call; the client itself
    opens a new TCP connection on each ``request`` call.

    Raises:
        RuntimeError: If neither ``configure()`` nor
            ``configure_from_active_context()`` has been called yet.
    """
    if _host is None or _port is None or _ssl_ctx is None or _serializer is None:
        raise RuntimeError(
            "deps.configure() or deps.configure_from_active_context() must be"
            " called before get_client()"
        )
    return TcpClient(_host, _port, _ssl_ctx, _serializer, timeout=_timeout)


def get_serializer() -> SerializerPort:
    """Return the shared ``SerializerPort`` instance.

    Raises:
        RuntimeError: If neither ``configure()`` nor
            ``configure_from_active_context()`` has been called yet.
    """
    if _serializer is None:
        raise RuntimeError(
            "deps.configure() or deps.configure_from_active_context() must be"
            " called before get_serializer()"
        )
    return _serializer
