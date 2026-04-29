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

``deps`` is a module-level singleton store. Each CLI command calls
``configure()`` once at startup to validate certificates and initialise all
shared objects. Subsequent calls to the factory accessors ``get_client()`` and
``get_serializer()`` return the already-constructed instances.

This pattern keeps commands thin: they describe intent, call ``configure``,
then delegate to single-purpose async functions. Swapping the serializer or
transport in tests is done by calling ``configure`` with different arguments.
"""

import ssl
from pathlib import Path

from tourctl.core.client import TcpClient
from tourillon.core.net.tcp.tls import TlsConfigurationError, build_ssl_context
from tourillon.core.ports.serializer import SerializerPort
from tourillon.infra.msgpack.serializer import MsgPackSerializer

_host: str | None = None
_port: int | None = None
_ssl_ctx: ssl.SSLContext | None = None
_serializer: SerializerPort | None = None
_timeout: float = 10.0
# Remember the certificate paths used to build the SSL context so that
# reconfiguration with identical arguments is a no-op. This keeps repeated
# calls to ``configure()`` idempotent in single-threaded CLI usage.
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

    Call this once at the top of every CLI command, before calling
    ``get_client()`` or ``get_serializer()``. If any certificate path is
    missing or the SSL context cannot be built, ``TlsConfigurationError`` is
    raised immediately so the command can surface a clear message before
    entering the event loop.

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

    # If configure() is called multiple times with the same arguments, avoid
    # rebuilding the SSL context and serializer. This is safe for the CLI
    # where configure() is called at process startup, but it keeps behaviour
    # deterministic if callers re-run configure() with identical params.
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


def get_client() -> TcpClient:
    """Return a fresh ``TcpClient`` bound to the configured endpoint.

    A new ``TcpClient`` instance is returned on every call; the client itself
    opens a new TCP connection on each ``request`` call.

    Raises:
        RuntimeError: If ``configure()`` has not been called yet.
    """
    if _host is None or _port is None or _ssl_ctx is None or _serializer is None:
        raise RuntimeError("deps.configure() must be called before get_client()")
    return TcpClient(_host, _port, _ssl_ctx, _serializer, timeout=_timeout)


def get_serializer() -> SerializerPort:
    """Return the shared ``SerializerPort`` instance.

    Raises:
        RuntimeError: If ``configure()`` has not been called yet.
    """
    if _serializer is None:
        raise RuntimeError("deps.configure() must be called before get_serializer()")
    return _serializer
