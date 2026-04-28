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
"""mTLS context factory — enforces mutual TLS with TLS 1.3 minimum."""

import os
import ssl
from pathlib import Path

MINIMUM_TLS_VERSION: ssl.TLSVersion = ssl.TLSVersion.TLSv1_3


class TlsConfigurationError(ValueError):
    """Raised when TLS configuration is invalid or certificate material cannot be loaded.

    This error surfaces at context-construction time so that misconfigured nodes
    fail fast on startup rather than at the moment they attempt to accept or
    establish a connection.
    """


def build_ssl_context(
    certfile: str | os.PathLike[str],
    keyfile: str | os.PathLike[str],
    cafile: str | os.PathLike[str],
    *,
    server_side: bool = True,
) -> ssl.SSLContext:
    """Construct an SSLContext that enforces mutual TLS with a TLS 1.3 minimum.

    Both sides of every Tourillon connection must present a certificate signed
    by the shared CA. There is no plaintext fallback and no optional-certificate
    mode: verify_mode is always ssl.CERT_REQUIRED.

    For server-side contexts check_hostname is disabled because the server
    authenticates clients by CA chain rather than by hostname. For client-side
    contexts check_hostname is enabled so that the server certificate SAN is
    verified against the address the client is connecting to.

    Raise TlsConfigurationError if any path does not exist or if the SSL
    library rejects the certificate material. This converts low-level OSError
    and ssl.SSLError into a single, consistently named exception that callers
    can catch without depending on the ssl module.

    Parameters:
        certfile: PEM file containing the certificate for this endpoint.
        keyfile: PEM file containing the private key matching certfile.
        cafile: PEM file containing the CA certificate(s) used to verify peers.
        server_side: When True (default) build a server context; when False
            build a client context.

    Returns:
        A fully configured ssl.SSLContext ready for use with asyncio streams.

    Raises:
        TlsConfigurationError: If a path is missing or certificate loading fails.
    """
    cert_path = Path(certfile)
    key_path = Path(keyfile)
    ca_path = Path(cafile)

    if not cert_path.exists():
        raise TlsConfigurationError(f"certfile not found: {cert_path}")
    if not key_path.exists():
        raise TlsConfigurationError(f"keyfile not found: {key_path}")
    if not ca_path.exists():
        raise TlsConfigurationError(f"cafile not found: {ca_path}")

    protocol = ssl.PROTOCOL_TLS_SERVER if server_side else ssl.PROTOCOL_TLS_CLIENT
    ctx = ssl.SSLContext(protocol)
    ctx.minimum_version = MINIMUM_TLS_VERSION
    ctx.check_hostname = not server_side
    ctx.verify_mode = ssl.CERT_REQUIRED

    try:
        ctx.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
    except (ssl.SSLError, OSError) as exc:
        raise TlsConfigurationError(
            f"failed to load cert chain ({cert_path}, {key_path}): {exc}"
        ) from exc

    try:
        ctx.load_verify_locations(cafile=str(ca_path))
    except (ssl.SSLError, OSError) as exc:
        raise TlsConfigurationError(
            f"failed to load CA file ({ca_path}): {exc}"
        ) from exc

    return ctx
