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
import tempfile
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


def build_ssl_context_from_data(
    cert_pem: bytes,
    key_pem: bytes,
    ca_pem: bytes,
    *,
    server_side: bool = True,
) -> ssl.SSLContext:
    """Construct an SSLContext from inline PEM bytes rather than file paths.

    This function is the companion to build_ssl_context for use with the
    inline base64 config model, where TLS material is embedded in config.toml
    rather than stored on disk as separate files.

    The CA certificate is loaded in-memory via load_verify_locations(cadata=).
    The cert and key require temporary files because the ssl module's
    load_cert_chain API does not accept in-memory bytes; the temp directory is
    deleted immediately after the chain is loaded.

    Raise TlsConfigurationError on any SSL or I/O failure. See also
    build_ssl_context for the file-path variant and for the rationale behind
    the mTLS settings applied here (TLS 1.3 minimum, CERT_REQUIRED,
    check_hostname tied to server_side).

    Parameters:
        cert_pem: PEM-encoded certificate bytes.
        key_pem: PEM-encoded private key bytes.
        ca_pem: PEM-encoded CA certificate bytes.
        server_side: When True (default) build a server context; when False
            build a client context.

    Returns:
        A fully configured ssl.SSLContext ready for use with asyncio streams.

    Raises:
        TlsConfigurationError: If the certificate material is invalid.
    """
    protocol = ssl.PROTOCOL_TLS_SERVER if server_side else ssl.PROTOCOL_TLS_CLIENT
    ctx = ssl.SSLContext(protocol)
    ctx.minimum_version = MINIMUM_TLS_VERSION
    ctx.check_hostname = not server_side
    ctx.verify_mode = ssl.CERT_REQUIRED

    try:
        ctx.load_verify_locations(cadata=ca_pem.decode())
    except (ssl.SSLError, OSError, UnicodeDecodeError) as exc:
        raise TlsConfigurationError(
            f"failed to load CA data from inline PEM: {exc}"
        ) from exc

    with tempfile.TemporaryDirectory() as tmpdir:
        cert_path = Path(tmpdir) / "cert.pem"
        key_path = Path(tmpdir) / "key.pem"
        cert_path.write_bytes(cert_pem)
        key_path.write_bytes(key_pem)
        if hasattr(os, "chmod"):
            # Restrict temporarily written key/cert files to owner-only on POSIX
            # platforms. Windows does not honor POSIX permission bits so skip.
            os.chmod(cert_path, 0o600)
            os.chmod(key_path, 0o600)
        try:
            ctx.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
        except (ssl.SSLError, OSError) as exc:
            raise TlsConfigurationError(
                f"failed to load cert chain from inline PEM data: {exc}"
            ) from exc

    return ctx
