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
"""CLI sub-commands for PKI certificate management.

The pki sub-application exposes three commands that cover the certificate
bootstrap workflow: generating a Certificate Authority, issuing server
certificates with mandatory Subject Alternative Names, and issuing client
certificates. All certificate generation is delegated through
create_pki_adapter() from tourillon.bootstrap.pki; this module is responsible
only for argument parsing, validation, and user-facing output.

Typical operator workflow:

    tourillon pki ca        --common-name "Tourillon CA" --out-dir ./pki
    tourillon pki server    --ca-cert ./pki/ca.crt --ca-key ./pki/ca.key \\
                            --common-name node-1 --san-dns node-1.internal \\
                            --out-dir ./pki/node-1
    tourillon pki client    --ca-cert ./pki/ca.crt --ca-key ./pki/ca.key \\
                            --common-name my-app --out-dir ./pki/clients/app
"""

from pathlib import Path

import typer

from tourillon.bootstrap.pki import create_pki_adapter
from tourillon.core.ports.pki import CaRequest, CertRequest, PkiError
from tourillon.infra.cli.output import (
    print_error,
    print_key_value,
    print_success,
    print_warning,
)

app = typer.Typer(
    name="pki",
    help="Manage TLS certificates for Tourillon nodes and clients.",
    no_args_is_help=True,
)

_CA_CERT_NAME = "ca.crt"
_CA_KEY_NAME = "ca.key"
_SERVER_CERT_NAME = "server.crt"
_SERVER_KEY_NAME = "server.key"
_CLIENT_CERT_NAME = "client.crt"
_CLIENT_KEY_NAME = "client.key"


def _guard_no_overwrite(path: Path, *, force: bool) -> None:
    """Abort with a user-friendly error if path exists and force is False."""
    if path.exists() and not force:
        print_error(
            f"{path} already exists. Pass --force to overwrite.",
            exit_code=1,
        )


@app.command()
def ca(
    common_name: str = typer.Option(
        ..., "--common-name", "-n", help="Common Name (CN) for the CA certificate."
    ),
    out_dir: Path = typer.Option(
        ..., "--out-dir", "-o", help="Directory to write ca.crt and ca.key."
    ),
    days: int = typer.Option(
        3650, "--days", help="Certificate validity in days.", show_default=True
    ),
    key_size: int = typer.Option(
        4096,
        "--key-size",
        help="RSA key size in bits (2048 or 4096).",
        show_default=True,
    ),
    force: bool = typer.Option(
        False, "--force", help="Overwrite existing certificate files."
    ),
) -> None:
    """Generate a self-signed Certificate Authority key pair.

    Writes ca.crt (certificate) and ca.key (private key, mode 0600) into
    OUT_DIR. The CA private key is the trust root for all cluster mTLS
    connections and must be stored offline after this step.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    out_cert = out_dir / _CA_CERT_NAME
    out_key = out_dir / _CA_KEY_NAME
    _guard_no_overwrite(out_key, force=force)

    request = CaRequest(
        common_name=common_name,
        valid_days=days,
        key_size=key_size,
        out_cert=out_cert,
        out_key=out_key,
    )
    try:
        create_pki_adapter().generate_ca(request)
    except PkiError as exc:
        print_error(str(exc))

    print_key_value(
        "CA Generated",
        [
            ("Certificate", str(out_cert)),
            ("Private key", str(out_key)),
            ("Validity", f"{days} days"),
            ("Key size", f"{key_size} bits"),
        ],
    )
    print_warning(
        "Store the CA private key offline. " "Never copy it to any cluster node."
    )


@app.command()
def server(
    ca_cert: Path = typer.Option(
        ..., "--ca-cert", help="Path to the CA certificate file."
    ),
    ca_key: Path = typer.Option(
        ..., "--ca-key", help="Path to the CA private key file."
    ),
    common_name: str = typer.Option(
        ..., "--common-name", "-n", help="Common Name (CN) for the server certificate."
    ),
    san_dns: list[str] = typer.Option(
        [], "--san-dns", help="DNS Subject Alternative Name (repeatable)."
    ),
    san_ip: list[str] = typer.Option(
        [], "--san-ip", help="IP Subject Alternative Name (repeatable)."
    ),
    out_dir: Path = typer.Option(
        ..., "--out-dir", "-o", help="Directory to write server.crt and server.key."
    ),
    days: int = typer.Option(
        365, "--days", help="Certificate validity in days.", show_default=True
    ),
    key_size: int = typer.Option(
        2048,
        "--key-size",
        help="RSA key size in bits (2048 or 4096).",
        show_default=True,
    ),
    force: bool = typer.Option(
        False, "--force", help="Overwrite existing certificate files."
    ),
) -> None:
    """Issue a server certificate signed by an existing CA.

    At least one --san-dns or --san-ip argument is required. Modern TLS
    clients do not use the CN field for hostname verification; a bare CN-only
    server certificate will be rejected by clients.

    Writes server.crt and server.key (mode 0600) into OUT_DIR.
    """
    if not san_dns and not san_ip:
        print_error(
            "At least one --san-dns or --san-ip argument is required for server certificates."
        )

    if not ca_cert.exists():
        print_error(f"CA certificate not found: {ca_cert}")
    if not ca_key.exists():
        print_error(f"CA private key not found: {ca_key}")

    out_dir.mkdir(parents=True, exist_ok=True)
    out_cert = out_dir / _SERVER_CERT_NAME
    out_key = out_dir / _SERVER_KEY_NAME
    _guard_no_overwrite(out_key, force=force)

    request = CertRequest(
        common_name=common_name,
        san_dns=tuple(san_dns),
        san_ip=tuple(san_ip),
        valid_days=days,
        ca_cert=ca_cert,
        ca_key=ca_key,
        out_cert=out_cert,
        out_key=out_key,
        key_size=key_size,
    )
    try:
        create_pki_adapter().issue_cert(request)
    except PkiError as exc:
        print_error(str(exc))

    print_key_value(
        "Server Certificate Issued",
        [
            ("Certificate", str(out_cert)),
            ("Private key", str(out_key)),
            ("CN", common_name),
            ("DNS SANs", ", ".join(san_dns) or "—"),
            ("IP SANs", ", ".join(san_ip) or "—"),
            ("Validity", f"{days} days"),
        ],
    )
    print_success("Server certificate ready.")


@app.command()
def client(
    ca_cert: Path = typer.Option(
        ..., "--ca-cert", help="Path to the CA certificate file."
    ),
    ca_key: Path = typer.Option(
        ..., "--ca-key", help="Path to the CA private key file."
    ),
    common_name: str = typer.Option(
        ..., "--common-name", "-n", help="Common Name (CN) for the client certificate."
    ),
    out_dir: Path = typer.Option(
        ..., "--out-dir", "-o", help="Directory to write client.crt and client.key."
    ),
    days: int = typer.Option(
        365, "--days", help="Certificate validity in days.", show_default=True
    ),
    key_size: int = typer.Option(
        2048,
        "--key-size",
        help="RSA key size in bits (2048 or 4096).",
        show_default=True,
    ),
    force: bool = typer.Option(
        False, "--force", help="Overwrite existing certificate files."
    ),
) -> None:
    """Issue a client certificate signed by an existing CA.

    Writes client.crt and client.key (mode 0600) into OUT_DIR. Client
    certificates do not require Subject Alternative Names.
    """
    if not ca_cert.exists():
        print_error(f"CA certificate not found: {ca_cert}")
    if not ca_key.exists():
        print_error(f"CA private key not found: {ca_key}")

    out_dir.mkdir(parents=True, exist_ok=True)
    out_cert = out_dir / _CLIENT_CERT_NAME
    out_key = out_dir / _CLIENT_KEY_NAME
    _guard_no_overwrite(out_key, force=force)

    request = CertRequest(
        common_name=common_name,
        san_dns=(),
        san_ip=(),
        valid_days=days,
        ca_cert=ca_cert,
        ca_key=ca_key,
        out_cert=out_cert,
        out_key=out_key,
        key_size=key_size,
    )
    try:
        create_pki_adapter().issue_cert(request)
    except PkiError as exc:
        print_error(str(exc))

    print_key_value(
        "Client Certificate Issued",
        [
            ("Certificate", str(out_cert)),
            ("Private key", str(out_key)),
            ("CN", common_name),
            ("Validity", f"{days} days"),
        ],
    )
    print_success("Client certificate ready.")
