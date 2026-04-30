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
"""CLI sub-commands for node config file and client context generation.

The config sub-application exposes two commands that cover the end-to-end
config bootstrap workflow:

    tourillon config generate       — issue a server certificate and write a
                                      fully self-contained config.toml.
    tourillon config generate-context — issue a client certificate and write (or
                                      update) a named entry in contexts.toml.

Both commands sign certificates using create_pki_adapter() and embed the
resulting PEM material as inline base64 in the output files. Neither command
writes *file path references; every output file is self-contained.

Typical operator workflow (Milestone 1):

    tourillon pki ca --common-name "Tourillon CA" --out-dir ./pki
    tourillon config generate \\
        --node-id node-1 --ca-cert ./pki/ca.crt --ca-key ./pki/ca.key \\
        --san-dns node-1.internal --kv-bind 0.0.0.0:7000 --out ./node-1.toml
    tourillon config generate-context prod \\
        --ca-cert ./pki/ca.crt --ca-key ./pki/ca.key \\
        --kv-endpoint node-1.internal:7000
"""

import base64
import tempfile
from pathlib import Path

import typer

from tourillon.bootstrap.config import build_node_toml, write_config_file
from tourillon.bootstrap.contexts import (
    ClusterEntry,
    ContextCredentials,
    ContextEndpoints,
    ContextEntry,
    load_contexts,
)
from tourillon.bootstrap.pki import create_pki_adapter
from tourillon.core.ports.pki import CertRequest, PkiError
from tourillon.infra.cli.output import (
    print_error,
    print_key_value,
    print_success,
    print_warning,
)

app = typer.Typer(
    name="config",
    help="Generate node config files and client context entries.",
    no_args_is_help=True,
)

_DEFAULT_DATA_DIR = str(Path.home() / ".local" / "share" / "tourillon")


def _issue_cert_bytes(
    *,
    ca_cert: Path,
    ca_key: Path,
    common_name: str,
    san_dns: list[str],
    san_ip: list[str],
    days: int,
    key_size: int = 2048,
) -> tuple[bytes, bytes]:
    """Issue a certificate and return (cert_pem, key_pem) as raw bytes.

    Use a temporary directory so the PKI adapter can write its output files
    normally; the files are read back and the directory is deleted before
    this function returns.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_cert = Path(tmpdir) / "cert.pem"
        tmp_key = Path(tmpdir) / "key.pem"
        request = CertRequest(
            common_name=common_name,
            san_dns=tuple(san_dns),
            san_ip=tuple(san_ip),
            valid_days=days,
            ca_cert=ca_cert,
            ca_key=ca_key,
            out_cert=tmp_cert,
            out_key=tmp_key,
            key_size=key_size,
        )
        create_pki_adapter().issue_cert(request)
        return tmp_cert.read_bytes(), tmp_key.read_bytes()


@app.command()
def generate(
    node_id: str = typer.Option(..., "--node-id", help="Unique node identifier."),
    ca_cert: Path = typer.Option(..., "--ca-cert", help="Path to the CA certificate."),
    ca_key: Path = typer.Option(..., "--ca-key", help="Path to the CA private key."),
    san_dns: list[str] = typer.Option(
        [], "--san-dns", help="DNS Subject Alternative Name (repeatable)."
    ),
    san_ip: list[str] = typer.Option(
        [], "--san-ip", help="IP Subject Alternative Name (repeatable)."
    ),
    kv_bind: str = typer.Option(
        "0.0.0.0:7000", "--kv-bind", help="KV listener bind address (host:port)."
    ),
    kv_advertise: str = typer.Option(
        "",
        "--kv-advertise",
        help="KV listener advertise address (host:port). Defaults to --kv-bind.",
    ),
    data_dir: str = typer.Option(
        _DEFAULT_DATA_DIR,
        "--data-dir",
        help="Directory for durable log and state files.",
        show_default=True,
    ),
    log_level: str = typer.Option(
        "info", "--log-level", help="Log level: debug, info, warning, error."
    ),
    days: int = typer.Option(
        365, "--days", help="Certificate validity in days.", show_default=True
    ),
    out: Path = typer.Option(..., "--out", help="Output path for config.toml."),
) -> None:
    """Generate a fully self-contained node config.toml with inline TLS material.

    Issue a server certificate signed by the supplied CA, embed the certificate,
    private key, and CA certificate as base64 inline into a complete config.toml,
    and write it at mode 0600. The node can be started immediately with:

        tourillon node start --config <OUT>

    At least one --san-dns or --san-ip flag is required so that TLS clients can
    verify the server certificate hostname.
    """
    if not san_dns and not san_ip:
        print_error(
            "At least one --san-dns or --san-ip argument is required for"
            " the server certificate."
        )

    for label, path in [("--ca-cert", ca_cert), ("--ca-key", ca_key)]:
        if not path.exists():
            print_error(f"{label} not found: {path}")

    cert_pem: bytes = b""
    key_pem: bytes = b""
    try:
        cert_pem, key_pem = _issue_cert_bytes(
            ca_cert=ca_cert,
            ca_key=ca_key,
            common_name=node_id,
            san_dns=san_dns,
            san_ip=san_ip,
            days=days,
        )
    except PkiError as exc:
        print_error(f"Certificate issuance failed: {exc}")

    ca_pem = ca_cert.read_bytes()

    resolved_kv_advertise = kv_advertise or kv_bind

    toml_content = build_node_toml(
        node_id=node_id,
        data_dir=data_dir,
        log_level=log_level,
        cert_data=base64.b64encode(cert_pem).decode(),
        key_data=base64.b64encode(key_pem).decode(),
        ca_data=base64.b64encode(ca_pem).decode(),
        kv_bind=kv_bind,
        kv_advertise=resolved_kv_advertise,
    )

    # If the caller supplied a directory (or a path with no suffix), treat it
    # as the parent directory and append "<node_id>.toml" automatically.
    if out.is_dir() or not out.suffix:
        out = out / f"{node_id}.toml"

    try:
        write_config_file(out, toml_content, mode=0o600)
    except OSError as exc:
        print_error(f"Failed to write config file: {exc}", exit_code=2)

    print_key_value(
        "Config Generated",
        [
            ("Output", str(out)),
            ("Node ID", node_id),
            ("KV bind", kv_bind),
            ("KV advertise", resolved_kv_advertise),
            ("Cert validity", f"{days} days"),
        ],
    )
    print_success(f"Start the node with: tourillon node start --config {out}")


@app.command(name="generate-context")
def generate_context(
    context_name: str = typer.Argument(help="Name for the new context entry."),
    ca_cert: Path = typer.Option(..., "--ca-cert", help="Path to the CA certificate."),
    ca_key: Path = typer.Option(..., "--ca-key", help="Path to the CA private key."),
    kv_endpoint: str = typer.Option(
        ..., "--kv-endpoint", help="KV endpoint (host:port) for this context."
    ),
    common_name: str = typer.Option(
        "",
        "--common-name",
        help="Certificate CN. Defaults to context name.",
    ),
    days: int = typer.Option(
        365, "--days", help="Certificate validity in days.", show_default=True
    ),
    contexts_file: Path = typer.Option(
        Path.home() / ".config" / "tourillon" / "contexts.toml",
        "--contexts-file",
        help="Path to the contexts file. Defaults to ~/.config/tourillon/contexts.toml.",
        show_default=False,
        hidden=True,
    ),
) -> None:
    """Issue a client certificate and write a named context to contexts.toml.

    Generate a client certificate signed by the supplied CA, embed the
    certificate, private key, and CA certificate as base64 inline, and write
    (or update) a named context entry in contexts.toml at mode 0600.

    After this command, activate the context with:

        tourctl config use-context <CONTEXT_NAME>

    Then use tourctl kv commands without explicit certificate flags.
    """
    for label, path in [("--ca-cert", ca_cert), ("--ca-key", ca_key)]:
        if not path.exists():
            print_error(f"{label} not found: {path}")

    cn = common_name or context_name

    cert_pem2: bytes = b""
    key_pem2: bytes = b""
    try:
        cert_pem2, key_pem2 = _issue_cert_bytes(
            ca_cert=ca_cert,
            ca_key=ca_key,
            common_name=cn,
            san_dns=[],
            san_ip=[],
            days=days,
        )
    except PkiError as exc:
        print_error(f"Client certificate issuance failed: {exc}")

    ca_pem2 = ca_cert.read_bytes()

    is_first: bool = False
    entry = ContextEntry(
        name=context_name,
        cluster=ClusterEntry(
            name=context_name,
            ca_data=base64.b64encode(ca_pem2).decode(),
        ),
        endpoints=ContextEndpoints(kv=kv_endpoint),
        credentials=ContextCredentials(
            cert_data=base64.b64encode(cert_pem2).decode(),
            key_data=base64.b64encode(key_pem2).decode(),
        ),
    )

    try:
        contexts = load_contexts(contexts_file)
        is_first = not contexts.contexts
        contexts.upsert_context(entry)
        if is_first:
            contexts.current_context = context_name
        contexts.save(contexts_file)
    except OSError as exc:
        print_error(f"Failed to write contexts file: {exc}", exit_code=2)

    print_key_value(
        "Context Written",
        [
            ("Context", context_name),
            ("CN", cn),
            ("KV endpoint", kv_endpoint),
            ("Cert validity", f"{days} days"),
            ("Contexts file", str(contexts_file)),
        ],
    )
    if is_first:
        print_success(f"Context {context_name!r} created and set as active context.")
    else:
        print_success(
            f"Context {context_name!r} written. "
            f"Run: tourctl config use-context {context_name}"
        )
    print_warning("contexts.toml contains private key material — keep it at mode 0600.")
