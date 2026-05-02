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
"""tourillon CLI entry point — pki and config subcommands."""

from __future__ import annotations

import base64
import logging
import os
import stat
import tempfile
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from tourillon.core.ports.pki import CaRequest, CertRequest, PkiError
from tourillon.core.structure.config import NodeSize
from tourillon.core.structure.contexts import (
    ClusterRef,
    ContextEntry,
    CredentialsConfig,
    EndpointsConfig,
)
from tourillon.infra.contexts import load_contexts, save_contexts
from tourillon.infra.pki.x509 import (
    CryptographyCaAdapter,
    CryptographyCertIssuerAdapter,
)

logger = logging.getLogger(__name__)

app = typer.Typer(name="tourillon", add_completion=False, no_args_is_help=True)
pki_app = typer.Typer(no_args_is_help=True)
config_app = typer.Typer(no_args_is_help=True)

app.add_typer(pki_app, name="pki")
app.add_typer(config_app, name="config")

_console = Console()
_err_console = Console(stderr=True)


@pki_app.command("ca")
def pki_ca(
    out_cert: Annotated[
        Path, typer.Option("--out-cert", help="Output CA certificate path")
    ],
    out_key: Annotated[
        Path, typer.Option("--out-key", help="Output CA private key path (mode 0600)")
    ],
    common_name: Annotated[str, typer.Option("--common-name")] = "Tourillon CA",
    valid_days: Annotated[int, typer.Option("--valid-days")] = 3650,
    key_size: Annotated[int, typer.Option("--key-size")] = 4096,
) -> None:
    """Generate a new self-signed Certificate Authority."""
    adapter = CryptographyCaAdapter()
    request = CaRequest(
        common_name=common_name,
        valid_days=valid_days,
        key_size=key_size,
        out_cert=out_cert,
        out_key=out_key,
    )
    try:
        adapter.generate_ca(request)
    except PkiError as exc:
        _err_console.print(f"✗ {exc}")
        raise typer.Exit(1) from exc

    _console.print(f"✓ CA certificate written to {out_cert}")
    _console.print(f"✓ CA private key written to {out_key}  (mode 0600)")


@config_app.command("generate")
def config_generate(
    ca_cert: Annotated[Path, typer.Option("--ca-cert", help="CA certificate path")],
    ca_key: Annotated[Path, typer.Option("--ca-key", help="CA private key path")],
    node_id: Annotated[str, typer.Option("--node-id", help="Node identifier")],
    size: Annotated[
        str, typer.Option("--size", help="Node size (XS S M L XL XXL)")
    ] = "M",
    kv_bind: Annotated[str, typer.Option("--kv-bind")] = "0.0.0.0:7700",
    peer_bind: Annotated[str, typer.Option("--peer-bind")] = "0.0.0.0:7701",
    out: Annotated[Path, typer.Option("--out")] = Path("config.toml"),
    valid_days: Annotated[int, typer.Option("--valid-days")] = 365,
) -> None:
    """Issue a server certificate and write a fully-populated config.toml."""
    try:
        NodeSize(size)
    except ValueError:
        valid = " ".join(s.value for s in NodeSize)
        _err_console.print(f'✗ Invalid node size "{size}". Valid values: {valid}')
        raise typer.Exit(1) from None

    issuer = CryptographyCertIssuerAdapter()

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        leaf_cert = td_path / "node.crt"
        leaf_key = td_path / "node.key"
        req = CertRequest(
            common_name=node_id,
            san_dns=(node_id,),
            san_ip=(),
            valid_days=valid_days,
            ca_cert=ca_cert,
            ca_key=ca_key,
            out_cert=leaf_cert,
            out_key=leaf_key,
        )
        try:
            issuer.issue_cert(req)
        except PkiError as exc:
            _err_console.print(f"✗ {exc}")
            raise typer.Exit(1) from exc

        cert_pem = leaf_cert.read_bytes()
        key_pem = leaf_key.read_bytes()
        ca_pem = ca_cert.read_bytes()

    cert_b64 = base64.b64encode(cert_pem).decode()
    key_b64 = base64.b64encode(key_pem).decode()
    ca_b64 = base64.b64encode(ca_pem).decode()

    import tomli_w

    config_dict = {
        "schema_version": 1,
        "node": {"id": node_id, "size": size, "data_dir": "./node-data"},
        "tls": {"cert_data": cert_b64, "key_data": key_b64, "ca_data": ca_b64},
        "servers": {
            "kv": {"bind": kv_bind, "advertise": ""},
            "peer": {"bind": peer_bind, "advertise": ""},
        },
        "cluster": {"seeds": [], "rf": 3, "partition_shift": 10},
        "join": {
            "max_retries": -1,
            "attempt_timeout": "10s",
            "deadline": "120s",
            "backoff_base": "2s",
            "backoff_max": "30s",
            "max_concurrent": 4,
        },
        "drain": {
            "max_retries": -1,
            "attempt_timeout": "30s",
            "deadline": "300s",
            "backoff_base": "5s",
            "backoff_max": "60s",
            "max_concurrent": 4,
            "bandwidth_fraction": 1.0,
        },
    }

    try:
        out.write_bytes(tomli_w.dumps(config_dict).encode())
        os.chmod(out, stat.S_IRUSR | stat.S_IWUSR)
    except OSError as exc:
        _err_console.print(f"✗ Cannot write to {out}: {exc.strerror.lower()}")
        raise typer.Exit(1) from exc

    _console.print(f"✓ Certificate issued for {node_id}")
    _console.print(f"✓ Config written to {out} (mode 0600)")


@config_app.command("generate-context")
def config_generate_context(
    name: Annotated[str, typer.Argument(help="Context name")],
    ca_cert: Annotated[Path, typer.Option("--ca-cert", help="CA certificate path")],
    ca_key: Annotated[Path, typer.Option("--ca-key", help="CA private key path")],
    out: Annotated[
        Path, typer.Option("--out", help="Path to contexts.toml to create or update")
    ],
    kv: Annotated[
        str | None, typer.Option("--kv", help="KV endpoint (host:port)")
    ] = None,
    peer: Annotated[
        str | None, typer.Option("--peer", help="Peer endpoint (host:port)")
    ] = None,
    use: Annotated[bool, typer.Option("--use/--no-use")] = False,
    valid_days: Annotated[int, typer.Option("--valid-days")] = 365,
) -> None:
    """Issue a client certificate and write a named context to contexts.toml."""

    if not kv and not peer:
        _err_console.print("✗ At least one of --kv or --peer must be provided.")
        raise typer.Exit(1)

    issuer = CryptographyCertIssuerAdapter()

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        leaf_cert = td_path / "client.crt"
        leaf_key_path = td_path / "client.key"
        req = CertRequest(
            common_name=f"tourctl-{name}",
            san_dns=(),
            san_ip=(),
            valid_days=valid_days,
            ca_cert=ca_cert,
            ca_key=ca_key,
            out_cert=leaf_cert,
            out_key=leaf_key_path,
        )
        try:
            issuer.issue_cert(req)
        except PkiError as exc:
            _err_console.print(f"✗ {exc}")
            raise typer.Exit(1) from exc

        cert_b64 = base64.b64encode(leaf_cert.read_bytes()).decode()
        key_b64 = base64.b64encode(leaf_key_path.read_bytes()).decode()
        ca_b64 = base64.b64encode(ca_cert.read_bytes()).decode()

    contexts_file = load_contexts(out)
    entry = ContextEntry(
        name=name,
        cluster=ClusterRef(name=name, ca_data=ca_b64),
        endpoints=EndpointsConfig(kv=kv, peer=peer),
        credentials=CredentialsConfig(cert_data=cert_b64, key_data=key_b64),
    )
    contexts_file.upsert(entry)

    if use or contexts_file.current_context is None:
        contexts_file.current_context = name

    try:
        save_contexts(out, contexts_file)
    except OSError as exc:
        _err_console.print(f"✗ Cannot write to {out}: {exc.strerror.lower()}")
        raise typer.Exit(1) from exc

    _console.print("✓ Client certificate issued")
    _console.print(f'✓ Context "{name}" written to {out}')
