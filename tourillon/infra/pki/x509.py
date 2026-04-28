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
"""X.509 PKI adapter that generates certificates using the cryptography library."""

import ipaddress
import os
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from tourillon.core.ports.pki import (
    CaRequest,
    CertRequest,
    PkiError,
)

_VALID_KEY_SIZES: frozenset[int] = frozenset({2048, 4096})


class X509CertificateAuthority:
    """PKI adapter that generates and signs X.509 certificates.

    This class implements both CertificateAuthorityPort and CertificateIssuerPort.
    All operations are synchronous and CPU-bound. The CLI layer is responsible
    for offloading them to a thread via asyncio.to_thread when necessary, though
    in practice the RSA key generation time is acceptable for an interactive CLI.

    Private key files are always written with mode 0600. On Windows, os.chmod
    does not enforce Unix-style ACLs; operators must secure the files manually
    or rely on filesystem-level access controls.
    """

    def _write_atomic(self, path: Path, data: bytes, mode: int = 0o600) -> None:
        """Write data to path atomically by staging through a temporary file.

        Stage the data to a sibling temporary file, set permissions, then
        rename into place. Raise PkiError if the write, chmod, or rename fails.
        Any temporary file created before a failure is removed.
        """
        dirpath = path.parent
        tmp: Path | None = None
        try:
            dirpath.mkdir(parents=True, exist_ok=True)
            fd, tmp_name = tempfile.mkstemp(dir=str(dirpath))
            tmp = Path(tmp_name)
            try:
                os.write(fd, data)
            finally:
                os.close(fd)
            if sys.platform != "win32":
                os.chmod(tmp, mode)
            tmp.replace(path)
            tmp = None
        except PkiError:
            raise
        except Exception as exc:
            raise PkiError(f"Failed to write {path}: {exc}") from exc
        finally:
            if tmp is not None and tmp.exists():
                tmp.unlink(missing_ok=True)

    def _generate_rsa_key(self, key_size: int) -> RSAPrivateKey:
        """Generate an RSA private key with the given bit size."""
        if key_size not in _VALID_KEY_SIZES:
            raise PkiError(f"key_size must be one of {sorted(_VALID_KEY_SIZES)}")
        return rsa.generate_private_key(public_exponent=65537, key_size=key_size)

    def _key_to_pem(self, key: RSAPrivateKey) -> bytes:
        """Serialise an RSA private key to unencrypted PKCS#1 PEM bytes."""
        return key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def generate_ca(self, request: CaRequest) -> None:
        """Generate a self-signed CA certificate and private key to disk.

        Write request.out_cert (PEM certificate) and request.out_key (PEM
        private key, mode 0600). Raise PkiError on any failure.
        """
        try:
            private_key = self._generate_rsa_key(request.key_size)
            subject = issuer = x509.Name(
                [x509.NameAttribute(x509.NameOID.COMMON_NAME, request.common_name)]
            )
            now = datetime.now(UTC)
            cert = (
                x509.CertificateBuilder()
                .subject_name(subject)
                .issuer_name(issuer)
                .public_key(private_key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(now)
                .not_valid_after(now + timedelta(days=request.valid_days))
                .add_extension(
                    x509.BasicConstraints(ca=True, path_length=None), critical=True
                )
                .add_extension(
                    x509.KeyUsage(
                        digital_signature=False,
                        content_commitment=False,
                        key_encipherment=False,
                        data_encipherment=False,
                        key_agreement=False,
                        key_cert_sign=True,
                        crl_sign=True,
                        encipher_only=False,
                        decipher_only=False,
                    ),
                    critical=True,
                )
                .add_extension(
                    x509.SubjectKeyIdentifier.from_public_key(private_key.public_key()),
                    critical=False,
                )
                .add_extension(
                    x509.AuthorityKeyIdentifier.from_issuer_public_key(
                        private_key.public_key()
                    ),
                    critical=False,
                )
                .sign(private_key=private_key, algorithm=hashes.SHA256())
            )
            self._write_atomic(
                request.out_cert, cert.public_bytes(serialization.Encoding.PEM)
            )
            self._write_atomic(request.out_key, self._key_to_pem(private_key))
        except PkiError:
            raise
        except Exception as exc:
            raise PkiError(f"CA generation failed: {exc}") from exc

    def issue_cert(self, request: CertRequest) -> None:
        """Issue a leaf certificate signed by the CA described in the request.

        Read the CA certificate and private key ephemerally, generate a new
        RSA key pair for the leaf, sign the certificate, then write out_cert
        and out_key to disk. Raise PkiError on any failure.
        """
        try:
            ca_cert = x509.load_pem_x509_certificate(request.ca_cert.read_bytes())
            ca_private_key = serialization.load_pem_private_key(
                request.ca_key.read_bytes(), password=None
            )
            private_key = self._generate_rsa_key(request.key_size)
            subject = x509.Name(
                [x509.NameAttribute(x509.NameOID.COMMON_NAME, request.common_name)]
            )
            now = datetime.now(UTC)
            san_entries: list[x509.GeneralName] = [
                x509.DNSName(dns) for dns in request.san_dns
            ] + [x509.IPAddress(ipaddress.ip_address(ip)) for ip in request.san_ip]
            builder = (
                x509.CertificateBuilder()
                .subject_name(subject)
                .issuer_name(ca_cert.subject)
                .public_key(private_key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(now)
                .not_valid_after(now + timedelta(days=request.valid_days))
                .add_extension(
                    x509.SubjectKeyIdentifier.from_public_key(private_key.public_key()),
                    critical=False,
                )
                .add_extension(
                    x509.AuthorityKeyIdentifier.from_issuer_public_key(
                        ca_cert.public_key()
                    ),
                    critical=False,
                )
            )
            if san_entries:
                builder = builder.add_extension(
                    x509.SubjectAlternativeName(san_entries), critical=False
                )
            cert = builder.sign(private_key=ca_private_key, algorithm=hashes.SHA256())
            self._write_atomic(
                request.out_cert, cert.public_bytes(serialization.Encoding.PEM)
            )
            self._write_atomic(request.out_key, self._key_to_pem(private_key))
        except PkiError:
            raise
        except Exception as exc:
            raise PkiError(f"Certificate issuance failed: {exc}") from exc
