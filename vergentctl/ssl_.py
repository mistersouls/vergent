import ipaddress

from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from datetime import datetime, timedelta, UTC
import getpass


def gen_root_ca(out_cert: str, out_key: str, days: int = 3650, ask_passphrase: bool = False):
    """
    Generate a self-signed root CA certificate.
    The private key is encrypted with a passphrase if requested.
    """

    passphrase = None
    if ask_passphrase:
        p1 = getpass.getpass("Enter passphrase for CA key: ")
        p2 = getpass.getpass("Confirm passphrase: ")
        if p1 != p2:
            print("Error: passphrases do not match")
            exit(1)
        passphrase = p1

    # Generate private key
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=4096,
    )

    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, "Vergent Root CA"),
    ])

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(tz=UTC))
        .not_valid_after(datetime.now(tz=UTC) + timedelta(days=days))
        .add_extension(
            x509.BasicConstraints(ca=True, path_length=None),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,  # REQUIRED for a CA
                crl_sign=True,  # REQUIRED for a CA
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(private_key=key, algorithm=hashes.SHA256())
    )

    encryption = (
        serialization.BestAvailableEncryption(passphrase.encode())
        if passphrase else serialization.NoEncryption()
    )

    with open(out_key, "wb") as f:
        f.write(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=encryption,
            )
        )

    with open(out_cert, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))

    print(f"Root CA generated:\n  cert: {out_cert}\n  key:  {out_key}")


def gen_node_cert(
    ca: str,
    ca_key: str,
    out_cert: str,
    out_key: str,
    cn: str,
    days: int = 365,
    ca_passphrase: str = None,
    san_dns=None,
    san_ip=None,
):
    san_dns = san_dns or []
    san_ip = san_ip or []

    # Load CA private key
    with open(ca_key, "rb") as f:
        ca_private_key = serialization.load_pem_private_key(
            f.read(),
            password=ca_passphrase.encode() if ca_passphrase else None
        )

    # Load CA certificate
    with open(ca, "rb") as f:
        ca_cert = x509.load_pem_x509_certificate(f.read())

    # Generate node private key
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=4096,
    )

    subject = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, cn),
    ])

    # Build SAN list
    san_list = []

    # CN is also added as SAN automatically
    try:
        san_list.append(x509.IPAddress(ipaddress.ip_address(cn)))
    except ValueError:
        san_list.append(x509.DNSName(cn))

    # Additional SAN entries
    for dns in san_dns:
        san_list.append(x509.DNSName(dns))

    for ip in san_ip:
        san_list.append(x509.IPAddress(ipaddress.ip_address(ip)))

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(tz=UTC))
        .not_valid_after(datetime.now(tz=UTC) + timedelta(days=days))
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None),
            critical=True,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_cert.public_key()),
            critical=False,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectAlternativeName(san_list),
            critical=False,
        )
        .sign(private_key=ca_private_key, algorithm=hashes.SHA256())
    )

    # Write node private key (unencrypted)
    with open(out_key, "wb") as f:
        f.write(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )

    with open(out_cert, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))

    print(f"Node certificate generated:\n  cert: {out_cert}\n  key:  {out_key}")


def inspect_cert(cert_path: str):
    with open(cert_path, "rb") as f:
        cert = x509.load_pem_x509_certificate(f.read())

    print("Certificate Information")
    print("-----------------------")
    print(f"Subject: {cert.subject.rfc4514_string()}")
    print(f"Issuer:  {cert.issuer.rfc4514_string()}")
    print(f"Serial:  {cert.serial_number}")
    print(f"Valid From: {cert.not_valid_before_utc}")
    print(f"Valid To:   {cert.not_valid_after_utc}")

    # Basic Constraints
    bc = cert.extensions.get_extension_for_class(x509.BasicConstraints).value
    print(f"Is CA: {bc.ca}")

    # Subject Alternative Names (SAN)
    try:
        san_ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        san = san_ext.value
        print("Subject Alternative Names:")
        for name in san:
            print(f"  - {name}")
    except x509.ExtensionNotFound:
        print("Subject Alternative Names: (none)")


