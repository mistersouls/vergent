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
"""Bootstrap factory for the PKI adapter.

This module provides a single wiring point for the PKI adapter used by the
CLI and other infrastructure. Callers should depend on the port interfaces
in tourillon.core.ports.pki; this factory returns a concrete implementation.

Swapping to a different PKI provider (for example Vault, CFSSL, or a cloud
KMS-backed implementation) requires changing this function only.
"""

from typing import Protocol

from tourillon.core.ports.pki import CertificateAuthorityPort, CertificateIssuerPort
from tourillon.infra.pki.x509 import X509CertificateAuthority


class PkiAdapter(CertificateAuthorityPort, CertificateIssuerPort, Protocol):
    """Composite protocol for objects that implement CA and issuer ports."""


def create_pki_adapter() -> PkiAdapter:
    """Return the concrete PKI adapter used by the application.

    This function is the single wiring point for the PKI adapter. It returns
    an object that implements the CertificateAuthorityPort and
    CertificateIssuerPort protocols. To switch PKI backends, modify this
    factory to return a different implementation.
    """

    return X509CertificateAuthority()
