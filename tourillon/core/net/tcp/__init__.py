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
"""TCP/TLS transport adapter for the Tourillon core."""

from tourillon.core.net.tcp.connection import Connection
from tourillon.core.net.tcp.server import TcpServer
from tourillon.core.net.tcp.testing import MemoryConnection
from tourillon.core.net.tcp.tls import TlsConfigurationError, build_ssl_context

__all__ = [
    "Connection",
    "MemoryConnection",
    "TcpServer",
    "TlsConfigurationError",
    "build_ssl_context",
]
