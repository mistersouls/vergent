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
"""Driven port for binary serialization of Envelope payloads.

SerializerPort is the abstraction through which the protocol layer encodes and
decodes the structured data carried inside Envelope payloads. Keeping the
serialization strategy behind a port allows the adapter to be swapped without
touching any handler or dispatcher code.

Implementations must be:

- Deterministic: encoding the same logical value must always produce the same
  byte sequence, so that content-addressable comparisons remain reliable.
- Pure: no side effects, no I/O.
- Safe against malformed input: decode must raise ValueError on invalid data
  rather than allowing arbitrary exceptions to propagate into the dispatch loop.
- Binary-transparent: raw bytes fields (e.g. key and value payloads) must
  survive a round-trip through encode → decode without corruption or
  re-encoding.
"""

from typing import Any, Protocol


class SerializerPort(Protocol):
    """Driven port that converts between Python objects and wire-format bytes.

    The single encode/decode pair is intentionally generic: each handler is
    free to define the structure of the object it passes to encode and
    expects back from decode, as long as the chosen serialization format
    supports the field types used. Implementations must never widen the
    return type of decode beyond the structure that was passed to encode, and
    must raise ValueError on any input that cannot be decoded.
    """

    def encode(
        self, data: Any
    ) -> bytes:  # noqa: ANN401 — payload structure varies per handler
        """Encode a Python object into its wire-format byte representation.

        Raise ValueError if data contains a type that the serialization format
        cannot represent.
        """
        ...

    def decode(
        self, raw: bytes
    ) -> Any:  # noqa: ANN401 — payload structure varies per handler
        """Decode wire-format bytes back into a Python object.

        Raise ValueError if raw is malformed or cannot be decoded.
        """
        ...
