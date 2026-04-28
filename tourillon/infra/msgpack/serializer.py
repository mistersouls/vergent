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
"""MessagePack adapter for the SerializerPort driven port.

MsgPackSerializer is the concrete implementation of SerializerPort that uses
the msgpack binary format. MessagePack is chosen over JSON for three reasons
that are specifically relevant to Tourillon:

- Binary transparency: raw bytes values (key payloads, store values) survive
  a round-trip without base64 re-encoding, keeping the wire format compact.
- Performance: msgpack encoding and decoding is significantly faster than JSON
  for the binary-heavy payloads typical of a KV store.
- Compactness: msgpack integers and bytes are encoded in fewer bytes than their
  JSON equivalents, reducing bandwidth between nodes.

The adapter is stateless and therefore safe to share as a singleton across all
connections and handlers.
"""

from typing import Any

import msgpack

from tourillon.core.ports.serializer import SerializerPort


class MsgPackSerializer:
    """MessagePack-based implementation of SerializerPort.

    Encoding uses use_bin_type=True so that Python bytes objects are
    serialised as msgpack bin rather than raw, enabling lossless round-trips
    for arbitrary binary payloads. Decoding uses raw=False so that msgpack str
    fields are returned as Python str rather than bytes, matching the field
    types defined in the KV wire protocol.
    """

    def encode(self, data: Any) -> bytes:  # noqa: ANN401
        """Encode data to MessagePack bytes.

        Raise ValueError if data contains a type that msgpack cannot represent.
        """
        try:
            return msgpack.packb(data, use_bin_type=True)
        except (TypeError, msgpack.PackException) as exc:
            raise ValueError(f"msgpack encode failed: {exc}") from exc

    def decode(self, raw: bytes) -> Any:  # noqa: ANN401
        """Decode MessagePack bytes back to a Python object.

        Raise ValueError if raw is malformed or cannot be decoded.
        """
        try:
            return msgpack.unpackb(raw, raw=False)
        except (msgpack.UnpackException, Exception) as exc:
            raise ValueError(f"msgpack decode failed: {exc}") from exc


def _assert_protocol() -> None:
    _: SerializerPort = MsgPackSerializer()  # type: ignore[assignment]


_assert_protocol()
