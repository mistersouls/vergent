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
"""Tests for tourillon.core.structure.envelope — binary frame encode/decode."""

import uuid

import pytest

from tourillon.core.structure.envelope import PROTO_VERSION, Envelope


def test_envelope_create_uses_proto_version_constant() -> None:
    """create must stamp the current PROTO_VERSION onto the envelope."""
    env = Envelope.create(b"payload")
    assert env.proto_version == PROTO_VERSION


def test_envelope_create_auto_generates_correlation_id() -> None:
    """create must generate a valid UUID when correlation_id is not supplied."""
    env = Envelope.create(b"")
    assert isinstance(env.correlation_id, uuid.UUID)


def test_envelope_create_unique_correlation_ids() -> None:
    """Two calls to create without a correlation_id must yield distinct UUIDs."""
    a = Envelope.create(b"")
    b = Envelope.create(b"")
    assert a.correlation_id != b.correlation_id


def test_envelope_create_uses_provided_correlation_id() -> None:
    """create must use the caller-supplied correlation_id when given."""
    cid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    env = Envelope.create(b"x", correlation_id=cid)
    assert env.correlation_id == cid


def test_envelope_create_sets_schema_id() -> None:
    """create must preserve the schema_id argument."""
    env = Envelope.create(b"", schema_id=42)
    assert env.schema_id == 42


def test_envelope_create_default_schema_id_is_zero() -> None:
    """create must default schema_id to 0 when not supplied."""
    env = Envelope.create(b"")
    assert env.schema_id == 0


def test_envelope_encode_decode_roundtrip_empty_payload() -> None:
    """An envelope with an empty payload must survive an encode→decode cycle."""
    original = Envelope.create(b"")
    recovered = Envelope.decode(original.encode())
    assert recovered == original


def test_envelope_encode_decode_roundtrip_binary_payload() -> None:
    """An envelope with arbitrary binary payload must survive an encode→decode cycle."""
    payload = bytes(range(256))
    original = Envelope.create(payload, schema_id=3)
    recovered = Envelope.decode(original.encode())
    assert recovered.payload == payload
    assert recovered.schema_id == 3


def test_envelope_encode_decode_preserves_correlation_id() -> None:
    """correlation_id must survive encode/decode unchanged."""
    cid = uuid.uuid4()
    original = Envelope.create(b"data", correlation_id=cid)
    recovered = Envelope.decode(original.encode())
    assert recovered.correlation_id == cid


def test_envelope_encode_decode_preserves_proto_version() -> None:
    """proto_version must survive encode/decode unchanged."""
    env = Envelope.create(b"v")
    assert Envelope.decode(env.encode()).proto_version == PROTO_VERSION


def test_envelope_decode_raises_on_empty_bytes() -> None:
    """decode must raise ValueError when given an empty byte string."""
    with pytest.raises(ValueError, match="frame too short"):
        Envelope.decode(b"")


def test_envelope_decode_raises_on_short_frame() -> None:
    """decode must raise ValueError when the frame is shorter than the header."""
    with pytest.raises(ValueError, match="frame too short"):
        Envelope.decode(b"\x00" * 5)


def test_envelope_decode_raises_on_truncated_payload() -> None:
    """decode must raise ValueError when payload_len exceeds available bytes."""
    env = Envelope.create(b"hello world")
    frame = env.encode()
    with pytest.raises(ValueError, match="truncated payload"):
        Envelope.decode(frame[:-3])


def test_envelope_decode_ignores_trailing_bytes() -> None:
    """decode must read exactly payload_len bytes and ignore any trailing data."""
    env = Envelope.create(b"abc")
    frame = env.encode() + b"\xff\xff"
    recovered = Envelope.decode(frame)
    assert recovered.payload == b"abc"
