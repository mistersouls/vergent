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
"""Tests for tourillon.infra.msgpack.serializer — MsgPackSerializer."""

import pytest

from tourillon.infra.msgpack.serializer import MsgPackSerializer


def test_encode_returns_bytes_for_dict() -> None:
    s = MsgPackSerializer()
    result = s.encode({"key": b"value"})
    assert isinstance(result, bytes)


def test_decode_roundtrip_preserves_data() -> None:
    s = MsgPackSerializer()
    original = {"a": 1, "b": b"bytes"}
    assert s.decode(s.encode(original)) == original


def test_encode_raises_value_error_for_unencodable_type() -> None:
    s = MsgPackSerializer()
    with pytest.raises(ValueError, match="msgpack encode failed"):
        s.encode({"key": lambda: None})  # lambda is not msgpack-serialisable


def test_decode_raises_value_error_for_malformed_bytes() -> None:
    s = MsgPackSerializer()
    with pytest.raises(ValueError, match="msgpack decode failed"):
        s.decode(b"\xff\xff\xff\xff\xff")  # not valid msgpack
