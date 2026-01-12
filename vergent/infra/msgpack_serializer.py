import struct
from typing import Protocol

import msgpack


class Serializable(Protocol):
    def to_dict(self) -> dict:
        ...


class Serializer:
    @staticmethod
    def serialize(s: Serializable) -> bytes:
        payload = msgpack.packb(s.to_dict(), use_bin_type=True)
        frame = struct.pack("!I", len(payload)) + payload
        return frame

    @staticmethod
    def deserialize(s: bytes) -> dict:
        return msgpack.unpackb(s, raw=False)
