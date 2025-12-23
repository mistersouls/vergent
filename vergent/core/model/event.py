import struct
from dataclasses import dataclass, asdict
from typing import Mapping, Any

import msgpack


@dataclass
class Event:
    type: str
    payload: Mapping[str, Any]

    def to_dict(self) -> Mapping[str, Any]:
        return asdict(self)

    def to_frame(self) -> bytes:
        payload = msgpack.packb(self.to_dict(), use_bin_type=True)
        frame = struct.pack("!I", len(payload)) + payload
        return frame
