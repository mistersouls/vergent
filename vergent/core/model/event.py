from dataclasses import dataclass, asdict
from typing import Mapping, Any


@dataclass
class Event:
    type: str
    payload: Mapping[str, Any]

    def to_dict(self) -> Mapping[str, Any]:
        return asdict(self)

