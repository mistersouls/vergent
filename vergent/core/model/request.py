from dataclasses import dataclass


@dataclass
class PutRequest:
    request_id: str
    key: bytes
    value: bytes
    quorum_write: int
    timeout: float
