from dataclasses import dataclass


@dataclass
class PutRequest:
    request_id: str
    key: bytes
    value: bytes
    quorum_write: int
    timeout: float


@dataclass
class GetRequest:
    request_id: str
    key: bytes
    quorum_read: int
    timeout: float
