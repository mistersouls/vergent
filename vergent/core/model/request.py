from dataclasses import dataclass

@dataclass
class WriteRequest:
    request_id: str
    key: bytes
    quorum_write: int
    timeout: float


@dataclass
class PutRequest(WriteRequest):
    value: bytes


@dataclass
class DeleteRequest(WriteRequest):
    pass


@dataclass
class GetRequest:
    request_id: str
    key: bytes
    quorum_read: int
    timeout: float
