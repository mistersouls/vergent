from dataclasses import dataclass
from typing import Literal

from vergent.core.model.vnode import SizeClass

MemberShipState = Literal["alive", "suspect", "dead"]


@dataclass
class Membership:
    node_id: str
    peer_address: str
    replication_address: str
    size: SizeClass
    tokens: list[int]
    epoch: int = 0

    def __post_init__(self) -> None:
        if isinstance(self.size, str):
            self.size = SizeClass[self.size]

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "peer_address": self.peer_address,
            "replication_address": self.replication_address,
            "epoch": self.epoch,
            "size": self.size.name,
            "tokens": [f"{t:032x}" for t in self.tokens],
        }

    @classmethod
    def from_dict(cls, data: dict) -> Membership:
        size = data["size"]
        if isinstance(size, str):
            size = SizeClass[size]

        tokens_hex = data["tokens"]
        tokens = [int(h, 16) for h in tokens_hex]

        return cls(
            node_id=data["node_id"],
            peer_address=data["peer_address"],
            replication_address=data["replication_address"],
            epoch=data.get("epoch", 0),
            size=size,
            tokens=tokens,
        )

@dataclass
class MembershipChange:
    before: Membership
    after: Membership


@dataclass(frozen=True)
class MembershipDiff:
    added: list[Membership]
    removed: list[Membership]
    updated: list[MembershipChange]
    bucket_id: str

    @property
    def changed(self) -> bool:
        return bool(self.added or self.removed or self.updated)

    @classmethod
    def from_empty(cls, bucket_id: str) -> MembershipDiff:
        return MembershipDiff(added=[], removed= [], updated=[], bucket_id=bucket_id)
