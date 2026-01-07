from dataclasses import dataclass
from typing import Literal

from vergent.core.model.vnode import SizeClass

MemberShipState = Literal["alive", "suspect", "dead"]


@dataclass
class Membership:
    node_id: str
    address: str
    # state: MemberShipState
    size: SizeClass
    epoch: int = 0

    def __post_init__(self) -> None:
        if isinstance(self.size, str):
            self.size = SizeClass[self.size]

    # def update_status(self, status: MemberShipStatus) -> None:
    #     if status != self.status:
    #         self.status = status
    #         self.epoch += 1

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "address": self.address,
            # "status": self.status,
            "epoch": self.epoch,
            "size": self.size.name
        }
