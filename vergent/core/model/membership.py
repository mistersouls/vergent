from dataclasses import dataclass
from typing import Literal


MemberShipStatus = Literal["alive", "suspect", "dead"]


@dataclass
class Membership:
    address: str
    status: MemberShipStatus
    epoch: int = 0

    def update_status(self, status: MemberShipStatus) -> None:
        if status != self.status:
            self.status = status
            self.epoch += 1
