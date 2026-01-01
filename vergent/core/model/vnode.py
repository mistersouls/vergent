from enum import Enum
from typing import Self

from vergent.core.model.token_ import Token
from vergent.core.space import HashSpace


class SizeClass(Enum):
    XS      = 1 << 0    # 1 vnode
    S       = 1 << 1
    M       = 1 << 2
    L       = 1 << 3
    XL      = 1 << 4
    XXL     = 1 << 5


class VNode(tuple):
    """
    VNode represents a virtual node on the 128-bit hash ring.

    The token defines the vnode's position on the ring and is immutable.
    The node_id identifies the physical node currently responsible for this
    vnode and may change during rebalancing or scaling operations.
    """

    __slots__ = ()

    def __new__(cls, token: int, node_id: str) -> Self:
        return super().__new__(cls, (token, node_id))

    @property
    def token(self) -> int:
        """The immutable 128-bit token of this vnode."""
        return self[0]

    @property
    def node_id(self) -> str:
        """The identifier of the physical node currently owning this vnode."""
        return self[1]

    @classmethod
    def random_vnode(cls, node_id: str) -> VNode:
        token = HashSpace.random_token(node_id)
        return cls(token, node_id)

    @classmethod
    def generate_vnodes(cls, node_id: str, size: SizeClass) -> list[VNode]:
        return [cls.random_vnode(node_id) for _ in range(size.value)]

    def repr_token(self) -> str:
        int_token = str(self.token)
        if len(int_token) >= 12:
            int_token = f"{int_token[:6]}…{int_token[-6:]}"
        hex_token = f"{self.token:032x}"[:12]
        return f"Token(hash={int_token}, hex={hex_token})"

    def __repr__(self) -> str:
        return f"VNode(token={self.repr_token()}, node_id={self.node_id})"
