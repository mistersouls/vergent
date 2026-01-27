from typing import Protocol

from vergent.core.model.state import NodeMeta, NodePhase
from vergent.core.p2p.hlc import HLC


class NodeMetaStore(Protocol):
    def get(self) -> NodeMeta:
        ...

    def save(self, state: NodeMeta) -> None:
        ...

    def set_tokens(self, tokens: list[int]) -> NodeMeta:
        ...

    def set_phase(self, phase: NodePhase) -> NodeMeta:
        ...

    def set_hlc(self, hlc: HLC) -> NodeMeta:
        ...
