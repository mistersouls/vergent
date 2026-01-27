import json
import os
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, Field, field_validator

from vergent.core.exception import NodeMetaStoreError, NodeMetaCorrupted, NodeMetaVersionMismatch
from vergent.core.model.state import NodePhase, NodeMeta
from vergent.core.model.vnode import SizeClass
from vergent.core.p2p.hlc import HLC


class StoredNodeMeta(BaseModel):
    version: Annotated[
        str,
        Field(description="Format version of the node.meta file.")
    ]

    node_id: Annotated[
        str,
        Field(description="Identifier of the node that owns these vnodes.")
    ]

    size: Annotated[
        SizeClass,
        Field(description="Declared size class (XS/S/M/L/XL/XXL).")
    ]

    tokens: Annotated[
        list[int],
        Field(
            description=(
                "List of vnode tokens owned by this node. Each token is a 128‑bit "
                "integer (hex strings are accepted and converted automatically)."
            )
        )
    ]

    phase: Annotated[
        NodePhase,
        Field(description="Node lifecycle phase.")
    ]

    hlc: Annotated[
        HLC,
        Field(description="Hybrid Logical Clock state.")
    ]

    @field_validator("size", mode="before")
    @classmethod
    def parse_size(cls, v):
        if isinstance(v, str):
            return SizeClass[v]
        return v

    @field_validator("tokens", mode="before")
    @classmethod
    def parse_tokens(cls, v):
        if isinstance(v, list):
            out = []
            for item in v:
                if isinstance(item, int):
                    out.append(item)
                elif isinstance(item, str):
                    out.append(int(item, 16))
                else:
                    raise TypeError(f"Invalid token type: {type(item)}")
            return out
        raise TypeError("tokens must be a list of ints or hex strings")

    def to_meta(self) -> NodeMeta:
        return NodeMeta(
            node_id=self.node_id,
            size=self.size,
            tokens=self.tokens,
            phase=self.phase,
            hlc=self.hlc
        )

    @classmethod
    def from_meta(cls, meta: NodeMeta, version: str) -> StoredNodeMeta:
        return StoredNodeMeta(
            version=version,
            node_id=meta.node_id,
            size=meta.size,
            tokens=sorted(meta.tokens),
            phase=meta.phase,
            hlc=meta.hlc
        )


class SafeNodeMetaStore:
    def __init__(self, path: Path, version: str = "1") -> None:
        self._path = path
        self._version = version

    def get(self) -> NodeMeta:
        meta = self.load(self._path, expected_version=self._version)
        return meta.to_meta()

    def save(self, meta: NodeMeta) -> None:
        stored = StoredNodeMeta.from_meta(meta, self._version)
        self._validate(stored)
        self._atomic_write(stored)

    def set_tokens(self, tokens: list[int]) -> NodeMeta:
        stored_meta = self.load(self._path, expected_version=self._version)
        stored_meta.tokens = sorted(tokens)
        self._validate(stored_meta)
        self._atomic_write(stored_meta)
        return stored_meta.to_meta()

    def set_phase(self, phase: NodePhase) -> NodeMeta:
        stored_meta = self.load(self._path, expected_version=self._version)
        stored_meta.phase = phase
        self._validate(stored_meta)
        self._atomic_write(stored_meta)
        return stored_meta.to_meta()

    def set_hlc(self, hlc: HLC) -> NodeMeta:
        stored_meta = self.load(self._path, expected_version=self._version)
        if hlc < stored_meta.hlc:
            raise NodeMetaStoreError("HLC regression detected")
        stored_meta.hlc = hlc
        self._validate(stored_meta)
        self._atomic_write(stored_meta)
        return stored_meta.to_meta()

    @staticmethod
    def load(path: Path, expected_version: str) -> StoredNodeMeta:
        try:
            with path.open("r") as f:
                data = json.load(f)
        except FileNotFoundError:
            raise NodeMetaCorrupted(f"node.meta file missing: {path}")
        except json.JSONDecodeError:
            raise NodeMetaCorrupted(f"node.meta file corrupted: {path}")

        if "version" not in data:
            raise NodeMetaCorrupted("Missing version field in node.meta")

        if data["version"] != expected_version:
            raise NodeMetaVersionMismatch(
                f"node.meta version {data['version']} != expected {expected_version}"
            )

        try:
            return StoredNodeMeta(**data)
        except Exception as e:
            raise NodeMetaCorrupted(f"Invalid node.meta content: {e}")

    def _atomic_write(self, meta: StoredNodeMeta) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        payload = meta.model_dump_json(indent=2)

        with tmp.open("w") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())

        tmp.replace(self._path)

    @staticmethod
    def _validate(meta: StoredNodeMeta) -> None:
        if meta.tokens != sorted(meta.tokens):
            raise NodeMetaStoreError("Tokens must be sorted")

        # HLC monotonicity is enforced in set_hlc,
        # but we keep the invariant explicit here
        if not isinstance(meta.hlc, HLC):
            raise NodeMetaStoreError("Invalid HLC instance")
