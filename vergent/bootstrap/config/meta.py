from pathlib import Path
from typing import Annotated, List
from pydantic import BaseModel, Field, field_validator, ConfigDict
import json

from vergent.core.model.vnode import SizeClass


class VNodeMeta(BaseModel):
    """
    Persistent local state describing the vnode assignments of this node.
    This file is authoritative for the ring on this node.
    """

    model_config = ConfigDict(
        json_encoders={SizeClass: lambda v: v.name},
    )

    version: Annotated[
        int,
        Field(description="Format version of the vnodes.meta file.")
    ]

    node_id: Annotated[
        str,
        Field(description="Identifier of the node that owns these vnodes.")
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

    size: Annotated[
        SizeClass,
        Field(description="Declared size class (XS/S/M/L/XL/XXL).")
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
        # Accept list[str] or list[int]
        if isinstance(v, list):
            out = []
            for item in v:
                if isinstance(item, int):
                    out.append(item)
                elif isinstance(item, str):
                    # Convert hex string → int
                    out.append(int(item, 16))
                else:
                    raise TypeError(f"Invalid token type: {type(item)}")
            return out

        raise TypeError("tokens must be a list of ints or hex strings")

    @classmethod
    def load(cls, path: Path) -> VNodeMeta:
        with path.open("r") as f:
            data = json.load(f)
        return cls(**data)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            f.write(self.model_dump_json(indent=2))
