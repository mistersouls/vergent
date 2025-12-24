from dataclasses import dataclass
from typing import Iterable, Any

from vergent.core.p2p.hlc import HLC


@dataclass(frozen=True)
class ValueVersion:
    """
    Represents a versioned value stored in the database.

    Fields:
        - value: raw bytes or None (None indicates a tombstone)
        - hlc: Hybrid Logical Clock timestamp
        - is_tombstone: True if this represents a logical delete
        - origin: optional node identifier for debugging/tracing

    This structure is the unit of replication and conflict resolution.
    """
    value: bytes | None
    hlc: HLC
    is_tombstone: bool
    origin: str | None = None

    @staticmethod
    def tombstone(hlc: HLC, origin: str | None = None) -> ValueVersion:
        """Create a tombstone version for a DELETE operation."""
        return ValueVersion(value=None, hlc=hlc, is_tombstone=True, origin=origin)

    @staticmethod
    def from_bytes(
        value: bytes,
        hlc: HLC,
        origin: str | None = None,
    ) -> ValueVersion:
        """Create a versioned value for a PUT operation."""
        return ValueVersion(value=value, hlc=hlc, is_tombstone=False, origin=origin)

    def to_dict(self) -> dict[str, Any]:
        """Serialize this version into a JSON‑compatible dictionary."""
        return {
            "value": self.value,
            "hlc": self.hlc.to_dict(),
            "is_tombstone": self.is_tombstone,
            "origin": self.origin,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ValueVersion:
        """Deserialize a version from a dictionary."""
        return cls(
            value=data["value"],
            hlc=HLC.from_dict(data["hlc"]),
            is_tombstone=bool(data["is_tombstone"]),
            origin=data.get("origin"),
        )


class ConflictResolver:
    """
    Last‑Writer‑Wins (LWW) conflict resolver based on HLC.

    Invariant:
        Given a set of concurrent versions, all nodes must deterministically
        choose the same "winning" version.

    Rule:
        - The version with the largest HLC wins.
        - HLC provides a total order, so ties are impossible unless the
          timestamps are identical down to node_id (extremely rare).
    """

    @staticmethod
    def resolve(versions: Iterable[ValueVersion]) -> ValueVersion | None:
        """
        Resolve a set of concurrent versions into a single version.

        Parameters:
            versions: iterable of ValueVersion objects (from quorum reads,
                      anti‑entropy sync, or gossip repair)

        Returns:
            The LWW winner, or None if the set is empty.
        """
        iterator = iter(versions)
        try:
            best = next(iterator)
        except StopIteration:
            return None

        for candidate in iterator:
            if candidate.hlc > best.hlc:
                best = candidate

        return best
