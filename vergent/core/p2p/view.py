from dataclasses import dataclass
from typing import Any

from vergent.core.model.membership import Membership
from vergent.core.model.vnode import SizeClass


# class MembershipView:
#     def __init__(self, node_id, initial_peers: dict[str, Membership]) -> None:
#         self._node_id = node_id
#
#         self._entries: dict[str, Membership] = initial_peers.copy()
#
#         # Ensure local node exists in the table
#         if node_id not in self._entries:
#             raise ValueError("Local node must be present in initial_peers.")
#
#         self._latest_epoch = max(e.epoch for e in self._entries.values())
#
#     @property
#     def latest_epoch(self) -> int:
#         return self._latest_epoch
#
#     @classmethod
#     def from_snapshot(cls, owner: Membership, snapshot: dict[str, dict[str, Any]]) -> MembershipView:
#         entries: dict[str, Membership] = {owner.node_id: owner}
#
#         for peer_id, data in snapshot.items():
#             if not isinstance(data, dict):
#                 raise ValueError(f"Invalid entry for peer {peer_id}: expected dict.")
#
#             try:
#                 address = data["address"]
#                 size = data["size"]
#                 epoch = data["epoch"]
#             except KeyError as ex:
#                 raise ValueError(f"Missing field {ex} in snapshot entry for {peer_id}")
#
#             entries[peer_id] = Membership(
#                 node_id=peer_id,
#                 address=address,
#                 epoch=epoch,
#                 size=SizeClass[size],
#             )
#
#         return cls(node_id=owner.node_id, initial_peers=entries)
#
#     def get(self, peer: str) -> Membership | None:
#         return self._entries.get(peer)
#
#     def all_peers(self) -> dict[str, Membership]:
#         return self._entries
#
#     def alive_peers(self) -> dict[str, Membership]:
#         return {
#             peer: membership
#             for peer, membership in self._entries.items()
#             if membership.status == "alive"
#         }
#
#     def suspect_peers(self) -> dict[str, Membership]:
#         return {
#             peer: membership
#             for peer, membership in self._entries.items()
#             if membership.status == "suspect"
#         }
#
#     def dead_peers(self) -> dict[str, Membership]:
#         return {
#             peer: membership
#             for peer, membership in self._entries.items()
#             if membership.status == "dead"
#         }
#
#     def set_alive(self, peer: str) -> None:
#         entry = self._entries[peer]
#         entry.update_status("alive")
#
#     def set_suspect(self, peer: str) -> None:
#         entry = self._entries[peer]
#         entry.update_status("suspect")
#
#     def set_dead(self, peer: str) -> None:
#         entry = self._entries[peer]
#         entry.update_status("dead")
#
#     def merge(self, other: MembershipView) -> None:
#         for peer, other_entry in other.all_peers().items():
#             local_entry = self._entries.get(peer)
#
#             if local_entry is None:
#                 self._entries[peer] = Membership(
#                     node_id=peer,
#                     address=other_entry.address,
#                     size=other_entry.size,
#                     epoch=other_entry.epoch,
#                 )
#                 continue
#
#             # Conflict resolution
#             if other_entry.epoch > local_entry.epoch:
#                 local_entry.status = other_entry.status
#                 local_entry.epoch = other_entry.epoch
#
#     def snapshot(self) -> dict[str, dict[str, object]]:
#         """
#         Return an immutable snapshot of the membership table.
#         Format:
#             {
#                 peer: {
#                     "address": str,
#                     "status": "alive" | "suspect" | "dead",
#                     "epoch": int,
#                 },
#                 ...
#             }
#         """
#         return {
#             peer: entry.to_dict()
#             for peer, entry in self._entries.items()
#         }


@dataclass
class View:
    epoch: int
    buckets: dict[int, int]
    address: str
    peer: str
