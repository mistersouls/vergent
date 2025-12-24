from vergent.core.model.membership import Membership


class MembershipView:
    def __init__(self, node: str, initial_peers: set[str]) -> None:
        self.node = node

        self._entries: dict[str, Membership] = {
            peer: Membership(address=peer, status="dead", epoch=0)
            for peer in initial_peers
        }

        # Ensure local node exists in the table
        if node not in self._entries:
            raise ValueError("Local node must be present in initial_peers.")

        # Local node starts as alive
        self._entries[node].update_status("alive")

    def get(self, peer: str) -> Membership | None:
        return self._entries.get(peer)

    def all_peers(self) -> dict[str, Membership]:
        return self._entries

    def alive_peers(self) -> dict[str, Membership]:
        return {
            peer: membership
            for peer, membership in self._entries.items()
            if membership.status == "alive"
        }

    def suspect_peers(self) -> dict[str, Membership]:
        return {
            peer: membership
            for peer, membership in self._entries.items()
            if membership.status == "suspect"
        }

    def dead_peers(self) -> dict[str, Membership]:
        return {
            peer: membership
            for peer, membership in self._entries.items()
            if membership.status == "dead"
        }

    def set_alive(self, peer: str) -> None:
        entry = self._entries[peer]
        entry.update_status("alive")

    def set_suspect(self, peer: str) -> None:
        entry = self._entries[peer]
        entry.update_status("suspect")

    def set_dead(self, peer: str) -> None:
        entry = self._entries[peer]
        entry.update_status("dead")

    def merge(self, other: MembershipView) -> None:
        for peer, other_entry in other.all_peers().items():
            local_entry = self._entries.get(peer)

            if local_entry is None:
                self._entries[peer] = Membership(
                    address=peer,
                    status=other_entry.status,
                    epoch=other_entry.epoch,
                )
                continue

            # Conflict resolution
            if other_entry.epoch > local_entry.epoch:
                local_entry.status = other_entry.status
                local_entry.epoch = other_entry.epoch

    def snapshot(self) -> dict[str, dict[str, object]]:
        """
        Return an immutable snapshot of the membership table.
        Format:
            {
                peer: {
                    "address": str,
                    "status": "alive" | "suspect" | "dead",
                    "epoch": int,
                },
                ...
            }
        """
        return {
            peer: {
                "address": entry.address,
                "status": entry.status,
                "epoch": entry.epoch,
            }
            for peer, entry in self._entries.items()
        }
