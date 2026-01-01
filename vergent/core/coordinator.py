@dataclass
class WriteResult:
    node_id: str
    ok: bool
    error: Exception | None = None


@dataclass
class ReadResult:
    node_id: str
    ok: bool
    value: bytes | None
    version: Version  # timestamp, vector clock, etc.
    error: Exception | None = None


class Coordinator:
    def __init__(self, ring: Ring, transport: Transport):
        self._ring = ring
        self._transport = transport

    async def put(self, key: bytes, value: bytes, N: int, W: int, timeout: float) -> bool:
        replicas = self._ring.preference_list(key, N)  # N replicas
        # Contexte : version, timestamp, vector clock, etc.
        context = self._build_write_context(key, value)

        # On envoie en parallèle à tous les N
        tasks = [
            self._transport.send_write(replica.node_id, key, value, context)
            for replica in replicas
        ]

        # Maintenant, on collecte les résultats jusqu’à W succès ou timeout
        success = 0
        errors: list[WriteResult] = []

        for coro in asyncio.as_completed(tasks, timeout=timeout):
            try:
                result: WriteResult = await coro
            except Exception as exc:
                errors.append(WriteResult(node_id="unknown", ok=False, error=exc))
                continue

            if result.ok:
                success += 1
                if success >= W:
                    # On a atteint le quorum → succès global
                    return True
            else:
                errors.append(result)

        # Si on sort de la boucle sans avoir atteint W → échec
        # Ici tu peux logguer, déclencher hinted handoff, etc.
        return False

    async def get(self, key: bytes, N: int, R: int, timeout: float) -> bytes | None:
        replicas = self._ring.preference_list(key, N)
        context = self._build_read_context(key)

        tasks = [
            self._transport.send_read(replica.node_id, key, context)
            for replica in replicas
        ]

        responses: list[ReadResult] = []
        for coro in asyncio.as_completed(tasks, timeout=timeout):
            try:
                result: ReadResult = await coro
            except Exception as exc:
                responses.append(ReadResult(node_id="unknown", ok=False, value=None, version=None, error=exc))
                continue

            if result.ok:
                responses.append(result)
                if len([r for r in responses if r.ok]) >= R:
                    break

        # Pas assez de réponses valides → échec / timeout
        ok_responses = [r for r in responses if r.ok]
        if len(ok_responses) < R:
            return None  # ou raise ReadQuorumError

        # Choisir la "meilleure" version (LWW, vector clocks, CRDT, etc.)
        chosen = self._resolve_versions(ok_responses)

        # Optionnel : read‑repair en arrière‑plan
        self._maybe_read_repair(key, chosen, replicas, ok_responses)

        return chosen.value
