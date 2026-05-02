---
mode: agent
description: Add a new Envelope kind handler inside an existing domain handler class, registered via @dispatcher.register.
---

# Add a new Envelope kind handler

Handler kind string: **`${input:kind}`**
(e.g. `kv.put`, `ring.fetch`, `rebalance.commit`)

Domain group: **`${input:domain}`**
(e.g. `kv`, `ring`, `rebalance`, `node` — determines which handler class owns this method)

## Context — read first

Re-read:
- `proposals/proposal-bootstrap-05022026-001.md` §Transport layer, §TCP client,
  §Core invariants (Envelope error paths table).
- The proposal that introduces `${input:kind}` for the full protocol semantics.

## Handler class organisation

Handlers are **grouped by domain**, not one class per kind. Each domain module
exposes one handler class whose methods are decorated with
`@dispatcher.register("<kind>")`. The `Dispatcher` calls the registered method
directly when an envelope with that kind arrives.

```
tourillon/core/handlers/
  kv.py          ← KvHandlers  (kv.put, kv.get, kv.delete, kv.replicate, kv.hint)
  ring.py        ← RingHandlers  (ring.fetch, ring.propose_join)
  rebalance.py   ← RebalanceHandlers  (rebalance.plan.request, rebalance.plan,
                                        rebalance.transfer, rebalance.commit)
  node.py        ← NodeHandlers  (node.inspect, node.joined)
```

Create a new module only if `${input:kind}` introduces a brand-new domain.
Otherwise add the method to the existing class for `${input:domain}`.

## Adding a method to an existing handler class

```python
# tourillon/core/handlers/${input:domain}.py

class ${input:Domain}Handlers:
    """Handle all `${input:domain}.*` envelope kinds on the peer/KV listener.

    Each public method is registered with the Dispatcher via
    @dispatcher.register and is called with (receive, send) when a matching
    envelope arrives on any active connection.
    """

    # ...existing methods...

    @dispatcher.register("${input:kind}")
    async def handle_${input:verb}(
        self,
        receive: ReceiveEnvelope,
        send: SendEnvelope,
    ) -> None:
        """Handle one `${input:kind}` request and send the response.

        Raise nothing — unhandled exceptions close the connection silently.
        Send an `error.*` envelope for application-level rejections so the
        caller can correlate the error to its outstanding request.
        """
        env = await receive()
        # TODO: deserialise payload via self._serializer.decode(env.payload)
        # TODO: enforce phase guard (see below)
        # TODO: apply domain logic
        # TODO: send response: await send(Envelope(kind="...", ...))
        logger.info(
            "handled %s",
            "${input:kind}",
            extra={"correlation_id": str(env.correlation_id)},
        )
```

## Creating a new domain handler class

Only when `${input:domain}` has no existing handler class:

```python
# Copyright 2026 Tourillon Contributors
# ... (full Apache 2.0 header)
"""Handlers for all `${input:domain}.*` envelope kinds."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tourillon.core.ports.transport import ReceiveEnvelope, SendEnvelope
    from tourillon.infra.transport.dispatcher import Dispatcher

logger = logging.getLogger(__name__)


class ${input:Domain}Handlers:
    """Handle all `${input:domain}.*` envelope kinds.

    Inject domain services and ports via __init__; never hard-code addresses
    or access infrastructure directly. Register with a Dispatcher instance by
    calling register(dispatcher) after construction.
    """

    def __init__(self, ...) -> None: ...  # inject ports

    def register(self, dispatcher: Dispatcher) -> None:
        """Register all handler methods with the given Dispatcher."""
        dispatcher.register("${input:kind}", self.handle_${input:verb})
        # Register additional kinds here as they are added.

    async def handle_${input:verb}(
        self,
        receive: ReceiveEnvelope,
        send: SendEnvelope,
    ) -> None:
        """Handle one `${input:kind}` request and send the response."""
        ...
```

## Registration at startup

In the bootstrap / wiring layer (never in `core/`):

```python
handlers = ${input:Domain}Handlers(serializer=..., phase_reader=..., store=...)
handlers.register(dispatcher)
```

## Envelope error conventions

```python
await send(Envelope(
    kind="error.<event>",
    payload=self._serializer.encode({"reason": "..."}),
    correlation_id=env.correlation_id,
))
return
```

Never raise an exception for application-level errors — always respond then return.

## Phase guards

```python
# KV write handlers (kv.put, kv.delete):
if self._phase_reader.current() != MemberPhase.READY:
    await send(Envelope(kind="error.not_ready",
                        payload=self._serializer.encode({}),
                        correlation_id=env.correlation_id))
    return

# KV read / replication handlers (kv.get, kv.replicate):
if self._phase_reader.current() not in (MemberPhase.READY, MemberPhase.DRAINING):
    await send(Envelope(kind="error.not_ready",
                        payload=self._serializer.encode({}),
                        correlation_id=env.correlation_id))
    return
```

## Tests to write

Add to `tests/core/handlers/test_${input:domain}.py` (one test file per domain class):

- Happy path — valid `${input:kind}` request → correct response kind and payload.
- Phase guard — wrong phase → `error.not_ready` with matching `correlation_id`.
- Invalid payload — deserialisation failure → appropriate error envelope.
- Multiple in-flight `${input:kind}` requests with distinct `correlation_id`s
  all receive the correct matching response.
