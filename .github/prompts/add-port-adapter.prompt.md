---
mode: agent
description: Add a hexagonal port (Protocol) in core/ports/ and its infra adapter in infra/.
---

# Add a port and its infra adapter

Port name: **`${input:portName}`**
(e.g. `StoragePort`, `CertificateAuthorityPort`, `GossipPort`)

## What is a port?

A port is a `typing.Protocol` defined in `tourillon/core/ports/` that the
domain layer depends on for any I/O or infrastructure capability. The infra
layer provides one or more concrete adapters. Tests inject in-memory fakes.

## Step 1 — Core port file

`tourillon/core/ports/${input:portModule}.py`

```python
# Copyright 2026 Tourillon Contributors
# ... (full Apache 2.0 header)
"""${input:portName} — port definition for <capability>."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ${input:portName}(Protocol):
    """<Describe: what capability this port abstracts, which proposals use it,
    and what ordering/thread-safety guarantees implementors must provide.>

    Implementors must:
    - <list key constraints, e.g. "be safe to call from any coroutine">
    - <list error conditions, e.g. "raise StorageError on I/O failure">
    """

    # TODO: define methods; all I/O methods must be async
    async def example_method(self, ...) -> ...:
        """<Imperative summary ending with a period.>

        Raise <PortError subclass> when <condition>.
        """
        ...
```

Rules:
- All I/O methods are `async def`.
- Pure query methods (no I/O) may be `def`.
- No implementation code; no `import` of third-party libraries.
- The `@runtime_checkable` decorator allows `isinstance()` checks in tests.

## Step 2 — Port error type (if new)

If this port introduces a new error type, add it to the same file:

```python
class ${input:portName}Error(Exception):
    """Raised by ${input:portName} implementations for any failure.

    Wraps underlying infrastructure errors so callers never need to import
    third-party exception hierarchies.
    """
```

## Step 3 — Infra adapter file

`tourillon/infra/${input:adapterModule}/${input:adapterClass}.py`

```python
# Copyright 2026 Tourillon Contributors
# ... (full Apache 2.0 header)
"""Concrete adapter implementing ${input:portName}."""

from __future__ import annotations

import logging

# Import third-party libraries here — NOT in core/
from tourillon.core.ports.${input:portModule} import ${input:portName}, ${input:portName}Error

logger = logging.getLogger(__name__)


class ${input:adapterClass}:
    """${input:portName} implementation using <library/technology>.

    <Describe the underlying technology and any configuration it requires.>
    """

    def __init__(self, ...) -> None:
        """Initialise the adapter.

        <Describe parameters.>
        """
        ...

    async def example_method(self, ...) -> ...:
        """<Mirror the port docstring.>"""
        try:
            ...
        except SomeLibraryError as exc:
            raise ${input:portName}Error(str(exc)) from exc
```

## Step 4 — In-memory fake for tests

`tests/fakes/${input:portModule}_fake.py`

```python
# Copyright 2026 Tourillon Contributors
# ... (full Apache 2.0 header)
"""In-memory fake implementing ${input:portName} for use in unit tests."""

from __future__ import annotations

from tourillon.core.ports.${input:portModule} import ${input:portName}


class Fake${input:portName}:
    """Thread-safe, in-memory ${input:portName} for unit tests.

    <Describe any controllable failure modes (e.g. raise_on_next_call).>
    """

    def __init__(self) -> None:
        self.calls: list[...] = []
        self.should_fail: bool = False

    async def example_method(self, ...) -> ...:
        self.calls.append(...)
        if self.should_fail:
            from tourillon.core.ports.${input:portModule} import ${input:portName}Error
            raise ${input:portName}Error("injected failure")
        return ...
```

## Step 5 — Verify the Protocol conformance in a test

```python
def test_adapter_satisfies_port() -> None:
    from tourillon.core.ports.${input:portModule} import ${input:portName}
    from tourillon.infra.${input:adapterModule}.${input:adapterClass} import ${input:adapterClass}
    assert isinstance(${input:adapterClass}(...), ${input:portName})
```
