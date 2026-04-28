# Protocol

## Transport and Session Model

- All communication MUST run over mutually authenticated TLS channels.
- Node-to-node and client-to-node flows MUST use the same framing rules.
- Connection reuse SHOULD be enabled to reduce handshake overhead.

The transport is implemented over asyncio streams (`asyncio.start_server` /
`asyncio.open_connection`) wrapped with an SSL context. There is no plaintext
path. Every accepted connection is served by a `Connection` object that owns the
`StreamReader` and `StreamWriter` pair for that connection and handles all
Envelope framing on behalf of higher layers.

Security policy details are defined in `docs/security.md`.

## Message Envelope (binary-safe, self-describing)

Each message MUST be wrapped in an `Envelope`. The header begins with a fixed
23-byte block followed by a variable-length `kind` field:

```
offset      bytes   field
0           1       proto_version  (uint8)
1           2       schema_id      (uint16, big-endian)
3           16      correlation_id (UUID as 16 raw bytes)
19          4       payload_len    (uint32, big-endian)
23          1       kind_len       (uint8, 1 … KIND_MAX_LEN)
24          N       kind           (UTF-8 string, N = kind_len)
24 + N      M       payload        (M = payload_len)
```

The `correlation_id` allows any sender to match a response to the originating
request without shared state. The `schema_id` gives callers an independent
versioning handle for their payload serialisation format. The `proto_version`
lets receivers reject frames from incompatible protocol generations before
attempting to parse the payload.

The `kind` field is an open UTF-8 string that identifies the operation or message
category. Its UTF-8 byte length must be between 1 and `KIND_MAX_LEN` (64) bytes
inclusive. Implementations raise `ValueError` at both construction and decode time
when either bound is violated. There is no central registry of kind values: new
kinds may be introduced freely without coordination, and the `Dispatcher` routes
frames by matching the `kind` string against registered handlers.

Well-known kind values used by the built-in handlers:

| kind                 | Direction         | Description |
|----------------------|-------------------|-------------|
| `put`                | client → node     | Write or replace a value for a `StoreKey`. |
| `get`                | client → node     | Read the current value for a `StoreKey`. |
| `delete`             | client → node     | Tombstone a `StoreKey`. |
| `put.ack`            | node → client     | Acknowledgement for a `put`. |
| `get.response`       | node → client     | Response carrying the value(s) for a `get`. |
| `delete.ack`         | node → client     | Acknowledgement for a `delete`. |
| `replicate`          | node → node       | Inter-node update propagation. |
| `handoff.push`       | node → node       | Delayed update transfer to a recovered owner. |
| `rebalance.plan`     | node → node       | Deterministic ownership-delta plan after join/leave. |
| `rebalance.transfer` | node → node       | Partition/version transfer from previous owner to new owner. |
| `rebalance.commit`   | node → node       | Ownership handover finalisation after transfer validation. |
| `error`              | any               | Protocol-level error frame. |

The `Dispatcher` closes the connection without sending a response when no handler
is registered for the received `kind`.

## Connection Handler and Dispatcher

Application logic is decoupled from transport concerns through the
`ConnectionHandler` interface. A `ConnectionHandler` is an async callable with
the signature:

```
async def __call__(receive: ReceiveEnvelope, send: SendEnvelope) -> None
```

where `receive()` suspends until the next `Envelope` arrives from the peer and
`send(envelope)` writes a framed envelope to the wire. The handler owns all
business logic for the duration of the connection and never interacts with
`StreamReader`, `StreamWriter`, or TLS directly.

The `Dispatcher` maps each `kind` value to exactly one registered handler. Handlers
are registered at node startup and are never modified at runtime. When an `Envelope`
arrives, the `Dispatcher` reads its `kind` field, looks up the registered handler,
and invokes it with the injected `receive` and `send` coroutines. If no handler is
registered for the kind, the connection is closed immediately. Each call to
`receive()` inside a handler returns the full `Envelope`, giving the handler access
to the `correlation_id` and `schema_id` needed to frame its response.

Backpressure is enforced through an `asyncio.Event`-based flow-control helper.
When the asyncio transport signals that its write buffer is full, the `send`
coroutine suspends until the buffer drains, preventing handlers from overwhelming
the network.

## Operation Types and Semantics

All operations below are described at the payload level. The payload format is
schema-specific and identified by `schema_id`; the `kind` field in the envelope
header is the routing key, not a payload field.

- `put`: write or replace a value under a `StoreKey` with deterministic HLC ordering metadata.
- `get`: read the current winning value for a `StoreKey`.
- `delete`: insert an ordered tombstone for a `StoreKey`.
- `replicate`: propagate a `Version` or `Tombstone` from one node to another.
- `handoff.push`: deliver buffered hints to a recovered node, preserving original ordering metadata.
- `rebalance.plan`: communicate a deterministic ownership-delta plan after a ring membership change.
- `rebalance.transfer`: stream partition data from the previous owner to the new owner.
- `rebalance.commit`: finalise an ownership handover after transfer integrity checks succeed.

Each write-like operation MUST include idempotency-relevant identifiers.

Rebalance semantics:
- Join/leave events MUST trigger rebalance operations for affected partitions.
- Rebalance transfer MUST preserve per-key ordering metadata and append-only version history.
- `REBALANCE_COMMIT` MUST occur only after transfer integrity checks succeed.

## Versioning and Compatibility

- Protocol versioning MUST be explicit in every envelope.
- Nodes SHOULD support rolling upgrades across at least one adjacent major/minor compatibility window.
- Breaking changes MUST define migration and rejection behavior.

## Error Model and Retry Semantics

Errors MUST be categorized:
- `CLIENT_INPUT_ERROR`
- `AUTHENTICATION_ERROR`
- `AUTHORIZATION_ERROR`
- `TEMPORARY_UNAVAILABLE`
- `CONFLICT_OR_ORDERING_ERROR`
- `INTERNAL_ERROR`

Retry rules:
- Temporary errors MAY be retried with bounded backoff.
- Non-retryable errors MUST be explicit.
- Duplicate retries MUST not violate deterministic state evolution.
