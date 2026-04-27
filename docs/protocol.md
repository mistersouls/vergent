# Protocol

## Transport and Session Model

- All communication MUST run over mutually authenticated TLS channels.
- Node-to-node and client-to-node flows MUST use the same framing rules.
- Connection reuse SHOULD be enabled to reduce handshake overhead.

Security policy details are defined in `docs/security.md`.

## Message Envelope (binary-safe, self-describing)

Each message MUST include:
- Protocol version.
- Message type.
- Correlation/request identifier.
- Payload schema identifier.
- Payload bytes (binary-safe).
- Integrity fields required by transport/security layers.

Envelope requirements:
- Messages MUST be parseable without out-of-band schema assumptions.
- Unknown fields SHOULD be ignored when forward compatibility allows.
- Unknown mandatory fields MUST fail fast with explicit error codes.

## Operation Types and Semantics

Baseline operations:
- `PUT`: write or replace value with deterministic metadata.
- `GET`: read value and associated metadata.
- `DELETE`: tombstone-based deletion under deterministic ordering.
- `REPLICATE`: inter-node update propagation.
- `HANDOFF_PUSH`: delayed update transfer to recovered owner.
- `REBALANCE_PLAN`: deterministic ownership-delta plan generation after join/leave.
- `REBALANCE_TRANSFER`: partition/version transfer from previous owner to new owner.
- `REBALANCE_COMMIT`: ownership handover finalization after transfer validation.

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
