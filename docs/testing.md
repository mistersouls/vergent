# Testing Strategy

## Determinism and Convergence Tests

MUST validate:
- Identical final state for identical per-key histories across replicas.
- Idempotent replay of duplicate updates.
- Deterministic tie-breaking under concurrent writes.

SHOULD include property-based tests for ordering invariants.

## Protocol Compliance Tests

MUST validate:
- Envelope parsing and schema discrimination.
- Required field validation and explicit error mapping.
- Version negotiation and compatibility behavior.

SHOULD include backward/forward compatibility matrix tests.

## Security and mTLS Tests

MUST validate:
- Connection rejection without valid client/server certificates.
- Certificate expiry/revocation handling.
- Unauthorized role attempts for protected operations.

SHOULD include TLS configuration hardening checks in CI.

## Fault Injection and Recovery Tests

MUST validate:
- Node crash/restart with deterministic state recovery.
- Network partition/heal with eventual deterministic convergence.
- Ordered hinted handoff under prolonged temporary failures.

SHOULD include chaos-style tests for churn and packet loss.

## Scale and Longevity Tests

MUST validate:
- Sustained load behavior and memory growth limits.
- Ring rebalance under rolling joins/leaves.
- Long-duration stability of replication lag and handoff queues.

## Test Matrix and Gates

Release gates SHOULD require:
- Critical invariant tests passing.
- Security baseline tests passing.
- No unresolved deterministic divergence defects.
- Performance and scalability thresholds within agreed bounds.

