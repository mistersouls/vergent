# Objective

Define and deliver **Tourillon**, a leaderless, peer-to-peer distributed
key-value database that guarantees predictable data evolution, deterministic convergence,
and secure node-to-node/client-to-node communication in large, dynamic clusters.

## Functional Definition

Tourillon must:
- Operate without a central leader; any node can process reads and writes.
- Partition and replicate data through a consistent-hashing ring.
- Preserve a deterministic ordering of updates for each key.
- Ensure deterministic, conflict-free convergence across replicas.
- Support hinted handoff while preserving per-key update ordering.
- Scale horizontally to thousands of nodes.
- Remain compatible with standard load balancers (for example round-robin strategies).
- Use a binary-safe, self-describing protocol.
- Enforce mandatory mutual TLS for all communications.
- Keep the update model lightweight, deterministic, and implementation-friendly.

## Expected Benefits

- Deterministic conflict-free convergence in eventually consistent environments.
- Leaderless replication with no coordination bottleneck.
- Better resilience during transient failures via ordered hinted handoff.
- Linear-ish operational scaling across large clusters.
- Easy deployment behind common infrastructure and load balancers.
- Strong default transport security through mandatory mTLS.
- Stable behavior suitable for predictable planning, testing, and operations.

## Scope Guardrails

- Prefer deterministic behavior to heuristic conflict resolution.
- Avoid introducing central coordination components.
- Prioritize protocol clarity and cross-node interoperability.
- Keep implementation decisions aligned with ecosystem practicality.
