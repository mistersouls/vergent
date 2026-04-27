# Plan Prompt: Tourillon

## Objective and Scope

Generate an implementation-ready plan for Tourillon,
a leaderless peer-to-peer distributed key-value database.
The plan must preserve deterministic data evolution, deterministic convergence,
ordered hinted handoff, compatibility with standard load balancers, and mandatory mTLS.

## Required Inputs

- `objective.prompt.md` as the source of truth.
- Current repository state and existing docs.
- Explicit constraints from maintainers (scope, timeline, non-goals).

## Output Requirements

Produce markdown-only artifacts that combine:
- Product intent and user value (descriptive).
- Engineering requirements and acceptance criteria (`MUST`, `SHOULD`, `MAY`).

Each artifact must include:
- Goals.
- Normative requirements.
- Design rationale.
- Open questions.
- Done criteria.

## Hybrid Style Rules

- Start each document with a short functional intent paragraph.
- Follow with normative sections using RFC-2119 keywords.
- Keep implementation details practical for Python, but avoid early over-commitment.
- Prefer explicit invariants and failure behavior over high-level claims.

## Normative+Descriptive Rules

- Use `MUST` for protocol/security/convergence invariants.
- Use `SHOULD` for operational defaults and recommended patterns.
- Use `MAY` for optional extensions.
- Pair critical requirements with concise rationale.

## Required Artifacts Map

- `README.md`
- `docs/architecture.md`
- `docs/convergence.md`
- `docs/protocol.md`
- `docs/security.md`
- `docs/operations.md`
- `docs/scalability.md`
- `docs/testing.md`
- `docs/roadmap.md`

## Acceptance Checklist

- Deterministic ordering and convergence are precisely specified.
- Leaderless ring partitioning/replication is unambiguous.
- Hinted handoff ordering guarantees are explicit.
- Binary-safe self-describing protocol is defined.
- mTLS is mandatory for all inter-node and client-node traffic.
- Scaling assumptions and limits are documented for large clusters.
- Test strategy covers determinism, faults, protocol, and security.
