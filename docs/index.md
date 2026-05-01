# Tourillon Documentation

Tourillon is a leaderless, peer-to-peer distributed key-value database written
in Python 3.14. It guarantees deterministic convergence, ordered hinted
handoff, consistent-hashing ring partitioning, and mandatory mTLS for all
traffic.

This index describes how the documentation is organised and recommends a
reading order for each audience.

---

## Lifecycle documentation — start here

The most important question to answer when working with Tourillon is: **what
is a node doing right now, and what can it do next?** These four documents
answer that question for every phase of a node's life.

| Document | Answers |
|----------|---------|
| [`lifecycle/node-startup.md`](lifecycle/node-startup.md) | What must exist before `tourillon node start` succeeds? What does the startup sequence do? What state is the node in right after startup? |
| [`lifecycle/node-join.md`](lifecycle/node-join.md) | What triggers a join? How does a first-node bootstrap differ from a subsequent join? How does partition data move to the joining node? |
| [`lifecycle/node-restart.md`](lifecycle/node-restart.md) | How does restart behaviour differ depending on the persisted phase (IDLE, JOINING, READY, DRAINING)? When is `generation` incremented? |
| [`lifecycle/node-drain.md`](lifecycle/node-drain.md) | What triggers a drain? Why is it irreversible? How do partition transfers work during drain? What happens on completion? |

---

## Reference documentation

These documents define the normative specifications for subsystems. They are
more precise but more technical than the lifecycle documents.

| Document | Covers |
|----------|--------|
| [`architecture.md`](architecture.md) | Keyspace model, `StoreKey`, storage command objects, CLI and config layers, dual-endpoint model |
| [`membership.md`](membership.md) | `MemberPhase` FSM, `Member` value object, local failure detection, relationship to ring epochs |
| [`ring.md`](ring.md) | Hash space, vnodes, `Ring`, `Partitioner`, `LogicalPartition`, placement strategy, ring epochs |
| [`rebalance.md`](rebalance.md) | Rebalance plan derivation, transfer/commit protocol, dual-ownership window, worst-case scenarios |
| [`protocol.md`](protocol.md) | `Envelope` framing, `kind` registry, `Dispatcher`, error model |
| [`convergence.md`](convergence.md) | HLC ordering model, append-only version stream, conflict model, replica apply rules |
| [`security.md`](security.md) | mTLS requirements, certificate lifecycle, PKI bootstrap, threat model |
| [`scalability.md`](scalability.md) | Capacity model, vnode trade-offs, partition grid sizing, churn-burst guardrails |
| [`operations.md`](operations.md) | CLI reference, PKI commands, config generation, `tourctl` contexts, runbooks |
| [`roadmap.md`](roadmap.md) | Milestones M0–M5 with deliverables and exit criteria |
| [`testing.md`](testing.md) | Test strategy organised by lifecycle phase with concrete in-memory scenarios |

---

## Reading paths by audience

### I am a new contributor

Read in this order:
1. `lifecycle/node-startup.md` — understand the preconditions and startup sequence.
2. `lifecycle/node-join.md` — understand how a cluster is formed.
3. `architecture.md` — understand the hexagonal structure and config model.
4. `membership.md` — understand the FSM you will implement or extend.
5. `ring.md` — understand how keys are routed.
6. `testing.md` — understand what a testable deliverable looks like.

### I am implementing a milestone

1. Read `roadmap.md` for your milestone's deliverables and exit criteria.
2. Read the relevant lifecycle documents for the phases your milestone touches.
3. Read the normative spec for each subsystem you are implementing.
4. Use `testing.md` as the authoritative source for which test scenarios are
   required before marking a milestone complete.

### I am operating a cluster

1. Read `operations.md` for the CLI reference and runbooks.
2. Read `lifecycle/node-startup.md` and `lifecycle/node-join.md` for the
   provisioning and join procedures.
3. Read `lifecycle/node-drain.md` for graceful scale-in.
4. Read `security.md` for certificate management policies.

### I am reviewing a pull request

1. Check `lifecycle/node-restart.md` for any code that reads persisted phase.
2. Check `membership.md §6` for any code that touches ring epochs.
3. Check `convergence.md` for any code that writes or merges version records.
4. Check `testing.md` for the phase-based test requirements that must be met.

---

## Document stability

| Status | Meaning |
|--------|---------|
| **Normative** | Behaviour defined here is authoritative. Code must not contradict it. |
| **Informative** | Describes rationale, examples, or operational guidance. |

All documents in `docs/` are normative unless otherwise stated. RFC 2119
keywords (`MUST`, `SHOULD`, `MAY`) appear only in normative sections.
