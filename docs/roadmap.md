# Roadmap

## Milestone 0: Repository Bootstrap

Goal: set up a production-ready repository foundation before core implementation.

Deliverables:
- Repository structure in place (`tourillon/`, `tests/`, `docs/`, tooling/config files).
- Test scaffold available and runnable locally.
- `uv` workflow defined for environment setup, dependency sync, and command execution.
- Code quality tooling configured (`black`, `ruff`) and wired through `pre-commit`.
- Apache License 2.0 added and referenced in project metadata.

Exit criteria:
- A fresh clone can install and run tests with `uv` using documented commands.
- `pre-commit` runs successfully and enforces formatting/cleanup hooks.
- License and baseline project structure are validated in CI.

---

## Milestone 1: Single-node Core with Transport, CLI and Operator Client

Goal: implement a local storage engine, deterministic update model, and a
working transport layer so that end-to-end connectivity can be verified on a
single node before any multi-node logic is added. The operator CLI and `tourctl`
client are developed in parallel so a complete single-node workflow
(bootstrap CA → start node → put/get/delete) can be exercised from the start.

At this milestone the node has no lifecycle phases: it starts, binds a single
`servers.kv` listener, and serves traffic immediately. `MemberPhase`, gossip,
ring partitioning, and `servers.peer` are introduced in later milestones.

Deliverables:
- `StoreKey(keyspace, key)` as the canonical addressing unit across all layers.
- `Version` and `Tombstone` carry a `StoreKey` instead of a bare key string.
- `LocalStoragePort` accepts operation objects (`WriteOp`, `ReadOp`, `DeleteOp`)
  so that signatures remain stable as the system evolves.
- Per-key deterministic ordering metadata generation via HLC.
- In-memory local log with idempotent replay (persistence is out of scope).
- `Envelope` carrying an open `kind: str` field (max 64 bytes) as the routing
  discriminant; constraints enforced at construction and decode time.
- `ConnectionHandler(receive, send)` interface and `Dispatcher` routing by
  `kind`, both defined as ports in the core hexagon.
- `TcpServer` adapter wrapping `asyncio.start_server` with a mandatory mTLS
  SSL context. No plaintext fallback.
- `Connection` adapter framing and deframing `Envelope` objects over
  `StreamReader` / `StreamWriter` with `asyncio.Event`-based backpressure.
- `tourillon pki ca` — generate a self-signed CA certificate and private key.
- `tourillon node start` — start a node with mTLS, handling SIGINT/SIGTERM
  cleanly. Binds `servers.kv` for client KV traffic. Startup form:
  `tourillon node start --config ./node-1.toml`.
- `tourillon config generate` — issue a server certificate signed by the
  supplied CA, embed cert + key + CA as inline base64 in a complete
  `config.toml` at mode `0600`.
- `tourillon config generate-context NAME` — issue a client certificate, embed
  it inline, and write a named context entry in `contexts.toml` at mode `0600`.
- `tourillon version` — display the installed version.
- Shell autocompletion via `tourillon --install-completion`.
- `tourillon/infra/pki/x509.py` adapter behind `core/ports/pki.py` Protocol
  contracts reusable across all cert-generating commands.
- `TourillonConfig` dataclass validated at startup before any I/O; four-level
  precedence: CLI flag > environment variable > config file > built-in default.
- Production-quality UX: no raw stack traces, readable Rich output, semantic
  exit codes, private key files at mode 0600.
- `tourctl kv get`, `tourctl kv put`, `tourctl kv delete` over mTLS using the
  active context.
- `tourctl config use-context NAME` and `tourctl config list`.

Exit criteria:
- `docs/testing.md` Phase 0 (provisioning) and Phase 1 (startup/IDLE) scenarios
  pass with in-memory adapters.
- An mTLS-authenticated test peer can send a `put` and receive a `put.ack`,
  verified through the TCP integration test suite.
- Connections without valid mutual TLS certificates are refused.
- `tourillon --help` and all sub-command `--help` pages render correctly.
- An operator can bootstrap a CA, issue server and client certs, start a node,
  and run `tourctl kv put/get/delete` against it in a single terminal session.
- `uv run pytest --cov-fail-under=90` passes.

---

## Milestone 2: Ring and Membership Foundations

Goal: implement the ring subsystem and membership data structures as pure,
in-memory, network-free models that can be fully validated by unit and
property-based tests. This milestone produces no new CLI commands and no
inter-node traffic; it lays the algorithmic foundation that every subsequent
milestone builds on.

Deliverables:
- `HashSpace(bits: int = 128)` — configurable-width circular hash space;
  `bits=128` in production, smaller values (e.g. `bits=8`) for tests; hash
  function is MD5 output truncated to `bits`; all arithmetic modulo `2**bits`.
- `VNode(node_id: str, token: int)` — atomic unit of ring position.
- `Ring` — immutable sorted sequence of vnodes; O(log n) successor lookup via
  `bisect_right`; `add_vnodes` and `drop_nodes` return new instances;
  `iter_from` for clockwise traversal.
- `Partitioner(partition_shift: int)` — maps tokens to `LogicalPartition` via
  `pid = h >> (bits - partition_shift)`; `placement_for_token(token, ring)`
  as the primary routing entry point; `pid_for_hash` and `segment_for_pid` as
  lower-level helpers.
- `LogicalPartition(pid, start, end)` — immutable hash-space segment covering
  the half-open interval `(start, end]`.
- `PartitionPlacement(partition, vnode)` — ephemeral owner binding; `address`
  property for stable storage key prefix; never persisted.
- `PlacementStrategy` Protocol — single method
  `preference_list(placement, ring) -> list[str]`; pure function of its
  arguments. `SimplePreferenceStrategy` default implementation: distinct-node
  clockwise walk; `READY` and `DRAINING` eligible; `IDLE` and `JOINING`
  excluded.
- `MemberPhase` enum (`IDLE`, `JOINING`, `READY`, `DRAINING`) in
  `core/structure/membership.py`. Phase transitions occur only when operations
  complete; restarts resume the persisted phase; `DRAINING` is irreversible.
- `Member` value object: `node_id`, `peer_address`, `generation`, `seq`,
  `phase`. No wall-clock timestamps. `supersedes()` merge rule:
  higher `generation` wins; equal generation, higher `seq` wins.
- Replication factor `rf` as a configurable cluster-wide parameter (default 3),
  validated at startup.

Exit criteria:
- `docs/testing.md` Phase 8 (ring invariants) scenarios pass for `bits=8` and
  `bits=128` configurations.
- A ring constructed with `bits=8`, `partition_shift=4` produces the same
  `preference_list` output as an equivalent `bits=128` instance for identical
  `StoreKey` inputs.
- `SimplePreferenceStrategy` correctly excludes `IDLE` and `JOINING` nodes and
  includes `DRAINING` nodes.
- `Member.supersedes()` correctly orders all four generation/seq combinations.
- `uv run pytest --cov-fail-under=90` passes.

---

## Milestone 3: Single-node Join (`tourillon node join` — first-node bootstrap)

Goal: implement the first half of `docs/lifecycle/node-join.md` — the case
where a node joins with no peers. After this milestone, a node has a complete
operational lifecycle for the single-node case: it starts in `IDLE`, accepts a
`join` command, transitions to `READY`, serves traffic with partition awareness,
and restarts correctly from both `IDLE` and `READY`.

Deliverables:
- `servers.peer` listener added to `tourillon node start`, with its own mTLS
  `SSLContext` and independent `Dispatcher`. Config section `[servers.peer]`
  validated at startup.
- `MemberPhase` persisted in the local storage backend before any gossip is
  emitted (in-memory adapter sufficient).
- Phase guards on all request handlers: `IDLE` rejects KV operations with
  `TEMPORARY_UNAVAILABLE`; `READY` accepts them.
- `tourillon node join` CLI command (and `tourctl node join`) — first-node
  bootstrap path: no seeds configured or no seeds reachable → node assigns all
  partitions to itself, publishes ring epoch 1, transitions to `READY`.
- `generation` incremented exactly once on `IDLE → JOINING`. `seq` reset to 0.
  Both values persisted alongside phase.
- Restart from `IDLE` → remains `IDLE`, no gossip, waits for `join` command.
- Restart from `READY` → re-announces `READY` immediately (seq + 1), serves
  traffic. `generation` unchanged.
- `docs/lifecycle/node-startup.md` and `docs/lifecycle/node-restart.md`
  (IDLE and READY sections) fully implemented.

Exit criteria:
- `docs/testing.md` Phase 1 (IDLE rejects KV), Phase 2 (first-node bootstrap),
  and Phase 4 (restart from IDLE, restart from READY — generation invariant)
  scenarios pass.
- `tourillon node start → tourillon node join → READY` is demonstrable in a
  single terminal session.
- After SIGTERM + restart the node still holds `phase=READY`, serves `put/get`,
  and emits a gossip record with the same `generation` and `seq` incremented
  by 1.
- `uv run pytest --cov-fail-under=90` passes.

---

## Milestone 4: Multi-node Join (`tourillon node join` — seeded cluster)

Goal: implement the second half of `docs/lifecycle/node-join.md` — a node
joining an existing cluster via seed contact, including the partition data
transfer required to make it a full replica. After this milestone a two-node
cluster is reachable: both nodes hold consistent ring views, both serve reads
and writes, and a crash mid-join resumes correctly.

Implementing join for a second node requires the rebalance transfer protocol
(`rebalance.plan / rebalance.transfer / rebalance.commit`) because the joining
node must receive its assigned partition data. This protocol is implemented here,
scoped to the join path. The drain path (source-initiated transfer) is completed
in Milestone 6.

Deliverables:
- Gossip engine: seed contact over `servers.peer`, `ring.fetch` request/response,
  ring epoch management, `Member` record propagation.
- `ring.propose_join` flow: joiner sends its vnode set to seeds; every node
  independently computes ring epoch `E+1` from the updated vnode set.
- Partition assignment computation: pure function of `(E_new, ring)`, identical
  on every node.
- `rebalance.plan` envelope kind: source advertises partition range, epoch
  context, and integrity metadata to the joiner.
- `rebalance.transfer` envelope kind: source streams `Version` and `Tombstone`
  records in HLC order; joiner stores them in a quarantined staging area (not
  visible to reads until commit).
- `rebalance.commit` envelope kind: joiner validates integrity digest, promotes
  quarantined partition to live read path, notifies source.
- Dual-ownership window: writes to migrating partitions accepted by both source
  and target and cross-replicated via `replicate`.
- Joining node transitions to `READY` once all primary replica slots have a
  committed `rebalance.commit`.
- Restart from `JOINING`:
  - `generation` not re-incremented.
  - Already-committed partitions not re-transferred.
  - In-progress transfers resumed from last per-batch HLC checkpoint.
  - `docs/lifecycle/node-restart.md` (JOINING section) fully implemented.

Exit criteria:
- `docs/testing.md` Phase 2 (second node joins), Phase 4 (restart from JOINING)
  scenarios pass.
- Two in-memory nodes A and B: B joins A → both reach `READY` → both compute
  identical `preference_list` for any `StoreKey`.
- Crash B mid-transfer → restart B → B resumes without re-transferring already
  committed partitions → B reaches `READY`.
- `uv run pytest --cov-fail-under=90` passes.

---

## Milestone 5: Replication and Hinted Handoff

Goal: writes reach all members of the replica set and temporary unavailability
of a replica does not lose acknowledged writes. After this milestone a 3-node
cluster survives a temporary node loss without data loss and converges
correctly once the node recovers.

Deliverables:
- `replicate` envelope kind: coordinator fans out writes to all preference-list
  members and awaits quorum acknowledgement.
- Quorum acknowledgement logic: write considered successful once `⌊rf/2⌋ + 1`
  replicas confirm.
- Proxy paths for non-owned partitions: a node that receives a write or read
  for a key it does not own forwards it to the correct coordinator.
- Local reachability tracking: `REACHABLE / SUSPECT / DEAD` per peer, driven
  by operation failures, never propagated via gossip.
- Hinted handoff queue: writes destined for a `DEAD` peer are stored locally
  with original HLC metadata unchanged.
- `handoff.push` envelope kind: hints delivered to recovering peer in HLC order.
- Backpressure on replication path via `asyncio.Semaphore`.
- Convergence properties: idempotent apply, out-of-order delivery, tombstone
  propagation — all as specified in `docs/convergence.md`.

Exit criteria:
- `docs/testing.md` Phase 3 (write acknowledged by quorum, deterministic
  preference list, proxy), Phase 6 (convergence: idempotent apply,
  out-of-order, tombstone), and Phase 7 (failure detection, hinted handoff,
  HLC-ordered replay) scenarios pass.
- In a 3-node in-memory cluster: write on A acknowledged → readable from B and
  C independently.
- B goes down → writes are hinted on A → B restarts → A delivers hints in HLC
  order → B's state matches what it would hold without the gap.
- `uv run pytest --cov-fail-under=90` passes.

---

## Milestone 6: `tourillon node leave` (READY → DRAINING → IDLE)

Goal: implement `docs/lifecycle/node-drain.md` — a node can gracefully leave
the cluster, transfer all its partitions to the remaining nodes, and reach
`IDLE`. A crash mid-drain resumes correctly from the last checkpoint. After this
milestone the full IDLE → JOINING → READY → DRAINING → IDLE cycle is
exercisable end-to-end.

Deliverables:
- `tourctl node leave` command triggering `READY → DRAINING` transition
  (persisted before gossip).
- `DRAINING` nodes: stay in `preference_list` for reads; return
  `TEMPORARY_UNAVAILABLE` for primary writes so coordinators fall through.
- Drain protocol: source derives rebalance plan for its partitions, streams to
  target nodes via `rebalance.plan / rebalance.transfer / rebalance.commit`
  (extending the transfer protocol from Milestone 4 with the source-initiated
  drain path).
- Rate-limiting on transfer sources via configurable bandwidth fraction;
  concurrent transfer bound via `asyncio.Semaphore`.
- Drain completion: all partitions committed → leave notice broadcast → ring
  epoch `E+1` without this node's vnodes → `DRAINING → IDLE` transition.
- Restart from `DRAINING`: resume from drain checkpoint; already-committed
  partitions not re-transferred; `docs/lifecycle/node-restart.md` (DRAINING
  section) fully implemented.
- Irreversibility enforced: `join` and second `leave` during `DRAINING` both
  return `INVALID_PHASE`.
- Worst-case scenarios from `docs/rebalance.md`: crash mid-commit, gossip lag
  during drain, network partition between source and target mid-transfer.

Exit criteria:
- `docs/testing.md` Phase 5 (happy-path drain, cancel rejected, DRAINING in
  preference list, TEMPORARY_UNAVAILABLE for primary writes) and Phase 4
  (restart from DRAINING) scenarios pass.
- 3-node in-memory cluster: A leaves → B and C absorb A's partitions →
  A reaches `IDLE` → ring epoch advances once.
- Crash A mid-drain → restart A → drain resumes from checkpoint → A reaches
  `IDLE` without re-transferring already-committed partitions.
- `uv run pytest --cov-fail-under=90` passes.
- `uv run pre-commit run --all-files` passes.
- Property-based tests (Hypothesis) assert: for any sequence of join/leave
  events on an in-memory cluster, the final ownership map and per-key HLC
  histories on every surviving node are identical.

---

## Milestone 7: Operations and Hardening

Goal: production-readiness baseline covering certificate lifecycle automation,
operator tooling, observability, and validated operational procedures.

Deliverables:
- `tourillon pki rotate` command: zero-downtime mTLS certificate rotation via a
  dual-cert window allowing old and new certificates to coexist during rollover.
- Revocation strategy defined and documented: choice between CRL, OCSP stapling,
  or short-lived certificates, with rationale in `docs/security.md`.
- `tourctl ring inspect` command: current ring state, per-partition ownership,
  replica sets, and pending rebalance operations.
- `tourctl log tail` command: stream structured logs from a remote node over
  mTLS.
- Structured logging with correlation IDs propagated across all distributed code
  paths (replication, proxy, handoff, rebalance).
- Liveness and readiness probes suitable for load balancers and container
  orchestrators.
- Rolling upgrade runbook: step-by-step procedure for upgrading protocol
  versions across a live cluster without downtime.

Exit criteria:
- Certificate rotation completes on a running cluster with no connection
  interruptions observed by a continuous client.
- `tourctl ring inspect` output is consistent with the ring state reported by
  each node's own view.
- Liveness and readiness probes return correct status under normal operation and
  during a staged node failure.
- Rolling upgrade procedure is executed in a multi-node test environment without
  data loss.

---

## Milestone 8: Scale Validation

Goal: validate large-cluster behaviour, identify scalability limits, and publish
reliability thresholds in `docs/scalability.md`.

Deliverables:
- Capacity test suite against representative cluster sizes (5, 10, and 30 nodes)
  covering put/get/delete throughput and latency under sustained load.
- Hotspot and rebalance validation under asymmetric key distributions.
- Long-running stability campaigns surfacing memory leaks, task accumulation,
  and clock drift effects.
- Fault-injection suite: abrupt node loss, network partition between node
  subsets, clock drift injection, certificate expiration mid-run.
- Scalability and reliability thresholds published in `docs/scalability.md`.

Exit criteria:
- All capacity tests produce results at or above the thresholds defined in
  `docs/scalability.md`.
- No stability campaign surfaces unrecovered errors or resource leaks.
- All fault-injection scenarios result in eventual convergence within documented
  time bounds with no permanent data loss.
- `docs/scalability.md` is reviewed and accepted as the public reference for
  cluster sizing guidance.
