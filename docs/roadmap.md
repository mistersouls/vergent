# Roadmap

## Milestone 0: Repository Bootstrap

Goal: set up a production-ready repository foundation before core implementation.

Deliverables:
- Repository structure in place (`tourillon/`, `tests/`, `docs/`, tooling/config files).
- Test scaffold available and runnable locally.
- `uv` workflow defined for environment setup, dependency sync, and command execution.
- Code quality tooling configured (`black`, `autoflake`) and wired through `pre-commit`.
- Apache License 2.0 added and referenced in project metadata.

Exit criteria:
- A fresh clone can install and run tests with `uv` using documented commands.
- `pre-commit` runs successfully and enforces formatting/cleanup hooks.
- License and baseline project structure are validated in CI.

## Milestone 1: Single-node Core with Transport, CLI and Operator Client

Goal: implement local storage engine, deterministic update model, and a working
transport layer so that end-to-end connectivity is verified from the start.
The operator CLI and `tourctl` client are developed in parallel with the core so
that a real end-to-end workflow (bootstrap CA → start node → put/get/delete) can
be exercised before moving to multi-node replication.

Deliverables:
- `StoreKey(keyspace, key)` as the canonical addressing unit across all layers.
- `Version` and `Tombstone` carry a `StoreKey` instead of a bare key string.
- `LocalStoragePort` accepts operation objects (`WriteOp`, `ReadOp`,
  `DeleteOp`) so that signatures remain stable as the system evolves.
- Per-key deterministic ordering metadata generation via HLC.
- Durable local log and state with idempotent replay.
- `Envelope` carrying an open `kind: str` field (max 64 bytes) as the routing
  discriminant; constraints enforced at construction and decode time.
- `ConnectionHandler(receive, send)` interface and `Dispatcher` routing by
  `kind`, both defined as ports in the core hexagon.
- `TcpServer` adapter in `infra/tcp/` wrapping `asyncio.start_server` with an
  mTLS SSL context. No plaintext fallback.
- `Connection` adapter that frames and deframes `Envelope` objects over
  `StreamReader` / `StreamWriter` with `asyncio.Event`-based backpressure.
- `tourillon pki ca` — generate a self-signed CA certificate and private key.
- `tourillon node start` — start a node with mTLS, handling SIGINT/SIGTERM
  cleanly. The node binds a single TCP listener on `servers.kv` for client KV
  traffic (put/get/delete) with mandatory mutual TLS. The startup form is
  `tourillon node start --config ./node-1.toml`; individual override flags
  such as `--node-id` or `--log-level` may be appended and always shadow the
  corresponding config file value.
- `tourillon config generate` — issue a server certificate signed by the
  supplied CA, embed the certificate, private key, and CA certificate as base64
  inline into a complete `config.toml`, and write it at mode `0600`. Accepts
  `--node-id`, `--ca-cert`, `--ca-key`, `--san-dns`, `--san-ip`,
  `--kv-bind <host:port>`, `--kv-advertise <host:port>` (optional),
  and `--out`. Uses the same `tourillon/infra/pki/x509.py` PKI adapter as
  `tourillon config generate-context`.
- `tourillon config generate-context NAME` — issue a client certificate signed
  by the supplied CA, embed the certificate, private key, and CA certificate as
  base64 inline, and write (or update) a named context entry in
  `~/.config/tourillon/contexts.toml` at mode `0600`. Accepts `--ca-cert`,
  `--ca-key`, `--kv-endpoint <host:port>` (required in M1), `--common-name`
  (optional, defaults to NAME), and `--days` (optional). Uses the same PKI
  adapter as `tourillon config generate`.
- `tourillon version` — display the installed version.
- Shell autocompletion via `tourillon --install-completion`.
- `tourillon/infra/pki/x509.py` adapter behind `core/ports/pki.py` Protocol
  contracts so that certificate generation logic is reusable across
  `tourillon config generate`, `tourillon config generate-context`, and cert
  rotation.
- `TourillonConfig` dataclass in `tourillon/core/config.py` as the canonical
  in-memory configuration representation, validated at startup before any I/O.
  Config values are resolved in precedence order: CLI flag > environment variable
  > config file > built-in default.
- Production-quality UX: no raw stack traces, readable Rich output, semantic
  exit codes, private key files written with mode 0600.
- `tourctl` operator package with `tourctl kv get`, `tourctl kv put`, and
  `tourctl kv delete` commands connecting to the node's `servers.kv` endpoint
  over mTLS using the active context (`tourctl/core/client.py`).
- `tourctl config use-context NAME` — set the active context in
  `~/.config/tourillon/contexts.toml` so that subsequent `tourctl` commands
  connect to the named cluster without requiring explicit flags.
- `tourctl config list` — list all available contexts and mark the active one.

Exit criteria:
- Determinism tests pass for single-node put/get/delete workflows.
- An mTLS-authenticated test peer can send a `put` envelope and receive a
  `put.ok`, verified through the TCP integration test suite
  (`core/net/tcp/testing.py`).
- Connections without valid mutual TLS certificates are refused.
- `tourillon --help` and all sub-command `--help` pages render correctly.
- An operator can bootstrap a CA, issue server and client certs, and start a
  node pointing to those certs in a single terminal session.
- `tourctl kv get/put/delete` complete successfully against a running node.
- `pytest --cov-fail-under=90` passes.

## Milestone 2: Multi-node Replication and Partition Rebalance

Goal: implement leaderless replication over a consistent-hashing ring with
partition rebalancing, hinted handoff, and gossip-based membership.

### Phase 2a — Ring and Membership

Goal: establish a shared, self-consistent view of cluster topology so that every
node can independently compute partition ownership without a coordinator.

Deliverables:
- Ring subsystem in `tourillon/core/ring/`:
  - `HashSpace(bits: int = 128)` — configurable-width circular hash space.
    `bits=128` in production; smaller values (e.g. `bits=8`) for tests. Hash
    function is MD5 output truncated to `bits`. All arithmetic is modulo
    `2**bits`. Token generation is handled by the node bootstrap layer.
  - `VNode(node_id: str, token: int)` — atomic unit of ring position.
    A physical node owns multiple vnodes; `token` is an `int` in `[0, 2**bits)`.
  - `Ring` — immutable sorted sequence of vnodes; O(log n) successor lookup
    via `bisect_right` on token values; `add_vnodes` and `drop_nodes` return
    new `Ring` instances; `iter_from` for clockwise ring walks.
  - `Partitioner(partition_shift: int)` — maps tokens to `LogicalPartition`
    instances via `pid = h >> (bits - partition_shift)`; validates
    `partition_shift < bits` at construction. Exposes
    `placement_for_token(token, ring) → PartitionPlacement` as the primary
    routing entry point, combining partition lookup and ring successor resolution
    into a single step; `pid_for_hash` and `segment_for_pid` remain available as
    lower-level helpers. `partition_shift=17` (131 072 partitions) is the
    recommended floor for clusters expected to grow to 10 000 nodes; see
    `docs/ring.md` §4.2 for the full sizing table.
  - `LogicalPartition(pid, start, end)` — immutable hash-space segment
    covering the half-open interval `(start, end]`.
  - `PartitionPlacement(partition, vnode)` — ephemeral binding of a logical
    partition to its current ring owner; produced by
    `Partitioner.placement_for_token`; `address` property returns the stable
    storage key prefix for the partition; recomputed after every ring mutation,
    never persisted.
  - `PlacementStrategy` Protocol — single method
    `preference_list(placement, ring) -> list[str]`; must be a pure
    function of its arguments. `SimplePreferenceStrategy` is the default
    implementation: distinct-node clockwise ring walk, `READY` and `DRAINING`
    nodes are eligible, `IDLE` and `JOINING` are excluded. Future
    implementations may enforce topology-label anti-affinity without changing
    any call site.
- Replication factor `rf` exposed as a configurable cluster-wide parameter
  (default 3), validated at startup before any partition assignment takes place.
- `MemberPhase` enum (`IDLE`, `JOINING`, `READY`, `DRAINING`) in
  `core/structure/membership.py` representing each node's self-declared
  operational state. Phase transitions occur only when operations complete;
  restarts resume the persisted phase. `DRAINING` is irreversible for now.
- `Member` value object carrying `node_id`, `peer_address`, `generation`,
  `seq`, and `phase`. No wall-clock timestamps; no local-only fields.
  Local reachability observations (SUSPECT / DEAD) are never transmitted.
- Gossip-based membership dissemination: each node propagates its own
  `MemberPhase` transitions (JOINING, READY, DRAINING) to peers. Local
  reachability state (SUSPECT / DEAD) is determined independently by each node
  from operation failures and is never propagated.
- Deterministic partition ownership per ring epoch: any two nodes holding the
  same epoch produce identical `preference_list` outputs for a given key.
- Ring state versioned with a monotonic epoch; envelopes carrying a stale epoch
  are rejected on receipt.
- `docs/ring.md` as the normative specification for the hash space, virtual-node
  ring, partitioner, logical partitions, placement strategy, and ring epoch
  model.
- `docs/membership.md` documenting both FSMs, the local-only failure detection
  invariant, phase persistence, restart behaviour, and the relationship between
  phase transitions and ring epochs.

Exit criteria:
- Two or more nodes converge to the same ring view within a bounded time after
  a join or leave event.
- `preference_list` for any `StoreKey` returns the same ordered node sequence
  on all nodes holding the current ring epoch.
- Failure detection marks an unresponsive node as suspect within a configurable
  timeout.
- A test instance using `bits=8` and `partition_shift=4` converges to the same
  ownership map as an equivalent `bits=128` instance for all applicable
  determinism tests.

### Phase 2b — Replication and Hinted Handoff

Goal: replicate writes to all members of a replica set and handle temporary
unavailability without data loss.

Deliverables:
- `replicate` flow over the existing transport: coordinator fans out writes to
  replica-set members and awaits quorum acknowledgement.
- Proxy read and write paths for keys whose primary partition is not owned by
  the receiving node.
- Hinted handoff queue persisting writes destined for temporarily unavailable
  replicas, with `handoff.push` kind and full ordering metadata preserved.
- Deterministic delivery order when a hinted handoff target rejoins: entries
  replayed in HLC order.
- Backpressure on the replication path via `asyncio.Semaphore` to prevent
  unbounded in-flight write accumulation.

Exit criteria:
- Multi-node convergence tests pass: a write acknowledged by quorum is readable
  from any replica after recovery.
- A replica that rejoins after a gap receives all missed writes via hinted
  handoff in deterministic order.
- Proxy paths route reads and writes correctly when tested against a node that
  does not own the target partition.

### Phase 2c — Partition Rebalance

Goal: move partition data safely across ring members when topology changes
(scale-out, scale-in, or ring epoch change).

The normative design, full deliverable breakdown, and exit criteria for
partition rebalance are defined in **Milestone 3 — Rebalance** (see below).
Phase 2c depends on the ring-epoch plumbing from Phase 2a and the replication
transport from Phase 2b being fully in place; Milestone 3 is sequenced
immediately after Phase 2b is complete.

## Milestone 3: Rebalance

Goal: implement the full partition rebalance subsystem as specified in
`docs/rebalance.md`, enabling safe, deterministic, and observable ownership
migration across ring members under continuous churn, gossip lag, and crash
scenarios, without violating convergence or availability guarantees.

### Deliverables

- `tourillon/core/ports/rebalance.py` — `RebalancePort` Protocol; types
  `RebalancePlan`, `TransferBatch`, `RebalanceCommit`, `IntegrityDigest`;
  `RebalanceError` taxonomy covering at minimum `EPOCH_UNKNOWN`,
  `EPOCH_SUPERSEDED`, `INTEGRITY_FAILURE`, `PEER_NOT_IN_RING`, and
  `CONFLICT_OR_ORDERING_ERROR`.
- `tourillon/core/structure/rebalance/plan.py` — `derive_plan(e_old, e_new,
  ring) -> RebalancePlan` pure function: deterministic, stateless, no
  wall-clock time, no random tiebreaks; supports optional folding of adjacent
  epoch transitions into a single composed plan.
- `tourillon/core/structure/rebalance/coordinator.py` — async rebalance
  coordinator using `asyncio.TaskGroup` for structured concurrency,
  `asyncio.Semaphore` to bound concurrent in-flight transfers per node, and
  `asyncio.Event` for end-to-end backpressure signalling; handles plan
  supersession by a newer epoch, crash-recovery checkpoints, and the full
  drain lifecycle.
- `tourillon/core/handlers/rebalance.py` — handler dispatchers for the three
  envelope kinds (`rebalance.plan`, `rebalance.transfer`, `rebalance.commit`);
  validates epoch context on receipt, applies transferred records through the
  standard idempotent append-only path, and enforces the dual-ownership write
  fan-out and read merge rules.
- Extensions to `LocalStoragePort` for quarantine staging (accept in-progress
  transfer batches without making them visible to reads) and atomic promote
  (commit a fully validated quarantined partition to the live read path in a
  single atomic operation); the in-memory storage adapter updated to implement
  both extensions.
- Durable rebalance log recording, keyed by `(partition, E_new)`: plan
  acceptance on both source and target sides, per-batch transfer progress (last
  HLC applied at the target), commit issuance (target), and commit
  acknowledgement (source); replay of any log entry MUST be idempotent.
- Structured observability: log events for plan derivation
  `(E_old, E_new, partitions, source, target)`, transfer batches
  `(partition, batch_id, records, bytes, last_hlc)`, commit decisions
  `(partition, target, decision, integrity_digest)`, and aborts/retries with
  explicit reason codes; metrics for partitions queued, in-flight, completed,
  and failed; bytes and records transferred per source and per target;
  time-to-commit p50/p95/p99; plans aborted due to epoch supersession; hinted
  handoff backlog created or drained as a side effect — all as specified in
  `docs/rebalance.md`; events and metrics surfaced through `tourctl ring
  inspect` and structured-log paths.
- Operator-plane wiring: ring-epoch change events trigger plan derivation in
  the coordinator; `tourctl/` extended with rebalance status sub-commands;
  rate-limit fraction and semaphore concurrency bound exposed as configurable
  parameters in `TourillonConfig`.

### Exit Criteria

- The deterministic plan derivation is a pure function of
  `(epoch_old, epoch_new, ring_old, ring_new)` and produces identical output
  on every node for the same inputs, with no wall-clock time or random
  tiebreaks.
- `rebalance.plan`, `rebalance.transfer`, and `rebalance.commit` are fully
  implemented with integrity validation and idempotent replay.
- The dual-ownership window preserves availability and quorum semantics across
  handover, validated by automated tests.
- Crash, partition, and storm scenarios listed under *Worst-Case Scenarios* in
  `docs/rebalance.md` are covered by automated fault-injection tests and pass
  reliably.
- Observability events and metrics listed in `docs/rebalance.md` are emitted
  and consumable via `tourctl ring inspect` and structured-log paths.
- All rebalance traffic is verified to traverse mTLS exclusively, with
  certificate identity tied to ring membership.
- `uv run pytest --cov-fail-under=90` passes.
- `uv run pre-commit run --all-files` passes.
- Property-based tests (Hypothesis) assert that for any sequence of topology
  events applied in any order, the final ownership map and per-key HLC
  histories on every surviving replica are identical.
- A negative mTLS test verifies that a transfer peer whose certificate identity
  is not present in the current ring epoch is rejected at the transport layer
  before any rebalance payload is processed.

## Milestone 4: Operations and Hardening

Goal: production-readiness baseline covering certificate lifecycle automation,
operator tooling, observability, and validated operational procedures.

Deliverables:
- `tourillon pki rotate` command: rotating mTLS certificates with zero downtime
  (dual-cert window allowing old and new certificate to coexist during rollover).
- Revocation strategy defined and documented: choice between CRL, OCSP stapling,
  or short-lived certificates, with rationale recorded in `docs/architecture.md`.
- `tourctl ring inspect` command: display current ring state, per-partition
  ownership, replica sets, and pending rebalance operations.
- `tourctl log tail` command: stream structured logs from a remote node over an
  mTLS-authenticated connection.
- Structured logging with correlation IDs propagated across all distributed code
  paths (replication, proxy, handoff, rebalance).
- Liveness and readiness probes suitable for load balancers and container
  orchestrators, documented with expected response semantics.
- Rolling upgrade runbook: step-by-step procedure for upgrading protocol versions
  across a live cluster without downtime.
- Backup and restore procedures: documented, scripted, and validated against a
  representative data set in a test environment.

Exit criteria:
- Certificate rotation completes on a running cluster with no connection
  interruptions observed by a continuous client.
- `tourctl ring inspect` output is consistent with the ring state reported by
  each node's own view.
- Liveness and readiness probes return correct status under normal operation and
  during a staged node failure.
- Rolling upgrade procedure is executed successfully in a multi-node test
  environment without data loss.
- Backup followed by full restore passes data integrity checks.

## Milestone 5: Scale Validation

Goal: validate large-cluster behavior, identify scalability limits, and publish
reliability thresholds.

Deliverables:
- Capacity test suite executed against representative cluster sizes (5, 10, and
  30 nodes) covering put/get/delete throughput and latency under sustained load.
- Hotspot and rebalance validation under asymmetric key distributions (skewed
  keyspace access patterns and uneven partition sizes).
- Long-running stability campaigns: mixed read/write/delete workloads sustained
  over extended periods to surface memory leaks, goroutine/task accumulation,
  and clock drift effects.
- Fault-injection suite covering: abrupt node loss, network partition between
  subsets of nodes, clock drift injection, and certificate expiration mid-run.
- Scalability and reliability thresholds published in `docs/scalability.md`
  (latency percentiles, throughput ceilings, convergence time bounds, maximum
  tested cluster size).

Exit criteria:
- All capacity tests produce results at or above the thresholds defined in
  `docs/scalability.md`.
- Hotspot tests confirm that rebalancing redistributes load within the bounds
  defined for the tested distributions.
- No stability campaign surfaces unrecovered errors or resource leaks over the
  full run duration.
- All fault-injection scenarios result in eventual convergence within documented
  time bounds with no permanent data loss.
- `docs/scalability.md` is reviewed and accepted as the public reference for
  cluster sizing guidance.
