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
- `tourillon pki server` — issue a server certificate signed by the CA, with
  mandatory Subject Alternative Names (SAN).
- `tourillon pki client` — issue a client certificate signed by the CA.
- `tourillon node start` — start a single TCP node with mTLS, handling
  SIGINT/SIGTERM cleanly.
- `tourillon version` — display the installed version.
- Shell autocompletion via `tourillon --install-completion`.
- `tourillon/infra/pki/x509.py` adapter behind `core/ports/pki.py` Protocol
  contracts so that certificate generation logic is reusable for cert rotation.
- Production-quality UX: no raw stack traces, readable Rich output, semantic
  exit codes, private key files written with mode 0600.
- `tourctl` operator package with `tourctl kv get`, `tourctl kv put`, and
  `tourctl kv delete` commands connecting to a live node over mTLS
  (`tourctl/core/client.py`).

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
- `RingPort` Protocol defined in `core/ports/ring.py`, decoupling ring logic from
  transport and storage layers.
- Consistent-hash ring implementing `StoreKey → token → partition → replica set`
  resolution.
- Replication factor N exposed as a configurable parameter (default 3).
- Gossip-based membership dissemination covering join, graceful leave, and
  failure detection (heartbeat + suspicion model).
- Deterministic partition ownership per ring version so that any two nodes
  compute identical replica sets for a given key at a given ring epoch.
- Ring state versioned with a monotonic epoch; stale views are rejected on receipt.

Exit criteria:
- Two or more nodes converge to the same ring view within a bounded time after
  a join or leave event.
- Ownership queries for any `StoreKey` return the same replica set on all nodes
  holding the current ring epoch.
- Failure detection marks an unresponsive node as suspect within a configurable
  timeout.

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

Deliverables:
- `rebalance.plan` flow: source node emits the list of partitions and keys to
  transfer given the new ring epoch.
- `rebalance.transfer` flow: streaming bulk transfer of key/version/tombstone
  data between nodes with rate-limiting to avoid saturating the network.
- `rebalance.commit` flow: target node confirms integrity before the source
  drops ownership; commit is withheld if integrity validation fails.
- Progress observability: partitions queued, partitions in flight, bytes
  transferred, estimated lag exposed through a structured log event per
  transfer batch.
- Rate-limiting configurable per node to allow rebalance to coexist with
  foreground traffic.

Exit criteria:
- Adding or removing a node triggers a rebalance that moves only the affected
  partitions (no unnecessary data shuffling).
- Rebalance completes without read/write unavailability on non-migrating
  partitions.
- A transfer that fails integrity validation does not result in a committed
  ownership change.

## Milestone 3: Operations and Hardening

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

## Milestone 4: Scale Validation

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
