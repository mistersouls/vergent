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

## Milestone 1: Single-node Core with Transport

Goal: implement local storage engine, deterministic update model, and a working
transport layer so that end-to-end connectivity is verified from the start.

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

Exit criteria:
- Determinism tests pass for single-node put/get/delete workflows.
- A client can connect over mTLS, send a `put` envelope, and receive a `put.ack`.
- Connections without valid mutual TLS certificates are refused.

## Milestone 2: Multi-node Replication

Goal: implement leaderless replication over consistent-hashing ring.

Deliverables:
- Membership and ring ownership logic.
- `StoreKey → partition token → replica set` resolution using the ring.
- Replication flows using the `replicate` kind over the existing transport.
- Read/write proxy paths for keys not owned by the receiving node.
- Hinted handoff with preserved ordering metadata, using the `handoff.push` kind.

Exit criteria:
- Multi-node convergence tests pass under failure/recovery scenarios.

## Pre-Milestone 3: CLI and PKI Bootstrap

Goal: provide operators with a production-ready CLI before the hardening phase
begins. This milestone is the prerequisite for all certificate lifecycle
automation planned in Milestone 3.

Deliverables:
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

Exit criteria:
- `tourillon --help` and all sub-command `--help` pages render correctly.
- An operator can bootstrap a CA, issue server and client certs, and start a
  node pointing to those certs in a single terminal session.
- Connections without valid mTLS certificates are still refused.
- `pytest --cov-fail-under=90` continues to pass.

## Milestone 3: Operations and Hardening

Goal: production-readiness baseline.

Deliverables:
- Full mTLS certificate lifecycle support (rotation, renewal, revocation).
- Observability, alerting, and runbooks.
- Upgrade and backup/restore workflows.

Exit criteria:
- Security, protocol, and recovery gates pass in staging.

## Milestone 4: Scale Validation

Goal: validate large-cluster behavior and SLO guardrails.

Deliverables:
- Capacity tests and hotspot/rebalance validation.
- Long-running stability and fault-injection campaigns.

Exit criteria:
- Agreed scalability and reliability thresholds are met.
