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

## Milestone 1: Single-node Core

Goal: implement local storage engine and deterministic update model.

Deliverables:
- Per-key deterministic ordering metadata generation.
- Durable local log/state with idempotent replay.
- Initial protocol envelope and local request handling.

Exit criteria:
- Determinism tests pass for single-node workflows.

## Milestone 2: Multi-node Replication

Goal: implement leaderless replication over consistent-hashing ring.

Deliverables:
- Membership and ring ownership logic.
- Replication flows and read/write proxy paths.
- Hinted handoff with preserved ordering metadata.

Exit criteria:
- Multi-node convergence tests pass under failure/recovery scenarios.

## Milestone 3: Operations and Hardening

Goal: production-readiness baseline.

Deliverables:
- Full mTLS certificate lifecycle support.
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
