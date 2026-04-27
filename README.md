# Tourillon

## What is Tourillon?

Tourillon is a leaderless, peer-to-peer distributed key-value database focused on one promise: your data evolves predictably, even under failures, churn, and network instability.

You can send requests to any node, scale horizontally behind standard load balancers, and keep deterministic convergence across replicas.

## Why users choose Tourillon

- Predictable outcomes: replicas converge deterministically.
- No leader bottleneck: every node can serve requests.
- Built for real-world instability: reboots, network issues, and churn are first-class conditions.
- Secure by default: mandatory mutual TLS (mTLS) for all communications.
- Protocol clarity: binary-safe, self-describing messages for robust interoperability.

## Typical use cases

- Distributed metadata/state storage for platform services.
- Multi-node environments where deterministic conflict behavior matters.
- Infrastructure running behind standard load balancers with frequent scaling events.

## Core Guarantees

- Any node can serve client requests; no central leader is required.
- Data partitioning and replication are driven by a consistent-hashing ring.
- Per-key updates are append-only versions ordered deterministically.
- Concurrent writes can produce multiple valid versions returned on `GET`.
- Hinted handoff preserves update ordering when recovering from temporary failures.
- Join/leave events trigger deterministic rebalance operations.
- All communications use mandatory mutual TLS (mTLS).
- Protocol messages are binary-safe and self-describing.

## Current status

Tourillon is currently in a specification-first phase. The repository defines architecture, protocol, convergence, security, and operations contracts before implementation starts.

## Getting started

### Understanding the project

1. Read the project objective in `.github/prompts/objective.prompt.md`.
2. Read the planning contract in `.github/prompts/plan-tourillon.prompt.md`.
3. Follow the technical docs in `docs/` (architecture → convergence → protocol → operations).
4. Track implementation phases in `docs/roadmap.md`.

### Development setup

Tourillon uses [`uv`](https://docs.astral.sh/uv/) for environment and dependency management.

```bash
# Install uv (if not already installed)
pip install uv

# Install all dev dependencies
uv sync --extra dev

# Run the test suite
uv run pytest

# Run code quality checks (black + autoflake + misc hooks)
uv run pre-commit run --all-files

# Install pre-commit hooks into your local git checkout (one-time)
uv run pre-commit install
```

### Contributing and Copilot instructions

Development guidelines, code style rules, architecture invariants, and
GitHub Copilot repository instructions are defined in
[`.github/copilot-instructions.md`](.github/copilot-instructions.md).

### License

Distributed under the [Apache License 2.0](LICENSE).

## Documentation Map

- `.github/prompts/objective.prompt.md`: global functional objective.
- `.github/prompts/plan-tourillon.prompt.md`: planning generation contract.
- `docs/architecture.md`: system structure, ring behavior, read/write paths.
- `docs/convergence.md`: deterministic ordering and replica convergence.
- `docs/protocol.md`: transport, message envelope, operations, compatibility.
- `docs/security.md`: mTLS and trust-management requirements.
- `docs/operations.md`: deployment and runbooks.
- `docs/scalability.md`: capacity, rebalancing, large-cluster constraints.
- `docs/testing.md`: verification strategy and test matrix.
- `docs/roadmap.md`: milestone-based delivery path.

## Next milestone

- Implement Milestone 1: single-node core, protocol framing, and deterministic update log.
