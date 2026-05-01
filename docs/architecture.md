# Architecture

## System Context and Goals

Tourillon targets predictable behavior in large, failure-prone environments.
The architecture combines leaderless request handling with deterministic per-key update
evolution so any node can safely serve traffic behind standard load balancers.

## Keyspace Model

A keyspace is a client-supplied logical scope that partitions the key namespace,
analogous to namespaces in Kubernetes or keyspaces in Cassandra. Every operation
in Tourillon is expressed against a specific keyspace: there is no "default" or
implicit keyspace. Callers provide the keyspace alongside the key for every read,
write, and delete.

Keyspaces serve two roles. At the application level they provide logical isolation
between datasets so that keys with identical names in different keyspaces never
collide. At the infrastructure level the keyspace participates, together with the
key, in computing the consistent-hash token that determines ring ownership and
replica placement.

### StoreKey — the canonical addressing unit

The pair `(keyspace, key)` is represented as a single immutable value object named
`StoreKey`. Every storage operation, every `Version`, every `Tombstone`, and every
log entry carries a `StoreKey` as its address. Passing a `StoreKey` rather than two
loose string arguments provides a stable, typed unit of identity that can be passed
across layer boundaries without caller confusion about argument ordering.

## Storage Command Objects

The `LocalStoragePort` methods each accept a dedicated command object rather than
a flat list of parameters. The three operations are `WriteOp`, `ReadOp`, and
`DeleteOp`. Each carries a `StoreKey` address plus the arguments specific to
the operation: `value` and `now_ms` for a write, `now_ms` only for a deletion, and
the address alone for a read.

This design means that extending an operation — for example by adding a consistency
level, a TTL, or a partition identifier — requires adding a field to the relevant
command dataclass rather than changing the method signature. Callers are decoupled
from future additions by construction.

## Partition Reference

A partition token identifies one slice of the consistent-hash ring. Within the
storage layer the partition is a routing and grouping concern, not a value property.
`Version` and `Tombstone` do not carry a partition token because the partition is
a function of the `StoreKey` and the current ring state. It can always be recomputed
and must not be persisted as part of the causal record.

The router layer resolves `StoreKey → partition token → replica set` before
calling the storage port. The storage implementation may organise its internal
structures by partition for rebalance efficiency, but this organisation is opaque
to callers of `LocalStoragePort`.

### Adding partition_id in a future milestone

When the ring layer is introduced (Milestone 2), the router will have already
resolved the partition token before calling the storage port. At that point,
`WriteOp` and `DeleteOp` can each gain an optional `partition_id` field
that the router pre-fills. The storage implementation can use that field to
organise data by partition — which makes partition transfer during rebalance
cheaper — without any change to the `LocalStoragePort` method signatures.
`ReadOp` does not need a `partition_id` field because reads follow the same
`StoreKey`-based lookup path regardless of which partition owns the key.

This evolution path is safe precisely because of the command object design: adding
a new field to a frozen dataclass is backwards-compatible for all existing callers
that construct the object, and the storage port method signatures are unchanged.

## Node Responsibilities

Each node MUST:
- Serve read and write requests for keys it owns or proxies.
- Participate in membership and ring-state dissemination.
- Replicate updates to responsible peers.
- Persist local state and update metadata needed for deterministic replay.

Each node SHOULD:
- Expose health and readiness signals for load balancers.
- Apply bounded backpressure under overload.

## CLI Layer

The `tourillon/infra/cli/` package is a **primary (driving) adapter** that lives
in the `infra/` adapters layer alongside secondary adapters such as `memory/` and
`pki/`. Grouping all adapters under `infra/` means adding a future delivery
mechanism — `infra/api/` or `infra/grpc/` — follows the same pattern without
touching `core/` or `bootstrap/`. The CLI does not participate in the distributed
data path and takes no part in replication, ring management, or ordering.

The CLI reaches inward only through two bootstrap factory entry points.
`tourillon.bootstrap.node.create_tcp_node` is the single assembly factory that
wires storage, dispatcher, TLS context, and TCP server together; the CLI calls it
to start a node and otherwise has no dependency on any core domain object. PKI
operations are routed through `tourillon.bootstrap.pki.create_pki_adapter`, which
constructs the concrete PKI implementation and returns it typed as the
`CertificateAuthorityPort` and `CertificateIssuerPort` protocols defined in
`tourillon.core.ports.pki`; the CLI uses the port-defined request and response
types (`CaRequest`, `CertRequest`, `PkiError`) and never imports `tourillon.infra`
directly. The `cryptography` package is a mandatory Tourillon runtime dependency —
no optional extra is required.

This boundary means the CLI can be tested in full isolation using `typer.testing.CliRunner`
and temporary directory fixtures, with no live sockets or TLS handshakes required.

The CLI must never import core domain objects (clocks, envelopes, versions,
log entries) directly. Any error originating in the core or infra layers is
surfaced to the operator as a readable Rich error panel with an appropriate exit
code; raw stack traces are never exposed.

A thin `NodeRunner` shim in `tourillon/infra/cli/_runner.py` wraps the asyncio event
loop and installs OS signal handlers for SIGINT and SIGTERM so that
`tourillon node start` terminates cleanly on both POSIX and Windows.

Future operational commands (ring inspect, log tail, cert rotate) follow the
same pattern: they are registered on the root Typer application and delegate to
existing port contracts or new infra adapters, never importing core internals.

## Configuration Layer

Tourillon resolves its runtime parameters through a four-level precedence chain:
CLI flag > environment variable > config file > built-in default. A value
supplied at a higher precedence level always shadows the same value at a lower
level. This allows operators to ship a base config file per node and still
override individual settings for a single invocation without editing the file.

### Config file format and location

The config file is a TOML document parsed at startup using the Python standard
library `tomllib` module (read-only, Python 3.11+). The file is read once,
validated, and then discarded; it is never written or modified at runtime.
If the file changes on disk while a node is running the changes have no effect
until the process is restarted.

**Why TOML and not YAML.** `tomllib` is stdlib; YAML would require `PyYAML` or
`ruamel.yaml` as a third-party dependency. TOML's strict native typing (integers,
booleans, arrays) maps directly onto the typed `TourillonConfig` dataclass and
eliminates an entire class of implicit-coercion bugs that YAML introduces (the
Norway problem, `yes`/`no`/`on`/`off` booleans, octal literals). Both formats
support inline comments and multi-line arrays; neither offers a meaningful
authoring advantage for this use case. Writing TOML for `config generate` is done
with `tomli-w`, a small well-maintained library with no transitive dependencies;
this is the only additional runtime dependency the config system introduces. YAML's
single advantage — familiarity to Kubernetes operators — does not outweigh these
costs, given the project philosophy of preferring stdlib over third-party
dependencies unless there is a meaningful complexity reduction.

The default location is `~/.config/tourillon/config.toml`. An alternate path
is accepted via the `--config` CLI flag. The `tourillon config generate` command
issues a certificate on the fly and writes a fully self-contained config to the
path supplied with `--out`.

### TourillonConfig — canonical in-memory representation

`tourillon/core/config.py` defines `TourillonConfig`, a frozen dataclass that is
the single authoritative in-memory representation of all resolved configuration
values. It is constructed by `tourillon.bootstrap.config.load_config`, which
applies the precedence chain, validates every field, and raises a structured
`ConfigError` on any violation. No other module reads the config file or
environment variables directly; every subsystem receives a `TourillonConfig`
instance injected through its constructor.

`TourillonConfig` is a pure data object with no dependency on any infra adapter.
It MUST be constructed and validated before any socket is bound, any file is
opened, or any TLS context is created. The `[tls]` section is represented by
`cert_data`, `key_data`, and `ca_data` fields containing base64-encoded PEM
material; no `*file` path fields exist in the config schema.

### Dual-endpoint model

Each Tourillon node MUST expose two distinct TCP listeners, configured
independently in the `[servers]` section of the config file:

- **`servers.kv`** — serves client data-plane traffic: `put`, `get`, and
  `delete` operations. This is the address application clients and load
  balancers connect to. The mTLS context for this listener MUST trust a client
  CA that is authoritative for application clients, which MAY differ from the
  peer CA.

- **`servers.peer`** — serves both inter-node traffic (replication, gossip,
  hinted handoff) and operator client connections from `tourctl`. This listener
  SHOULD be bound to an interface reachable by cluster peers and operators but
  not necessarily exposed to the public data plane. Its mTLS context SHOULD
  trust a separate peer/operator CA, allowing firewall rules and certificate
  policies to enforce strict separation between data-plane and peer-plane
  clients.

Each endpoint is described by a `bind` address (the `host:port` the OS socket
listens on) and an `advertise` address (the `host:port` gossiped to peers and
returned to clients for routing). These differ when the node runs behind NAT, a
load balancer, or a container port mapping; `advertise` defaults to `bind` when
omitted.

Keeping the two listeners independent means the node can enforce separate mTLS
trust anchors, different client certificate requirements, and distinct network
policies per plane without any shared state between the two SSL contexts. Both
listeners use the same server certificate and private key (the node's identity)
but MUST configure separate `ssl.SSLContext` instances.

### tourctl contexts

`tourctl` — the operator client binary — stores named cluster connections in
`~/.config/tourillon/contexts.toml`. Each context entry is composed of:

- A **cluster** block: a logical cluster name and the CA certificate used to
  verify the cluster's nodes.
- An **endpoints** block: an optional `kv` field (`host:port`) pointing to the
  node's `servers.kv` listener, and an optional `peer` field (`host:port`)
  pointing to the node's `servers.peer` listener. At least one must be present.
  A context exposing only `kv` is valid for `tourctl kv` commands; a context
  exposing only `peer` is valid for operator commands such as `tourctl ring
  inspect` and `tourctl log tail`.
- A **credentials** block: the client certificate and private key that `tourctl`
  presents when connecting to either endpoint.

The file also stores a single `current-context` key naming the active context.
When present, `tourctl` uses that context for all connections without requiring
explicit `--context` flags. `tourillon config generate-context` issues a client
certificate signed by the supplied CA and writes a fully self-contained context
entry; `tourctl config use-context` updates `current-context` and
`tourctl config list` displays all available contexts with the active one marked.
`tourctl` never issues certificates or writes PKI material — it only reads and
navigates the contexts file. These files are never modified by a running
`tourillon` node process.

#### Certificate storage: inline base64

All Tourillon config files — both `config.toml` and `contexts.toml` — store
certificate and key material as **inline base64-encoded PEM** exclusively. There
are no `*file` path variants. This design eliminates the dual-representation
complexity and makes every config file fully self-contained.

Both files are written at mode `0600` because both embed private key material.
A single `write_config_file(path, payload, mode=0o600)` utility in the bootstrap
layer is the only code path allowed to write either file; callers never invoke
`os.chmod` directly.

`tourillon config generate` and `tourillon config generate-context` both invoke
the CA/issuer port to sign a new certificate before writing the config. The
operator only ever supplies `--ca-cert` and `--ca-key`; they are never asked to
provide a pre-existing leaf certificate. `tourctl` never touches PKI material.

`tourillon config generate-context` MUST write to a temporary file and atomically
replace the existing contexts file with `os.replace` so a crash mid-write never
corrupts the active context list.

## Transport Layer

Tourillon uses custom binary transport built on asyncio streams with mandatory
mutual TLS. The transport is introduced alongside the single-node storage core so
that end-to-end connectivity can be validated before replication logic is added.

Each accepted connection is handed to a `ConnectionHandler`, an async callable that
receives two injected coroutines: `receive()`, which suspends until the next `Envelope`
arrives from the peer, and `send(envelope)`, which writes a framed `Envelope` to the
wire. The handler owns the application logic for the lifetime of that connection and
never touches framing, serialisation, or TLS directly.

A `Dispatcher` routes incoming envelopes to the handler registered for their
`kind`. Handlers are registered at startup and are never modified at runtime.
The transport layer closes the connection immediately for any frame whose `kind`
has no registered handler, without returning an application-level response.

The server component wraps `asyncio.start_server` with an SSL context that enforces
mutual TLS. There is no cleartext fallback. Connections that fail certificate
validation are closed immediately without returning any application-level error.

## Membership Model

Tourillon maintains cluster membership through two orthogonal FSMs that live on
every node. Neither FSM requires a central coordinator.

### MemberPhase — self-declared operational state

`MemberPhase` is what a node declares about its own operational readiness. It is
propagated to peers via gossip and persisted locally so that a restarted node
resumes the phase it held before the crash without waiting for an operator
command.

```
IDLE ──[join cmd]──► JOINING ──[bootstrap done]──► READY ──[leave cmd]──► DRAINING
 ▲                                                                              │
 └──────────────────────────────[drain done]─────────────────────────────────────┘
```

- **IDLE** — process is running, holds no partition responsibility, and has not
  been commanded to join. This is the initial state on first startup and the
  state the node returns to after a successful drain.
- **JOINING** — an explicit `join` command was issued; the node is bootstrapping
  (contacting seeds, receiving the current `RingView`, loading its assigned
  partitions). The node re-enters this phase automatically on restart if its
  persisted phase was `JOINING`.
- **READY** — fully operational; the node accepts reads, writes, and replication
  traffic for its assigned partitions. On restart from a persisted `READY` phase
  the node resumes `READY` immediately and re-announces itself to seeds.
- **DRAINING** — an explicit `leave` command was issued; the node is transferring
  its partitions to other replicas and refuses new writes for those partitions.
  On restart the drain resumes from where it stopped. `DRAINING` is
  irreversible: there is no cancel.  When all partition transfers finish the node
  broadcasts a leave notice and returns to `IDLE`.

Phase transitions happen **only when operations complete**, never solely because
the process restarted.

### Local failure detection — operation-driven, never global

Tourillon does not maintain a separate heartbeat loop to detect failures. A peer
is considered suspect when a local operation directed at it fails (write,
replicate, probe). The suspicion is a **local observation** on the node that
experienced the failure. Two nodes may simultaneously hold different views of
the same peer's reachability — this is normal and expected.

The local reachability state (`REACHABLE / SUSPECT / DEAD`) is maintained
internally by the gossip engine and is **never propagated** to other nodes.
Propagating it would conflate a local observation with a global decision and
would violate the leaderless invariant.

`MemberPhase` alone travels on the wire. Remote nodes learn that a peer has
departed only when that peer broadcasts its own phase transition (DRAINING,
then the final leave notice). Crashes are detected locally by each observer
independently, through operation failures.

## Data Partitioning and Replication Ring

The normative specification for the ring subsystem is `docs/ring.md`. This
section summarises the layered model and the invariants that the rest of the
architecture depends on.

Tourillon maps every `StoreKey` to a deterministic, ordered set of replica
nodes through three independent abstractions:

**Hash space.** A configurable-width circular integer domain `[0, 2**bits)`
into which keys and node identifiers are projected via a deterministic hash
function. The production value is `bits=128`; smaller values (e.g. `bits=8`)
are used in tests to reduce the combinatorial search space while preserving
all structural ring properties. All position arithmetic is modular; interval
membership uses the half-open convention `(a, b]`.

**Virtual-node ring.** Each physical node is assigned multiple virtual nodes
virtual nodes (vnodes), each identified by `(str, Token)` where `Token ∈ [0, 2**bits)`.
The ring is an immutable sorted sequence of vnodes. The successor of any
token is found in O(log n) via `bisect_right`. All ring mutations return a
new ring instance; no in-place modification is ever performed. A larger vnode
count per physical node distributes load more evenly and reduces the data
volume moved per topology event. For a cluster planned to scale to 10 000
nodes, `partition_shift=17` (131 072 logical partitions) and an appropriate
vnode count per node must be set at bootstrap and cannot be changed without
a full data migration.

**Logical partition grid.** The hash space is divided into `2**partition_shift`
equal, static partitions. These partitions are independent of vnodes: the
same `PartitionId` always covers the same segment of the hash space regardless
of how the ring topology changes. A `LogicalPartition(pid, start, end)` is
the unit of ownership in the ring view and the unit of transfer in the rebalance
protocol. `PartitionPlacement(partition, vnode)` ties a `LogicalPartition` to its
current ring owner; `PartitionPlacement.address` is the stable storage key prefix
used to group records belonging to the same partition.

**Preference list.** `Partitioner.placement_for_token(token, ring)` resolves a
token directly to a `PartitionPlacement` — the ephemeral binding of a
`LogicalPartition` to its current ring owner. Starting from `placement.vnode`,
the placement strategy walks the ring clockwise, collecting distinct physical
node identifier strings until `rf` eligible nodes have been found. Eligibility is
determined by `MemberPhase`: `READY` and `DRAINING` nodes are included; `IDLE`
and `JOINING` nodes are excluded. The preference list is a pure function of
`(placement, ring)` and produces an identical result on every node for the same
inputs. `PlacementStrategy` is a Protocol, allowing future implementations to add
topology-label-based anti-affinity without changing any call site.

The replication factor `rf` is a configurable cluster-wide parameter,
defaulting to 3, validated at startup before any partition assignment takes
place. Deterministic ownership — the guarantee that any two nodes compute
identical replica sets for a given key at a given ring epoch — is the
foundation that prevents split-brain interpretation of data responsibility.

## Rebalance Model

- Rebalance MUST be triggered on ring membership changes and topology updates.
- Rebalance MUST move only partitions impacted by ownership deltas.
- Source and destination nodes MUST preserve per-key ordering metadata during transfer.
- Rebalance SHOULD be rate-limited to avoid saturating foreground read/write traffic.
- Rebalance progress MUST be observable (queued partitions, transferred bytes, lag).
- Rebalance completion MUST be validated before retiring previous owners.

Rationale: bounded and observable rebalancing preserves availability while keeping deterministic convergence guarantees.

## Read/Write Flows

Write flow (logical):
1. Client sends a `WriteOp(StoreKey, value, now_ms)` to any node over mTLS.
2. Receiving node resolves `StoreKey → partition token → replica set` via the ring.
3. Node ticks the HLC clock to obtain deterministic ordering metadata.
4. Node records the resulting `Version` in its local log and replicates to peers.
5. Node returns an ack based on the configured write consistency.

Read flow (logical):
1. Client sends a `ReadOp(StoreKey)` to any node over mTLS.
2. Receiving node resolves the owner set for the `StoreKey`.
3. Node fetches local and remote candidate states.
4. Node returns the value selected by the deterministic HLC ordering rule.

## Failure and Recovery Model

- Temporary peer failure MUST trigger hinted handoff buffering.
- Recovered peers MUST receive missed updates in deterministic order.
- Recovery replay MUST be idempotent.
- Node restart MUST rebuild in-memory indexes from durable log/state before serving writes.
- Recovery-driven ownership changes MUST reuse the same deterministic rebalance rules.

## Architectural Invariants

- No central leader or single coordinator is required for normal operation.
- For a fixed ring version and key history, all healthy replicas MUST derive the same visible state.
- Transport security is mandatory and described in `docs/security.md`.
- Message contract and compatibility are defined in `docs/protocol.md`.
