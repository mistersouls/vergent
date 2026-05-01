# Node Startup

This document describes precisely what happens when a Tourillon process starts:
from the first line of `tourillon node start` through the moment the node
enters the `IDLE` phase and is ready to accept an explicit `join` command. It
is the authoritative reference for process startup, not for joining or serving
data.

Cross-references:
- `docs/lifecycle/node-join.md` — what happens after the `join` command
- `docs/lifecycle/node-restart.md` — how startup differs when a persisted phase is present
- `docs/architecture.md` — `TourillonConfig` and the dual-endpoint model
- `docs/security.md` — mTLS certificate requirements

---

## 1. Preconditions

Before `tourillon node start` can succeed, three independent artifacts must
exist on the machine running the command.

### 1.1 Configuration file

A valid TOML configuration file must be present at the path supplied with
`--config` (or at `~/.config/tourillon/config.toml` if the flag is omitted).
The file must contain, at minimum:

- `[node].id` — a stable, unique string identifier for this node.
- `[tls].cert_data`, `[tls].key_data`, `[tls].ca_data` — base64-encoded PEM
  material for the node's identity certificate, its private key, and the CA
  certificate used to verify peers.
- `[servers.kv].bind` — the `host:port` the KV listener will bind to.
- `[servers.peer].bind` — the `host:port` the peer listener will bind to.

If any mandatory field is absent, the startup fails with a `ConfigError` and
exit code 1 before any socket is touched.

### 1.2 TLS material

The base64-encoded material embedded in `[tls]` must pass these validations
before startup continues:

- The server certificate can be decoded to a valid X.509 DER structure.
- The certificate has not expired at the moment of startup.
- The private key matches the server certificate's public key.
- The CA certificate can be decoded and is a self-signed or intermediate CA.

Any failure here produces a `ConfigError` with an explicit reason. The node
does not bind any socket and exits with code 1.

### 1.3 Unique node identity

`[node].id` must be configured before startup. Tourillon does not generate a
node identifier at runtime. The identifier must be stable across restarts;
changing it on restart is equivalent to introducing a new node into the ring
and will cause data ownership confusion. See `docs/lifecycle/node-restart.md`
for the generation-counter implications of reuse under a different identity.

---

## 2. Startup Sequence

The startup sequence is strictly ordered. A failure at any step aborts the
process and releases any resources allocated in previous steps.

### Step 1 — Configuration resolution

`tourillon.bootstrap.config.load_config` applies the four-level precedence
chain (CLI flag → environment variable → config file → built-in default) and
produces a validated, fully resolved `TourillonConfig` instance. This function
performs only in-memory work; no socket is opened and no filesystem path other
than the config file itself is read at this stage.

### Step 2 — TLS context construction

Two independent `ssl.SSLContext` instances are constructed from the resolved
`TourillonConfig`:

- One for `servers.kv`, using the KV listener's trust anchor (falls back to
  `[tls].ca_data` if `[servers.kv].ca_data` is not set).
- One for `servers.peer`, using the peer listener's trust anchor (falls back
  to `[tls].ca_data` if `[servers.peer].ca_data` is not set).

Both contexts enforce mutual TLS (`ssl.CERT_REQUIRED`). There is no plaintext
fallback and no way to disable mTLS through configuration. If either context
cannot be constructed the startup aborts with exit code 1.

### Step 3 — Storage initialisation

The local storage backend is initialised. For the in-memory adapter this means
allocating the in-process data structures. The storage backend must be ready to
accept read and write operations before the listeners are bound; this ordering
prevents a race where an incoming connection attempts to write before the
storage is ready.

No persisted phase is consulted at this step. Phase resumption happens in
Step 4.

### Step 4 — Phase consultation

The bootstrap layer reads the persisted `MemberPhase` from the storage backend.
If no phase has been persisted (first startup), the phase is `IDLE`. If a phase
has been persisted, the startup branches into the restart flow described in
`docs/lifecycle/node-restart.md` rather than continuing the fresh-start
sequence below.

On a fresh startup, the node continues with `phase = IDLE`. Nothing is written
to storage yet at this step.

### Step 5 — TCP listeners binding

Two TCP listeners are bound in sequence, using the `ssl.SSLContext` instances
from Step 2:

- `servers.kv` listener (for client KV traffic).
- `servers.peer` listener (for inter-node gossip, replication, and `tourctl`
  operator traffic).

Because both listeners enforce mTLS, a connection attempt without a valid
client certificate is rejected at the TLS handshake layer before any Tourillon
application protocol byte is exchanged. The `Dispatcher` registered on each
listener serves only the envelope kinds appropriate to that plane; an envelope
routed to the wrong plane is closed without a response.

If either `bind` address is already in use or the OS refuses the bind call,
startup aborts with exit code 1.

### Step 6 — IDLE state announcement

The node persists `phase = IDLE` to the local storage backend. Persisting the
phase before entering the asyncio event loop guarantees that a crash at any
point after this step will be recovered as an `IDLE` restart, not as a
corrupted partial start.

No gossip is emitted in `IDLE`. The node does not announce itself to any peer
and does not contact any seed. It simply waits.

### Step 7 — Asyncio event loop entry

The process enters the asyncio event loop. The two TCP listeners are now
accepting connections. The node responds to health-check probes directed at
either listener but rejects data-plane operations (`put`, `get`, `delete`) with
`TEMPORARY_UNAVAILABLE` because it holds no partition ownership in `IDLE`.

---

## 3. What a Node in IDLE Can and Cannot Do

| Operation | IDLE behaviour |
|-----------|---------------|
| Bind KV listener and accept TLS connections | Yes |
| Bind peer listener and accept TLS connections | Yes |
| Respond to `get` or `put` | No — `TEMPORARY_UNAVAILABLE` |
| Respond to `delete` | No — `TEMPORARY_UNAVAILABLE` |
| Participate in gossip | No — not registered with any seed |
| Compute a preference list | No — no ring view available |
| Appear in any peer's ring view | No — not announced |
| Accept a `join` command | Yes — this is the only valid next step |

---

## 4. Failure and Abort Paths

The table below summarises every exit-code-1 condition that can occur during
startup before the event loop is entered.

| Condition | Abort step | Reason |
|-----------|-----------|--------|
| Config file missing or unreadable | Step 1 | No config to load |
| Mandatory field absent from config | Step 1 | `ConfigError` raised |
| TLS cert/key mismatch | Step 2 | Cannot construct SSL context |
| TLS cert expired at startup time | Step 2 | Certificate validation failure |
| `servers.kv` bind address already in use | Step 5 | OS-level `OSError` |
| `servers.peer` bind address already in use | Step 5 | OS-level `OSError` |
| `servers.kv` and `servers.peer` bind to the same address | Step 5 | Second bind fails |
| Storage initialisation error | Step 3 | Backend unavailable |

All abort paths emit a structured error log entry (not a raw stack trace) and
exit with code 1. The operator is expected to resolve the root cause before
retrying.

---

## 5. Observable Signals at Startup

Once Step 5 succeeds and the event loop is entered, the following signals
indicate a healthy `IDLE` node:

- The KV listener accepts new TCP connections (TLS handshake completes for
  clients presenting a valid certificate).
- The peer listener accepts new TCP connections.
- A health-probe envelope sent to either listener receives an appropriate
  response.

The node's `phase` field in any gossip record it eventually emits once it joins
will be `IDLE` with `generation` set to the value read from storage (or 1 on
first start).
