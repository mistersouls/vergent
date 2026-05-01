# Testing Strategy

This document defines the test strategy for Tourillon, organised by node
lifecycle phase. Each section describes what must be validated, identifies the
appropriate test fixture (in-memory unless otherwise noted), and provides the
expected observable for every scenario. Test names follow the convention
`test_<phase>_<condition>_<expected>`.

All tests use the in-memory storage adapter and — where distributed behaviour
is required — an in-memory gossip transport that avoids real sockets. mTLS is
exercised only in the integration and security test tiers, not in unit tests.
This constraint keeps unit tests fast and deterministic while still providing
end-to-end coverage of the TLS path in the integration tier.

---

## How to Read This Document

Each section corresponds to one lifecycle phase or cross-phase concern. Within
each section, scenarios are listed with:

- **Fixture** — what the test constructs (e.g., single in-memory node,
  two-node in-memory cluster).
- **Action** — what the test does.
- **Expected** — the observable outcome that constitutes a pass.

---

## Phase 0 — Provisioning

These tests validate configuration loading and TLS material validation before
any socket is bound.

### Scenario: valid config and certificates produce `TourillonConfig`

**Fixture:** a `TourillonConfig` constructed programmatically with valid
inline base64-encoded PEM material (self-signed CA, matching leaf cert and
key).

**Action:** call `load_config` with the programmatic config.

**Expected:** `TourillonConfig` is returned without error; all fields carry
their expected values.

### Scenario: expired TLS certificate raises `ConfigError` before binding

**Fixture:** a config with a certificate whose `not_after` date is in the
past.

**Action:** call `load_config`.

**Expected:** `ConfigError` is raised with a message referencing certificate
expiry; no socket is bound.

### Scenario: cert/key mismatch raises `ConfigError`

**Fixture:** a config where `cert_data` is signed by one CA but `key_data`
belongs to a different key pair.

**Action:** call `load_config` or attempt TLS context construction.

**Expected:** `ConfigError` is raised indicating a cert/key mismatch.

### Scenario: missing mandatory field raises `ConfigError`

**Fixture:** a config dict missing `[node].id`.

**Action:** call `load_config`.

**Expected:** `ConfigError` is raised referencing the missing field name.

### Scenario: duplicate bind addresses are rejected

**Fixture:** a config where `servers.kv.bind` and `servers.peer.bind` are
set to the same `host:port`.

**Action:** attempt node startup.

**Expected:** startup fails with exit code 1 before accepting any connection.

---

## Phase 1 — Startup (IDLE)

These tests validate the startup sequence up to the `IDLE` state.

### Scenario: fresh-start node enters IDLE

**Fixture:** in-memory node with valid config and no persisted phase.

**Action:** start the node.

**Expected:** `node.phase == MemberPhase.IDLE`; no gossip emitted; no ring
view present.

### Scenario: IDLE node rejects KV operations

**Fixture:** in-memory node in `IDLE`.

**Action:** send a `put` envelope to the KV listener.

**Expected:** response carries `TEMPORARY_UNAVAILABLE`; no data written to
storage.

### Scenario: IDLE node rejects gossip-plane operations

**Fixture:** in-memory node in `IDLE`.

**Action:** send a `replicate` envelope to the peer listener.

**Expected:** connection is closed or response carries `TEMPORARY_UNAVAILABLE`;
no ring update applied.

---

## Phase 2 — Join (IDLE → JOINING → READY)

### Scenario: first-node bootstrap — no seeds

**Fixture:** single in-memory node with no configured seeds.

**Action:** issue `join` command.

**Expected:**
- Node briefly enters `JOINING`, then transitions to `READY` immediately
  (no partitions to receive from other nodes).
- Ring epoch is 1.
- Node's vnodes are present in the ring.
- `node.phase == MemberPhase.READY`.

### Scenario: second node joins a single-node cluster

**Fixture:** two in-memory nodes A and B sharing an in-memory gossip transport.
A is already `READY`. B is configured with A's address as a seed.

**Action:** issue `join` command on B.

**Expected:**
- B enters `JOINING`.
- B receives the ring view from A (epoch 1).
- Ring epoch advances to 2 after B's vnodes are added.
- B receives partition data for its assigned primary slots via
  `rebalance.transfer`.
- On commit, B transitions to `READY`.
- `B.phase == MemberPhase.READY`.
- Both A and B compute the same preference list for any `StoreKey`.

### Scenario: join command rejected if node is already JOINING

**Fixture:** in-memory node in `JOINING`.

**Action:** issue a second `join` command.

**Expected:** error `ALREADY_JOINING`; `node.phase` unchanged; no new gossip
emitted.

### Scenario: join command rejected if no seeds reachable within timeout

**Fixture:** in-memory node with seeds configured to unreachable addresses.

**Action:** issue `join` command with a short timeout.

**Expected:** after the timeout the join is suspended; `node.phase` is still
`JOINING` (the intent is preserved); a structured log entry records the
failure.

### Scenario: JOINING node is absent from preference lists

**Fixture:** three in-memory nodes. A and C are `READY`. B is `JOINING`.
Replication factor `rf = 2`.

**Action:** compute `preference_list` for a `StoreKey` on both A and C.

**Expected:** neither A's nor C's preference list contains B; the list
contains only `READY` and/or `DRAINING` nodes.

---

## Phase 3 — Ready

### Scenario: write acknowledged by quorum is readable from all replicas

**Fixture:** three in-memory nodes (A, B, C), all `READY`, `rf = 3`.

**Action:** `put(key="k", value="v")` routed to A; wait for quorum ack.

**Expected:** `get(key="k")` returns `"v"` from A, B, and C independently.

### Scenario: deterministic preference list on all nodes

**Fixture:** three in-memory nodes all `READY` with the same ring view.

**Action:** compute `preference_list` for the same `StoreKey` on each node.

**Expected:** all three nodes return the same ordered list.

### Scenario: write routed to non-owner node is proxied

**Fixture:** two in-memory nodes A and B both `READY`. A owns partition P;
B does not own partition P.

**Action:** `put` for a key that maps to partition P, sent to B.

**Expected:** B proxies the write to A; A stores the value; B returns the
ack to the caller.

---

## Phase 4 — Restart Semantics

These tests exercise the restart behaviour for each persisted phase. All tests
use an in-memory backend that survives a node restart within the same process.

### Scenario: restart from IDLE — stays IDLE

**Fixture:** in-memory node; persisted phase = `IDLE`.

**Action:** stop and restart the node.

**Expected:** `node.phase == MemberPhase.IDLE` after restart; no gossip
emitted; no `generation` change.

### Scenario: restart from JOINING — resumes join

**Fixture:** two in-memory nodes A and B. B has been told to join A but
has not yet completed. B's persisted phase = `JOINING`. Simulate a crash
by stopping B mid-transfer.

**Action:** restart B.

**Expected:**
- B re-contacts A.
- B resumes partition transfers from the last committed checkpoint.
- B eventually reaches `READY`.
- `generation` is the same as before the crash.

### Scenario: restart from READY — immediately serves traffic

**Fixture:** in-memory node; persisted phase = `READY` with a valid ring view
stored.

**Action:** stop and restart the node.

**Expected:**
- `node.phase == MemberPhase.READY` immediately after restart.
- `node.seq` is 1 higher than before the restart.
- `node.generation` is unchanged.
- The node accepts `put` and `get` requests immediately.

### Scenario: restart from READY — `generation` is not incremented

**Fixture:** in-memory node; persisted `generation = 3`.

**Action:** restart the node.

**Expected:** after restart `generation` is still 3; the gossip record
emitted carries `generation = 3` and `seq` incremented by 1.

### Scenario: restart from DRAINING — drain resumes

**Fixture:** two in-memory nodes A (draining) and B (target). A has transferred
partitions P1 and P2 but crashed before transferring P3.

**Action:** restart A.

**Expected:**
- P1 and P2 are not re-transferred.
- P3 transfer resumes from the last per-batch checkpoint.
- A completes the drain and transitions to `IDLE`.

---

## Phase 5 — Drain (READY → DRAINING → IDLE)

### Scenario: happy-path drain

**Fixture:** three in-memory nodes A, B, C all `READY`, `rf = 3`.

**Action:** issue `leave` on A.

**Expected:**
- `A.phase == MemberPhase.DRAINING`.
- A returns `TEMPORARY_UNAVAILABLE` for primary writes on its partitions.
- A still serves reads.
- Partition data is transferred to B and C.
- `rebalance.commit` is issued by B and C for all partitions.
- A broadcasts a leave notice.
- `A.phase == MemberPhase.IDLE`.
- Ring epoch advances once.

### Scenario: drain cannot be cancelled

**Fixture:** in-memory node in `DRAINING`.

**Action:** issue a `join` command or a second `leave` command.

**Expected:** `INVALID_PHASE` or `ALREADY_DRAINING` error; `A.phase`
unchanged.

### Scenario: DRAINING node remains in preference list for reads

**Fixture:** two in-memory nodes A (draining) and B (READY), `rf = 2`.

**Action:** compute `preference_list` for a `StoreKey`.

**Expected:** A is present in the preference list; `get(key)` returns a valid
response served by A.

### Scenario: DRAINING node returns `TEMPORARY_UNAVAILABLE` for primary writes

**Fixture:** as above.

**Action:** send a `put` intended for A as primary coordinator.

**Expected:** A returns `TEMPORARY_UNAVAILABLE`; the coordinator retries
against B.

---

## Phase 6 — Convergence and Ordering

### Scenario: concurrent writes produce multiple visible versions

**Fixture:** two in-memory nodes A and B, same HLC timestamp, both writing
different values for the same key simultaneously.

**Action:** write `v1` on A and `v2` on B at the same logical time but with
different `node_id`.

**Expected:** `get(key)` returns both `v1` and `v2`; neither is silently
discarded; the HLC tiebreak (by `node_id`) picks a deterministic winner for
single-value APIs but both are present in the version stream.

### Scenario: idempotent apply — same `Version` applied twice

**Fixture:** in-memory node.

**Action:** apply the same `Version` to local storage twice (e.g., via a
duplicate `replicate` message).

**Expected:** the key's version history contains the entry exactly once; no
duplicate in the log.

### Scenario: out-of-order delivery converges to correct state

**Fixture:** in-memory node with an empty key.

**Action:** apply version V2 (HLC = T2) followed by version V1 (HLC = T1 < T2).

**Expected:** after both versions are applied the log contains V1 and V2 in
HLC order; the winning visible value is V2.

### Scenario: tombstone is preserved through replication

**Fixture:** two in-memory nodes A and B.

**Action:** `delete(key)` on A; replicate to B.

**Expected:** `get(key)` on B returns a tombstone marker (or "not found"
depending on the API contract), not the last live value.

---

## Phase 7 — Local Failure Detection

### Scenario: failed operation marks peer as SUSPECT

**Fixture:** two in-memory nodes A and B. B's transport is set to return a
connection error.

**Action:** A attempts to send a `replicate` to B; the call fails.

**Expected:** B's local reachability state on A is `SUSPECT`; the gossip
record A emits to other peers does not include any reachability field for B.

### Scenario: successful operation after failure resets to REACHABLE

**Fixture:** as above; B's transport is restored to working state.

**Action:** A successfully sends a probe to B.

**Expected:** B's local reachability on A transitions back to `REACHABLE`; no
gossip event is emitted related to this recovery; `B.phase` in A's gossip
table is unchanged.

### Scenario: DEAD peer activates hinted handoff

**Fixture:** three in-memory nodes A (primary), B (replica), C (replica). C
is classified as `DEAD` on A.

**Action:** `put(key)` routed to A with `rf = 3`.

**Expected:** A stores a hint for C in its handoff queue; A records the write
with the original HLC metadata; the write is acknowledged once A and B (quorum
of 2) confirm.

### Scenario: hinted writes replayed in HLC order on recovery

**Fixture:** A holds three hints for C: writes at HLC T1, T3, and T2 (queued
out of order due to arrival timing).

**Action:** C transitions back to `REACHABLE`; A delivers the hinted handoff
queue.

**Expected:** C receives the writes in HLC order (T1, T2, T3); C's final state
matches what it would have if none of the writes had been hinted.

---

## Phase 8 — Ring Invariants

### Scenario: deterministic placement across nodes

**Fixture:** three in-memory nodes with the same `HashSpace` and `Partitioner`
configuration (e.g., `bits=8`, `partition_shift=4`).

**Action:** compute `placement_for_token(t, ring)` for 100 distinct token
values on each node.

**Expected:** all three nodes return identical `PartitionPlacement` results for
every token.

### Scenario: small ring (`bits=8`) preserves all structural invariants

**Fixture:** ring with `bits=8`, `partition_shift=4` (16 partitions over 256
positions), three vnodes per node.

**Action:** add a node; remove a node; compute preference lists before and
after each operation.

**Expected:**
- After add: ring epoch advances; preference lists for most tokens are stable;
  only tokens in the ownership delta change assignment.
- After remove: same stability guarantee.
- Preference lists always contain distinct physical nodes.

### Scenario: preference-list walk respects phase eligibility

**Fixture:** five in-memory nodes. Phase distribution: A=READY, B=JOINING,
C=READY, D=DRAINING, E=IDLE. `rf = 3`.

**Action:** compute `preference_list` for a key whose ring walk visits all
five nodes.

**Expected:** list contains A, C, D in walk order; B and E are excluded.

---

## Security and Protocol Tests

These tests require the integration tier (real `ssl.SSLContext`).

### Scenario: connection without client certificate is refused

**Fixture:** a running node bound on `servers.kv` with mTLS enforced.

**Action:** open a TLS connection without presenting a client certificate.

**Expected:** the TLS handshake fails; no application-level response is
returned; the connection is closed.

### Scenario: `Envelope` with unknown `kind` closes the connection

**Fixture:** in-memory dispatcher with no handler registered for `"unknown"`.

**Action:** send an `Envelope` with `kind = "unknown"`.

**Expected:** the connection is closed without a response; no error log entry
blames the application layer.

### Scenario: `Envelope` with `kind` longer than 64 bytes raises `ValueError`

**Fixture:** in-memory `Envelope` construction.

**Action:** construct an `Envelope` with a `kind` whose UTF-8 encoding is 65
bytes.

**Expected:** `ValueError` is raised at construction time.

---

## Test Infrastructure Requirements

- All tests in phases 0–7 MUST be executable without real network sockets.
  Use in-memory adapters for storage and for node-to-node transport.
- Each test file MUST carry the Apache 2.0 license header.
- Test names MUST follow `test_<phase>_<condition>_<expected>`.
- Lifecycle phase MAY be annotated with `@pytest.mark.<phase>` for selective
  runs (`pytest -m phase2_join`).
- Property-based tests using Hypothesis SHOULD cover §Phase 6 (convergence)
  and §Phase 8 (ring invariants) to assert that properties hold for arbitrary
  sequences of operations.
- Coverage gate: `uv run pytest --cov-fail-under=90`.
