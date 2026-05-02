---
mode: agent
description: Implement or extend consistent-hashing ring logic (HashSpace, VNode, Ring, Partitioner, PlacementStrategy) following proposal 002.
---

# Ring implementation task

Task: **${input:task}**
(e.g. "Implement HashSpace and VNode", "Add Ring.iter_from", "Implement SimplePreferenceStrategy")

## Mandatory pre-reading

Read **`proposals/proposal-node-join-05022026-002.md`** §2.1–§2.5.

Key facts to absorb before writing any code:

| Concept | Rule |
|---------|------|
| `HashSpace` | MD5 truncated to `bits` significant bits. Default `bits=128`. Test mode `bits=8`. Never a global singleton. |
| `VNode` | `frozen dataclass(node_id, token)`. Token derived deterministically: `hash(f"{node_id}:{index}")`. |
| `Ring` | **Immutable** sorted sequence of `VNode` by ascending token. `add_vnodes` / `drop_nodes` return new instances. |
| `successor(token)` | `bisect_right` on token values; wraps to index 0 at end. O(log n). |
| `Partitioner` | Static `2**partition_shift` grid. `pid = h >> (bits - partition_shift)`. Fixed for cluster lifetime. |
| `PartitionPlacement` | Ephemeral — never persisted. Recompute after every ring mutation. |
| `SimplePreferenceStrategy` | Clockwise walk; deduplicate by `node_id`; skip ineligible phases; stop at `rf` entries. Pure function. |

## Phase eligibility table (for PlacementStrategy)

| Phase | Eligible for preference list? |
|-------|-------------------------------|
| `IDLE` | No |
| `JOINING` | No |
| `READY` | Yes |
| `PAUSED` | No |
| `DRAINING` | Yes (reads only; write coordinators fall through) |
| `FAILED` | No |

## File targets

```
tourillon/core/ring/hashspace.py      ← HashSpace
tourillon/core/ring/vnode.py           ← VNode dataclass
tourillon/core/ring/ring.py            ← Ring
tourillon/core/ring/partitioner.py     ← Partitioner, LogicalPartition, PartitionPlacement
tourillon/core/ring/placement.py       ← PlacementStrategy Protocol, SimplePreferenceStrategy
```

## Immutability contract

```python
ring_v1 = Ring(vnodes=[...])
ring_v2 = ring_v1.add_vnodes(new_batch)
assert ring_v1 is not ring_v2          # new instance
assert ring_v1._vnodes == original     # original untouched
```

Every method that modifies the vnode set returns a brand new `Ring`.
No locks needed on the read path (async-safe by design).

## Determinism contract

```python
# Same inputs → same ring on every node in the cluster
r1 = Ring.from_vnodes(vnode_set)
r2 = Ring.from_vnodes(vnode_set)
assert r1.successor(token) == r2.successor(token)  # for all tokens
```

## Tests to write

In `tests/core/ring/test_ring.py` (mark: `@pytest.mark.ring`):

```python
# Test 1 — add_vnodes returns new Ring; original is unchanged
# Test 2 — drop_nodes returns new Ring; original is unchanged
# Test 3 — placement_for_token is deterministic across Ring instances
# Test 4 — preference_list excludes JOINING, IDLE, PAUSED, FAILED nodes
# Test 5 — preference_list includes DRAINING nodes
# Test 6 — preference_list stops at rf distinct node_ids
# Test 7 — successor wraps at ring boundary (index 0)
# Test 8 — bits=8 ring: pid and successor match bits=128 equivalent
```

Use `bits=8` in all unit tests for speed; use `bits=128` only in e2e tests.

## Common mistakes to avoid

- `HashSpace` must NOT be a module-level singleton.
- `PartitionPlacement` must NOT be stored to `state.toml`.
- `partition_shift` must be validated: `1 ≤ partition_shift ≤ bits - 1`.
- `Ring.successor` must use `bisect_right`, not linear scan.
- Token generation: use `HashSpace.hash`, not `random` or `uuid`.
