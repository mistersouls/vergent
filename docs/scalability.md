# Scalability

This document describes the capacity model, configuration parameters that
affect scale, and the operational constraints that must be set before a
cluster can grow to thousands of nodes. It is an informative complement to
the normative specifications in `docs/ring.md` and `docs/rebalance.md`.

---

## 1. Capacity Model

Tourillon's scalability profile is determined by three interacting parameters
that must be chosen at cluster bootstrap time and cannot be changed without a
full data migration:

- **`bits`** — the hash-space width (production default: 128). Governs the
  range of token values but not the number of partitions.
- **`partition_shift`** — determines the number of logical partitions
  (`total = 2**partition_shift`). This is the most important sizing decision
  because it is immutable. See §2 for sizing guidance.
- **`vnodes_per_node`** — the number of virtual-node positions each physical
  node claims in the ring. Higher counts improve load distribution at the cost
  of proportionally larger ring state to gossip.

Starting capacity planning from these parameters allows operators to bound the
maximum data movement per topology event before any node is deployed:

```
per_event_movement ≈ (data_volume_per_node / vnodes_per_node) × Δownership
```

where `Δownership` is the fraction of partitions whose primary assignment
changes for a single join or leave event in a balanced ring.

---

## 2. Partition Grid Sizing

The partition grid is immutable. Choosing a `partition_shift` that is too
small will limit how evenly the cluster distributes data as it scales, and
increasing it later requires a full data migration. The recommended rule is
to target at least 10× the maximum expected node count in total partitions.

| Maximum planned node count | Minimum `partition_shift` | Total partitions |
|---------------------------:|-------------------------:|-----------------:|
| 100                        | 10                       | 1 024            |
| 1 000                      | 14                       | 16 384           |
| 10 000                     | 17                       | 131 072          |
| 100 000                    | 20                       | 1 048 576        |

For test clusters using `bits=8`, `partition_shift` must be at most 7 (at most
128 partitions) because the step size would be zero for any larger value.

---

## 3. Virtual-Node Count Trade-offs

A larger `vnodes_per_node` value distributes each node's ownership across more
ring positions, which:

- Reduces the data volume moved per join or leave event (each vnode covers a
  smaller token range).
- Smooths load distribution across nodes, particularly important when nodes
  have heterogeneous storage capacity.
- Increases the size of the ring state that must be gossiped and stored in
  memory on every node.

For a production cluster planned at 1 000 nodes with `partition_shift=14`
(16 384 partitions), a `vnodes_per_node` value of 16 gives each node
approximately 16 primary partition slots on average, keeping the preference-list
walk short while providing meaningful smoothing.

The `vnodes_per_node` count is fixed at cluster bootstrap and must be agreed
upon by all initial members. Changing it mid-cluster requires a targeted
rebalance plan that is currently out of scope.

---

## 4. Horizontal Scaling Mechanics

Nodes join and leave without a global stop-the-world operation. When a node
joins, the ring epoch advances by one and the rebalance coordinator on each
affected node independently derives the same deterministic plan for moving
partition data. Only the partitions in the ownership delta are transferred;
stable partitions are unaffected.

The practical saturation limit for the rebalance path is determined by:

- The rate-limit fraction configured on transfer sources (default: no more
  than 25% of outbound bandwidth for rebalance traffic, leaving the remainder
  for client-facing reads and writes).
- The concurrency bound on in-flight transfers per node (governed by the
  `asyncio.Semaphore` in the coordinator).

Operators who need to scale out quickly but have a large data volume should
admit joining nodes in small batches so that each batch's rebalance completes
before the next batch joins.

---

## 5. Failure Domain Isolation

Tourillon's `PlacementStrategy` walks the ring clockwise to collect distinct
physical nodes for a preference list. In the first milestone the strategy
enforces distinct physical nodes but ignores topology labels. A future
`TopologyAwarePlacementStrategy` will add a third eligibility criterion: no
two replicas in the same failure domain (rack, availability zone, or region).

Until topology-aware placement is in place, operators deploying in a multi-zone
environment should distribute vnodes across zones by assigning them sequentially
during the bootstrap provisioning step, ensuring the deterministic walk
naturally interleaves nodes from different zones.

A network partition that isolates a subset of nodes reduces the available
replica count for partitions whose owners are split across the partition. The
system degrades safely: reads and writes targeting a partition whose quorum is
temporarily unreachable return `TEMPORARY_UNAVAILABLE`. Convergence and
ordering guarantees are maintained; availability may temporarily reduce. No
permanent data loss occurs as long as at least one replica per partition
survives the partition.

---

## 6. Churn-Burst Guardrails

Tourillon is designed to remain safe under continuous churn, but operators
should be aware of the following operational limits:

- **Burst joins.** Admitting many nodes simultaneously can generate a cascade
  of ring epochs if each join triggers its own epoch. The membership layer
  should coalesce simultaneous joins into batches, limiting epoch churn to a
  manageable number per minute.
- **Burst leaves.** Simultaneous loss of multiple nodes (VM reboot storm,
  zone failure) is handled by the failure detection model: each surviving node
  independently marks the lost nodes as `DEAD` based on operation failures, and
  the subsequent ring epoch removes all their vnodes in one transition.
- **Flapping membership.** A node that oscillates between `REACHABLE` and
  `SUSPECT` must not generate an unbounded series of ring epochs.  The
  membership layer applies a minimum suspicion duration before promoting a
  local observation to a ring-level event.

---

## 7. Performance Guardrails (Targets)

The values below represent operational targets for a healthy cluster and should
be used as alerting thresholds. They are not yet measured against a production
deployment and will be updated when scale validation results are available
(Milestone 5).

| Metric | Target |
|--------|--------|
| p50 write latency (KV plane, quorum ack) | < 5 ms |
| p99 write latency | < 50 ms |
| Replication lag (p95) | < 1 s under normal load |
| Hinted handoff queue drain time | < 30 s after target recovery |
| Ring epoch propagation | < 5 s to all nodes in a 100-node cluster |
| Convergence time after partition heal | Bounded by handoff drain time + anti-entropy scan interval |

---

## 8. Scalability Exit Criteria

These criteria apply to Milestone 5 scale validation:

- Controlled tests demonstrate stable throughput and latency at the p50/p99
  targets above under sustained client load with at least 10 nodes.
- Ring changes (join and leave) complete within documented guardrails without
  violating convergence during the transition.
- Fault-injection tests (single-node crash, zone-level partition) produce
  expected degraded-mode behaviour and full recovery once the fault is resolved.
- No unrecovered resource leak (memory, open file descriptors, asyncio tasks)
  is observed over a 1-hour stability campaign.
