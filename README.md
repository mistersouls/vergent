# vergent

Vergent is a leaderless, peer‑to‑peer distributed database that blends strong per‑key consistency with global, deterministic convergence.
Built for resilience, simplicity, and high performance across unreliable networks.

## Motivation

I’m building **Vergent** because I want a distributed database that stays simple, predictable, and understandable.
Most systems rely on heavy coordination, leader elections, background threads, and complex recovery paths.
I want something different: a system that behaves cleanly on every node, even when the network is unreliable.

## Philosophy

My approach is to keep the local engine minimal and deterministic, and to make coordination explicit rather than implicit.
Every node should be able to operate independently, every decision should be explainable, and consistency should be something
I choose per operation instead of something imposed globally.

I want strong consistency when I need it, high availability when I choose it, and guaranteed convergence without relying on permanent
leaders or centralized control.


## Goal

The goal of **Vergent** is to explore a new direction for distributed data: a leaderless, peer‑to‑peer datastore that remains easy to reason
about while offering a practical balance between performance, safety, and simplicity. I want a system that embraces decentralization
without sacrificing clarity, and that scales naturally across unreliable networks.

---

## Current Status

Vergent already includes:

### A versioned local storage engine

Based on hybrid logical clocks (HLC), last‑write‑wins semantics, and deterministic conflict resolution.

### Asynchronous replication

Every write (`put`/`delete`) is replicated to all peers using a simple, explicit event‑based protocol.

### A gossip‑based membership layer

Nodes exchange membership views, detect failures, and discover new peers automatically.
The gossip layer is active but not yet exploited for anti‑entropy or repair — that will come next.

### A minimal CLI client: `vergentctl`

A lightweight interactive shell that lets you:

- connect to a node
- run `get`, `put`, `delete`
- inspect values and HLC timestamps
- observe replication in real time

---

## Running locally

You can run a single node or multiple nodes on your machine.

### Start a node

```bash
git clone https://github.com/mistersouls/vergent
cd vergent
python -m vergent --port 2003 --peers 127.0.0.1:2001 127.0.0.1:2002 --data-dir <DISTINCT DATA DIR FOR THIS NODE> --log-level DEBUG
```

This starts a node with:
* an embedded storage backend
* replication enabled
* gossip membership enabled

Start as many nodes as you want by changing the port and peer list.

### Using the CLI (`vergentctl`)

Vergent ships with a minimal interactive client:

```bash
python -m vergentctl 127.0.0.1:2001
```

Example session:

```bash
vergent(127.0.0.1:2001)> put foo bar
OK hlc=...

vergent(127.0.0.1:2001)> get foo
bar

vergent(127.0.0.1:2001)> delete foo
OK hlc=...
```

Writes are automatically replicated to all peers.
