# Vergent

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

- connect to a node over **mandatory mTLS**
- run `get`, `put`, `delete`
- inspect values and HLC timestamps
- observe replication in real time
- generate certificates (Root CA + node certificates)
- inspect certificates (SAN, validity, CA flags, etc.)

---

## Running locally

Vergent enforces **strict mutual TLS (mTLS)**.  
Before starting any node, you must generate:

- a **Root CA**
- one or more **node certificates** signed by this CA

All certificate operations are handled by `vergentctl`.

---

### 1. Generate a Root CA

```bash
python -m vergentctl gen-root \
  --out ca.pem \
  --key-out ca.key \
  --ask-passphrase
```

This creates:
* `ca.pem` — Root CA certificate
* `ca.key` — Root CA private key (encrypted with a passphrase)


### 2. Generate a node certificate

For local multi‑node testing, you can generate a certificate valid for:

* `127.0.0.1`
* `localhost`
* a node name (e.g. `node-1)`

```bash
python -m vergentctl gen-cert \
  --ca ca.pem \
  --ca-key ca.key \
  --ca-passphrase <PASS> \
  --out node-1.crt \
  --key-out node-1.key \
  --cn node-1 \
  --san-dns node-1 \
  --san-dns localhost \
  --san-ip 127.0.0.1
```

You may reuse the same certificate for all local nodes, or generate one per node.


### 3. Start a node

```bash
python -m vergent \
  --port 2003 \
  --peers 127.0.0.1:2001 127.0.0.1:2002 \
  --data-dir <DISTINCT DATA DIR FOR THIS NODE> \
  --log-level DEBUG \
  --tls-certfile node-1.crt \
  --tls-keyfile node-1.key \
  --tls-cafile ca.pem
```

This starts a node with:
* an embedded storage backend
* replication enabled
* gossip membership enabled
* strict mTLS (no insecure fallback)

Start as many nodes as you want by changing the port and peer list.


### 4. Using the CLI (vergentctl)

Vergent ships with a minimal interactive client:

```bash
python -m vergentctl connect 127.0.0.1:2001 \
  --tls-certfile client.crt \
  --tls-keyfile client.key \
  --tls-cafile ca.pem
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


### 5. Inspecting certificates

```bash
python -m vergentctl inspect-cert --cert node-1.crt
```

Example output:

```bash
Certificate Information
-----------------------
Subject: CN=node-1
Issuer:  CN=Vergent Root CA
Serial:  575796643942207073303331147382241448308185652364
Valid From: 2025-12-25 23:10:39+00:00
Valid To:   2026-12-25 23:10:39+00:00
Is CA: False
Subject Alternative Names:
  - <DNSName(value='node-1')>
  - <DNSName(value='localhost')>
  - <IPAddress(value=127.0.0.1)>
```
