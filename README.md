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

