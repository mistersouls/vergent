# Custom Agents

These are custom agent definition files for this repository.

## Required format

Each agent file in `.github/agents/` now uses YAML frontmatter plus an instruction body.
This is the expected structure:

```markdown
---
name: <agent-name>
description: <short role summary>
model: gpt-5
tools: [read, write, edit, terminal]
workspace_access: full
orchestration: <coordinator|worker>
handoff_targets: [list, of, agent, names]
version: 1
---

# System Instructions
...
```

## Agents

- `coordinator.agent.md`
- `architect.agent.md`
- `devops.agent.md`
- `reviewer.agent.md`
- `tester.agent.md`
- `backend.agent.md`
- `frontend.agent.md`

## Coordination rules

- `coordinator` delegates work to worker subagents.
- All workers have full workspace read/write/edit access.
- Workers follow Architect contracts.
- All outputs use: `Plan -> Changes -> Validation -> Risks`.
- Workers must consult `docs/lifecycle/` when implementing or reviewing any
  code that touches `MemberPhase`, gossip, ring epochs, or restart behaviour.
- Test coverage is organised by lifecycle phase per `docs/testing.md`.
