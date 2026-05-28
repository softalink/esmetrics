# Project state files

Source of truth for what the agent is currently working on and what blocks
progress. See [`PLAN.md` §16](../../PLAN.md) for the autonomous-operation
pacing rules.

| File | Purpose |
|---|---|
| `progress-log.md` | Append-only daily checkpoint log. Newest on top. |
| `backlog.md` | Ordered list of next tasks within the *current* phase. |
| `blockers.md` | Open items requiring owner input. Empty is the desired state. |
| `decisions.md` | Architectural Decision Records (ADRs). One entry per non-trivial decision. |
| `phase-N-report.md` | Generated at the end of each phase; not present before then. |
