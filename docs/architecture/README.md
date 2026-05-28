# Architecture docs

Per-subsystem design documents. Each file is the authoritative design for one
subsystem; updated as the subsystem evolves. Phase 1+ work populates these.

Planned files:
- `storage-engine.md` — mergeset, indexdb, parts, merger, retention.
- `promql-engine.md` — parser, planner, executor, function semantics.
- `ingest-protocols.md` — wire-format parsers + dispatch.
- `scrape-agent.md` — scrape loop, relabel engine, persistent queue.
- `alerting.md` — rule evaluator, Alertmanager client.
- `auth-proxy.md` — vmauth-compatible reverse proxy.
- `backup-restore.md` — snapshot + incremental backup design.
