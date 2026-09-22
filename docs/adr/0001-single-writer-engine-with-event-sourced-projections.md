# ADR 0001: Single-writer matching engine with event-sourced projections

- Status: accepted
- Date: 2026-09-15

## Context

Reconciliation state is a join across sources that arrive at different times.
Two properties are non-negotiable in BFSI: every decision must be explainable
after the fact, and re-delivery of an input (Kafka at-least-once, file re-sends,
operator replays) must never double-count. Distributed matching (several
workers sharing one window) makes both hard: matching becomes a race, and
"which rule fired, on which candidates" depends on timing.

## Decision

1. **One engine, one mutex, one window index per process.** `recon.Engine`
   serialises all state transitions. Parallelism is obtained by partitioning
   the input (Kafka partitions keyed by `counterparty|currency`) and running one
   engine per partition set, never by sharing a window.
2. **The event log is the source of truth.** Every transition appends to
   `recon_events` (`leg.ingested`, `match.created`, `break.opened`,
   `break.resolved`, `leg.expired`, `window.closed`, `replay.started`). The
   `legs`, `matches` and `breaks` tables are projections.
3. **Atomic change sets.** The engine emits one `ChangeSet` (legs, matches,
   breaks, events) per transition; `ports.Store.Apply` persists it in a single
   transaction. Only after `Apply` returns does the in-memory index change and
   the Kafka offset get committed.
4. **Deterministic identifiers.** Leg IDs are `source:txn_ref:sha256(content)[:12]`;
   match and break IDs are hashes of their sorted leg IDs. Replaying the log
   therefore reproduces byte-identical matches and breaks, and manual
   `break.resolved` events can be re-applied by ID.
5. **`POST /v1/replay?from=` rebuilds projections** by resetting them and
   re-feeding `leg.ingested` events through the *current* rules. This is how a
   rule version bump is applied retroactively, and how a corrupted projection
   is repaired.

## Consequences

- Throughput per engine is bounded by one core and the store's transaction
  latency; the memory store handles ~70k legs/s (BenchmarkIngestPairs), Postgres is bounded by round
  trips (batch ingest amortises them). Scale-out is horizontal by partition.
- The open-leg index is memory-resident and rebuilt from `legs WHERE status='open'`
  on start; a crash loses no state because the index is derived.
- Replay is a projection-wide operation (it truncates `legs/matches/breaks`).
  It is an operational action, not a request path, and is documented as such.
