# ADR 0003: At-least-once ingest, content-hash idempotency and window expiry

- Status: accepted
- Date: 2026-09-15

## Context

Inputs arrive from Kafka topics (`ledger.legs`, `rail.legs`), HTTP batches and
occasionally re-sent settlement files. Exactly-once across a broker, a database
and an HTTP API is not achievable without a global transaction coordinator, and
the failure mode that matters (a double-counted leg) can be prevented locally.

## Decision

- **Consumer commits after persistence.** The franz-go consumer disables
  auto-commit, hands a polled batch to the engine, and commits offsets only
  when every leg's `ChangeSet` has been applied. A handler error leaves the
  offsets uncommitted; the batch is redelivered after a rebalance or restart.
- **Idempotency key = content hash.** `Leg.ID = source:txn_ref:sha256(fields)[:12]`
  covers every business field (not ingestion metadata). A redelivered identical
  leg is detected by `HasLeg` and recorded as `leg.duplicate` with no state
  change. A leg with the same `(source, txn_ref)` but different content is a
  *business* duplicate and opens a `duplicate` break instead of being silently
  merged.
- **Validation is all-or-nothing per batch, persistence is per leg.** A bad
  record rejects the whole HTTP batch before any state changes; over Kafka,
  undecodable records are logged and skipped so a poison message cannot stall
  the partition.
- **Window expiry is engine-driven.** The open-leg index carries a TTL from
  `ingested_at`; a sweeper goroutine calls `Engine.Sweep` every
  `RECON_SWEEP_INTERVAL`, and `POST /v1/windows/close` forces an EOD cut-off.
  Expired legs become `window_expired` breaks; a partner arriving later heals
  a single-leg break through `T2.late-arrival@v1` rather than opening a second
  break.

## Failure handling

| Failure | Behaviour |
| --- | --- |
| Store unavailable during ingest | `Apply` fails, HTTP returns 500 / Kafka offsets are not committed; `/readyz` goes 503 |
| Crash mid-batch | Legs already applied are durable; on restart the index is rebuilt from open legs and the batch is redelivered; applied legs are duplicates |
| Rebalance mid-batch | `BlockRebalanceOnPoll` holds the rebalance until the batch is committed or abandoned |
| Poison record | Logged with topic/partition/offset, skipped, offset advanced with the rest of the batch |
| Rule bug discovered later | Fix as a new rule version, `POST /v1/replay?from=1` |

## Consequences

- No exactly-once machinery, no outbox table, no coordinator: idempotency is a
  property of the data model.
- Duplicate detection needs a `LegsByTxnRef` lookup per leg; the Postgres
  adapter indexes `legs(txn_ref)` for it.
