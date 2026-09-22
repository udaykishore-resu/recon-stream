# Runbook

## Service level objectives

| SLO | Target | Measured by |
| --- | --- | --- |
| Ingest availability | 99.9% of `POST /v1/legs` return non-5xx | `recon_http_requests_total{route="POST /v1/legs"}` |
| Ingest latency | p99 < 250 ms per 1 000-leg batch (Postgres) | `recon_ingest_batch_duration_seconds` |
| Consumer lag | < 60 s behind head on `ledger.legs`, `rail.legs` | Kafka exporter `kafka_consumergroup_lag{group="recon-stream"}` |
| Auto-match rate | ≥ 95% over a trailing day | `GET /v1/stats` `auto_match_rate`, or `recon_legs_ingested_total{outcome="matched"}` / total |
| Break ageing | no open break older than 2 business days | `GET /v1/breaks?status=open&min_age=48h` |

## Metrics

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `recon_http_requests_total` | counter | route, method, status | RED: rate and errors |
| `recon_http_request_duration_seconds` | histogram | route, method | RED: duration |
| `recon_ingest_batch_duration_seconds` | histogram | | engine time per batch |
| `recon_legs_ingested_total` | counter | source, outcome | matched / break / open / duplicate / resolved_break |
| `recon_matches_total` | counter | tier, rule | which rule fires how often |
| `recon_match_residual_minor` | histogram | tier | drift inside tolerance |
| `recon_breaks_opened_total` | counter | category, trigger | break mix |
| `recon_open_legs` | gauge | | legs waiting in the window |
| `recon_kafka_commits_total` | counter | result | offset commit outcomes |

Go runtime and process collectors are included.

## Alerts

| Alert | Expression (PromQL) | Severity | First response |
| --- | --- | --- | --- |
| ReconIngestErrors | `sum(rate(recon_http_requests_total{route="POST /v1/legs",status="5xx"}[5m])) > 0` | page | check `/readyz`, Postgres connectivity, logs with `"msg":"ingest failed"` |
| ReconNotReady | `up{job="recon-stream"} == 1 and probe_success{path="/readyz"} == 0` for 2m | page | store or Kafka ping failing; see checks in the readyz body |
| ReconConsumerLag | `kafka_consumergroup_lag{group="recon-stream"} > 10000` for 10m | ticket | engine throughput vs partition count; scale partitions and replicas together |
| ReconMatchRateLow | `sum(rate(recon_legs_ingested_total{outcome="matched"}[1h])) / sum(rate(recon_legs_ingested_total{outcome=~"matched|break"}[1h])) < 0.9` | ticket | feed change (refs renamed, new fee schedule)? inspect `GET /v1/breaks?status=open` categories |
| ReconOpenLegsGrowing | `deriv(recon_open_legs[30m]) > 0` for 2h | ticket | one side of a feed has stopped; check producer health before the window expires them all |
| ReconKafkaCommitErrors | `rate(recon_kafka_commits_total{result="error"}[5m]) > 0` | page | broker health; redeliveries are safe but lag will grow |

## Dashboards

1. **Reconciliation overview**: auto-match rate, matches by rule (stacked),
   open breaks by category, open legs, oldest open break age.
2. **Service health**: RED panels per route, ingest batch duration, consumer
   lag, commit errors, Go heap and goroutines.

## Common failures

### Postgres unavailable
Symptoms: 5xx on ingest, `/readyz` 503 with `checks.store`. The consumer stops
committing; nothing is lost. Once Postgres is back the same batch is
redelivered and de-duplicated by content hash.

### Kafka rebalance storm
Symptoms: repeated `kafka batch processed` gaps, lag sawtooth. The consumer
blocks rebalances while a batch is in flight (`BlockRebalanceOnPoll`); long
batches with slow Postgres can exceed `session.timeout`. Reduce batch size via
partitions or increase broker session timeout.

### Matching rule regression
Symptoms: match rate drops after a deploy that changed `Rules` or added a rule
version. Roll back the deploy, then `POST /v1/replay?from=<first affected seq>`
after consumers are paused to re-derive projections under the good rules.

### Window closed too early / too late
`RECON_WINDOW_TTL` is measured from ingestion. Late settlement files produce a
burst of `missing_counterparty` breaks that heal automatically when the partner
arrives (`T2.late-arrival@v1`). If a file is known to be late, delay the
EOD `POST /v1/windows/close`.

### Stale projections after a crash
The index is rebuilt from `legs WHERE status='open'` on start. If projections
are suspected to be inconsistent with the event log, replay from 1.

## Operational procedures

- **EOD cut-off**: `curl -X POST $URL/v1/windows/close` (optionally `?older_than=2h`).
- **Replay**: pause consumers, `curl -X POST "$URL/v1/replay?from=1"`, resume.
  Replay truncates projections but never touches `recon_events`.
- **Resolve a break**: `curl -X POST $URL/v1/breaks/<id>/resolve -d '{"reason":"...","actor":"..."}'`.
  Resolutions are audited as `break.resolved` events and survive replays.

## Rollback

Deploy the previous image tag with `helm rollback`. Schema migrations are
additive and idempotent (`CREATE ... IF NOT EXISTS`); a previous binary runs
against a newer schema. Rule version changes are rolled back by redeploying
and replaying if projections were already re-derived.

## Capacity

The in-memory engine processes roughly 70 000 legs/s on one core (`go test -bench BenchmarkIngestPairs ./internal/domain/recon/`); with
Postgres, throughput is bounded by transaction round-trips (one per leg
transition). Size Kafka partitions so that per-replica lag stays within the
SLO, and set `RECON_T3_MAX_CANDIDATES`/`RECON_T3_MAX_SUBSET` conservatively;
they bound the worst-case CPU of a single leg.
