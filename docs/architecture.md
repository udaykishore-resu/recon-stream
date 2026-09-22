# Architecture

recon-stream is a single Go service: a deterministic matching engine wrapped in
an HTTP API and an optional Kafka consumer, persisting through a small store
port with in-memory and Postgres adapters.

## Containers

```mermaid
flowchart LR
    subgraph Sources
        L[Core ledger]
        R[Card network / payment rail]
        O[Ops UI / scripts]
    end
    subgraph Kafka
        T1[(ledger.legs)]
        T2[(rail.legs)]
    end
    subgraph recon-stream
        K[Kafka consumer<br/>franz-go, manual commit]
        H[HTTP API<br/>net/http, OpenAPI 3.1]
        E[Matching engine<br/>T1 / T2 / T3, window index]
        C[BreakClassifier<br/>heuristic@v1]
        S[ports.Store]
    end
    subgraph Storage
        P[(Postgres<br/>legs, matches, breaks,<br/>recon_events)]
        M[(in-memory store<br/>make run / tests)]
    end
    subgraph Observability
        PR[Prometheus]
        OT[OTel collector]
    end
    L --> T1 --> K
    R --> T2 --> K
    O -->|POST /v1/legs, GET /v1/breaks, ...| H
    K --> E
    H --> E
    E --> C
    E --> S
    S --> P
    S --> M
    H -->|/metrics| PR
    H -->|OTLP traces| OT
```

## Hot path: one leg arrives

```mermaid
sequenceDiagram
    autonumber
    participant Src as Kafka / HTTP
    participant Eng as Engine
    participant Ix as Open-leg index
    participant Clf as BreakClassifier
    participant St as Store (tx)

    Src->>Eng: Ingest(legs)
    Eng->>Eng: Validate all, assign ID = source:txn_ref:hash12
    loop each leg
        Eng->>St: HasLeg(id)?
        alt already seen
            Eng->>St: Apply(leg.duplicate)
        else new
            Eng->>St: LegsByTxnRef(txn_ref) → related
            alt same source re-used txn_ref
                Eng->>Clf: Classify(duplicate)
                Eng->>St: Apply(leg=break, break.opened)
            else
                Eng->>Ix: Candidates(window = ccy,counterparty; other source)
                Eng->>Eng: T1 exact → T2 tolerant → T2 amount-date → T3 subset-sum
                alt match found
                    Eng->>St: Apply(legs=matched, match, match.created)
                    Eng->>Ix: Remove(matched legs)
                else same-ref partner open but incompatible
                    Eng->>Clf: Classify(ref_conflict)
                    Eng->>St: Apply(legs=break, break.opened)
                    Eng->>Ix: Remove(partner)
                else partner expired into break, amount within tolerance
                    Eng->>St: Apply(match, break=resolved, match.created, break.resolved)
                else
                    Eng->>St: Apply(leg=open, leg.ingested)
                    Eng->>Ix: Add(leg)
                end
            end
        end
    end
    Eng-->>Src: BatchResult
    Src->>Src: commit offsets (Kafka only)
```

## Window expiry

A sweeper calls `Engine.Sweep` every `RECON_SWEEP_INTERVAL`. Legs whose
`ingested_at + RECON_WINDOW_TTL` has passed are removed from the index and
turned into `window_expired` breaks, classified with the same interface.
`POST /v1/windows/close` performs the same operation immediately (EOD cut-off).

## Replay

`POST /v1/replay?from=N` appends `replay.started`, truncates `legs/matches/
breaks`, clears the index and re-feeds every `leg.ingested` event with
`seq >= N` through the engine with the current rules. Derived events are
appended again (they are new facts under the new rules); original
`leg.ingested` events are never duplicated; manual `break.resolved` events are
re-applied by their deterministic break ID.

## Packages

| Path | Responsibility |
| --- | --- |
| `internal/domain/recon` | Leg/Match/Break types, rules, index, matcher, engine, events. No I/O. |
| `internal/domain/classify` | `heuristic@v1` BreakClassifier. |
| `internal/ports` | `Store` and `LegSource` interfaces. |
| `internal/adapters/memory` | In-memory store and channel source. |
| `internal/adapters/postgres` | pgx store; migrations applied on start. |
| `internal/adapters/kafka` | franz-go consumer group with manual commits. |
| `internal/api/http` | Handlers, middleware (request ID, tracing, RED metrics, recovery). |
| `internal/observability` | slog JSON logger, OTel tracer, Prometheus registry. |
| `internal/config` | Env parsing and validation. |
| `cmd/recon-stream` | Wiring, graceful shutdown, sweeper. |

## Scaling model

One engine is a single writer over its windows. To scale, key Kafka messages by
`counterparty|currency` and run one replica per partition group; windows never
span partitions, so replicas never contend. Postgres is shared; each replica's
event sequence is global, and replay is a per-cluster operation performed with
consumers paused.
