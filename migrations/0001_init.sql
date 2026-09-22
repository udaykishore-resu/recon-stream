-- recon-stream schema. legs/matches/breaks are projections; recon_events is the
-- append-only source of truth and is never truncated by the application.

CREATE TABLE IF NOT EXISTS legs (
    id            TEXT PRIMARY KEY,               -- source:txn_ref:hash12 (idempotency key)
    source        TEXT        NOT NULL,
    txn_ref       TEXT        NOT NULL,
    amount_minor  BIGINT      NOT NULL CHECK (amount_minor > 0),
    currency      CHAR(3)     NOT NULL,
    value_date    DATE        NOT NULL,
    direction     TEXT        NOT NULL CHECK (direction IN ('debit', 'credit')),
    counterparty  TEXT        NOT NULL,
    attrs         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    ingested_at   TIMESTAMPTZ NOT NULL,
    status        TEXT        NOT NULL CHECK (status IN ('open', 'matched', 'break')),
    match_id      TEXT,
    break_id      TEXT
);
CREATE INDEX IF NOT EXISTS legs_txn_ref_idx ON legs (txn_ref);
CREATE INDEX IF NOT EXISTS legs_open_idx    ON legs (status) WHERE status = 'open';

CREATE TABLE IF NOT EXISTS matches (
    id              TEXT PRIMARY KEY,
    leg_ids         TEXT[]           NOT NULL,
    tier            SMALLINT         NOT NULL,
    rule_id         TEXT             NOT NULL,
    confidence      DOUBLE PRECISION NOT NULL,
    residual_minor  BIGINT           NOT NULL,
    currency        CHAR(3)          NOT NULL,
    matched_at      TIMESTAMPTZ      NOT NULL
);

CREATE TABLE IF NOT EXISTS breaks (
    id             TEXT PRIMARY KEY,
    leg_ids        TEXT[]           NOT NULL,
    currency       CHAR(3)          NOT NULL,
    counterparty   TEXT             NOT NULL,
    category       TEXT             NOT NULL,
    confidence     DOUBLE PRECISION NOT NULL,
    classifier_id  TEXT             NOT NULL,
    trigger        TEXT             NOT NULL,
    status         TEXT             NOT NULL CHECK (status IN ('open', 'resolved')),
    opened_at      TIMESTAMPTZ      NOT NULL,
    resolved_at    TIMESTAMPTZ,
    reason         TEXT             NOT NULL DEFAULT '',
    actor          TEXT             NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS breaks_open_idx ON breaks (opened_at) WHERE status = 'open';
CREATE INDEX IF NOT EXISTS breaks_category_idx ON breaks (category, status);

CREATE TABLE IF NOT EXISTS recon_events (
    seq           BIGSERIAL PRIMARY KEY,
    type          TEXT        NOT NULL,
    at            TIMESTAMPTZ NOT NULL,
    aggregate_id  TEXT        NOT NULL,
    payload       JSONB       NOT NULL
);
CREATE INDEX IF NOT EXISTS recon_events_aggregate_idx ON recon_events (aggregate_id);

CREATE TABLE IF NOT EXISTS schema_migrations (
    version    TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
