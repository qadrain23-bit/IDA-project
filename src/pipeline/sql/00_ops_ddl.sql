-- Observability (ops) schema in Postgres.
-- Every pipeline step writes here; the Grafana dashboards read only from here.
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS staging;

-- one shared id for every batch file load and every stream run
CREATE SEQUENCE IF NOT EXISTS ops.load_id_seq;

CREATE TABLE IF NOT EXISTS ops.batch_load_log (
    load_id        BIGINT PRIMARY KEY,
    refresh_id     INT,
    source_key     TEXT NOT NULL,
    file_name      TEXT NOT NULL,
    file_size_mb   NUMERIC(12,3),
    started_at     TIMESTAMPTZ NOT NULL,
    ended_at       TIMESTAMPTZ,
    duration_s     NUMERIC(12,3),
    rows_read      BIGINT,
    rows_loaded    BIGINT,
    rows_rejected  BIGINT,
    status         TEXT NOT NULL,          -- RUNNING / SUCCESS / FAILED
    error_message  TEXT
);

CREATE TABLE IF NOT EXISTS ops.stream_run (
    load_id           BIGINT PRIMARY KEY,
    refresh_id        INT,
    source_key        TEXT NOT NULL,
    file_name         TEXT,
    started_at        TIMESTAMPTZ NOT NULL,
    ended_at          TIMESTAMPTZ,
    duration_s        NUMERIC(12,3),
    events_sent       BIGINT,
    events_loaded     BIGINT,
    events_malformed  BIGINT,
    events_dropped    BIGINT,              -- lost because an API call failed
    throughput_eps    NUMERIC(12,2),       -- events per second
    avg_latency_s     NUMERIC(12,4),
    p95_latency_s     NUMERIC(12,4),
    status            TEXT NOT NULL
);

-- per-second window of a stream run (for the throughput line chart)
CREATE TABLE IF NOT EXISTS ops.stream_window (
    load_id          BIGINT NOT NULL,
    window_start     TIMESTAMPTZ NOT NULL,
    events_loaded    INT NOT NULL,
    events_malformed INT NOT NULL DEFAULT 0,
    avg_latency_s    NUMERIC(12,4),
    max_latency_s    NUMERIC(12,4),
    PRIMARY KEY (load_id, window_start)
);

CREATE TABLE IF NOT EXISTS staging.rejected_rows (
    load_id      BIGINT NOT NULL,
    source_key   TEXT NOT NULL,
    record_key   TEXT,
    reason       TEXT NOT NULL,
    raw_record   JSONB,
    rejected_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS ops.storage_snapshot (
    snapshot_at    TIMESTAMPTZ NOT NULL,
    refresh_id     INT,
    layer          TEXT NOT NULL,           -- staging_postgres / mart_duckdb
    table_name     TEXT NOT NULL,
    source_domain  TEXT,
    row_count      BIGINT,
    size_mb        NUMERIC(12,3),
    min_date       DATE,
    max_date       DATE,
    distinct_months INT
);

CREATE TABLE IF NOT EXISTS ops.query_log (
    query_run_id   BIGSERIAL PRIMARY KEY,
    run_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    refresh_id     INT,
    query_name     TEXT NOT NULL,
    widget         TEXT,
    variant        TEXT NOT NULL,           -- base (star-schema joins) / optimized (summary table)
    duration_ms    NUMERIC(12,3),
    rows_returned  INT,
    status         TEXT NOT NULL,           -- SUCCESS / FAILED
    error_message  TEXT
);

CREATE TABLE IF NOT EXISTS ops.pipeline_run (
    run_id       BIGSERIAL PRIMARY KEY,
    refresh_id   INT,
    step         TEXT NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL,
    ended_at     TIMESTAMPTZ,
    status       TEXT NOT NULL,
    details      JSONB
);

-- what the DuckDB transform has already picked up from staging (incremental loads)
CREATE TABLE IF NOT EXISTS ops.etl_watermark (
    source_key    TEXT PRIMARY KEY,
    last_load_id  BIGINT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ
);
