-- Audit table: latest recorded outcome per task per DAG run (ADR-026).
-- Run as retail_dev, so the table belongs to the role that writes it.

USE ROLE retail_dev;

CREATE TABLE IF NOT EXISTS ecommerce_db.audit.pipeline_audit (
    dag_name       VARCHAR       NOT NULL,
    run_id         VARCHAR       NOT NULL,  -- Airflow run_id, unique only within one DAG
    task_name      VARCHAR       NOT NULL,  -- Airflow task_id
    status         VARCHAR       NOT NULL,  -- SUCCESS / FAILED / SKIPPED
    dvt_status     VARCHAR,                 -- MATCH / MISMATCH / SKIPPED, NULL when no validation result
    rows_ingested  NUMBER(38,0),            -- NULL means not measured, 0 means measured and none
    rows_failed    NUMBER(38,0),
    error_message  VARCHAR,                 -- short, never connection details
    details        VARIANT,                 -- per-table counts, failed checks, skip_reason
    try_number     NUMBER(38,0),            -- attempt that wrote this row, not a history
    started_at     TIMESTAMP_NTZ,           -- UTC, NULL for a skipped task
    finished_at    TIMESTAMP_NTZ,           -- UTC
    recorded_at    TIMESTAMP_NTZ NOT NULL,  -- when the writer last wrote this row, UTC
    CONSTRAINT pk_pipeline_audit PRIMARY KEY (dag_name, run_id, task_name)  -- not enforced, the MERGE is
) COMMENT = 'Latest recorded outcome per task per DAG run, not attempt history (ADR-026)';
