# Architecture

This document describes the target architecture of the project together with
the current implementation status.

The target architecture represents the intended end-state of the platform.

The implementation status identifies which components have already been
completed and which are planned for future iterations.

---

## Scope

This project demonstrates an end-to-end Azure Data Engineering platform,
covering infrastructure provisioning, data ingestion, transformation,
modelling, orchestration, validation and monitoring.

The implementation uses a self-built PostgreSQL 16 OLTP source
(`database/` — customers, orders, order_items, payments, products, stores,
employees, currencies, exchange_rates) together with Azure-native services
provisioned using Terraform.

The architecture is inspired by production data platforms while remaining
appropriately scoped for a portfolio project. Architectural trade-offs and
alternative approaches are documented in `architecture-decisions.md`.

> **Development note**
>
> The stack stays deployed between sessions. Cost is controlled per
> component: suspended warehouses, serverless compute, and PostgreSQL stopped
> by hand (ADR-021). This affects the development workflow but not the target
> architecture.

---

## Target Architecture

```
Sources
  freecurrencyapi.com API
        │  fetch_fx_rates.py, the source's rate feed (ADR-022)
        ▼
  PostgreSQL retail_oltp  ──┐
  Terraform + GitHub      ──┴──▶  Azure Data Factory  (watermark-based incremental)
                                          │
                                          ▼
                               ADLS Gen2  (3 zones)
                         Raw zone │ Curated zone │ Served zone
                         JSON,    │ Delta, one   │ Parquet per table,
                         date-    │ table per    │ plus a manifest of
                         part.    │ source table │ the files written
                                          │
                                          ▼
                              Databricks PySpark
                         raw → curated: cast, DQ rules,
                         dedupe, MERGE on primary key
                         curated → served: Change Data Feed
                                    │         │
                              happy path    bad records
                                    │         │
                                    ▼         ▼
                           Snowflake raw     Dead-letter table
                         COPY INTO from the  (error_reason,
                         manifest, append-   raw_payload,
                         only change log     failed_at)
                                    │         │
                                    ▼         └──▶ reprocess loop
                                   dbt
                         staging dedupe, FX to GBP,
                         star schema, incremental
                         fact, rejected lines, tests
                                    │
                                    ▼
                                   DVT
                         Row counts · nulls
                         sum reconciliation
                         per increment
                                    │
                              ┌─────┴─────┐
                              ▼           ▼
                        Audit table   Airflow alerts
                        (Snowflake)   Retry → Slack →
                        run_id,       log to audit
                        rows,
                        pass/fail
                              │
                              ▼
                   Streamlit dashboard
                   reads audit table live
                   + Azure Monitor (infra logs)

Airflow orchestrates end-to-end, invoking ADF, Databricks, dbt, and DVT.
Terraform provisions all Azure infrastructure as code.
```

---

## Component Responsibilities

| Component | Role |
|---|---|
| Azure Data Factory | Watermark-based incremental ingestion. Owns the watermark read and update cycle via Lookup, Copy, and Stored Procedure activities. |
| Azure SQL Database | Watermark control store. Provides transactional watermark updates and pipeline configuration. |
| ADLS Gen2 | Three-zone data lake: raw (JSON written by ADF, partitioned by extraction date), curated (Delta, one table per source table, incremental merge), served (Parquet in source shape, one folder per table). |
| Databricks PySpark | Raw → curated: type casting, DQ rules, dedupe, MERGE on each table's primary key (ADR-010), dead-letter routing. Curated → served: Change Data Feed export by Delta version, recorded in a manifest (ADR-020). No joins, no business values. |
| Snowflake | `raw` holds an append-only change log, loaded by COPY INTO from the files named in the manifest (ADR-019, ADR-020). |
| dbt | Staging dedupes the change log to the current version. Intermediate and marts convert to GBP and build the star schema; `fact_sales` loads incrementally, unloadable lines go to `fact_sales_rejected` (ADR-012, ADR-024). dbt tests on every build. |
| DVT | Post-load validation. Row counts, null checks, sum reconciliation per increment. Results written to audit table. |
| Airflow | End-to-end orchestration. Invokes ADF, Databricks, dbt, DVT. Owns retry logic, audit logging, Slack alerts. |
| Azure Key Vault | Secret management. ADF reads credentials at runtime via Managed Identity. |
| Streamlit | Data quality dashboard. Reads `pipeline_audit` table live. Surfaces pass/fail trends and dead-letter volume. |
| Azure Monitor | Infrastructure and pipeline log aggregation. |
| Terraform | All Azure infrastructure provisioned as code. Remote state in Azure Storage. |
| GitHub Actions | CI. pytest on push and PR, Terraform fmt + validate on PR. A dbt job is planned. |

---

## ADLS Gen2 Zone Structure

```
raw/
    <table_name>/
        year=YYYY/month=MM/day=DD/    ← ADF copies each retail_oltp table here, untouched,
            <table>_<run_id>.json        partitioned by extraction date
                                         (customers, orders, order_items, payments, products,
                                          stores, employees, currencies, exchange_rates, ...)

curated/
    <table_name>/                     ← cleaned and deduplicated Delta tables, one per
                                         source table, merged on primary key

curated/_dead_letter/             ← rejected rows with reasons and raw payload,
                                         registered as retail_dev.ops.dead_letter

served/
    <table_name>/
        year=YYYY/month=MM/day=DD/
            export_id=<uuid>/         ← Parquet in source shape, one directory per
                                         export attempt. Only files recorded in
                                         retail_dev.ops.served_manifest are loaded
```

ADF writes straight to `raw` — there is no separate landing zone. The Copy
activity already partitions by extraction date, so an extra hop would be a
copy with no transformation attached to it.

---

## Watermark Control Schema

```sql
-- Tracks the last successfully loaded watermark per pipeline
pipeline_watermark_control
  pipeline_name     VARCHAR   -- e.g. 'orders', 'customers'
  last_watermark    DATETIME  -- last successfully processed updated_at
  updated_at        DATETIME  -- timestamp of last watermark update

-- Static configuration per pipeline
pipeline_config
  pipeline_name     VARCHAR
  source_type       VARCHAR   -- 'PostgreSQL'
  watermark_column  VARCHAR   -- 'updated_at'
  lookback_days     INT       -- re-query window for late-arriving records
  window_size_hours INT
  is_active         BIT
```

---

## Implementation Status

| Component | Status | Notes |
|---|---|---|
| Terraform — Azure infrastructure | Done | ADLS, ADF, SQL, Databricks, Key Vault, Monitor |
| ADLS Gen2 — zone structure | Done | raw/curated/served containers created |
| PostgreSQL — OLTP source database | Done | `database/` — schema, seed data, generated transactions |
| ADF — linked services (ADLS, Key Vault, Postgres, watermark SQL) | Done | `ls_adls_dev`, `ls_key_vault`, `ls_postgres_dev`, `ls_sql_watermark_ctrl` — all created directly in ADF Studio, not Terraform. |
| ADF — per-table ingestion pipeline | Done | `pl_load_data` — config-driven watermark ingestion across all 12 `retail_oltp` tables (Option C hybrid, ADR-009). See `ingestion/README.md`. |
| Azure SQL — watermark tables | Done | `pipeline_watermark_control`, `pipeline_config` seeded per retail_oltp table |
| freecurrencyapi.com — fetch script | Done | `ingestion/api_ingest/` with unit tests, `--write-postgres` upsert |
| Databricks — PySpark transformation | Done | raw → curated MERGE per table (ADR-010), curated → served CDF export with manifest (ADR-020) |
| Dead-letter handler | Done | Delta at `curated/_dead_letter/`, `retail_dev.ops.dead_letter` (ADR-012, ADR-016) |
| Snowflake — raw load | Done | COPY INTO from the manifest, row counts checked per export, schema drift check (ADR-019, ADR-020) |
| Snowflake — star schema | Done | `DBT_DEV_MARTS`: `fact_sales`, `fact_sales_rejected`, four dimensions (ADR-024) |
| dbt — staging, intermediate, mart models | Done | `dbt build` PASS=137. `dbt docs` not generated yet |
| DVT — validation suite | Planned | |
| Airflow — main + reprocess DAGs | Planned | |
| Streamlit — data quality dashboard | Planned | |
| GitHub Actions CI | Done | `tests.yml`: pytest on push and PR. `terraform.yml`: fmt + validate on PR. No dbt job yet |
