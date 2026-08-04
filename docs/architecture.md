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
> To minimise Azure costs, the development environment is provisioned on
> demand with Terraform and destroyed after each development session.
> This affects the development workflow but not the target architecture.

---

## Target Architecture

```
Sources
  PostgreSQL retail_oltp  ──┐
  freecurrencyapi.com API ──┼──▶  Azure Data Factory  (watermark-based incremental)
  Terraform + GitHub      ──┘              │
                                          ▼
                               ADLS Gen2  (3 zones)
                         Raw zone │ Curated zone │ Served zone
                         Parquet, │ Parquet,     │ Snowflake-ready
                         date-    │ incremental  │ Parquet
                         part.    │ partitions   │
                                          │
                                          ▼
                              Databricks PySpark
                         Incremental merge · FX conversion
                         cleaning + enrichment (flat → flat)
                                    │         │
                              happy path    bad records
                                    │         │
                                    ▼         ▼
                           Snowflake + dbt   Dead-letter table
                         Incremental models  (error_reason,
                         star schema         raw_payload,
                         dbt tests + docs    failed_at)
                                    │         │
                                    ▼         └──▶ reprocess loop
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
| ADLS Gen2 | Three-zone data lake: raw (source files), curated (Parquet, partitioned by date), served (Snowflake-ready Parquet). |
| Databricks PySpark | Raw → Curated → Served transformation. Incremental merge on composite key. FX enrichment. Dead-letter routing. |
| Snowflake + dbt | Dimensional modelling. Star schema built from served Parquet. Incremental dbt models, dbt tests, dbt docs. |
| DVT | Post-load validation. Row counts, null checks, sum reconciliation per increment. Results written to audit table. |
| Airflow | End-to-end orchestration. Invokes ADF, Databricks, dbt, DVT. Owns retry logic, audit logging, Slack alerts. |
| Azure Key Vault | Secret management. ADF reads credentials at runtime via Managed Identity. |
| Streamlit | Data quality dashboard. Reads `pipeline_audit` table live. Surfaces pass/fail trends and dead-letter volume. |
| Azure Monitor | Infrastructure and pipeline log aggregation. |
| Terraform | All Azure infrastructure provisioned as code. Remote state in Azure Storage. |
| GitHub Actions | CI pipeline. Terraform validate + plan on PR. dbt test on PR. |

---

## ADLS Gen2 Zone Structure

```
landing/
    <table_name>/
        year=YYYY/month=MM/day=DD/    ← ADF copies each retail_oltp table here, untouched
                                         (customers, orders, order_items, payments, products,
                                          stores, employees, currencies, exchange_rates, ...)

raw/
    <table_name>/
        year=YYYY/month=MM/day=DD/    ← Databricks partitions by extraction date

curated/
    retail/
        year=YYYY/month=MM/day=DD/    ← cleaned, FX-enriched Parquet

served/
    fact_sales/
        year=YYYY/month=MM/day=DD/    ← Snowflake-ready Parquet
```

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
| ADLS Gen2 — zone structure | Done | raw/curated/served/landing containers created |
| PostgreSQL — OLTP source database | Done | `database/` — schema, seed data, generated transactions |
| ADF — linked services (ADLS, Key Vault, Postgres, watermark SQL) | Done | `ls_adls_dev`, `ls_key_vault`, `ls_postgres_dev`, `ls_sql_watermark_ctrl` — all created directly in ADF Studio, not Terraform. |
| ADF — per-table ingestion pipeline | Done | `pl_load_data` — config-driven watermark ingestion across all 12 `retail_oltp` tables (Option C hybrid, ADR-009). See `ingestion/README.md`. |
| Azure SQL — watermark tables | Done | `pipeline_watermark_control`, `pipeline_config` seeded per retail_oltp table |
| freecurrencyapi.com — fetch script | Done | `ingestion/api_ingest/` with unit tests, `--write-postgres` upsert |
| Databricks — PySpark transformation | Planned | |
| Dead-letter handler | Planned | |
| Snowflake — star schema | Planned | |
| dbt — staging, intermediate, mart models | Planned | |
| DVT — validation suite | Planned | |
| Airflow — main + reprocess DAGs | Planned | |
| Streamlit — data quality dashboard | Planned | |
| GitHub Actions CI | Planned | |
