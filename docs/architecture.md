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

The implementation uses the UCI Online Retail dataset (~500,000 rows,
Dec 2010 – Dec 2011) together with Azure-native services provisioned
using Terraform.

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
  UCI Online Retail CSV  ──┐
  ExchangeRate API       ──┼──▶  Azure Data Factory  (watermark-based incremental)
  Terraform + GitHub     ──┘              │
                                          ▼
                               ADLS Gen2  (3 zones)
                         Raw zone │ Curated zone │ Served zone
                         CSV,     │ Parquet,     │ Snowflake-ready
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
raw/
  source/
    uci_retail/
      online_retail.csv               ← full source file, uploaded once
  fx_rates/
    year=YYYY/month=MM/day=DD/        ← daily FX rate JSON from ExchangeRate API

curated/
  uci_retail/
    year=YYYY/month=MM/day=DD/        ← Parquet, partitioned by InvoiceDate
  fx_rates/
    year=YYYY/month=MM/day=DD/

served/
  fact_sales/
    year=YYYY/month=MM/day=DD/        ← Snowflake-ready Parquet
```

---

## Watermark Control Schema

```sql
-- Tracks the last successfully loaded watermark per pipeline
pipeline_watermark_control
  pipeline_name     VARCHAR   -- e.g. 'pl_ingest_uci_retail'
  last_watermark    DATETIME  -- last successfully processed InvoiceDate
  updated_at        DATETIME  -- timestamp of last watermark update

-- Static configuration per pipeline
pipeline_config
  pipeline_name     VARCHAR
  source_type       VARCHAR   -- 'csv_file'
  watermark_column  VARCHAR   -- 'InvoiceDate'
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
| UCI CSV — raw zone upload | Done | `raw/source/uci_retail/online_retail.csv` |
| ADF — linked services | Done | `ls_adls_dev`, `ls_azure_sql`, `ls_key_vault` |
| ADF — datasets | Done | `ds_uci_retail_csv`, `ds_watermark_sql` |
| ADF — ingestion pipeline | In progress | `pl_ingest_uci_retail` |
| Azure SQL — watermark tables | Done | `pipeline_watermark_control`, `pipeline_config` seeded |
| ExchangeRate API — fetch script | Done | `ingestion/api_ingest/` with unit tests |
| Databricks — PySpark transformation | Planned | |
| Dead-letter handler | Planned | |
| Snowflake — star schema | Planned | |
| dbt — staging, intermediate, mart models | Planned | |
| DVT — validation suite | Planned | |
| Airflow — main + reprocess DAGs | Planned | |
| Streamlit — data quality dashboard | Planned | |
| GitHub Actions CI | Planned | |
