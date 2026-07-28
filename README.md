# retail-snowflake-pipeline
# Retail Sales Analytics Pipeline

> 🚧 **Status: In Progress** 

A production-style end-to-end data engineering pipeline built on UK e-commerce
transaction data. Demonstrates incremental loading, data validation, dead-letter
error handling, and observability across a modern Azure + Snowflake stack.

---

## Overview

This project runs against a self-built PostgreSQL 16 OLTP source
(`database/`) — a normalized, multi-country retail operational database
(customers, orders, order_items, payments, products, stores, employees),
enriches order values with live FX rates from
[freecurrencyapi.com](https://freecurrencyapi.com), transforms the
normalized source into a star schema in Snowflake, and validates every
incremental load with Google's Data Validation Tool (DVT).

**Business scenario:** An international retailer (stores in the UK,
Germany, France, and Canada) needs a reliable pipeline that loads new
orders, converts multi-currency order values to USD, detects data quality
issues automatically, and alerts the team on failures.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  SOURCES                                                    │
│  PostgreSQL retail_oltp ──┐                                 │
│  freecurrencyapi.com API──┼──▶  Azure Data Factory          │
│  Terraform + GitHub     ──┘     (watermark incremental)     │
└─────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────┐
│  STORAGE — ADLS Gen2 (3 zones)                              │
│  Raw zone          Curated zone        Served zone          │
│  JSON/CSV,    →    Parquet,        →   Snowflake-ready      │
│  date-part.        incremental         Parquet              │
│                    partitions                               │
└─────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────┐
│  TRANSFORM — Databricks PySpark                             │
│  Incremental merge · FX conversion · flat → star schema     │
│                    │               │                        │
│              happy path        bad records                  │
│                    │               ▼                        │
│                    │        Dead-letter table               │
│                    │        (error + raw payload)  ──loop─▶ │
└────────────────────┼────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  WAREHOUSE — Snowflake + dbt                                │
│  Incremental models · star schema · dbt tests + docs        │
│                    │                                        │
│                    ▼                                        │
│  VALIDATION — DVT                                           │
│  Row counts · null checks · sum reconciliation per load     │
│                    │                                        │
│         ┌──────────┴──────────┐                            │
│         ▼                     ▼                            │
│   Audit table           Airflow alerts                      │
│   run_id, rows,         Retry → Slack →                     │
│   pass/fail, ts         log to audit                        │
└─────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────┐
│  OBSERVE                                                    │
│  Streamlit dashboard — DVT results, row counts, pass/fail   │
│  Azure Monitor — infra and pipeline logs                    │
└─────────────────────────────────────────────────────────────┘

Airflow DAG orchestrates every layer end-to-end.
Terraform provisions all Azure infrastructure as code.
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Cloud infrastructure | Azure (ADLS Gen2, ADF, Databricks, Monitor) |
| Infrastructure as code | Terraform |
| Transformation | Databricks PySpark |
| Data warehouse | Snowflake |
| Data modelling | dbt Core + dbt-snowflake |
| Data validation | DVT (Data Validation Tool) |
| Orchestration | Apache Airflow |
| Dashboard | Streamlit |
| CI/CD | GitHub Actions |
| Language | Python 3.11, SQL |

---

## Key Engineering Patterns

**Incremental loading**
Every mutable `retail_oltp` table has an `updated_at` column refreshed by a
PostgreSQL trigger on insert/update. ADF uses a per-table watermark strategy
— each run pulls only rows newer than the last watermark
(`ingestion/watermark/watermark_control.sql`). Databricks performs PySpark
upsert (merge) on each table's primary key. dbt models use `incremental`
materialisation with `unique_key = 'sale_id'`.

**Dead-letter pattern**
Records that fail at any pipeline stage (null `customer_id`, negative
`order_items.quantity` that aren't returns, invalid `payment_status`,
referential integrity failures) are captured in a Snowflake `dead_letter`
table with error reason and raw payload. A separate Airflow DAG handles
reprocessing of failed records.

**Observability**
Every pipeline run writes a row to a `pipeline_audit` table in Snowflake
— run ID, rows ingested, rows failed, DVT status, start/end timestamps.
A Streamlit app reads this table live and surfaces pass/fail trends,
row count history, and dead-letter counts.

**FX enrichment**
GBP transaction values are enriched with daily exchange rates from
[freecurrencyapi.com](https://freecurrencyapi.com) — free tier supports
historical data back to 2010, enabling multi-currency reporting in the star schema.

**Databricks vs dbt — separation of responsibilities**
Databricks and dbt each own a distinct layer — they are not interchangeable:

| | Databricks | dbt |
|---|---|---|
| Responsibility | Data preparation | Dimensional modelling |
| Input | Raw Postgres extract (Parquet) from ADLS | Clean flat Parquet from served zone |
| Output | Clean enriched flat Parquet | Star schema tables in Snowflake |
| Language | PySpark | SQL |
| Tests | Unit tests on transformation logic | Data quality tests on the model |

Databricks cleans, enriches with FX rates, and routes bad records to dead-letter —
but outputs a **flat enriched file**, not a star schema.
dbt reads that flat file and builds `dim_customer`, `dim_product`, `dim_date`,
and `fact_sales`. The star schema lives entirely in dbt/Snowflake.

---

## Star Schema

```
                    dim_date
                       │
dim_customer ──── fact_sales ──── dim_product
```

Engineered from a single flat source file into four tables:
`fact_sales`, `dim_customer`, `dim_product`, `dim_date`,
plus `pipeline_audit` and `dead_letter` operational tables.

---

## Project Structure

```
retail-snowflake-pipeline/
├── database/            # PostgreSQL OLTP source system
├── terraform/          # Azure infrastructure as code
├── ingestion/          # ADF pipelines + freecurrencyapi.com script
├── transformation/     # Databricks PySpark notebooks
├── dbt/                # dbt models (staging → intermediate → marts)
├── validation/         # DVT validation suite
├── orchestration/      # Airflow DAGs (pipeline + reprocess)
├── dashboard/          # Streamlit data quality app
└── .github/workflows/  # CI: Terraform validate + dbt test on PR
```

---

## Build Progress

- [x] Project architecture designed
- [x] FX rates ingestion script — freecurrencyapi.com (`ingestion/api_ingest/`)
- [x] Unit tests — 15 tests passing (`tests/ingestion/api_ingest/`)
- [x] Terraform — Azure infrastructure
- [ ] ADLS Gen2 — 3-zone storage with date partitioning
- [ ] ADF — watermark-based incremental pipeline
- [ ] Databricks — PySpark incremental transformation
- [ ] Dead-letter handler
- [ ] Snowflake — star schema
- [ ] dbt — staging, intermediate, mart models + tests
- [ ] DVT — validation suite
- [ ] Airflow — main + reprocess DAGs
- [ ] Streamlit — data quality dashboard
- [ ] GitHub Actions CI

---

## Setup

### Prerequisites
- Docker (for the PostgreSQL source — `database/docker-compose.yml` — and for Airflow)
- Azure subscription
- Snowflake account
- [freecurrencyapi.com](https://freecurrencyapi.com) API key — free tier sufficient
- Python 3.11
- Terraform >= 1.6

### Environment variables
Copy `.env.example` to `.env` and fill in your credentials:
```bash
cp .env.example .env
```

### Infrastructure

Before running Terraform for the first time, create the remote state storage manually:

```bash
az group create \
  --name retail-pipeline-tfstate-rg \
  --location uksouth

az storage account create \
  --name retailpipelinetfstate \
  --resource-group retail-pipeline-tfstate-rg \
  --location uksouth \
  --sku Standard_LRS

az storage container create \
  --name tfstate \
  --account-name retailpipelinetfstate
```

Then provision all infrastructure:

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

### Run FX rates ingestion

```bash
# activate virtual environment
source venv/bin/activate

# fetch GBP rates for a date range
python -m ingestion.api_ingest.main --start 2025-01-01 --end 2025-01-03

# output saved to ingestion/api_ingest/output/
```

### Run tests

```bash
pytest tests/ingestion/api_ingest/ -v
```

### Run the pipeline
```bash
# Start Airflow
cd orchestration
docker compose up -d

# Trigger the main DAG
# Open Airflow UI at http://localhost:8080
# Trigger: pipeline_dag
```

---

## Source Database

**PostgreSQL retail_oltp** — self-built, normalized OLTP schema simulating
a live international retailer
- 12 tables: customers, orders, order_items, payments, products, stores,
  employees, currencies, exchange_rates, and more
- Generated data includes intentional operational messiness (nulls,
  duplicate business keys, negative quantities, invalid statuses, missing
  FX rates) so the DQ/dead-letter layers have real problems to catch
- Full schema and design rationale: `database/README.md`

---

