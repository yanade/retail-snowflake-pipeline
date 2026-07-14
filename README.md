# retail-snowflake-pipeline
# Retail Sales Analytics Pipeline

> 🚧 **Status: In Progress** 

A production-style end-to-end data engineering pipeline built on UK e-commerce
transaction data. Demonstrates incremental loading, data validation, dead-letter
error handling, and observability across a modern Azure + Snowflake stack.

---

## Overview

This project ingests ~500k real UK retail transactions from the
[UCI Online Retail dataset](https://archive.ics.uci.edu/dataset/352/online+retail),
enriches them with live FX rates from the ExchangeRate API, transforms
a flat source file into a star schema in Snowflake, and validates every
incremental load with Google's Data Validation Tool (DVT).

**Business scenario:** A UK-based online retailer needs a reliable daily
pipeline that loads new transactions, converts GBP order values to USD,
detects data quality issues automatically, and alerts the team on failures.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  SOURCES                                                    │
│  UCI Online Retail CSV  ──┐                                 │
│  freecurrencyapi API       ──┼──▶  Azure Data Factory          │
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
The UCI dataset spans Dec 2010 – Dec 2011. Daily incremental loads are
simulated by partitioning on `InvoiceDate`. ADF uses a watermark-based
strategy — each run pulls only rows newer than the last loaded date.
Databricks performs PySpark upsert (merge) on composite key
`invoice_no + stock_code`. dbt models use `incremental` materialisation
with `unique_key = 'sale_id'`.

**Dead-letter pattern**
Records that fail at any pipeline stage (null `InvoiceNo`, unparseable
dates, referential integrity failures) are captured in a Snowflake
`dead_letter` table with error reason and raw payload. A separate
Airflow DAG handles reprocessing of failed records.

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
| Input | Raw flat CSV from ADLS | Clean flat Parquet from served zone |
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
├── terraform/          # Azure infrastructure as code
├── ingestion/          # ADF pipelines + ExchangeRate API script
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
- Azure subscription
- Snowflake account
- [freecurrencyapi.com](https://freecurrencyapi.com) API key — free tier sufficient
- Python 3.11
- Terraform >= 1.6
- Docker (for Airflow)

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
python -m ingestion.api_ingest.main --start 2010-12-02 --end 2010-12-03

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

## Dataset

**UCI Online Retail** — real UK e-commerce transactions (Dec 2010 – Dec 2011)
- ~500,000 rows, 9 source columns
- Source: https://archive.ics.uci.edu/dataset/352/online+retail
- License: CC BY 4.0

---

