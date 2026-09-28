# Retail Sales Analytics Pipeline

> 🚧 **Status: In Progress.** Source, ingestion and the raw-to-curated
> transformation run end to end. The served zone, dbt, DVT, Airflow and the
> dashboard are designed but not built yet.

A production-style data engineering pipeline from a live PostgreSQL OLTP
database to a star schema in Snowflake, with incremental loading, data quality
rules, dead-letter handling and validation on every load.

---

## Overview

The source is a self-built PostgreSQL 16 OLTP database (`database/`): a
normalized, multi-country retail system with customers, orders, order lines,
payments, products, stores, employees, currencies and exchange rates. It is
generated with deliberate operational messiness (nulls, duplicate business
keys, negative quantities, invalid statuses) so the quality layers have real
problems to catch.

**Business scenario:** a retailer with stores in the UK, Germany, France and
Canada needs a reliable pipeline that loads new and changed orders, converts
GBP, EUR and CAD order values to USD, detects data quality issues
automatically, and alerts the team on failures.

---

## Architecture

```
freecurrencyapi.com ──▶ fetch_fx_rates.py ──▶ PostgreSQL retail_oltp
                                                    │
                        Azure Data Factory          │  per-table watermark on updated_at,
                                                    ▼  plus a lookback window
ADLS raw        JSON, year=/month=/day= by extraction date
                                                    │
                        Databricks PySpark          │  cast, DQ rules, dedupe,
                                                    ▼  MERGE on primary key
ADLS curated    Delta, one table per source table ──────▶ dead-letter (Delta)
                                                    │
                        Databricks          planned │  Change Data Feed by Delta version
                                                    ▼
ADLS served     Parquet, one folder per table, plus a manifest of the files written
                                                    │
                        COPY INTO           planned │  files named in the manifest only
                                                    ▼
Snowflake raw   append-only change log, one typed table per source table
                                                    │
                        dbt                 planned │  dedupe, FX cross rates, star schema
                                                    ▼
Snowflake marts fact_sales, dim_customer, dim_product, dim_date
                                                    │
                        DVT, Airflow        planned ▼
pipeline_audit, Slack alerts, Streamlit dashboard
```

Airflow will orchestrate every layer end to end. Terraform provisions the Azure
infrastructure. Snowflake reads the served container through a storage
integration with read-only access to that container alone.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Source | PostgreSQL 16 (Azure Flexible Server, local Docker for development) |
| Cloud infrastructure | Azure (ADLS Gen2, ADF, Databricks, Key Vault, Azure SQL, Monitor) |
| Infrastructure as code | Terraform |
| Transformation | Databricks PySpark on serverless, Delta Lake, Unity Catalog |
| Data warehouse | Snowflake |
| Data modelling | dbt Core + dbt-snowflake |
| Data validation | DVT (Data Validation Tool) |
| Orchestration | Apache Airflow |
| Dashboard | Streamlit |
| CI | GitHub Actions |
| Language | Python 3.13, SQL |

---

## Key Engineering Patterns

**Incremental loading**
`updated_at` is set by a column default on insert and by a trigger on every
update. ADF keeps one watermark per table in Azure SQL and copies only rows
changed since the last run, re-reading a lookback window (three days for
orders, order lines and payments) so late commits are not lost. Databricks
keeps the latest version of each primary key and merges it into curated only
when it is newer, so re-running a load changes nothing. From curated onward,
increments are selected by Delta version through Change Data Feed, not by
`updated_at`.

**Raw in Snowflake is a change log, not a mirror** (planned)
`COPY INTO` only appends, and each increment carries every row that changed.
A key therefore appears once per version in `raw`, and counting rows there
overstates everything. dbt's staging models deduplicate to the current
version, and no model reads `raw` directly. The same log gives dimension
history without `dbt snapshot`. See ADR-020.

**Data quality and dead-letter**
Rows are rejected for a malformed JSON line, a value that will not convert to
its type, a NULL key or required column, a zero quantity, or an unknown
`order_status` or `payment_status`. Each rejected row keeps every reason it
failed and its original payload, in a Delta table registered as
`retail_dev.ops.dead_letter`. A bad row seen again on a later run is not
recorded twice.

Two things are deliberately **not** rejections: a NULL `customer_id` is a guest
checkout and maps to an unknown customer, and a negative quantity is a return,
flagged `is_return`.

**FX conversion** (planned, in dbt)
Rates are fetched from [freecurrencyapi.com](https://freecurrencyapi.com) with
GBP as the base, so each rate means "units per one GBP". Orders are in GBP, EUR
or CAD, so dbt converts through a cross rate:
`rate(GBP to USD) / rate(GBP to order currency)`. Orders without a rate for
their date go to a rejected table, not into the fact with a NULL amount.

**Databricks vs dbt**

| | Databricks | dbt |
|---|---|---|
| Responsibility | Validation, deduplication, change capture | Dimensional modelling, FX, business rules |
| Input | Raw JSON extracts from ADLS | Source-shaped change log in Snowflake `raw` |
| Output | Curated Delta tables, served Parquet | Star schema in Snowflake |
| Language | PySpark | SQL |
| Tests | Unit tests on transformation logic | Data tests on the models |

Databricks never joins tables or computes business values. Served keeps the
source shape, so grain and business rules live in exactly one place: dbt.

**Observability** (planned)
Every run will write a row to `pipeline_audit` in Snowflake: run ID, rows
ingested, rows failed, validation status, start and end time. A Streamlit app
will read it live.

---

## Star Schema

```
                    dim_date
                       │
dim_customer ──── fact_sales ──── dim_product
```

`fact_sales` has one row per order line. Order-level amounts are not carried as
measures, because they repeat on every line of an order. `order_status` is
carried, and every revenue figure must filter on it: cancelled orders are
roughly a tenth of line revenue in the generated data. Operational tables:
`pipeline_audit` and `dead_letter`.

---

## Project Structure

```
retail-snowflake-pipeline/
├── database/           # PostgreSQL OLTP source: schema, seed, data generators
├── ingestion/          # ADF pipeline exports, watermark tables, FX rates script
├── transformation/     # PySpark modules and Databricks notebooks
├── snowflake/          # Snowflake DDL: warehouse, role, storage integration, stage
├── terraform/          # Azure infrastructure as code
├── scripts/            # bootstrap, load, deploy and simulation scripts
├── tests/              # pytest suite
├── docs/               # architecture decisions and runbooks
└── .github/workflows/  # CI: pytest, terraform fmt and validate

Planned: dbt/, validation/, orchestration/, dashboard/
```

---

## Build Progress

- [x] PostgreSQL OLTP source with generated data and deliberate DQ issues
- [x] FX rates ingestion script (`ingestion/api_ingest/`)
- [x] Terraform: Azure infrastructure
- [x] ADF: watermark-based incremental pipeline into the raw zone
- [x] Databricks: raw to curated, with DQ rules, dedupe and MERGE
- [x] Dead-letter capture
- [x] Snowflake: warehouse, role, storage integration and stage
- [x] GitHub Actions CI
- [ ] Served zone export (Change Data Feed, manifest)
- [ ] Snowflake raw tables and COPY INTO
- [ ] dbt: staging, intermediate and mart models, tests
- [ ] DVT validation suite
- [ ] Airflow: main and reprocess DAGs
- [ ] Streamlit data quality dashboard

---

## Setup

### Prerequisites

- Azure subscription and the Azure CLI
- Snowflake account and the Snowflake CLI (`snow`)
- [freecurrencyapi.com](https://freecurrencyapi.com) API key, free tier
- Python 3.13, and Java 17 for PySpark
- Terraform >= 1.6
- Docker, for a local PostgreSQL

Copy `.env.example` to `.env` and fill in the values:

```bash
cp .env.example .env
```

### Runbooks

Setup is procedural and lives in `docs/`:

| Runbook | Covers |
|---|---|
| `docs/rebuild-runbook.md` | Azure from scratch: Terraform state, infrastructure, secrets, source data |
| `docs/snowflake-setup-runbook.md` | Snowflake account, key-pair auth, storage integration and stage |
| `docs/incremental-run-runbook.md` | Simulating source changes and running one incremental load |

### Run the tests

```bash
pytest -v
```

### Run a load by hand

Until Airflow exists, one load is three manual steps, described in
`docs/incremental-run-runbook.md`:

1. `./scripts/simulate_source_changes.sh` to create new and changed rows
2. Trigger `pl_load_data` in ADF Studio
3. Run the Databricks job `raw_to_curated` with `reader = batch`

---

## Documentation

- `docs/architecture-decisions.md`: every design decision with its
  alternatives, including the served zone contract (ADR-020)
- `database/README.md`: source schema and the data quality scenarios it
  generates
