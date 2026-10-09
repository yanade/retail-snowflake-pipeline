# Architecture Decisions

This document records the key architectural decisions made during the project.

The objective is not to document every implementation detail, but to explain
why specific technologies and design patterns were chosen, what alternatives
were considered, and what trade-offs were accepted.

The decisions reflect the project's primary goal: building a realistic,
interview-ready Azure Data Engineering portfolio while keeping the scope
appropriate for a Junior/Mid-level Data Engineer.

---

## ADR-001: Orchestration Strategy

### Decision
Airflow is the primary workflow orchestrator. Azure Data Factory is responsible
for Azure-native ingestion and incremental data movement.

### Context
The pipeline has three distinct concerns: data movement (PostgreSQL + FX API → ADLS),
transformation (PySpark), and validation + alerting (DVT, Slack, audit table).
ADF is optimized for Azure-native data movement but is less suitable for
Python-centric operational workflows such as DVT execution, custom validation
logic, and rich notification handling. Airflow can coordinate all three concerns
through a single DAG while delegating the actual data movement to ADF.

### Architecture
```
Airflow DAG  ←  single source of truth for pipeline state
     │
     ├── AzureDataFactoryRunPipelineOperator → ADF (ingestion)
     ├── DatabricksRunNowOperator            → Databricks (transformation)
     └── PythonOperator                      → DVT, audit table, Slack alert
```

### Rationale
Separating workflow orchestration from Azure-native ingestion reduces coupling
between orchestration logic and cloud-specific data movement services. In this
pipeline, that boundary is concrete: Airflow owns retries, branching, audit
logging, and Slack alerts — ADF owns the incremental copy from PostgreSQL into
ADLS and the watermark update in Azure SQL.

### Trade-off accepted
Two systems to monitor and maintain instead of one. When a pipeline fails,
the investigation crosses two UIs — Airflow for DAG state, ADF Monitor for
copy activity detail. This cost is acceptable because Airflow is the single
source of truth for pipeline status — ADF is always invoked from Airflow,
never independently.

### Consequences
- Airflow triggers ADF via `AzureDataFactoryRunPipelineOperator`
- ADF never knows it is part of a larger workflow — it executes when called
- Either layer can be replaced without rebuilding the other

### When this pattern is appropriate
This approach is most appropriate when workflow orchestration extends beyond
Azure-native ingestion and requires Python-based validation, cross-platform
integrations, or standardized orchestration across heterogeneous environments.

---

## ADR-002: Watermark Storage

### Decision
Use Azure SQL Database as the watermark control store.

### Alternatives Considered
- **ADLS JSON file** — no transaction guarantees; if the pipeline fails mid-run the watermark and data are out of sync
- **Delta metadata table** — requires Databricks to be running, couples the ingestion layer to the transformation layer
- **Snowflake control table** — not available until Epic 2; creates a dependency between Epic 1 and Epic 2

### Reason
Azure SQL Database provides native ADF integration (Lookup + Stored Procedure activities), atomic transactional updates, demonstrates additional Azure platform skills, and aligns with common enterprise ADF architectures.

### Consequences
- One additional Terraform module (`modules/sql/`)
- One additional ADF Linked Service (`ls_azure_sql`)
- Watermark survives pipeline failures — no duplicate or missing data if a run is interrupted

### Limitations
- The watermark column is `updated_at`, a server-assigned, trigger-refreshed
  timestamp on every retail_oltp table (database/schema.sql) — the
  production pattern noted as a future improvement in the original version
  of this ADR is now what's actually implemented (see ADR-008).
- Late-arriving records are partially mitigated by a configurable `lookback_days` window stored in `pipeline_config`.
- Deduplication of overlapping records from the lookback window is handled in Databricks, not in ADF or the watermark table.

### Coupling constraint
Advancing the watermark to `@window_end` is only safe because the ADF source query applies a configurable lookback window (`pipeline_config.lookback_days`) on every run. If the lookback window is removed, late-arriving records in already-processed windows will be permanently skipped. These two design decisions are coupled and must always be changed together.

---

## ADR-003: Secret Management

### Decision
Use Azure Key Vault for all credentials. Terraform provisions the vault as infrastructure only — secrets are bootstrapped separately via Azure CLI and never enter Terraform state.

### Alternatives Considered
- **Terraform manages secrets** — simple, one command deploys everything, but secret values are stored in Terraform state in plain text
- **Plain text in connection string** — password embedded in ADF config; visible in ADF UI and logs
- **Environment variables** — secrets appear in CI logs; not suitable for production

### Reason
Secrets and infrastructure have different lifecycles. A Key Vault exists for years and is managed by the platform team. A secret is rotated every 90 days and is managed by the security team. Mixing them in the same Terraform apply couples two concerns that change at different rates and for different reasons.

Keeping secrets outside Terraform means secret values never appear in `terraform.tfstate`, password rotation requires no Terraform run, and the pattern matches enterprise practice where a dedicated secrets pipeline manages credentials independently of infrastructure provisioning.

### Consequences
- ADF Managed Identity granted `Get` and `List` permissions on Key Vault secrets
- Secret bootstrap is a manual step documented in README
- Password rotation: update SQL first, then update Key Vault secret via CLI — ADF picks up the new value automatically on next run

### Dependency design
Key Vault access policy is provisioned as a standalone resource in `terraform/main.tf` — not inside any module. This breaks the circular dependency between `module.adf` (needs `key_vault_id`) and `module.keyvault` (needs `adf_principal_id`). Cross-module wiring belongs at the root level.

---

## ADR-004: Cost Management — Destroy After Every Session

### Status

Superseded by ADR-021 on 2026-09-28. The rule held while the stack was Azure
only and everything in it billed by the hour. It stopped being worth the effort
once Snowflake, a storage integration and an Entra service principal joined it:
the rebuild is now a 40 minute runbook plus a consent flow, paid every session
to save idle compute that suspends itself anyway.

### Decision
Run `terraform destroy` at the end of every dev session and `terraform apply` at the start of the next. Infrastructure is treated as ephemeral during development.

### Alternatives Considered
- **Leave infrastructure running** — simpler workflow, but Databricks Premium and Azure SQL accrue cost 24/7 even when idle
- **Pause individual services** — Databricks auto-suspends, SQL serverless auto-pauses, but the workspace and server still incur standing charges

### Reason
The project uses cost-sensitive services (Databricks Premium SKU, Azure SQL serverless). Destroying after each session eliminates all standing charges. Remote Terraform state in Azure Storage persists between sessions so the full stack can be recreated reliably with a single command.

### Consequences
- Secret bootstrap is required after every `terraform apply` — not just the first deployment
- Full redeploy takes approximately 10 minutes per session (Databricks workspace provisioning is the slowest resource)
- In production this pattern would not be used — infrastructure is persistent and secrets are managed by a dedicated rotation pipeline

---

## ADR-005: Regional Deployment Constraint — SQL Server in francecentral

### Decision
Deploy Azure SQL Server in `francecentral` while all other services remain in `uksouth`.

### Alternatives Considered
- **Raise a support request to unlock uksouth** — possible but slow; not justified for a portfolio project
- **Move all services to francecentral** — unnecessary redesign; only SQL is blocked
- **Replace Azure SQL with a different service** — would change the watermark architecture without addressing the root cause

### Reason
The free trial subscription blocks SQL Server provisioning in `uksouth` with error `ProvisioningDisabled`. Investigation confirmed `francecentral` allows Basic tier SQL on this subscription. This is a deployment constraint specific to the free trial, not an architectural choice.

The fix is minimal: a dedicated `sql_location` variable defaults to `francecentral` and is passed only to `module.sql`. All other modules continue to use `var.location = "uksouth"`.

### Consequences
- Minor cross-region latency between ADF (uksouth) and SQL (francecentral) — negligible for a watermark lookup that runs once per pipeline execution
- In production all services would be co-located in a single region for latency, data residency, and cost optimisation

---

## ADR-006: ADLS Upload Chunk Size

### Decision
Set `chunk_size=4 * 1024 * 1024` (4MB) explicitly on all ADLS Gen2 uploads via the Python SDK.

### Reason
The default chunk size in `azure-storage-file-datalake` is 100MB. Files smaller than 100MB are sent as a single HTTP request body. On a constrained connection, a single 48MB write exceeds the OS socket write timeout, which the SDK has no parameter to control — `timeout` on `upload_data()` is a server-side query parameter, and `read_timeout` on the client covers response waiting only.

Setting `chunk_size=4MB` splits the file into smaller writes, each completing well within the OS timeout.

### Investigation
The root cause was identified by eliminating variables in order:
1. Small file upload succeeded — confirmed size was the only variable
2. `timeout=300` on `upload_data()` had no effect — confirmed it is a server-side hint
3. `read_timeout=300` on the client had no effect — confirmed the failure was a write timeout, not a read timeout
4. SDK source (`_upload_helper.py`) confirmed `chunk_size` defaults to 100MB

### Consequences
- All upload scripts must set `chunk_size` explicitly — do not rely on the SDK default
- `max_concurrency=1` is paired with small chunk size on slow connections to avoid parallel writes competing for bandwidth

---

## ADR-007: Selecting an Incremental Ingestion Strategy for a Static File Source

### Status

Superseded by ADR-008 — the project moved from a static UCI CSV source to
a self-built PostgreSQL OLTP source. Retained for historical record.

### Context

The project uses the UCI Online Retail dataset, which is provided as a single
historical CSV file rather than a live operational source.

The goal is to demonstrate an end-to-end Azure Data Engineering platform
(Terraform, ADF, Databricks, Snowflake and dbt) while keeping the project
appropriate for a Junior/Mid-level portfolio.

Several ingestion patterns were evaluated before implementation.

### Decision

ADF copies the complete source CSV into ADLS.

Databricks applies incremental processing using the watermark stored in Azure
SQL (`InvoiceDate > last_watermark`).

ADF is responsible for orchestration, while Databricks is responsible for data
processing.

### Alternatives Considered

**Option 1 — Full file copy with Databricks watermark (Accepted)**
Simple implementation with the lowest complexity.
Watermark filtering is performed during transformation.

**Option 2 — Daily file ingestion**
Cleaner file-based incremental pattern.
Rejected due to additional preprocessing of the historical dataset.

**Option 3 — Azure SQL source**
Implements Microsoft's recommended watermark pattern.
Rejected because it introduces an artificial operational source.

**Option 4 — Event-driven ingestion**
Suitable for production event-driven systems.
Rejected because demonstrating watermark-based processing was a primary project objective.

### Rationale

The selected approach was considered the best balance between implementation
effort, learning value and overall portfolio scope. It allows the project to
focus on the complete Azure platform rather than optimising a single ingestion
component.

### Consequences

- Incremental filtering is performed in Databricks rather than ADF.
- ADF demonstrates orchestration using Lookup, Copy and Stored Procedure activities.
- The ingestion layer is intentionally simplified.
- The downstream architecture (Databricks, Snowflake and dbt) is independent of the ingestion strategy.

### Production Considerations

For production systems, a different ingestion pattern would normally be used:

- query-based watermark extraction for database sources;
- file-based incremental ingestion for daily file drops.

The current implementation is an intentional trade-off made for the objectives
of this portfolio project.

---

## ADR-008: Migrating from a Static CSV Source to a Self-Built PostgreSQL OLTP Source

### Status

Accepted. Supersedes ADR-007.

### Context

ADR-007 accepted the UCI Online Retail CSV as the project's source, with
`InvoiceDate` as an artificial watermark column and hand-picked bad rows
standing in for real operational messiness. In practice this limited what
the DQ/dead-letter/DVT layers could demonstrate — the "bad data" scenarios
were manufactured on demand rather than arising from the shape of a real
source system.

### Decision

Replace the static CSV with a self-built PostgreSQL 16 OLTP database
(`database/`) that models a normalized, multi-country retail operational
system (customers, orders, order_items, payments, products, stores,
employees, currencies, exchange_rates). `database/generate_data.py`
generates realistic transactional data with intentional but naturally
distributed operational messiness (nulls, duplicate business keys, negative
quantities, invalid statuses, missing FX rates), rather than manufacturing
specific rows for specific test cases.

### Alternatives Considered

**Option 1 — Keep the UCI CSV (status quo)**
Simplest, but the watermark column (`InvoiceDate`) is a source-assigned
business date, not the server-assigned `created_at`/`updated_at` a real
production pipeline would use — a gap ADR-002 explicitly flagged as a
limitation.

**Option 2 — Adopt an existing sample database (e.g. Pagila)**
Considered and rejected earlier in the project. Pagila is already clean and
normalized, which would weaken the DQ/dead-letter story, and its DVD-rental
domain doesn't naturally need multi-currency FX conversion.

**Option 3 — Self-built PostgreSQL OLTP source (Accepted)**
More upfront effort (schema design, data generator), but gives full control
over which realistic problems exist and lets the watermark be the genuine
production pattern (`updated_at` + trigger) rather than a documented
limitation.

### Rationale

A self-built operational source turns two previously-documented weaknesses
into strengths: the watermark column is now the real production pattern
(closing the gap ADR-002 flagged), and "bad data" arises naturally from
`generate_data.py`'s randomized generation rather than being hand-inserted
for a specific test.

### Consequences

- `ingestion/watermark/watermark_control.sql` now seeds one row per
  `retail_oltp` table instead of two CSV/API-specific pipelines.
- A local Python extractor implements the watermark contract (reads
  `pipeline_config`/`pipeline_watermark_control`, writes Parquet to ADLS
  `landing/<table>/`) — see `docs/architecture.md`'s Implementation Status.
  **Superseded in practice:** the ADF pipeline `pl_load_data` now implements
  this contract directly, writing JSON to `raw/<table>/` instead. The
  `landing` container was never used and has been removed.
- `ingestion/upload_raw_data.py` (CSV → ADLS upload) is retired — replaced
  by the per-table extractor above.
- ADR-002's "Limitations" section is updated: the watermark column
  limitation it flagged is resolved by this change.
- ADR-007 is superseded but retained for historical record.

---

## ADR-009: Hybrid PostgreSQL Deployment — Local Docker for Development, Azure Flexible Server for ADF

### Status

Accepted

### Context

ADR-008 moved the source system from a static CSV to a self-built PostgreSQL
OLTP database. That database initially ran only in local Docker
(`database/docker-compose.yml`). Azure Data Factory cannot reach a
database running on a developer's laptop without a Self-hosted Integration
Runtime — a real, standard pattern, but one that adds meaningful
operational overhead (service installation, registration keys, host
networking, ongoing availability) for a portfolio project's marginal
benefit over simply describing the pattern correctly.

### Decision

Run PostgreSQL in two places, for two different purposes:
- **Local Docker** (`database/docker-compose.yml`) — fast schema iteration
  and tuning `generate_data.py`'s intentional messiness, no cloud cost or
  round-trip.
- **Azure Database for PostgreSQL Flexible Server**
  (`terraform/modules/postgres/`) — the instance ADF actually connects to.
  Same `database/seed.py` and `database/generate_data.py` scripts populate
  either target; only `DATABASE_URL` differs.

### Alternatives Considered

**Option A — Local Docker only**
Free and fast, but structurally cannot deliver the project's own stated
architecture ("ADF — watermark-based incremental ingestion") without added
machinery ADF can't reach a laptop directly.

**Option B — Azure PostgreSQL only**
Architecturally correct and simplest to reason about, but loses fast local
iteration for schema/data-generator changes, and costs run continuously
during active development.

**Option C — Hybrid (Accepted)**
Combines both: fast local loop for development, cloud instance for
anything that needs to be real (ADF connectivity). No sync/migration
pipeline needed between them — `seed.py`/`generate_data.py` already take
`DATABASE_URL` as a parameter, so "hybrid" is just running the same
scripts against a different connection string, not a separate mechanism.

### Consequences

- `terraform/modules/postgres/` provisions the cloud server, firewall rule,
  and database, following the same three-resource shape as
  `terraform/modules/sql/`.
- `postgres_admin_password` follows the existing Key Vault bootstrap
  pattern from ADR-003 (`session.sh` → `terraform apply` →
  `scripts/bootstrap_keyvault.sh`).
- `scripts/bootstrap_postgres.sh` opens a firewall rule for the developer's
  current IP each session, mirroring `scripts/bootstrap_watermark.sh`'s
  existing pattern for the SQL server.
- `DATABASE_URL` has no default fallback (see `utils/db.py`,
  `database/seed.py`, `database/generate_data.py`) — it must be set
  explicitly via `.env`, so the target environment is always a conscious
  choice, never an accidental one.
- Under ADR-004's cost discipline, the Flexible Server is destroyed and
  recreated each session along with the rest of the stack; schema and data
  are reloaded via `database/schema.sql` and the generator scripts, not
  restored from a backup.

---

## ADR-010: Curated Zone Layout, One Delta Table Per Source Table

### Status

Accepted

### Context

`docs/architecture.md` originally described the curated zone as a single
`curated/retail/` dataset partitioned by date, implying the raw tables are
joined on the way in. The Databricks transformation notebook needs to be
restartable, and a single joined output makes a failure on one source table
a failure for all of them.

### Decision

The curated zone holds one Delta table per source table, at
`curated/<table_name>/`, mirroring the `retail_oltp` shape. Each is
registered as an external table `retail_dev.curated.<table_name>`.

No joins happen in the curated zone. All joining, FX enrichment and
denormalisation happen on the way to `served/`.

Auto Loader checkpoints live at `curated/_checkpoints/<table_name>/`. The
underscore prefix keeps them out of the table namespace, and they are
deliberately not registered as tables.

### Alternatives Considered

**Option A: a single joined `curated/retail/` dataset.**
Fewer objects, and the served zone becomes a thin formatting step. Rejected
because one bad source table blocks every table, a re-run reprocesses
everything, and the curated layer stops mirroring the source, which makes
lineage harder to explain.

**Option B: Parquet rather than Delta.**
Simpler and readable by anything. Rejected because incremental `MERGE`
requires a transaction log. Without Delta, an upsert becomes read-all,
rewrite-all, which defeats the point of incremental loading.

### Rationale

Per-table isolation is what makes step 4 restartable table by table. A
failure in `order_items` leaves `customers` loaded and committed. Each table
gets its own Delta log, its own checkpoint, and its own schema evolution,
so adding a column to one source table cannot disturb another.

### Consequences

- `docs/architecture.md`'s zone diagram is updated to `curated/<table_name>/`.
- The served zone is the only place a join exists, which makes the grain
  rule in ADR-011 enforceable in exactly one place.
- Checkpoint directories sit inside the curated container but are not tables.
  Anything enumerating the container must skip paths beginning with `_`.

---

## ADR-011: Grain of the Served Fact File, One Row Per Order Line

### Status

Accepted

Amended on 2026-09-28 by ADR-020. The grain rule stands unchanged, but its
subject moves: there is no fact file in the served zone. Served carries
source-shaped tables, and one row per `order_item_id` is the grain of
`fact_sales` in dbt.

### Context

Grain is the single most consequential modelling decision, and the majority
of dimensional modelling bugs are grain violations rather than logic errors.
It must be stated once, unambiguously, and never violated.

### Decision

**The served fact file has exactly one row per
`retail_oltp.order_items.order_item_id`.**

`sale_id` is derived deterministically from `order_item_id`, not generated.
This matters because dbt's incremental models use `unique_key = 'sale_id'`:
a randomly assigned surrogate key would change on every re-run and the
incremental merge would insert duplicates instead of updating rows.

**The additivity rule that follows from this grain:**

Order-level amounts (`orders.subtotal_amount`, `tax_amount`,
`shipping_amount`, `discount_amount`, `total_amount`, and
`payments.payment_amount`) repeat across every line of an order. They must
never be summed at line grain. Only line-level measures are additive:
`quantity`, `unit_price`, `discount_amount` and `tax_amount` from
`order_items`, and `line_total_amount`.

### Alternatives Considered

**Option A: one row per order.**
Simpler, and order totals become directly additive. Rejected because it
discards the product dimension entirely. No product-level analysis is
possible, which removes most of the point of `dim_product`.

**Option B: one row per payment.**
Rejected. Payments happen to be 1:1 with orders in the generated data, but
that is an artefact of `generate_data.py` inserting exactly one payment per
order, not a property of the domain. Partial payments and refunds would
break it.

### Rationale

Line grain is the standard retail sales fact, it supports both product and
order analysis, and it matches the `unique_key` the dbt design already
assumes.

### Consequences

- Joining `payments` onto the fact fans out one payment across several
  lines. `payment_status` may be carried as a degenerate attribute;
  `payment_amount` must not be carried as a measure.
- Returns are negative-quantity lines flagged `is_return`, per the decision
  that negative quantity is a return rather than a bad record. They remain
  at line grain and stay additive.
- A fan-out assertion belongs in the DVT suite: the row count of the served
  file must equal the row count of `curated.order_items` for the same
  increment. Any join that silently multiplies rows fails this immediately.

### Amendment, 2026-08-28: `order_status` is carried as a degenerate dimension

The original decision above asked which order-level **measures** must be kept
out of the fact, and answered correctly. It never asked which order-level
**attributes** the fact needs, and that omission left a defect.

Without `order_status`, a CANCELLED order contributes to revenue exactly like
a COMPLETED one, and nothing in `fact_sales` can tell them apart. Measured on
the seeded dataset: CANCELLED is 121 orders and 11,802.33 of 130,367.59 in
line revenue, which is 9.1%. PENDING adds a further 2.9% that is not yet
realised revenue at all. Every headline figure in the project was overstated
by roughly a ninth.

**Decision: `order_status` is carried into `fact_sales` as a degenerate
dimension**, alongside `order_id` and `order_number`.

It is an attribute, not a measure, so it does not conflict with the
additivity rule above. Repeating it across the lines of an order is correct:
you filter on it, you never sum it.

**Degenerate rather than its own dimension.** Six values, no attributes of
their own, and no hierarchy. A `dim_order_status` would add a join to every
revenue query in exchange for nothing.

Consequences:

- Every revenue figure must filter on `order_status`. This is not optional,
  and a report that omits the filter is wrong rather than merely incomplete.
- `order_status` and `is_return` are not interchangeable. `is_return` is line
  level, derived from `quantity < 0`. `order_status = 'RETURNED'` is order
  level. An order can be RETURNED while its lines stay positive, and a
  negative line can appear under any status.
- Unknown values are quarantined rather than passed through. A status nobody
  has seen before is an unknown answer to "does this count as revenue?", and
  letting it flow silently defaults that answer to yes. The whitelist lives in
  `transformation/config/table_config.py`; `orders.order_status` has no check
  constraint in the source, so the pipeline is the only place this is caught.

---

## ADR-012: FX Conversion to GBP, and an Interim Dead-Letter Location

### Status

Accepted

Amended on 2026-09-28 by ADR-020. FX conversion moves from Databricks to dbt,
so the Databricks stage no longer joins `exchange_rates` and cannot detect a
missing rate. The conversion formula and the rule against loading a NULL
measure are unchanged.

Missing rates are caught in two places instead. They are prevented:
`simulate_source_changes.sh` fails when any order lacks a rate for its date,
so a missing rate is an operational error and not an expected condition.
`bootstrap_fx_rates.sh` only reports the count of uncovered orders and does
not fail. And they are contained: in dbt the FX join splits
into two models from one source, rows with a rate feeding `fact_sales` and rows
without landing in a rejected table. A dbt test cannot do this, because a test
fails a run rather than routing a row.

Dead-letter therefore has two homes, one per stage: the Delta table for
ingestion rejects, a Snowflake table for modelling rejects. Both surface in the
dashboard.

Amended on 2026-10-09: dead-letter also reaches Snowflake, see the amendment
at the end.

### Context

`ingestion/api_ingest/fetch_fx_rates.py` fetches with `BASE_CURRENCY=GBP`,
so every row in `retail_oltp.exchange_rates` is GBP-based: the rate answers
"how many units of the target currency per one GBP".

Orders are not all in GBP. `database/seed.py` seeds stores in GB, DE, FR and
CA, and `generate_data.py` sets `orders.currency_code` from the store's
country. The fact table therefore holds GBP, EUR and CAD orders, and summing
their amounts as they are adds pounds to euros.

Separately, `dead_letter` is specified as a Snowflake table. Snowflake does
not exist yet, and the Databricks stage needs somewhere to put rejected
records now.

### Decision

**Reporting currency.** Every amount is reported in GBP, the retailer's home
currency and the base of the rates. An order in currency `C` converts by:

```
gbp_per_C = 1 / rate(GBP -> C)
```

For `C = GBP` the rate is 1 by definition. The source cannot store that row
(`base_currency_code <> target_currency_code`), so dbt adds it for each day.

dbt computes the general cross rate `rate(GBP -> R) / rate(GBP -> C)`, with
the base and the reporting currency `R` as vars (`fx_base_currency`,
`fx_reporting_currency`). Reporting in another currency is a config change.

**Fact columns.** `unit_price_original` and `total_original` keep the amount
in the order's own `currency_code`. `fx_rate_to_gbp` holds the rate actually
applied and `total_gbp` the converted amount, so every converted value is
reproducible from the row itself.

**Missing rates.** If no rate exists for a given `(rate_date, currency)`,
the row is routed to `fact_sales_rejected` with
`error_reason = 'missing_fx_rate'`. It is not loaded with a NULL `total_gbp`,
because a NULL measure silently understates every downstream sum.

**Dead-letter location.** `dead_letter` is a Delta table in ADLS at
`curated/_dead_letter/`, registered as `retail_dev.ops.dead_letter`, until
the Snowflake warehouse exists. `raw_payload` is stored as a `STRING`
containing JSON rather than a semi-structured type, which maps cleanly onto
Snowflake's `VARIANT` via `PARSE_JSON` when the table migrates.

### Alternatives Considered

- **Report in USD via cross rates:** every currency then depends on the USD
  rate, so one missing USD rate rejects a whole day.
- **Fetch rates with each order currency as base:** one API call per
  currency, and independent bases can disagree slightly.
- **Store only `total_gbp`:** loses what the customer actually paid, and a
  wrong rate becomes unrecoverable.
- **Allow NULL `total_gbp` when a rate is missing:** simplest, and the
  failure is invisible.

### Consequences

- GBP orders never miss a rate; `missing_fx_rate` can only affect EUR and CAD
  orders.
- Migrating `dead_letter` to Snowflake is a known future task, and the
  `STRING`-holding-JSON choice exists specifically to make it cheap.
- The API returns rates for every day including weekends, so no
  `missing_fx_rate` rejection occurs with the current data. The
  `int_order_lines_routed` unit test covers the path instead.
- `fact_sales.fx_rate_to_gbp` and `total_gbp` are `not_null`-tested on every
  build (ADR-024).

### Amendment, 2026-10-09: dead_letter reaches Snowflake through served

- **Path.** `curated/_dead_letter` is exported, manifested and loaded like a
  curated table: `export_source()` resolves its path and columns, Change Data
  Feed is on, and the first export is a `full_reload` snapshot. It lands in
  `raw.dead_letter`; `stg_dead_letter` keeps the newest version of each
  `record_id`.
- **Why.** DVT needs rejected keys next to staging (ADR-025), and the
  dashboard reads Snowflake.
- **Delta stays the writer.** Databricks writes rejects; Snowflake holds a
  copy one export later. A `reprocessed` change arrives through CDF.
- **`raw_payload`** stays VARCHAR in raw; queries that need fields use
  `PARSE_JSON`.
- **Supersedes:** "until the Snowflake warehouse exists" in the Decision, and
  the Consequences bullet that the migration is a future task.

---

## ADR-013: Dimension Shape, Star Over Snowflake, and an Unknown Member for Guest Checkouts

### Status

Accepted

### Context

ADR-008 replaced the UCI CSV source with a self-built PostgreSQL OLTP schema.
The dimension definitions were never systematically revised afterwards, so
they still described UCI columns (`StockCode`, `Description`, `CustomerID`)
that do not exist in the new source. Two genuine modelling choices were
buried inside that stale description and had never been decided explicitly.

The first: `retail_oltp` normalises `products` against `product_categories`
and `suppliers`. A dimensional model can preserve that normalisation or
collapse it.

The second: `generate_data.py` produces guest checkouts by returning `None`
from `insert_customer()` for roughly 8% of orders, so `orders.customer_id`
is legitimately NULL. The dead-letter rules said null `customer_id` should be
routed to dead-letter, which would discard 8% of all sales.

### Decision

**Star, not snowflake.** `category_name` and `supplier_name` are flattened
into `dim_product`. `product_categories` and `suppliers` are still ingested
into the curated zone, but never become dimensions of their own.

**Guest checkouts map to an unknown member.** A NULL `orders.customer_id`
resolves to `customer_key = -1` in `fact_sales`.

**`dim_customer` must physically contain that row.** The unknown member is an
artificial row inserted by the model, not an implied value. It carries
`customer_key = -1`, `customer_id = NULL`, and a recognisable label such as
`customer_number = 'UNKNOWN'`.

**The dead-letter condition on null `customer_id` is removed.** Databricks
stage rejections are now: zero quantity, invalid `payment_status`, unresolved
`product_id`, and missing FX rate.

### Alternatives Considered

**Snowflake the product dimension.**
Keeps `dim_category` and `dim_supplier` separate, avoids repeating category
names across products, and makes a category rename a single-row update.
Rejected because it adds two joins to every product query for a dimension
with 10 products and 10 categories, where the duplication it prevents is
measured in kilobytes.

**Route guest checkouts to dead-letter, as originally written.**
Rejected. A guest sale is a real sale. Revenue is revenue whether or not the
buyer is known, and discarding 8% of transactions would misstate every
revenue figure in the project.

**Leave `customer_key = -1` as a convention without inserting the row.**
Rejected, and this is the failure mode worth naming. Without the physical
row: the fact-to-dimension join finds no match, dbt's `relationships` test on
`fact_sales.customer_key` fails, and every customer-segmented report silently
drops 8% of sales. The convention only works if the row exists.

### Rationale

The distinction driving both halves of this ADR is **legitimate absence
versus data error**.

A guest checkout is a legitimate absence. The business genuinely does not
know who the customer was, and that is a normal, expected outcome. It gets an
unknown member so the sale stays in the fact table and stays countable.

An unresolved `product_id` is a data error. `generate_data.py` produces it by
writing `source_product_sku = 'UNKNOWN-xxxx'` with a NULL `product_id` for
about 2.5% of lines, simulating a broken reference. There is no product
context to recover, so the row goes to dead-letter for investigation rather
than being silently attributed to a placeholder product.

Same NULL, different meaning, different handling. Deciding which one applies
is a modelling judgement, not a technical one, which is exactly why it needs
recording.

### Consequences

- `dim_customer` gains one artificial row. Any row count assertion on that
  dimension must account for it: `count(dim_customer) = count(customers) + 1`.
- DVT should assert the unknown member exists before `fact_sales` loads.
  If it is missing, the fact load produces orphan keys rather than failing.
- dbt's `relationships` test on `fact_sales.customer_key` becomes meaningful:
  it now catches genuine referential breaks, because the expected NULL case
  has a home.
- `dim_product` carries denormalised `category_name` and `supplier_name`. A
  category rename requires updating every affected product row, which is the
  accepted cost of the star.
- The schema definitions these decisions describe are moving out of
  `CLAUDE.md`, which is gitignored, into `docs/`. Agent instructions belong
  in `CLAUDE.md`; schemas, grain and data quality rules are project
  artefacts and must be committed.

---

## ADR-014: A Dedicated Container for the Unity Catalog Managed Location

### Status

Accepted

### Context

Creating the `retail_dev` catalog failed with `INVALID_STATE: Metastore
storage root URL does not exist. Default Storage is enabled in your account.`
The auto-provisioned metastore has no root storage, so a catalog must either
use Databricks Default Storage or be given an explicit `MANAGED LOCATION`.

Every table in this project is external, with its path stated in the
`CREATE TABLE`. The managed location should therefore never be used. The
question is what happens when it is used by accident.

That accident is not hypothetical. These two lines differ by one argument:

```python
df.write.saveAsTable("retail_dev.curated.orders")            # managed
df.write.option("path", ...).saveAsTable("retail_dev...")    # external
```

Omitting `option("path", ...)` creates a managed table. It succeeds silently:
the table appears, the data is queryable, and nothing indicates the bytes
went somewhere unintended.

### Decision

Create a dedicated `managed` container in the `retailpipelinedevx7k` storage
account, register it as external location `retail_managed`, and point the
catalog at it:

```sql
CREATE CATALOG retail_dev
MANAGED LOCATION 'abfss://managed@retailpipelinedevx7k.dfs.core.windows.net/';
```

The container is expected to stay empty for the life of the project.

### Alternatives Considered

**Databricks Default Storage.**
Zero configuration, and no possibility of overlapping the data containers.
Rejected because an accidental managed table would land in Databricks-owned
storage: outside the storage account, outside Azure Cost Management, and
unreachable by any tool that is not Databricks. It would also introduce a
second storage location that `docs/architecture.md` does not describe.

**`MANAGED LOCATION` inside the `curated` container.**
No new container. Rejected because it places managed storage in the same
container as external tables, and correctness then depends on every future
path staying outside the managed prefix. A container boundary does not
depend on anyone remembering anything.

### Rationale

Both rejected options are safe while nothing goes wrong. The chosen one is
the only one that is still recoverable when something does. The mistake it
guards against is silent, one keyword wide, and sitting in the exact code
path we are about to write.

The cost is one Terraform resource and one external location.

### Consequences

- `terraform/modules/adls/main.tf` gains a `managed` filesystem.
- A non-empty `managed` container is a signal, not a normal state: it means
  a managed table was created by mistake and should be investigated.
- Verified empirically rather than assumed: Unity Catalog accepts a managed
  location that is covered by an external location, provided it is a
  different container from the external tables. `CREATE CATALOG` returned
  `OK` and all three schemas were created under it.

---

## ADR-015: Targeting Spark 4 Locally, and Decoupling Type Conversion from ANSI Mode

### Status

Accepted. The Spark 4 assumption below was a prediction when this ADR was
written, with no workspace to test it against. It was measured on serverless
compute on 2026-09-17 and holds:

```
spark version:     4.2.0
ansi enabled:      true
session timezone:  Etc/UTC
```

The serverless runtime currently serves exactly the `pyspark==4.2.0` pinned
locally, and ANSI is on by default rather than by configuration. Neither is
guaranteed to stay true: serverless moves on Databricks' schedule, which is the
reason this ADR argues for declaring `spark.sql.ansi.enabled` explicitly instead
of relying on the version default.

### Context

`CLAUDE.md` recorded the transformation stack as Python 3.11 and Spark 3.5.
That pin came from the project's own early notes rather than from any external
constraint, and it was never re-derived after the Databricks design settled on
serverless compute.

Serverless changes the question. On classic compute the Databricks Runtime
version is chosen at cluster creation, so a local environment can be matched to
it. On serverless the runtime is managed by Databricks and moves forward on its
own schedule. There is no version to match, which means exact local parity is
not merely inconvenient, it is unattainable by construction.

The real constraint is therefore not the documented pin but the runtime this
code will execute on when Azure returns in September 2026, which will be on the
Spark 4 line rather than 3.5.

Package compatibility was verified against PyPI metadata rather than assumed:

- `delta-spark` 3.x declares `pyspark>=3.5.0,<3.6.0`
- `delta-spark` 4.4.0 declares `pyspark>=4.0.1,<=4.2.0`
- `pyspark` 3.5.x declares Python support up to 3.11 only
- `pyspark` 4.2.0 declares Python 3.10 through 3.14

The local machine already runs Python 3.13.7 and OpenJDK 17.

### Decision

**Local development targets the Spark 4 line.** `requirements.txt` pins
`pyspark==4.2.0`, `delta-spark==4.4.0`, and `pyarrow>=18.0.0`, running on the
existing Python 3.13 interpreter and OpenJDK 17. No interpreter downgrade and
no virtualenv rebuild are required.

**Type conversion from the raw zone does not depend on the ANSI default.**
Raw JSON fields that can carry operational garbage are read as strings and
converted with `try_cast`, which returns NULL on failure regardless of the ANSI
setting. A failed conversion is then classified by the ordinary data quality
rules and routed to dead-letter. This is what the dead-letter design requires:
a bad value must survive long enough to be classified, not abort the batch.

**`spark.sql.ansi.enabled` is still declared explicitly in
`transformation/spark_session.py`, set to `true`, and set to the same value on
the Databricks side.** ANSI governs more than casts, including arithmetic
overflow and division by zero, and those are defects in code we write rather
than in data we receive.

The division of responsibility is therefore: ANSI enabled for the logic we
control, so it fails loudly, and `try_cast` for the data we do not trust, so it
fails into dead-letter.

### Alternatives Considered

**Pin local development to Spark 3.5 to match a Databricks Runtime.**
The conventional answer, and correct on classic compute. Rejected because
serverless exposes no runtime version to match, so the parity being bought is
imaginary. It would also force a downgrade to Python 3.11, a virtualenv
rebuild, and a re-run of the existing test suite, all to align with a version
that will not be running in production.

**Leave ANSI mode at the version default.**
Rejected, and this is the failure mode worth naming. With ANSI disabled a
failed cast returns NULL silently; with ANSI enabled it raises. Spark 3.5
defaults to disabled and Spark 4.0 defaults to enabled. The dead-letter
classification in this project is built entirely on how invalid values behave,
so inheriting the default would make data quality semantics a function of
whichever runtime Databricks happens to be serving that month. Rules tested
against silent NULLs would begin failing whole jobs after a runtime upgrade
nobody requested.

**Disable ANSI globally so that failed casts yield NULL.**
The simplest way to keep the dead-letter path working, and the initial
instinct. Rejected because it buys the soft behaviour everywhere rather than
only where it is wanted. An arithmetic overflow in the FX conversion, or a
division by zero in a computed measure, would also degrade to NULL and pass
silently into `fact_sales`. `try_cast` confines the soft behaviour to the exact
conversions that need it.

**Defer local Spark entirely and first execute the code on Databricks in
September.**
Rejected on three grounds. Modules importing `pyspark.sql.types` cannot be
import-checked at all without the package, so several hundred lines would be
unverifiable by any tool. The bugs that matter most here are silent: a join
that violates the ADR-011 grain produces a valid DataFrame and a wrong revenue
figure rather than an exception. And under ADR-004's destroy-after-session cost
discipline, each September debugging cycle costs an infrastructure rebuild and
a cluster start, making it the most expensive possible place to discover a
typo.

### Rationale

On serverless, "which Spark version" has no stable answer, so the durable move
is to stop depending on version defaults at all.

Pinning the local packages buys a working feedback loop, where a mistake costs
seconds instead of a cluster start. Using `try_cast` removes the one behaviour
whose drift would actually corrupt results, and removes it at the point of
conversion rather than through a global switch. Together they replace an
unattainable goal, matching the cluster, with an achievable one: making the
behaviour that matters independent of the cluster.

### Consequences

- `CLAUDE.md` and `README.md` updated from Python 3.11 to 3.13, and the
  transformation row now distinguishes local pins from the serverless runtime.
- `requirements.txt` gains three pinned dependencies.
- `transformation/spark_session.py` must set `spark.sql.ansi.enabled`
  explicitly, with a comment stating why, so it is not tidied away later as
  redundant configuration.
- `try_cast` is the required conversion function for any field arriving from
  the raw zone. A plain `cast` in that path is a defect, because it makes the
  dead-letter route depend on a session setting.
- `try_cast` cannot by itself distinguish a value that was NULL in the source
  from a value that failed to parse. Where that distinction matters, and per
  ADR-013 it does for `orders.customer_id`, the raw string must be tested for
  NULL before conversion. That produces two separate `error_reason` values
  rather than one.
- Local Delta tables never leave the laptop, so Delta protocol compatibility
  with the cluster is not a concern.
- Auto Loader (`cloudFiles`) remains untestable locally. `read_raw()` is the
  single boundary function whose body changes when the pipeline moves to
  Databricks.
- When compute is created in September, the first thing to verify is the Spark
  version actually served, and whether ANSI is enabled there. If the project
  ends up on classic compute pinned to Spark 3.5, this decision needs
  revisiting.

---

## ADR-016: Capturing the Dead-Letter Raw Payload Before Type Conversion

### Status

Accepted

### Context

`raw_payload` must show what actually arrived, for investigation and for the
`reprocess_dead_letter` DAG. But `try_cast` (ADR-015) turns an unconvertible
value into NULL, so a payload built after casting shows `null` for exactly
the rows rejected because of that value. A malformed line is worse: all
columns are NULL, so its payload would be `{}`.

### Decision

`add_raw_payload()` in `transformation/raw_payload.py` runs between
`read_raw()` and `cast_to_target()` and adds `_raw_payload` as a JSON string:

- malformed line: the original text from `_corrupt_record`
- otherwise: the source columns via `to_json`, keeping NULLs as `null`
  (`ignoreNullFields` disabled)

It passes through casting unchanged, is dropped before the MERGE, and is
stored only in dead-letter.

### Alternatives Considered

- **Build it at dead-letter time:** loses unconvertible values.
- **Inside `read_raw()` or `cast_to_target()`:** mixes two responsibilities
  into one function.
- **Re-read the file via `_source_file`:** exact bytes, but costly, and the
  file may be gone by the time someone reprocesses.

### Rationale

Evidence must be captured at the last point it is still true.

### Consequences

- Faithful to values, not byte-identical: numbers appear as strings. Exact
  bytes remain recoverable via `_source_file`.
- Contains personal data, so dead-letter needs source-level access control.

### Amendment: the payload is evidence, not identity

`raw_payload` is not stable between runs. ADF writes amounts as JSON number
tokens (`0.00`), the reader declares every column a string, and Spark rendered
the same token as `"0.00"` in one run and `"0.0"` in the next. `record_id`
hashed the payload, so a rerun with no new data re-inserted 111 known problems.

**Decision:** `record_id = xxhash64(source_table, source_key, updated_at,
error_reason, _corrupt_record)`. A parsed row is identified by its version
(primary key plus `updated_at`, which the source trigger changes on every
update) and its reasons. A malformed line has neither, so its exact text
identifies it. `raw_payload` stays in the table for people to read.

Records holding only a BOM or whitespace, which ADF writes for a 0-row copy,
are dropped before validation instead of being dead-lettered.

**Consequences:**

- Corrects the first consequence above: a number is not always rendered as
  the same string.
- A row changed in the source gets one record per version.
- The new formula changes every existing id, so `dead_letter` is emptied once
  and rebuilt from raw when this is deployed.

---

## ADR-017: Pinning the Spark Session Time Zone to UTC

### Status

Accepted

### Context

`to_date()` on a timestamp picks a calendar day using
`spark.sql.session.timeZone`, which defaults to the machine's zone. The
developer laptop runs Europe/London; Databricks serverless runs `Etc/UTC`
(measured 2026-09-17). `2026-08-26T23:30:00Z` is the 26th in UTC and the 27th
in London during summer. The FX join (ADR-012) matches
`to_date(order_date)` to `rate_date`, so the two environments would pick
different days' rates and produce different GBP totals, with no error.

### Decision

`REQUIRED_CONFIGS` in `transformation/spark_session.py` sets
`spark.sql.session.timeZone = UTC`. `build_local_spark()` applies it locally,
and `apply_required_configs()` applies it to the session Databricks provides.

### Alternatives Considered

- **Europe/London**, because the business is UK-based: daylight saving moves
  day boundaries in March and October, so the same pipeline would give
  different answers depending on the time of year.
- **Leave the default:** local tests and production silently disagree.

### Rationale

A setting that changes results must be declared, not inherited. UTC matches
how the source stores `timestamptz`, how ADF stamps watermarks, and has no
daylight saving transitions.

### Consequences

- Day boundaries in curated and served are UTC days. Reporting by UK local
  day must convert explicitly, in the served layer or dbt.
- `test_timestamp_maps_to_utc_calendar_day` fails if the setting is lost.

---

## ADR-018: Dimension and Status History via dbt Snapshots, Not in Curated

### Status

Superseded by ADR-020 on 2026-09-28.

Snapshots cannot run over curated: dbt uses the Snowflake adapter and curated
is Delta in ADLS. They could only run over `ecommerce_db.raw`, and under
ADR-020 raw is an append-only change log that already holds every delivered
version of a row. An SCD2 interval is then a window function over it, so a
snapshot would be a second history mechanism on top of the first, carrying the
same limitation that nothing before the pipeline exists.

What survives is the criterion in the Decision below: a table gets a history
model when it has a mutable attribute that a business question can ask about
"as of" a date. By that rule the candidates are still `customers`, `orders`,
`products` and `payments`, and `exchange_rates` never needs one.

### Context

Curated is a Type 1 mirror of the source: one row per key, latest state
(ADR-010). A MERGE overwrites the previous state, so questions such as how a
customer's status changed, or how many orders were in a given status on a
given date, cannot be answered from it.

### Decision

History is modelled with `dbt snapshot` (strategy `timestamp`, `updated_at`)
over curated tables. A table gets a snapshot when it has a mutable attribute
that a business question can ask about "as of" a date. Events do not: a fact
row carries its own date and its values as they were at the time, so
`fact_sales.unit_price_original` already answers "what price was charged".

By that rule the candidates are `customers`, `orders`, `products` and
`payments`; `exchange_rates` is a fact and never needs one. Snapshots are added
as questions appear, starting with `customers` and `orders`.

Snapshots produce interval rows (`dbt_valid_from`, `dbt_valid_to`), so a
point-in-time question is a range predicate:

    where <date> >= dbt_valid_from and (dbt_valid_to is null or <date> < dbt_valid_to)

They run in the same Airflow DAG as the transformation, immediately after it
and before the marts. Curated is unchanged.

### Alternatives Considered

- **Append-only or SCD2 curated:** every downstream model would then have to
  filter for the current version, and one forgotten filter doubles revenue
  silently. With dbt in the same DAG it also captures no extra versions.
- **Periodic snapshot fact (one row per order per day):** simpler queries, but
  volume grows as orders times days for answers the interval model already gives.
- **Accumulating snapshot (milestone columns):** compact, but assumes a
  forward-only flow, which `CANCELLED` and `RETURNED` break.
- **Delta Change Data Feed on curated:** free change log, but bounded by table
  retention and not a modelled history.

### Rationale

One mechanism answers every "as of" question, and curated stays a mirror that
cannot be misread.

### Consequences

- The snapshot must run on every pipeline run. A skipped run loses the versions
  that existed between runs.
- Changes within one run are never captured: the watermark delivers the latest
  state of a row, not every change. A full change log would require CDC on the
  source, which is out of scope.
- Adding a snapshot later is cheap: history can be backfilled from raw, so the
  initial set does not have to be complete.

## ADR-019: ADLS to Snowflake via Storage Integration, Stage and COPY INTO

### Status

Accepted

Amended on 2026-09-28 by ADR-020's amendment. COPY no longer names only its
own run's files: it names every manifest row from the last 7 days, so the
64-day load memory matters again and the window must stay well inside it.

### Context

Databricks writes the served zone to ADLS as Parquet, partitioned
`year=/month=/day=`. Snowflake has to receive it. Airflow already owns
orchestration end to end, and every load must be idempotent and auditable
because DVT runs against each increment.

### Decision

A storage integration authenticates Snowflake to ADLS through a service
principal in our Entra tenant, not a key or a SAS token, and holds Storage Blob
Data Reader on the `served` container alone. An external stage points at that
container, and `COPY INTO` loads the files named in the run's manifest
(ADR-020) into `ecommerce_db.raw`. Airflow issues the COPY, records its
per-file results in `pipeline_audit`, and only then runs DVT and dbt.

Snowflake remembers which files a table has already loaded and skips them, so
re-running the DAG is a no-op. That record expires after 64 days, which the
manifest makes irrelevant: a run only ever names its own files.

### Alternatives Considered

- **External tables:** no data movement, but every dbt model then scans ADLS,
  and partition metadata becomes another thing to refresh.
- **Snowpipe:** near real time, but the trigger lives in Azure, so the DAG
  cannot tell when a load finished without polling.
- **Databricks Snowflake connector:** fewer moving parts, but it couples the
  transformation tool to the warehouse and puts Snowflake credentials in
  Databricks.
- **SAS token on the stage:** no Azure role assignment needed, but the secret
  then lives inside the stage definition and expires.

### Rationale

Each tool keeps one job, Airflow keeps one place to look when a run fails, and
idempotency comes from the loader rather than from code we maintain.

### Consequences

- `COPY INTO` appends. Combined with the increment defined in ADR-020, this
  makes `ecommerce_db.raw` an append-only **change log rather than a mirror**:
  a key reappears with every change it undergoes. Deduplication therefore
  belongs to dbt's staging models, and no model may read `raw` directly.
- `COPY INTO` returns per-file results, which is what `pipeline_audit` records.
- Reloading files requires `FORCE`, so it is a deliberate, logged act.
- The integration spans two clouds, but all three parts are codeable:
  `azuread_service_principal` for Snowflake's fixed client id,
  `azurerm_role_assignment`, and the Snowflake provider's storage integration.
  Only the first run is manual, while that client id is still unknown.
- The stage is read only. Unloading from Snowflake back to ADLS would need
  Storage Blob Data Contributor.

## ADR-020: The Served Zone Contract

### Status

Accepted

### Context

Curated is a Delta mirror of the source, maintained by MERGE, with deletion
vectors enabled. Snowflake reads from the served zone (ADR-019). What served
contains, and what counts as one increment, determines what dbt must do and
what DVT can compare. Getting it wrong is not visible until a number in a
report is wrong.

### Decision

**Shape.** One directory per curated table, Parquet, in source shape. All
twelve, including those no mart uses yet: narrowing the set would couple
Databricks to dbt's model list. No joins and no derived columns, so the star
schema and the FX conversion both belong to dbt.

**Increment.** Rows are selected by Delta **version**, not by the source's
`updated_at`, and read through Change Data Feed, keeping `insert` and
`update_postimage`. `updated_at` is event time; a row delivered late by ADF's
lookback window (three days for orders) arrives in curated carrying an older
`updated_at`, and an `updated_at` watermark would skip it permanently. A Delta
version is processing time and cannot skip anything.

**Partitioning.** `served/<table>/year=/month=/day=` by run date.

**Manifest.** A failed Spark write leaves its finished task files in the
folder with no commit marker, and a later overwrite does not remove them.
Spark skips them; COPY INTO loads them. After each successful write, the
export records `inputFiles()` and the row count next to the table's Delta
version, and COPY INTO names exactly those files with `FILES = (...)`. A whole
partition is never loaded.

**Immutability.** A written file is never rewritten or deleted before it is
loaded. Two runs on one day leave two sets of files. The manifest and the
audit record refer to files by name.

**History.** No `dbt snapshot`. Because raw is a change log, an SCD2 interval
is derived from it directly, after deduplicating on `(<pk>, updated_at)`:

    dbt_valid_from = updated_at
    dbt_valid_to   = lead(updated_at) over (partition by <pk> order by updated_at)

The history is at pipeline cadence: states that changed twice between two ADF
runs appear once. ADR-018's criterion for which tables need history is
unchanged.

**Schema evolution.** None, in either direction. `MATCH_BY_COLUMN_NAME`
ignores unmatched file columns and writes NULL for unmatched table columns,
both without error, so drift is silent. A check compares `source_schemas.py`
field names against `INFORMATION_SCHEMA.COLUMNS` and fails on a difference.
Names only: a type change is a rebuild.

**Deletes.** Out of scope. The source never deletes and curated is built from
upserts only. Divergence would be caught by DVT's key comparison. A full
answer needs CDC on the source.

**Raw tables.** Twelve typed tables, written by hand with unquoted names, not
generated: a template from `INFER_SCHEMA` creates quoted lower-case names that
every model would have to quote. The schema check above catches drift from
any cause, including a table edited in Snowsight. Timestamps are
`TIMESTAMP_NTZ` holding UTC.

### Alternatives Considered

- **Star built in Spark:** moves grain and business rules out of dbt, losing
  declarative tests, lineage and docs.
- **Watermark on `updated_at`:** one concept across every layer, but it
  discards the guarantee ADF's lookback window was built to provide.
- **Snowflake reading curated's Delta files:** no export step, but deletion
  vectors leave superseded rows inside live files, which only a Delta reader
  skips.
- **Loading the whole partition:** a simpler COPY, but it loads files from
  failed writes.
- **Full dump each run:** does not avoid deduplication, only adds volume.
- **MERGE into Snowflake raw:** raw stays a mirror, but COPY INTO cannot merge,
  so it needs a staging table and a second statement.
- **dbt snapshots over raw:** a named and familiar tool, but a second history
  mechanism over data that already records every version.
- **VARIANT column instead of typed tables:** no DDL to maintain, but Parquet
  already carries types, and a bad value becomes a silent NULL inside a model.

### Rationale

Each tool keeps one job, and every property the pipeline claims is traceable to
the component that provides it.

### Consequences

- Delivery is **at least once up to raw and exactly once at the marts**. The
  Parquet write and the version advance are separate transactions and cannot be
  made one. The deduplication in dbt's staging models is what makes the second
  half true, so removing it doubles revenue on any retry.
- COPY INTO's `rows_loaded` must equal the manifest's row count. A mismatch is
  FAIL in `pipeline_audit`.
- DVT cannot compare the source against `raw`: counts will not match by
  construction. It compares source keys against the deduplicated staging
  layer, and every missing key must appear as an unprocessed `source_key` in
  dead-letter. The dead-letter row count is not the expected difference: one
  key can have several records, and a key whose newest version was rejected
  keeps its older version in staging.
- Change Data Feed must be enabled per curated table, sees nothing from before
  it was enabled, and is bounded by Delta log retention. A long gap between
  runs breaks the chain and requires a full reload of that table.
- The watermark store holds a Delta version and the manifest per table, and
  assumes a single writer, which holds while Airflow runs with
  `max_active_runs=1`.
- The account time zone is America/Los_Angeles. The pipeline user must run with
  `TIMEZONE = 'UTC'`, or comparing an NTZ column with `CURRENT_TIMESTAMP()`
  shifts by seven or eight hours.
- Raw as a change log is the project's least intuitive property. It has to be
  stated in the README, not only here.

### Amendment, 2026-09-28: the manifest is a Delta table, and the load reads it

The decision above tied each COPY to the files of one run. A crash between
export and load would then leave files that no later run names.

**Decision:**

- **Manifest.** `retail_dev.ops.served_manifest`, append-only, one row per
  table per run: `run_id`, `table_name`, `table_id`, `export_mode`
  (`snapshot` or `cdf`), `start_version`, `end_version`, `files`, `row_count`,
  `written_at`. The last exported version is `max(end_version)`; there is no
  separate state table. Parquet is written first, and the appended row is the
  commit point.
- **Load.** COPY names the files of every manifest row from the last 7 days
  and skips those already loaded. A crash between export and load loses
  nothing. The window stays well under COPY's 64-day load memory, and the load
  fails if an unloaded row is older than the window.
- **Run directory.** `served/<table>/year=/month=/day=/run_id=<id>/`, so
  `inputFiles()` returns this run's files only.
- **Pinned version.** The export reads the current version N first, then
  reads at N: `versionAsOf` for a snapshot, `startingVersion =
  max(end_version) + 1` and `endingVersion = N` for CDF. A MERGE committed
  during the export cannot reach the files without reaching the manifest.
- **Full reload by flag only.** The export fails when a table has no manifest
  rows and `full_reload` is not set, or when `table_id` differs from the last
  row (the table was rebuilt and its versions restarted). `export_mode`, not a
  NULL `start_version`, says what a row is.
- **Empty runs** write a row with `row_count = 0` and no files, so
  `end_version` advances.

**Supersedes:** the Partitioning path gains the `run_id=` level; COPY names
the files of all unloaded manifest rows in the window, not one run's; the row
count check is per manifest row, summing `rows_loaded` for its files from
`COPY_HISTORY`, since one COPY can span several rows and one row's files can
span two COPYs; the watermark store is the manifest alone, still single
writer.

### Amendment, 2026-10-01: refinements found while implementing the export

- **Watermark.** The export watermark is the end_version and table_id of the
  table's latest manifest row, ordered by written_at, not max(end_version).
  Manifest writes are single-writer and serialized, so written_at is commit
  order. After a rebuild and full_reload, the new snapshot row is the latest
  even though its end_version is lower; rows of a previous table_id never
  become the watermark again.
- **Empty runs.** A new Delta version gives a manifest row even with
  row_count = 0, and its files list is empty. When N equals the last
  end_version, nothing is read and no row is written.
- **Export directory.** One directory per export attempt,
  `export_id=<uuid>`, not per run. Airflow retries a task with the same
  run_id, so a run_id directory left by a failed attempt would fail every
  retry under errorifexists. A failed attempt's directory is never in the
  manifest, so it is never loaded. The manifest's run_id column still holds
  the Airflow run.

### Amendment, 2026-10-03: row counts are checked against raw, not COPY_HISTORY

- **Check.** For each manifest row in the load window, the rows in
  `raw.<table>` whose `_source_file` is one of its files must equal its
  `row_count`. Fewer is a missing or partial load, more is a double load.
- **Why.** It checks the state, not a report of the process. It has no
  retention limit (INFORMATION_SCHEMA.COPY_HISTORY keeps 14 days), a FORCE
  reload shows up as a doubled count, and it does not matter how many COPY
  statements loaded one export.
- **COPY results** stay the per-file record for `pipeline_audit`.
- **Supersedes:** "summing `rows_loaded` for its files from `COPY_HISTORY`"
  in the 2026-09-28 amendment, and the Consequences bullet that COPY INTO's
  `rows_loaded` must equal the manifest's row count. A mismatch is still FAIL.

## ADR-021: Cost Control by Suspending Compute, Not Destroying the Stack

### Status

Accepted

### Context

ADR-004 destroyed and rebuilt the whole stack after every session. That was
cheap while the stack was Terraform only. It is not cheap now: Snowflake's
storage integration needs an Entra consent flow and a role assignment that
Terraform cannot yet reproduce, and the rebuild runbook runs 40 minutes.

Meanwhile every expensive component learned to idle. The cost of keeping the
stack is no longer the cost of leaving it running.

### Decision

The stack stays. Cost is controlled per component instead:

| Component | Control |
|---|---|
| Snowflake warehouse | `AUTO_SUSPEND = 60`, `INITIALLY_SUSPENDED` |
| Snowflake account | resource monitor, 50 credits monthly, suspend at 100% |
| Databricks | serverless, stops when a job ends |
| PostgreSQL Flexible Server | stopped by hand between sessions |
| Azure SQL (watermark) | serverless, auto-pauses after 60 idle minutes |
| ADLS, Key Vault, ADF | storage and definitions, negligible when idle |

Ending a session means stopping PostgreSQL. Nothing else needs an action.

### Alternatives Considered

- **Keep ADR-004:** correct on cost, but the Snowflake consent flow makes the
  rebuild manual, so the tax is paid in attention rather than in money.
- **Destroy Azure, keep Snowflake:** the two are joined by a role assignment on
  a storage account, so destroying one breaks the other silently.

### Rationale

Idle compute now suspends itself. Paying a 40 minute rebuild to avoid pennies
is a worse trade than watching a resource monitor.

### Consequences

- The monthly floor is no longer zero. Storage and a stopped database bill.
- `rebuild-runbook.md` becomes a recovery procedure rather than a routine one.
- Cost discipline now depends on the resource monitor firing, which makes its
  `NOTIFY_USERS` recipient a real dependency rather than decoration.

---

## ADR-022: FX Rates Are Source-Owned Data

### Status

Accepted

### Context

FX rates come from freecurrencyapi.com. `fetch_fx_rates.py --write-postgres`
writes them into `retail_oltp.exchange_rates`, and ADF extracts that table like
any other. No ADR said who owns the table, and CLAUDE.md drew the API feeding
ADF directly. The gap surfaced as a bug: the upsert updated unchanged rows, the
trigger bumped `updated_at`, and every re-fetch looked like a change downstream.

### Decision

`exchange_rates` belongs to the source system. `fetch_fx_rates.py` plays the
source's rate feed, the role a treasury process plays for an ERP's currency
table, not a pipeline stage. The pipeline only reads the table. A writer into a
source table must not touch unchanged rows, so the upsert updates a row only
when its rate or source changed (`is distinct from`).

### Alternatives Considered

- **API straight to raw, one file per `rate_date`:** idempotent by overwrite,
  and the pipeline never writes to a source, but it changes the raw layout, the
  primary key, curated, the served contract and the Snowflake raw table.
- **`on conflict do nothing`:** simpler, but freezes a rate the provider later
  corrects.

### Rationale

It matches how retail systems hold rates, and keeps one extraction path, the
watermark, for every source table.

### Consequences

- A re-fetch is harmless; an explicit range only saves API calls.
- A provider correction flows downstream as a real change.
- Verified on Azure PostgreSQL, 2026-10-03: an identical re-fetch kept
  `updated_at`, a changed rate was restored and re-stamped.
- In production the data team would not run this feed. It lives in
  `ingestion/api_ingest/` only because the project builds its own source.

---

## ADR-023: Terraform Owns the Factory, ADF Git Owns Its Content

### Status

Accepted

### Context

Terraform created the factory and also two linked services, `ls_adls_dev`
and `ls_key_vault`. ADF Git holds all four linked services, the datasets and
the pipeline, and Publish deploys them. Two definitions of the same two objects
meant whoever wrote last won. It broke Publish on 2026-10-03: the copies ADF
imported from the Terraform-built factory carried a `type` line that made
Publish try to delete them. `terraform plan` showed no drift that day, so the
conflict was latent rather than active.

### Decision

Terraform owns the infrastructure: the factory, its managed identity, the
GitHub connection, Key Vault access and the storage role. ADF Git owns the
content: every linked service, dataset and pipeline. The two linked services
leave Terraform through `removed` blocks with `destroy = false`, so nothing
in Azure changes.

### Alternatives Considered

- **Everything in Terraform:** loses Studio authoring and the pipeline history.
- **Linked services in Terraform only:** Git mode needs every referenced object
  in Git, so Publish fails.
- **Terraform creates, then ignores changes:** keeps a second definition that
  nobody applies, the state the July import came from.
- **ARM deployment through CI/CD, parameterised URLs:** right for several
  environments, too much for one.

### Rationale

One owner per object removes the race. Six of the eight content objects
already lived only in Git, and the rebuild already deploys them with Publish.

### Consequences

- The storage and Key Vault URLs stay hard-coded in the linked service JSON;
  a renamed resource still means editing it by hand (rebuild runbook).
- A second environment is the trigger for parameterised ARM deployment.
- Terraform no longer manages the linked services, so `terraform destroy`
  leaves them to go with the factory.

---

## ADR-024: The Mart Layer in dbt

### Status

Accepted

### Context

ADR-011, ADR-012 and ADR-013 fixed the grain, the FX formula and the dimension
shape before dbt existed. Building the marts settled what they left open: which
dimensions, how keys are made, where unloadable lines go, and how the fact
stays correct when it loads incrementally.

### Decision

- **Shape.** `fact_sales` with `dim_customer`, `dim_product`, `dim_store` and
  `dim_date`. `dim_store` is new: every sale has exactly one store, and the
  store has attributes of its own. An employee dimension is deferred; its key
  is already on the order.
- **Keys.** A dimension key is the source id. Marts rebuild every run, so a
  sequence would renumber and orphan the fact. `-1` is the unknown customer
  (ADR-013). There is no unknown store: a sale without a channel is an error,
  and `not_null` catches it because `relationships` skips nulls.
- **Type 1 dimensions.** "As of" questions use the staging history models.
- **Routing.** `int_order_lines_routed` gives each line an `error_reason`,
  `missing_order` or `missing_fx_rate`. `fact_sales` takes the nulls,
  `fact_sales_rejected` the rest, and a test proves every line lands once.
- **Measure.** `total_original` is `quantity * unit_price`: gross, before
  discount, ex VAT. `total_gbp` is rounded per line.
- **Incremental.** `fact_sales` merges on `sale_id` the lines whose line,
  order or rate was loaded after the newest `source_loaded_at` already in it.
  `fact_sales_rejected` is rebuilt every run.

### Alternatives Considered

- **`dim_supplier`, `dim_category`:** reached only through the product (ADR-013).
- **Hashed keys:** deterministic too, but solve a multi-source problem this project lacks.
- **Watermark on `order_items` only:** misses a status change and a late rate.
- **Watermark on dbt's run time:** compares two different clocks.
- **`fact_sales` as a table:** fast enough at this volume; incremental proves the pattern.

### Consequences

- A merge never deletes. A line that must leave `fact_sales` needs
  `--full-refresh`; no such path exists today.
- A new column or a changed filter needs `--full-refresh`, because
  `on_schema_change` is `ignore`.
- Summed `total_gbp` can differ by pennies from a converted total. DVT
  reconciles sums in the original currency.
- Order statuses are listed in `table_config.py` and `_marts.yml`; drift fails
  `accepted_values`.
- DVT candidate: sales in a store after its `closed_date`. The generator never
  filters closed stores; the current data ends before Paris closes.

---

## ADR-025: Cross-System Validation with DVT

### Status

Accepted

### Context

dbt tests see only Snowflake. A file lost between PostgreSQL and Snowflake
leaves every model consistent and every test green. Rows also leave on
purpose: Databricks rejects some to dead-letter. The check must tell a
rejected row from a lost one without repeating the reject rules.

### Decision

- **Scope.** DVT checks only across systems, PostgreSQL against Snowflake
  staging. Grain, keys and routing stay dbt tests.
- **Window.** Source rows with `created_at <= window_end`, the ADF watermark
  passed in by the caller. Never a bound read from Snowflake: a lost load
  would move it too.
- **Tables without reject rules (9).** Row counts must be equal.
- **Tables with reject rules (3).** Per key, the newest version the pipeline
  saw, accepted in staging or rejected in `stg_dead_letter`, must equal the
  source: count and sums, payments and orders per currency. A test ties this
  split to `table_config.py`.
- **Money in integer cents.** DVT compares decimal aggregates as float32; a
  one cent difference passed.
- **Runner.** `validation/run_validations.py` builds connections from the
  environment in a temporary folder, calls the DVT CLI per check, reads every
  JSON line and exits 1 on any status but success. DVT exits 0 on a mismatch.

### Alternatives Considered

- **Reject rules repeated in the source query:** agrees with a broken rule instead of catching it.
- **Source against raw:** raw is a change log; counts differ by design (ADR-020).
- **Bound from `max(updated_at)` in Snowflake:** moves with the data it should check.
- **DVT YAML configs:** embed the SQL text and a fixed date.
- **DVT Python API:** internal, unlike the CLI and its JSON output.

### Consequences

- `venv-dvt` runs Python 3.11: DVT 8.10 pins ibis 7.1, whose numpy and
  pyarrow have no 3.13 wheels.
- The PostgreSQL password is a process argument for a few seconds per run.
- Snowflake object names must be upper case; ibis quotes lower case ones.
- Results are files in `validation/results/`, for Airflow to write to
  `pipeline_audit`.
- Sums on tables updated in place assume a quiet source between ADF and DVT:
  PostgreSQL keeps no old versions, so a row updated after the window shows
  as a mismatch. A frequently loaded live source would check counts per
  increment on every run and sums nightly on settled rows, or reconcile
  against the extracted files instead of the live table.
- Counts are cumulative, not per increment: fine at this volume, a full scan
  of every table on each run at scale.


---
## ADR-026: Pipeline Audit Table

### Status

Accepted

### Context

Each task reports in its own shape: notebook exit JSON, `load_raw`'s export
checks, DVT's results file and exit code. The dashboard needs one row per task
per run, and a retry must not add rows or turn a crash into a data failure.

### Decision

- **Table.** `ecommerce_db.audit.pipeline_audit`, one row per task per DAG
  run, key `(dag_name, run_id, task_name)`: Airflow's run_id is unique only
  within one DAG.
- **Two statuses.** `status` SUCCESS / FAILED / SKIPPED says whether the task
  ran, in Airflow's words. `dvt_status` MATCH / MISMATCH / SKIPPED says whether
  source and target agreed, NULL when there is no verdict. A mismatch is
  FAILED + MISMATCH, a DVT crash FAILED + NULL.
- **Counts.** NULL is not measured, 0 is measured and none. `load_raw` counts
  rows in raw from this run's exports, the same on every retry. Per-table
  counts and failed checks go to `details` VARIANT.
- **Latest outcome, not history.** One MERGE per record; a retry updates the
  row and `try_number` shows which attempt wrote it.
- **Code.** `audit/records.py` holds the rules Snowflake cannot enforce (no
  CHECK, PK not enforced), `outcomes.py` maps each task's output, `writer.py`
  MERGEs with bound values, `recorder.py` runs a task, records it and
  re-raises the task's own error.

### Alternatives Considered

- **One row per task per table:** three-part key, a fake table name for dbt.
- **Append every attempt:** full retry history, not needed yet.
- **`load_raw` rows from COPY:** a retry that skips every file reports 0.
- **CLIs write their own row:** a crashed CLI cannot record its crash.
- **dbt model over the table:** no consumer besides Streamlit.

### Consequences

- A missing row means unknown: a killed worker runs no Python, and callbacks
  may not run for upstream_failed tasks. The DAG ends with an `all_done` task.
- An earlier failed attempt shows only as `try_number` and in the Airflow log.
- One writer per key at a time; parallel tasks write different keys.
- The DAG deletes `validation/results/<run_id>.json` before DVT, or a crashed
  retry reads the previous attempt's file.
- The DAG passes its run_id to `02_curated_to_served`, or `load_raw` counts 0.
- `ALL PRIVILEGES` on the audit schema is broader than needed; production
  grants USAGE and CREATE TABLE.
