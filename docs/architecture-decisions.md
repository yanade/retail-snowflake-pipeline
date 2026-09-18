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

## ADR-012: FX Conversion via GBP Cross Rates, and an Interim Dead-Letter Location

### Status

Accepted

### Context

Two specifications disagree with the data.

`ingestion/api_ingest/fetch_fx_rates.py` fetches with `BASE_CURRENCY=GBP`,
so every row in `retail_oltp.exchange_rates` is GBP-based: the rate answers
"how many units of the target currency per one GBP".

The README describes converting order values to USD. But orders are not
denominated in GBP. `database/seed.py` seeds stores in GB, DE and CA, and
`generate_data.py` sets `orders.currency_code` from `store.currency_code`.
So the fact table contains GBP, EUR and CAD orders, and a direct
"GBP amount times GBP-to-USD rate" join is wrong for most of them.

Separately, `dead_letter` is specified as a Snowflake table. Snowflake does
not exist yet, and the Databricks stage needs somewhere to put rejected
records now.

### Decision

**FX direction.** Exchange rates remain GBP-based, one base currency for the
whole table. An order in currency `C` converts to USD by cross rate:

```
usd_per_C = rate(GBP -> USD) / rate(GBP -> C)
```

For `C = GBP` the denominator is 1 by definition and the GBP-to-USD rate is
used directly.

**Fact columns.** `CLAUDE.md`'s `unit_price_gbp` and `total_gbp` are renamed
to `unit_price_original` and `total_original`, alongside an explicit
`currency_code` column. The old names assert a currency the data does not
have. `fx_rate_to_usd` holds the derived cross rate actually applied, so
every converted value is reproducible from the row itself.

**Missing rates.** If no rate exists for a given `(rate_date, currency)`,
the row is routed to dead-letter with `error_reason = 'missing_fx_rate'`.
It is not loaded with a NULL `total_usd`, because a NULL measure silently
understates every downstream sum.

**Dead-letter location.** `dead_letter` is a Delta table in ADLS at
`curated/_dead_letter/`, registered as `retail_dev.ops.dead_letter`, until
the Snowflake warehouse exists. `raw_payload` is stored as a `STRING`
containing JSON rather than a semi-structured type, which maps cleanly onto
Snowflake's `VARIANT` via `PARSE_JSON` when the table migrates.

### Alternatives Considered

**Refetch rates with each order currency as base.**
Removes the cross-rate arithmetic, but multiplies API calls by the number of
currencies, and independently fetched bases can disagree slightly, so
converting EUR to USD directly and via GBP would give different answers.

**Store only GBP totals.**
Matches the original column names, but misstates every non-GBP order. The
column names were the error, not the data.

**Allow NULL `total_usd` when a rate is missing.**
Simplest to implement and the failure is invisible, which is exactly the
objection.

### Consequences

- Notebook 02 needs `exchange_rates` pivoted or self-joined, because two
  rates for the same date are required to compute one cross rate.
- `CLAUDE.md`'s star schema section needs its column names updated.
- Migrating `dead_letter` to Snowflake is a known future task, and the
  `STRING`-holding-JSON choice exists specifically to make it cheap.
- Because the real API returned rates for all 33 days including weekends,
  no `missing_fx_rate` rejections will occur naturally with the current
  data. The path still needs a test, which means seeding a gap deliberately.
- DVT should assert that every served row has a non-null `fx_rate_to_usd`.

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
different days' rates and produce different USD totals, with no error.

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
