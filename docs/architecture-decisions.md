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
The pipeline has three distinct concerns: data movement (CSV → ADLS),
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
logging, and Slack alerts — ADF owns the incremental copy from ADLS and the
watermark update in Azure SQL.

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
