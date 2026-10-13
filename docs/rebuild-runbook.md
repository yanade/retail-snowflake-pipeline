# Rebuild Runbook

Cold-start procedure for deploying this project to a fresh Azure subscription,
or rebuilding after `terraform destroy`.

Under ADR-021 the stack is no longer destroyed between sessions, so this is a
recovery procedure rather than a routine one. Ending a session now means
stopping PostgreSQL, see "Ending a session" below.

The Snowflake layer is not covered here, it has its own procedure in
`snowflake-setup-runbook.md`. The two are joined at one point: rebuilding Azure
destroys the `served` container and with it the role assignment Snowflake reads
through. After a rebuild, re-run step 6 of that runbook. The consent itself
survives, because Snowflake's service principal lives in Entra ID rather than
in the subscription.

Expect 40 to 60 minutes, most of it waiting on Terraform.

---

## Account-specific values

Everything below changes when you move to a new subscription. Nothing here is
committed to the repository.

| Value | Where it lives | How to get it |
|---|---|---|
| Azure subscription | Azure CLI login | `az login` |
| SQL + Postgres admin passwords | `session.sh` (gitignored) | You choose them. Must be recreated by hand. |
| `DATABASE_URL` | `.env` (gitignored) | Built from `terraform output postgres_server_fqdn` |
| `EXCHANGE_RATE_API_KEY` | `.env` | Unchanged, unless rotated |
| Databricks workspace URL | n/a | `terraform output databricks_workspace_url` |
| Access connector resource ID | n/a | `terraform state show module.databricks.azurerm_databricks_access_connector.adls` |

**Two globally unique storage account names.** Azure storage account names are
unique across all of Azure, and this project needs two of them.

`retailpipelinedevx7k` is the data lake, created by Terraform, derived as
`lower(replace(project_name + environment, "-", "")) + "x7k"`.

The `x7k` suffix exists because of what happened on 2026-09-17. The old
subscription had been destroyed and then disabled, its resource groups showed
as empty in the portal, and DNS resolved nothing for either storage account.
`az storage account check-name` still answered `AlreadyExists` for both. A
disabled subscription can keep storage account names reserved even when nothing
is visible in it, and nothing in the portal tells you for how long. Check the
names before editing any code:

```bash
az storage account check-name --name retailpipelinedevx7k \
  --query '{available:nameAvailable, reason:reason}' -o table
```

If a name is taken, change the suffix in `terraform/modules/adls/main.tf` and
update the hardcoded references: `transformation/sql/unity_catalog_setup.sql`
(x2), `ingestion/adf_pipelines/linkedService/ls_adls_dev.json`,
`docs/architecture-decisions.md` ADR-014 (x2), `ingestion/README.md`. The ADF
linked service JSON is the one most easily missed, and missing it sends every
Copy activity to a storage account that does not exist.

`retailpipelinetfstatex7k` is the Terraform state backend, created by hand (see
step 0), and it lives in its own resource group `retail-pipeline-tfstate-rg`.
Because it is outside Terraform, `terraform destroy` does **not** remove it.
Free the name on the old subscription with:

```bash
az group delete --name retail-pipeline-tfstate-rg --yes
```

Run that only after `terraform destroy` has finished, since Terraform needs
its state to do the destroy.

---

## Prerequisites

- `az`, `terraform`, `psql`, `sqlcmd` (`brew install sqlcmd`), Python venv
- `az login` against the target subscription. On a machine that has used
  another account, `az account clear` first, then confirm with
  `az account show --query '{name:name, subscriptionId:id, user:user.name}' -o table`.
  Table output hides a key literally called `id`, so alias it.
- `session.sh` recreated, exporting `TF_VAR_sql_admin_password`,
  `TF_VAR_postgres_admin_password` and `ARM_SUBSCRIPTION_ID`. Since azurerm
  4.0 the subscription ID must be given explicitly, so read it from the CLI
  rather than hardcoding it:

  ```bash
  export ARM_SUBSCRIPTION_ID=$(az account show --query id -o tsv)
  ```

- `.env` present (see `.env.example`)

---

## Steps

### 0. Remote state backend

`backend.tf` stores Terraform state in Azure Storage. That storage account is
created by hand, outside Terraform, and must exist before `terraform init`.
On a fresh subscription, skipping this makes step 1 fail immediately.

On a brand new subscription, register the storage resource provider first.
Nothing else registers it: the portal registers providers when you create
resources through it, and Terraform registers its own set when it runs, but
step 0 uses plain `az`. Without it every storage command fails with
`SubscriptionNotFound`, which reads like the subscription does not exist.

```bash
az provider register --namespace Microsoft.Storage
# wait until this prints Registered
az provider show --namespace Microsoft.Storage --query registrationState -o tsv
```

```bash
az group create \
  --name retail-pipeline-tfstate-rg \
  --location uksouth

az storage account create \
  --name retailpipelinetfstatex7k \
  --resource-group retail-pipeline-tfstate-rg \
  --location uksouth \
  --sku Standard_LRS

az storage container create \
  --name tfstate \
  --account-name retailpipelinetfstatex7k
```

`retailpipelinetfstatex7k` is globally unique, like the data lake account. If the
name is taken, change it here and in `backend.tf`.

### 1. Infrastructure

```bash
cd terraform && terraform init -reconfigure && terraform apply
```

`-reconfigure` matters when the previous deployment used a different state
storage account. `.terraform/terraform.tfstate` caches the old backend settings,
and a plain `init` offers to copy state from a backend that is gone.

Verify: `terraform output` prints the storage account, ADF name, Databricks
URL and Postgres FQDN.

### 2. Key Vault secrets

```bash
source session.sh
./scripts/bootstrap_keyvault.sh
```

### 3. Allow your machine to reach PostgreSQL

```bash
./scripts/bootstrap_postgres.sh
```

Adds a firewall rule for your current IP. Home IPs change, so this is per
session, not per rebuild.

### 4. Update `.env`

Set `DATABASE_URL` using the new `postgres_server_fqdn` and your Postgres
admin password. Nothing else in `.env` changes.

### 5. Schema and generated data

```bash
./scripts/load_database.sh
```

Generates 90 days ending today in UTC, `--seed 42` (ADR-027). The dates move
with the clock; to repeat a run exactly, call `generate_data.py` with
`--end-date`.

Verify: about 4,500 orders, the first order 89 days before today, no
`order_date` after `now()`.

### 6. FX rates

`load_database.sh` seeds and generates but never fetches exchange rates, so
`exchange_rates` is empty until this runs. Skipping it means every FX
conversion in notebook 02 fails, and the failure surfaces far downstream.

```bash
./scripts/bootstrap_fx_rates.sh
```

The script derives the range from `retail_oltp.orders`: earliest order date to
yesterday. The API serves end-of-day rates, so today's has not been published
yet; today's orders wait for tomorrow's run (ADR-027). Pass an explicit range
to override it, ending yesterday at the latest:

```bash
./scripts/bootstrap_fx_rates.sh 2026-07-16 2026-10-12
```

It exits non-zero if `exchange_rates` is still empty afterwards, and reports
orders with no rate before today, and today's orders awaiting one.

Verify: 356 rows (89 dates x 4 currencies), `Orders with no matching rate: 0`,
and `Today's orders awaiting tomorrow's rate` above 0.

### 7. Watermark control tables

```bash
./scripts/bootstrap_watermark.sh
```

Creates `pipeline_config` and `pipeline_watermark_control` in `watermark-db`
and seeds all 12 pipelines with `last_watermark = '1900-01-01'`. That sentinel
is what makes the first pipeline run a full backfill.

### 8. ADLS permissions (conditional)

Only if you hit `AuthorizationPermissionMismatch`:

```bash
./scripts/bootstrap_adls_permissions.sh
```

### 9. Azure Data Factory

Git integration is declared in Terraform, so the factory reconnects to the
repository automatically. In ADF Studio: **Publish**, then **Debug** or
**Trigger now** on `pl_load_data`.

`terraform apply` leaves the factory empty. Linked services, datasets and the
pipeline are owned by ADF Git (ADR-023), so they only exist after this first
Publish.

Verify:

```sql
SELECT pipeline_name, rows_loaded, last_watermark
FROM pipeline_watermark_control ORDER BY pipeline_name;
```

All 12 rows should show a current watermark and non-zero `rows_loaded` for
`orders`, `order_items`, `payments`, `customers`, `exchange_rates`.

### 10. Unity Catalog

None of this is in Terraform yet, so it is manual. Catalog Explorer:

**Storage credential**

| Field | Value |
|---|---|
| Name | `retail_adls_credential` |
| Type | Azure Managed Identity |
| Access connector ID | from `terraform state show` (see table above) |
| Read-only | off |

**External locations.** All use `retail_adls_credential`, file events off.
Force-create past the File Events failure: the connector holds only
data-plane roles, which is deliberate (see ADR notes on file events).

| Name | URL |
|---|---|
| `retail_raw` | `abfss://raw@retailpipelinedevx7k.dfs.core.windows.net/` |
| `retail_curated` | `abfss://curated@retailpipelinedevx7k.dfs.core.windows.net/` |
| `retail_served` | `abfss://served@retailpipelinedevx7k.dfs.core.windows.net/` |
| `retail_managed` | `abfss://managed@retailpipelinedevx7k.dfs.core.windows.net/` |

**Catalog and schemas**

Run `transformation/sql/unity_catalog_setup.sql` in the SQL Editor.

Verify: `SHOW SCHEMAS IN retail_dev;` returns `curated`, `served`, `ops`,
`default`.

---

## Resetting the data

Replaces the data and keeps the infrastructure: no Terraform, no Unity Catalog
credentials, no Snowflake consent. Reset every layer, not some. The new source
restarts its sequences, so a curated row left behind with `order_id = 1` would
take the new order 1 as an update (ADR-027).

1. Snowflake: `TRUNCATE` the 13 raw tables and `audit.pipeline_audit` as
   `retail_admin`. `TRUNCATE`, unlike `DELETE`, also clears COPY load history.
2. Unity Catalog: `DROP TABLE` the curated tables, `ops.dead_letter` and
   `ops.served_manifest`. They are external, so the files stay.
3. ADLS: delete the top-level directories in `raw`, `curated` and `served`.
   Delete the contents, never the container: Snowflake's role is assigned on
   `served`.
4. Watermarks: `UPDATE pipeline_watermark_control SET last_watermark = '1900-01-01', rows_loaded = NULL`.
5. Postgres: `DROP SCHEMA retail_oltp CASCADE`, then steps 5 and 6 above.
   `schema.sql` creates only what is missing, so without the drop the new rows
   land on the old ones.
6. `rm -f validation/results/*.json`.

Then the incremental runbook, steps 2 to 8, with four differences:
`curated_to_served` with `full_reload = true` and `tables` empty;
`00_register_tables` after it; `dbt build --full-refresh`, because
`fact_sales` still holds the old rows; `full_reload` back to `false` in the
job afterwards. Reference numbers: incremental runbook, Reset run, 2026-10-13.

---

## Ending a session

ADR-021 replaced "destroy everything" with "suspend everything". One action is
left:

```bash
az postgres flexible-server stop --name retail-pipeline-dev-pg --resource-group retail-pipeline-dev-rg
```

Start it again before any `terraform plan` or `apply`. The refresh reads the
`retail_source` database and fails with `ServerStoppedError` on a stopped
server.

Everything else idles on its own: the Azure SQL watermark database auto-pauses
after 60 idle minutes, Databricks serverless stops when a job ends, and the
Snowflake warehouse suspends after 60 idle seconds, under an account-level
resource monitor capped at 50 credits a month.

Storage, Key Vault and the ADF definitions bill whether or not you are working.
That floor is what ADR-021 accepts in exchange for not rebuilding.

---

## Known gaps

- Unity Catalog objects are manual. Codifying them with the Databricks
  Terraform provider is a planned change.
- `session.sh` and `.env` are gitignored by design and must be recreated by
  hand. `.env.example` lists the required keys.
- Classic Databricks clusters may be unavailable on a trial subscription with
  no VM quota. Serverless compute needs no quota and is what this project
  uses.
