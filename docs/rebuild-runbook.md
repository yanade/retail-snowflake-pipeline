# Rebuild Runbook

Cold-start procedure for deploying this project to a fresh Azure subscription,
or rebuilding after `terraform destroy`.

Under ADR-004's cost discipline the whole stack is destroyed and recreated
rather than left running, so this is a routine procedure, not a disaster
recovery one.

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

Deterministic: `--seed 42`, and `generate_data.py` hardcodes
`start_date = 2025-01-01`. The same data comes back every time.

Verify: `orders` should be non-zero and order dates should start 2025-01-01.

### 6. FX rates

`load_database.sh` seeds and generates but never fetches exchange rates, so
`exchange_rates` is empty until this runs. Skipping it means every FX
conversion in notebook 02 fails, and the failure surfaces far downstream.

```bash
./scripts/bootstrap_fx_rates.sh
```

The script derives the range from `retail_oltp.orders`: earliest order date to
latest order date plus three days. The rates therefore always cover the data
that was just generated, including after `generate_incremental_data.py` appends
new days. Pass an explicit range to override it:

```bash
./scripts/bootstrap_fx_rates.sh 2025-01-01 2025-02-02
```

It exits non-zero if `exchange_rates` is still empty afterwards, and reports how
many orders have no matching rate.

Verify: 132 rows (33 dates x 4 currencies) and `Orders with no matching rate: 0`.

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

## Known gaps

- Unity Catalog objects are manual. Codifying them with the Databricks
  Terraform provider is a planned change.
- `session.sh` and `.env` are gitignored by design and must be recreated by
  hand. `.env.example` lists the required keys.
- Classic Databricks clusters may be unavailable on a trial subscription with
  no VM quota. Serverless compute needs no quota and is what this project
  uses.
