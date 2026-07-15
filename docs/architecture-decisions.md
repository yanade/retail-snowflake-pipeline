# Architecture Decision Records

This file documents key design decisions made during the build of the retail-snowflake-pipeline.
Each ADR records what was decided, what alternatives were considered, and why.

---

## ADR-001: Watermark Storage



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

---

## ADR-002: Secret Management



### Decision
Use Azure Key Vault for all credentials. Terraform provisions the vault as infrastructure only — secrets are bootstrapped separately via Azure CLI and never enter Terraform state.

### Alternatives Considered
- **Terraform manages secrets** — simple, one command deploys everything, but secret values are stored in Terraform state in plain text; anyone with state file access can read credentials
- **Plain text in connection string** — password embedded in ADF config; visible in ADF UI and logs
- **Environment variables** — secrets appear in CI logs; not suitable for production

### Reason
Secrets and infrastructure have different lifecycles. A Key Vault exists for years and is managed by the platform team. A secret is rotated every 90 days and is managed by the security team. Mixing them in the same Terraform apply couples two concerns that change at different rates and for different reasons.

Keeping secrets outside Terraform means:
- Secret values never appear in `terraform.tfstate`
- Password rotation requires no Terraform run — only an Azure CLI command
- The pattern matches enterprise practice where a dedicated secrets pipeline or security team manages credentials independently of infrastructure provisioning

### Implementation
Terraform provisions the Key Vault (empty). After `terraform apply`, secrets are bootstrapped once via Azure CLI:
```bash
az keyvault secret set \
  --vault-name retail-pipeline-dev-kv \
  --name sql-admin-password \
  --value "YourPassword123!"
```
ADF reads the secret at runtime via its Managed Identity. No password is stored in ADF configuration.

### Consequences
- One additional Terraform module (`modules/keyvault/`) — vault only, no secrets
- One additional ADF Linked Service (`ls_key_vault`)
- ADF Managed Identity granted `Get` permission on Key Vault secrets via standalone access policy in `main.tf`
- Secret bootstrap is a manual step documented in README
- Password rotation: update SQL first, then update Key Vault secret via CLI — ADF picks up new value automatically on next run, no redeployment needed
- Production upgrade path: Key Vault rotation policy + Azure Function for automated rotation, or passwordless Managed Identity authentication between ADF and SQL

### Dependency design
Key Vault access policy is provisioned as a standalone resource in `terraform/main.tf` — not inside any module. This breaks the circular dependency between `module.adf` (needs `key_vault_id`) and `module.keyvault` (needs `adf_principal_id`). Cross-module wiring belongs at the root level.

---

## ADR-003: Cost Management — Destroy After Every Session



### Decision
Run `terraform destroy` at the end of every dev session and `terraform apply` at the start of the next. Infrastructure is treated as ephemeral during development.

### Alternatives Considered
- **Leave infrastructure running** — simpler workflow, but Databricks Premium and Azure SQL accrue cost 24/7 even when idle
- **Pause individual services** — Databricks auto-suspends, SQL serverless auto-pauses, but the workspace and server still incur standing charges

### Reason
The project uses cost-sensitive services (Databricks Premium SKU, Azure SQL serverless). Destroying after each session eliminates all standing charges. Remote Terraform state in Azure Storage persists between sessions so the full stack can be recreated reliably with a single command.

### Implementation
A local `session.sh` file (gitignored, never committed) stores environment variables and serves as the session entry point:

```bash
# Start of session
source session.sh
cd terraform && terraform apply

# Bootstrap secret after every fresh apply
az keyvault secret set \
  --vault-name retail-pipeline-dev-kv \
  --name sql-admin-password \
  --value "$TF_VAR_sql_admin_password"

# End of session
terraform destroy
```

### Consequences
- Secret bootstrap is required after every `terraform apply` — not just the first deployment
- `session.sh` must never be committed — contains credentials as environment variables
- Full redeploy takes ~10 minutes per session (Databricks workspace provisioning is the slowest resource)
- In production this pattern would not be used — infrastructure is persistent and secrets are managed by a dedicated rotation pipeline

---

## ADR-004: Regional Deployment Constraint — SQL Server in francecentral



### Decision
Deploy Azure SQL Server in `francecentral` while all other services remain in `uksouth`.

### Alternatives Considered
- **Raise a support request to unlock uksouth** — possible but slow; not justified for a portfolio project
- **Move all services to francecentral** — unnecessary redesign; only SQL is blocked
- **Replace Azure SQL with a different service** — would change the watermark architecture without addressing the root cause

### Reason
The free trial subscription (`Azure subscription 1`) blocks SQL Server provisioning in `uksouth` with error `ProvisioningDisabled`. Investigation confirmed `francecentral` allows Basic tier SQL on this subscription. This is a deployment constraint specific to the free trial, not an architectural choice.

The fix is minimal: a dedicated `sql_location` variable defaults to `francecentral` and is passed only to `module.sql`. All other modules continue to use `var.location = "uksouth"`.

### Consequences
- SQL Server is in `francecentral`, all other resources are in `uksouth`
- Minor cross-region latency between ADF (uksouth) and SQL (francecentral) — negligible for a watermark lookup that runs once per pipeline execution
- In production all services would be co-located in a single region for latency, data residency, and cost optimisation
