# Snowflake Setup Runbook

Procedure for standing up the Snowflake layer: a new trial account, key-pair
authentication, the account objects, and read access to the served zone in
ADLS.

Needed whenever the 30-day trial expires and a new account replaces it, or when
setting up on another machine. The Azure side is assumed to exist already, see
`rebuild-runbook.md`.

Expect 30 to 40 minutes, most of it waiting on the Azure role assignment to
propagate.

---

## Account-specific values

Everything here changes with a new account. None of it is committed.

| Value | Where it lives | How to get it |
|---|---|---|
| Account identifier | `.env`, `~/.snowflake/connections.toml` | Snowsight, account menu, Account Details. The **identifier**, not the server URL |
| Login name | same | Same panel, `Login name` |
| Private key | `~/.snowflake/rsa_key.p8` | Generated per machine, never copied between machines |
| Azure tenant id | `.env` | `az account show --query tenantId -o tsv` |
| ADLS account name | `.env` | `az storage account list -o table` |
| Snowflake service principal | n/a | `DESC STORAGE INTEGRATION`, changes every time the integration is recreated |

The trial is 30 days or until the balance runs out, whichever comes first.
Do the work that needs a live warehouse first and the documentation last.

---

## Prerequisites

- `az login`, with the subscription that owns the data lake selected
- The `served` container exists (`terraform apply` in `terraform/`)
- Repository cloned, `venv` active, `.env` present

---

## Steps

### 1. Create the trial account

`https://signup.snowflake.com/`, no card required.

Three choices that cannot be changed afterwards:

- **Cloud: Azure.** The storage integration authenticates through Entra ID.
- **Region: UK South (London).** Must match the region of the data lake, or
  every load pays cross-region transfer.
- **Edition.** Standard costs roughly $2 per credit, Enterprise roughly $3, and
  the trial balance is in dollars. Nothing in this project needs an Enterprise
  feature. The sign-up form suggests Enterprise.

Confirm the choices afterwards:

```bash
snow sql -c retail_admin -q "SELECT CURRENT_REGION()"
```

Expect `AZURE_UKSOUTH`. The Account Details panel does not show the region.

### 2. Install the CLI

```bash
pip install snowflake-cli && snow --version
```

This is a tool, not a project dependency: it does not belong in
`requirements.txt`. dbt and Airflow use their own connectors.

### 3. Generate a key pair

Browser SSO (`authenticator = externalbrowser`) does **not** work here: it means
federated sign-in through a SAML identity provider, and a trial account has
none. Password authentication drags in mandatory MFA. Key-pair is what
Snowflake recommends for programmatic access, and it is what dbt and Airflow
will use.

```bash
cd ~/.snowflake && openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt && openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub && chmod 600 rsa_key.p8
```

The private key never leaves this machine. Only the public half goes to
Snowflake, which is why Snowflake has no "generate key" button.

Build the statement into a file rather than the clipboard: copying anything
else from a browser or a chat window in between overwrites the clipboard, and
`tr -d '\n'` output printed to a terminal picks up zsh's trailing `%`, which is
not part of the key.

```bash
printf "ALTER USER <login name> SET RSA_PUBLIC_KEY='%s';\n" "$(grep -v 'KEY-----' ~/.snowflake/rsa_key.pub | tr -d '\n')" > /tmp/alter_user.sql && open -e /tmp/alter_user.sql
```

Run that statement in Snowsight (Projects, Workspaces, SQL file) as
ACCOUNTADMIN. This is the only statement executed in the browser, because there
is no working connection yet.

Verify the key arrived intact by comparing fingerprints, before trying to
connect:

```bash
openssl rsa -pubin -in ~/.snowflake/rsa_key.pub -outform DER | openssl dgst -sha256 -binary | openssl enc -base64
```

```sql
DESC USER <login name>;   -- row RSA_PUBLIC_KEY_FP, value after "SHA256:"
```

Then remove the temporary file.

### 4. Write connections.toml

`~/.snowflake/connections.toml`. In **this** file the section header is the bare
connection name. `[connections.name]` is the form used inside `config.toml`,
and putting it here creates one connection literally called `connections`.

```toml
[retail_admin]
account = "<account identifier>"
user = "<login name>"
authenticator = "SNOWFLAKE_JWT"
private_key_file = "/Users/<you>/.snowflake/rsa_key.p8"
role = "ACCOUNTADMIN"

[retail_dev]
account = "<account identifier>"
user = "<login name>"
authenticator = "SNOWFLAKE_JWT"
private_key_file = "/Users/<you>/.snowflake/rsa_key.p8"
role = "retail_dev"
warehouse = "retail_dev_wh"
database = "ecommerce_db"
schema = "raw"
```

```bash
chmod 600 ~/.snowflake/connections.toml
```

The CLI refuses to read it with wider permissions.

Check what the CLI parsed, before touching the network:

```bash
snow connection list
```

Two rows with non-empty parameters. Then:

```bash
snow connection test -c retail_admin
```

### 5. Apply the account objects and the storage integration

`.env` must carry `AZURE_TENANT_ID`, `ADLS_ACCOUNT_NAME` and
`ADLS_SERVED_CONTAINER`; the script fails loudly if any is missing.

```bash
./scripts/deploy_snowflake.sh
```

The stage at the end will be created but cannot read anything until step 6.
That is expected on a first run.

### 6. Consent, then grant the role in Azure

```bash
snow sql -c retail_admin -q "DESC STORAGE INTEGRATION azure_adls_served" --format json
```

`--format json` because the table form squeezes the columns to nothing.

Open `AZURE_CONSENT_URL` in a browser and click Accept. This registers
Snowflake's service principal in the Entra tenant. The page afterwards may be
blank or show a redirect error; that means nothing. Judge by the directory:

```bash
az ad sp list --display-name "<part of AZURE_MULTI_TENANT_APP_NAME before the underscore>" --query "[].{name:displayName, id:id}" -o table
```

Grant it read access to the served container only:

```bash
SP_ID=$(az ad sp list --display-name "<same prefix>" --query "[0].id" -o tsv) && SCOPE="$(az storage account show --name <adls account> --resource-group retail-pipeline-dev-rg --query id -o tsv)/blobServices/default/containers/served" && echo "sp=$SP_ID" && echo "scope=$SCOPE"
```

```bash
az role assignment create --assignee-object-id "$SP_ID" --assignee-principal-type ServicePrincipal --role "Storage Blob Data Reader" --scope "$SCOPE"
```

Reader, not Contributor: Snowflake only reads. Container scope, not account
scope: the other three zones stay out of reach.

Allow two to three minutes for the assignment to take effect.

### 7. Re-run the deployment

```bash
./scripts/deploy_snowflake.sh
```

Everything already present is skipped, and the `ALTER` statements reapply the
settings. Nothing is destroyed.

### 8. Verify the whole chain

Put one file in the served zone, so the check proves access rather than the
absence of an error:

```bash
echo "smoke" > /tmp/hello.txt && az storage blob upload --account-name <adls account> --container-name served --name _smoke/hello.txt --file /tmp/hello.txt --auth-mode login --only-show-errors
```

Read it back as the **working role**, not as admin:

```bash
snow sql -c retail_dev -q "LIST @ecommerce_db.raw.served_stage"
```

Two rows: the zero-byte `_smoke` directory object, which exists because the
storage account has a hierarchical namespace, and `hello.txt` at 6 bytes.

Clean up:

```bash
az storage blob delete --account-name <adls account> --container-name served --name _smoke/hello.txt --auth-mode login --only-show-errors && rm /tmp/hello.txt
```

Then the object checks. `SHOW` returns too many columns to read in a terminal,
so pipe it through `RESULT_SCAN`:

```bash
snow sql -c retail_admin -q "SHOW WAREHOUSES LIKE 'RETAIL_DEV_WH'; SELECT \"name\", \"state\", \"size\", \"auto_suspend\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));"
```

Expect `SUSPENDED`, `X-Small`, `60`.

```bash
snow sql -c retail_admin -q "SHOW RESOURCE MONITORS; SELECT \"name\", \"level\", \"credit_quota\", \"used_credits\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));"
```

Expect `level = ACCOUNT`, quota 50.

```bash
snow sql -c retail_dev -q "SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_SCHEMA()"
```

Expect `RETAIL_DEV`, `RETAIL_DEV_WH`, `RAW`. This is the decisive check: it
proves the grants work, not just that the objects exist.

---

## Traps

Each of these cost time on 2026-09-27.

**`externalbrowser` fails with `390190 ... SAML Identity Provider`.** Snowsight's
Config File tab offers it as a template, but it means federated SSO. A trial
account has no identity provider. Use key-pair.

**`Connection <name> is not configured`.** The section header in
`connections.toml` carries a `connections.` prefix. TOML then nests it, and the
CLI sees one connection named `connections` with no parameters. `snow connection
list` shows this immediately.

**`CREATE ... IF NOT EXISTS` silently does nothing.** Every object in the DDL is
therefore restated with an `ALTER ... SET`. Without that, a re-run reports
success and changes nothing. The trial account ships with a `COMPUTE_WH`, which
is exactly why this project uses `retail_dev_wh` instead.

**An invalid tenant id is accepted by `CREATE`.** It is validated on first use,
so the failure appears at `DESC`, not at creation. `successfully created` does
not mean correctly configured.

**`dfs.core.windows.net` does not work.** ADLS Gen2 is still addressed as
`blob.core.windows.net` in a storage integration and a stage.

**Placeholders reach the server.** `<tenant id>` left in a SQL file creates a
broken object. The templated files fail instead: `snow sql` renders `<% name %>`
from `-D` and aborts with `'name' is undefined` before connecting.

---

## Known gaps

- `deploy_snowflake.sh` applies all three files in one pass, but the stage
  cannot read anything until the Azure consent and role assignment exist.
  Running the script twice works; a `--phase` argument would be clearer.
- The storage integration is not in Terraform. All three parts are codeable
  (`azuread_service_principal` for Snowflake's fixed client id,
  `azurerm_role_assignment`, the Snowflake provider), but only after a first
  manual run reveals that client id. See ADR-019.
- No teardown procedure. An expired trial is simply abandoned, and this runbook
  is run again against a new account.
