#!/bin/bash
# Bootstrap Key Vault secrets after every terraform apply.
# Run this once per session before starting ADF pipelines.
#
# Prerequisites:
#   source session.sh   — sets TF_VAR_sql_admin_password

set -e  # exit immediately if any command fails

echo "Bootstrapping Key Vault secrets..."

az keyvault secret set \
  --vault-name retail-pipeline-dev-kv \
  --name sql-admin-password \
  --value "$TF_VAR_sql_admin_password"

echo "Done. sql-admin-password is set in Key Vault."
