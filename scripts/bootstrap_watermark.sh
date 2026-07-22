#!/bin/bash
# Creates watermark control tables and seeds initial data in watermark-db.
# Run once after terraform apply and bootstrap_keyvault.sh.
#
# Prerequisites:
#   source session.sh   — sets TF_VAR_sql_admin_password
#   sqlcmd installed    — brew install sqlcmd

set -e  # exit immediately if any command fails

SQL_SERVER=$(cd terraform && terraform output -raw sql_server_fqdn)
SQL_DATABASE="watermark-db"
SQL_USER="sqladmin"
SQL_FILE="ingestion/watermark/watermark_control.sql"

LOCAL_IP=$(curl -s ifconfig.me)
echo "Adding firewall rule for IP: $LOCAL_IP..."

az sql server firewall-rule create \
  --name allow-local-dev \
  --server retail-pipeline-dev-sql \
  --resource-group retail-pipeline-dev-rg \
  --start-ip-address "$LOCAL_IP" \
  --end-ip-address "$LOCAL_IP"

echo "Creating watermark tables in $SQL_DATABASE..."

sqlcmd \
  -S "$SQL_SERVER" \
  -d "$SQL_DATABASE" \
  -U "$SQL_USER" \
  -P "$TF_VAR_sql_admin_password" \
  -i "$SQL_FILE"

echo "Done. Verifying tables..."

sqlcmd \
  -S "$SQL_SERVER" \
  -d "$SQL_DATABASE" \
  -U "$SQL_USER" \
  -P "$TF_VAR_sql_admin_password" \
  -Q "SELECT pipeline_name, source_type, watermark_column, is_active FROM pipeline_config;"

echo "Watermark bootstrap complete."
