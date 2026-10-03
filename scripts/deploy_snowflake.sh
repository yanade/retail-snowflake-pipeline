#!/bin/bash
# Apply the Snowflake DDL, with locations taken from .env.
#
# Usage:
#   ./scripts/deploy_snowflake.sh             # uses the retail_admin connection
#   ./scripts/deploy_snowflake.sh other_conn

set -e

CONNECTION="${1:-retail_admin}"

# snow doesn't read .env, same pattern as load_database.sh
if [ -z "$ADLS_ACCOUNT_NAME" ] && [ -f .env ]; then
  export $(grep -E '^(ADLS_ACCOUNT_NAME|ADLS_SERVED_CONTAINER|AZURE_TENANT_ID|SNOWFLAKE_USER)=' .env | xargs)
fi

# :? fails with the given message if the variable is empty
: "${ADLS_ACCOUNT_NAME:?not set, check .env}"
: "${ADLS_SERVED_CONTAINER:?not set, check .env}"
: "${AZURE_TENANT_ID:?not set, check .env}"
: "${SNOWFLAKE_USER:?not set, check .env}"

VARS=(-D "adls_account=$ADLS_ACCOUNT_NAME"
      -D "served_container=$ADLS_SERVED_CONTAINER"
      -D "tenant_id=$AZURE_TENANT_ID"
      -D "snowflake_user=$SNOWFLAKE_USER")

snow sql -c "$CONNECTION" "${VARS[@]}" -f snowflake/ddl/create_schema.sql

# No placeholders: table names and columns are fixed by the architecture (ADR-020)
snow sql -c "$CONNECTION" -f snowflake/ddl/create_raw_tables.sql

snow sql -c "$CONNECTION" "${VARS[@]}" -f snowflake/ddl/create_storage_integration.sql

echo "Consent and the Azure role assignment must be done before the stage."
snow sql -c "$CONNECTION" "${VARS[@]}" -f snowflake/ddl/create_stage.sql