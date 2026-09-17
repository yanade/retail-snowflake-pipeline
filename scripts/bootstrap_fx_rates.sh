#!/bin/bash
# Fetches FX rates for the date range covered by retail_oltp.orders and upserts
# them into retail_oltp.exchange_rates.
#
# Run after load_database.sh. Nothing else fetches rates, and skipping this
# leaves every downstream FX conversion without a rate to use.
#
# Prerequisites:
#   .env with DATABASE_URL, EXCHANGE_RATE_API_KEY, BASE_CURRENCY, TARGET_CURRENCIES
#
# Usage:
#   ./scripts/bootstrap_fx_rates.sh                    # range derived from orders
#   ./scripts/bootstrap_fx_rates.sh 2025-01-01 2025-02-02   # explicit range

set -e  # exit immediately if any command fails

BUFFER_DAYS=3  # fetch a few days past the last order, so later orders still convert

# read .env automatically
if [ -z "$DATABASE_URL" ] && [ -f .env ]; then
  export $(grep '^DATABASE_URL=' .env | xargs)
fi

if [ -z "$DATABASE_URL" ]; then
  echo "Error: DATABASE_URL is not set (checked shell env and .env)." >&2
  exit 1
fi

START_DATE="$1"
END_DATE="$2"

# No dates given: derive them from the data, so the rates always cover the orders
if [ -z "$START_DATE" ] || [ -z "$END_DATE" ]; then
  # -t no header, -A unaligned, -c one command: gives a bare value, not a table
  START_DATE=$(psql "$DATABASE_URL" -tAc "SELECT min(order_date)::date FROM retail_oltp.orders;")
  END_DATE=$(psql "$DATABASE_URL" -tAc "SELECT max(order_date)::date + $BUFFER_DAYS FROM retail_oltp.orders;")
fi

if [ -z "$START_DATE" ] || [ -z "$END_DATE" ]; then
  echo "Error: orders table is empty and no date range was given. Run load_database.sh first." >&2
  exit 1
fi

echo "Fetching FX rates from $START_DATE to $END_DATE..."

# --write-postgres is what actually upserts. Without it the script only writes JSON.
python -m ingestion.api_ingest.fetch_fx_rates \
  --start "$START_DATE" \
  --end "$END_DATE" \
  --write-postgres

# Fail loudly: a silent zero here would only surface downstream in Databricks
ROWS=$(psql "$DATABASE_URL" -tAc "SELECT count(*) FROM retail_oltp.exchange_rates;")
if [ "$ROWS" -eq 0 ]; then
  echo "Error: exchange_rates is still empty after the fetch." >&2
  exit 1
fi

# Orders whose currency has no rate for their date would hit dead-letter later
UNCOVERED=$(psql "$DATABASE_URL" -tAc "
  SELECT count(*)
  FROM retail_oltp.orders o
  WHERE o.currency_code <> '${BASE_CURRENCY:-GBP}'
    AND NOT EXISTS (
      SELECT 1 FROM retail_oltp.exchange_rates r
      WHERE r.rate_date = o.order_date::date
        AND r.target_currency_code = o.currency_code
    );")

echo "Done. exchange_rates holds $ROWS rows ($START_DATE to $END_DATE)."
echo "Orders with no matching rate: $UNCOVERED"