#!/bin/bash
# Simulate a day of source activity: new orders plus late-arriving status
# updates, so the next ADF run has something incremental to carry.
#
# Prerequisites:
#   Postgres running (az postgres flexible-server start ...)
#   .env with DATABASE_URL pointing at the Azure server
#
# Usage:
#   ./scripts/simulate_source_changes.sh             # 2 days, 20 orders/day, 25 updates
#   ./scripts/simulate_source_changes.sh 3 30 50     # days, orders per day, updates
#   SEED=11 ./scripts/simulate_source_changes.sh     # a different random draw

set -e  # exit immediately if any command fails

DAYS="${1:-2}"
ORDERS_PER_DAY="${2:-20}"
UPDATE_COUNT="${3:-25}"
SEED="${SEED:-7}"  # fixed by default, so a run is repeatable

# psql doesn't read .env automatically, same pattern as load_database.sh
if [ -z "$DATABASE_URL" ] && [ -f .env ]; then
  export $(grep '^DATABASE_URL=' .env | xargs)
fi

if [ -z "$DATABASE_URL" ]; then
  echo "Error: DATABASE_URL is not set (checked shell env and .env)." >&2
  exit 1
fi

snapshot() {
  # -tAc: no header, no padding, one command, so the value is usable as text
  psql "$DATABASE_URL" -tAc "
    SELECT 'orders=' || count(*)
        || ' last_order_date=' || (max(order_date) AT TIME ZONE 'UTC')::date
        || ' last_update=' || max(updated_at)
    FROM retail_oltp.orders;"
}

echo "Before: $(snapshot)"

echo "Appending ${DAYS} day(s) at about ${ORDERS_PER_DAY} orders per day..."
# cd database: the generator imports its helpers from generate_data.py next to it
(cd database && python generate_incremental_data.py --days "$DAYS" --orders-per-day "$ORDERS_PER_DAY" --seed "$SEED")

echo "Changing the status of ${UPDATE_COUNT} existing orders..."
# The lowest ids, so the same rows can be checked in curated afterwards.
# The set_updated_at trigger refreshes updated_at, which is what the watermark sees.
UPDATED=$(psql "$DATABASE_URL" -tAc "
  WITH changed AS (
    UPDATE retail_oltp.orders
    SET order_status = CASE WHEN order_status = 'SHIPPED' THEN 'COMPLETED' ELSE 'SHIPPED' END
    WHERE order_id IN (SELECT order_id FROM retail_oltp.orders ORDER BY order_id LIMIT ${UPDATE_COUNT})
    RETURNING order_id
  )
  SELECT count(*) FROM changed;")

echo "After:  $(snapshot)"
echo "Updated ${UPDATED} existing orders."

# New order dates may fall outside the fetched FX range. Failing here is much
# cheaper than a missing rate surfacing three stages downstream.
UNCOVERED=$(psql "$DATABASE_URL" -tAc "
  SELECT count(*)
  FROM retail_oltp.orders o
  WHERE o.currency_code <> '${BASE_CURRENCY:-GBP}'
    AND NOT EXISTS (
      SELECT 1 FROM retail_oltp.exchange_rates r
      WHERE r.rate_date = (o.order_date AT TIME ZONE 'UTC')::date
        AND r.target_currency_code = o.currency_code
    );")

if [ "$UNCOVERED" -gt 0 ]; then
  echo "Error: ${UNCOVERED} orders have no FX rate for their date. Run ./scripts/bootstrap_fx_rates.sh" >&2
  exit 1
fi

echo "FX coverage complete. Ready to trigger pl_load_data in ADF."
