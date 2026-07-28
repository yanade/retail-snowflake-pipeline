#!/bin/bash
# Load the retail_oltp schema and populate it with seed + generated data.
# Assumes the PostgreSQL server/database already exist (created via
# terraform apply — terraform/modules/postgres/, see ADR-009).
#
# Prerequisites:
#   DATABASE_URL set in .env at the project root (see database/README.md)
#   Run once per session — the server comes back empty every time
#   (ADR-004/ADR-009 destroy-and-recreate cost discipline).

set -e  # exit immediately if any command fails

# psql doesn't read .env automatically (that's Python-specific, via
# python-dotenv in seed.py/generate_data.py) — pull it in for this step.
if [ -z "$DATABASE_URL" ] && [ -f .env ]; then
  export $(grep '^DATABASE_URL=' .env | xargs)
fi

if [ -z "$DATABASE_URL" ]; then
  echo "Error: DATABASE_URL is not set (checked shell env and .env)." >&2
  exit 1
fi

echo "Loading schema..."
psql "$DATABASE_URL" -f database/schema.sql

echo "Loading master/reference data..."
(cd database && python seed.py)

echo "Generating transactional data..."
(cd database && python generate_data.py --days 30 --orders-per-day 50 --seed 42)

echo "Done."
