#!/usr/bin/env bash
set -euo pipefail

# Helper to generate a squashed schema SQL file from a running Postgres DB.
# Usage: run from repo root; requires docker compose with a `db` service
# that respects POSTGRES_USER/POSTGRES_DB env vars in compose file.

OUT_FILE="bot/migrations/schemas/squashed_schema.sql"

echo "Generating squashed schema to $OUT_FILE"

docker compose exec -T db pg_dump -s -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  | sed '/^\s*SET\s\|^\s*SELECT pg_catalog.set_config/d' \
  | sed '/^\s*COMMENT ON\s\|^\s*ALTER\sOWNER\s\|^\s*REVOKE\s\|^\s*GRANT\s/d' \
  > "$OUT_FILE"

echo "Post-processing: remove lines that may break when run under a different user."
echo "Wrote: $OUT_FILE"

echo "Next: review $OUT_FILE, remove anything non-DDL if needed, then commit and run migrations."
