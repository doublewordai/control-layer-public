#!/usr/bin/env bash

set -euo pipefail

: "${DATABASE_URL:?set DATABASE_URL to the credits/http_analytics database}"

echo "backfill_batch_aggregates_denorm: set-based service_tier backfill"

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -X <<'SQL'
\timing on
UPDATE batch_aggregates ba
   SET service_tier = CASE
         WHEN (SELECT b.completion_window
                 FROM fusillade.batches b
                WHERE b.id = ba.fusillade_batch_id) = '24h'
         THEN 'batch' ELSE 'async' END
 WHERE ba.service_tier IS NULL;
SQL

echo "backfill_batch_aggregates_denorm: DONE"
