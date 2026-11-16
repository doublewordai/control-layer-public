#!/usr/bin/env bash

set -euo pipefail

: "${DATABASE_URL:?set DATABASE_URL to the credits/http_analytics database}"
WINDOW="${WINDOW:-50000}"
SLEEP_SECONDS="${SLEEP_SECONDS:-0.05}"
START_ID="${START_ID:-0}"

psql_q() { psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -X -qtAc "$1"; }

MAX_ID="${MAX_ID:-$(psql_q 'SELECT max(id) FROM http_analytics')}"

echo "backfill_credits_fusillade_request_id: ha.id windows [${START_ID}, ${MAX_ID}]  window=${WINDOW}  sleep=${SLEEP_SECONDS}s"

lo="$START_ID"
total=0
SECONDS=0
while [ "$lo" -le "$MAX_ID" ]; do
  hi=$(( lo + WINDOW ))
  updated=$(psql_q "
    SET enable_hashjoin = off;
    SET enable_mergejoin = off;
    WITH upd AS (
      UPDATE credits_transactions ct
         SET fusillade_request_id = ha.fusillade_request_id
        FROM http_analytics ha
       WHERE ha.id >= ${lo} AND ha.id < ${hi}
         AND ha.fusillade_request_id IS NOT NULL
         AND ct.source_id = ha.id::text
         AND ct.transaction_type = 'usage'
         AND ct.fusillade_request_id IS NULL
      RETURNING 1)
    SELECT count(*) FROM upd;")
  total=$(( total + updated ))
  printf '  ha.id<%-12s  linked +%-7s  (total %s)  [%ds]\n' "$hi" "$updated" "$total" "$SECONDS"
  lo="$hi"
  sleep "${SLEEP_SECONDS}"
done

echo "backfill_credits_fusillade_request_id: DONE"
printf '  credit rows linked : %s\n' "$total"
printf '  duration           : %dm %02ds\n' $(( SECONDS / 60 )) $(( SECONDS % 60 ))
echo "  re-run to catch rows inserted during the sweep (idempotent via fusillade_request_id IS NULL)" >&2
