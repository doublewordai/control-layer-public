#!/usr/bin/env bash

set -euo pipefail

: "${DATABASE_URL:?set DATABASE_URL to the http_analytics database}"
BATCH_SIZE="${BATCH_SIZE:-20000}"
SLEEP_SECONDS="${SLEEP_SECONDS:-0.1}"

psql_q() { psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -X -qtAc "$1"; }

bounds=$(psql_q "SELECT coalesce(min(id),1) || ' ' || coalesce(max(id),0) FROM http_analytics")
MIN_ID="${bounds%% *}"
MAX_ID="${bounds##* }"

CURSOR=$(( ${START_ID:-$MIN_ID} - 1 ))

echo "backfill_uncached_cost: sweeping id (${CURSOR}, ${MAX_ID}]  batch=${BATCH_SIZE}  sleep=${SLEEP_SECONDS}s"

total=0
batches=0
SECONDS=0
while [ "$CURSOR" -lt "$MAX_ID" ]; do
  hi=$(( CURSOR + BATCH_SIZE ))
  if [ "$hi" -gt "$MAX_ID" ]; then hi="$MAX_ID"; fi
  affected=$(psql_q "
    WITH upd AS (
      UPDATE http_analytics
         SET uncached_cost = total_cost
       WHERE id > ${CURSOR} AND id <= ${hi}
         AND uncached_cost IS NULL
         AND total_cost IS NOT NULL
      RETURNING 1
    )
    SELECT count(*) FROM upd;")
  total=$(( total + affected ))
  batches=$(( batches + 1 ))
  CURSOR="$hi"
  printf '  id<=%-12s  filled +%-6s  (total %s)\n' "$hi" "$affected" "$total"
  sleep "${SLEEP_SECONDS}"
done

elapsed=$SECONDS
if [ "$elapsed" -gt 0 ]; then rate=$(( total / elapsed )); else rate="$total"; fi
echo "backfill_uncached_cost: DONE"
printf '  rows filled : %s\n' "$total"
printf '  duration    : %dm %02ds (wall-clock, incl. throttle sleeps)\n' $(( elapsed / 60 )) $(( elapsed % 60 ))
printf '  throughput  : ~%s rows/s\n' "$rate"
printf '  batches     : %s  (BATCH_SIZE=%s, SLEEP_SECONDS=%s)\n' "$batches" "$BATCH_SIZE" "$SLEEP_SECONDS"
printf '  id swept    : up to %s\n' "$MAX_ID"
