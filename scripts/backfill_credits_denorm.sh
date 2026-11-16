#!/usr/bin/env bash

set -euo pipefail

: "${DATABASE_URL:?set DATABASE_URL to the credits/http_analytics database}"
BATCH_SIZE="${BATCH_SIZE:-100000}"
SLEEP_SECONDS="${SLEEP_SECONDS:-0.1}"
START_SEQ="${START_SEQ:-0}"
FLEX_CUTOFF="${FLEX_CUTOFF:-2026-01-01}"
HELPER_IDX=idx_credits_tx_seq_backfill

psql_q() { psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -X -qtAc "$1"; }

MAX_SEQ=$(psql_q "SELECT COALESCE(MAX(seq), 0) FROM credits_transactions;")
if [ -z "$MAX_SEQ" ] || [ "$MAX_SEQ" -eq 0 ]; then
  echo "backfill_credits_denorm: no credits_transactions rows found; nothing to do" >&2
  exit 0
fi

echo "backfill_credits_denorm: sweeping seq (${START_SEQ}, ${MAX_SEQ}]  batch=${BATCH_SIZE}  sleep=${SLEEP_SECONDS}s"

echo "backfill_credits_denorm: (re)building helper index ${HELPER_IDX} CONCURRENTLY (may take a few minutes)…"
trap 'psql_q "DROP INDEX CONCURRENTLY IF EXISTS ${HELPER_IDX};" >/dev/null 2>&1 || true' EXIT INT TERM
psql_q "DROP INDEX CONCURRENTLY IF EXISTS ${HELPER_IDX};"
psql_q "CREATE INDEX CONCURRENTLY ${HELPER_IDX} ON credits_transactions (seq);"

CURSOR="$START_SEQ"
total_rows=0
batches=0
SECONDS=0
while [ "$CURSOR" -lt "$MAX_SEQ" ]; do
  hi=$(( CURSOR + BATCH_SIZE ))
  if [ "$hi" -gt "$MAX_SEQ" ]; then hi="$MAX_SEQ"; fi
  affected=$(psql_q "
    UPDATE credits_transactions ct
       SET service_tier = CASE WHEN ct.fusillade_batch_id IS NOT NULL THEN 'async' ELSE 'realtime' END
     WHERE ct.seq > ${CURSOR} AND ct.seq <= ${hi}
           AND ct.transaction_type = 'usage';
    UPDATE credits_transactions ct
       SET service_tier = 'batch'
      FROM fusillade.batches b
     WHERE ct.fusillade_batch_id = b.id
           AND b.completion_window = '24h'
           AND ct.seq > ${CURSOR} AND ct.seq <= ${hi}
           AND ct.service_tier = 'async';
    SELECT count(*) FROM credits_transactions
     WHERE seq > ${CURSOR} AND seq <= ${hi} AND transaction_type = 'usage';")
  total_rows=$(( total_rows + affected ))
  batches=$(( batches + 1 ))
  CURSOR="$hi"
  printf '  seq<=%-12s  rows %-7s  (total %s)\n' "$hi" "$affected" "$total_rows"
  sleep "${SLEEP_SECONDS}"
done

flex_where_cutoff=""
if [ -n "$FLEX_CUTOFF" ]; then flex_where_cutoff="AND ha.timestamp >= '${FLEX_CUTOFF}'"; fi
echo "backfill_credits_denorm: Phase 2 flex — scanning http_analytics ${FLEX_CUTOFF:+from ${FLEX_CUTOFF} }…"
flex_rows=$(psql_q "
  WITH f AS (
    UPDATE credits_transactions ct
       SET service_tier = 'flex'
      FROM http_analytics ha
     WHERE ha.id::text = ct.source_id
           ${flex_where_cutoff}
           AND ha.fusillade_batch_id IS NULL
           AND COALESCE(ha.batch_sla, '') <> ''
           AND ct.fusillade_batch_id IS NULL
           AND ct.service_tier = 'realtime'
    RETURNING 1
  )
  SELECT count(*) FROM f;")

elapsed=$SECONDS
echo "backfill_credits_denorm: DONE"
printf '  usage rows classified : %s\n' "$total_rows"
printf '  flex rows (Phase 2)   : %s\n' "$flex_rows"
printf '  duration              : %dm %02ds (sweep only, excl. index build)\n' $(( elapsed / 60 )) $(( elapsed % 60 ))
printf '  batches               : %s  (BATCH_SIZE=%s, SLEEP_SECONDS=%s)\n' "$batches" "$BATCH_SIZE" "$SLEEP_SECONDS"
printf '  seq swept             : (%s, %s]\n' "$START_SEQ" "$MAX_SEQ"

echo "backfill_credits_denorm: dropping helper index ${HELPER_IDX} CONCURRENTLY…"
psql_q "DROP INDEX CONCURRENTLY IF EXISTS ${HELPER_IDX};"

echo
echo "Spot-check tier distribution:"
echo "  psql \"\$DATABASE_URL\" -c \"SELECT service_tier, count(*) FROM credits_transactions WHERE transaction_type='usage' GROUP BY 1 ORDER BY 2 DESC;\""
