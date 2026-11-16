#!/usr/bin/env bash

set -euo pipefail

: "${DATABASE_URL:?set DATABASE_URL to the http_analytics database}"
BATCH_SIZE="${BATCH_SIZE:-20000}"
SLEEP_SECONDS="${SLEEP_SECONDS:-0.1}"

psql_q() { psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -X -qtAc "$1"; }

psql_q "CREATE TABLE IF NOT EXISTS user_model_usage_daily_backfill_progress (
          id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id = TRUE),
          last_swept_id BIGINT NOT NULL DEFAULT 0,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());" >/dev/null
psql_q "INSERT INTO user_model_usage_daily_backfill_progress (last_swept_id)
        VALUES (0) ON CONFLICT (id) DO NOTHING;" >/dev/null

WATERMARK=$(psql_q "SELECT backfill_watermark FROM user_model_usage_daily_cursor WHERE id = TRUE;")
CURSOR=$(psql_q "SELECT last_swept_id FROM user_model_usage_daily_backfill_progress WHERE id = TRUE;")

if [ -z "$WATERMARK" ]; then
  echo "backfill_user_model_usage_daily: no watermark found (is migration 110 applied?)" >&2
  exit 1
fi

echo "backfill_user_model_usage_daily: sweeping id (${CURSOR}, ${WATERMARK}]  batch=${BATCH_SIZE}  sleep=${SLEEP_SECONDS}s"

total_rows=0
batches=0
SECONDS=0
while [ "$CURSOR" -lt "$WATERMARK" ]; do
  hi=$(( CURSOR + BATCH_SIZE ))
  if [ "$hi" -gt "$WATERMARK" ]; then hi="$WATERMARK"; fi
  affected=$(psql_q "
    WITH agg AS (
      INSERT INTO user_model_usage_daily (user_id, model, usage_date, input_tokens, output_tokens, cost, request_count)
      SELECT user_id,
             model,
             (timestamp AT TIME ZONE 'UTC')::date,
             COALESCE(SUM(prompt_tokens), 0),
             COALESCE(SUM(completion_tokens), 0),
             COALESCE(SUM(total_cost), 0),
             COUNT(*)
      FROM http_analytics
      WHERE id > ${CURSOR} AND id <= ${hi}
            AND user_id IS NOT NULL AND model IS NOT NULL
            AND status_code BETWEEN 200 AND 299
      GROUP BY user_id, model, (timestamp AT TIME ZONE 'UTC')::date
      ON CONFLICT (user_id, model, usage_date) DO UPDATE SET
          input_tokens  = user_model_usage_daily.input_tokens  + EXCLUDED.input_tokens,
          output_tokens = user_model_usage_daily.output_tokens + EXCLUDED.output_tokens,
          cost          = user_model_usage_daily.cost          + EXCLUDED.cost,
          request_count = user_model_usage_daily.request_count + EXCLUDED.request_count,
          updated_at    = NOW()
      RETURNING 1
    ), prog AS (
      UPDATE user_model_usage_daily_backfill_progress
         SET last_swept_id = ${hi}, updated_at = NOW()
       WHERE id = TRUE
    )
    SELECT count(*) FROM agg;")
  total_rows=$(( total_rows + affected ))
  batches=$(( batches + 1 ))
  CURSOR="$hi"
  printf '  id<=%-12s  rollup rows +%-6s  (total %s)\n' "$hi" "$affected" "$total_rows"
  sleep "${SLEEP_SECONDS}"
done

elapsed=$SECONDS
echo "backfill_user_model_usage_daily: DONE"
printf '  rollup rows written : %s\n' "$total_rows"
printf '  duration            : %dm %02ds (wall-clock, incl. throttle sleeps)\n' $(( elapsed / 60 )) $(( elapsed % 60 ))
printf '  batches             : %s  (BATCH_SIZE=%s, SLEEP_SECONDS=%s)\n' "$batches" "$BATCH_SIZE" "$SLEEP_SECONDS"
printf '  id swept            : up to %s (watermark)\n' "$WATERMARK"
echo
echo "Validate before cutover — daily SUM vs the old all-time rollup should match per (user, model):"
echo "  psql \"\$DATABASE_URL\" -f scripts/validate_user_model_usage_daily.sql"
