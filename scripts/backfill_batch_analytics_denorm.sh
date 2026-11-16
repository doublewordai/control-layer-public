#!/usr/bin/env bash

set -euo pipefail

: "${DATABASE_URL:?set DATABASE_URL to the credits/http_analytics database}"
QUIESCENT_INTERVAL="${QUIESCENT_INTERVAL:-25 hours}"

echo "backfill_batch_analytics_denorm: set-based backfill  quiescent='${QUIESCENT_INTERVAL}'"

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -X <<SQL
\timing on
BEGIN;

DROP TABLE IF EXISTS _bf_analytics;
CREATE TEMP TABLE _bf_analytics ON COMMIT DROP AS
  SELECT fusillade_batch_id                             AS fbid,
         COUNT(id)                                       AS total_requests,
         COALESCE(SUM(prompt_tokens), 0)                 AS prompt_tokens,
         COALESCE(SUM(completion_tokens), 0)             AS completion_tokens,
         COALESCE(SUM(reasoning_tokens), 0)              AS reasoning_tokens,
         COALESCE(SUM(total_tokens), 0)                  AS total_tokens,
         COALESCE(SUM(duration_ms), 0)                   AS sum_duration_ms,
         COUNT(duration_ms)                              AS count_duration_ms,
         COALESCE(SUM(duration_to_first_byte_ms), 0)     AS sum_ttfb_ms,
         COUNT(duration_to_first_byte_ms)                AS count_ttfb_ms,
         COALESCE(SUM(uncached_cost), 0)                 AS total_list_cost
  FROM http_analytics
  WHERE fusillade_batch_id IS NOT NULL
    AND user_id IS NOT NULL
    -- Exclude the system user, matching the live fold which skips Uuid::nil().
    AND user_id <> '00000000-0000-0000-0000-000000000000'
    AND status_code BETWEEN 200 AND 299
  GROUP BY fusillade_batch_id;
CREATE INDEX ON _bf_analytics (fbid);

-- Batches WITH retained data.
UPDATE batch_aggregates ba
   SET total_requests          = b.total_requests,
       total_prompt_tokens     = b.prompt_tokens,
       total_completion_tokens = b.completion_tokens,
       total_reasoning_tokens  = b.reasoning_tokens,
       total_tokens            = b.total_tokens,
       sum_duration_ms         = b.sum_duration_ms,
       count_duration_ms       = b.count_duration_ms,
       sum_ttfb_ms             = b.sum_ttfb_ms,
       count_ttfb_ms           = b.count_ttfb_ms,
       total_list_cost         = b.total_list_cost,
       analytics_backfilled_at = NOW()
  FROM _bf_analytics b
 WHERE b.fbid = ba.fusillade_batch_id
   AND ba.analytics_backfilled_at IS NULL
   AND ba.updated_at < NOW() - INTERVAL '${QUIESCENT_INTERVAL}';

-- Eligible batches with NO retained http_analytics (aged out / all-non-2xx) -> zeros.
UPDATE batch_aggregates ba
   SET total_requests = 0, total_prompt_tokens = 0, total_completion_tokens = 0,
       total_reasoning_tokens = 0, total_tokens = 0, sum_duration_ms = 0,
       count_duration_ms = 0, sum_ttfb_ms = 0, count_ttfb_ms = 0, total_list_cost = 0,
       analytics_backfilled_at = NOW()
 WHERE ba.analytics_backfilled_at IS NULL
   AND ba.updated_at < NOW() - INTERVAL '${QUIESCENT_INTERVAL}'
   AND NOT EXISTS (SELECT 1 FROM _bf_analytics b WHERE b.fbid = ba.fusillade_batch_id);

COMMIT;
SQL

echo "backfill_batch_analytics_denorm: DONE (re-run to pick up batches that have since quiesced)"
