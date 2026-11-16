#!/usr/bin/env bash

set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL is required}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
SLEEP_SECONDS="${SLEEP_SECONDS:-2}"
MIN_AGE_DAYS="${MIN_AGE_DAYS:-14}"
QUEUES="${QUEUES:-}"
MAX_BATCHES="${MAX_BATCHES:-0}"
LOCK_TIMEOUT="${LOCK_TIMEOUT:-2s}"
STATEMENT_TIMEOUT="${STATEMENT_TIMEOUT:-30s}"
DRY_RUN="${DRY_RUN:-0}"

psql_q() {
    psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -At "$@"
}

run_sql() {
    printf "SET lock_timeout = '%s';\nSET statement_timeout = '%s';\n%s\n" \
        "$LOCK_TIMEOUT" "$STATEMENT_TIMEOUT" "$1" \
        | psql_q -v "queues=${QUEUES}"
}

is_uint() { [[ "$1" =~ ^(0|[1-9][0-9]*)$ ]]; }
is_uint "$BATCH_SIZE"   || { echo "BATCH_SIZE must be a non-negative integer without leading zeros" >&2; exit 2; }
is_uint "$MIN_AGE_DAYS" || { echo "MIN_AGE_DAYS must be a non-negative integer without leading zeros" >&2; exit 2; }
is_uint "$MAX_BATCHES"  || { echo "MAX_BATCHES must be a non-negative integer without leading zeros" >&2; exit 2; }
(( BATCH_SIZE > 0 ))    || { echo "BATCH_SIZE must be at least 1" >&2; exit 2; }
[[ "$SLEEP_SECONDS" =~ ^[0-9]+(\.[0-9]+)?$ ]] || { echo "SLEEP_SECONDS must be a number" >&2; exit 2; }
[[ "$LOCK_TIMEOUT" =~ ^[0-9]+(ms|s|min)?$ && "$STATEMENT_TIMEOUT" =~ ^[0-9]+(ms|s|min)?$ ]] \
    || { echo "LOCK_TIMEOUT / STATEMENT_TIMEOUT must be a Postgres duration such as 5s or 500ms" >&2; exit 2; }

predicate="state IN ('succeeded', 'failed')
      AND created_at < now() - make_interval(days => ${MIN_AGE_DAYS})
      AND created_at + ttl < now()
      AND (:'queues' = '' OR task_queue_name = ANY (string_to_array(:'queues', ',')))"

index_ok=$(psql_q -c "SELECT EXISTS (
    SELECT 1 FROM pg_index i
    JOIN pg_class c ON c.oid = i.indexrelid
    JOIN pg_am am ON am.oid = c.relam
    WHERE i.indexrelid = to_regclass('underway.idx_task_created_at')
      AND i.indrelid = 'underway.task'::regclass
      AND am.amname = 'btree'
      AND i.indisvalid AND i.indisready AND NOT i.indisunique
      AND i.indnkeyatts = 1 AND i.indnatts = 1
      AND pg_get_indexdef(i.indexrelid, 1, true) = 'created_at'
      AND i.indoption::text = '0'
      AND i.indpred IS NULL AND i.indexprs IS NULL)")
if [[ "$index_ok" != "t" ]]; then
    echo "underway.idx_task_created_at is missing or not a valid (created_at) btree; deploy the release that adds it (and let its migration finish) before purging" >&2
    exit 1
fi

if [[ "$DRY_RUN" == "1" ]]; then
    echo "Rows qualifying for deletion (bounded by STATEMENT_TIMEOUT=${STATEMENT_TIMEOUT}; raise it for a very large backlog):"
    run_sql "SELECT task_queue_name, state, count(*) FROM underway.task WHERE ${predicate} GROUP BY 1, 2 ORDER BY 3 DESC"
    exit 0
fi

echo "Purging expired underway tasks: batch=${BATCH_SIZE} sleep=${SLEEP_SECONDS}s min_age=${MIN_AGE_DAYS}d queues=${QUEUES:-all}"

total=0
batch=0
while :; do
    batch=$((batch + 1))
    deleted=$(run_sql "WITH victims AS (
                             SELECT task_queue_name, id
                             FROM underway.task
                             WHERE ${predicate}
                             ORDER BY created_at
                             LIMIT ${BATCH_SIZE}
                             FOR UPDATE SKIP LOCKED
                         ), gone AS (
                             DELETE FROM underway.task t
                             USING victims v
                             WHERE t.task_queue_name = v.task_queue_name AND t.id = v.id
                             RETURNING 1
                         )
                         SELECT count(*) FROM gone" | tail -n 1)
    total=$((total + deleted))
    printf '%s batch %d: deleted %d (total %d)\n' "$(date -u +%FT%TZ)" "$batch" "$deleted" "$total"
    if (( deleted < BATCH_SIZE )); then
        echo "Done: nothing left to purge (total ${total})"
        break
    fi
    if (( MAX_BATCHES > 0 && batch >= MAX_BATCHES )); then
        echo "Stopping after ${MAX_BATCHES} batches (total ${total}); rerun to continue"
        break
    fi
    sleep "$SLEEP_SECONDS"
done
