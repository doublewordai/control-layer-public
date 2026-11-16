
use crate::error::Result;
use crate::manager::RetainedResponseRetirementOutcome;
use crate::{PoolProvider, PostgresRequestManager};
use chrono::{Datelike, NaiveDate, Weekday};

use super::partition_retirement::{FamilySpec, retire};

fn monday_lower(lower: NaiveDate) -> bool {
    lower.weekday() == Weekday::Mon
}

fn weekly_child(week_start: NaiveDate) -> String {
    format!(
        "batch_requests_archive_y{}w{:02}",
        week_start.iso_week().year(),
        week_start.iso_week().week()
    )
}

const BATCH_ARCHIVE_FAMILY: FamilySpec = FamilySpec {
    parent: "batch_requests_archive",
    bucket_table: "batch_archive_buckets",
    bucket_key: "week_start",
    bounds_days: 7,
    valid_lower: monday_lower,
    canonical_child: weekly_child,
    orphan_fence_sql: r#"
        SELECT EXISTS (
            SELECT 1
            FROM batch_archive_buckets bucket
            WHERE (
                bucket.state = 'retiring'
                AND NOT EXISTS (
                    SELECT 1
                    FROM retention_partition_retirements journal
                    WHERE journal.parent_table = 'batch_requests_archive'
                      AND journal.partition_schema = bucket.partition_schema
                      AND journal.partition_table = bucket.partition_table
                      AND journal.partition_oid = bucket.partition_oid
                      AND journal.lower_bound = bucket.week_start
                      AND journal.upper_bound = bucket.week_start + 7
                      AND journal.completed_at IS NULL
                )
            ) OR (
                bucket.state = 'retired'
                AND NOT EXISTS (
                    SELECT 1
                    FROM retention_partition_retirements journal
                    WHERE journal.parent_table = 'batch_requests_archive'
                      AND journal.partition_schema = bucket.partition_schema
                      AND journal.partition_table = bucket.partition_table
                      AND journal.partition_oid = bucket.partition_oid
                      AND journal.lower_bound = bucket.week_start
                      AND journal.upper_bound = bucket.week_start + 7
                      AND journal.completed_at IS NOT NULL
                )
            )
        )
        "#,
    candidate_sql: r#"
        SELECT bucket.partition_schema,
               namespace.oid AS partition_schema_oid,
               parent.oid AS parent_oid,
               bucket.partition_table,
               bucket.partition_oid,
               bucket.week_start AS lower_bound,
               bucket.week_start + 7 AS upper_bound
        FROM batch_archive_buckets bucket
        JOIN pg_namespace namespace
          ON namespace.nspname = bucket.partition_schema
        JOIN pg_class parent
          ON parent.relnamespace = namespace.oid
         AND parent.relname = 'batch_requests_archive'
        WHERE bucket.state = 'active'
          AND (bucket.week_start + 7 + make_interval(days => $1))
                <= statement_timestamp() AT TIME ZONE 'UTC'
          AND NOT EXISTS (
              SELECT 1
              FROM batches b
              WHERE b.counts_frozen_at IS NULL
                AND b.deleted_at IS NULL
                AND b.cancelled_at IS NULL
                AND b.created_at >= (bucket.week_start::timestamp AT TIME ZONE 'UTC')
                AND b.created_at < ((bucket.week_start + 7)::timestamp AT TIME ZONE 'UTC')
          )
          AND NOT EXISTS (
              SELECT 1
              FROM batches b
              WHERE b.location IN ('live', 'split')
                AND b.counts_frozen_at IS NULL
                AND b.deleted_at IS NULL
                AND b.archive_bucket = bucket.week_start
          )
          AND NOT EXISTS (
              SELECT 1
              FROM batches b
              WHERE b.location IN ('live', 'split')
                AND b.counts_frozen_at IS NOT NULL
                AND b.deleted_at IS NULL
                AND b.archive_bucket = bucket.week_start
          )
        ORDER BY bucket.week_start
        LIMIT 1
        "#,
    candidate_binds_retention: true,
    recheck_sql: Some(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM batches b
            WHERE b.counts_frozen_at IS NULL
              AND b.deleted_at IS NULL
              AND b.cancelled_at IS NULL
              AND b.created_at >= ($1::date::timestamp AT TIME ZONE 'UTC')
              AND b.created_at < (($1::date + 7)::timestamp AT TIME ZONE 'UTC')
        ) OR EXISTS (
            SELECT 1
            FROM batches b
            WHERE b.location IN ('live', 'split')
              AND b.counts_frozen_at IS NULL
              AND b.deleted_at IS NULL
              AND b.archive_bucket = $1::date
        ) OR EXISTS (
            SELECT 1
            FROM batches b
            WHERE b.location IN ('live', 'split')
              AND b.counts_frozen_at IS NOT NULL
              AND b.deleted_at IS NULL
              AND b.archive_bucket = $1::date
        )
        "#,
    ),
    completion_sql: Some(
        "UPDATE batches SET retention_expired_at = $3 \
         WHERE deleted_at IS NOT NULL AND retention_expired_at IS NULL \
           AND archive_bucket >= $1 AND archive_bucket < $2 \
           AND NOT EXISTS (SELECT 1 FROM requests r WHERE r.batch_id = batches.id)",
    ),
};

pub(super) async fn retire_expired_batch_archive_partition<P: PoolProvider>(
    manager: &PostgresRequestManager<P>,
    select_new: bool,
    retention_days: i32,
) -> Result<RetainedResponseRetirementOutcome> {
    retire(
        manager,
        &BATCH_ARCHIVE_FAMILY,
        select_new,
        Some(retention_days),
    )
    .await
}
