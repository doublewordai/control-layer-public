-- Compute pool reported by the upstream worker that served the request
-- (read from the onwards ServedBy response extension at capture time).
-- NULL for requests served by providers that do not report a pool, for
-- requests that never reached an upstream, and for historical rows.
SET LOCAL lock_timeout = '5s';

ALTER TABLE http_analytics ADD COLUMN IF NOT EXISTS compute_pool TEXT;

COMMENT ON COLUMN http_analytics.compute_pool IS
    'Compute pool identifier reported by the upstream worker that served the request; NULL when unreported';
