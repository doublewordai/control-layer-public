-- OpenRouter listing for a model (DW-1287): the per-model values of OpenRouter's
-- model feed, written by model provisioning from settings.openrouter. NULL keeps
-- the model out of the feed. Nullable with no default, so adding it rewrites no rows.
-- The ALTER needs a brief ACCESS EXCLUSIVE lock on a hot table: fail fast
-- rather than queue requests behind a blocked migration.
SET LOCAL lock_timeout = '5s';

ALTER TABLE deployed_models ADD COLUMN openrouter JSONB;

COMMENT ON COLUMN deployed_models.openrouter IS
    'OpenRouter listing (crate::openrouter::listing::OpenRouterListing); NULL leaves the model out of the OpenRouter feed';
