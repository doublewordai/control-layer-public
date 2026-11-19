-- Prices written through the admin API: one row per decision, with who made
-- it, why, the price billing charged before and the one it charges after.
-- The tariff rows themselves carry provisioning_source = 'admin-api'.
SET LOCAL lock_timeout = '5s';

CREATE TABLE account_price_changes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- The caller's Idempotency-Key: a retry with the same key returns the
    -- stored response instead of writing again.
    request_id TEXT UNIQUE CONSTRAINT chk_account_price_changes_request_id CHECK (length(request_id) BETWEEN 1 AND 255),
    -- Fingerprint of the request the key was first used with, so a key reused
    -- for a different request is refused rather than replayed.
    request_fingerprint TEXT NOT NULL,
    -- noop: a keyed DELETE that found nothing to end, kept so its retry
    -- replays instead of ending a price set since.
    action TEXT NOT NULL CONSTRAINT chk_account_price_changes_action CHECK (action IN ('set', 'end', 'noop')),
    -- No foreign keys: the audit outlives the account and the model.
    user_id UUID NOT NULL,
    deployed_model_id UUID NOT NULL,
    serving_class TEXT,
    valid_from TIMESTAMPTZ NOT NULL,
    old_input_price_per_token DECIMAL(12, 8),
    old_output_price_per_token DECIMAL(12, 8),
    old_cache_read_multiplier DECIMAL(6, 4),
    new_input_price_per_token DECIMAL(12, 8),
    new_output_price_per_token DECIMAL(12, 8),
    new_cache_read_multiplier DECIMAL(6, 4),
    reason TEXT NOT NULL CONSTRAINT chk_account_price_changes_reason CHECK (length(btrim(reason)) > 0),
    changed_by UUID NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- What the request answered, replayed for a retry.
    response JSONB NOT NULL
);

COMMENT ON TABLE account_price_changes IS
  'Audit of account prices set or ended through the admin API (provisioning_source = admin-api): who, when, why, and the billed price before and after.';
COMMENT ON COLUMN account_price_changes.changed_by IS
  'The user who made the change. Not a foreign key, so the audit outlives the user.';

CREATE INDEX idx_account_price_changes_account_model
    ON account_price_changes (user_id, deployed_model_id, changed_at DESC);

COMMENT ON COLUMN model_tariffs.provisioning_source IS
  'NULL = manually managed; org-overlays = organisation catalog; account-catalog = account catalog; admin-api = the account price admin API';
COMMENT ON COLUMN model_cache_tariffs.provisioning_source IS
  'NULL = manually managed; org-overlays = organisation catalog; account-catalog = account catalog; admin-api = the account price admin API';
