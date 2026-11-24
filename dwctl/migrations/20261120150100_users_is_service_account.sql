-- Marks accounts that exist only to own service API keys. They are created and
-- updated by the service-account loader, never sign in, and are the only
-- accounts the service roles take effect on.
SET LOCAL lock_timeout = '5s';

ALTER TABLE users ADD COLUMN IF NOT EXISTS is_service_account BOOLEAN NOT NULL DEFAULT FALSE;

-- A service account can never hold a sign-in credential or be linked to an
-- identity-provider subject, whichever code path tries. Added NOT VALID so it
-- takes no table scan here; the next migration validates it.
ALTER TABLE users ADD CONSTRAINT users_service_account_no_sign_in CHECK (
    NOT is_service_account
    OR (password_hash IS NULL AND external_user_id IS NULL AND user_type = 'individual')
) NOT VALID;
