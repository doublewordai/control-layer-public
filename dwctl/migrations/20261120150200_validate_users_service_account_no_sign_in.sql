-- Validates the check added NOT VALID by the previous migration. VALIDATE
-- takes SHARE UPDATE EXCLUSIVE, so it doesn't block reads or writes.
SET LOCAL lock_timeout = '5s';

ALTER TABLE users VALIDATE CONSTRAINT users_service_account_no_sign_in;
