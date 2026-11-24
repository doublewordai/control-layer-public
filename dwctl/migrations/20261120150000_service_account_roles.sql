-- Roles held only by service accounts (users.is_service_account): each grants
-- one narrow capability to a machine caller instead of a platform-wide key.
ALTER TYPE user_role ADD VALUE IF NOT EXISTS 'QUEUEDEMANDREADER';
ALTER TYPE user_role ADD VALUE IF NOT EXISTS 'CATALOGUEREADER';
ALTER TYPE user_role ADD VALUE IF NOT EXISTS 'CATALOGUEREADERALL';
ALTER TYPE user_role ADD VALUE IF NOT EXISTS 'OPENROUTERPRICEMANAGER';
