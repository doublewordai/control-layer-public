-- Organization-wide default for the per-membership 'manage_keys' role.
--
-- Migration 128 made key creation in an organization a per-member grant and
-- left NEW memberships without it unless the inviter ticked a box. That box
-- only exists on the invite and add-member forms: someone who joins through
-- their email domain (auto-join, or an approved join request) never passes
-- one, so they land in the workspace unable to create a key and nothing on
-- their side says why. In practice the default surprised far more teams than
-- it protected.
--
-- What members could already NOT do is see or touch each other's keys: the
-- key list is scoped to the caller's own keys for everyone but owners and
-- admins. So the default flips to "members may create their own keys", and
-- the restrictive behaviour becomes a setting an organization chooses:
--
--   members_can_create_keys = true  (default) — a new plain member is given
--     the 'manage_keys' role when their membership is created, whichever way
--     they joined.
--   members_can_create_keys = false — new plain members start without it and
--     hold only keys an owner/admin issues to them, as before.
--
-- The setting is only the DEFAULT applied when a membership is created. The
-- per-member role in organization_member_roles stays the single source of
-- truth for what a member may do, so an owner/admin can still grant or revoke
-- it for one person, and flipping the setting never changes existing members.
--
-- Lives on `users` because organizations are rows in `users`
-- (`user_type = 'organization'`), alongside the other org-wide flags. It is
-- meaningless on a personal row.
--
-- Additive for the previous release: its pods never read the column, and the
-- constant default makes this a metadata-only change.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS members_can_create_keys BOOLEAN NOT NULL DEFAULT true;

COMMENT ON COLUMN users.members_can_create_keys IS
    'Organizations only: when true (default), a new plain member is granted the manage_keys org role when their membership is created, so they can create and manage their own API keys. When false, new members hold only keys issued to them by an owner/admin. Existing memberships are unaffected by changes.';

-- Members who joined while the default was "not granted" get the role now, so
-- the new default describes every workspace rather than only future joiners.
-- Pending invites are included so acceptance lands on the same footing.
-- Owners/admins hold the capability implicitly and never need a row.
--
-- This cannot tell a member who was never opted in from one an owner/admin
-- deliberately switched off: both are simply the absence of a row. An
-- organization that wants the restrictive behaviour turns the setting off and
-- revokes per member, which is the same two steps it takes on a new workspace.
INSERT INTO organization_member_roles (user_organization_id, role)
SELECT uo.id, 'manage_keys'
FROM user_organizations uo
WHERE uo.role = 'member'
  AND uo.status IN ('active', 'pending')
ON CONFLICT DO NOTHING;
