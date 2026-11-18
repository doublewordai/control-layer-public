-- One row per hosted checkout the payment handlers open, so the number of
-- checkouts that never ended in a payment can be capped per user (see
-- `limits.checkout`).
--
-- `actor_id` is the user who opened the checkout and is what the cap counts.
-- `payer_id` is the billing account it was for: the same user, or an
-- organization they manage. Counting by payer would give a user a fresh
-- allowance for every organization they create.
--
-- The row is inserted before the provider is called, in the same transaction
-- that counts the user's unpaid rows under a per-user lock, so parallel
-- requests cannot all pass the cap. `session_id` is filled in once the
-- provider returns.
--
-- A row is "unpaid" until the session it records completes, at which point the
-- fulfilment path sets `completed_at` on exactly that row.
CREATE TABLE checkout_attempts (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    payer_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    kind         TEXT NOT NULL CHECK (kind IN ('payment', 'setup', 'auto_topup')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- The payment provider's id for the hosted checkout session. NULL only
    -- between reserving the row and the provider returning.
    session_id   TEXT UNIQUE,
    completed_at TIMESTAMPTZ
);

CREATE INDEX checkout_attempts_unpaid_by_actor ON checkout_attempts (actor_id, created_at) WHERE completed_at IS NULL;
