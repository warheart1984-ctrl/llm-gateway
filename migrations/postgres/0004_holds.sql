-- HOLD: requests that wait for a person to approve them. See
-- governance::ledger::holds.
--
-- A hold records the request's identity, never its content: the keyed
-- fingerprint of the canonical request, the model, the output cap and the
-- worst-case cost. The client keeps the request and sends it again, with
-- `Hold-Id`, once the hold is approved; the fingerprint is how the gateway
-- knows it is the same request.
CREATE TABLE IF NOT EXISTS holds (
    id                   UUID        PRIMARY KEY,
    tenant_id            TEXT        NOT NULL,
    -- The key that made the request.
    requested_by         TEXT        NOT NULL,
    endpoint             TEXT        NOT NULL,
    model                TEXT        NOT NULL,
    max_output_tokens    BIGINT      NOT NULL CHECK (max_output_tokens >= 0),
    exposure_nano_usd    BIGINT      NOT NULL CHECK (exposure_nano_usd >= 0),
    -- Why it was held: which rule matched.
    reason               TEXT        NOT NULL,
    fingerprint          BYTEA       NOT NULL,
    state                TEXT        NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'approved', 'denied', 'expired', 'consumed')),
    -- How long an approval stays usable, fixed when the hold is made.
    approval_valid_secs  BIGINT      NOT NULL CHECK (approval_valid_secs > 0),
    -- `tenant/key` of whoever approved or denied it.
    decided_by           TEXT,
    decided_at           TIMESTAMPTZ,
    note                 TEXT,
    -- The reservation that used the approval.
    reservation_id       UUID,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Pending: when it stops waiting. Approved: when the approval lapses.
    expires_at           TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS holds_tenant_state ON holds (tenant_id, state, expires_at);
CREATE INDEX IF NOT EXISTS holds_live ON holds (expires_at) WHERE state IN ('pending', 'approved');
CREATE INDEX IF NOT EXISTS holds_reservation ON holds (reservation_id) WHERE reservation_id IS NOT NULL;

-- Hold transitions go on the decision record too.
ALTER TABLE decisions DROP CONSTRAINT IF EXISTS decisions_kind_check;
ALTER TABLE decisions ADD CONSTRAINT decisions_kind_check
    CHECK (kind IN ('refused', 'failed', 'admin_action', 'hold'));
