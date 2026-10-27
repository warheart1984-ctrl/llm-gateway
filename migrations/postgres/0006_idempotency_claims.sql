-- A key claimed before a conversation fold, and before the budget reservation
-- that adopts it. Not a charge: the reservation below still takes the key in
-- the same transaction as the money. Released when that reservation never
-- happens, and dropped by the sweeper once the lease lapses.
CREATE TABLE IF NOT EXISTS idempotency_claims (
    tenant_id        TEXT        NOT NULL,
    idempotency_key  TEXT        NOT NULL,
    id               UUID        NOT NULL,
    fingerprint      BYTEA       NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idempotency_claims_id ON idempotency_claims (id);
CREATE INDEX IF NOT EXISTS idempotency_claims_lease ON idempotency_claims (lease_expires_at);
