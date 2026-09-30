-- Leases and shared rate limits. See governance::ledger.
--
-- A reservation is live while its lease is: the process serving it renews
-- the lease periodically, and the sweeper takes only reservations whose lease
-- has lapsed, so a long stream is never swept while it runs. Reservations
-- open before this migration keep the old behaviour: one hour from creation.
ALTER TABLE reservations ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;
UPDATE reservations SET lease_expires_at = created_at + interval '1 hour' WHERE lease_expires_at IS NULL;
ALTER TABLE reservations ALTER COLUMN lease_expires_at SET NOT NULL;

-- The sweeper's scan, and the concurrency count: live reservations per tenant.
CREATE INDEX IF NOT EXISTS reservations_live ON reservations (tenant_id, lease_expires_at) WHERE state = 'open';
CREATE INDEX IF NOT EXISTS reservations_lease ON reservations (lease_expires_at) WHERE state = 'open';

-- Requests and tokens per tenant per aligned UTC minute (`minute` = minutes
-- since the Unix epoch, by the database clock). A fixed window: exact and
-- shared across replicas, at the cost of a burst at the boundary.
CREATE TABLE IF NOT EXISTS rate_minutes (
    tenant_id  TEXT   NOT NULL,
    minute     BIGINT NOT NULL,
    requests   BIGINT NOT NULL DEFAULT 0 CHECK (requests >= 0),
    tokens     BIGINT NOT NULL DEFAULT 0 CHECK (tokens >= 0),
    PRIMARY KEY (tenant_id, minute)
);
