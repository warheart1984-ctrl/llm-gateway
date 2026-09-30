-- Leases and shared rate limits, in SQLite terms. See the Postgres migration
-- of the same number. Reservations open before this migration keep the old
-- behaviour: one hour from creation. Times are Unix seconds.
ALTER TABLE reservations ADD COLUMN lease_expires_at INTEGER NOT NULL DEFAULT 0;
UPDATE reservations SET lease_expires_at = created_at + 3600 WHERE lease_expires_at = 0;

CREATE INDEX IF NOT EXISTS reservations_live ON reservations (tenant_id, lease_expires_at) WHERE state = 'open';
CREATE INDEX IF NOT EXISTS reservations_lease ON reservations (lease_expires_at) WHERE state = 'open';

CREATE TABLE IF NOT EXISTS rate_minutes (
    tenant_id  TEXT    NOT NULL,
    minute     INTEGER NOT NULL,
    requests   INTEGER NOT NULL DEFAULT 0 CHECK (requests >= 0),
    tokens     INTEGER NOT NULL DEFAULT 0 CHECK (tokens >= 0),
    PRIMARY KEY (tenant_id, minute)
) STRICT;
