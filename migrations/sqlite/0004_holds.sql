-- HOLD, in SQLite terms. See the Postgres migration of the same number.
-- Times are Unix seconds.
CREATE TABLE IF NOT EXISTS holds (
    id                   TEXT    PRIMARY KEY,
    tenant_id            TEXT    NOT NULL,
    requested_by         TEXT    NOT NULL,
    endpoint             TEXT    NOT NULL,
    model                TEXT    NOT NULL,
    max_output_tokens    INTEGER NOT NULL CHECK (max_output_tokens >= 0),
    exposure_nano_usd    INTEGER NOT NULL CHECK (exposure_nano_usd >= 0),
    reason               TEXT    NOT NULL,
    fingerprint          BLOB    NOT NULL,
    state                TEXT    NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'approved', 'denied', 'expired', 'consumed')),
    approval_valid_secs  INTEGER NOT NULL CHECK (approval_valid_secs > 0),
    decided_by           TEXT,
    decided_at           INTEGER,
    note                 TEXT,
    reservation_id       TEXT,
    created_at           INTEGER NOT NULL,
    expires_at           INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS holds_tenant_state ON holds (tenant_id, state, expires_at);
CREATE INDEX IF NOT EXISTS holds_live ON holds (expires_at) WHERE state IN ('pending', 'approved');
CREATE INDEX IF NOT EXISTS holds_reservation ON holds (reservation_id) WHERE reservation_id IS NOT NULL;

-- SQLite cannot alter a CHECK constraint, so the decisions table is rebuilt
-- with `hold` among its kinds. Every existing row is kept.
CREATE TABLE decisions_v2 (
    id          INTEGER PRIMARY KEY,
    request_id  TEXT    NOT NULL,
    tenant_id   TEXT    NOT NULL,
    key_id      TEXT    NOT NULL,
    kind        TEXT    NOT NULL CHECK (kind IN ('refused', 'failed', 'admin_action', 'hold')),
    endpoint    TEXT    NOT NULL,
    model       TEXT,
    code        TEXT    NOT NULL,
    reason      TEXT    NOT NULL,
    created_at  INTEGER NOT NULL
) STRICT;
INSERT INTO decisions_v2 (id, request_id, tenant_id, key_id, kind, endpoint, model, code, reason, created_at)
    SELECT id, request_id, tenant_id, key_id, kind, endpoint, model, code, reason, created_at FROM decisions;
DROP TABLE decisions;
ALTER TABLE decisions_v2 RENAME TO decisions;
CREATE INDEX IF NOT EXISTS decisions_tenant_time ON decisions (tenant_id, created_at);
CREATE INDEX IF NOT EXISTS decisions_time ON decisions (created_at);
