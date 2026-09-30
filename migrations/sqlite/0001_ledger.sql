-- The spend ledger on SQLite: the Postgres schema in SQLite terms, with the
-- same rules. All money is integer nano-USD (1 USD = 1e9). STRICT tables
-- reject a value of the wrong type instead of silently storing it.
--
-- `day` is days since the Unix epoch by the host clock; `created_at` and
-- `closed_at` are Unix seconds.
CREATE TABLE IF NOT EXISTS spend_days (
    tenant_id       TEXT    NOT NULL,
    day             INTEGER NOT NULL,
    spent_nano_usd  INTEGER NOT NULL DEFAULT 0 CHECK (spent_nano_usd >= 0),
    PRIMARY KEY (tenant_id, day)
) STRICT;

CREATE TABLE IF NOT EXISTS reservations (
    id                 TEXT    PRIMARY KEY,
    tenant_id          TEXT    NOT NULL,
    day                INTEGER NOT NULL,
    reserved_nano_usd  INTEGER NOT NULL CHECK (reserved_nano_usd >= 0),
    prompt_nano_usd    INTEGER NOT NULL CHECK (prompt_nano_usd >= 0),
    state              TEXT    NOT NULL DEFAULT 'open'
        CHECK (state IN ('open', 'settled', 'abandoned', 'released', 'committed', 'swept')),
    delta_nano_usd     INTEGER,
    idempotency_key    TEXT,
    fingerprint        BLOB,
    response           TEXT,
    created_at         INTEGER NOT NULL,
    closed_at          INTEGER
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS reservations_idempotency
    ON reservations (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS reservations_open
    ON reservations (created_at)
    WHERE state = 'open';
