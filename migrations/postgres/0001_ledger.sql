-- The spend ledger. All money is integer nano-USD (1 USD = 1e9).
--
-- `spend_days` is the budget: one row per tenant per UTC day, where `day` is
-- days since the Unix epoch as computed by the database clock, so replicas
-- with skewed clocks cannot disagree about which day a request belongs to.
CREATE TABLE IF NOT EXISTS spend_days (
    tenant_id       TEXT    NOT NULL,
    day             BIGINT  NOT NULL,
    spent_nano_usd  BIGINT  NOT NULL DEFAULT 0 CHECK (spent_nano_usd >= 0),
    PRIMARY KEY (tenant_id, day)
);

-- One row per admitted request: what was authorised, how it closed, what it
-- cost. A row is created in the same transaction that charges the budget,
-- before the provider is contacted, and leaves `open` exactly once.
CREATE TABLE IF NOT EXISTS reservations (
    id                 UUID        PRIMARY KEY,
    tenant_id          TEXT        NOT NULL,
    day                BIGINT      NOT NULL,
    reserved_nano_usd  BIGINT      NOT NULL CHECK (reserved_nano_usd >= 0),
    prompt_nano_usd    BIGINT      NOT NULL CHECK (prompt_nano_usd >= 0),
    state              TEXT        NOT NULL DEFAULT 'open'
        CHECK (state IN ('open', 'settled', 'abandoned', 'released', 'committed', 'swept')),
    -- Actual minus reserved, set when the row closes. Billed = reserved + delta.
    delta_nano_usd     BIGINT,
    -- Client-supplied, unique per tenant while held. Cleared when the key is
    -- reused after a released attempt or after the retention window.
    idempotency_key    TEXT,
    fingerprint        BYTEA,
    -- The answer, for replay under the same key (completions only).
    response           TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at          TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS reservations_idempotency
    ON reservations (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- The sweeper's scan: reservations still open after a crash.
CREATE INDEX IF NOT EXISTS reservations_open
    ON reservations (created_at)
    WHERE state = 'open';
