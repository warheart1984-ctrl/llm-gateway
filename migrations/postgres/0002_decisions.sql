-- Decisions: why work a known caller asked for did not happen, and what
-- operators changed. No prompts, no answers. See governance::ledger::decisions.
CREATE TABLE IF NOT EXISTS decisions (
    id          BIGSERIAL   PRIMARY KEY,
    request_id  UUID        NOT NULL,
    tenant_id   TEXT        NOT NULL,
    key_id      TEXT        NOT NULL,
    kind        TEXT        NOT NULL CHECK (kind IN ('refused', 'failed', 'admin_action')),
    endpoint    TEXT        NOT NULL,
    model       TEXT,
    code        TEXT        NOT NULL,
    reason      TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS decisions_tenant_time ON decisions (tenant_id, created_at);
CREATE INDEX IF NOT EXISTS decisions_time ON decisions (created_at);
