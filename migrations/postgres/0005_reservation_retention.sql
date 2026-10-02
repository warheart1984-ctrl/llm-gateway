-- Closed reservations are kept for `ledger.reservation_retention_days`, then
-- deleted, and a stored answer is cleared once its idempotency key expires.
-- The sweeper finds both by age; this keeps that from scanning the table.
CREATE INDEX IF NOT EXISTS reservations_closed ON reservations (closed_at) WHERE state <> 'open';
