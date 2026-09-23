-- The fulfilment store. One row per processed order; the table is the ground
-- truth for how much work actually completed, independent of what the consumer
-- believes it did.
CREATE TABLE IF NOT EXISTS fulfilment (
  order_id   BIGINT PRIMARY KEY,
  cls        TEXT        NOT NULL,
  run_id     TEXT        NOT NULL,
  queue_ms   INTEGER     NOT NULL,   -- end-to-end queue delay, from the payload's enqueued_at
  written_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS fulfilment_run_idx ON fulfilment (run_id);
