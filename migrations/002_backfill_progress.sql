-- Progress tables for resumable offline backfills (labels first, then reasoning).
-- Apply in Supabase SQL editor before running scripts/backfill_labels.py --live.

CREATE TABLE IF NOT EXISTS label_backfill_progress (
    tx_hash TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_label_backfill_progress_at
    ON label_backfill_progress (processed_at DESC);

CREATE TABLE IF NOT EXISTS reasoning_backfill_progress (
    tx_hash TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reasoning_backfill_progress_at
    ON reasoning_backfill_progress (processed_at DESC);
