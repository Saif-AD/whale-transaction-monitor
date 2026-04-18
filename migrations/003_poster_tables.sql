-- Phase 1B: Whale alert auto-poster tables
-- posted_tx_hashes: dedup + per-token cooldown tracking
-- poster_state: watermark and runtime state

CREATE TABLE IF NOT EXISTS posted_tx_hashes (
    tx_hash TEXT PRIMARY KEY,
    token_symbol TEXT DEFAULT '',
    channel TEXT NOT NULL DEFAULT 'telegram',
    posted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_posted_tx_posted_at
    ON posted_tx_hashes (posted_at DESC);

CREATE INDEX IF NOT EXISTS idx_posted_tx_token
    ON posted_tx_hashes (token_symbol, posted_at DESC);

CREATE TABLE IF NOT EXISTS poster_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
