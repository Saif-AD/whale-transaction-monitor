-- Phase 3: CoinGecko response caches
--
-- Two independent caches with short TTLs, both keyed for fast lookup:
--   * coingecko_ohlc_cache — 24h OHLC arrays for chart rendering, 1h TTL
--   * coingecko_token_price_cache — per-token USD prices for classification, 5m TTL
--
-- These are deliberately SEPARATE from price_snapshots, which is a
-- time-series history table (append-only, PK=id, unique=ticker+timestamp)
-- written by a different job. Do not merge them.

CREATE TABLE IF NOT EXISTS coingecko_ohlc_cache (
    coingecko_id TEXT PRIMARY KEY,
    ohlc_data JSONB NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_coingecko_ohlc_cache_fetched_at
    ON coingecko_ohlc_cache (fetched_at DESC);


CREATE TABLE IF NOT EXISTS coingecko_token_price_cache (
    token_address TEXT NOT NULL,
    chain TEXT NOT NULL,
    price_usd NUMERIC NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (token_address, chain)
);

CREATE INDEX IF NOT EXISTS idx_coingecko_token_price_cache_fetched_at
    ON coingecko_token_price_cache (fetched_at DESC);
