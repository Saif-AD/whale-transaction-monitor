-- Wallet Tracker Tables Migration
-- Run this in your Supabase SQL editor to create the required tables.

-- 1. wallet_profiles: Cached per-wallet aggregates
CREATE TABLE IF NOT EXISTS wallet_profiles (
    address TEXT PRIMARY KEY,
    chain TEXT DEFAULT '',
    entity_name TEXT DEFAULT '',
    entity_type TEXT DEFAULT '',
    tags TEXT[] DEFAULT '{}',
    smart_money_score FLOAT DEFAULT 0,
    total_volume_usd_30d FLOAT DEFAULT 0,
    total_volume_usd_all FLOAT DEFAULT 0,
    tx_count_30d INTEGER DEFAULT 0,
    tx_count_all INTEGER DEFAULT 0,
    buy_count INTEGER DEFAULT 0,
    sell_count INTEGER DEFAULT 0,
    portfolio_value_usd FLOAT DEFAULT 0,
    pnl_estimated_usd FLOAT DEFAULT 0,
    first_seen TIMESTAMPTZ,
    last_active TIMESTAMPTZ,
    top_tokens JSONB DEFAULT '[]',
    top_counterparties JSONB DEFAULT '[]',
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wallet_profiles_score ON wallet_profiles (smart_money_score DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_profiles_volume ON wallet_profiles (total_volume_usd_30d DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_profiles_chain ON wallet_profiles (chain);
CREATE INDEX IF NOT EXISTS idx_wallet_profiles_entity ON wallet_profiles (entity_name);

-- 2. watchlists
CREATE TABLE IF NOT EXISTS watchlists (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. watchlist_addresses
CREATE TABLE IF NOT EXISTS watchlist_addresses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    watchlist_id UUID NOT NULL REFERENCES watchlists(id) ON DELETE CASCADE,
    address TEXT NOT NULL,
    chain TEXT DEFAULT '',
    custom_label TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    added_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_watchlist_addresses_wl ON watchlist_addresses (watchlist_id);
CREATE INDEX IF NOT EXISTS idx_watchlist_addresses_addr ON watchlist_addresses (address);

-- 4. wallet_alerts
CREATE TABLE IF NOT EXISTS wallet_alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    address TEXT NOT NULL,
    chain TEXT DEFAULT '',
    alert_type TEXT DEFAULT 'any_move' CHECK (alert_type IN ('any_move', 'large_move', 'buy_only', 'sell_only')),
    min_usd_value FLOAT DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    notify_socketio BOOLEAN DEFAULT TRUE,
    notify_telegram BOOLEAN DEFAULT FALSE,
    telegram_chat_id TEXT DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wallet_alerts_address ON wallet_alerts (address);
CREATE INDEX IF NOT EXISTS idx_wallet_alerts_active ON wallet_alerts (is_active) WHERE is_active = TRUE;
