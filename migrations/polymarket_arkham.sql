-- Arkham-powered Polymarket enrichment tables + columns.
--
-- Populated by scripts/sync_arkham_polymarket.py (Railway cron, hourly; see
-- railway.arkham_polymarket.toml). Layers on top of the public-API tables in
-- migrations/polymarket_tables.sql:
--   * adds arkham_entity (real, entity-resolved names) to whales + holders
--   * polymarket_leaderboard — PnL-ranked traders (Arkham /polymarket/leaderboard)
--   * polymarket_activity    — live whale trade tape (Arkham /polymarket/activity)
--
-- Apply via the Supabase SQL editor or psql. Safe to re-run (IF NOT EXISTS).

-- ── Entity-resolved names on existing tables ───────────────────────────
alter table polymarket_whales
  add column if not exists arkham_entity text;

alter table polymarket_market_holders
  add column if not exists arkham_entity text;

-- ── PnL leaderboard (Arkham /polymarket/leaderboard) ───────────────────
create table if not exists polymarket_leaderboard (
  proxy_wallet   text not null,
  period         text not null default '1d',   -- 1d | 1w | 1m | all
  entity_name    text,                          -- Arkham-resolved name (nullable)
  name           text,                          -- pseudonym / display fallback
  profile_image  text,
  pnl            double precision default 0,
  volume         double precision default 0,
  rank           integer,
  updated_at     timestamptz not null default now(),
  primary key (proxy_wallet, period)
);

create index if not exists idx_pm_leaderboard_period_rank
  on polymarket_leaderboard (period, rank asc);
create index if not exists idx_pm_leaderboard_period_pnl
  on polymarket_leaderboard (period, pnl desc);

-- ── Live whale activity tape (Arkham /polymarket/activity) ──────────────
create table if not exists polymarket_activity (
  tx_hash        text,
  condition_id   text,
  proxy_wallet   text not null,
  entity_name    text,                          -- Arkham-resolved name (nullable)
  name           text,
  side           text,                          -- buy | sell
  outcome        text,
  outcome_index  integer,
  usd_value      double precision default 0,
  price          double precision,
  size           double precision,
  ts             timestamptz,
  updated_at     timestamptz not null default now(),
  -- A trade is uniquely identified by wallet + market + timestamp + side.
  -- (tx_hash is not always present on the activity feed.)
  primary key (proxy_wallet, condition_id, ts, side)
);

create index if not exists idx_pm_activity_ts on polymarket_activity (ts desc);
create index if not exists idx_pm_activity_usd on polymarket_activity (usd_value desc);
create index if not exists idx_pm_activity_market on polymarket_activity (condition_id, ts desc);

-- ── RLS: public, read-only marketing data (same policy as the base tables) ──
alter table polymarket_leaderboard enable row level security;
alter table polymarket_activity    enable row level security;

do $$
begin
  if not exists (select 1 from pg_policies where tablename='polymarket_leaderboard' and policyname='pm_leaderboard_read') then
    create policy pm_leaderboard_read on polymarket_leaderboard for select using (true);
  end if;
  if not exists (select 1 from pg_policies where tablename='polymarket_activity' and policyname='pm_activity_read') then
    create policy pm_activity_read on polymarket_activity for select using (true);
  end if;
end $$;

-- ── RPC: distinct categories with counts (replaces a 10k-row client scan) ──
create or replace function polymarket_category_counts()
returns table(category text, n bigint)
language sql
stable
as $$
  select category, count(*)::bigint as n
  from polymarket_markets
  where category is not null and category <> ''
  group by category
  order by n desc;
$$;
