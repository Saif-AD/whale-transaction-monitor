-- Polymarket whale-radar tables.
--
-- Populated by scripts/sync_polymarket.py (Railway cron, see
-- railway.polymarket.toml). Read by the Sonar frontend "Polymarket terminal"
-- (Top Markets / Top Whales / whale drill-down), same pattern as the
-- all_whale_transactions -> frontend flow.
--
-- Apply via the Supabase SQL editor or psql. Safe to re-run (IF NOT EXISTS).

-- ── Markets ────────────────────────────────────────────────────────────
create table if not exists polymarket_markets (
  condition_id          text primary key,
  question              text not null,
  slug                  text,
  category              text,
  outcomes              jsonb default '[]'::jsonb,      -- ["Yes","No"]
  outcome_prices        jsonb default '[]'::jsonb,      -- [0.54, 0.46]
  clob_token_ids        jsonb default '[]'::jsonb,
  volume_24h            double precision default 0,
  liquidity             double precision default 0,
  whale_flow            double precision default 0,      -- Σ top-holder size
  whale_count           integer default 0,
  one_day_price_change  double precision,
  end_date              timestamptz,
  image                 text,
  updated_at            timestamptz not null default now()
);

create index if not exists idx_pm_markets_vol on polymarket_markets (volume_24h desc);
create index if not exists idx_pm_markets_whaleflow on polymarket_markets (whale_flow desc);

-- ── Whales (aggregated across top markets) ─────────────────────────────
create table if not exists polymarket_whales (
  proxy_wallet   text primary key,
  name           text,
  profile_image  text,
  total_amount   double precision default 0,   -- Σ position size across markets
  markets_count  integer default 0,
  positions      jsonb default '[]'::jsonb,     -- optional snapshot of open positions
  updated_at     timestamptz not null default now()
);

create index if not exists idx_pm_whales_total on polymarket_whales (total_amount desc);

-- ── Per-market top holders (which whale is in which market) ─────────────
create table if not exists polymarket_market_holders (
  condition_id   text not null,
  proxy_wallet   text not null,
  name           text,
  amount         double precision default 0,
  outcome_index  integer default 0,
  updated_at     timestamptz not null default now(),
  primary key (condition_id, proxy_wallet, outcome_index)
);

create index if not exists idx_pm_holders_market on polymarket_market_holders (condition_id, amount desc);

-- RLS: these are public, read-only marketing data. Enable RLS and allow
-- anon SELECT; writes happen only via the service-role cron.
alter table polymarket_markets         enable row level security;
alter table polymarket_whales          enable row level security;
alter table polymarket_market_holders  enable row level security;

do $$
begin
  if not exists (select 1 from pg_policies where tablename='polymarket_markets' and policyname='pm_markets_read') then
    create policy pm_markets_read on polymarket_markets for select using (true);
  end if;
  if not exists (select 1 from pg_policies where tablename='polymarket_whales' and policyname='pm_whales_read') then
    create policy pm_whales_read on polymarket_whales for select using (true);
  end if;
  if not exists (select 1 from pg_policies where tablename='polymarket_market_holders' and policyname='pm_holders_read') then
    create policy pm_holders_read on polymarket_market_holders for select using (true);
  end if;
end $$;
