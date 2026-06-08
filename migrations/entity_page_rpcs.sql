-- Entity-page aggregation RPCs.
--
-- The Sonar entity detail page (/entity/[name]) previously pulled up to
-- 5000 rows per query and aggregated in Node (no GROUP BY in PostgREST). These
-- functions push the aggregation into Postgres so the page fetches a handful
-- of summarized rows instead. Read by app/entity/[name]/page.js via .rpc().
--
-- Matching mirrors the old in-memory logic: rows where from_label = name OR
-- to_label = name. Ensure indexes exist on all_whale_transactions(from_label)
-- and (to_label) for these to be fast.
--
-- Apply via the Supabase SQL editor or psql. Safe to re-run (CREATE OR REPLACE).

-- Headline stats for an entity.
create or replace function entity_stats(p_name text)
returns table (
  tx_count       bigint,
  total_volume   double precision,
  chain_count    bigint,
  address_count  bigint,
  first_seen     timestamptz,
  last_active    timestamptz
)
language sql
stable
as $$
  select
    count(*)::bigint,
    coalesce(sum(usd_value), 0)::double precision,
    count(distinct lower(blockchain))::bigint,
    (count(distinct from_address) + count(distinct to_address))::bigint,
    min(timestamp),
    max(timestamp)
  from all_whale_transactions
  where from_label = p_name or to_label = p_name;
$$;

-- Top tokens by volume for an entity.
create or replace function entity_top_tokens(p_name text, p_limit int default 10)
returns table (
  token_symbol text,
  tx_count     bigint,
  volume       double precision
)
language sql
stable
as $$
  select
    coalesce(token_symbol, 'UNKNOWN') as token_symbol,
    count(*)::bigint as tx_count,
    coalesce(sum(usd_value), 0)::double precision as volume
  from all_whale_transactions
  where from_label = p_name or to_label = p_name
  group by coalesce(token_symbol, 'UNKNOWN')
  order by volume desc
  limit greatest(1, p_limit);
$$;

-- Addresses most associated with an entity (from-side when from_label matches,
-- to-side when to_label matches), ranked by transaction count.
create or replace function entity_associated_addresses(p_name text, p_limit int default 20)
returns table (
  address  text,
  tx_count bigint,
  volume   double precision
)
language sql
stable
as $$
  with combined as (
    select from_address as address, usd_value
    from all_whale_transactions
    where from_label = p_name
    union all
    select to_address as address, usd_value
    from all_whale_transactions
    where to_label = p_name
  )
  select
    address,
    count(*)::bigint as tx_count,
    coalesce(sum(usd_value), 0)::double precision as volume
  from combined
  where address is not null
  group by address
  order by tx_count desc
  limit greatest(1, p_limit);
$$;
