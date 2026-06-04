# Polymarket Integration & Whale-Intelligence Roadmap

Single source of truth for the Polymarket radar + everything we plan to build
on top of the whale-intelligence terminal. Backend lives in this repo
(`whale-transaction-monitor`); the frontend terminal lives in the Sonar repo
and reads Supabase tables this backend populates.

---

## 1. Current state (shipped)

### Polymarket radar — LIVE
| Piece | File | Notes |
|---|---|---|
| Keyless client | `polymarket/client.py` | Public Gamma + Data APIs (markets, holders, positions, value) |
| Sync cron | `scripts/sync_polymarket.py` | Dry-run default; `--live` upserts to Supabase |
| Tables | `migrations/polymarket_tables.sql` | `polymarket_markets`, `polymarket_market_holders`, `polymarket_whales` (applied) |
| Railway service | `railway.polymarket.toml` | 10-min cron, no API key needed |
| Frontend | Sonar `/polymarket` | Top Markets, Whale Leaderboard, drill-down drawer |

Data is **seeded** (20 markets, 4 with full holder data). Deploy the Railway
cron to keep all 20 fresh.

### Data source — what we pull and why
- **Polymarket Gamma API** (`gamma-api.polymarket.com`) — markets, odds, volume. Official public read API (same one their site uses).
- **Polymarket Data API** (`data-api.polymarket.com`) — `/holders` (whales), `/positions`, `/value`. Snapshot, not historical.
- Keyless. The only **keyed** Polymarket API is the **CLOB** (trading auth) — not needed for a read-only radar.
- We cache into Supabase, so the frontend never hits Polymarket directly (insulated from rate limits / Cloudflare).

### Entity & figure intelligence — LIVE
- 186 curated entities, 155 with verified+cited addresses (`curated_entities.addresses`).
- Confidence tiers: `verified=true` (green) vs `verified=false` (amber COMMUNITY).
- Deep-research batches loaded via `scripts/import_curated_entities.py` ← `arkham_backfill/seeds_curated_entities.py`.

---

## 2. The two API keys that unlock "real" data

Both pipelines are **already built**; they just need a key + a run. These are
the single biggest quality unlocks — bigger than adding more addresses.

### ARKHAM_API_KEY — address discovery + labels at scale
Auto-fills the 31 empty entities and enriches existing ones.
```bash
export ARKHAM_API_KEY=ak_...
python -m arkham_backfill.verify_slugs --dry-run      # prune 404 slugs
python arkham_backfill/backfill.py --live             # Arkham -> addresses table
python scripts/enrich_curated_entities_from_labels.py --live   # addresses -> curated pages
```
Seed list already expanded to 86 entities (`arkham_backfill/entities.py`).

### ZERION_API_KEY — real portfolios (holdings, value, PnL)
Turns figure pages from "net on-chain flow estimate" into actual holdings.
This is the most *visible* upgrade.
```bash
export ZERION_API_KEY=zk_...
python scripts/profile_curated_addresses.py --live    # -> wallet_profiles
```
Without it, figure pages correctly show "net on-chain flow," not holdings.
Note: curated wallets have no stored tx history, so `smart_money_score`/`tags`
stay weak until Zerion provides value — Zerion is the real dependency here.

---

## 3. Polymarket future work (depth + differentiation)

This is our unique wedge — Arkham does NOT do Polymarket well. Lean in.

| # | Feature | What | Data source | Key? |
|---|---|---|---|---|
| P1 | **Whale labeling** | Cross-reference PM `proxy_wallet`s against our 166K label store + `curated_entities`; add `known_entity` column to `polymarket_whales`. De-anonymizes whales. | existing labels | none |
| P2 | **PnL / position history** | Per-whale realized PnL + equity curve over time. Public Data API is snapshot-only. | **Polymarket subgraph** (Goldsky/TheGraph) | none |
| P3 | **Whale drill-down via Arkham** | Link PM proxy wallet → real identity + funding source (which CEX funded them, cross-chain). The "who is this whale really" move. | Arkham | ARKHAM |
| P4 | **New-big-bet alerts** | Notify when a tracked whale opens a large position or a market sees a whale-flow spike. | our tables + poster | none |
| P5 | **Follow-a-whale** | Watchlist a PM whale; surface their new bets in the Following tab. | watchlists | none |
| P6 | **Market resolution history** | Track resolved markets + whale win-rates for a credibility leaderboard. | subgraph | none |

---

## 4. Whale-intelligence terminal future work

| # | Feature | What | Data source | Key? |
|---|---|---|---|---|
| W1 | **Entity → entity flow graph** | Arkham's signature view: "Binance → Wintermute $40M this week." We already store `from_label`/`to_label` on every whale tx. Add `/api/flows` + a graph/sankey. | `all_whale_transactions` | none |
| W2 | **Global search** | One search bar across entities/addresses/markets. Today search is client-side per-directory only. Add a server `/api/search` over `curated_entities` (all categories) + labels. | curated + labels | none |
| W3 | **Verified-entity credibility chip** | `✓ N verified · M chains · sourced` from already-loaded addresses (no new query). Amber `COMMUNITY` when only OSINT. | `curated_entities.addresses` | none |
| W4 | **Smart-money / tags badge** | Max score + union of tags across a figure's addresses. Dormant until `wallet_profiles` populated. | `wallet_profiles` | ZERION |
| W5 | **AI entity summaries** | "What did this entity do this week" via the Orca copilot pointed at the entity tx feed. | Orca + txs | (LLM) |
| W6 | **PnL / smart-money leaderboards** | Rank wallets by realized PnL / win-rate, on-chain + Polymarket combined. | wallet_profiles + PM | mixed |

---

## 5. Data quality & hygiene

| # | Task | Why |
|---|---|---|
| H1 | Merge **alias entities** (`uniswap`/`uniswap-treasury`, `lido`/`lido-treasury`, `coinbase`/`coinbase-custody`, etc.) | Directory currently double-counts |
| H2 | Fix **beeple contamination** (balaji's wallet leaked onto beeple via a bad source label) | 1 row, credibility |
| H3 | **Freshness cron** for addresses/labels | Prevent stale data; reuse `railway.cron.toml` pattern |
| H4 | Deep-research remaining individuals (`francis-suarez` has no verifiable wallet; revisit `gainzy` Solana OSINT) | Coverage |

---

## 6. Priority recommendation

1. **Deploy the Polymarket cron** (`railway.polymarket.toml`) — 5 min, makes the radar self-sustaining.
2. **Zerion key** → real portfolios (most visible upgrade).
3. **Entity → entity flow graph (W1)** — highest-impact feature that needs *no* key (data already exists).
4. **PM whale labeling (P1)** — free de-anonymization using existing labels.
5. **Arkham key** → breadth (fills empty entities) + PM whale identity (P3).

Everything above #2 is gated only on the two API keys; the rest is buildable now.
