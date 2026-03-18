# BigQuery & Whale Wallet Tracking — Implementation Report

**Sonar Tracker | March 2026**
**Prepared by: Saif Al Dhaheri**

---

## Executive Summary

This report covers the full evolution of BigQuery integration in the Sonar Tracker whale monitoring system, the current state of whale wallet tracking, and a UI implementation roadmap for sonartracker.io. The BigQuery layer has been overhauled from a bulk data dumper into a quality-gated, cost-managed intelligence pipeline covering 5 blockchains with 342,000+ enriched addresses.

---

## 1. What Was Done — Recent Development Timeline

### Week of March 12–15 (Saif)

| Commit | Description |
|--------|-------------|
| Solana Yellowstone gRPC Streaming | Implemented real-time SPL token monitoring via Alchemy's Yellowstone gRPC, replacing polling-based Solana monitoring |
| Clean Solana Address Lists | Removed 35 invalid entries, added verified CEX/DEX addresses for Solana chain |
| Fix Polygon & XRP Classification | Expanded CEX address databases, enabled XRP OfferCreate transaction type |
| Expand Exchange Address Databases | Added +74 verified exchange addresses across all 5 chains (Ethereum, Polygon, Solana, Bitcoin, XRP) |
| Fix API for All Chains | Updated API to query the `all_whale_transactions` view across all supported chains |
| Expand Token Monitoring | Added +10 ERC-20 tokens and +3 Solana SPL tokens to the monitoring pipeline |

### Week of March 16 (BigQuery Overhaul — Claude Sessions)

| Commit | Description |
|--------|-------------|
| Cross-Chain Verified Whale Discovery | New BigQuery pipeline that finds whales active on 3+ chains (shared addresses, bridge users, multi-chain stablecoin whales) |
| Address Verification Gate | Built `address_verification.py` — every BigQuery candidate must pass Etherscan/Moralis verification ($500K+ balance OR known contract label) before entering Supabase |
| Remove Flooding Scripts | Deleted old standalone BigQuery discovery scripts that were inserting hundreds of thousands of empty/unverified addresses into Supabase |
| Fix Live BigQuery Analyzer | Added 6-hour caching (TTL), daily query budget (10/day), dust transaction filtering to stay within free tier |
| 5-Chain Discovery Pipeline | New unified discovery script covering Ethereum, Polygon, Bitcoin, Solana, and XRP with chain-specific thresholds |
| Threshold Tuning | Lowered discovery thresholds to catch more mid-tier whales while maintaining quality gates |

### Eduardo (March 12)

| Commit | Description |
|--------|-------------|
| Major Pipeline Overhaul | WebSocket architecture, per-chain Supabase tables, new classification engines |

---

## 2. BigQuery System Architecture

### Full History

**May 2025 — Initial Setup**
First BigQuery integration built alongside API integrations (Etherscan, Moralis, GitHub scraping). Created `bigquery_public_data_extractor.py` for pulling whale addresses from Google's public blockchain datasets.

**June 2025 — Whale Discovery V1**
Automated `whale_discovery_agent.py` (1,700+ lines) that discovers whales via BigQuery + APIs. Added DeFiLlama entity exports (28K+ DeFi entities, 1.8K CEX entities). Created `run_daily_discovery.sh` for scheduled cron-based discovery. This is where the flooding problem originated — bulk inserts with no quality gate.

**July 2025 — 3-Tier Smart BigQuery System**
Created the live pipeline's Phase 8 fallback (`bigquery_analyzer.py`) that only fires when classification confidence is below 0.70. Built the 8-phase classification pipeline (3,844 lines). BigQuery became a last-resort in the live pipeline, but standalone discovery scripts continued flooding.

**March 2026 — Quality Overhaul (Current)**
Complete overhaul: verification gates, caching, budget limits, removal of flooding scripts, and replacement with a quality-gated 5-chain discovery pipeline.

### Current Architecture

```
┌──────────────────────────────────────────────────────┐
│                  BigQuery Public Datasets              │
│  crypto_ethereum · crypto_polygon · crypto_bitcoin     │
│  crypto_solana · crypto_xrp                            │
└──────────────────┬───────────────────────────────────┘
                   │
        ┌──────────▼──────────┐
        │  Discovery Pipeline  │
        │  (Batch — Periodic)  │
        │                      │
        │  • Volume whales     │
        │  • Smart money       │
        │  • Cross-chain       │
        │  • Bridge users      │
        └──────────┬──────────┘
                   │
        ┌──────────▼──────────┐
        │  Verification Gate   │
        │                      │
        │  Etherscan labels    │
        │  Moralis net worth   │
        │  $500K+ OR known     │
        │  contract label      │
        └──────────┬──────────┘
                   │
        ┌──────────▼──────────┐
        │  Supabase addresses  │
        │  342K+ addresses     │
        │  70K+ named entities │
        └──────────┬──────────┘
                   │
     ┌─────────────┼─────────────┐
     │             │             │
┌────▼────┐  ┌────▼────┐  ┌────▼────┐
│  Live   │  │  Whale  │  │   API   │
│ Monitor │  │ Intel   │  │ Endpoints│
│ Phase 8 │  │ Dashboard│ │         │
│ Fallback│  │         │  │         │
└─────────┘  └─────────┘  └─────────┘
```

### Three BigQuery Components

| Component | File | Purpose | Status |
|-----------|------|---------|--------|
| Live Analyzer | `utils/bigquery_analyzer.py` | On-demand classification fallback for unknown addresses during live monitoring. Only fires when confidence < 0.70. | Working — 10 queries/day budget, 6hr cache |
| Public Data Extractor | `utils/bigquery_public_data_extractor.py` | Batch extraction from 7+ chain datasets with multi-criteria confidence scoring | Working — 1,673 lines |
| Discovery Pipeline | `bigquery_discover_whales.py` | End-to-end discovery: BigQuery → verification → Supabase upsert | Working — 5 chains, quality-gated |

### Chain-Specific Discovery Thresholds

| Chain | Min Per TX | Min Total Volume | Verification |
|-------|-----------|-----------------|--------------|
| Ethereum | 0.5 ETH (~$1,500) | 10 ETH (~$30,000) | Etherscan label + Moralis balance |
| Polygon | 1,000 MATIC (~$800) | 25,000 MATIC (~$20,000) | PolygonScan + Moralis |
| Bitcoin | 0.1 BTC (~$9,500) | 1 BTC (~$95,000) | High threshold = self-verifying |
| Solana | 10 SOL (~$1,500) | 100 SOL (~$15,000) | Helius + Solscan |
| XRP | 5,000 XRP (~$12,500) | 50,000 XRP (~$125,000) | High threshold = self-verifying |

---

## 3. Database State (Supabase)

### Addresses Table

| Metric | Value |
|--------|-------|
| Total addresses | 342,000+ |
| Named entities (entity_name set) | 70,000+ |
| Named CEX addresses | 11,682 |
| Named DEX/protocol addresses | 10,951 |
| Named whale addresses | 47,930 |
| Manually verified famous wallets | 28+ |

**Famous wallets include:** Vitalik Buterin, Justin Sun, Donald Trump, Paradigm, Wintermute, Jump Trading, and others.

### Entity Categories Available

| Category | Examples |
|----------|---------|
| exchange | Binance, Coinbase, Kraken, Gemini, KuCoin |
| individual | Vitalik Buterin, Justin Sun, Trump |
| institution | Paradigm, Jump Trading, Wintermute |
| protocol | Uniswap, Aave, MakerDAO, Curve |
| stablecoin | Tether Treasury, Circle USDC |
| infrastructure | Polygon Bridge, Arbitrum Bridge |
| mev | MEV bots |
| trader | Active trading wallets |

### Whale Transactions Table

| Metric | Value |
|--------|-------|
| Total transactions | 233,090+ |
| BUY transactions | ~48,600 |
| SELL transactions | ~48,400 |
| TRANSFER transactions | ~136,000 (being reclassified) |
| CEX transactions | 90,336 |
| DEX transactions | 2,849 |
| EOA transactions | 136,137 |

---

## 4. SonarTracker.io — Current UI Pages

| Page | Current Functionality |
|------|---------------------|
| **Dashboard** | Live whale transactions feed, multi-chain filters, real-time WebSocket updates |
| **Leaderboard** | Top tokens by buying/selling trends, net flow sorting, 24h activity |
| **Heatmap** | Interactive token activity visualization, real-time flow analysis across chains |
| **ORCA 2.0** | AI advisor for whale-informed trading recommendations |
| **News** | AI-curated crypto news feed categorized by market impact |

---

## 5. UI Implementation Roadmap — Where to Add BigQuery Features

### Priority 1: Dashboard — Entity Name Resolution

**Where:** Main transaction feed on Dashboard page

**Current state:** Transactions show raw addresses like `0xd8da6bf2...`

**Proposed:** Show named entities inline:
> "**Vitalik Buterin** SOLD 500 ETH to **Binance** ($1.6M)"

**Backend ready:** `from_label`, `to_label`, `whale_address`, `counterparty_type` columns already populated in `whale_transactions` table.

**Effort:** Low — frontend-only change, data already available via API.

---

### Priority 2: Leaderboard — Add "Top Whales" Tab

**Where:** New tab alongside existing token leaderboard

**What it shows:**
- Top 20 named whales ranked by 24h volume
- Buy/sell ratio per whale
- Cross-chain activity badge (active on 3+ chains)
- Entity type icon (individual, institution, exchange)

**New API endpoint needed:** `GET /api/leaderboard`

**Data source:** `whale_transactions` joined with `addresses` table (70K+ named entities)

**Effort:** Medium — needs new API endpoint + frontend tab.

---

### Priority 3: Dashboard — Entity Category Filter

**Where:** New dropdown filter on Dashboard, alongside existing blockchain/token filters

**Filter options:** Exchanges, Individuals, Institutions, Protocols, Stablecoins, MEV Bots

**Backend ready:** `analysis_tags.category` field populated on 70K+ addresses.

**Effort:** Low-Medium — new filter dropdown + API parameter.

---

### Priority 4: Whale Profile Pages

**Where:** New page at `/whale/{name}`

**What it shows:**
- Entity name, type, and category
- All known addresses across chains
- Transaction history timeline (buy/sell chart)
- 24h/7d/30d volume
- Most traded tokens
- Counterparty breakdown (which exchanges they use)

**New API endpoints needed:**
- `GET /api/whale/{name}` — transactions for named entity
- `GET /api/whale/{address}/profile` — full whale profile
- `GET /api/entities` — searchable list of all named entities

**Effort:** High — new page, multiple API endpoints, charting.

---

### Priority 5: Heatmap — Cross-Chain Whale Flow Overlay

**Where:** Overlay on existing Heatmap page

**What it shows:**
- Whales moving funds between chains (bridge detection from BigQuery cross-chain discovery)
- Stablecoin concentration flows across chains
- Visual connections between chains when same whale is active on multiple

**Data source:** `bigquery_crosschain_verified_whales.py` output (shared-address whales, bridge users, multi-chain stablecoin whales)

**Effort:** High — requires heatmap integration + new data layer.

---

### Priority 6: ORCA 2.0 — BigQuery-Verified Intelligence Feed

**Where:** ORCA AI Advisor backend

**Enhancement:** Feed ORCA verified whale intelligence instead of raw transaction data:
- Verified whale scores from BigQuery (confidence-weighted)
- Cross-chain flow patterns (whales moving between chains = strong signal)
- Entity-level aggregation (institutions buying vs individuals selling)
- Historical whale behavior patterns

**Effort:** Medium — backend data pipeline enhancement.

---

## 6. Known Issues & Next Steps

### Issues to Resolve

| Issue | Impact | Priority |
|-------|--------|----------|
| BigQuery credentials expired (`sonar-286@sonarium-480012`) | Phase 8 live fallback skipped for unknown addresses | Medium |
| DeFi classification BUY bias | All DeFi transactions default to BUY instead of analyzing token flow | High |
| 133K+ TRANSFER reclassification in progress | Leaderboard/stats accuracy affected until complete | Medium |
| Solana BigQuery dataset stopped updating (March 2025) | Solana discovery relies on Helius verification only | Low |

### Recommended Next Steps

1. **Renew BigQuery service account credentials** on project `peak-seat-465413-u9`
2. **Implement DeFi classification fix** per `CLASSIFICATION_FIX_PROMPT.md` spec
3. **Build the Leaderboard "Top Whales" tab** — highest user-facing impact
4. **Add entity name resolution to Dashboard** — quick win, data already available
5. **Schedule discovery pipeline** as daily cron job for ongoing whale discovery

---

*Document generated March 18, 2026*
*Sonar Tracker — Whale Transaction Monitoring System*
