# UI Agent Handoff — Backend Updates & New Capabilities

This document describes all backend changes made to the whale transaction monitoring system. The UI/dashboard should be updated to take advantage of these new capabilities.

---

## 1. Database Changes (Supabase)

### `addresses` table — Now enriched with 70k+ named entities

The `addresses` table has been enriched with named entities from public datasets. Key fields:

| Column | Description | Example |
|--------|-------------|---------|
| `address` | Wallet address (lowercase) | `0xd8da6bf26964af9d7eed9e03e53415d37aa96045` |
| `blockchain` | Chain name (normalized) | `ethereum`, `polygon`, `bsc`, `bitcoin`, `solana` |
| `address_type` | Classification | `CEX`, `DEX`, `WHALE` |
| `entity_name` | **Human-readable name** | `Vitalik Buterin`, `Binance`, `Wintermute` |
| `label` | Descriptive label | `Ethereum Co-Founder`, `Binance 8 (Cold)` |
| `confidence` | Label confidence (0-1) | `0.95` |
| `signal_potential` | Signal importance | `HIGH`, `MEDIUM`, `LOW` |
| `source` | Where the label came from | `eth_labels_api`, `etherscan_labels_github`, `manual_curation` |
| `analysis_tags` | JSON metadata | `{"category": "individual", "subcategory": "crypto_founder", "is_famous": true}` |

**Stats after enrichment:**
- ~342k total addresses
- ~70k+ now have `entity_name` set (was ~34k before)
- 11,682 named CEX addresses (Binance, Coinbase, Kraken, etc.)
- 10,951 named DEX/protocol addresses (Uniswap, Aave, Curve, etc.)
- 47,930 named whale addresses
- 28+ manually verified famous wallets (Vitalik, Justin Sun, Trump, Paradigm, Wintermute, etc.)

### `whale_transactions` table — Now has whale perspective columns

Each transaction now tracks who the whale is and who the counterparty is:

| Column | Description | Example |
|--------|-------------|---------|
| `transaction_hash` | TX hash | `0xabc...` |
| `classification` | `BUY`, `SELL`, `TRANSFER`, `DEFI` | `BUY` |
| `confidence` | Classification confidence (0-1) | `0.92` |
| `token_symbol` | Token traded | `ETH`, `USDC` |
| `usd_value` | USD value | `1,500,000` |
| `whale_score` | Whale importance score | `0.85` |
| `blockchain` | Chain | `ethereum` |
| `from_address` | Sender | `0x28c6...` (Binance) |
| `to_address` | Receiver | `0xd8da...` (Vitalik) |
| `whale_address` | **The identified whale** | `0xd8da...` |
| `counterparty_address` | The other party | `0x28c6...` |
| `counterparty_type` | `CEX`, `DEX`, `EOA` | `CEX` |
| `is_cex_transaction` | Was a CEX involved? | `true` |
| `from_label` | Sender label | `Binance Hot Wallet` |
| `to_label` | Receiver label | `Vitalik Buterin` |
| `reasoning` | AI classification reasoning | `High-confidence CEX withdrawal...` |
| `timestamp` | When it happened | `2026-02-20T10:33:40Z` |

**Stats:**
- 233,090+ total transactions
- ~48,600 BUY, ~48,400 SELL, ~136k TRANSFER (TRANSFER count reducing via reclassification)
- 90,336 CEX transactions, 2,849 DEX, 136,137 EOA

---

## 2. New UI Features to Build

### Feature A: Named Whale Tracking

**Query to get a whale's activity by name:**
```sql
SELECT wt.*, a.entity_name, a.label, a.analysis_tags
FROM whale_transactions wt
JOIN addresses a ON wt.whale_address = a.address AND wt.blockchain = a.blockchain
WHERE a.entity_name = 'Vitalik Buterin'
ORDER BY wt.timestamp DESC
LIMIT 50;
```

**Supabase client equivalent:**
```python
# Get all transactions for a specific whale by name
whale_addr_result = supabase.table('addresses') \
    .select('address') \
    .eq('entity_name', 'Vitalik Buterin') \
    .eq('blockchain', 'ethereum') \
    .execute()

addresses = [r['address'] for r in whale_addr_result.data]

txs = supabase.table('whale_transactions') \
    .select('*') \
    .in_('whale_address', addresses) \
    .order('timestamp', desc=True) \
    .limit(50) \
    .execute()
```

### Feature B: Famous Entity Watchlist

**Query to get all famous/trackable entities:**
```python
# Get all entities that have the 'is_famous' flag or known category
famous = supabase.table('addresses') \
    .select('entity_name, address_type, label, address, analysis_tags') \
    .not_.is_('entity_name', 'null') \
    .order('entity_name') \
    .execute()

# Group by entity_name for the UI
entities = {}
for row in famous.data:
    name = row['entity_name']
    if name not in entities:
        entities[name] = {'addresses': [], 'type': row['address_type']}
    entities[name]['addresses'].append(row['address'])
```

**Categories available in `analysis_tags.category`:**
- `exchange` — Binance, Coinbase, Kraken, etc.
- `individual` — Vitalik, Justin Sun, Trump, etc.
- `institution` — Paradigm, Jump, Wintermute, etc.
- `protocol` — Uniswap, Aave, MakerDAO, etc.
- `stablecoin` — Tether Treasury, Circle USDC
- `infrastructure` — Bridges (Polygon, Arbitrum, Avalanche)
- `mev` — MEV bots
- `watchlist` — Blocked/flagged addresses
- `nft` — NFT collections and platforms
- `trader` — Active trading addresses

### Feature C: Entity-Level Aggregation on Dashboard

Instead of showing raw addresses, the dashboard can now show entity names:

```python
# For each transaction in the dashboard, resolve names
tx_hash = '0xabc...'
tx = supabase.table('whale_transactions') \
    .select('*, from_label, to_label, whale_address, counterparty_address, counterparty_type') \
    .eq('transaction_hash', tx_hash) \
    .single() \
    .execute()

# Resolve whale name
whale_name = None
if tx.data['whale_address']:
    name_result = supabase.table('addresses') \
        .select('entity_name, label') \
        .eq('address', tx.data['whale_address']) \
        .execute()
    if name_result.data:
        whale_name = name_result.data[0].get('entity_name') or name_result.data[0].get('label')

# Display: "Vitalik Buterin bought 1,000 ETH from Binance ($3.2M)"
```

### Feature D: Top Whales Leaderboard

```python
# Get the most active named whales in the last 24h
from datetime import datetime, timedelta
since = (datetime.utcnow() - timedelta(hours=24)).isoformat()

result = supabase.table('whale_transactions') \
    .select('whale_address, classification, usd_value, token_symbol') \
    .gte('timestamp', since) \
    .in_('classification', ['BUY', 'SELL']) \
    .not_.is_('whale_address', 'null') \
    .execute()

# Aggregate by whale_address
from collections import defaultdict
whale_activity = defaultdict(lambda: {'buys': 0, 'sells': 0, 'volume': 0, 'tx_count': 0})
for tx in result.data:
    wa = tx['whale_address']
    whale_activity[wa]['tx_count'] += 1
    whale_activity[wa]['volume'] += float(tx['usd_value'] or 0)
    if tx['classification'] == 'BUY':
        whale_activity[wa]['buys'] += 1
    else:
        whale_activity[wa]['sells'] += 1

# Resolve names
top_whales = sorted(whale_activity.items(), key=lambda x: -x[1]['volume'])[:20]
for addr, stats in top_whales:
    name_result = supabase.table('addresses') \
        .select('entity_name') \
        .eq('address', addr) \
        .limit(1) \
        .execute()
    name = name_result.data[0]['entity_name'] if name_result.data else addr[:12] + '...'
    # Display in UI
```

### Feature E: Institution/Category Filter

Allow filtering the dashboard by entity category:

```python
# Get all institutional addresses
institutions = supabase.table('addresses') \
    .select('address, entity_name') \
    .not_.is_('entity_name', 'null') \
    .execute()

# Filter by analysis_tags category (parsed from JSON)
# Categories: exchange, individual, institution, protocol, stablecoin, infrastructure
```

### Feature F: Counterparty Intelligence

Each transaction now shows WHO the whale traded with:

```
whale_address     = 0xd8da... (Vitalik Buterin)
counterparty      = 0x28c6... (Binance)
counterparty_type = CEX
classification    = SELL
→ "Vitalik Buterin SOLD 500 ETH to Binance ($1.6M)"
```

---

## 3. Backend Code Changes Summary

### Classification fixes (`utils/classification_final.py`)
- Added `normalize_blockchain()` function — normalizes chain names (eth→ethereum, BSC→bsc, etc.)
- Improved `_institutional_cex_classification()` — now searches both `entity_name` AND `label` fields, handles 60k+ BigQuery-discovered addresses with null entity_name at 0.88 confidence
- CEX/DEX engine `analyze()` methods now normalize blockchain before querying

### Whale perspective fixes (`enhanced_monitor.py`)
- `_determine_whale_perspective()` now uses single batch Supabase query (was 2 separate queries)
- CEX detection now checks `entity_name` field and uses 15 exchange names (was 5)
- Imported `normalize_blockchain` for consistent chain name handling

### Data fixes (Supabase)
- 22 known exchange addresses (Coinbase, Binance, Kraken, etc.) fixed from wrong `address_type` (WHALE/DEFI → CEX)
- 20 addresses enriched with proper `entity_name`
- 664 stray `blockchain='eth'` records cleaned up
- 70k+ addresses enriched with entity names from eth-labels + etherscan-labels

### New scripts
- `reclassify_transfers.py` — Re-evaluates 136k TRANSFER transactions with improved logic, updating those that should be BUY/SELL
- `enrich_famous_addresses.py` — Pulls 70k+ named addresses from public datasets and upserts into Supabase

---

## 4. API Endpoints (existing — `api/whale_intelligence_api.py`)

These endpoints already work and will automatically benefit from the enriched data:

| Endpoint | Description |
|----------|-------------|
| `GET /api/whale-signals` | Per-token buy/sell percentages from whale activity |
| `GET /api/token/{symbol}` | Detailed transactions for a specific token |
| `GET /api/system-stats` | System health and transaction counts |
| `GET /` | HTML dashboard with auto-refresh |

**Suggested new endpoints for UI agent to add:**

| Endpoint | Description |
|----------|-------------|
| `GET /api/whale/{name}` | All transactions for a named entity (e.g., "Vitalik Buterin") |
| `GET /api/entities` | List all named entities with their address count and activity |
| `GET /api/entities/categories` | List entities grouped by category (exchange, individual, institution, etc.) |
| `GET /api/leaderboard` | Top 20 most active named whales in last 24h |
| `GET /api/whale/{address}/profile` | Full profile for a whale address (name, type, transaction history, buy/sell ratio) |

---

## 5. What Was Fixed & What's In Progress

### Already fixed
- **Supabase address data**: 22 major exchange addresses corrected (were mislabeled as WHALE/DEFI instead of CEX), 70k+ addresses enriched with entity names, blockchain values normalized, 664 stray records cleaned
- **CEX detection code**: Now handles 60k+ BigQuery-discovered addresses with null entity_name at 0.88 confidence (was 0.75), searches both entity_name AND label fields for exchange names
- **Blockchain normalization**: All Supabase queries now normalize chain names (eth→ethereum, BSC→bsc, etc.) preventing silent lookup misses
- **Whale perspective queries**: Consolidated from 2 separate DB calls to 1 batch query, CEX detection expanded from 5 exchange names to 15

### Actively running
- **TRANSFER reclassification**: `reclassify_transfers.py` is re-evaluating ~133k TRANSFER transactions with the improved address matching. Already converted ~2,700 to BUY/SELL. This will substantially reduce the TRANSFER rate as it completes.

### Planned improvement (not yet implemented)
- **DeFi/DEX classification logic**: For DeFi and DEX transactions specifically, the buy/sell logic still uses address directionality (who is from/to) rather than token flow analysis (stablecoin vs volatile direction). A `CLASSIFICATION_FIX_PROMPT.md` file contains the detailed prompt for this — it will fix the DeFi BUY bias (all DeFi defaulting to BUY) and make `classify_from_whale_perspective` respect the upstream 7-phase analysis instead of overriding it. This is the biggest remaining improvement for classification accuracy.
- **BigQuery credentials expired**: Service account `sonar-286@sonarium-480012` returns `invalid_grant`. Phase 8 (BigQuery whale detection) is skipped. Not critical but reduces analysis depth for ambiguous transactions.
