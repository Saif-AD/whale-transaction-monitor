# Prompt: Build a Famous Wallet Address Database (10k+ entities)

## Goal

Build a comprehensive, production-grade database of **10,000+ named crypto wallet addresses** belonging to famous individuals, institutions, companies, governments, hedge funds, VCs, DAOs, and notable traders. These will be stored in the existing Supabase `addresses` table to enable real-time tracking of who is behind whale transactions.

## Categories to Cover (with target counts)

### 1. Centralized Exchanges — Hot/Cold Wallets (~2,000 addresses)
Binance (all known wallets), Coinbase, Kraken, OKX, KuCoin, Huobi, Bybit, Gemini, Bitstamp, Bitfinex, Crypto.com, Gate.io, MEXC, Bittrex, Poloniex, etc.

### 2. Famous Individuals (~200 addresses)
- **Crypto founders**: Vitalik Buterin (0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045), CZ (Changpeng Zhao), Brian Armstrong, Sam Bankman-Fried (historical), Justin Sun, Hayden Adams, Andre Cronje, Do Kwon (historical)
- **Tech billionaires**: Elon Musk, Mark Cuban, Tim Draper
- **Public figures**: Donald Trump, Gary Vaynerchuk, Paris Hilton, Snoop Dogg, Steve Aoki
- **Notable traders**: Tetranode, Cobie, Hsaka, GCR, Andrew Kang, Ansem

### 3. Institutional Investors & Hedge Funds (~500 addresses)
- **Crypto funds**: a16z (Andreessen Horowitz), Paradigm, Polychain Capital, Pantera Capital, Galaxy Digital, Three Arrows Capital (historical), Alameda Research (historical), Jump Crypto, Wintermute, DWF Labs, Cumberland
- **TradFi**: BlackRock, Fidelity, Grayscale, ARK Invest, Goldman Sachs, JP Morgan
- **Family offices and VCs**: Dragonfly Capital, Framework Ventures, Multicoin Capital, Delphi Digital

### 4. Corporations & Treasuries (~300 addresses)
- MicroStrategy, Tesla, SpaceX, Block (Square), Coinbase (corporate treasury), Robinhood
- Wrapped BTC custodians (BitGo, etc.)
- Stablecoin treasuries (Circle/USDC, Tether/USDT)

### 5. Governments (~100 addresses)
- US Government (DOJ seized assets), UK Government, El Salvador, Bhutan, Germany (historical BTC sales), Ukraine donation wallets
- SEC enforcement wallets

### 6. DeFi Protocols & DAOs (~2,000 addresses)
- Protocol treasuries: Uniswap, Aave, Compound, MakerDAO, Lido, Curve, Convex, Yearn, Synthetix, dYdX
- Major LP positions, governance multisigs
- Bridge contracts: Arbitrum Bridge, Optimism Bridge, Polygon Bridge, Wormhole

### 7. NFT/Gaming Whales (~500 addresses)
- Pranksy, Punk6529, Gary Vee, Beeple
- Yuga Labs, OpenSea, Blur treasury

### 8. Market Makers & MEV (~500 addresses)
- Wintermute, Jump Trading, Alameda, Cumberland, Genesis Trading, Flow Traders
- Known MEV bots and searchers

### 9. Layer 2 & Infrastructure (~500 addresses)
- Arbitrum, Optimism, zkSync, Starknet foundation wallets
- Chainlink, The Graph, Filecoin foundation wallets

### 10. Notorious/Watchlist (~200 addresses)
- Known hacker wallets (Lazarus Group, Ronin Bridge hacker, Wormhole exploiter)
- Tornado Cash-linked addresses
- Rug pull deployers (for detection)

## Data Sources to Use

### Free APIs (use these first)
1. **eth-labels API** (free, 170k+ addresses): `https://eth-labels-production.up.railway.app/swagger`
   - Endpoints: `/accounts?chainId=1` for Ethereum, `chainId=56` for BSC, `chainId=137` for Polygon
   - Search by label to find exchange addresses, protocol addresses, etc.

2. **Etherscan Label Cloud** (free with API key):
   - `https://etherscan.io/labelcloud` — browse all labeled address categories
   - API: `https://docs.etherscan.io/api-reference/endpoint/exportaddresstags`

3. **GitHub datasets**:
   - `brianleect/etherscan-labels` — 30k+ Ethereum labels in JSON/CSV
   - `dawsbot/eth-labels` — 170k+ labels, MIT license, can clone the `data/` folder
   - `DataDr69/labeled_ethereum_addresses_dataset` — academic dataset

### Public On-Chain Sources
4. **ENS reverse lookups** — Many famous wallets have .eth names (vitalik.eth, etc.)
5. **Nansen** public dashboards — Top wallet labels are visible
6. **Arkham Intelligence** — Entity pages show some addresses publicly at `info.arkm.com`
7. **DeBank** — Public portfolio pages show wallet addresses

### Manual Curation
8. **Etherscan tagged addresses** — Search for "Binance", "Coinbase" etc. on Etherscan and collect all labeled hot/cold wallets
9. **Protocol documentation** — Most DeFi protocols publish their contract addresses in docs
10. **Government seizure records** — DOJ press releases contain wallet addresses

## Supabase Schema (existing — do NOT modify)

Insert into the `addresses` table with these fields:

```python
{
    'address': '0x...',               # The wallet address (lowercase)
    'blockchain': 'ethereum',          # ethereum, polygon, bsc, bitcoin, solana
    'address_type': 'CEX',            # CEX, DEX, WHALE, or keep existing type
    'label': 'Binance Hot Wallet 1',  # Descriptive label
    'entity_name': 'Binance',         # The entity/person name (CRITICAL for identification)
    'confidence': 0.95,               # How confident we are in the label (0.0-1.0)
    'signal_potential': 'HIGH',       # HIGH, MEDIUM, LOW
    'source': 'etherscan_labels',     # Where the label came from
    'detection_method': 'public_label', # How it was verified
    'analysis_tags': {                 # JSON metadata
        'category': 'exchange',        # exchange, individual, institution, protocol, government
        'subcategory': 'tier_1_cex',   # More specific classification
        'is_famous': True,             # Flag for notable entities
        'real_name': 'Changpeng Zhao', # Real name if known
        'twitter': '@caboreal'         # Social handle if relevant
    }
}
```

## Implementation Approach

### Phase 1: Bulk import from free datasets
1. Clone `dawsbot/eth-labels` repo and parse the `data/` directory
2. Download `brianleect/etherscan-labels` JSON dumps
3. Cross-reference both datasets, deduplicate, and map to our schema
4. Upsert into Supabase — update `entity_name` where our existing addresses match, insert new ones

### Phase 2: API enrichment
1. Query eth-labels API for all Ethereum addresses: `/accounts?chainId=1`
2. Query for BSC (`chainId=56`) and Polygon (`chainId=137`)
3. Parse labels to extract entity names and categories

### Phase 3: Manual curation of high-value targets
1. Manually verify the top 100 most important addresses (exchanges, famous people, institutions)
2. Add government wallets from public records
3. Add protocol treasury/multisig addresses from official docs

### Phase 4: Update existing 60k unnamed CEX addresses
1. Cross-reference our 60,861 CEX addresses (entity_name=null) against the imported datasets
2. Update entity_name for any matches
3. For remaining unmatched, try Etherscan API label lookup

## Output

Write a Python script called `enrich_famous_addresses.py` that:
1. Downloads/parses the free datasets
2. Maps them to our Supabase schema
3. Upserts into the `addresses` table (respect the unique constraint on `address, blockchain`)
4. Prints progress and summary statistics
5. Can be re-run safely (idempotent via upsert)

## Constraints

- Do NOT modify the Supabase table schema
- Do NOT delete existing addresses — only update or insert
- Use upsert with `on_conflict='address,blockchain'` to avoid duplicates
- Normalize all addresses to lowercase before inserting
- Set `confidence` based on source reliability (Etherscan label = 0.95, GitHub dataset = 0.85, manual = 0.90)
- The script should be runnable with `python enrich_famous_addresses.py` and take ~30-60 minutes
- Handle rate limits on any APIs gracefully

## Success Criteria

After running:
- 10,000+ addresses should have meaningful `entity_name` values
- The top 50 exchanges should each have 20+ labeled addresses
- At least 100 famous individual wallets should be identifiable
- The dashboard should be able to show "Vitalik Buterin sold 1000 ETH" instead of "0xd8dA... sold 1000 ETH"
