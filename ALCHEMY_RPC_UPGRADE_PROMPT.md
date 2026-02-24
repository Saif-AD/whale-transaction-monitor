# Professional Prompt: Upgrade to Alchemy RPC as Primary Data Source

## Goal

Upgrade the whale monitoring system to use **Alchemy RPC as the primary data source** for all chains:

- **EVM chains (Ethereum, Polygon)**: Etherscan discovers transfers, Alchemy fetches full receipts for $50k+ transactions
- **Solana**: Alchemy replaces Helius as primary RPC for BOTH discovery AND parsed transaction fetching (Helius becomes fallback)
- **Bitcoin**: Whale Alert discovers transfers, Alchemy fetches full transaction data for input/output analysis
- **Tron**: NEW — Alchemy provides both discovery and transaction data (currently not monitored)
- **XRP**: Keeps existing WebSocket (Alchemy doesn't support XRP)

This unlocks the highest-accuracy classification signal: decoded swap events with stablecoin flow analysis across ALL chains, not just EVM.

## Why This Matters

Currently the system detects whale transfers via Etherscan API (summary data only: from, to, amount). But Etherscan doesn't provide the **full transaction receipt** with decoded event logs. Without receipts, the system can't:
- Decode Uniswap Swap events with signed amount0/amount1
- See all ERC-20 Transfer events within a single transaction
- Determine stablecoin vs volatile token flow direction
- Identify multi-hop DEX routes

The `evm_parser.py` already has the code to decode all of this — it just needs the receipt data. With Alchemy RPC, every $50k+ transaction gets full receipt analysis, enabling `VERIFIED_SWAP_BUY/SELL` classifications at 0.90+ confidence.

## Alchemy API Details

### Endpoints (already configured in `config/api_keys.py`)

| Chain | RPC Endpoint | Status |
|-------|-------------|--------|
| Ethereum | `https://eth-mainnet.g.alchemy.com/v2/{key}` | Working |
| Polygon | `https://polygon-mainnet.g.alchemy.com/v2/{key}` | Working |
| Solana | `https://solana-mainnet.g.alchemy.com/v2/{key}` | Working |
| Bitcoin | `https://btc-mainnet.g.alchemy.com/v2/{key}` | Available |
| Tron | `https://tron-mainnet.g.alchemy.com/v2/{key}` | Available |

API Key: Already set in `config/api_keys.py` as `ALCHEMY_API_KEY`

### Key Alchemy API Methods

**Ethereum/Polygon (EVM chains):**
- `eth_getTransactionReceipt` — Full receipt with logs for one transaction (~25 CU)
- `alchemy_getTransactionReceipts` — All receipts for an entire block in one call (more efficient for batch processing)
- `eth_getLogs` — Query event logs by topic/address/block range
- `eth_getTransactionByHash` — Transaction details (value, input data, gas)
- `alchemy_getAssetTransfers` — Alchemy's enhanced API for historical transfers (ERC-20, ERC-721, ERC-1155, native)

**Solana:**
- `getTransaction` with `encoding: "jsonParsed"` — Returns parsed SPL Token transfer instructions in human-readable JSON
- `getSignaturesForAddress` — Recent transaction signatures for an address
- `getBlock` with `transactionDetails: "full"` — Full block with all transactions

**Bitcoin:**
- `btc_getTransactionByHash` — Full Bitcoin transaction with inputs/outputs
- Standard Bitcoin JSON-RPC methods

### Rate Limits (Free Tier)
- 30M Compute Units/month
- 25 requests/second
- `eth_getTransactionReceipt` = ~25 CU per call
- Budget: ~1.2M receipt fetches/month on free tier
- At $50k threshold with ~500 qualifying transactions/day = ~15k receipts/month (well within limits)

## Current Architecture

### Data Flow (current)
```
Etherscan API → ERC-20 Transfer events (summary only)
    → WhaleIntelligenceEngine.analyze_transaction_comprehensive()
        → Phase 1: _analyze_blockchain_specific() → evm_parser.analyze_dex_swap(tx_hash)
            → evm_parser calls eth_getTransactionReceipt via Web3
            → Decodes swap events, transfer events
            → Returns VERIFIED_SWAP_BUY/SELL
```

The evm_parser ALREADY fetches receipts via Web3 when it can connect. The problem was the dead Alchemy key. Now that the key is working, Phase 1 should produce verified swap results.

### What Needs to Change

1. **Ensure every $50k+ transaction gets a receipt fetch** — Currently some code paths skip the receipt if the initial Etherscan data seems sufficient. Force receipt fetching for high-value transactions.

2. **Add Alchemy as primary RPC with Etherscan as fallback** — If Alchemy is down or rate-limited, fall back to Etherscan for transfer data (degraded but functional).

3. **Add Solana receipt fetching via Alchemy** — Currently Solana uses Helius. Add Alchemy Solana as primary, Helius as fallback.

4. **Add Bitcoin and Tron transaction fetching** — Currently Bitcoin comes through Whale Alert only. Add Alchemy Bitcoin RPC for receipt-level data.

5. **Parse Solana transaction receipts properly** — Solana's `getTransaction` with `jsonParsed` encoding returns structured SPL Token transfer data. Parse this for token flow analysis.

## Files to Modify

### `config/api_keys.py`
- Add Alchemy Solana, Bitcoin, Tron RPC URLs
- Add `ALCHEMY_SOLANA_RPC`, `ALCHEMY_BITCOIN_RPC`, `ALCHEMY_TRON_RPC` constants
- Update RPC provider fallback lists to put Alchemy first

### `chains/ethereum.py` (~300 lines)
- After detecting a whale transfer via Etherscan, fetch the full receipt from Alchemy if `usd_value >= 50000`
- Pass the receipt data to the `WhaleIntelligenceEngine` so Phase 1 can decode swap events
- If Alchemy fails, continue with Etherscan data only (graceful degradation)

### `chains/polygon.py`
- Same pattern as ethereum.py — fetch Alchemy receipt for $50k+ transactions

### `chains/solana.py` and `chains/solana_api.py`
- Add Alchemy Solana as primary RPC (currently uses Helius)
- Fetch full transaction with `encoding: "jsonParsed"` for $50k+ transactions
- Parse SPL Token transfer instructions for token flow analysis
- Fall back to Helius if Alchemy fails

### `utils/evm_parser.py` (~2100 lines)
- Ensure `analyze_dex_swap()` can accept a pre-fetched receipt (avoid double-fetching)
- Add method signature: `analyze_dex_swap(tx_hash, receipt=None)` — if receipt is provided, skip the RPC call
- This way the chain module fetches the receipt once and passes it through

### `utils/solana_parser.py`
- Add Alchemy `getTransaction` (jsonParsed) as primary method
- Parse SPL Token transfer instructions to determine:
  - Which tokens the user sent (token flow OUT)
  - Which tokens the user received (token flow IN)
  - Whether SOL or stablecoins (USDC/USDT) were involved
  - Classification: stablecoin OUT = BUY, stablecoin IN = SELL

### `utils/classification_final.py`
- In `_analyze_blockchain_specific()`, if a receipt is passed in the transaction data, use it directly instead of making another RPC call
- In `_analyze_evm_transaction()`, check for `transaction.get('receipt')` before calling `parser.analyze_dex_swap()`

### `enhanced_monitor.py`
- In `store_whale_transaction()`, pass receipt data through to the engine when available
- In the Whale Alert processing path, fetch receipt from Alchemy before classification

## Implementation Details

### Receipt Fetching Strategy

```python
RECEIPT_USD_THRESHOLD = 50_000  # Only fetch receipts for $50k+ transactions

async def fetch_receipt_if_needed(tx_hash: str, usd_value: float, blockchain: str) -> Optional[dict]:
    """Fetch full transaction receipt from Alchemy for high-value transactions."""
    if usd_value < RECEIPT_USD_THRESHOLD:
        return None
    
    rpc_url = get_alchemy_rpc(blockchain)  # Returns appropriate Alchemy endpoint
    if not rpc_url:
        return None
    
    try:
        response = requests.post(rpc_url, json={
            'jsonrpc': '2.0',
            'method': 'eth_getTransactionReceipt',
            'params': [tx_hash],
            'id': 1
        }, timeout=10)
        result = response.json().get('result')
        if result:
            return result
    except Exception as e:
        logger.warning(f"Alchemy receipt fetch failed for {tx_hash}: {e}")
    
    return None
```

### Solana Parsed Transaction Fetching

```python
def fetch_solana_transaction(signature: str, rpc_url: str) -> Optional[dict]:
    """Fetch parsed Solana transaction from Alchemy."""
    try:
        response = requests.post(rpc_url, json={
            'jsonrpc': '2.0',
            'method': 'getTransaction',
            'params': [signature, {
                'encoding': 'jsonParsed',
                'maxSupportedTransactionVersion': 0,
                'commitment': 'confirmed'
            }],
            'id': 1
        }, timeout=10)
        return response.json().get('result')
    except Exception:
        return None
```

### Solana Token Flow Extraction

Parsed Solana transactions contain structured transfer data:
```json
{
  "type": "transfer",
  "info": {
    "source": "UserWallet...",
    "destination": "DEXPool...",
    "amount": "1000000",
    "authority": "UserWallet..."
  }
}
```

Parse these to build token flow:
- Group transfers by direction (user sent vs user received)
- Check if SOL, USDC, or USDT was sent/received
- Apply same stablecoin flow logic: stablecoin OUT = BUY, stablecoin IN = SELL

### RPC Fallback Chain

```
EVM (Ethereum/Polygon):
    Discovery: Etherscan/Polygonscan API (always)
    Receipts:  Alchemy → Infura → Cloudflare → skip (address-based only)

Solana:
    Primary:   Alchemy Solana RPC (discovery + parsed transactions)
    Fallback:  Helius RPC (discovery + parsed transactions)
    Fallback2: Solscan API (summary data only)

Bitcoin:
    Discovery: Whale Alert WebSocket (always)
    Receipts:  Alchemy Bitcoin RPC → skip (Whale Alert data only)

Tron:
    Primary:   Alchemy Tron RPC (discovery + transactions)
    Fallback:  None (new chain, no legacy source)

XRP:
    Primary:   XRP WebSocket (existing, no change)
```

### Rate Limit Protection

```python
import time
from collections import defaultdict

class AlchemyRateLimiter:
    """Prevent exceeding 25 req/sec and 30M CU/month."""
    
    def __init__(self, max_rps=20, monthly_cu_budget=28_000_000):
        self.max_rps = max_rps
        self.monthly_budget = monthly_cu_budget
        self.cu_used = 0
        self.last_request_time = 0
        self.request_count_this_second = 0
    
    def can_request(self, cu_cost=25) -> bool:
        if self.cu_used + cu_cost > self.monthly_budget:
            return False
        
        now = time.time()
        if now - self.last_request_time >= 1.0:
            self.request_count_this_second = 0
            self.last_request_time = now
        
        return self.request_count_this_second < self.max_rps
    
    def record_request(self, cu_cost=25):
        self.cu_used += cu_cost
        self.request_count_this_second += 1
```

## Constraints

- **$50k USD threshold** for receipt fetching — never fetch receipts for transactions below this to protect rate limits
- **Max 20 requests/second to Alchemy** (leave headroom below the 25 rps limit)
- **Monthly CU budget cap at 28M** (leave 2M buffer below 30M free tier)
- Do NOT remove Etherscan as a data source — it remains the discovery mechanism for EVM whale transfers
- **Alchemy is the single reliable provider** — Polygonscan, Helius, Solscan, and other secondary APIs are unreliable in this environment. Design the system so Alchemy alone is sufficient for full operation. Fallbacks are a safety net, not a dependency.
- Do NOT change the Supabase schema
- Do NOT change the API response format
- Keep the existing evm_parser.py logic — it already correctly decodes swap events when given a receipt
- Graceful degradation is mandatory — if Alchemy is down, the system must continue working with Etherscan-only data
- Thread-safe rate limiting (multiple chain monitors run concurrently)

## Data Flow After Upgrade

**Different chains use different discovery mechanisms.** Etherscan is EVM-only. For Solana, Bitcoin, and Tron, Alchemy handles BOTH discovery AND receipts.

```
EVM CHAINS (Ethereum, Polygon):
    Etherscan/Polygonscan API → Discovers whale ERC-20 transfers
        │
        ├── usd_value < $50k → Classify with address-based logic only
        │
        └── usd_value >= $50k → Fetch full receipt from Alchemy RPC
                └── eth_getTransactionReceipt → decode swap events
                └── VERIFIED_SWAP_BUY/SELL (0.90+)

SOLANA:
    Alchemy Solana RPC (primary) / Helius (fallback) → Discovery + Receipts
        │
        ├── getSignaturesForAddress → Discover whale transactions
        │
        └── getTransaction (jsonParsed) → Full parsed SPL Token transfers
                └── Token flow analysis → BUY/SELL (stablecoin direction)

BITCOIN:
    Whale Alert WebSocket → Discovers large BTC transfers
        │
        └── Alchemy Bitcoin RPC → btc_getTransactionByHash
                └── Input/output analysis → value flow direction

TRON:
    Alchemy Tron RPC → Discovery + Receipts (NEW — not yet implemented)
        │
        ├── Discover TRC-20 transfers to/from known exchanges
        │
        └── Full transaction data → token flow analysis

XRP:
    XRP WebSocket (existing) → Discovers large XRP transfers
        └── Classify via address matching (CEX direction)

FALLBACK (any chain):
    If Alchemy is down → Continue with existing sources
        └── Etherscan (EVM), Helius (Solana), Whale Alert (BTC), XRP WS
        └── Address-based classification only (degraded)
```

## Success Criteria

After implementation:
- Every $50k+ EVM transaction should have a full receipt fetched and decoded
- DEX swaps should produce `VERIFIED_SWAP_BUY/SELL` with 0.88-0.92 confidence
- Solana $50k+ transactions should have parsed token flow analysis
- Alchemy CU usage should stay under 28M/month
- Rate limit should never exceed 20 req/sec
- If Alchemy goes down, system continues with Etherscan (with warning log)
- No change in behavior for transactions under $50k
