# Professional Prompt: Fix Buy/Sell Classification Algorithm

## You are a senior crypto quant engineer. Fix the transaction classification system.

## Problem Statement

A production whale monitoring system classifies crypto transactions as BUY, SELL, TRANSFER, or DEFI. Currently:
- **92% of whale addresses show 100% one-sided activity** (all BUY or all SELL) — this is wrong
- **58% of transactions default to TRANSFER** because the system can't determine direction
- **All DeFi interactions are classified as BUY** regardless of actual direction
- The 7-phase analysis engine produces good signals but gets **overridden** by a simple from/to address check

## How Buy/Sell Classification SHOULD Work in Crypto

The **ground truth** for buy vs sell is **token flow direction**, not address direction:

### CEX Transactions (address direction IS meaningful)
- `Binance → User` = User receives tokens = **BUY** (withdrawal after purchase)
- `User → Binance` = User sends tokens = **SELL** (deposit to sell)
- This is correct and should be kept as-is

### DEX Swap Transactions (token flow determines direction)
A DEX swap produces multiple ERC-20 Transfer events in a single transaction:
- User sends USDC to Uniswap Router → Router sends ETH to User = **BUY** (spent stablecoins, received volatile)
- User sends ETH to Uniswap Router → Router sends USDC to User = **SELL** (spent volatile, received stablecoins)

The classification rules based on **stablecoin flow**:
| Tokens OUT | Tokens IN | Classification | Confidence |
|-----------|----------|---------------|------------|
| Stablecoin (USDC/USDT/DAI) | Volatile token | **BUY** | 0.90 |
| Volatile token | Stablecoin | **SELL** | 0.90 |
| ETH/WETH | Other token | **BUY** | 0.85 |
| Other token | ETH/WETH | **SELL** | 0.85 |
| Stablecoin A | Stablecoin B | **TRANSFER** | 0.80 |
| Volatile A | Volatile B | Context-dependent | 0.70 |

### DeFi Protocol Interactions (action determines direction)
| Action | Classification | Reasoning |
|--------|---------------|-----------|
| Stake tokens INTO protocol | **SELL-like** (locking capital) or **DEFI** | NOT a buy |
| Unstake/withdraw FROM protocol | **BUY-like** (unlocking capital) or **DEFI** | NOT a buy |
| Deposit into lending pool | **DEFI** | Capital deployment, not directional |
| Borrow from lending pool | **DEFI** | Leverage, not directional |
| LP add liquidity | **DEFI** | Neutral position |
| LP remove liquidity | **DEFI** | Neutral position |
| DEX swap via protocol | Use **token flow analysis** above | |

### Uniswap V3 Swap Events (decoded)
The Swap event has signed `amount0` and `amount1`:
- **Negative amount** = token flowed INTO the pool = user SOLD that token
- **Positive amount** = token flowed OUT of the pool = user BOUGHT that token
- Check which token is the stablecoin to determine final BUY/SELL

## The 3 Bugs to Fix

### Bug 1: `classify_from_whale_perspective()` overrides everything

**File**: `enhanced_monitor.py`, line ~287

```python
def classify_from_whale_perspective(whale_address, from_address, to_address, counterparty_type, original_classification):
    trade = counterparty in TRADE_COUNTERPARTY_TYPES  # {'CEX', 'DEX'}
    if whale_addr == to_addr:
        return 'BUY' if trade else 'TRANSFER'
    if whale_addr == from_addr:
        return 'SELL' if trade else 'TRANSFER'
```

This function runs AFTER the 7-phase analysis engine and **throws away the result**, replacing it with a simple from/to check. For DEX transactions this is completely wrong — in a DEX swap, the whale both sends and receives tokens in the same transaction, so the from/to tells you nothing about buy vs sell.

**Fix**: Make this function respect the upstream classification:

```python
def classify_from_whale_perspective(whale_address, from_address, to_address, 
                                     counterparty_type, original_classification):
    whale_addr = (whale_address or '').lower()
    from_addr = (from_address or '').lower()
    to_addr = (to_address or '').lower()
    counterparty = (counterparty_type or '').upper()
    
    if not whale_addr:
        return original_classification
    
    # For CEX transactions, address direction is meaningful
    if counterparty == 'CEX':
        if whale_addr == to_addr:
            return 'BUY'
        elif whale_addr == from_addr:
            return 'SELL'
    
    # For DEX/DeFi, TRUST the upstream 7-phase classification
    # It has token flow analysis that we don't have here
    if counterparty == 'DEX':
        if original_classification in ('BUY', 'SELL'):
            return original_classification
        # Only fall back to address direction if upstream was inconclusive
        if whale_addr == to_addr:
            return 'BUY'
        elif whale_addr == from_addr:
            return 'SELL'
    
    # For EOA (wallet-to-wallet), keep as TRANSFER unless upstream says otherwise
    if original_classification in ('BUY', 'SELL'):
        return original_classification
    
    return 'TRANSFER'
```

### Bug 2: Every DeFi interaction defaults to BUY

**File**: `utils/classification_final.py`

The `_analyze_defi_protocol_interaction` method (around line 1758-1797) and the DefiLlama/slug-based detection (lines 1844-1928) hardcode `ClassificationType.BUY` for virtually every protocol:

```python
# Lines 1795-1797 - even "receiving from protocol" = BUY (!!)
if protocol_interactions[0][0] == 'from':
    return ClassificationType.BUY, f"User deploying capital to ..."
else:
    return ClassificationType.BUY, f"User receiving assets from ..."  # THIS IS WRONG
```

And all slug-based protocol mappings:
```python
protocol_mappings = {
    'uniswap': (ClassificationType.BUY, ...),    # WRONG - should depend on token flow
    'curve': (ClassificationType.BUY, ...),       # WRONG
    'aave': (ClassificationType.BUY, ...),        # WRONG - lending is DEFI, not BUY
    'compound': (ClassificationType.BUY, ...),    # WRONG
    ...
}
```

**Fix**: Replace all hardcoded BUY with `ClassificationType.DEFI` for protocol interactions, and `ClassificationType.TRANSFER` for DEX interactions that need token flow analysis:

- For **DEX protocols** (Uniswap, Curve, Balancer, PancakeSwap, SushiSwap): Return `ClassificationType.TRANSFER` with a flag `requires_token_flow_analysis=True` so the master classifier knows to check token flow
- For **lending protocols** (Aave, Compound): Return `ClassificationType.DEFI`
- For **staking protocols** (Lido, Rocket Pool): Return `ClassificationType.DEFI`
- For **bridges**: Return `ClassificationType.TRANSFER`
- The "receiving from protocol" case (line 1797) should return `ClassificationType.DEFI`, NOT BUY

### Bug 3: Token flow analysis exists but gets bypassed

**File**: `utils/evm_parser.py` has excellent token flow analysis:

- `_classify_swap_direction_enhanced()` — analyzes stablecoin vs volatile token flows, handles multi-hop router swaps, ETH internal transfers
- `_classify_direction_advanced()` — comprehensive token flow analysis using Transfer events
- `_analyze_swap_events_for_direction()` — decodes Uniswap Swap events with signed amounts

These produce `VERIFIED_SWAP_BUY` and `VERIFIED_SWAP_SELL` results with 0.85-0.92 confidence.

**But they get bypassed** because:
1. The `_map_to_final_classification()` method in `classification_final.py` (line ~2654) maps `VERIFIED_SWAP_BUY → BUY` and `VERIFIED_SWAP_SELL → SELL` correctly, but...
2. The DeFi classification phase runs FIRST and returns `BUY` for any DEX interaction
3. The master classifier takes the highest-confidence result, which is usually the DeFi phase's hardcoded BUY at 0.88+ confidence
4. The token flow analysis result gets buried

**Fix**: The classification priority in `_determine_master_classification()` must be changed:

```
PRIORITY ORDER (highest to lowest):
1. VERIFIED_SWAP_BUY / VERIFIED_SWAP_SELL from evm_parser (token flow) → 0.92 confidence
2. CEX direction from address matching → 0.90 confidence  
3. Stablecoin flow analysis → 0.85 confidence
4. DeFi protocol type → informational only, NEVER determines buy/sell direction
5. Behavioral heuristics → tiebreaker
```

## Files to Modify

### `enhanced_monitor.py` (~3824 lines)
- **`classify_from_whale_perspective()`** (line ~287): Rewrite to respect upstream classification for DEX/DeFi transactions. Only override for CEX where address direction is meaningful.

### `utils/classification_final.py` (~5262 lines)
- **`_analyze_defi_protocol_interaction()`** and related methods (lines ~1758-1928): Replace all hardcoded `ClassificationType.BUY` with `ClassificationType.DEFI` or `ClassificationType.TRANSFER`
- **Protocol slug mappings** (lines ~1882-1888): Change all from BUY to DEFI
- **Generic DeFi fallback** (lines ~1795-1797): Change from BUY to DEFI
- **`_determine_master_classification()`** (line ~3775): Change priority to prefer token flow results (VERIFIED_SWAP_BUY/SELL) over DeFi protocol classification
- **`_execute_enhanced_weighted_aggregation()`** (line ~3827): Change phase weights to give blockchain_specific (which contains evm_parser token flow results) higher weight than dex_protocol when token flow data is present
- **`_map_to_final_classification()`** (line ~2654): Already correct, keep as-is
- **`_evaluate_stage1_results()`** (line ~4663): When a VERIFIED_SWAP result exists in Phase 1, allow early exit even if other phases say TRANSFER
- **`_finalize_early_exit()`** (line ~2922): Stop mapping STAKING and DEFI to BUY (lines 2930-2934). STAKING should stay DEFI, and DEFI should stay DEFI.

### `utils/evm_parser.py` (~2100 lines)
- **No bugs here** — this file has the correct token flow logic. But ensure its results are properly propagated through `PhaseResult.raw_data` so the master classifier can access them.

## Detailed Fix Instructions

### Step 1: Fix `classify_from_whale_perspective` in enhanced_monitor.py

Replace the current function (lines ~287-337) with logic that:
- For `counterparty_type == 'CEX'`: Use address direction (keep current logic)
- For `counterparty_type == 'DEX'`: Trust the `original_classification` from the 7-phase engine
- For `counterparty_type == 'EOA'`: Trust the `original_classification` if it's BUY/SELL, otherwise return TRANSFER
- Never override a BUY/SELL from the engine with TRANSFER

### Step 2: Fix DeFi BUY bias in classification_final.py

Find every instance where a DeFi protocol interaction returns `ClassificationType.BUY` and change it:

**DEX protocols** (Uniswap, Curve, Balancer, etc.):
- Change from `ClassificationType.BUY` to `ClassificationType.TRANSFER`
- Add `raw_data['requires_token_flow'] = True` to signal the master classifier

**Lending/Staking protocols** (Aave, Compound, Lido):
- Change from `ClassificationType.BUY` to `ClassificationType.DEFI`

**Generic fallback** (line ~1795-1797):
- "Deploying capital" = `ClassificationType.DEFI`
- "Receiving from protocol" = `ClassificationType.DEFI`

### Step 3: Fix master classifier priority

In `_determine_master_classification()`:

Before checking DeFi/CEX priority phases, check if any phase produced a `VERIFIED_SWAP_BUY` or `VERIFIED_SWAP_SELL` result. If so, use that result directly — it's the most reliable signal because it's based on actual decoded swap events and token flow.

```python
# Check for verified swap results first (highest priority)
for phase_name, result in phase_results.items():
    if result.raw_data and result.raw_data.get('verified_swap'):
        if result.classification in [ClassificationType.BUY, ClassificationType.SELL]:
            return result.classification, max(result.confidence, 0.88), f"Verified swap: {phase_name}"
```

### Step 4: Stop mapping DEFI/STAKING to BUY in _finalize_early_exit

Lines ~2930-2934 currently do:
```python
if final_classification == ClassificationType.STAKING:
    final_classification = ClassificationType.BUY
elif final_classification == ClassificationType.DEFI:
    final_classification = ClassificationType.BUY
```

Change to:
```python
if final_classification == ClassificationType.STAKING:
    final_classification = ClassificationType.DEFI
# DEFI stays as DEFI — it's not a directional trade
```

### Step 5: Ensure evm_parser results propagate

In `_analyze_blockchain_specific()`, when the evm_parser produces a result, store the swap direction in `raw_data`:

```python
phase_result.raw_data['verified_swap'] = True
phase_result.raw_data['swap_direction'] = 'BUY' or 'SELL'
phase_result.raw_data['token_flow'] = {
    'tokens_sent': [...],
    'tokens_received': [...],
    'stablecoin_direction': 'OUT' or 'IN'
}
```

This allows the master classifier to use token flow even if the phase confidence was low.

## Constraints

- Do NOT restructure the 7-phase pipeline architecture
- Do NOT remove the near-duplicate detection system
- Do NOT change the Supabase table schema
- Do NOT change the API response format in `whale_intelligence_api.py`
- Keep backward compatibility with whale_alert, ethereum, polygon, solana chain modules
- The `ClassificationType` enum values must remain the same
- Maintain all existing imports and class interfaces
- The `store_whale_transaction` method must continue to work unchanged

## Classification Flow After Fix

```
Transaction arrives
    │
    ├─► Phase 1: Blockchain Specific (evm_parser token flow analysis)
    │   └─► If VERIFIED_SWAP_BUY/SELL → HIGH PRIORITY SIGNAL (0.90+)
    │
    ├─► Phase 2: Stablecoin Flow Analysis
    │   └─► Stablecoin out = BUY, Stablecoin in = SELL
    │
    ├─► Phase 3: CEX Classification (address matching)
    │   └─► CEX→User = BUY, User→CEX = SELL (0.88-0.95)
    │
    ├─► Phase 4: DEX/DeFi Classification
    │   └─► Returns DEFI or TRANSFER (NEVER hardcoded BUY)
    │   └─► Protocol type is context, not direction
    │
    ├─► Phase 5: Wallet Behavior
    │
    ├─► Master Classifier
    │   └─► Priority: Token flow > CEX direction > Stablecoin flow > Protocol type
    │   └─► DEFI stays DEFI, STAKING stays DEFI
    │
    └─► classify_from_whale_perspective (final pass)
        ├─► CEX counterparty: Use address direction
        ├─► DEX counterparty: TRUST the master classifier result
        └─► EOA: TRUST if BUY/SELL, else TRANSFER
```

## Success Criteria

After the fix:
- Addresses should show a **mix of BUY and SELL** (60/40, 70/30 etc.)
- TRANSFER rate should drop from 58% to under 30%
- DeFi interactions should classify as **DEFI**, not BUY
- DEX swaps should use **token flow** (stablecoin direction) not address position
- CEX deposit/withdrawal should still work correctly
- `classify_from_whale_perspective` should NOT override verified token flow analysis

## Testing

After making changes, verify with these scenarios:

1. **Whale swaps USDC for ETH on Uniswap** → Should be BUY (stablecoin out, volatile in)
2. **Whale swaps ETH for USDC on Uniswap** → Should be SELL (volatile out, stablecoin in)
3. **Whale withdraws ETH from Binance** → Should be BUY (CEX → User)
4. **Whale deposits ETH to Coinbase** → Should be SELL (User → CEX)
5. **Whale stakes ETH in Lido** → Should be DEFI (not BUY)
6. **Whale unstakes from Aave** → Should be DEFI (not BUY)
7. **Whale transfers ETH to another wallet** → Should be TRANSFER
8. **Whale does multi-hop swap: USDC → WETH → ARB** → Should be BUY (stablecoin out)
