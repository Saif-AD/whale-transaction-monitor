# Professional Prompt: Fix Multi-Chain Startup + Alchemy Rate Limit

## Two Problems to Solve

### Problem 1: Solana, XRP, and Whale Alert (Bitcoin) threads don't start

Currently when `python enhanced_monitor.py` runs, only Ethereum and Polygon monitors start. Solana, XRP, and Whale Alert (Bitcoin) threads silently fail to launch. The startup messages for these chains never appear in the terminal.

### Problem 2: Web3 swap monitors burn through Alchemy's free tier in hours

The Web3 event-driven swap monitors (`eth_getLogs` polling for Uniswap V2/V3, Curve, Balancer, 1inch on Ethereum + Polygon) poll every few seconds across 6+ DEX contracts on 2 chains. This generates ~720 RPC calls/minute = ~1M CU/hour. The Alchemy free tier (30M CU/month) would be exhausted in ~28 hours.

Meanwhile, our actual receipt fetching (for $50k+ transaction classification) only made 2 calls in 3 minutes. The swap monitors are the problem — and they're mostly failing anyway (Polygon POA `extraData` errors, `400 Bad Request`).

## What's Working Now

- **Etherscan multi-token polling**: Working perfectly. Discovers ERC-20 transfers across 108 tokens in 6 groups. This is the primary data source and doesn't use Alchemy CU at all.
- **Alchemy receipt fetching**: Working. Fetches full receipts for $50k+ transactions. Only ~2 CU per qualifying transaction.
- **CEX classification**: Working at 0.88-0.97 confidence.
- **Near-duplicate detection**: Working correctly.

## What's NOT Working

- **Web3 swap monitors**: Burning CU on `eth_getLogs` polls that mostly fail. These monitors scan for Uniswap/Curve/Balancer/1inch swap events in real-time. On Polygon they fail with POA `extraData` errors. On Ethereum they produce `400 Bad Request` for some contract queries.
- **Solana thread**: Not starting. Uses Alchemy Solana RPC (newly configured) but the thread initialization fails silently.
- **XRP thread**: Not starting. WebSocket-based.
- **Whale Alert thread**: Attempts to connect but gets `401 Unauthorized` — API key expired.

## Files to Modify

### `enhanced_monitor.py` (~3839 lines)

**Fix 1: Disable Web3 swap event polling (save CU)**

Find where the Web3 swap monitors are started:
- `✅ Web3 Ethereum swaps monitor started`
- `✅ Web3 Polygon swaps monitor started`
- `✅ 1inch Ethereum monitor started`
- `✅ 1inch Polygon monitor started`

These use `eth_getLogs` to scan every block for DEX swap events. **Disable these monitors entirely.** The Etherscan multi-token polling already discovers whale transfers, and Alchemy receipt fetching handles the deep analysis for $50k+ transactions. The real-time swap monitors add no value since:
1. They burn ~1M CU/hour
2. Most calls fail (POA errors on Polygon, 400s on some Ethereum contracts)
3. The Covalent API calls they trigger also fail (402 Payment Required)
4. The `real_time_classification.py` classify attempts all return `UNKNOWN (0.00 confidence)`

Keep the Web3 connection test (it's useful for receipt fetching) but remove the continuous `get_logs` polling loops.

**Fix 2: Ensure Solana thread starts**

The `start_solana_thread()` function is called but the thread either fails to start or starts without producing output. Debug and fix the initialization. The Alchemy Solana RPC is configured and tested working at `https://solana-mainnet.g.alchemy.com/v2/{key}`.

The Solana monitoring should use `getSignaturesForAddress` to poll for large transactions to monitored Solana wallets/tokens, similar to how Etherscan polling works for EVM.

**Fix 3: Ensure XRP thread starts**

The `start_xrp_thread()` function uses a WebSocket connection. Verify it connects and produces output. This is independent of Alchemy.

**Fix 4: Handle Whale Alert 401 gracefully**

The Whale Alert WebSocket returns `401 Unauthorized`. The current code retries after 120 seconds, which is correct. But it should log a clear warning that the API key needs renewal and not spam the terminal with full response headers.

### `utils/real_time_classification.py`

This module's `classify_1inch_swap` and `classify_uniswap_v2_swap` methods are called by the Web3 monitors and they ALL fail:
- `Failed to classify 1inch swap: Could not determine token_in/token_out from Transfer deltas`
- `Failed to classify Uniswap V2 swap: extraData is 803 bytes, but should be 32`
- `Covalent API error 402` (expired key)

After disabling the Web3 swap monitors, these errors will stop entirely.

### `chains/solana.py` and `chains/solana_api.py`

Debug why the Solana thread doesn't produce any output. Check:
1. Is `start_solana_thread()` actually being called?
2. Does it connect to the Alchemy Solana RPC?
3. Is it finding any qualifying transactions above the $50k threshold?
4. Are there any silent exceptions being swallowed?

## Alchemy CU Budget Strategy

After fixing:

| Component | CU/hour | Status |
|-----------|---------|--------|
| Web3 swap monitors (get_logs) | ~1,000,000 | **DISABLED** |
| Receipt fetching ($50k+ txs) | ~500 | Active |
| Solana getTransaction calls | ~1,000 | Active |
| Total | ~1,500 | Well within free tier |

Monthly projection: ~1,500 CU/hour × 720 hours = ~1.08M CU/month (3.6% of 30M budget)

## Constraints

- Do NOT disable the Etherscan multi-token polling — it's the primary data source
- Do NOT disable the Alchemy receipt fetching for $50k+ transactions — it's needed for swap classification
- Do NOT remove the Web3 connection initialization — keep `Web3(HTTPProvider(ALCHEMY_ETHEREUM_RPC))` alive for receipt fetching
- Only disable the continuous `get_logs` polling loops
- Keep the existing `utils/alchemy_rpc.py` rate limiter
- Graceful degradation: if Solana/XRP threads fail, the system continues with Ethereum

## Success Criteria

After fixing:
- Monitor starts and shows: Ethereum, Polygon, Solana, XRP threads all running
- Alchemy CU usage drops from ~1M/hour to under ~2k/hour
- No more `get_logs error` spam in the terminal
- No more `Failed to classify 1inch/Uniswap` errors
- Solana large transactions ($50k+) are detected and classified
- Whale Alert shows a clear "API key expired" message (not full HTTP headers)
- System runs 24/7 without exhausting the Alchemy free tier
