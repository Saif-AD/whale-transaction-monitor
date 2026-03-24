from __future__ import annotations
import requests
import time
from datetime import datetime
from typing import Dict, List, Optional
from config.api_keys import ETHERSCAN_API_KEY, ETHERSCAN_API_KEYS
import random
from config.settings import (
    GLOBAL_USD_THRESHOLD,
    last_processed_block,
    etherscan_buy_counts,
    etherscan_sell_counts,
    print_lock
)
from data.tokens import TOKENS_TO_MONITOR, TOKEN_PRICES
from utils.classification_final import WhaleIntelligenceEngine, comprehensive_stablecoin_analysis
from utils.base_helpers import safe_print, log_error
from utils.summary import record_transfer
from utils.summary import has_been_classified, mark_as_classified
from data.market_makers import MARKET_MAKER_ADDRESSES, FILTER_SETTINGS
from utils.dedup import deduplicator, get_dedup_stats, deduped_transactions, handle_event

import logging
eth_logger = logging.getLogger(__name__)

# Global variable for batch timing
last_batch_storage_time = time.time()

def _is_whale_relevant_transaction(from_addr: str, to_addr: str, token_symbol: str) -> bool:
    """
    🎯 PROFESSIONAL WHALE FILTERING: Only process transactions relevant to whale monitoring
    
    Filters out random wallet-to-wallet transfers and focuses on:
    - DEX router interactions (Uniswap, SushiSwap, 1inch, etc.)
    - CEX deposits/withdrawals (Binance, Coinbase, etc.)
    - Major DeFi protocol interactions (Aave, Compound, etc.)
    - Bridge transactions (cross-chain activity)
    
    This fixes the ETH "TRANSFER" issue by only monitoring actual trading activity.
    """
    # Convert to lowercase for comparison
    from_addr = from_addr.lower()
    to_addr = to_addr.lower()
    
    # Known DEX routers (expanded list from our database expansion)
    dex_routers = {
        '0x7a250d5630b4cf539739df2c5dacb4c659f2488d',  # Uniswap V2 Router
        '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45',  # Uniswap V3 Router
        '0xe592427a0aece92de3edee1f18e0157c05861564',  # Uniswap V3 Router 2
        '0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad',  # Uniswap Universal Router
        '0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b',  # Uniswap Universal Router V1
        '0x000000000004444c5dc75cb358380d2e3de08a90',  # Uniswap Permit2
        '0x000000000022d473030f116ddee9f6b43ac78ba3',  # Uniswap Permit2 V1
        '0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f',  # SushiSwap Router
        '0x1111111254fb6c44bac0bed2854e76f90643097d',  # 1inch Router V4
        '0x1111111254eeb25477b68fb85ed929f73a960582',  # 1inch V5 Router
        '0x111111125421ca6dc452d289314280a0f8842a65',  # 1inch V6 Router
        '0xdef1c0ded9bec7f1a1670819833240f027b25eff',  # 0x Protocol Exchange
        '0x99a58482ba3d06e0e1e9444c8b7a8c7649e8c9c1',  # Curve Router
        '0xba12222222228d8ba445958a75a0704d566bf2c8',  # Balancer V2 Vault
        '0x9008d19f58aabd9ed0d60971565aa8510560ab41',  # CoW Protocol Settlement
        '0x881d40237659c251811cec9c364ef91dc08d300c',  # MetaMask Swap
        '0x6131b5fae19ea4f9d964eac0408e4408b66337b5',  # Kyber Router
    }
    
    # Known CEX addresses (major exchanges)
    cex_addresses = {
        '0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be',  # Binance Hot Wallet
        '0xd551234ae421e3bcba99a0da6d736074f22192ff',  # Binance Hot Wallet 2
        '0x28c6c06298d514db089934071355e5743bf21d60',  # Binance Hot Wallet 14
        '0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43',  # Coinbase Hot Wallet
        '0x71660c4005ba85c37ccec55d0c4493e66fe775d3',  # Coinbase Hot Wallet 2
        '0x2910543af39aba0cd09dbb2d50200b3e800a63d2',  # Kraken Hot Wallet
        '0x6cc5f688a315f3dc28a7781717a9a798a59fda7b',  # OKEx Hot Wallet
        '0xdc76cd25977e0a5ae17155770273ad58648900d3',  # Huobi Hot Wallet
        '0xf89d7b9c864f589bbf53a82105107622b35eaa40',  # Bybit Hot Wallet
        '0x1522900b6dafac587d499a862861c0869be6e428',  # KuCoin Hot Wallet
    }
    
    # Major DeFi protocols (lending, staking, etc.)
    defi_protocols = {
        '0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9',  # Aave Lending Pool
        '0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2',  # Aave Pool V3
        '0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b',  # Compound cDAI
        '0x39aa39c021dfbae8fac545936693ac917d5e7563',  # Compound cUSDC
        '0xae7ab96520de3a18e5e111b5eaab95820216e558',  # Lido stETH
        '0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7',  # Curve 3Pool
    }
    
    # Bridge contracts (cross-chain activity)
    bridge_contracts = {
        '0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf',  # Polygon Bridge
        '0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a',  # Arbitrum Bridge
        '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1',  # Optimism Gateway
        '0x6b7a87899490ece95443e979ca9485cbe7e71522',  # Multichain Router
    }
    
    # Check if transaction involves any whale-relevant addresses
    whale_relevant_addresses = dex_routers | cex_addresses | defi_protocols | bridge_contracts
    
    # Transaction is whale-relevant if either from or to address is in our list
    is_relevant = from_addr in whale_relevant_addresses or to_addr in whale_relevant_addresses
    
    # Special case: For major tokens like ETH, WETH, USDC - be more selective
    major_tokens = {'ETH', 'WETH', 'USDC', 'USDT', 'DAI'}
    if token_symbol in major_tokens:
        # For major tokens, ONLY process if going through known infrastructure
        return is_relevant
    else:
        # For altcoins (like PEPE), process all large transactions as they're likely trading
        # Also process if going through known infrastructure
        return True  # Most altcoin large transactions are trading activity
    
    return is_relevant


def fetch_erc20_transfers(contract_address, sort="desc", start_block: int = 0, end_block: int = 99999999, page: int | None = None, offset: int | None = None):
    
    url = "https://api.etherscan.io/v2/api"
    
    # Key rotation for better throughput and failover
    api_key = random.choice(ETHERSCAN_API_KEYS)
    
    params = {
        "chainid": 1,  # Ethereum mainnet
        "module": "account",
        "action": "tokentx",
        "contractaddress": contract_address,
        "startblock": start_block,
        "endblock": end_block,
        "sort": sort,
        "apikey": api_key
    }
    if page is not None:
        params["page"] = page
    if offset is not None:
        params["offset"] = offset
    # Robust fetch with retries, key rotation, and backoff
    max_attempts = 4
    backoff = 1.5
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            safe_print(f"\n📡 Fetching ERC-20 transfers for contract: {contract_address}")
            safe_print(f"Full URL: {url}?{'&'.join([f'{k}={v}' for k,v in params.items()])}")

            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()

            status = data.get("status")
            message = data.get("message", "")
            result = data.get("result", [])

            # v2 "No transactions found" is normal empty result
            if status == "0" and message == "No transactions found":
                return []

            if status == "1" and isinstance(result, list):
                transfers = result
                safe_print(f"✅ Found {len(transfers)} transfers")
                if transfers:
                    sample = transfers[0]
                    if isinstance(sample, dict):
                        safe_print(f"Sample transfer value: {sample.get('value', 'N/A')}")
                        safe_print(f"Sample transfer block: {sample.get('blockNumber', 'N/A')}")
                return transfers

            # Handle rate limits or generic NOTOK by rotating key and retrying
            safe_print(f"❌ Etherscan API error: {message or 'Unknown'}")
            safe_print(f"Full response: {data}")
            # rotate key for next attempt
            params["apikey"] = random.choice(ETHERSCAN_API_KEYS)
            time.sleep(backoff * attempt)
            continue

        except requests.RequestException as e:
            last_error = e
            safe_print(f"❌ Error fetching transfers (attempt {attempt}/{max_attempts}): {e}")
            log_error(str(e))
            # rotate key and backoff
            params["apikey"] = random.choice(ETHERSCAN_API_KEYS)
            time.sleep(backoff * attempt)
            continue
        except Exception as e:
            last_error = e
            error_msg = f"❌ Error fetching transfers: {str(e)}"
            safe_print(error_msg)
            log_error(error_msg)
            return []

    # If all attempts failed
    if last_error:
        log_error(f"Etherscan fetch failed after retries: {last_error}")
    return []


# In ethereum.py

def print_new_erc20_transfers():
    """Continuously poll Etherscan for ERC-20 transfers (runs in its own thread)."""
    from config.settings import shutdown_flag
    safe_print("✅ Ethereum Etherscan monitor started (60s interval)")

    # Always seed block tracker to current tip — never process historical backlog
    tip = 0
    for _attempt in range(3):
        try:
            url = "https://api.etherscan.io/v2/api"
            api_key = random.choice(ETHERSCAN_API_KEYS)
            resp = requests.get(url, params={
                "chainid": 1, "module": "proxy",
                "action": "eth_blockNumber", "apikey": api_key
            }, timeout=10).json()
            tip = int(resp.get("result", "0x0"), 16)
            if tip > 0:
                break
        except Exception:
            time.sleep(1)

    if tip > 0:
        for symbol in TOKENS_TO_MONITOR:
            last_processed_block[symbol] = tip
        safe_print(f"   Ethereum tip: block {tip} (starting from live)")
    else:
        safe_print("   WARNING: Could not fetch Ethereum tip — will use Etherscan's latest only")

    while not shutdown_flag.is_set():
        try:
            _poll_erc20_transfers_once()
        except Exception as e:
            safe_print(f"Ethereum poll error: {e}")
        shutdown_flag.wait(timeout=60)


# ---------------------------------------------------------------------------
# Alchemy-based Ethereum ERC-20 transfer fetching (primary path)
# ---------------------------------------------------------------------------

# Track last scanned block for the Alchemy path
_alchemy_last_block: int | None = None

# Pre-built contract lists (avoid rebuilding every cycle)
_ETH_CONTRACT_ADDRESSES = [info["contract"] for info in TOKENS_TO_MONITOR.values()]
_ETH_CONTRACT_MAP = {
    info["contract"].lower(): (symbol, info["decimals"])
    for symbol, info in TOKENS_TO_MONITOR.items()
}


def _alchemy_fetch_eth_transfers(from_hex: str, to_hex: str, contract_addresses: list) -> list | None:
    """Fetch ERC-20 transfers via Alchemy alchemy_getAssetTransfers.

    Alchemy caps contractAddresses at ~5 per call, so we batch.
    Returns partial results if some batches fail (resilient).
    Only returns None if ALL batches fail.
    """
    try:
        from utils.alchemy_rpc import fetch_asset_transfers
        BATCH_SIZE = 5
        all_results: list = []
        failures = 0

        for i in range(0, len(contract_addresses), BATCH_SIZE):
            batch = contract_addresses[i : i + BATCH_SIZE]
            transfers = fetch_asset_transfers(
                blockchain='ethereum',
                from_block=from_hex,
                to_block=to_hex,
                contract_addresses=batch,
                category=["erc20"],
            )
            if transfers:
                all_results.extend(transfers)
            else:
                failures += 1

        total_batches = (len(contract_addresses) + BATCH_SIZE - 1) // BATCH_SIZE
        if failures == total_batches:
            return None  # All batches failed — trigger Etherscan fallback
        return all_results  # Return whatever we got (even empty list = success with no transfers)
    except Exception as e:
        eth_logger.warning(f"Alchemy Ethereum transfers error: {e}")
        return None


def _process_alchemy_eth_transfer(tx: dict, contract_map: dict) -> dict | None:
    """Process an Alchemy alchemy_getAssetTransfers result into an event."""
    raw_contract = (tx.get("rawContract") or {})
    contract_addr = raw_contract.get("address", "").lower()
    symbol_info = contract_map.get(contract_addr)
    if not symbol_info:
        return None

    symbol, decimals = symbol_info
    price = TOKEN_PRICES.get(symbol, 0)
    if price == 0:
        return None

    value = tx.get("value")
    if value is None:
        raw_hex = raw_contract.get("value", "0x0")
        try:
            raw_int = int(raw_hex, 16)
            value = raw_int / (10 ** decimals)
        except (ValueError, TypeError):
            return None

    usd_value = float(value) * price
    if usd_value < GLOBAL_USD_THRESHOLD:
        return None

    return {
        "blockchain": "ethereum",
        "tx_hash": tx.get("hash", ""),
        "from": tx.get("from", ""),
        "to": tx.get("to", ""),
        "symbol": symbol,
        "amount": float(value),
        "usd_value": usd_value,
        "estimated_usd": usd_value,
        "timestamp": time.time(),
        "source": "ethereum_alchemy",
        "block_num": tx.get("blockNum", ""),
    }


def _classify_and_store_eth(event: dict):
    """Classify an Ethereum Alchemy event and route through dedup pipeline."""
    from_addr = event.get('from', '')
    to_addr = event.get('to', '')
    symbol = event.get('symbol', '')

    # Whale-relevance filter for major tokens
    if not _is_whale_relevant_transaction(from_addr, to_addr, symbol):
        return

    # Process through the universal classifier
    classification = 'TRANSFER'
    confidence = 0.0
    enriched_transaction = None
    try:
        from utils.classification_final import process_and_enrich_transaction
        enriched_transaction = process_and_enrich_transaction(event)
        if enriched_transaction:
            if isinstance(enriched_transaction, dict):
                classification = enriched_transaction.get('classification', 'TRANSFER')
                confidence = enriched_transaction.get('confidence', 0.0)
            elif hasattr(enriched_transaction, 'classification'):
                classification = enriched_transaction.classification.value if hasattr(enriched_transaction.classification, 'value') else str(enriched_transaction.classification)
                confidence = getattr(enriched_transaction, 'confidence', 0.0)
    except Exception:
        pass

    event['classification'] = classification.upper()
    if 'usd_value' not in event:
        event['usd_value'] = event.get('estimated_usd', 0)

    if not handle_event(event):
        return

    # Update counters
    if classification.upper() in ("BUY", "MODERATE_BUY", "BUY_MODERATE"):
        etherscan_buy_counts[symbol] += 1
    elif classification.upper() in ("SELL", "MODERATE_SELL", "SELL_MODERATE"):
        etherscan_sell_counts[symbol] += 1

    estimated_usd = event.get('usd_value', event.get('estimated_usd', 0))
    whale_indicator = ""
    if isinstance(enriched_transaction, dict) and enriched_transaction.get('is_whale_transaction'):
        whale_indicator = " 🐋"

    safe_print(f"\n[{symbol} | ${estimated_usd:,.2f} USD] Alchemy | Tx {event['tx_hash'][:24]}...{whale_indicator}")
    safe_print(f"  From: {from_addr[:24]}...")
    safe_print(f"  To:   {to_addr[:24]}...")
    safe_print(f"  Amount: {event['amount']:,.2f} {symbol} (~${estimated_usd:,.2f} USD)")
    safe_print(f"  Classification: {classification.upper()} (confidence: {confidence:.2f})")

    record_transfer(symbol, event['amount'], from_addr, to_addr, event['tx_hash'])

    # Persist to Supabase
    if enriched_transaction:
        try:
            from utils.supabase_writer import store_transaction
            classification_data = {
                'classification': classification.upper() if classification else 'TRANSFER',
                'confidence': confidence,
                'whale_score': enriched_transaction.get('whale_score', 0.0) if isinstance(enriched_transaction, dict) else 0.0,
                'reasoning': enriched_transaction.get('reasoning', '') if isinstance(enriched_transaction, dict) else '',
            }
            store_transaction(event, classification_data)
        except Exception as e:
            safe_print(f"  Supabase write error: {e}")


def _poll_alchemy_transfers_once() -> bool:
    """Try to poll Ethereum ERC-20 transfers via Alchemy.

    Returns True if Alchemy succeeded, False if we should fall back to Etherscan.
    Always advances the block pointer to prevent re-scanning stale ranges.
    """
    global _alchemy_last_block

    try:
        from utils.alchemy_rpc import fetch_eth_block_number
        tip = fetch_eth_block_number('ethereum')
    except Exception:
        return False

    if tip is None:
        return False

    if _alchemy_last_block is None:
        _alchemy_last_block = tip
        return True  # First run — seed the pointer, skip processing

    if tip <= _alchemy_last_block:
        return True  # No new blocks

    # Ethereum ~12s block time, 60s poll → ~5 blocks. Cap at 20 to avoid huge scans.
    scan_from = max(_alchemy_last_block + 1, tip - 20)
    from_hex = hex(scan_from)
    to_hex = hex(tip)
    blocks_scanned = tip - scan_from + 1

    transfers = _alchemy_fetch_eth_transfers(from_hex, to_hex, _ETH_CONTRACT_ADDRESSES)
    if transfers is None:
        # Alchemy failed — still advance pointer so we don't re-scan old blocks
        _alchemy_last_block = tip
        return False

    seen_hashes: set = set()
    processed = 0
    for tx in transfers:
        event = _process_alchemy_eth_transfer(tx, _ETH_CONTRACT_MAP)
        if event and event['tx_hash'] not in seen_hashes:
            seen_hashes.add(event['tx_hash'])
            # Fetch receipt for high-value txs
            if event.get('usd_value', 0) >= 50_000:
                try:
                    from utils.alchemy_rpc import fetch_evm_receipt
                    receipt = fetch_evm_receipt(event['tx_hash'], 'ethereum')
                    if receipt:
                        event['receipt'] = receipt
                except Exception:
                    pass
            _classify_and_store_eth(event)
            processed += 1

    if processed > 0:
        safe_print(f"  Alchemy Ethereum transfers: {processed} whale tx in {blocks_scanned} blocks")

    _alchemy_last_block = tip
    return True


_alchemy_consecutive_failures = 0

def _poll_erc20_transfers_once():
    """Single polling cycle for ERC-20 transfers.

    Primary: Alchemy alchemy_getAssetTransfers (batched, all tokens).
    Fallback: Etherscan per-token polling (only after 3 consecutive Alchemy failures).
    """
    global _alchemy_consecutive_failures

    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    safe_print(f"\n[{current_time}] 🔍 Checking ERC-20 transfers...")

    if _poll_alchemy_transfers_once():
        _alchemy_consecutive_failures = 0
        return

    _alchemy_consecutive_failures += 1
    safe_print(f"  Alchemy Ethereum failed ({_alchemy_consecutive_failures}/3)")

    # Only fall back to Etherscan after 3 consecutive failures
    # Single blips shouldn't trigger a 60s Etherscan crawl
    if _alchemy_consecutive_failures >= 3:
        safe_print("  Alchemy down 3 cycles — using Etherscan fallback")
        _poll_etherscan_transfers_once()
        _alchemy_consecutive_failures = 0


def _poll_etherscan_transfers_once():
    """Etherscan-based per-token polling (fallback path)."""
    global last_processed_block

    transactions_processed = 0

    token_items = list(TOKENS_TO_MONITOR.items())
    random.shuffle(token_items)
    for symbol, info in token_items:
        contract = info["contract"]
        decimals = info["decimals"]
        price = TOKEN_PRICES.get(symbol, 0)

        if price == 0:
            safe_print(f"Skipping {symbol} - no price data")
            continue

        # Rate-limit between tokens to avoid Etherscan 429s (3 keys × 5 req/s = 15 req/s max)
        time.sleep(0.3)

        # Rolling start block per token symbol to avoid reprocessing
        start_block = last_processed_block.get(symbol, 0)
        transfers = fetch_erc20_transfers(contract, sort="desc", start_block=start_block)
        if not transfers:
            continue

        new_transfers = []
        for tx in transfers:
            block_num = int(tx["blockNumber"])
            if block_num <= last_processed_block.get(symbol, 0):
                break
            new_transfers.append(tx)

        # Always advance the block pointer even if we cap how many we print
        if new_transfers:
            highest_block = max(int(t["blockNumber"]) for t in new_transfers)
            last_processed_block[symbol] = max(last_processed_block.get(symbol, 0), highest_block)

        MAX_PER_TOKEN_PER_CYCLE = 10
        if len(new_transfers) > MAX_PER_TOKEN_PER_CYCLE:
            new_transfers = new_transfers[:MAX_PER_TOKEN_PER_CYCLE]

        for tx in reversed(new_transfers):
            try:
                raw_value = int(tx["value"])
                token_amount = raw_value / (10 ** decimals)
                estimated_usd = token_amount * price

                if estimated_usd >= GLOBAL_USD_THRESHOLD:
                    from_addr = tx["from"]
                    to_addr = tx["to"]
                    tx_hash = tx["hash"]

                    if not _is_whale_relevant_transaction(from_addr, to_addr, symbol):
                        continue

                    event = {
                        "blockchain": "ethereum",
                        "tx_hash": tx_hash,
                        "from": from_addr,
                        "to": to_addr,
                        "symbol": symbol,
                        "amount": token_amount,
                        "estimated_usd": estimated_usd,
                        "block_number": int(tx["blockNumber"])
                    }

                    # Fetch full receipt from Alchemy for $50k+ transactions
                    if estimated_usd >= 50_000:
                        try:
                            from utils.alchemy_rpc import fetch_evm_receipt
                            receipt = fetch_evm_receipt(tx_hash, 'ethereum')
                            if receipt:
                                event['receipt'] = receipt
                        except Exception:
                            pass

                    from utils.classification_final import process_and_enrich_transaction

                    enriched_transaction = process_and_enrich_transaction(event)

                    classification = 'TRANSFER'
                    confidence = 0.0
                    if enriched_transaction:
                        if isinstance(enriched_transaction, dict):
                            classification = enriched_transaction.get('classification', 'TRANSFER')
                            confidence = enriched_transaction.get('confidence', 0.0)
                        elif hasattr(enriched_transaction, 'classification'):
                            classification = enriched_transaction.classification.value if hasattr(enriched_transaction.classification, 'value') else str(enriched_transaction.classification)
                            confidence = getattr(enriched_transaction, 'confidence', 0.0)

                    event['classification'] = classification.upper()
                    event['usd_value'] = estimated_usd
                    handle_event(event)

                    if enriched_transaction:
                        block_number = int(tx["blockNumber"])
                        timestamp = int(tx.get("timeStamp", "0"))

                        transactions_processed += 1

                        if classification.upper() in ("BUY", "MODERATE_BUY", "BUY_MODERATE"):
                            etherscan_buy_counts[symbol] += 1
                        elif classification.upper() in ("SELL", "MODERATE_SELL", "SELL_MODERATE"):
                            etherscan_sell_counts[symbol] += 1

                        ts_val = int(tx.get("timeStamp", "0"))
                        human_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts_val)) if ts_val else "Unknown"

                        whale_indicator = " 🐋" if isinstance(enriched_transaction, dict) and enriched_transaction.get('is_whale_transaction') else ""
                        safe_print(f"\n[{symbol} | ${estimated_usd:,.2f} USD] Block {tx['blockNumber']} | Tx {tx_hash}{whale_indicator}")
                        safe_print(f"  Time: {human_time}")
                        safe_print(f"  From: {from_addr}")
                        safe_print(f"  To:   {to_addr}")
                        safe_print(f"  Amount: {token_amount:,.2f} {symbol} (~${estimated_usd:,.2f} USD)")
                        safe_print(f"  Classification: {classification.upper()} (confidence: {confidence:.2f})")

                        if isinstance(enriched_transaction, dict) and enriched_transaction.get('whale_classification'):
                            safe_print(f"  Whale Analysis: {enriched_transaction['whale_classification']}")

                        record_transfer(symbol, token_amount, from_addr, to_addr, tx_hash)

                    if enriched_transaction:
                        try:
                            from utils.supabase_writer import store_transaction
                            classification_data = {
                                'classification': classification.upper() if classification else 'TRANSFER',
                                'confidence': confidence,
                                'whale_score': getattr(enriched_transaction, 'final_whale_score', 0.0) if hasattr(enriched_transaction, 'final_whale_score') else (enriched_transaction.get('whale_score', 0.0) if isinstance(enriched_transaction, dict) else 0.0),
                                'reasoning': getattr(enriched_transaction, 'master_classifier_reasoning', '') if hasattr(enriched_transaction, 'master_classifier_reasoning') else (enriched_transaction.get('reasoning', '') if isinstance(enriched_transaction, dict) else ''),
                            }
                            store_transaction(event, classification_data)
                        except Exception as e:
                            safe_print(f"  Supabase write error: {e}")

            except Exception as e:
                error_msg = f"Error processing {symbol} transfer: {str(e)}"
                safe_print(error_msg)
                log_error(error_msg)
                continue


# chains/ethereum.py
# Add this at the end of the file or update the existing function

def test_etherscan_connection():
    """Test Etherscan API connection"""
    url = "https://api.etherscan.io/v2/api"
    
    # Test with first key
    api_key = ETHERSCAN_API_KEYS[0]
    
    params = {
        "chainid": 1,  # Ethereum mainnet
        "module": "stats",
        "action": "ethsupply",
        "apikey": api_key
    }
    try:
        safe_print("Testing Etherscan API connection...")
        r = requests.get(url, params=params, timeout=20)
        data = r.json()
        if data.get("status") == "1":
            safe_print("✅ Etherscan API connection successful")
            return True
        else:
            safe_print(f"❌ Etherscan API error: {data.get('message', 'No message')}")
            return False
    except Exception as e:
        error_msg = f"❌ Error connecting to Etherscan: {e}"
        safe_print(error_msg)
        log_error(error_msg)
        return False