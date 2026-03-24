from __future__ import annotations
"""
Arbitrum One whale transaction monitor.

Primary: Alchemy alchemy_getAssetTransfers (single call covers all tokens).
Polls every 60 seconds.
"""

import time
import logging

from config.settings import (
    GLOBAL_USD_THRESHOLD,
    shutdown_flag,
)
from data.tokens import ARBITRUM_TOKENS_TO_MONITOR, TOKEN_PRICES
from utils.base_helpers import safe_print

logger = logging.getLogger(__name__)

POLL_INTERVAL = 60

# --- Known Arbitrum DEX / CEX / DeFi addresses for classification ---
ARBITRUM_DEX_ADDRESSES = {
    '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45',  # Uniswap V3 Router
    '0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad',  # Uniswap Universal Router
    '0xe592427a0aece92de3edee1f18e0157c05861564',  # Uniswap V3 Router 2
    '0x1b02da8cb0d097eb8d57a175b88c7d8b47997506',  # SushiSwap Router
    '0xdef1c0ded9bec7f1a1670819833240f027b25eff',  # 0x Exchange Proxy
    '0x1111111254eeb25477b68fb85ed929f73a960582',  # 1inch V5 Router
    '0x111111125421ca6dc452d289314280a0f8842a65',  # 1inch V6 Router
    '0xba12222222228d8ba445958a75a0704d566bf2c8',  # Balancer V2 Vault
    '0xc873fecbd354f5a56e00e710b90ef4201db2448d',  # Camelot Router V2
    '0x6131b5fae19ea4f9d964eac0408e4408b66337b5',  # Kyber Network Router
}
ARBITRUM_CEX_ADDRESSES = {
    '0x28c6c06298d514db089934071355e5743bf21d60',  # Binance
    '0xb38e8c17e38363af6ebdcb3dae12e0243582891d',  # Binance 2
    '0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43',  # Coinbase
    '0x6cc5f688a315f3dc28a7781717a9a798a59fda7b',  # OKX
    '0xf89d7b9c864f589bbf53a82105107622b35eaa40',  # Bybit
    '0x2910543af39aba0cd09dbb2d50200b3e800a63d2',  # Kraken
    '0x1522900b6dafac587d499a862861c0869be6e428',  # KuCoin
}
ARBITRUM_DEFI = {
    '0x794a61358d6845594f94dc1db02a252b5b4814ad',  # Aave V3 Pool
    '0xa97684ead0e402dc232d5a977953df7ecbab3cdb',  # Aave V3 Pool Addresses Provider
    '0x489ee077994b6658eafa855c308275ead8097c4a',  # GMX Vault
    '0xc8ee91a54287db53897056e12d9819156d3822fb',  # GMX GMX Staking
    '0x09f77e8a13de9a35a7231028187e9fd5db8a2acb',  # GMX GLP Manager
    '0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a',  # Arbitrum Bridge
    '0x4dbd4fc535ac27206064b68ffcf827b0a60bab3f',  # Arbitrum Delayed Inbox
}


def _classify_arbitrum_transfer(from_addr: str, to_addr: str) -> str:
    """Classify an Arbitrum transfer using known addresses."""
    from_addr = from_addr.lower()
    to_addr = to_addr.lower()

    from_is_cex = from_addr in ARBITRUM_CEX_ADDRESSES
    to_is_cex = to_addr in ARBITRUM_CEX_ADDRESSES
    from_is_dex = from_addr in ARBITRUM_DEX_ADDRESSES
    to_is_dex = to_addr in ARBITRUM_DEX_ADDRESSES

    if from_is_cex and not to_is_cex:
        return 'BUY'
    if to_is_cex and not from_is_cex:
        return 'SELL'
    if from_is_dex and not to_is_dex:
        return 'BUY'
    if to_is_dex and not from_is_dex:
        return 'SELL'
    return 'TRANSFER'


def _alchemy_get_block_number() -> int | None:
    """Get latest Arbitrum block number via Alchemy."""
    try:
        from utils.alchemy_rpc import get_alchemy_rpc, _rpc_call
        rpc_url = get_alchemy_rpc('arbitrum')
        if not rpc_url:
            return None
        result = _rpc_call(rpc_url, 'eth_blockNumber', [], cu_cost=10)
        if result:
            return int(result, 16)
    except Exception as e:
        logger.warning(f"Alchemy Arbitrum block number error: {e}")
    return None


def _alchemy_fetch_transfers(from_hex: str, to_hex: str, contract_addresses: list) -> list | None:
    """Fetch ERC-20 transfers via Alchemy alchemy_getAssetTransfers for Arbitrum."""
    try:
        from utils.alchemy_rpc import fetch_asset_transfers
        BATCH_SIZE = 5
        all_results: list = []

        for i in range(0, len(contract_addresses), BATCH_SIZE):
            batch = contract_addresses[i : i + BATCH_SIZE]
            transfers = fetch_asset_transfers(
                blockchain='arbitrum',
                from_block=from_hex,
                to_block=to_hex,
                contract_addresses=batch,
                category=["erc20"],
            )
            if transfers:
                all_results.extend(transfers)

        return all_results if all_results else None
    except Exception as e:
        logger.warning(f"Alchemy Arbitrum transfers error: {e}")
        return None


def _process_alchemy_transfer(tx: dict, contract_map: dict) -> dict | None:
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
        "blockchain": "arbitrum",
        "tx_hash": tx.get("hash", ""),
        "from": tx.get("from", ""),
        "to": tx.get("to", ""),
        "symbol": symbol,
        "amount": float(value),
        "usd_value": usd_value,
        "timestamp": time.time(),
        "source": "arbitrum_alchemy",
        "block_num": tx.get("blockNum", ""),
    }


def _classify_and_store(event: dict):
    """Classify an Arbitrum event and store via dedup pipeline."""
    from utils.dedup import handle_event
    from config.settings import arbitrum_buy_counts, arbitrum_sell_counts

    classification = _classify_arbitrum_transfer(event.get('from', ''), event.get('to', ''))

    # Fall back to WhaleIntelligenceEngine for unknown addresses
    if classification == 'TRANSFER':
        try:
            from utils.classification_final import process_and_enrich_transaction
            enriched = process_and_enrich_transaction(event)
            if enriched and isinstance(enriched, dict):
                classification = enriched.get('classification', 'TRANSFER').upper()
        except Exception:
            pass

    event['classification'] = classification

    if not handle_event(event):
        return

    if 'BUY' in classification:
        arbitrum_buy_counts[event['symbol']] += 1
    elif 'SELL' in classification:
        arbitrum_sell_counts[event['symbol']] += 1

    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    safe_print(
        f"\n[ARBITRUM - {event['symbol']} | ${event['usd_value']:,.0f} USD] "
        f"{classification} | Block {event.get('block_num', '?')}"
    )
    safe_print(f"  Time : {current_time}")
    safe_print(f"  TX   : {event['tx_hash'][:24]}...")
    safe_print(f"  From : {event.get('from', '?')[:24]}...")
    safe_print(f"  To   : {event.get('to', '?')[:24]}...")


def print_new_arbitrum_transfers():
    """Continuously polls for large Arbitrum ERC-20 transfers via Alchemy."""
    safe_print("Arbitrum monitor started (60s interval)")

    contract_addresses = [
        info["contract"] for info in ARBITRUM_TOKENS_TO_MONITOR.values()
    ]
    contract_map = {
        info["contract"].lower(): (symbol, info["decimals"])
        for symbol, info in ARBITRUM_TOKENS_TO_MONITOR.items()
    }

    current_block = _alchemy_get_block_number()
    if current_block:
        safe_print(f"   Arbitrum tip: block {current_block}")
        last_block = current_block
    else:
        safe_print("   Arbitrum: could not fetch initial block, will retry")
        last_block = None

    seen_hashes = set()

    while not shutdown_flag.is_set():
        try:
            tip = _alchemy_get_block_number()
            if tip is None:
                safe_print("Arbitrum: block fetch failed, retrying...")
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            if last_block is None:
                last_block = tip
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            if tip <= last_block:
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            # Arbitrum produces blocks at ~0.25s → many blocks per cycle
            # Cap scan range to avoid huge queries
            scan_from = max(last_block + 1, tip - 500)
            from_hex = hex(scan_from)
            to_hex = hex(tip)
            blocks_scanned = tip - scan_from + 1

            safe_print(f"  Arbitrum: scanning blocks {scan_from}-{tip} ({blocks_scanned} blocks)")

            transfers = _alchemy_fetch_transfers(from_hex, to_hex, contract_addresses)

            if transfers is not None:
                processed = 0
                for tx in transfers:
                    event = _process_alchemy_transfer(tx, contract_map)
                    if event and event['tx_hash'] not in seen_hashes:
                        seen_hashes.add(event['tx_hash'])
                        _classify_and_store(event)
                        processed += 1
                if processed > 0:
                    safe_print(f"  Arbitrum: {processed} ERC-20 whale tx in {blocks_scanned} blocks")

            # Also scan native ETH transfers on Arbitrum
            try:
                from utils.alchemy_rpc import fetch_asset_transfers as _fetch_at
                eth_transfers = _fetch_at('arbitrum', from_hex, to_hex, category=['external'])
                if eth_transfers:
                    eth_price = TOKEN_PRICES.get('WETH', TOKEN_PRICES.get('ETH', 2000))
                    eth_processed = 0
                    for tx in eth_transfers:
                        val = float(tx.get('value', 0) or 0)
                        usd_value = val * eth_price
                        if usd_value < GLOBAL_USD_THRESHOLD:
                            continue
                        tx_hash = tx.get('hash', '')
                        if tx_hash in seen_hashes:
                            continue
                        seen_hashes.add(tx_hash)
                        event = {
                            "blockchain": "arbitrum",
                            "tx_hash": tx_hash,
                            "from": tx.get("from", ""),
                            "to": tx.get("to", ""),
                            "symbol": "ETH",
                            "amount": val,
                            "usd_value": usd_value,
                            "timestamp": time.time(),
                            "source": "arbitrum_alchemy_native",
                            "block_num": tx.get("blockNum", ""),
                        }
                        _classify_and_store(event)
                        eth_processed += 1
                    if eth_processed > 0:
                        safe_print(f"  Arbitrum: {eth_processed} native ETH whale tx in {blocks_scanned} blocks")
            except Exception as e:
                logger.warning(f"Arbitrum native ETH scan error: {e}")

            last_block = tip

            if len(seen_hashes) > 10000:
                seen_hashes.clear()

        except Exception as e:
            safe_print(f"  Arbitrum poll error: {e}")
            logger.warning(f"Arbitrum poll error: {e}")

        shutdown_flag.wait(timeout=POLL_INTERVAL)


def test_arbitrum_connection():
    """Quick check that Arbitrum Alchemy API is reachable."""
    block = _alchemy_get_block_number()
    if block:
        safe_print(f"Arbitrum connection OK (block {block})")
        return True
    safe_print("Arbitrum connection failed")
    return False
