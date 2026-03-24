from __future__ import annotations
"""
Base L2 whale transaction monitor.

Primary: Alchemy alchemy_getAssetTransfers (single call covers all tokens).
Polls every 60 seconds.
"""

import time
import logging

from config.settings import (
    GLOBAL_USD_THRESHOLD,
    shutdown_flag,
)
from data.tokens import BASE_TOKENS_TO_MONITOR, TOKEN_PRICES
from utils.base_helpers import safe_print

logger = logging.getLogger(__name__)

POLL_INTERVAL = 60

# --- Known Base DEX / CEX / DeFi addresses for classification ---
BASE_DEX_ADDRESSES = {
    '0x2626664c2603336e57b271c5c0b26f421741e481',  # Uniswap V3 Router (Base)
    '0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad',  # Uniswap Universal Router
    '0xcf77a3ba9a5ca399b7c97c74d54e5b1beb874e43',  # Aerodrome Router
    '0x6cb442acf35158d5eda88fe602221b67b400be3e',  # Aerodrome V2 Router
    '0xdef1c0ded9bec7f1a1670819833240f027b25eff',  # 0x Exchange Proxy
    '0x1111111254eeb25477b68fb85ed929f73a960582',  # 1inch V5 Router
    '0x111111125421ca6dc452d289314280a0f8842a65',  # 1inch V6 Router
    '0xba12222222228d8ba445958a75a0704d566bf2c8',  # Balancer V2 Vault
    '0x327df1e6de05895d2ab08513aadd9313fe505d86',  # BaseSwap Router
    '0x8c1a3cf8f83074169fe5d7ad50b978e1cd6b37c7',  # SushiSwap V3 Router
}
BASE_CEX_ADDRESSES = {
    '0x3304e22ddaa22bcdc5fca2269b418046ae7b566a',  # Coinbase Base Bridge
    '0xec8672b9f02fcb3af4cb505146ff2da10e54ff98',  # Coinbase Commerce
    '0x28c6c06298d514db089934071355e5743bf21d60',  # Binance
    '0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43',  # Coinbase
    '0x6cc5f688a315f3dc28a7781717a9a798a59fda7b',  # OKX
    '0xf89d7b9c864f589bbf53a82105107622b35eaa40',  # Bybit
}
BASE_DEFI = {
    '0xa238dd80c259a72e81d7e4664a9801593f98d1c5',  # Aave V3 Pool (Base)
    '0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22',  # Compound V3 cUSDC (Base)
    '0x3154cf16ccdb4c6d922629664174b904d80f2c35',  # Base Bridge
    '0x49048044d57e1c92a77f79988d21fa8faf74e97e',  # Base Portal
}


def _classify_base_transfer(from_addr: str, to_addr: str) -> str:
    """Classify a Base transfer using known addresses."""
    from_addr = from_addr.lower()
    to_addr = to_addr.lower()

    from_is_cex = from_addr in BASE_CEX_ADDRESSES
    to_is_cex = to_addr in BASE_CEX_ADDRESSES
    from_is_dex = from_addr in BASE_DEX_ADDRESSES
    to_is_dex = to_addr in BASE_DEX_ADDRESSES

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
    """Get latest Base block number via Alchemy."""
    try:
        from utils.alchemy_rpc import get_alchemy_rpc, _rpc_call
        rpc_url = get_alchemy_rpc('base')
        if not rpc_url:
            return None
        result = _rpc_call(rpc_url, 'eth_blockNumber', [], cu_cost=10)
        if result:
            return int(result, 16)
    except Exception as e:
        logger.warning(f"Alchemy Base block number error: {e}")
    return None


def _alchemy_fetch_transfers(from_hex: str, to_hex: str, contract_addresses: list) -> list | None:
    """Fetch ERC-20 transfers via Alchemy alchemy_getAssetTransfers for Base."""
    try:
        from utils.alchemy_rpc import fetch_asset_transfers
        BATCH_SIZE = 5
        all_results: list = []

        for i in range(0, len(contract_addresses), BATCH_SIZE):
            batch = contract_addresses[i : i + BATCH_SIZE]
            transfers = fetch_asset_transfers(
                blockchain='base',
                from_block=from_hex,
                to_block=to_hex,
                contract_addresses=batch,
                category=["erc20"],
            )
            if transfers:
                all_results.extend(transfers)

        return all_results if all_results else None
    except Exception as e:
        logger.warning(f"Alchemy Base transfers error: {e}")
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
        "blockchain": "base",
        "tx_hash": tx.get("hash", ""),
        "from": tx.get("from", ""),
        "to": tx.get("to", ""),
        "symbol": symbol,
        "amount": float(value),
        "usd_value": usd_value,
        "timestamp": time.time(),
        "source": "base_alchemy",
        "block_num": tx.get("blockNum", ""),
    }


def _classify_and_store(event: dict):
    """Classify a Base event and store via dedup pipeline."""
    from utils.dedup import handle_event
    from config.settings import base_buy_counts, base_sell_counts

    classification = _classify_base_transfer(event.get('from', ''), event.get('to', ''))

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
        base_buy_counts[event['symbol']] += 1
    elif 'SELL' in classification:
        base_sell_counts[event['symbol']] += 1

    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    safe_print(
        f"\n[BASE - {event['symbol']} | ${event['usd_value']:,.0f} USD] "
        f"{classification} | Block {event.get('block_num', '?')}"
    )
    safe_print(f"  Time : {current_time}")
    safe_print(f"  TX   : {event['tx_hash'][:24]}...")
    safe_print(f"  From : {event.get('from', '?')[:24]}...")
    safe_print(f"  To   : {event.get('to', '?')[:24]}...")


def print_new_base_transfers():
    """Continuously polls for large Base ERC-20 transfers via Alchemy."""
    safe_print("Base monitor started (60s interval)")

    contract_addresses = [
        info["contract"] for info in BASE_TOKENS_TO_MONITOR.values()
    ]
    contract_map = {
        info["contract"].lower(): (symbol, info["decimals"])
        for symbol, info in BASE_TOKENS_TO_MONITOR.items()
    }

    current_block = _alchemy_get_block_number()
    if current_block:
        safe_print(f"   Base tip: block {current_block}")
        last_block = current_block
    else:
        safe_print("   Base: could not fetch initial block, will retry")
        last_block = None

    seen_hashes = set()

    while not shutdown_flag.is_set():
        try:
            tip = _alchemy_get_block_number()
            if tip is None:
                safe_print("Base: block fetch failed, retrying...")
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            if last_block is None:
                last_block = tip
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            if tip <= last_block:
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            # Base produces ~1 block per 2s → ~30 blocks per 60s cycle
            scan_from = max(last_block + 1, tip - 60)
            from_hex = hex(scan_from)
            to_hex = hex(tip)
            blocks_scanned = tip - scan_from + 1

            safe_print(f"  Base: scanning blocks {scan_from}-{tip} ({blocks_scanned} blocks)")

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
                    safe_print(f"  Base: {processed} ERC-20 whale tx in {blocks_scanned} blocks")

            # Also scan native ETH transfers on Base
            try:
                from utils.alchemy_rpc import fetch_asset_transfers as _fetch_at
                eth_transfers = _fetch_at('base', from_hex, to_hex, category=['external'])
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
                            "blockchain": "base",
                            "tx_hash": tx_hash,
                            "from": tx.get("from", ""),
                            "to": tx.get("to", ""),
                            "symbol": "ETH",
                            "amount": val,
                            "usd_value": usd_value,
                            "timestamp": time.time(),
                            "source": "base_alchemy_native",
                            "block_num": tx.get("blockNum", ""),
                        }
                        _classify_and_store(event)
                        eth_processed += 1
                    if eth_processed > 0:
                        safe_print(f"  Base: {eth_processed} native ETH whale tx in {blocks_scanned} blocks")
            except Exception as e:
                logger.warning(f"Base native ETH scan error: {e}")

            last_block = tip

            # Trim seen_hashes to prevent unbounded growth
            if len(seen_hashes) > 10000:
                seen_hashes.clear()

        except Exception as e:
            safe_print(f"  Base poll error: {e}")
            logger.warning(f"Base poll error: {e}")

        shutdown_flag.wait(timeout=POLL_INTERVAL)


def test_base_connection():
    """Quick check that Base Alchemy API is reachable."""
    block = _alchemy_get_block_number()
    if block:
        safe_print(f"Base connection OK (block {block})")
        return True
    safe_print("Base connection failed")
    return False
