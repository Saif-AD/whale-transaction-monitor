#!/usr/bin/env python3
"""
Reclassify TRANSFER transactions using improved whale perspective logic.

Fetches all TRANSFER-classified whale_transactions, re-evaluates them using
the updated _determine_whale_perspective and classify_from_whale_perspective
logic, and updates any that should be BUY or SELL.

Designed to run for several hours over 136k+ transactions.
Progress is printed every batch and the script can be safely interrupted.
"""

import sys
import os
import time
from datetime import datetime
from collections import defaultdict

# Prevent BigQuery from initializing on import
os.environ['SKIP_BIGQUERY_INIT'] = '1'

from supabase import create_client
from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
from data.addresses import known_exchange_addresses, DEX_ADDRESSES

_BLOCKCHAIN_ALIASES = {
    'eth': 'ethereum', 'ETH': 'ethereum', 'Ethereum': 'ethereum',
    'ETHEREUM': 'ethereum', 'mainnet': 'ethereum',
    'poly': 'polygon', 'matic': 'polygon', 'POLYGON': 'polygon',
    'BSC': 'bsc', 'Bsc': 'bsc', 'bnb': 'bsc',
    'btc': 'bitcoin', 'BTC': 'bitcoin',
    'sol': 'solana', 'SOL': 'solana',
}

def normalize_blockchain(blockchain):
    if not blockchain:
        return 'ethereum'
    cleaned = blockchain.strip()
    return _BLOCKCHAIN_ALIASES.get(cleaned, cleaned.lower())

BATCH_SIZE = 500
PROGRESS_INTERVAL = 5  # batches between detailed progress prints


def get_supabase():
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


def is_cex_address(addr_type, label, entity_name):
    """Check if address is a CEX using type, label, and entity_name."""
    cex_types = {'CEX', 'CEX Wallet', 'exchange', 'Exchange Wallet'}
    if addr_type in cex_types:
        return True
    searchable = f"{label} {entity_name}".lower()
    cex_names = ['binance', 'coinbase', 'kraken', 'okx', 'gate.io',
                 'kucoin', 'huobi', 'crypto.com', 'bybit', 'gemini',
                 'bitstamp', 'bitfinex', 'bittrex', 'poloniex', 'mexc']
    return any(name in searchable for name in cex_names)


def determine_perspective(sb, from_addr, to_addr, blockchain):
    """Lightweight whale perspective determination for reclassification."""
    blockchain = normalize_blockchain(blockchain)
    addresses_to_check = [a.lower() for a in [from_addr, to_addr] if a]
    
    from_data = None
    to_data = None
    
    if addresses_to_check:
        result = sb.table('addresses') \
            .select('address, address_type, label, entity_name') \
            .in_('address', addresses_to_check) \
            .eq('blockchain', blockchain) \
            .execute()
        for row in (result.data or []):
            addr = row.get('address', '').lower()
            if from_addr and addr == from_addr.lower():
                from_data = row
            elif to_addr and addr == to_addr.lower():
                to_data = row

    from_type = from_data.get('address_type', '') if from_data else ''
    to_type = to_data.get('address_type', '') if to_data else ''
    from_label = from_data.get('label', '') if from_data else ''
    to_label = to_data.get('label', '') if to_data else ''
    from_entity = from_data.get('entity_name', '') if from_data else ''
    to_entity = to_data.get('entity_name', '') if to_data else ''

    from_is_cex = is_cex_address(from_type, from_label, from_entity)
    to_is_cex = is_cex_address(to_type, to_label, to_entity)
    from_is_dex = from_type in ['DEX', 'dex_router', 'DEX Router'] or (from_addr or '').lower() in DEX_ADDRESSES
    to_is_dex = to_type in ['DEX', 'dex_router', 'DEX Router'] or (to_addr or '').lower() in DEX_ADDRESSES

    # Also check hardcoded known exchanges as fallback
    if not from_is_cex and (from_addr or '').lower() in known_exchange_addresses:
        from_is_cex = True
    if not to_is_cex and (to_addr or '').lower() in known_exchange_addresses:
        to_is_cex = True

    if from_is_cex and not to_is_cex:
        return 'CEX', to_addr, from_addr
    elif to_is_cex and not from_is_cex:
        return 'CEX', from_addr, to_addr
    elif from_is_dex and not to_is_dex:
        return 'DEX', to_addr, from_addr
    elif to_is_dex and not from_is_dex:
        return 'DEX', from_addr, to_addr
    elif from_is_cex and to_is_cex:
        return 'CEX_INTERNAL', None, None
    else:
        return 'EOA', from_addr, to_addr


def reclassify(counterparty_type, whale_addr, from_addr, to_addr):
    """Determine BUY/SELL/TRANSFER from the whale's perspective."""
    trade = counterparty_type in {'CEX', 'DEX'}
    
    if not whale_addr:
        return 'TRANSFER'
    
    whale = whale_addr.lower()
    frm = (from_addr or '').lower()
    to = (to_addr or '').lower()
    
    if whale == to:
        return 'BUY' if trade else 'TRANSFER'
    if whale == frm:
        return 'SELL' if trade else 'TRANSFER'
    
    return 'TRANSFER'


def main():
    sb = get_supabase()
    
    # Count total TRANSFER transactions
    count_result = sb.table('whale_transactions') \
        .select('id', count='exact') \
        .eq('classification', 'TRANSFER') \
        .execute()
    total = count_result.count
    
    print("=" * 70)
    print("  TRANSFER RECLASSIFICATION SCRIPT")
    print("=" * 70)
    print(f"  Total TRANSFER transactions to evaluate: {total:,}")
    print(f"  Batch size: {BATCH_SIZE}")
    print(f"  Estimated batches: {(total // BATCH_SIZE) + 1}")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()
    
    stats = defaultdict(int)
    stats['total'] = total
    processed = 0
    updated = 0
    errors = 0
    batch_num = 0
    start_time = time.time()
    last_id = 0
    
    while processed < total:
        batch_num += 1
        
        try:
            # Paginate using id cursor for consistent ordering
            batch = sb.table('whale_transactions') \
                .select('id, from_address, to_address, blockchain, classification, '
                        'whale_address, counterparty_type') \
                .eq('classification', 'TRANSFER') \
                .gt('id', last_id) \
                .order('id') \
                .limit(BATCH_SIZE) \
                .execute()
            
            if not batch.data:
                break
            
            last_id = batch.data[-1]['id']
            updates = []
            
            for tx in batch.data:
                from_addr = tx.get('from_address', '')
                to_addr = tx.get('to_address', '')
                blockchain = tx.get('blockchain', 'ethereum')
                
                if not from_addr or not to_addr:
                    processed += 1
                    continue
                
                try:
                    ct, whale_addr, counterparty_addr = determine_perspective(
                        sb, from_addr, to_addr, blockchain
                    )
                    
                    new_cls = reclassify(ct, whale_addr, from_addr, to_addr)
                    
                    if new_cls != 'TRANSFER':
                        updates.append({
                            'id': tx['id'],
                            'classification': new_cls,
                            'whale_address': whale_addr,
                            'counterparty_address': counterparty_addr,
                            'counterparty_type': ct,
                            'is_cex_transaction': ct == 'CEX'
                        })
                        stats[new_cls] += 1
                    else:
                        stats['STILL_TRANSFER'] += 1
                    
                except Exception as e:
                    errors += 1
                    if errors <= 10:
                        print(f"  [!] Error on tx {tx['id']}: {e}")
                
                processed += 1
            
            # Batch update
            if updates:
                for upd in updates:
                    tx_id = upd.pop('id')
                    sb.table('whale_transactions').update(upd).eq('id', tx_id).execute()
                updated += len(updates)
            
            # Progress reporting
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            eta_seconds = (total - processed) / rate if rate > 0 else 0
            eta_min = eta_seconds / 60
            
            if batch_num % PROGRESS_INTERVAL == 0 or len(batch.data) < BATCH_SIZE:
                print(f"  [{datetime.now().strftime('%H:%M:%S')}] "
                      f"Batch {batch_num} | "
                      f"{processed:,}/{total:,} ({processed/total*100:.1f}%) | "
                      f"Reclassified: {updated:,} | "
                      f"Rate: {rate:.1f}/s | "
                      f"ETA: {eta_min:.0f}min")
            
        except KeyboardInterrupt:
            print(f"\n  Interrupted at batch {batch_num}")
            break
        except Exception as e:
            errors += 1
            print(f"  [!] Batch {batch_num} error: {e}")
            # Reconnect on connection errors
            if 'ConnectionTerminated' in str(e) or 'timeout' in str(e).lower():
                print(f"  [!] Reconnecting to Supabase...")
                time.sleep(5)
                try:
                    sb = get_supabase()
                    print(f"  [!] Reconnected successfully")
                except Exception as e2:
                    print(f"  [!] Reconnect failed: {e2}")
                    time.sleep(10)
            else:
                time.sleep(2)
            continue
    
    # Final report
    elapsed = time.time() - start_time
    print()
    print("=" * 70)
    print("  RECLASSIFICATION COMPLETE")
    print("=" * 70)
    print(f"  Processed:    {processed:,}")
    print(f"  Reclassified: {updated:,} ({updated/max(processed,1)*100:.1f}%)")
    print(f"    -> BUY:     {stats['BUY']:,}")
    print(f"    -> SELL:    {stats['SELL']:,}")
    print(f"  Still TRANSFER: {stats['STILL_TRANSFER']:,}")
    print(f"  Errors:       {errors:,}")
    print(f"  Duration:     {elapsed/60:.1f} minutes")
    print(f"  Rate:         {processed/elapsed:.1f} tx/sec")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted.")
        sys.exit(0)
