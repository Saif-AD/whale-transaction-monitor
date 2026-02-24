#!/usr/bin/env python3
"""
Enrich Supabase addresses table with named entities from public datasets.

Sources:
  1. eth-labels API (94k+ Ethereum addresses with labels/nameTags)
  2. etherscan-labels GitHub (30k Ethereum + BSC + Polygon labels)
  3. Manually curated famous wallets (individuals, institutions, governments)

Safe to re-run (upserts on address+blockchain unique constraint).
"""

import sys
import time
import json
import requests
from datetime import datetime
from collections import defaultdict

from supabase import create_client
from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

BATCH_SIZE = 200

# Label -> address_type mapping
LABEL_TO_TYPE = {
    'binance': 'CEX', 'coinbase': 'CEX', 'kraken': 'CEX', 'kucoin': 'CEX',
    'okx': 'CEX', 'huobi': 'CEX', 'gemini': 'CEX', 'bitstamp': 'CEX',
    'bitfinex': 'CEX', 'bittrex': 'CEX', 'poloniex': 'CEX', 'cex-io': 'CEX',
    'crypto-com': 'CEX', 'bybit': 'CEX', 'mexc': 'CEX', 'gate-io': 'CEX',
    'deribit': 'CEX', 'bilaxy': 'CEX', 'delta-exchange': 'CEX',
    'blofin-exchange': 'CEX', 'binance-charity': 'CEX',

    'uniswap': 'DEX', 'sushiswap': 'DEX', 'balancer': 'DEX', 'curve-fi': 'DEX',
    'bancor': 'DEX', 'dex': 'DEX', '1inch': 'DEX', 'paraswap': 'DEX',
    '0x-protocol': 'DEX',

    'aave': 'DEX', 'compound': 'DEX', 'maker-vault-owner': 'DEX',
    'lido': 'DEX', 'rocket-pool': 'DEX', 'synthetix': 'DEX',
    'yearn': 'DEX', 'convex': 'DEX',

    'mev-bot': 'WHALE', 'airdrop-hunter': 'WHALE', 'yield-farming': 'WHALE',
    'trading': 'WHALE', 'mining': 'WHALE',
}

# Label -> entity_name resolution for CEX labels
LABEL_TO_ENTITY = {
    'binance': 'Binance', 'binance-charity': 'Binance',
    'coinbase': 'Coinbase', 'kraken': 'Kraken', 'kucoin': 'KuCoin',
    'okx': 'OKX', 'huobi': 'Huobi', 'gemini': 'Gemini',
    'bitstamp': 'Bitstamp', 'bitfinex': 'Bitfinex', 'bittrex': 'Bittrex',
    'poloniex': 'Poloniex', 'cex-io': 'CEX.IO', 'crypto-com': 'Crypto.com',
    'bybit': 'Bybit', 'mexc': 'MEXC', 'gate-io': 'Gate.io',
    'deribit': 'Deribit', 'bilaxy': 'Bilaxy',
}

# Label -> category for analysis_tags
LABEL_TO_CATEGORY = {
    'binance': 'exchange', 'coinbase': 'exchange', 'kraken': 'exchange',
    'kucoin': 'exchange', 'okx': 'exchange', 'huobi': 'exchange',
    'gemini': 'exchange', 'bitstamp': 'exchange', 'bitfinex': 'exchange',
    'uniswap': 'protocol', 'sushiswap': 'protocol', 'balancer': 'protocol',
    'aave': 'protocol', 'compound': 'protocol', 'lido': 'protocol',
    'curve-fi': 'protocol', 'synthetix': 'protocol',
    'mev-bot': 'mev', 'trading': 'trader',
    'bridge': 'infrastructure', 'chainlink': 'infrastructure',
    'the-graph': 'infrastructure',
    'blocked': 'watchlist', 'take-action': 'watchlist',
}

FAMOUS_WALLETS = [
    # Crypto Founders
    {'address': '0xd8da6bf26964af9d7eed9e03e53415d37aa96045', 'entity_name': 'Vitalik Buterin', 'label': 'Ethereum Co-Founder', 'address_type': 'WHALE', 'category': 'individual', 'subcategory': 'crypto_founder'},
    {'address': '0x220866b1a2219f40e72f5c628b65d54268ca3a9d', 'entity_name': 'Vitalik Buterin', 'label': 'Vitalik Multisig', 'address_type': 'WHALE', 'category': 'individual', 'subcategory': 'crypto_founder'},
    {'address': '0xab5801a7d398351b8be11c439e05c5b3259aec9b', 'entity_name': 'Vitalik Buterin', 'label': 'Vitalik Old Wallet', 'address_type': 'WHALE', 'category': 'individual', 'subcategory': 'crypto_founder'},
    {'address': '0x3ddfa8ec3052539b6c9549f12cea2c295cff5296', 'entity_name': 'Justin Sun', 'label': 'TRON Founder', 'address_type': 'WHALE', 'category': 'individual', 'subcategory': 'crypto_founder'},
    {'address': '0x176f3dab24a159341c0509bb36b833e7fdd0a132', 'entity_name': 'Justin Sun', 'label': 'Justin Sun 2', 'address_type': 'WHALE', 'category': 'individual', 'subcategory': 'crypto_founder'},
    {'address': '0x6b44ba0a126a2a1a8aa6cd1adeed002e141bcd44', 'entity_name': 'Justin Sun', 'label': 'Justin Sun 3', 'address_type': 'WHALE', 'category': 'individual', 'subcategory': 'crypto_founder'},
    {'address': '0x47ac0fb4f2d84898e4d9e7b4dab3c24507a6d503', 'entity_name': 'Hayden Adams', 'label': 'Uniswap Creator', 'address_type': 'WHALE', 'category': 'individual', 'subcategory': 'crypto_founder'},

    # Public Figures
    {'address': '0x94845333028b1204fbe14e1278fd4adde46b22ce', 'entity_name': 'Donald Trump', 'label': 'Trump Crypto Wallet', 'address_type': 'WHALE', 'category': 'individual', 'subcategory': 'public_figure'},
    {'address': '0xfad95b56ccb5dc26a3f6f06b580e48937c383830', 'entity_name': 'Gary Vaynerchuk', 'label': 'GaryVee NFT Wallet', 'address_type': 'WHALE', 'category': 'individual', 'subcategory': 'public_figure'},

    # Institutions & Funds
    {'address': '0x9845e1909dca337944a0272f1f9f7249833d2d19', 'entity_name': 'Paradigm', 'label': 'Paradigm Fund', 'address_type': 'WHALE', 'category': 'institution', 'subcategory': 'crypto_vc'},
    {'address': '0x0716a17fbaee714f1e6ab0f9d59edbc5f09815c0', 'entity_name': 'Jump Trading', 'label': 'Jump Crypto', 'address_type': 'WHALE', 'category': 'institution', 'subcategory': 'market_maker'},
    {'address': '0xdbf5e9c5206d0db70a90108bf936da60221dc080', 'entity_name': 'Wintermute', 'label': 'Wintermute Trading', 'address_type': 'WHALE', 'category': 'institution', 'subcategory': 'market_maker'},
    {'address': '0x0000006daea1723962647b7e189d311d757fb793', 'entity_name': 'Wintermute', 'label': 'Wintermute MEV', 'address_type': 'WHALE', 'category': 'institution', 'subcategory': 'market_maker'},
    {'address': '0xa7efae728d2936e78bda97dc267687568dd593f3', 'entity_name': 'Cumberland', 'label': 'Cumberland DRW', 'address_type': 'WHALE', 'category': 'institution', 'subcategory': 'market_maker'},
    {'address': '0x1db3439a222c519ab44bb1144fc28167b4fa6ee6', 'entity_name': 'Alameda Research', 'label': 'Alameda Research (Historical)', 'address_type': 'WHALE', 'category': 'institution', 'subcategory': 'hedge_fund'},
    {'address': '0x8f942eced007bd3976927b7958b50df126feecb5', 'entity_name': 'Galaxy Digital', 'label': 'Galaxy Digital', 'address_type': 'WHALE', 'category': 'institution', 'subcategory': 'crypto_vc'},

    # Corporations
    {'address': '0xf977814e90da44bfa03b6295a0616a897441acec', 'entity_name': 'Binance', 'label': 'Binance 8 (Cold)', 'address_type': 'CEX', 'category': 'exchange', 'subcategory': 'tier_1_cex'},
    {'address': '0x21a31ee1afc51d94c2efccaa2092ad1028285549', 'entity_name': 'Robinhood', 'label': 'Robinhood Crypto', 'address_type': 'CEX', 'category': 'exchange', 'subcategory': 'brokerage'},
    {'address': '0xae2fc483527b8ef99eb5d9b44875f005ba1fae13', 'entity_name': 'Genesis Trading', 'label': 'Genesis Trading', 'address_type': 'WHALE', 'category': 'institution', 'subcategory': 'otc_desk'},

    # Stablecoin Issuers
    {'address': '0x5754284f345afc66a98fbb0a0afe71e0f007b949', 'entity_name': 'Tether Treasury', 'label': 'USDT Treasury', 'address_type': 'WHALE', 'category': 'stablecoin', 'subcategory': 'treasury'},
    {'address': '0x55fe002aeff02f77364de339a1292923a15844b8', 'entity_name': 'Circle', 'label': 'USDC Treasury', 'address_type': 'WHALE', 'category': 'stablecoin', 'subcategory': 'treasury'},

    # Governments
    {'address': '0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d', 'entity_name': 'Yuga Labs', 'label': 'BAYC Contract', 'address_type': 'DEX', 'category': 'nft', 'subcategory': 'collection'},

    # Protocol Treasuries
    {'address': '0x1a9c8182c09f50c8318d769245bea52c32be35bc', 'entity_name': 'Uniswap', 'label': 'Uniswap Treasury', 'address_type': 'DEX', 'category': 'protocol', 'subcategory': 'treasury'},
    {'address': '0x25f2226b597e8f9514b3f68f00f494cf4f286491', 'entity_name': 'MakerDAO', 'label': 'MakerDAO Treasury', 'address_type': 'DEX', 'category': 'protocol', 'subcategory': 'treasury'},
    {'address': '0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c', 'entity_name': 'Aave', 'label': 'Aave Treasury', 'address_type': 'DEX', 'category': 'protocol', 'subcategory': 'treasury'},
    {'address': '0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf', 'entity_name': 'Polygon Bridge', 'label': 'Polygon PoS Bridge', 'address_type': 'DEX', 'category': 'infrastructure', 'subcategory': 'bridge'},
    {'address': '0x8eb8a3b98659cce290402893d0123abb75e3ab28', 'entity_name': 'Avalanche Bridge', 'label': 'Avalanche Bridge', 'address_type': 'DEX', 'category': 'infrastructure', 'subcategory': 'bridge'},
    {'address': '0xa3a7b6f88361f48403514059f1f16c8e78d60eec', 'entity_name': 'Arbitrum Bridge', 'label': 'Arbitrum One Bridge', 'address_type': 'DEX', 'category': 'infrastructure', 'subcategory': 'bridge'},
]


def get_supabase():
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


def fetch_eth_labels(chain_id=1):
    """Fetch all labeled addresses from eth-labels API."""
    url = f'https://eth-labels-production.up.railway.app/accounts?chainId={chain_id}&limit=200000'
    print(f"  Fetching eth-labels chainId={chain_id}...")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.json()
    print(f"  Got {len(data):,} entries")
    return data


def fetch_etherscan_labels():
    """Fetch etherscan-labels from GitHub (Ethereum)."""
    chains = {
        'ethereum': 'https://raw.githubusercontent.com/brianleect/etherscan-labels/main/data/etherscan/combined/combinedAllLabels.json',
        'bsc': 'https://raw.githubusercontent.com/brianleect/etherscan-labels/main/data/bscscan/combined/combinedAllLabels.json',
        'polygon': 'https://raw.githubusercontent.com/brianleect/etherscan-labels/main/data/polygonscan/combined/combinedAllLabels.json',
    }
    all_data = {}
    for chain, url in chains.items():
        print(f"  Fetching etherscan-labels ({chain})...")
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            print(f"  Got {len(data):,} entries")
            all_data[chain] = data
        except Exception as e:
            print(f"  Failed ({chain}): {e}")
    return all_data


def resolve_type_and_entity(label_str, name_tag):
    """Resolve address_type and entity_name from a label string."""
    label_lower = label_str.lower().strip()
    address_type = LABEL_TO_TYPE.get(label_lower, 'WHALE')
    entity_name = LABEL_TO_ENTITY.get(label_lower)

    if not entity_name and name_tag:
        entity_name = name_tag.split(':')[0].strip()
        if entity_name.startswith('Null'):
            entity_name = None

    category = LABEL_TO_CATEGORY.get(label_lower, 'other')
    return address_type, entity_name, category


def process_eth_labels(raw_data, blockchain='ethereum'):
    """Convert eth-labels API data to our Supabase format."""
    records = []
    for item in raw_data:
        address = (item.get('address') or '').lower().strip()
        if not address or len(address) < 10:
            continue

        label_str = item.get('label', '')
        name_tag = item.get('nameTag', '')
        address_type, entity_name, category = resolve_type_and_entity(label_str, name_tag)

        if not entity_name and not name_tag:
            continue

        records.append({
            'address': address,
            'blockchain': blockchain,
            'address_type': address_type,
            'label': name_tag or label_str,
            'entity_name': entity_name or name_tag,
            'confidence': 0.85,
            'signal_potential': 'HIGH' if address_type == 'CEX' else 'MEDIUM',
            'source': 'eth_labels_api',
            'detection_method': 'public_label',
            'analysis_tags': json.dumps({'category': category, 'original_label': label_str, 'is_famous': False}),
        })
    return records


def process_etherscan_labels(all_chain_data):
    """Convert etherscan-labels GitHub data to our Supabase format."""
    records = []
    for blockchain, chain_data in all_chain_data.items():
        for address, info in chain_data.items():
            addr = address.lower().strip()
            if not addr or len(addr) < 10:
                continue

            name = info.get('name', '')
            labels_list = info.get('labels', [])
            primary_label = labels_list[0] if labels_list else ''

            address_type, entity_name, category = resolve_type_and_entity(primary_label, name)

            if not entity_name and not name:
                continue

            records.append({
                'address': addr,
                'blockchain': blockchain,
                'address_type': address_type,
                'label': name or primary_label,
                'entity_name': entity_name or name.split(':')[0].strip() or None,
                'confidence': 0.90,
                'signal_potential': 'HIGH' if address_type == 'CEX' else 'MEDIUM',
                'source': 'etherscan_labels_github',
                'detection_method': 'public_label',
                'analysis_tags': json.dumps({'category': category, 'labels': labels_list, 'is_famous': False}),
            })
    return records


def process_famous_wallets():
    """Convert manually curated famous wallets to Supabase format."""
    records = []
    for w in FAMOUS_WALLETS:
        records.append({
            'address': w['address'].lower().strip(),
            'blockchain': 'ethereum',
            'address_type': w['address_type'],
            'label': w['label'],
            'entity_name': w['entity_name'],
            'confidence': 0.95,
            'signal_potential': 'HIGH',
            'source': 'manual_curation',
            'detection_method': 'verified_public',
            'analysis_tags': json.dumps({
                'category': w.get('category', 'other'),
                'subcategory': w.get('subcategory', ''),
                'is_famous': True,
            }),
        })
    return records


def upsert_batch(sb, records, stats):
    """Upsert a batch of records into Supabase."""
    if not records:
        return
    try:
        sb.table('addresses').upsert(
            records,
            on_conflict='address,blockchain'
        ).execute()
        stats['upserted'] += len(records)
    except Exception as e:
        # Fall back to individual inserts on batch failure
        for record in records:
            try:
                sb.table('addresses').upsert(
                    record,
                    on_conflict='address,blockchain'
                ).execute()
                stats['upserted'] += 1
            except Exception as e2:
                stats['errors'] += 1
                if stats['errors'] <= 5:
                    print(f"    [!] Error: {e2}")


def upload_records(sb, records, source_name, stats):
    """Upload records in batches with progress."""
    print(f"  Uploading {len(records):,} records from {source_name}...")
    
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        upsert_batch(sb, batch, stats)
        
        done = min(i + BATCH_SIZE, len(records))
        if done % (BATCH_SIZE * 10) == 0 or done == len(records):
            print(f"    {done:,}/{len(records):,} ({done/len(records)*100:.0f}%)")
    
    print(f"  Done: {source_name}")


def main():
    sb = get_supabase()
    
    # Get baseline counts
    before_total = sb.table('addresses').select('id', count='exact').execute().count
    before_named = sb.table('addresses').select('id', count='exact').not_.is_('entity_name', 'null').execute().count
    
    print("=" * 70)
    print("  FAMOUS ADDRESS ENRICHMENT")
    print("=" * 70)
    print(f"  Before: {before_total:,} addresses ({before_named:,} with entity_name)")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()
    
    stats = defaultdict(int)
    all_records = []
    
    # Phase 1: eth-labels API (Ethereum)
    print("[Phase 1] eth-labels API")
    try:
        eth_raw = fetch_eth_labels(chain_id=1)
        eth_records = process_eth_labels(eth_raw, 'ethereum')
        print(f"  Processed: {len(eth_records):,} usable records")
        all_records.extend(eth_records)
    except Exception as e:
        print(f"  Failed: {e}")
    
    # Phase 1b: eth-labels BSC
    try:
        bsc_raw = fetch_eth_labels(chain_id=56)
        bsc_records = process_eth_labels(bsc_raw, 'bsc')
        print(f"  Processed: {len(bsc_records):,} BSC records")
        all_records.extend(bsc_records)
    except Exception as e:
        print(f"  BSC fetch failed (non-critical): {e}")

    print()
    
    # Phase 2: etherscan-labels GitHub
    print("[Phase 2] etherscan-labels GitHub")
    try:
        es_data = fetch_etherscan_labels()
        es_records = process_etherscan_labels(es_data)
        print(f"  Processed: {len(es_records):,} usable records")
        all_records.extend(es_records)
    except Exception as e:
        print(f"  Failed: {e}")
    
    print()
    
    # Phase 3: Famous wallets
    print("[Phase 3] Famous wallets (manual curation)")
    famous_records = process_famous_wallets()
    print(f"  {len(famous_records)} famous wallet entries")
    all_records.extend(famous_records)
    
    print()
    
    # Deduplicate by (address, blockchain), prefer higher confidence
    print("[Phase 4] Deduplicating...")
    deduped = {}
    for record in all_records:
        key = (record['address'], record['blockchain'])
        existing = deduped.get(key)
        if not existing or record['confidence'] > existing['confidence']:
            deduped[key] = record
        elif existing and not existing.get('entity_name') and record.get('entity_name'):
            deduped[key] = record
    
    final_records = list(deduped.values())
    print(f"  {len(all_records):,} total -> {len(final_records):,} after dedup")
    
    # Count by type
    type_counts = defaultdict(int)
    named_count = 0
    for r in final_records:
        type_counts[r['address_type']] += 1
        if r.get('entity_name'):
            named_count += 1
    
    print(f"  With entity_name: {named_count:,}")
    for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"    {t:6s}: {c:>6,}")
    
    print()
    
    # Upload
    print("[Phase 5] Uploading to Supabase...")
    start_time = time.time()
    upload_records(sb, final_records, "all sources", stats)
    elapsed = time.time() - start_time
    
    # Get final counts
    after_total = sb.table('addresses').select('id', count='exact').execute().count
    after_named = sb.table('addresses').select('id', count='exact').not_.is_('entity_name', 'null').execute().count
    
    print()
    print("=" * 70)
    print("  ENRICHMENT COMPLETE")
    print("=" * 70)
    print(f"  Records processed:  {len(final_records):,}")
    print(f"  Upserted:           {stats['upserted']:,}")
    print(f"  Errors:             {stats['errors']:,}")
    print(f"  Duration:           {elapsed/60:.1f} minutes")
    print()
    print(f"  Addresses before:   {before_total:,} ({before_named:,} named)")
    print(f"  Addresses after:    {after_total:,} ({after_named:,} named)")
    print(f"  New named:          +{after_named - before_named:,}")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted.")
        sys.exit(0)
