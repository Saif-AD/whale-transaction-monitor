#!/usr/bin/env python3
"""
MULTI-CHAIN ADDRESS DISCOVERY - Ethereum, Bitcoin, Polygon, BSC
Discovers CEX, DEX, and Whale addresses across all major blockchains.
"""

import os
import sys
from google.cloud import bigquery
from supabase import create_client
from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
from datetime import datetime
import time

# Configuration
BIGQUERY_CREDENTIALS_PATH = "config/bigquery_credentials.json"
START_DATE = "2023-01-01"
BATCH_SIZE = 500

# Thresholds (lower = more coverage)
WHALE_MIN_NATIVE = 10  # 10 ETH/BTC/MATIC minimum (~$30k)
WHALE_MIN_COUNT = 2
CEX_MIN_TX = 1_000
CEX_MIN_COUNTERPARTIES = 100
DEX_MIN_TRANSFERS = 10_000

# Targets per chain
CEX_TARGET = 50_000
DEX_TARGET = 30_000
WHALE_TARGET = 50_000

# Blockchains
CHAINS = {
    'ethereum': {
        'dataset': 'crypto_ethereum',
        'decimals': 18,
        'has_tokens': True,
        'label': 'Ethereum (ERC-20)',
        'native_symbol': 'ETH'
    },
    'polygon': {
        'dataset': 'crypto_polygon',
        'decimals': 18,
        'has_tokens': True,
        'label': 'Polygon',
        'native_symbol': 'MATIC'
    },
    'bsc': {
        'dataset': 'crypto_bsc',
        'decimals': 18,
        'has_tokens': True,
        'label': 'Binance Smart Chain (BEP-20)',
        'native_symbol': 'BNB'
    },
    'bitcoin': {
        'dataset': 'crypto_bitcoin',
        'decimals': 8,
        'has_tokens': False,
        'label': 'Bitcoin',
        'native_symbol': 'BTC'
    },
}

SEED_CEX = [
    "0x28c6c06298d514db089934071355e5743bf21d60",
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549",
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d",
    "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43",
    "0x503828976d22510aad0201ac7ec88293211d23da",
]

print("\n" + "="*80)
print("🌍 MULTI-CHAIN ADDRESS DISCOVERY")
print("="*80)
print(f"\nChains: Ethereum, Polygon, BSC, Bitcoin")
print(f"Targets: {CEX_TARGET:,} CEX, {DEX_TARGET:,} DEX, {WHALE_TARGET:,} Whales PER CHAIN")
print()

# Initialize
if not os.path.exists(BIGQUERY_CREDENTIALS_PATH):
    print(f"❌ Credentials not found: {BIGQUERY_CREDENTIALS_PATH}")
    sys.exit(1)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_PATH
bq = bigquery.Client()
sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

print(f"✅ BigQuery: {bq.project}")
print(f"✅ Supabase connected")
print()

def estimate(query):
    job = bq.query(query, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
    gb = job.total_bytes_processed / (1024**3)
    cost = (job.total_bytes_processed / (1024**4)) * 5 * 0.79
    return gb, cost

def run_query(query, name):
    print(f"\n📊 {name}")
    try:
        gb, cost = estimate(query)
        print(f"   Scan: {gb:.1f} GB | Cost: £{cost:.2f}")
    except:
        pass
    
    print(f"   ⏳ Running...")
    start = time.time()
    results = list(bq.query(query))
    print(f"   ✅ {len(results):,} found in {time.time()-start:.1f}s")
    return results

def upsert(addresses):
    if not addresses:
        return 0
    
    # STEP 1: Deduplicate within batch (by address + blockchain)
    print(f"   🔍 Deduplicating {len(addresses):,} addresses...")
    unique = {}
    for addr in addresses:
        key = (addr['address'].lower(), addr['blockchain'])
        if key not in unique:
            unique[key] = addr
        else:
            # Keep higher confidence
            if addr.get('confidence', 0) > unique[key].get('confidence', 0):
                unique[key] = addr
    
    addresses = list(unique.values())
    print(f"   ✅ After dedup: {len(addresses):,} unique addresses")
    
    # STEP 2: Upsert to database (handles duplicates at DB level)
    print(f"   📤 Upserting {len(addresses):,}...")
    inserted = 0
    for i in range(0, len(addresses), BATCH_SIZE):
        batch = addresses[i:i+BATCH_SIZE]
        try:
            sb.table('addresses').upsert(batch, on_conflict='address,blockchain').execute()
            inserted += len(batch)
            print(f"      {inserted:,}/{len(addresses):,}", end='\r')
        except Exception as e:
            print(f"\n      ⚠️  Error: {e}")
    
    print(f"\n   ✅ Upserted {inserted:,} (new + updated, no duplicates)")
    return inserted

# CEX Discovery
def discover_cex(chain, cfg):
    seed = ", ".join([f"'{a.lower()}'" for a in SEED_CEX])
    query = f"""
    WITH out AS (
      SELECT LOWER(from_address) as addr, COUNT(*) as txs, COUNT(DISTINCT to_address) as uniq
      FROM `bigquery-public-data.{cfg['dataset']}.transactions`
      WHERE block_timestamp >= '{START_DATE}' AND from_address NOT IN ({seed})
      GROUP BY addr HAVING txs >= {CEX_MIN_TX} AND uniq >= {CEX_MIN_COUNTERPARTIES}
    ),
    inc AS (
      SELECT LOWER(to_address) as addr, COUNT(*) as txs, COUNT(DISTINCT from_address) as uniq
      FROM `bigquery-public-data.{cfg['dataset']}.transactions`
      WHERE block_timestamp >= '{START_DATE}' AND to_address NOT IN ({seed})
      GROUP BY addr HAVING txs >= {CEX_MIN_TX} AND uniq >= {CEX_MIN_COUNTERPARTIES}
    )
    SELECT COALESCE(out.addr, inc.addr) as address,
           COALESCE(out.txs,0) + COALESCE(inc.txs,0) as tx_count
    FROM out FULL JOIN inc ON out.addr = inc.addr
    ORDER BY tx_count DESC LIMIT {CEX_TARGET}
    """
    
    results = run_query(query, f"CEX Discovery ({chain.upper()})")
    return [{
        'address': r.address,
        'blockchain': chain,
        'address_type': 'CEX',
        'label': f'High-Activity Exchange Wallet ({chain.upper()}: {int(r.tx_count):,} txs)',
        'entity_name': None,
        'confidence': round(min(0.95, 0.70 + int(r.tx_count)/100000*0.20), 2),
        'signal_potential': 'N/A',
        'source': f'bigquery_{chain}_cex_pattern',
        'detection_method': 'transaction_volume_analysis',
        'analysis_tags': {'tx_count': int(r.tx_count), 'chain': chain},
        'created_at': datetime.utcnow().isoformat(),
        'updated_at': datetime.utcnow().isoformat(),
    } for r in results]

# DEX Discovery (EVM chains only)
def discover_dex(chain, cfg):
    if not cfg['has_tokens']:
        return []
    
    query = f"""
    SELECT LOWER(token_address) as address, COUNT(*) as transfers
    FROM `bigquery-public-data.{cfg['dataset']}.token_transfers`
    WHERE block_timestamp >= '{START_DATE}'
    GROUP BY address
    HAVING transfers >= {DEX_MIN_TRANSFERS}
    ORDER BY transfers DESC LIMIT {DEX_TARGET}
    """
    
    results = run_query(query, f"DEX Discovery ({chain.upper()})")
    return [{
        'address': r.address,
        'blockchain': chain,
        'address_type': 'DEX',
        'label': f'DEX Router/Pool ({chain.upper()}: {int(r.transfers):,} transfers)',
        'entity_name': None,
        'confidence': 0.80,
        'signal_potential': 'N/A',
        'source': f'bigquery_{chain}_dex_pattern',
        'detection_method': 'token_transfer_analysis',
        'analysis_tags': {'transfer_count': int(r.transfers), 'chain': chain},
        'created_at': datetime.utcnow().isoformat(),
        'updated_at': datetime.utcnow().isoformat(),
    } for r in results]

# Whale Discovery
def discover_whales(chain, cfg):
    seed = ", ".join([f"'{a.lower()}'" for a in SEED_CEX])
    decimals = cfg['decimals']
    
    query = f"""
    SELECT LOWER(from_address) as address,
           COUNT(*) as large_tx_count,
           MAX(SAFE_CAST(value AS FLOAT64) / 1e{decimals}) as max_native,
           SUM(SAFE_CAST(value AS FLOAT64) / 1e{decimals}) as total_native
    FROM `bigquery-public-data.{cfg['dataset']}.transactions`
    WHERE block_timestamp >= '{START_DATE}'
      AND SAFE_CAST(value AS FLOAT64) / 1e{decimals} > {WHALE_MIN_NATIVE}
      AND from_address NOT IN ({seed})
      AND from_address IS NOT NULL
    GROUP BY address
    HAVING large_tx_count >= {WHALE_MIN_COUNT}
    ORDER BY total_native DESC LIMIT {WHALE_TARGET}
    """
    
    results = run_query(query, f"Whale Discovery ({chain.upper()})")
    native_symbol = cfg.get('native_symbol', chain.upper())
    return [{
        'address': r.address,
        'blockchain': chain,
        'address_type': 'WHALE',
        'label': f'Verified Whale ({chain.upper()}: {int(r.large_tx_count)} large {native_symbol} txs)',
        'entity_name': None,
        'confidence': round(min(0.95, 0.75 + int(r.large_tx_count)*0.01), 2),
        'signal_potential': 'HIGH',
        'source': f'bigquery_{chain}_whale_verification',
        'detection_method': 'large_transaction_history',
        'analysis_tags': {
            'large_tx_count': int(r.large_tx_count),
            'max_native': float(r.max_native),
            'total_native': float(r.total_native),
            'chain': chain,
            'native_symbol': native_symbol,
        },
        'created_at': datetime.utcnow().isoformat(),
        'updated_at': datetime.utcnow().isoformat(),
    } for r in results]

# Main
def main():
    print("="*80)
    print("🌍 MULTI-CHAIN DISCOVERY - AUTO MODE")
    print("="*80)
    print(f"\n  Ethereum (ERC-20) | Polygon (MATIC) | BSC (BEP-20) | Bitcoin")
    print(f"\n  Queries: {len(CHAINS) * 3} total (CEX + DEX + Whale per chain)")
    print(f"  Targets: {CEX_TARGET:,} CEX, {DEX_TARGET:,} DEX, {WHALE_TARGET:,} Whales PER CHAIN")
    print(f"  Cost: £10-20 | Time: 2-3 hours")
    print()
    
    response = input("🚀 Start multi-chain discovery? (yes/no): ")
    if response.lower() != 'yes':
        return
    
    print("\n⚡ Running all queries automatically...\n")
    
    # Get BEFORE counts
    print("📊 Database state BEFORE:")
    total_before = sb.table('addresses').select('id', count='exact').execute()
    print(f"   Total: {total_before.count:,}")
    for addr_type in ['CEX', 'DEX', 'WHALE']:
        result = sb.table('addresses').select('id', count='exact').eq('address_type', addr_type).execute()
        print(f"   {addr_type}: {result.count:,}")
    print()
    
    total_added = 0
    
    for chain, cfg in CHAINS.items():
        print("\n" + "="*80)
        print(f"🔗 {cfg['label'].upper()}")
        print("="*80)
        
        # CEX
        cex = discover_cex(chain, cfg)
        if cex:
            total_added += upsert(cex)
        
        # DEX (EVM only)
        if cfg['has_tokens']:
            dex = discover_dex(chain, cfg)
            if dex:
                total_added += upsert(dex)
        
        # Whales
        whales = discover_whales(chain, cfg)
        if whales:
            total_added += upsert(whales)
    
    print("\n" + "="*80)
    print("✅ MULTI-CHAIN DISCOVERY COMPLETE")
    print("="*80)
    
    # Get AFTER counts
    print("\n📊 BEFORE vs AFTER (No Duplicates!):")
    print("─"*80)
    total_after = sb.table('addresses').select('id', count='exact').execute()
    total_increase = total_after.count - total_before.count
    
    print(f"   TOTAL:  {total_before.count:>8,} → {total_after.count:>8,} (+{total_increase:,})")
    
    for addr_type in ['CEX', 'DEX', 'WHALE']:
        result = sb.table('addresses').select('id', count='exact').eq('address_type', addr_type).execute()
        print(f"   {addr_type:6s}: {result.count:,}")
    
    print()
    print(f"🔍 Duplicate Check:")
    print(f"   ✅ Added {total_increase:,} unique addresses")
    print(f"   ✅ No duplicates (UPSERT with unique constraint)")
    print()
    print("📊 Verify by blockchain:")
    print("   SELECT blockchain, address_type, COUNT(*) FROM addresses")
    print("   WHERE source LIKE 'bigquery_%'")
    print("   GROUP BY blockchain, address_type ORDER BY blockchain, address_type;")
    print()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏸️  Interrupted")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


MULTI-CHAIN ADDRESS DISCOVERY - Ethereum, Bitcoin, Polygon, BSC
Discovers CEX, DEX, and Whale addresses across all major blockchains.
"""

import os
import sys
from google.cloud import bigquery
from supabase import create_client
from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
from datetime import datetime
import time

# Configuration
BIGQUERY_CREDENTIALS_PATH = "config/bigquery_credentials.json"
START_DATE = "2023-01-01"
BATCH_SIZE = 500

# Thresholds (lower = more coverage)
WHALE_MIN_NATIVE = 10  # 10 ETH/BTC/MATIC minimum (~$30k)
WHALE_MIN_COUNT = 2
CEX_MIN_TX = 1_000
CEX_MIN_COUNTERPARTIES = 100
DEX_MIN_TRANSFERS = 10_000

# Targets per chain
CEX_TARGET = 50_000
DEX_TARGET = 30_000
WHALE_TARGET = 50_000

# Blockchains
CHAINS = {
    'ethereum': {
        'dataset': 'crypto_ethereum',
        'decimals': 18,
        'has_tokens': True,
        'label': 'Ethereum (ERC-20)',
        'native_symbol': 'ETH'
    },
    'polygon': {
        'dataset': 'crypto_polygon',
        'decimals': 18,
        'has_tokens': True,
        'label': 'Polygon',
        'native_symbol': 'MATIC'
    },
    'bsc': {
        'dataset': 'crypto_bsc',
        'decimals': 18,
        'has_tokens': True,
        'label': 'Binance Smart Chain (BEP-20)',
        'native_symbol': 'BNB'
    },
    'bitcoin': {
        'dataset': 'crypto_bitcoin',
        'decimals': 8,
        'has_tokens': False,
        'label': 'Bitcoin',
        'native_symbol': 'BTC'
    },
}

SEED_CEX = [
    "0x28c6c06298d514db089934071355e5743bf21d60",
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549",
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d",
    "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43",
    "0x503828976d22510aad0201ac7ec88293211d23da",
]

print("\n" + "="*80)
print("🌍 MULTI-CHAIN ADDRESS DISCOVERY")
print("="*80)
print(f"\nChains: Ethereum, Polygon, BSC, Bitcoin")
print(f"Targets: {CEX_TARGET:,} CEX, {DEX_TARGET:,} DEX, {WHALE_TARGET:,} Whales PER CHAIN")
print()

# Initialize
if not os.path.exists(BIGQUERY_CREDENTIALS_PATH):
    print(f"❌ Credentials not found: {BIGQUERY_CREDENTIALS_PATH}")
    sys.exit(1)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_PATH
bq = bigquery.Client()
sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

print(f"✅ BigQuery: {bq.project}")
print(f"✅ Supabase connected")
print()

def estimate(query):
    job = bq.query(query, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
    gb = job.total_bytes_processed / (1024**3)
    cost = (job.total_bytes_processed / (1024**4)) * 5 * 0.79
    return gb, cost

def run_query(query, name):
    print(f"\n📊 {name}")
    try:
        gb, cost = estimate(query)
        print(f"   Scan: {gb:.1f} GB | Cost: £{cost:.2f}")
    except:
        pass
    
    print(f"   ⏳ Running...")
    start = time.time()
    results = list(bq.query(query))
    print(f"   ✅ {len(results):,} found in {time.time()-start:.1f}s")
    return results

def upsert(addresses):
    if not addresses:
        return 0
    
    # STEP 1: Deduplicate within batch (by address + blockchain)
    print(f"   🔍 Deduplicating {len(addresses):,} addresses...")
    unique = {}
    for addr in addresses:
        key = (addr['address'].lower(), addr['blockchain'])
        if key not in unique:
            unique[key] = addr
        else:
            # Keep higher confidence
            if addr.get('confidence', 0) > unique[key].get('confidence', 0):
                unique[key] = addr
    
    addresses = list(unique.values())
    print(f"   ✅ After dedup: {len(addresses):,} unique addresses")
    
    # STEP 2: Upsert to database (handles duplicates at DB level)
    print(f"   📤 Upserting {len(addresses):,}...")
    inserted = 0
    for i in range(0, len(addresses), BATCH_SIZE):
        batch = addresses[i:i+BATCH_SIZE]
        try:
            sb.table('addresses').upsert(batch, on_conflict='address,blockchain').execute()
            inserted += len(batch)
            print(f"      {inserted:,}/{len(addresses):,}", end='\r')
        except Exception as e:
            print(f"\n      ⚠️  Error: {e}")
    
    print(f"\n   ✅ Upserted {inserted:,} (new + updated, no duplicates)")
    return inserted

# CEX Discovery
def discover_cex(chain, cfg):
    seed = ", ".join([f"'{a.lower()}'" for a in SEED_CEX])
    query = f"""
    WITH out AS (
      SELECT LOWER(from_address) as addr, COUNT(*) as txs, COUNT(DISTINCT to_address) as uniq
      FROM `bigquery-public-data.{cfg['dataset']}.transactions`
      WHERE block_timestamp >= '{START_DATE}' AND from_address NOT IN ({seed})
      GROUP BY addr HAVING txs >= {CEX_MIN_TX} AND uniq >= {CEX_MIN_COUNTERPARTIES}
    ),
    inc AS (
      SELECT LOWER(to_address) as addr, COUNT(*) as txs, COUNT(DISTINCT from_address) as uniq
      FROM `bigquery-public-data.{cfg['dataset']}.transactions`
      WHERE block_timestamp >= '{START_DATE}' AND to_address NOT IN ({seed})
      GROUP BY addr HAVING txs >= {CEX_MIN_TX} AND uniq >= {CEX_MIN_COUNTERPARTIES}
    )
    SELECT COALESCE(out.addr, inc.addr) as address,
           COALESCE(out.txs,0) + COALESCE(inc.txs,0) as tx_count
    FROM out FULL JOIN inc ON out.addr = inc.addr
    ORDER BY tx_count DESC LIMIT {CEX_TARGET}
    """
    
    results = run_query(query, f"CEX Discovery ({chain.upper()})")
    return [{
        'address': r.address,
        'blockchain': chain,
        'address_type': 'CEX',
        'label': f'High-Activity Exchange Wallet ({chain.upper()}: {int(r.tx_count):,} txs)',
        'entity_name': None,
        'confidence': round(min(0.95, 0.70 + int(r.tx_count)/100000*0.20), 2),
        'signal_potential': 'N/A',
        'source': f'bigquery_{chain}_cex_pattern',
        'detection_method': 'transaction_volume_analysis',
        'analysis_tags': {'tx_count': int(r.tx_count), 'chain': chain},
        'created_at': datetime.utcnow().isoformat(),
        'updated_at': datetime.utcnow().isoformat(),
    } for r in results]

# DEX Discovery (EVM chains only)
def discover_dex(chain, cfg):
    if not cfg['has_tokens']:
        return []
    
    query = f"""
    SELECT LOWER(token_address) as address, COUNT(*) as transfers
    FROM `bigquery-public-data.{cfg['dataset']}.token_transfers`
    WHERE block_timestamp >= '{START_DATE}'
    GROUP BY address
    HAVING transfers >= {DEX_MIN_TRANSFERS}
    ORDER BY transfers DESC LIMIT {DEX_TARGET}
    """
    
    results = run_query(query, f"DEX Discovery ({chain.upper()})")
    return [{
        'address': r.address,
        'blockchain': chain,
        'address_type': 'DEX',
        'label': f'DEX Router/Pool ({chain.upper()}: {int(r.transfers):,} transfers)',
        'entity_name': None,
        'confidence': 0.80,
        'signal_potential': 'N/A',
        'source': f'bigquery_{chain}_dex_pattern',
        'detection_method': 'token_transfer_analysis',
        'analysis_tags': {'transfer_count': int(r.transfers), 'chain': chain},
        'created_at': datetime.utcnow().isoformat(),
        'updated_at': datetime.utcnow().isoformat(),
    } for r in results]

# Whale Discovery
def discover_whales(chain, cfg):
    seed = ", ".join([f"'{a.lower()}'" for a in SEED_CEX])
    decimals = cfg['decimals']
    
    query = f"""
    SELECT LOWER(from_address) as address,
           COUNT(*) as large_tx_count,
           MAX(SAFE_CAST(value AS FLOAT64) / 1e{decimals}) as max_native,
           SUM(SAFE_CAST(value AS FLOAT64) / 1e{decimals}) as total_native
    FROM `bigquery-public-data.{cfg['dataset']}.transactions`
    WHERE block_timestamp >= '{START_DATE}'
      AND SAFE_CAST(value AS FLOAT64) / 1e{decimals} > {WHALE_MIN_NATIVE}
      AND from_address NOT IN ({seed})
      AND from_address IS NOT NULL
    GROUP BY address
    HAVING large_tx_count >= {WHALE_MIN_COUNT}
    ORDER BY total_native DESC LIMIT {WHALE_TARGET}
    """
    
    results = run_query(query, f"Whale Discovery ({chain.upper()})")
    native_symbol = cfg.get('native_symbol', chain.upper())
    return [{
        'address': r.address,
        'blockchain': chain,
        'address_type': 'WHALE',
        'label': f'Verified Whale ({chain.upper()}: {int(r.large_tx_count)} large {native_symbol} txs)',
        'entity_name': None,
        'confidence': round(min(0.95, 0.75 + int(r.large_tx_count)*0.01), 2),
        'signal_potential': 'HIGH',
        'source': f'bigquery_{chain}_whale_verification',
        'detection_method': 'large_transaction_history',
        'analysis_tags': {
            'large_tx_count': int(r.large_tx_count),
            'max_native': float(r.max_native),
            'total_native': float(r.total_native),
            'chain': chain,
            'native_symbol': native_symbol,
        },
        'created_at': datetime.utcnow().isoformat(),
        'updated_at': datetime.utcnow().isoformat(),
    } for r in results]

# Main
def main():
    print("="*80)
    print("🌍 MULTI-CHAIN DISCOVERY - AUTO MODE")
    print("="*80)
    print(f"\n  Ethereum (ERC-20) | Polygon (MATIC) | BSC (BEP-20) | Bitcoin")
    print(f"\n  Queries: {len(CHAINS) * 3} total (CEX + DEX + Whale per chain)")
    print(f"  Targets: {CEX_TARGET:,} CEX, {DEX_TARGET:,} DEX, {WHALE_TARGET:,} Whales PER CHAIN")
    print(f"  Cost: £10-20 | Time: 2-3 hours")
    print()
    
    response = input("🚀 Start multi-chain discovery? (yes/no): ")
    if response.lower() != 'yes':
        return
    
    print("\n⚡ Running all queries automatically...\n")
    
    # Get BEFORE counts
    print("📊 Database state BEFORE:")
    total_before = sb.table('addresses').select('id', count='exact').execute()
    print(f"   Total: {total_before.count:,}")
    for addr_type in ['CEX', 'DEX', 'WHALE']:
        result = sb.table('addresses').select('id', count='exact').eq('address_type', addr_type).execute()
        print(f"   {addr_type}: {result.count:,}")
    print()
    
    total_added = 0
    
    for chain, cfg in CHAINS.items():
        print("\n" + "="*80)
        print(f"🔗 {cfg['label'].upper()}")
        print("="*80)
        
        # CEX
        cex = discover_cex(chain, cfg)
        if cex:
            total_added += upsert(cex)
        
        # DEX (EVM only)
        if cfg['has_tokens']:
            dex = discover_dex(chain, cfg)
            if dex:
                total_added += upsert(dex)
        
        # Whales
        whales = discover_whales(chain, cfg)
        if whales:
            total_added += upsert(whales)
    
    print("\n" + "="*80)
    print("✅ MULTI-CHAIN DISCOVERY COMPLETE")
    print("="*80)
    
    # Get AFTER counts
    print("\n📊 BEFORE vs AFTER (No Duplicates!):")
    print("─"*80)
    total_after = sb.table('addresses').select('id', count='exact').execute()
    total_increase = total_after.count - total_before.count
    
    print(f"   TOTAL:  {total_before.count:>8,} → {total_after.count:>8,} (+{total_increase:,})")
    
    for addr_type in ['CEX', 'DEX', 'WHALE']:
        result = sb.table('addresses').select('id', count='exact').eq('address_type', addr_type).execute()
        print(f"   {addr_type:6s}: {result.count:,}")
    
    print()
    print(f"🔍 Duplicate Check:")
    print(f"   ✅ Added {total_increase:,} unique addresses")
    print(f"   ✅ No duplicates (UPSERT with unique constraint)")
    print()
    print("📊 Verify by blockchain:")
    print("   SELECT blockchain, address_type, COUNT(*) FROM addresses")
    print("   WHERE source LIKE 'bigquery_%'")
    print("   GROUP BY blockchain, address_type ORDER BY blockchain, address_type;")
    print()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏸️  Interrupted")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

