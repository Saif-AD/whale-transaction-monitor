#!/usr/bin/env python3
"""
CROSS-CHAIN VERIFIED WHALE DISCOVERY via BigQuery

Finds addresses that are active whales on multiple chains (Ethereum, Polygon, BSC).
Cross-chain presence is one of the strongest signals for verified whale status because:
  - Bridges require significant capital and sophistication
  - Multi-chain whales are typically institutions, funds, or serious traders
  - Cross-chain activity confirms the address is not a one-off contract or bot

Discovery Methods:
  1. Shared-address whales: Same address active on 2+ EVM chains with large txs
  2. High-value bridge users: Addresses that bridged significant value cross-chain
  3. Multi-chain stablecoin whales: Large USDC/USDT holders across chains

All discovered addresses are upserted to Supabase with source='bigquery_crosschain_verified'.
"""

import os
import sys
import time
import logging
from datetime import datetime
from google.cloud import bigquery
from supabase import create_client
from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
from utils.address_verification import verify_batch

logger = logging.getLogger(__name__)

# Configuration
BIGQUERY_CREDENTIALS_PATH = "config/bigquery_credentials.json"
START_DATE = "2024-01-01"
BATCH_SIZE = 500

# Cross-chain verification thresholds
MIN_NATIVE_VALUE = 5          # 5 ETH/BNB/MATIC per large tx
MIN_LARGE_TXS = 3             # At least 3 large txs per chain
MIN_CHAINS = 2                # Must be active on 2+ chains
CROSS_CHAIN_WHALE_TARGET = 20_000

# EVM chains with shared address space (Bitcoin excluded - different address format)
EVM_CHAINS = {
    'ethereum': {
        'dataset': 'crypto_ethereum',
        'decimals': 18,
        'native_symbol': 'ETH',
        'native_price_usd': 3000,
    },
    'polygon': {
        'dataset': 'crypto_polygon',
        'decimals': 18,
        'native_symbol': 'MATIC',
        'native_price_usd': 0.8,
    },
    'bsc': {
        'dataset': 'crypto_bsc',
        'decimals': 18,
        'native_symbol': 'BNB',
        'native_price_usd': 600,
    },
}

# Known bridge contracts to identify bridge users
BRIDGE_CONTRACTS = {
    'ethereum': [
        '0x3ee18b2214aff97000d974cf647e7c347e8fa585',  # Wormhole
        '0x3014ca10b91cb3d0ad85fef7a3cb95bcac9c0f79',  # Multichain
        '0x5427fefa711eff984124bfbb1ab6fbf5e3da1820',  # Synapse
        '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1',  # Optimism Bridge
        '0x49048044d57e1c92a77f79988d21fa8faf74e97e',  # Base Bridge
        '0xa0c68c638235ee32657e8f720a23cec1bfc6c4a7',  # Polygon Bridge
        '0x401f6c983ea34274ec46f84d70b31c151321188b',  # LayerZero
    ],
    'polygon': [
        '0x5ac25b19068de2e60aef579f25bdc08a08ea0f9c',  # Polygon Bridge (Polygon side)
    ],
    'bsc': [
        '0x05185872898b6f94aa600177ef41b9334b1fa48b',  # Multichain BSC
    ],
}

# Major stablecoin contracts per chain
STABLECOINS = {
    'ethereum': {
        'USDT': '0xdac17f958d2ee523a2206206994597c13d831ec7',
        'USDC': '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
        'DAI':  '0x6b175474e89094c44da98b954eedeac495271d0f',
    },
    'polygon': {
        'USDT': '0xc2132d05d31c914a87c6611c10748aeb04b58e8f',
        'USDC': '0x2791bca1f2de4661ed88a30c99a7a9449aa84174',
        'DAI':  '0x8f3cf7ad23cd3cadbd9735aff958023239c6a063',
    },
    'bsc': {
        'USDT': '0x55d398326f99059ff775485246999027b3197955',
        'USDC': '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d',
        'DAI':  '0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3',
    },
}


def _init_clients():
    if not os.path.exists(BIGQUERY_CREDENTIALS_PATH):
        print(f"Credentials not found: {BIGQUERY_CREDENTIALS_PATH}")
        sys.exit(1)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_PATH
    bq = bigquery.Client()
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    return bq, sb


def estimate_query(bq, query):
    job = bq.query(query, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
    gb = job.total_bytes_processed / (1024**3)
    cost = (job.total_bytes_processed / (1024**4)) * 5 * 0.79
    return gb, cost


def run_query(bq, query, name):
    print(f"\n  {name}")
    try:
        gb, cost = estimate_query(bq, query)
        print(f"   Scan: {gb:.1f} GB | Est. cost: {cost:.2f}")
    except Exception:
        pass

    print(f"   Running...")
    start = time.time()
    results = list(bq.query(query))
    print(f"   {len(results):,} results in {time.time()-start:.1f}s")
    return results


def upsert_addresses(sb, addresses):
    if not addresses:
        return 0

    unique = {}
    for addr in addresses:
        key = (addr['address'].lower(), addr['blockchain'])
        if key not in unique or addr.get('confidence', 0) > unique[key].get('confidence', 0):
            unique[key] = addr

    addresses = list(unique.values())
    inserted = 0
    for i in range(0, len(addresses), BATCH_SIZE):
        batch = addresses[i:i + BATCH_SIZE]
        try:
            sb.table('addresses').upsert(batch, on_conflict='address,blockchain').execute()
            inserted += len(batch)
        except Exception as e:
            print(f"   Upsert error: {e}")

    print(f"   Upserted {inserted:,} addresses")
    return inserted


def discover_shared_address_whales(bq):
    """
    Find addresses that appear as large-value transactors on 2+ EVM chains.
    Since EVM chains share address format, the same private key controls the
    same address on all EVM chains. If an address moves large value on both
    Ethereum and Polygon, it's very likely a real whale.
    """
    # Build per-chain whale subqueries
    chain_queries = []
    for chain, cfg in EVM_CHAINS.items():
        decimals = cfg['decimals']
        chain_queries.append(f"""
        SELECT
            LOWER(from_address) AS address,
            '{chain}' AS chain,
            COUNT(*) AS large_tx_count,
            SUM(SAFE_CAST(value AS FLOAT64) / 1e{decimals}) AS total_native,
            MAX(SAFE_CAST(value AS FLOAT64) / 1e{decimals}) AS max_native,
            MAX(block_timestamp) AS last_active
        FROM `bigquery-public-data.{cfg['dataset']}.transactions`
        WHERE block_timestamp >= '{START_DATE}'
          AND SAFE_CAST(value AS FLOAT64) / 1e{decimals} > {MIN_NATIVE_VALUE}
          AND from_address IS NOT NULL
        GROUP BY address
        HAVING large_tx_count >= {MIN_LARGE_TXS}
        """)

    combined = " UNION ALL ".join(chain_queries)

    query = f"""
    WITH per_chain AS ({combined}),
    cross_chain AS (
        SELECT
            address,
            COUNT(DISTINCT chain) AS num_chains,
            ARRAY_AGG(STRUCT(chain, large_tx_count, total_native, max_native, last_active)
                      ORDER BY total_native DESC) AS chain_details,
            SUM(large_tx_count) AS total_large_txs,
            SUM(total_native) AS total_native_all_chains,
            MAX(last_active) AS last_active_any_chain
        FROM per_chain
        GROUP BY address
        HAVING num_chains >= {MIN_CHAINS}
    )
    SELECT
        address,
        num_chains,
        total_large_txs,
        total_native_all_chains,
        last_active_any_chain,
        chain_details
    FROM cross_chain
    ORDER BY num_chains DESC, total_large_txs DESC
    LIMIT {CROSS_CHAIN_WHALE_TARGET}
    """

    results = run_query(bq, query, "Cross-Chain Shared-Address Whale Discovery")
    addresses = []

    for r in results:
        chains_active = []
        chain_summary = {}
        for detail in r.chain_details:
            c = detail['chain']
            chains_active.append(c)
            chain_summary[c] = {
                'large_tx_count': int(detail['large_tx_count']),
                'total_native': float(detail['total_native']),
                'max_native': float(detail['max_native']),
            }

        # Confidence: base 0.80 + 0.05 per extra chain + tx count bonus
        confidence = min(0.98, 0.80 + (int(r.num_chains) - 2) * 0.05
                         + min(0.10, int(r.total_large_txs) * 0.002))

        # Create one record per chain the whale is active on
        for chain in chains_active:
            addresses.append({
                'address': r.address,
                'blockchain': chain,
                'address_type': 'WHALE',
                'label': f'Cross-Chain Verified Whale ({int(r.num_chains)} chains, '
                         f'{int(r.total_large_txs)} large txs)',
                'entity_name': None,
                'confidence': round(confidence, 2),
                'signal_potential': 'HIGH',
                'source': 'bigquery_crosschain_verified',
                'detection_method': 'cross_chain_shared_address',
                'analysis_tags': {
                    'num_chains': int(r.num_chains),
                    'chains_active': chains_active,
                    'total_large_txs': int(r.total_large_txs),
                    'chain_breakdown': chain_summary,
                },
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat(),
            })

    return addresses


def discover_bridge_whales(bq):
    """
    Find addresses that sent large value to known bridge contracts.
    Bridge users who move >$100K are very likely whales or institutions.
    """
    bridge_queries = []
    for chain, cfg in EVM_CHAINS.items():
        bridges = BRIDGE_CONTRACTS.get(chain, [])
        if not bridges:
            continue
        bridge_list = ", ".join([f"'{b.lower()}'" for b in bridges])
        decimals = cfg['decimals']
        price = cfg['native_price_usd']
        # $100K minimum in native value
        min_native = 100_000 / price

        bridge_queries.append(f"""
        SELECT
            LOWER(from_address) AS address,
            '{chain}' AS chain,
            COUNT(*) AS bridge_tx_count,
            SUM(SAFE_CAST(value AS FLOAT64) / 1e{decimals}) AS total_bridged_native,
            SUM(SAFE_CAST(value AS FLOAT64) / 1e{decimals} * {price}) AS total_bridged_usd,
            MAX(block_timestamp) AS last_bridge
        FROM `bigquery-public-data.{cfg['dataset']}.transactions`
        WHERE block_timestamp >= '{START_DATE}'
          AND LOWER(to_address) IN ({bridge_list})
          AND SAFE_CAST(value AS FLOAT64) / 1e{decimals} > {min_native}
          AND from_address IS NOT NULL
        GROUP BY address
        HAVING bridge_tx_count >= 2
        """)

    if not bridge_queries:
        return []

    combined = " UNION ALL ".join(bridge_queries)
    query = f"""
    WITH bridge_users AS ({combined})
    SELECT
        address,
        chain,
        bridge_tx_count,
        total_bridged_native,
        total_bridged_usd,
        last_bridge
    FROM bridge_users
    ORDER BY total_bridged_usd DESC
    LIMIT {CROSS_CHAIN_WHALE_TARGET}
    """

    results = run_query(bq, query, "High-Value Bridge Whale Discovery")
    addresses = []

    for r in results:
        confidence = min(0.95, 0.78 + min(0.12, float(r.total_bridged_usd) / 10_000_000 * 0.12)
                         + min(0.05, int(r.bridge_tx_count) * 0.01))

        addresses.append({
            'address': r.address,
            'blockchain': r.chain,
            'address_type': 'WHALE',
            'label': f'Bridge Whale ({r.chain.upper()}: ${float(r.total_bridged_usd):,.0f} bridged)',
            'entity_name': None,
            'confidence': round(confidence, 2),
            'signal_potential': 'HIGH',
            'source': 'bigquery_crosschain_verified',
            'detection_method': 'high_value_bridge_usage',
            'analysis_tags': {
                'bridge_tx_count': int(r.bridge_tx_count),
                'total_bridged_native': float(r.total_bridged_native),
                'total_bridged_usd': float(r.total_bridged_usd),
                'chain': r.chain,
            },
            'created_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat(),
        })

    return addresses


def discover_multichain_stablecoin_whales(bq):
    """
    Find addresses with large stablecoin transfers (>$500K per tx) on 2+ chains.
    Stablecoin whales are typically OTC desks, market makers, or institutions.
    """
    # ERC-20 Transfer event topic
    transfer_topic = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    min_stablecoin_raw = 500_000 * 10**6  # USDT/USDC use 6 decimals

    chain_queries = []
    for chain, cfg in EVM_CHAINS.items():
        stables = STABLECOINS.get(chain, {})
        if not stables:
            continue
        stable_list = ", ".join([f"'{addr.lower()}'" for addr in stables.values()])

        chain_queries.append(f"""
        SELECT
            CONCAT('0x', SUBSTR(topics[OFFSET(1)], 27)) AS address,
            '{chain}' AS chain,
            COUNT(*) AS large_stable_txs,
            COUNT(DISTINCT LOWER(address)) AS unique_stables_used
        FROM `bigquery-public-data.{cfg['dataset']}.logs`
        WHERE block_timestamp >= '{START_DATE}'
          AND topics[OFFSET(0)] = '{transfer_topic}'
          AND LOWER(address) IN ({stable_list})
          AND SAFE_CAST(CONCAT('0x', data) AS INT64) > {min_stablecoin_raw}
        GROUP BY address
        HAVING large_stable_txs >= 3
        """)

    if not chain_queries:
        return []

    combined = " UNION ALL ".join(chain_queries)
    query = f"""
    WITH per_chain AS ({combined}),
    cross_chain AS (
        SELECT
            address,
            COUNT(DISTINCT chain) AS num_chains,
            SUM(large_stable_txs) AS total_stable_txs,
            ARRAY_AGG(STRUCT(chain, large_stable_txs, unique_stables_used)) AS chain_details
        FROM per_chain
        GROUP BY address
        HAVING num_chains >= {MIN_CHAINS}
    )
    SELECT address, num_chains, total_stable_txs, chain_details
    FROM cross_chain
    ORDER BY total_stable_txs DESC
    LIMIT {CROSS_CHAIN_WHALE_TARGET}
    """

    results = run_query(bq, query, "Multi-Chain Stablecoin Whale Discovery")
    addresses = []

    for r in results:
        chains_active = [d['chain'] for d in r.chain_details]
        confidence = min(0.97, 0.82 + (int(r.num_chains) - 2) * 0.05
                         + min(0.10, int(r.total_stable_txs) * 0.002))

        for chain in chains_active:
            addresses.append({
                'address': r.address,
                'blockchain': chain,
                'address_type': 'WHALE',
                'label': f'Stablecoin Whale ({int(r.num_chains)} chains, '
                         f'{int(r.total_stable_txs)} large transfers)',
                'entity_name': None,
                'confidence': round(confidence, 2),
                'signal_potential': 'HIGH',
                'source': 'bigquery_crosschain_verified',
                'detection_method': 'multichain_stablecoin_volume',
                'analysis_tags': {
                    'num_chains': int(r.num_chains),
                    'chains_active': chains_active,
                    'total_stable_txs': int(r.total_stable_txs),
                },
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat(),
            })

    return addresses


def _group_by_chain(addresses: list) -> dict:
    """Group address records by blockchain for per-chain verification."""
    by_chain = {}
    for addr in addresses:
        chain = addr.get('blockchain', 'ethereum')
        by_chain.setdefault(chain, []).append(addr)
    return by_chain


def main():
    print("=" * 80)
    print("CROSS-CHAIN VERIFIED WHALE DISCOVERY")
    print("=" * 80)
    print(f"\n  Chains: {', '.join(c.upper() for c in EVM_CHAINS)}")
    print(f"  Methods: Shared-address whales, Bridge whales, Stablecoin whales")
    print(f"  Min chains: {MIN_CHAINS} | Min large txs/chain: {MIN_LARGE_TXS}")
    print(f"  Target: up to {CROSS_CHAIN_WHALE_TARGET:,} addresses per method")
    print()

    response = input("Start cross-chain whale discovery? (yes/no): ")
    if response.lower() != 'yes':
        return

    bq, sb = _init_clients()
    print(f"\n  BigQuery project: {bq.project}")
    print(f"  Supabase connected")

    # Get before counts
    before = sb.table('addresses').select('id', count='exact').execute()
    crosschain_before = (sb.table('addresses').select('id', count='exact')
                         .eq('source', 'bigquery_crosschain_verified').execute())
    print(f"\n  Database: {before.count:,} total addresses "
          f"({crosschain_before.count:,} cross-chain verified)")
    print()

    total_upserted = 0
    total_candidates = 0
    total_verified = 0

    # Method 1: Shared-address whales
    print("\n" + "-" * 60)
    print("METHOD 1: Shared-Address Whales")
    print("-" * 60)
    shared = discover_shared_address_whales(bq)
    total_candidates += len(shared)
    if shared:
        # Group by chain for per-chain verification
        by_chain = _group_by_chain(shared)
        for chain, addrs in by_chain.items():
            verified = verify_batch(addrs, chain, max_verify=500)
            total_verified += len(verified)
            if verified:
                total_upserted += upsert_addresses(sb, verified)

    # Method 2: Bridge whales
    print("\n" + "-" * 60)
    print("METHOD 2: High-Value Bridge Whales")
    print("-" * 60)
    bridge = discover_bridge_whales(bq)
    total_candidates += len(bridge)
    if bridge:
        by_chain = _group_by_chain(bridge)
        for chain, addrs in by_chain.items():
            verified = verify_batch(addrs, chain, max_verify=500)
            total_verified += len(verified)
            if verified:
                total_upserted += upsert_addresses(sb, verified)

    # Method 3: Multi-chain stablecoin whales
    print("\n" + "-" * 60)
    print("METHOD 3: Multi-Chain Stablecoin Whales")
    print("-" * 60)
    stable = discover_multichain_stablecoin_whales(bq)
    total_candidates += len(stable)
    if stable:
        by_chain = _group_by_chain(stable)
        for chain, addrs in by_chain.items():
            verified = verify_batch(addrs, chain, max_verify=500)
            total_verified += len(verified)
            if verified:
                total_upserted += upsert_addresses(sb, verified)

    # Summary
    after = sb.table('addresses').select('id', count='exact').execute()
    crosschain_after = (sb.table('addresses').select('id', count='exact')
                        .eq('source', 'bigquery_crosschain_verified').execute())
    new_addresses = after.count - before.count
    drop_rate = ((total_candidates - total_verified) / total_candidates * 100
                 if total_candidates > 0 else 0)

    print("\n" + "=" * 80)
    print("CROSS-CHAIN DISCOVERY COMPLETE")
    print("=" * 80)
    print(f"\n  BigQuery candidates:   {total_candidates:,}")
    print(f"  Passed verification:   {total_verified:,}")
    print(f"  Dropped (unverified):  {total_candidates - total_verified:,} ({drop_rate:.0f}%)")
    print(f"  Upserted to Supabase:  {total_upserted:,}")
    print(f"  Net new addresses:     {new_addresses:,}")
    print(f"\n  Database:")
    print(f"    Total:           {before.count:>8,} -> {after.count:>8,}")
    print(f"    Cross-chain:     {crosschain_before.count:>8,} -> {crosschain_after.count:>8,}")
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted")
        sys.exit(0)
    except Exception as e:
        print(f"\n\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
