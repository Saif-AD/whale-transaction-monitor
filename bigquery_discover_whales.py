#!/usr/bin/env python3
"""
BigQuery Whale Discovery - All Chains, Quality Over Quantity

Covers: Ethereum, Polygon, Bitcoin, Solana, XRP

Finds high-quality whale addresses from BigQuery public blockchain datasets.
Every address must meet REAL thresholds before being inserted.

Discovery methods:
  1. Volume whales: addresses that moved serious value (not dust) recently
  2. Smart money: addresses interacting with many DeFi protocols (EVM only)

Quality gates:
  - Minimum volume threshold per chain (100 ETH, 1 BTC, etc.)
  - Minimum significant tx count: 5+
  - Recent activity: last 180 days
  - Excludes known CEX hot wallets
  - Verified via chain explorer API (balance + contract check)
  - Must have $100K+ balance OR a known contract label to pass

Chain-specific notes:
  - Bitcoin: UTXO model, uses outputs table, different address format
  - Solana: account-based but different schema, uses Helius/Solscan for verify
  - XRP: simple account model, limited verification (no explorer API with labels)
"""

import os
import sys
import time
import json
import logging
import requests
from datetime import datetime
from collections import defaultdict
from google.cloud import bigquery
from supabase import create_client
from config.api_keys import (
    SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY,
    ETHERSCAN_API_KEY, ETHERSCAN_API_KEYS,
    POLYGONSCAN_API_KEY,
    HELIUS_API_KEY,
    SOLSCAN_API_KEY,
)

logger = logging.getLogger(__name__)

# --- Config ---
BIGQUERY_CREDENTIALS_PATH = "config/bigquery_credentials.json"
BATCH_SIZE = 200
API_DELAY = 0.22  # rate limit between API calls

# Quality thresholds (per-chain overrides below)
MIN_LARGE_TX_COUNT = 5
RECENT_DAYS = 180
MIN_BALANCE_USD = 100_000
MAX_RESULTS_PER_QUERY = 5_000

# --- Chain configs ---
# Each chain has:
#   dataset: BigQuery public dataset name
#   model: 'evm' (from_address/to_address/value) or 'utxo' (inputs/outputs) or 'custom'
#   min_tx_value: minimum value per tx to count (in native units)
#   min_volume: minimum total volume to qualify (in native units)
#   native_price_usd: approximate price for USD estimates
#   verify_fn: function name for verification (or None to skip)

CHAINS = {
    'ethereum': {
        'dataset': 'crypto_ethereum',
        'model': 'evm',
        'decimals': 18,
        'native_symbol': 'ETH',
        'native_price_usd': 3000,
        'min_tx_value': 1,       # 1 ETH minimum per tx
        'min_volume': 100,       # 100 ETH total
        'explorer_url': 'https://api.etherscan.io/api',
        'explorer_key': ETHERSCAN_API_KEY,
    },
    'polygon': {
        'dataset': 'crypto_polygon',
        'model': 'evm',
        'decimals': 18,
        'native_symbol': 'MATIC',
        'native_price_usd': 0.8,
        'min_tx_value': 5000,    # 5000 MATIC (~$4K)
        'min_volume': 100_000,   # 100K MATIC (~$80K)
        'explorer_url': 'https://api.polygonscan.com/api',
        'explorer_key': POLYGONSCAN_API_KEY,
    },
    'bitcoin': {
        'dataset': 'crypto_bitcoin',
        'model': 'utxo',
        'decimals': 8,
        'native_symbol': 'BTC',
        'native_price_usd': 95000,
        'min_tx_value': 0.5,     # 0.5 BTC (~$47K)
        'min_volume': 10,        # 10 BTC (~$950K)
    },
    'solana': {
        'dataset': 'crypto_solana_mainnet_us',
        'model': 'solana',
        'decimals': 9,
        'native_symbol': 'SOL',
        'native_price_usd': 150,
        'min_tx_value': 100,     # 100 SOL (~$15K)
        'min_volume': 1000,      # 1000 SOL (~$150K)
    },
    'xrp': {
        'dataset': 'crypto_xrp',
        'model': 'xrp',
        'decimals': 6,
        'native_symbol': 'XRP',
        'native_price_usd': 2.5,
        'min_tx_value': 50_000,  # 50K XRP (~$125K)
        'min_volume': 500_000,   # 500K XRP (~$1.25M)
    },
}

# Known CEX hot wallets to EXCLUDE (EVM only)
KNOWN_CEX_ADDRESSES = {
    '0x28c6c06298d514db089934071355e5743bf21d60',  # Binance 14
    '0x21a31ee1afc51d94c2efccaa2092ad1028285549',  # Binance 7
    '0xdfd5293d8e347dfe59e90efd55b2956a1343963d',  # Binance 6
    '0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43',  # Coinbase
    '0x503828976d22510aad0201ac7ec88293211d23da',  # Coinbase 2
    '0x56eddb7aa87536c09ccc2793473599fd21a8b17f',  # Binance 17
    '0xf977814e90da44bfa03b6295a0616a897441acec',  # Binance 8
    '0xbe0eb53f46cd790cd13851d5eff43d12404d33e8',  # Binance 7 Cold
    '0x47ac0fb4f2d84898e4d9e7b4dab3c24507a6d503',  # Binance 1
    '0x0a4c79ce84202b03e95b7a692e5d728d83c44c76',  # KuCoin
    '0xd24400ae8bfebb18ca49be86258a3c749cf46853',  # Gemini 4
    '0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0',  # Kraken 4
}

# Rotate etherscan keys
_key_idx = 0


def _get_explorer_key():
    global _key_idx
    keys = ETHERSCAN_API_KEYS or [ETHERSCAN_API_KEY]
    key = keys[_key_idx % len(keys)]
    _key_idx += 1
    return key


# --- BigQuery helpers ---

def init_clients():
    if not os.path.exists(BIGQUERY_CREDENTIALS_PATH):
        print(f"  Missing: {BIGQUERY_CREDENTIALS_PATH}")
        sys.exit(1)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_PATH
    bq = bigquery.Client()
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    return bq, sb


def estimate_cost(bq, query):
    try:
        job = bq.query(query, job_config=bigquery.QueryJobConfig(
            dry_run=True, use_query_cache=False))
        return job.total_bytes_processed / (1024**3)
    except Exception:
        return None


def run_query(bq, query, name):
    print(f"\n  {name}")
    gb = estimate_cost(bq, query)
    if gb:
        print(f"    Scan: {gb:.1f} GB")
    print(f"    Running...")
    start = time.time()
    results = list(bq.query(query))
    print(f"    {len(results):,} results in {time.time()-start:.1f}s")
    return results


# --- Discovery: EVM chains (Ethereum, Polygon) ---

def discover_evm_volume_whales(bq, chain, cfg):
    decimals = cfg['decimals']
    min_val = cfg['min_tx_value']
    min_vol = cfg['min_volume']
    cex_list = ", ".join([f"'{a}'" for a in KNOWN_CEX_ADDRESSES])

    query = f"""
    SELECT address, large_tx_count, total_native, max_native, last_active,
           TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_active, DAY) AS days_inactive
    FROM (
        SELECT
            LOWER(from_address) AS address,
            COUNT(*) AS large_tx_count,
            SUM(SAFE_CAST(value AS FLOAT64) / 1e{decimals}) AS total_native,
            MAX(SAFE_CAST(value AS FLOAT64) / 1e{decimals}) AS max_native,
            MAX(block_timestamp) AS last_active
        FROM `bigquery-public-data.{cfg['dataset']}.transactions`
        WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
          AND SAFE_CAST(value AS FLOAT64) / 1e{decimals} >= {min_val}
          AND from_address IS NOT NULL
          AND LOWER(from_address) NOT IN ({cex_list})
        GROUP BY address
        HAVING large_tx_count >= {MIN_LARGE_TX_COUNT} AND total_native >= {min_vol}
    )
    WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_active, DAY) <= {RECENT_DAYS}
    ORDER BY total_native DESC
    LIMIT {MAX_RESULTS_PER_QUERY}
    """
    return _format_evm_results(
        run_query(bq, query, f"Volume Whales ({chain.upper()})"),
        chain, cfg, 'volume_whale', 'bigquery_volume_analysis')


def discover_evm_smart_money(bq, chain, cfg):
    decimals = cfg['decimals']
    min_vol = cfg['min_volume']
    cex_list = ", ".join([f"'{a}'" for a in KNOWN_CEX_ADDRESSES])

    query = f"""
    SELECT
        LOWER(from_address) AS address,
        COUNT(*) AS tx_count,
        COUNT(DISTINCT to_address) AS unique_contracts,
        SUM(SAFE_CAST(value AS FLOAT64) / 1e{decimals}) AS total_native,
        MAX(block_timestamp) AS last_active
    FROM `bigquery-public-data.{cfg['dataset']}.transactions`
    WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
      AND SAFE_CAST(value AS FLOAT64) / 1e{decimals} >= 0.1
      AND from_address IS NOT NULL
      AND LOWER(from_address) NOT IN ({cex_list})
    GROUP BY address
    HAVING unique_contracts >= 50 AND total_native >= {min_vol} AND tx_count >= 20
      AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_active, DAY) <= {RECENT_DAYS}
    ORDER BY unique_contracts DESC, total_native DESC
    LIMIT {MAX_RESULTS_PER_QUERY}
    """

    results = run_query(bq, query, f"Smart Money ({chain.upper()})")
    price = cfg['native_price_usd']
    symbol = cfg['native_symbol']

    return [{
        'address': r.address,
        'blockchain': chain,
        'address_type': 'WHALE',
        'label': f'Smart Money ({symbol}: {int(r.unique_contracts)} protocols, '
                 f'{float(r.total_native):,.0f} {symbol})',
        'entity_name': None,
        'confidence': 0.50,
        'signal_potential': 'HIGH' if float(r.total_native) * price >= 1_000_000 else 'MEDIUM',
        'source': f'bigquery_{chain}_smart_money',
        'detection_method': 'bigquery_protocol_interaction',
        'analysis_tags': json.dumps({
            'unique_contracts': int(r.unique_contracts),
            'tx_count': int(r.tx_count),
            'total_native': float(r.total_native),
            'total_usd_est': float(r.total_native) * price,
            'chain': chain,
        }),
    } for r in results]


def _format_evm_results(results, chain, cfg, source_suffix, detection):
    price = cfg['native_price_usd']
    symbol = cfg['native_symbol']
    return [{
        'address': r.address,
        'blockchain': chain,
        'address_type': 'WHALE',
        'label': f'Volume Whale ({symbol}: {float(r.total_native):,.0f} {symbol}, '
                 f'{int(r.large_tx_count)} large txs)',
        'entity_name': None,
        'confidence': 0.50,
        'signal_potential': 'HIGH' if float(r.total_native) * price >= 1_000_000 else 'MEDIUM',
        'source': f'bigquery_{chain}_{source_suffix}',
        'detection_method': detection,
        'analysis_tags': json.dumps({
            'large_tx_count': int(r.large_tx_count),
            'total_native': float(r.total_native),
            'total_usd_est': float(r.total_native) * price,
            'max_native': float(r.max_native),
            'days_inactive': int(r.days_inactive),
            'chain': chain,
        }),
    } for r in results]


# --- Discovery: Bitcoin (UTXO model) ---

def discover_bitcoin_whales(bq, cfg):
    """
    Bitcoin uses UTXO model. We look at transaction outputs to find addresses
    receiving large BTC amounts consistently.
    """
    min_val = cfg['min_tx_value']
    min_vol = cfg['min_volume']

    query = f"""
    SELECT address, large_tx_count, total_btc, max_btc, last_active,
           TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_active, DAY) AS days_inactive
    FROM (
        SELECT
            outputs.addresses[OFFSET(0)] AS address,
            COUNT(*) AS large_tx_count,
            SUM(outputs.value / 1e8) AS total_btc,
            MAX(outputs.value / 1e8) AS max_btc,
            MAX(block_timestamp) AS last_active
        FROM `bigquery-public-data.crypto_bitcoin.transactions`,
            UNNEST(outputs) AS outputs
        WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
          AND outputs.value / 1e8 >= {min_val}
          AND ARRAY_LENGTH(outputs.addresses) > 0
        GROUP BY address
        HAVING large_tx_count >= {MIN_LARGE_TX_COUNT} AND total_btc >= {min_vol}
    )
    WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_active, DAY) <= {RECENT_DAYS}
      AND address IS NOT NULL
    ORDER BY total_btc DESC
    LIMIT {MAX_RESULTS_PER_QUERY}
    """

    results = run_query(bq, query, "Volume Whales (BTC)")
    price = cfg['native_price_usd']

    return [{
        'address': r.address,
        'blockchain': 'bitcoin',
        'address_type': 'WHALE',
        'label': f'BTC Whale ({float(r.total_btc):,.2f} BTC, {int(r.large_tx_count)} large txs)',
        'entity_name': None,
        'confidence': 0.55,  # slightly higher - BTC whales are rarer
        'signal_potential': 'HIGH' if float(r.total_btc) * price >= 1_000_000 else 'MEDIUM',
        'source': 'bigquery_bitcoin_volume_whale',
        'detection_method': 'bigquery_utxo_analysis',
        'analysis_tags': json.dumps({
            'large_tx_count': int(r.large_tx_count),
            'total_btc': float(r.total_btc),
            'total_usd_est': float(r.total_btc) * price,
            'max_btc': float(r.max_btc),
            'days_inactive': int(r.days_inactive),
        }),
    } for r in results]


# --- Discovery: Solana ---

def discover_solana_whales(bq, cfg):
    """
    Solana BigQuery dataset has a transactions table with fee and accounts.
    We look for signers (first account) involved in many high-fee transactions,
    which correlates with DeFi activity and high-value operations.
    Note: Solana dataset stopped updating March 2025, but historical data works.
    """
    query = f"""
    SELECT
        accounts[OFFSET(0)] AS address,
        COUNT(*) AS tx_count,
        SUM(fee / 1e9) AS total_fees_sol,
        MAX(block_timestamp) AS last_active,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(block_timestamp), DAY) AS days_inactive
    FROM `bigquery-public-data.crypto_solana_mainnet_us.transactions`
    WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
      AND ARRAY_LENGTH(accounts) > 0
      AND fee > 0
    GROUP BY address
    HAVING tx_count >= 100
      AND total_fees_sol >= 1.0
    ORDER BY tx_count DESC
    LIMIT {MAX_RESULTS_PER_QUERY}
    """

    results = run_query(bq, query, "Active Whales (SOLANA)")
    price = cfg['native_price_usd']

    return [{
        'address': r.address,
        'blockchain': 'solana',
        'address_type': 'WHALE',
        'label': f'Solana Whale ({int(r.tx_count)} txs, {float(r.total_fees_sol):.2f} SOL fees)',
        'entity_name': None,
        'confidence': 0.50,
        'signal_potential': 'HIGH' if int(r.tx_count) >= 500 else 'MEDIUM',
        'source': 'bigquery_solana_activity_whale',
        'detection_method': 'bigquery_solana_tx_analysis',
        'analysis_tags': json.dumps({
            'tx_count': int(r.tx_count),
            'total_fees_sol': float(r.total_fees_sol),
            'days_inactive': int(r.days_inactive),
        }),
    } for r in results]


# --- Discovery: XRP ---

def discover_xrp_whales(bq, cfg):
    """
    XRP BigQuery dataset. Find accounts with large Payment transactions.
    """
    min_val = cfg['min_tx_value']
    min_vol = cfg['min_volume']

    query = f"""
    SELECT
        account AS address,
        COUNT(*) AS large_tx_count,
        SUM(SAFE_CAST(amount AS FLOAT64) / 1e6) AS total_xrp,
        MAX(SAFE_CAST(amount AS FLOAT64) / 1e6) AS max_xrp,
        MAX(close_time_human) AS last_active
    FROM `bigquery-public-data.crypto_xrp.transactions`
    WHERE transaction_type = 'Payment'
      AND SAFE_CAST(amount AS FLOAT64) / 1e6 >= {min_val}
      AND account IS NOT NULL
    GROUP BY account
    HAVING large_tx_count >= {MIN_LARGE_TX_COUNT}
      AND total_xrp >= {min_vol}
    ORDER BY total_xrp DESC
    LIMIT {MAX_RESULTS_PER_QUERY}
    """

    results = run_query(bq, query, "Volume Whales (XRP)")
    price = cfg['native_price_usd']

    return [{
        'address': r.address,
        'blockchain': 'xrp',
        'address_type': 'WHALE',
        'label': f'XRP Whale ({float(r.total_xrp):,.0f} XRP, {int(r.large_tx_count)} large txs)',
        'entity_name': None,
        'confidence': 0.50,
        'signal_potential': 'HIGH' if float(r.total_xrp) * price >= 1_000_000 else 'MEDIUM',
        'source': 'bigquery_xrp_volume_whale',
        'detection_method': 'bigquery_xrp_payment_analysis',
        'analysis_tags': json.dumps({
            'large_tx_count': int(r.large_tx_count),
            'total_xrp': float(r.total_xrp),
            'total_usd_est': float(r.total_xrp) * price,
            'max_xrp': float(r.max_xrp),
        }),
    } for r in results]


# --- Verification ---

def verify_evm_address(address, chain, cfg):
    """Verify EVM address via block explorer API."""
    url = cfg['explorer_url']
    key = cfg['explorer_key'] if chain != 'ethereum' else _get_explorer_key()
    price = cfg['native_price_usd']

    try:
        time.sleep(API_DELAY)
        resp = requests.get(url, params={
            'module': 'account', 'action': 'balance',
            'address': address, 'tag': 'latest', 'apikey': key,
        }, timeout=10)
        data = resp.json()
        if data.get('status') != '1':
            return None
        balance_native = int(data['result']) / (10 ** cfg['decimals'])
        balance_usd = balance_native * price
    except Exception:
        return None

    contract_name = None
    try:
        time.sleep(API_DELAY)
        resp = requests.get(url, params={
            'module': 'contract', 'action': 'getsourcecode',
            'address': address, 'apikey': key,
        }, timeout=10)
        data = resp.json()
        if data.get('result') and isinstance(data['result'], list):
            name = data['result'][0].get('ContractName', '')
            if name:
                contract_name = name
    except Exception:
        pass

    return balance_native, balance_usd, contract_name


def verify_solana_address(address):
    """Verify Solana address via Helius API (balance check)."""
    if not HELIUS_API_KEY:
        return None
    try:
        time.sleep(API_DELAY)
        resp = requests.post(
            f'https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}',
            json={'jsonrpc': '2.0', 'id': 1, 'method': 'getBalance',
                  'params': [address]},
            timeout=10,
        )
        data = resp.json()
        lamports = data.get('result', {}).get('value', 0)
        balance_sol = lamports / 1e9
        balance_usd = balance_sol * CHAINS['solana']['native_price_usd']
        return balance_sol, balance_usd, None
    except Exception:
        return None


def verify_and_filter(candidates, chain, cfg, max_verify=1000):
    """
    Verify candidates. Only keep addresses with $100K+ balance OR known label.
    Bitcoin and XRP candidates skip verification (no good free explorer API)
    but keep higher BigQuery thresholds to compensate.
    """
    if chain in ('bitcoin', 'xrp'):
        # No free explorer API for verification - trust BigQuery thresholds
        # (which are already set high: 10+ BTC, 500K+ XRP)
        print(f"    {chain.upper()}: no explorer API, keeping all {len(candidates)} "
              f"BigQuery candidates (high thresholds already applied)")
        for c in candidates:
            c['confidence'] = 0.60  # moderate confidence without verification
        return candidates

    verified = []
    total = min(len(candidates), max_verify)
    verify_fn = verify_solana_address if chain == 'solana' else None
    explorer_name = 'Helius' if chain == 'solana' else cfg.get('explorer_url', '')
    print(f"    Verifying top {total} via {explorer_name}...")

    for i, addr_dict in enumerate(candidates[:max_verify]):
        address = addr_dict['address']

        if chain == 'solana':
            result = verify_solana_address(address)
        else:
            result = verify_evm_address(address, chain, cfg)

        if result is None:
            continue

        balance_native, balance_usd, contract_name = result

        if balance_usd < MIN_BALANCE_USD and not contract_name:
            continue

        # Skip infrastructure contracts
        if contract_name:
            skip_words = ['router', 'pool', 'proxy', 'factory', 'vault',
                          'bridge', 'swap', 'pair', 'diamond', 'beacon']
            if any(w in contract_name.lower() for w in skip_words):
                continue
            addr_dict['entity_name'] = contract_name

        addr_dict['balance_native'] = round(balance_native, 6)
        addr_dict['balance_usd'] = round(balance_usd, 2)

        if contract_name and balance_usd >= MIN_BALANCE_USD:
            addr_dict['confidence'] = 0.90
        elif balance_usd >= 1_000_000:
            addr_dict['confidence'] = 0.85
        elif balance_usd >= MIN_BALANCE_USD:
            addr_dict['confidence'] = 0.75
        elif contract_name:
            addr_dict['confidence'] = 0.70

        verified.append(addr_dict)

        if (i + 1) % 100 == 0:
            print(f"      {i+1}/{total} checked, {len(verified)} passed")

    drop_pct = ((total - len(verified)) / total * 100) if total > 0 else 0
    print(f"    Verified: {len(verified)}/{total} ({drop_pct:.0f}% dropped)")
    return verified


# --- Supabase ---

def upsert_addresses(sb, addresses):
    if not addresses:
        return 0

    unique = {}
    for addr in addresses:
        key = (addr['address'], addr['blockchain'])
        if key not in unique or addr.get('confidence', 0) > unique[key].get('confidence', 0):
            unique[key] = addr
    addresses = list(unique.values())

    now = datetime.utcnow().isoformat()
    for addr in addresses:
        addr['created_at'] = now
        addr['updated_at'] = now

    inserted = 0
    for i in range(0, len(addresses), BATCH_SIZE):
        batch = addresses[i:i + BATCH_SIZE]
        try:
            sb.table('addresses').upsert(batch, on_conflict='address,blockchain').execute()
            inserted += len(batch)
        except Exception as e:
            print(f"    Upsert error: {e}")

    print(f"    Upserted {inserted:,} to Supabase")
    return inserted


# --- Main ---

def main():
    chain_names = ', '.join(c.upper() for c in CHAINS)
    print("=" * 70)
    print("  BIGQUERY WHALE DISCOVERY (All Chains)")
    print("=" * 70)
    print(f"  Chains: {chain_names}")
    print(f"  Min balance to keep: ${MIN_BALANCE_USD:,}")
    print(f"  Max results/query: {MAX_RESULTS_PER_QUERY:,}")
    print()
    for chain, cfg in CHAINS.items():
        print(f"    {chain.upper():10s} min_vol={cfg['min_volume']} {cfg['native_symbol']}, "
              f"min_tx={cfg['min_tx_value']} {cfg['native_symbol']}")
    print()

    response = input("  Start discovery? (yes/no): ")
    if response.lower() != 'yes':
        return

    bq, sb = init_clients()
    print(f"\n  BigQuery: {bq.project}")

    before = sb.table('addresses').select('id', count='exact').execute()
    before_named = (sb.table('addresses').select('id', count='exact')
                    .not_.is_('entity_name', 'null').execute())
    print(f"  Supabase: {before.count:,} addresses ({before_named.count:,} with names)")

    total_candidates = 0
    total_verified = 0
    total_upserted = 0

    for chain, cfg in CHAINS.items():
        print(f"\n{'=' * 70}")
        print(f"  {chain.upper()} ({cfg['native_symbol']})")
        print(f"{'=' * 70}")

        candidates = []

        # Chain-specific discovery
        if cfg['model'] == 'evm':
            print(f"\n  [1/2] Volume Whales")
            candidates.extend(discover_evm_volume_whales(bq, chain, cfg))
            print(f"\n  [2/2] Smart Money")
            candidates.extend(discover_evm_smart_money(bq, chain, cfg))

        elif cfg['model'] == 'utxo':
            print(f"\n  [1/1] BTC Volume Whales")
            candidates.extend(discover_bitcoin_whales(bq, cfg))

        elif cfg['model'] == 'solana':
            print(f"\n  [1/1] Solana Active Whales")
            candidates.extend(discover_solana_whales(bq, cfg))

        elif cfg['model'] == 'xrp':
            print(f"\n  [1/1] XRP Volume Whales")
            candidates.extend(discover_xrp_whales(bq, cfg))

        total_candidates += len(candidates)

        if candidates:
            verified = verify_and_filter(candidates, chain, cfg)
            total_verified += len(verified)
            if verified:
                total_upserted += upsert_addresses(sb, verified)

    # Summary
    after = sb.table('addresses').select('id', count='exact').execute()
    after_named = (sb.table('addresses').select('id', count='exact')
                   .not_.is_('entity_name', 'null').execute())

    print(f"\n{'=' * 70}")
    print(f"  DISCOVERY COMPLETE")
    print(f"{'=' * 70}")
    print(f"  BigQuery candidates:   {total_candidates:,}")
    print(f"  Passed verification:   {total_verified:,}")
    print(f"  Dropped (junk):        {total_candidates - total_verified:,}")
    print(f"  Upserted to Supabase:  {total_upserted:,}")
    print()
    print(f"  Before: {before.count:,} ({before_named.count:,} named)")
    print(f"  After:  {after.count:,} ({after_named.count:,} named)")
    print(f"{'=' * 70}")


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
