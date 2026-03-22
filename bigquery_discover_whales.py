#!/usr/bin/env python3
"""
BigQuery Verified Whale Discovery

Discovers high-volume addresses from BigQuery public blockchain datasets,
verifies balances via chain-specific APIs, and upserts quality-gated records
into the Supabase `addresses` table.

Supported chains: Ethereum, Polygon, Bitcoin, Solana, XRP
"""

import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from supabase import create_client

from config.api_keys import (
    ETHERSCAN_API_KEYS,
    GOOGLE_APPLICATION_CREDENTIALS,
    GCP_PROJECT_ID,
    HELIUS_API_KEY,
    POLYGONSCAN_API_KEY,
    SUPABASE_SERVICE_ROLE_KEY,
    SUPABASE_URL,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("discover")

# ---------------------------------------------------------------------------
# Chain configs
# ---------------------------------------------------------------------------

CHAIN_CONFIGS: Dict[str, Dict[str, Any]] = {
    "ethereum": {
        "blockchain": "ethereum",
        "table": "bigquery-public-data.crypto_ethereum.transactions",
        "query_template": """
            SELECT
                address,
                SUM(vol) AS total_volume,
                COUNT(*) AS tx_count,
                AVG(vol) AS avg_per_tx
            FROM (
                SELECT from_address AS address, value / POW(10, 18) AS vol
                FROM `{table}`
                WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                  AND value / POW(10, 18) >= 0.5
                UNION ALL
                SELECT to_address AS address, value / POW(10, 18) AS vol
                FROM `{table}`
                WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                  AND value / POW(10, 18) >= 0.5
            )
            GROUP BY address
            HAVING total_volume >= {min_volume}
               AND avg_per_tx >= {min_avg}
               AND tx_count >= 2
            ORDER BY total_volume DESC
            LIMIT {limit}
        """,
        "min_volume": 10,
        "min_avg": 0.5,
        "native_symbol": "ETH",
        "label_prefix": "High-Volume ETH Transactor",
        "source": "bigquery_eth_volume_discovery",
    },
    "polygon": {
        "blockchain": "polygon",
        "table": "bigquery-public-data.crypto_polygon.transactions",
        "query_template": """
            SELECT
                address,
                SUM(vol) AS total_volume,
                COUNT(*) AS tx_count,
                AVG(vol) AS avg_per_tx
            FROM (
                SELECT from_address AS address, value / POW(10, 18) AS vol
                FROM `{table}`
                WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                  AND value / POW(10, 18) >= 1000
                UNION ALL
                SELECT to_address AS address, value / POW(10, 18) AS vol
                FROM `{table}`
                WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                  AND value / POW(10, 18) >= 1000
            )
            GROUP BY address
            HAVING total_volume >= {min_volume}
               AND avg_per_tx >= {min_avg}
               AND tx_count >= 2
            ORDER BY total_volume DESC
            LIMIT {limit}
        """,
        "min_volume": 25000,
        "min_avg": 1000,
        "native_symbol": "MATIC",
        "label_prefix": "High-Volume MATIC Transactor",
        "source": "bigquery_polygon_volume_discovery",
    },
    "bitcoin": {
        "blockchain": "bitcoin",
        "table": "bigquery-public-data.crypto_bitcoin.transactions",
        "query_template": """
            SELECT
                address,
                SUM(btc_value) AS total_volume,
                COUNT(*) AS tx_count,
                AVG(btc_value) AS avg_per_tx
            FROM (
                SELECT
                    outputs.addresses[OFFSET(0)] AS address,
                    outputs.value / 1e8 AS btc_value
                FROM `{table}`, UNNEST(outputs) AS outputs
                WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                  AND ARRAY_LENGTH(outputs.addresses) > 0
                  AND outputs.value / 1e8 >= 0.1
            )
            GROUP BY address
            HAVING total_volume >= {min_volume}
               AND avg_per_tx >= {min_avg}
               AND tx_count >= 2
            ORDER BY total_volume DESC
            LIMIT {limit}
        """,
        "min_volume": 1,
        "min_avg": 0.1,
        "native_symbol": "BTC",
        "label_prefix": "High-Volume BTC Transactor",
        "source": "bigquery_btc_volume_discovery",
    },
    "solana": {
        "blockchain": "solana",
        "table": "bigquery-public-data.crypto_solana_mainnet_us.Transactions",
        "query_template": """
            SELECT
                signer AS address,
                COUNT(*) AS tx_count,
                0 AS total_volume,
                0 AS avg_per_tx
            FROM `{table}`,
                 UNNEST(signers) AS signer
            WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
              AND err IS NULL
            GROUP BY signer
            HAVING tx_count >= {min_tx_count}
            ORDER BY tx_count DESC
            LIMIT {limit}
        """,
        "min_volume": 100,
        "min_avg": 0,
        "min_tx_count": 20,
        "native_symbol": "SOL",
        "label_prefix": "High-Activity SOL Transactor",
        "source": "bigquery_solana_activity_discovery",
    },
    "xrp": {
        "blockchain": "xrp",
        "table": "bigquery-public-data.crypto_xrp.transactions",
        "query_template": """
            SELECT
                Account AS address,
                SUM(SAFE_CAST(Amount AS FLOAT64) / 1e6) AS total_volume,
                COUNT(*) AS tx_count,
                AVG(SAFE_CAST(Amount AS FLOAT64) / 1e6) AS avg_per_tx
            FROM `{table}`
            WHERE TransactionType = 'Payment'
              AND ledger_close_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
              AND SAFE_CAST(Amount AS FLOAT64) / 1e6 >= 5000
            GROUP BY Account
            HAVING total_volume >= {min_volume}
               AND avg_per_tx >= {min_avg}
               AND tx_count >= 2
            ORDER BY total_volume DESC
            LIMIT {limit}
        """,
        "min_volume": 50000,
        "min_avg": 5000,
        "native_symbol": "XRP",
        "label_prefix": "High-Volume XRP Transactor",
        "source": "bigquery_xrp_volume_discovery",
    },
}

MAX_RESULTS_PER_CHAIN = 10_000
MIN_BALANCE_USD = 10_000

# Rough USD prices for balance checks (will be updated at runtime via CoinGecko)
_PRICES: Dict[str, float] = {
    "ETH": 3500.0,
    "MATIC": 0.50,
    "BTC": 85000.0,
    "SOL": 140.0,
    "XRP": 2.20,
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rotate_etherscan_key(idx: List[int] = [0]) -> str:
    key = ETHERSCAN_API_KEYS[idx[0] % len(ETHERSCAN_API_KEYS)]
    idx[0] += 1
    return key


def fetch_coingecko_prices() -> None:
    """Best-effort price refresh from CoinGecko."""
    try:
        from config.api_keys import COINGECKO_API_KEY
        url = "https://pro-api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": "ethereum,matic-network,bitcoin,solana,ripple",
            "vs_currencies": "usd",
            "x_cg_pro_api_key": COINGECKO_API_KEY,
        }
        r = requests.get(url, params=params, timeout=10)
        if r.ok:
            data = r.json()
            mapping = {
                "ethereum": "ETH",
                "matic-network": "MATIC",
                "bitcoin": "BTC",
                "solana": "SOL",
                "ripple": "XRP",
            }
            for cg_id, sym in mapping.items():
                if cg_id in data:
                    _PRICES[sym] = data[cg_id]["usd"]
            log.info(f"Prices updated: {_PRICES}")
    except Exception as e:
        log.warning(f"CoinGecko price fetch failed, using defaults: {e}")


# ---------------------------------------------------------------------------
# BigQuery discovery
# ---------------------------------------------------------------------------

def init_bigquery() -> bigquery.Client:
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_APPLICATION_CREDENTIALS,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    project = creds.project_id or GCP_PROJECT_ID
    client = bigquery.Client(credentials=creds, project=project)
    log.info(f"BigQuery client ready (project={project})")
    return client


def discover_chain(client: bigquery.Client, chain: str) -> List[Dict[str, Any]]:
    cfg = CHAIN_CONFIGS[chain]
    limit = MAX_RESULTS_PER_CHAIN

    if chain == "solana":
        sql = cfg["query_template"].format(
            table=cfg["table"],
            min_tx_count=cfg.get("min_tx_count", 20),
            limit=limit,
        )
    else:
        sql = cfg["query_template"].format(
            table=cfg["table"],
            min_volume=cfg["min_volume"],
            min_avg=cfg["min_avg"],
            limit=limit,
        )

    log.info(f"[{chain}] Running BigQuery discovery...")
    job = client.query(sql)
    rows = list(job.result())
    log.info(f"[{chain}] BigQuery returned {len(rows)} candidate addresses")
    return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Verification gates
# ---------------------------------------------------------------------------

def verify_evm_address(address: str, chain: str) -> Optional[Dict[str, Any]]:
    """Check balance and contract status via Etherscan/PolygonScan."""
    if chain == "ethereum":
        base_url = "https://api.etherscan.io/api"
        api_key = _rotate_etherscan_key()
    elif chain == "polygon":
        base_url = "https://api.polygonscan.com/api"
        api_key = POLYGONSCAN_API_KEY
    else:
        return None

    try:
        r = requests.get(base_url, params={
            "module": "account",
            "action": "balance",
            "address": address,
            "apikey": api_key,
        }, timeout=10)
        data = r.json()
        if data.get("status") != "1":
            return None
        balance_wei = int(data["result"])
        balance_native = balance_wei / 1e18

        sym = "ETH" if chain == "ethereum" else "MATIC"
        balance_usd = balance_native * _PRICES.get(sym, 0)

        r2 = requests.get(base_url, params={
            "module": "proxy",
            "action": "eth_getCode",
            "address": address,
            "apikey": api_key,
        }, timeout=10)
        code_data = r2.json()
        is_contract = code_data.get("result", "0x") not in ("0x", "0x0", "")

        return {
            "balance_native": balance_native,
            "balance_usd": balance_usd,
            "is_contract": is_contract,
        }
    except Exception as e:
        log.debug(f"EVM verify failed for {address}: {e}")
        return None


def verify_solana_address(address: str) -> Optional[Dict[str, Any]]:
    """Check SOL balance via Helius."""
    if not HELIUS_API_KEY:
        return None
    try:
        url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [address],
        }
        r = requests.post(url, json=payload, timeout=10)
        data = r.json()
        lamports = data.get("result", {}).get("value", 0)
        balance_sol = lamports / 1e9
        balance_usd = balance_sol * _PRICES.get("SOL", 0)
        return {"balance_native": balance_sol, "balance_usd": balance_usd, "is_contract": False}
    except Exception as e:
        log.debug(f"Solana verify failed for {address}: {e}")
        return None


def verify_address(address: str, chain: str) -> Optional[Dict[str, Any]]:
    if chain in ("ethereum", "polygon"):
        return verify_evm_address(address, chain)
    elif chain == "solana":
        return verify_solana_address(address)
    # Bitcoin and XRP: no easy free balance API with high rate limits.
    # We accept BigQuery-discovered addresses for these chains without
    # live balance verification, but with lower starting confidence.
    return {"balance_native": 0, "balance_usd": 0, "is_contract": False, "unverified": True}


# ---------------------------------------------------------------------------
# Supabase upsert
# ---------------------------------------------------------------------------

def get_existing_named_addresses(sb, chain: str) -> set:
    """Return addresses on this chain that already have a real entity_name."""
    try:
        result = (
            sb.table("addresses")
            .select("address")
            .eq("blockchain", chain)
            .not_.is_("entity_name", "null")
            .execute()
        )
        return {r["address"].lower() for r in (result.data or [])}
    except Exception as e:
        log.warning(f"Failed to fetch existing named addresses for {chain}: {e}")
        return set()


def upsert_batch(sb, records: List[Dict], stats: Dict[str, int]) -> None:
    if not records:
        return
    try:
        sb.table("addresses").upsert(records, on_conflict="address,blockchain").execute()
        stats["upserted"] += len(records)
    except Exception:
        for record in records:
            try:
                sb.table("addresses").upsert(record, on_conflict="address,blockchain").execute()
                stats["upserted"] += 1
            except Exception as e2:
                stats["errors"] += 1
                if stats["errors"] <= 5:
                    log.warning(f"Upsert error: {e2}")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

BATCH_SIZE = 100
API_RATE_DELAY = 0.22  # ~5 req/s per Etherscan free tier


def process_chain(bq_client: bigquery.Client, sb, chain: str, dry_run: bool = False) -> Dict[str, int]:
    cfg = CHAIN_CONFIGS[chain]
    stats: Dict[str, int] = defaultdict(int)

    candidates = discover_chain(bq_client, chain)
    stats["bq_candidates"] = len(candidates)
    if not candidates:
        return dict(stats)

    skip_set = get_existing_named_addresses(sb, cfg["blockchain"])
    log.info(f"[{chain}] {len(skip_set)} addresses already have entity_name — will skip")

    records: List[Dict] = []
    needs_verification = chain in ("ethereum", "polygon", "solana")

    for i, row in enumerate(candidates):
        addr = row["address"]
        if not addr:
            continue
        addr_lower = addr.lower() if chain != "bitcoin" else addr

        if addr_lower in skip_set:
            stats["skipped_named"] += 1
            continue

        confidence = 0.50
        address_type = "unknown"
        verification_info = {}

        if needs_verification:
            info = verify_address(addr, chain)
            if info is None:
                stats["verify_failed"] += 1
                continue

            if info.get("is_contract"):
                stats["skipped_contract"] += 1
                continue

            if info["balance_usd"] < MIN_BALANCE_USD:
                stats["skipped_low_balance"] += 1
                continue

            verification_info = info
            confidence = 0.60
            if info["balance_usd"] >= 100_000:
                confidence = 0.70
                address_type = "whale"
            elif info["balance_usd"] >= MIN_BALANCE_USD:
                confidence = 0.60
                address_type = "whale"

            if i % 50 == 0 and i > 0:
                log.info(f"[{chain}] Verified {i}/{len(candidates)}")
            time.sleep(API_RATE_DELAY)
        else:
            total_vol = float(row.get("total_volume", 0))
            tx_count = int(row.get("tx_count", 0))
            if chain == "bitcoin":
                if total_vol >= 10:
                    confidence = 0.60
                    address_type = "whale"
                elif total_vol >= 1:
                    confidence = 0.50
            elif chain == "xrp":
                if total_vol >= 500_000:
                    confidence = 0.60
                    address_type = "whale"
                elif total_vol >= 50_000:
                    confidence = 0.50

        vol_str = ""
        total_vol = float(row.get("total_volume", 0))
        tx_count = int(row.get("tx_count", 0))
        if total_vol > 0:
            vol_str = f" ({total_vol:,.0f} {cfg['native_symbol']}, {tx_count} txs)"

        label = f"{cfg['label_prefix']}{vol_str}"

        record = {
            "address": addr_lower if chain != "bitcoin" else addr,
            "blockchain": cfg["blockchain"],
            "label": label,
            "address_type": address_type,
            "entity_name": None,
            "confidence": confidence,
            "source": cfg["source"],
            "detection_method": "bigquery_volume_discovery",
            "is_verified": bool(verification_info and not verification_info.get("unverified")),
            "first_seen": datetime.utcnow().isoformat(),
            "analysis_tags": json.dumps({
                "total_volume": float(row.get("total_volume", 0)),
                "tx_count": int(row.get("tx_count", 0)),
                "avg_per_tx": float(row.get("avg_per_tx", 0)),
                "balance_usd": verification_info.get("balance_usd", 0) if verification_info else 0,
                "discovery_date": datetime.utcnow().strftime("%Y-%m-%d"),
            }),
        }

        records.append(record)

        if len(records) >= BATCH_SIZE and not dry_run:
            upsert_batch(sb, records, stats)
            records = []

    if records and not dry_run:
        upsert_batch(sb, records, stats)

    stats["accepted"] = stats["upserted"]
    return dict(stats)


def main():
    global MAX_RESULTS_PER_CHAIN

    parser = argparse.ArgumentParser(description="BigQuery Verified Whale Discovery")
    parser.add_argument("--chains", nargs="+", default=list(CHAIN_CONFIGS.keys()),
                        choices=list(CHAIN_CONFIGS.keys()),
                        help="Chains to discover (default: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Query BigQuery and verify but don't write to Supabase")
    parser.add_argument("--limit", type=int, default=MAX_RESULTS_PER_CHAIN,
                        help=f"Max results per chain (default: {MAX_RESULTS_PER_CHAIN})")
    args = parser.parse_args()

    MAX_RESULTS_PER_CHAIN = args.limit

    print("=" * 70)
    print("  BIGQUERY VERIFIED WHALE DISCOVERY")
    print("=" * 70)
    print(f"  Chains:    {', '.join(args.chains)}")
    print(f"  Max/chain: {MAX_RESULTS_PER_CHAIN:,}")
    print(f"  Dry run:   {args.dry_run}")
    print(f"  Started:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    fetch_coingecko_prices()

    bq_client = init_bigquery()
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    all_stats: Dict[str, Dict[str, int]] = {}

    for chain in args.chains:
        print(f"\n--- {chain.upper()} ---")
        try:
            chain_stats = process_chain(bq_client, sb, chain, dry_run=args.dry_run)
            all_stats[chain] = chain_stats
            for k, v in sorted(chain_stats.items()):
                print(f"  {k}: {v:,}")
        except Exception as e:
            log.error(f"[{chain}] Failed: {e}")
            all_stats[chain] = {"error": str(e)}

    print("\n" + "=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    for chain, st in all_stats.items():
        upserted = st.get("upserted", 0)
        candidates = st.get("bq_candidates", 0)
        err = st.get("error", "")
        if err:
            print(f"  {chain:10s}  ERROR: {err}")
        else:
            print(f"  {chain:10s}  {upserted:>6,} accepted / {candidates:>6,} candidates")
    print("=" * 70)


if __name__ == "__main__":
    main()
