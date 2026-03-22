#!/usr/bin/env python3
"""
Multi-Chain Whale Address Discovery

Discovers whale addresses for underserved chains (Solana, XRP, Polygon)
using chain-specific APIs and known entity lists, then upserts to Supabase.
"""

import logging
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

import requests
from supabase import create_client

from config.api_keys import (
    ETHERSCAN_API_KEYS,
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
log = logging.getLogger("multichain")

HELIUS_RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

# ============================================================================
# Known entities per chain
# ============================================================================

KNOWN_SOLANA_ENTITIES = [
    # CEX hot wallets
    {"address": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM", "entity_name": "Binance", "label": "Binance Hot Wallet", "address_type": "CEX"},
    {"address": "2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S", "entity_name": "Binance", "label": "Binance Hot Wallet 2", "address_type": "CEX"},
    {"address": "5tzFkiKscjHsFKrFoNosGNiCfNJPuTnM5r8eoKeoqV7p", "entity_name": "Binance", "label": "Binance Staking", "address_type": "CEX"},
    {"address": "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS", "entity_name": "Coinbase", "label": "Coinbase Hot Wallet", "address_type": "CEX"},
    {"address": "GJRs4FwHtemZ5ZE9x3FNvJ8TMwitKTh21yxdRPqn7npE", "entity_name": "Coinbase", "label": "Coinbase Commerce", "address_type": "CEX"},
    {"address": "u6PJ8DtQuPFnfmwHbGFULQ4u4EgjDiyYKjVEsynXq2w", "entity_name": "Kraken", "label": "Kraken Hot Wallet", "address_type": "CEX"},
    {"address": "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiWB7Y", "entity_name": "Kraken", "label": "Kraken Hot Wallet 2", "address_type": "CEX"},
    {"address": "ASTyfSima4LLAdDgoFGkgqoKowG1LZFDr9fAQrg7iaJZ", "entity_name": "OKX", "label": "OKX Hot Wallet", "address_type": "CEX"},
    {"address": "5VCwKtCXgCJ6kit5FybXjvriW3xELsFDhYrPSqtJNmcD", "entity_name": "OKX", "label": "OKX Hot Wallet 2", "address_type": "CEX"},
    {"address": "BmFdpraQhkiDQE6SNButpw4DFaXmaWqGCMvNvo7YQFEA", "entity_name": "Bybit", "label": "Bybit Hot Wallet", "address_type": "CEX"},
    {"address": "AC5RDfQFmDS1deWZos921JfqscXdByf4BKk5RdR8W9Mq", "entity_name": "Gate.io", "label": "Gate.io Hot Wallet", "address_type": "CEX"},
    {"address": "6qEv1jnbCi2XnRFbP9LaFC1nrQ2KR7sRqGg1pH4U54h7", "entity_name": "KuCoin", "label": "KuCoin Hot Wallet", "address_type": "CEX"},
    {"address": "CuieVDEDtLo7FypA9SbLM9saXFdb1dsshEkyErMqkRQq", "entity_name": "Bitget", "label": "Bitget Hot Wallet", "address_type": "CEX"},
    {"address": "HN7cABqLq46Es1jh92dQQisAq662SmxELLLsHHe4YWrH", "entity_name": "Crypto.com", "label": "Crypto.com Hot Wallet", "address_type": "CEX"},
    # Protocol treasuries and programs
    {"address": "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4", "entity_name": "Jupiter", "label": "Jupiter Aggregator V6", "address_type": "DEX"},
    {"address": "jupoNjAxXgZ4rjzxzPMP4oxduvQsQtZzyknqvzYNrNu", "entity_name": "Jupiter", "label": "Jupiter Limit Order", "address_type": "DEX"},
    {"address": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", "entity_name": "Raydium", "label": "Raydium AMM V4", "address_type": "DEX"},
    {"address": "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK", "entity_name": "Raydium", "label": "Raydium CLMM", "address_type": "DEX"},
    {"address": "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc", "entity_name": "Orca", "label": "Orca Whirlpool", "address_type": "DEX"},
    {"address": "MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD", "entity_name": "Marinade", "label": "Marinade Finance", "address_type": "DeFi"},
    {"address": "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So", "entity_name": "Marinade", "label": "mSOL Token Mint", "address_type": "DeFi"},
    {"address": "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn", "entity_name": "Jito", "label": "JitoSOL Token Mint", "address_type": "DeFi"},
    {"address": "Jito4APyf642JPZPx3hGc6WWJ8zPKtRbRs4P815Awbb", "entity_name": "Jito", "label": "Jito Staking", "address_type": "DeFi"},
    {"address": "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY", "entity_name": "Phoenix", "label": "Phoenix DEX", "address_type": "DEX"},
    {"address": "So1endDq2YkqhipRh3WViPa8hFvz0XP1SOsvRCRsTdIA", "entity_name": "Solend", "label": "Solend Protocol", "address_type": "DeFi"},
    {"address": "MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA", "entity_name": "Marginfi", "label": "Marginfi Protocol", "address_type": "DeFi"},
    {"address": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "entity_name": "SPL Token Program", "label": "SPL Token Program", "address_type": "infrastructure"},
    {"address": "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL", "entity_name": "Associated Token Program", "label": "Associated Token Program", "address_type": "infrastructure"},
    {"address": "11111111111111111111111111111111", "entity_name": "System Program", "label": "Solana System Program", "address_type": "infrastructure"},
    {"address": "Stake11111111111111111111111111111111111111", "entity_name": "Stake Program", "label": "Solana Stake Program", "address_type": "infrastructure"},
    # Solana Foundation / Labs
    {"address": "BPFLoaderUpgradeab1e11111111111111111111111", "entity_name": "BPF Loader", "label": "BPF Upgradeable Loader", "address_type": "infrastructure"},
]

KNOWN_XRP_ENTITIES = [
    # Ripple
    {"address": "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh", "entity_name": "Ripple", "label": "Ripple Genesis Account", "address_type": "foundation"},
    {"address": "rN7v3TafuK2ixPmGTbPWkNgBk2h4JQFsYb", "entity_name": "Ripple", "label": "Ripple Escrow", "address_type": "foundation"},
    {"address": "rfkE1aSy9G8Upk4JssnwBxhEv5p4mn2KTy", "entity_name": "Ripple", "label": "Ripple Distribution", "address_type": "foundation"},
    {"address": "rKLpjpCoXgLQQYQyj13zgay73rsgmzNH13", "entity_name": "Ripple", "label": "Ripple OTC", "address_type": "foundation"},
    {"address": "rs8ZPbYqgecRcDzQpJYAMhSzSHiVsKJFoR", "entity_name": "Ripple", "label": "Ripple Operations", "address_type": "foundation"},
    # CEX
    {"address": "rEy8TFcrAPvhpKrwyrscNYyqBGUkE7eshQ", "entity_name": "Binance", "label": "Binance Hot Wallet", "address_type": "CEX"},
    {"address": "rLHzPsX6oXkzU2qL12kHCH8G8cnZv1rBJh", "entity_name": "Bitstamp", "label": "Bitstamp Hot Wallet", "address_type": "CEX"},
    {"address": "rDsbeomae4FXwgQTJp9Rs64Qg9vDiTCdBv", "entity_name": "Bitstamp", "label": "Bitstamp Cold Wallet", "address_type": "CEX"},
    {"address": "rNQEMJA4BsoaheHVimsPour6NVk1t83wHa", "entity_name": "Bitstamp", "label": "Bitstamp Operations", "address_type": "CEX"},
    {"address": "raQwCVAJVqt71ceH8egHTKKHeDiSgQfJ6C", "entity_name": "Kraken", "label": "Kraken Hot Wallet", "address_type": "CEX"},
    {"address": "rcA8X3TVMST1n3CJeAdGk1RdRCHii7N2h", "entity_name": "Kraken", "label": "Kraken Cold Wallet", "address_type": "CEX"},
    {"address": "rG6FZ31hDHN1K5Dkbma3PSB5uVCuVVRzfn", "entity_name": "Bitfinex", "label": "Bitfinex Hot Wallet", "address_type": "CEX"},
    {"address": "rLFd1FzHnxM1LEd2eQGH1su9dQ9rFa2sMd", "entity_name": "Bitfinex", "label": "Bitfinex Cold Wallet", "address_type": "CEX"},
    {"address": "rBKz5MC2iXdoS3XgnNSYmF69K1Yo4NS3Ws", "entity_name": "Coinbase", "label": "Coinbase Hot Wallet", "address_type": "CEX"},
    {"address": "rw2ciyaNshpHe7bCHo4bRWq6pqqynnWKQg", "entity_name": "Bittrex", "label": "Bittrex Hot Wallet", "address_type": "CEX"},
    {"address": "rEb8TK3gBgk5auZkwc6sHnwrGVJH8DuaLh", "entity_name": "Bittrex", "label": "Bittrex Cold Wallet", "address_type": "CEX"},
    {"address": "rNxp4h8apvRis6mJf9Sh8C6iRxfrDWN7UN", "entity_name": "OKX", "label": "OKX Hot Wallet", "address_type": "CEX"},
    {"address": "rfP5WXtMnxuHrFBmpY7wHHg1HQUjsnwPS4", "entity_name": "Uphold", "label": "Uphold Hot Wallet", "address_type": "CEX"},
    {"address": "rU7ZpMMGkoCboQEPz9kJB6kBPtyN3UYBoj", "entity_name": "Uphold", "label": "Uphold Cold Wallet", "address_type": "CEX"},
    {"address": "rGhk2uLd7JFoFcRMSL7kywpdwXeJhRJMcH", "entity_name": "Huobi (HTX)", "label": "Huobi Hot Wallet", "address_type": "CEX"},
    {"address": "rPVMhWBsfF9iMXYj3aGOJCBCfQ94qRYbem", "entity_name": "Bithumb", "label": "Bithumb Hot Wallet", "address_type": "CEX"},
    {"address": "rLbKbPyuvs4wc1h13BEPHgbFGsRXMeFGL6", "entity_name": "Bitso", "label": "Bitso Hot Wallet", "address_type": "CEX"},
    {"address": "rNnGNna7A9KZGqSBCTsNEJi4LuBHaBmWnR", "entity_name": "Bybit", "label": "Bybit Hot Wallet", "address_type": "CEX"},
    {"address": "rhWt2ZMvBeAFPXGEVMwqCuJKTGaNth2mZp", "entity_name": "Gate.io", "label": "Gate.io Hot Wallet", "address_type": "CEX"},
    # SBI / B2C2 / other major entities
    {"address": "rDCgaaSBAWYfsKiiq7HsxAE5puf6oTsVP6", "entity_name": "SBI VC Trade", "label": "SBI VC Trade", "address_type": "CEX"},
    {"address": "rPsmHjpEhtTjBTEnNJC22GDXLBR2gYxzEu", "entity_name": "B2C2", "label": "B2C2 OTC", "address_type": "market_maker"},
]


# ============================================================================
# Solana: Discover whale wallets from DeFi token top holders
# ============================================================================

SOLANA_DEFI_TOKENS = [
    ("mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So", "mSOL", "Marinade Staked SOL"),
    ("J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn", "jitoSOL", "Jito Staked SOL"),
    ("7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj", "stSOL", "Lido Staked SOL"),
    ("bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1", "bSOL", "BlazeStake SOL"),
    ("HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3", "PYTH", "Pyth Network"),
    ("rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof", "RNDR", "Render Token"),
    ("7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs", "WETH", "Wrapped ETH"),
    ("4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R", "RAY", "Raydium"),
    ("orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE", "ORCA", "Orca"),
    ("MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey", "MNDE", "Marinade Governance"),
    ("EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm", "WIF", "dogwifhat"),
    ("DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", "BONK", "Bonk"),
    ("JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN", "JUP", "Jupiter"),
    ("jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL", "JTO", "Jito Governance"),
    ("TNSRxcUxoT9xBG3de7PiJyTDYu7kskLqcpddxnEJAS6", "TNSR", "Tensor"),
    ("85VBFQZC9TZkfaptBWjvUw7YbZjy52A6mjtPGjstQAmQ", "W", "Wormhole"),
]


def discover_solana_token_holders() -> List[Dict[str, Any]]:
    """Discover Solana whale wallets from top DeFi token holders."""
    records = []
    token_account_to_meta = {}

    for mint, symbol, name in SOLANA_DEFI_TOKENS:
        try:
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "getTokenLargestAccounts",
                "params": [mint],
            }
            r = requests.post(HELIUS_RPC, json=payload, timeout=15)
            data = r.json()
            if "result" not in data or "value" not in data["result"]:
                err = data.get("error", {}).get("message", "unknown")
                if "Too many accounts" not in err:
                    log.warning(f"  {symbol}: {err[:60]}")
                continue
            accounts = data["result"]["value"]
            log.info(f"  {symbol:8s} {len(accounts)} top holders")
            for a in accounts:
                amt = int(a["amount"]) / 10 ** int(a["decimals"])
                token_account_to_meta[a["address"]] = {
                    "token": symbol, "token_name": name, "amount": amt,
                }
            time.sleep(0.3)
        except Exception as e:
            log.warning(f"  {symbol}: error - {e}")

    log.info(f"Resolving {len(token_account_to_meta)} token accounts to owner wallets...")
    owner_tokens = defaultdict(list)

    batch_keys = list(token_account_to_meta.keys())
    for i in range(0, len(batch_keys), 10):
        batch = batch_keys[i:i + 10]
        for token_acct in batch:
            try:
                payload = {
                    "jsonrpc": "2.0", "id": 1,
                    "method": "getAccountInfo",
                    "params": [token_acct, {"encoding": "jsonParsed"}],
                }
                r = requests.post(HELIUS_RPC, json=payload, timeout=10)
                data = r.json()
                owner = data["result"]["value"]["data"]["parsed"]["info"]["owner"]
                meta = token_account_to_meta[token_acct]
                owner_tokens[owner].append(meta)
            except Exception:
                pass
        time.sleep(0.2)

    log.info(f"Resolved {len(owner_tokens)} unique owner wallets")

    for owner, holdings in owner_tokens.items():
        top = max(holdings, key=lambda h: h["amount"])
        all_tokens = ", ".join(sorted(set(h["token"] for h in holdings)))
        records.append({
            "address": owner,
            "blockchain": "solana",
            "label": f"Whale ({all_tokens} holder)",
            "address_type": "whale",
            "confidence": 0.65,
            "source": "helius_token_holder_discovery",
            "detection_method": "token_largest_accounts",
        })

    return records



# ============================================================================
# XRP: Verify balances for known entities
# ============================================================================

XRPL_RPC = "https://s1.ripple.com:51234"


def verify_xrp_balance(address: str) -> Optional[float]:
    """Get XRP balance for an address via XRPL public RPC."""
    try:
        payload = {
            "method": "account_info",
            "params": [{"account": address, "ledger_index": "validated"}],
        }
        r = requests.post(XRPL_RPC, json=payload, timeout=10)
        data = r.json()
        result = data.get("result", {})
        if result.get("status") != "success":
            return None
        balance_drops = int(result["account_data"]["Balance"])
        return balance_drops / 1e6
    except Exception:
        return None


# ============================================================================
# Polygon: Name top unnamed addresses via Etherscan V2
# ============================================================================

_etherscan_key_idx = [0]

def _rotate_etherscan_key() -> str:
    key = ETHERSCAN_API_KEYS[_etherscan_key_idx[0] % len(ETHERSCAN_API_KEYS)]
    _etherscan_key_idx[0] += 1
    return key


def get_polygon_address_label(address: str) -> Optional[str]:
    """Try to get a label for a Polygon address from Polygonscan API."""
    try:
        r = requests.get(
            "https://api.etherscan.io/v2/api",
            params={
                "chainid": "137",
                "module": "account",
                "action": "balance",
                "address": address,
                "apikey": POLYGONSCAN_API_KEY,
            },
            timeout=10,
        )
        data = r.json()
        if data.get("status") == "1":
            balance_wei = int(data["result"])
            return balance_wei
    except Exception:
        pass
    return None


# ============================================================================
# Supabase upsert helpers
# ============================================================================

def upsert_records(sb, records: List[Dict], chain: str) -> int:
    """Upsert records to Supabase, returning count of successful upserts."""
    if not records:
        return 0

    existing = set()
    try:
        resp = sb.table("addresses").select("address").eq("blockchain", chain).not_.is_("entity_name", "null").limit(5000).execute()
        existing = {r["address"].lower() for r in resp.data}
    except Exception:
        pass

    new_records = [r for r in records if r["address"].lower() not in existing]
    if not new_records:
        log.info(f"  [{chain}] All {len(records)} addresses already exist with entity names")
        return 0

    upserted = 0
    batch_size = 100
    for i in range(0, len(new_records), batch_size):
        batch = new_records[i:i + batch_size]
        try:
            sb.table("addresses").upsert(
                batch, on_conflict="address,blockchain"
            ).execute()
            upserted += len(batch)
        except Exception as e:
            log.warning(f"  [{chain}] Batch upsert failed, trying one-by-one: {e}")
            for rec in batch:
                try:
                    sb.table("addresses").upsert(
                        rec, on_conflict="address,blockchain"
                    ).execute()
                    upserted += 1
                except Exception:
                    pass

    return upserted


# ============================================================================
# Main
# ============================================================================

def main():
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    print("=" * 70)
    print("  MULTI-CHAIN WHALE ADDRESS DISCOVERY")
    print("=" * 70)

    stats = {}

    # --- SOLANA: Known entities ---
    print("\n--- SOLANA: Known Entities ---")
    sol_entity_records = []
    for ent in KNOWN_SOLANA_ENTITIES:
        sol_entity_records.append({
            "address": ent["address"],
            "blockchain": "solana",
            "label": ent["label"],
            "address_type": ent["address_type"],
            "entity_name": ent["entity_name"],
            "confidence": 0.95,
            "source": "known_entity_list",
            "detection_method": "manual_curated",
        })
    upserted = upsert_records(sb, sol_entity_records, "solana")
    log.info(f"  [solana] Known entities: {upserted} upserted / {len(sol_entity_records)} total")
    stats["solana_entities"] = upserted

    # --- SOLANA: Token holder discovery ---
    print("\n--- SOLANA: Token Holder Discovery ---")
    sol_token_records = discover_solana_token_holders()
    upserted = upsert_records(sb, sol_token_records, "solana")
    log.info(f"  [solana] Token holders: {upserted} upserted / {len(sol_token_records)} discovered")
    stats["solana_tokens"] = upserted

    # --- XRP: Known entities + balance verification ---
    print("\n--- XRP: Known Entities ---")
    xrp_records = []
    for ent in KNOWN_XRP_ENTITIES:
        balance = verify_xrp_balance(ent["address"])
        rec = {
            "address": ent["address"],
            "blockchain": "xrp",
            "label": ent["label"],
            "address_type": ent["address_type"],
            "entity_name": ent["entity_name"],
            "confidence": 0.90,
            "source": "known_entity_list",
            "detection_method": "manual_curated",
        }
        if balance is not None:
            rec["balance_native"] = balance
            rec["balance_usd"] = balance * 0.55
            status = f"${balance * 0.55:,.0f}"
        else:
            status = "unverified"
        log.info(f"  {ent['entity_name']:20s} {ent['address'][:16]}... {status}")
        xrp_records.append(rec)
        time.sleep(0.3)

    upserted = upsert_records(sb, xrp_records, "xrp")
    log.info(f"  [xrp] Known entities: {upserted} upserted / {len(xrp_records)} total")
    stats["xrp_entities"] = upserted

    # --- POLYGON: Name top unnamed addresses ---
    print("\n--- POLYGON: Balance Check for Top Unnamed ---")
    try:
        resp = sb.table("addresses").select("address,balance_usd").eq("blockchain", "polygon").is_("entity_name", "null").order("balance_usd", desc=True).limit(200).execute()
        unnamed_polygon = resp.data
        log.info(f"  [polygon] Found {len(unnamed_polygon)} top unnamed addresses to check")

        poly_updated = 0
        for i, row in enumerate(unnamed_polygon):
            addr = row["address"]
            bal = get_polygon_address_label(addr)
            if bal is not None:
                balance_matic = bal / 1e18
                if balance_matic > 10000:
                    try:
                        sb.table("addresses").update({
                            "balance_native": balance_matic,
                            "balance_usd": balance_matic * 0.45,
                            "address_type": "whale",
                            "confidence": 0.60,
                        }).eq("address", addr).eq("blockchain", "polygon").execute()
                        poly_updated += 1
                    except Exception:
                        pass
            if i % 20 == 0 and i > 0:
                log.info(f"  [polygon] Checked {i}/{len(unnamed_polygon)}")
            time.sleep(0.4)

        log.info(f"  [polygon] Updated {poly_updated} addresses with balances")
        stats["polygon_updated"] = poly_updated
    except Exception as e:
        log.warning(f"  [polygon] Error: {e}")
        stats["polygon_updated"] = 0

    # --- SUMMARY ---
    print("\n" + "=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    for key, val in stats.items():
        print(f"  {key:30s}  {val:>6,}")
    print("=" * 70)


if __name__ == "__main__":
    main()
