"""
Address Verification Gate

Verifies candidate addresses discovered by BigQuery before they're inserted
into Supabase. Only addresses that pass verification get stored.

Verification checks (using real APIs):
1. Etherscan: contract check, label lookup, ETH balance
2. Moralis: wallet net worth (multi-token balance in USD)
3. PolygonScan / BscScan: same as Etherscan for other chains

An address must pass at least one of:
- Has a known Etherscan/explorer label (entity_name gets populated)
- Has a real balance above the whale threshold ($500K+)
- Is a verified contract with significant interaction count

Addresses that fail all checks are dropped silently.
"""

import logging
import time
from typing import Dict, List, Optional, Tuple
import requests

from config.api_keys import (
    ETHERSCAN_API_KEY,
    ETHERSCAN_API_KEYS,
    POLYGONSCAN_API_KEY,
    MORALIS_API_KEY,
)

logger = logging.getLogger(__name__)

# Minimum balance (USD) to qualify as a whale without a label
MIN_WHALE_BALANCE_USD = 500_000

# Rate limiting: Etherscan free tier = 5 req/s
ETHERSCAN_DELAY = 0.25  # seconds between requests

# Explorer API base URLs per chain
EXPLORER_APIS = {
    'ethereum': {
        'base_url': 'https://api.etherscan.io/api',
        'api_key': ETHERSCAN_API_KEY,
    },
    'polygon': {
        'base_url': 'https://api.polygonscan.com/api',
        'api_key': POLYGONSCAN_API_KEY,
    },
    'bsc': {
        'base_url': 'https://api.bscscan.com/api',
        'api_key': ETHERSCAN_API_KEY,  # BscScan often accepts Etherscan keys
    },
}

# Approximate native token prices for balance USD conversion
NATIVE_PRICES = {
    'ethereum': 3000,
    'polygon': 0.8,
    'bsc': 600,
}

# Rotate Etherscan keys to avoid rate limits
_etherscan_key_index = 0


def _get_etherscan_key() -> str:
    global _etherscan_key_index
    keys = ETHERSCAN_API_KEYS if ETHERSCAN_API_KEYS else [ETHERSCAN_API_KEY]
    key = keys[_etherscan_key_index % len(keys)]
    _etherscan_key_index += 1
    return key


def _explorer_request(chain: str, params: dict) -> Optional[dict]:
    """Make a rate-limited request to a block explorer API."""
    cfg = EXPLORER_APIS.get(chain)
    if not cfg:
        return None

    # Use rotated key for ethereum
    if chain == 'ethereum':
        params['apikey'] = _get_etherscan_key()
    else:
        params['apikey'] = cfg['api_key']

    try:
        time.sleep(ETHERSCAN_DELAY)
        resp = requests.get(cfg['base_url'], params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get('status') == '1' or data.get('result'):
            return data
        return None
    except Exception as e:
        logger.debug(f"Explorer request failed for {chain}: {e}")
        return None


def get_explorer_label(address: str, chain: str) -> Optional[str]:
    """
    Check if an address has a known label on the block explorer.
    Uses the 'getaddressinfo' or contract source endpoint.
    Returns the label string or None.
    """
    # Try to get contract name (verified contracts have source code)
    data = _explorer_request(chain, {
        'module': 'contract',
        'action': 'getsourcecode',
        'address': address,
    })

    if data and data.get('result'):
        result = data['result']
        if isinstance(result, list) and len(result) > 0:
            contract_name = result[0].get('ContractName', '')
            if contract_name and contract_name != '':
                return contract_name

    return None


def get_native_balance(address: str, chain: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Get native token balance from explorer API.
    Returns (balance_native, balance_usd) or (None, None).
    """
    data = _explorer_request(chain, {
        'module': 'account',
        'action': 'balance',
        'address': address,
        'tag': 'latest',
    })

    if not data or not data.get('result'):
        return None, None

    try:
        raw = int(data['result'])
        decimals = 18 if chain in ('ethereum', 'polygon', 'bsc') else 8
        balance_native = raw / (10 ** decimals)
        price = NATIVE_PRICES.get(chain, 0)
        balance_usd = balance_native * price
        return round(balance_native, 6), round(balance_usd, 2)
    except (ValueError, TypeError):
        return None, None


def get_moralis_net_worth(address: str, chain: str) -> Optional[float]:
    """
    Get total wallet net worth in USD via Moralis API.
    This includes all ERC-20 tokens + native balance.
    """
    if not MORALIS_API_KEY:
        return None

    chain_map = {
        'ethereum': 'eth',
        'polygon': 'polygon',
        'bsc': 'bsc',
    }
    moralis_chain = chain_map.get(chain)
    if not moralis_chain:
        return None

    try:
        time.sleep(0.2)
        resp = requests.get(
            f'https://deep-index.moralis.io/api/v2.2/wallets/{address}/net-worth',
            headers={'X-API-Key': MORALIS_API_KEY, 'accept': 'application/json'},
            params={'chains[]': moralis_chain},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        total = float(data.get('total_networth_usd', 0))
        return round(total, 2) if total > 0 else None
    except Exception as e:
        logger.debug(f"Moralis net worth failed for {address}: {e}")
        return None


def get_tx_count(address: str, chain: str) -> Optional[int]:
    """Get transaction count for an address from explorer API."""
    data = _explorer_request(chain, {
        'module': 'proxy',
        'action': 'eth_getTransactionCount',
        'address': address,
        'tag': 'latest',
    })

    if not data or not data.get('result'):
        return None

    try:
        return int(data['result'], 16)
    except (ValueError, TypeError):
        return None


def verify_address(address: str, chain: str) -> Optional[Dict]:
    """
    Verify a single address. Returns enriched metadata dict if verified,
    or None if the address should be dropped.

    Verification passes if ANY of:
    1. Address has a known contract name / label
    2. Native balance >= $500K USD
    3. Moralis net worth >= $500K USD
    """
    result = {
        'entity_name': None,
        'balance_native': None,
        'balance_usd': None,
        'is_contract': False,
        'verification_source': None,
    }

    # Check 1: Explorer label (contract name)
    label = get_explorer_label(address, chain)
    if label:
        result['entity_name'] = label
        result['is_contract'] = True
        result['verification_source'] = 'explorer_label'
        # Still try to get balance for completeness
        bal_native, bal_usd = get_native_balance(address, chain)
        if bal_native is not None:
            result['balance_native'] = bal_native
            result['balance_usd'] = bal_usd
        return result

    # Check 2: Native balance
    bal_native, bal_usd = get_native_balance(address, chain)
    if bal_native is not None:
        result['balance_native'] = bal_native
        result['balance_usd'] = bal_usd
        if bal_usd and bal_usd >= MIN_WHALE_BALANCE_USD:
            result['verification_source'] = 'native_balance'
            return result

    # Check 3: Moralis total net worth (includes all tokens)
    net_worth = get_moralis_net_worth(address, chain)
    if net_worth and net_worth >= MIN_WHALE_BALANCE_USD:
        result['balance_usd'] = net_worth
        result['verification_source'] = 'moralis_net_worth'
        return result

    # Failed all checks
    return None


def verify_batch(
    candidates: List[Dict],
    chain: str,
    max_verify: int = 1000,
    progress_interval: int = 50,
) -> List[Dict]:
    """
    Verify a batch of candidate addresses. Only returns addresses that pass
    verification, with enriched metadata fields populated.

    Args:
        candidates: List of address dicts from BigQuery discovery
        chain: Blockchain name
        max_verify: Maximum addresses to verify (API rate limit budget)
        progress_interval: Print progress every N addresses

    Returns:
        List of verified address dicts with entity_name, balance populated
    """
    verified = []
    total = min(len(candidates), max_verify)

    logger.info(f"Verifying {total} {chain} candidate addresses...")
    print(f"   Verifying {total} {chain} candidates (dropping unverifiable)...")

    for i, addr_dict in enumerate(candidates[:max_verify]):
        address = addr_dict.get('address', '')
        if not address:
            continue

        enrichment = verify_address(address, chain)

        if enrichment:
            # Merge verification data into the address record
            if enrichment['entity_name']:
                addr_dict['entity_name'] = enrichment['entity_name']
            if enrichment['balance_native'] is not None:
                addr_dict['balance_native'] = enrichment['balance_native']
            if enrichment['balance_usd'] is not None:
                addr_dict['balance_usd'] = enrichment['balance_usd']

            # Adjust confidence based on verification quality
            base_conf = addr_dict.get('confidence', 0.5)
            if enrichment['entity_name']:
                addr_dict['confidence'] = min(0.95, base_conf + 0.10)
            elif enrichment['verification_source'] == 'moralis_net_worth':
                addr_dict['confidence'] = min(0.92, base_conf + 0.05)
            # else: native balance alone keeps existing confidence

            addr_dict['detection_method'] = (
                f"{addr_dict.get('detection_method', 'unknown')}"
                f"+verified_{enrichment['verification_source']}"
            )

            verified.append(addr_dict)

        if (i + 1) % progress_interval == 0:
            print(f"      {i+1}/{total} checked, {len(verified)} verified")

    drop_rate = ((total - len(verified)) / total * 100) if total > 0 else 0
    print(f"   Verification complete: {len(verified)}/{total} passed "
          f"({drop_rate:.0f}% dropped)")

    return verified
