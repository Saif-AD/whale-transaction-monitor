#!/usr/bin/env python3
"""Profile every curated_entities address into wallet_profiles.

Gives the figure/entity pages a real "portfolio file": total value,
smart-money score, behavioral tags, top tokens — keyed by address so the
frontend can join curated_entities.addresses -> wallet_profiles.

HOW IT WORKS
------------
For each EVM address listed in curated_entities.addresses, calls
utils.wallet_profiler.WalletProfiler.get_profile(), which builds + caches
a row in wallet_profiles (TTL-respecting). The profiler enriches with
Zerion when ZERION_API_KEY is configured; otherwise it falls back to a
transaction-based estimate.

DATA DEPENDENCY (read this before expecting real numbers)
---------------------------------------------------------
The portfolio VALUE + holdings are only real with a Zerion key. The
smart-money score + tags need transaction history in the per-chain
*_transactions tables. Most curated figure wallets (e.g. vitalik.eth)
have NO rows in those tables — their on-chain activity is fetched live
via RPC by the frontend, not stored — so without Zerion this job writes
mostly zeros. Set ZERION_API_KEY to make it meaningful. This is the
portfolio analogue of ARKHAM_API_KEY for address discovery.

Solana / Bitcoin addresses are skipped: the profiler lowercases
addresses (correct for EVM, corrupting for case-sensitive Base58/Bitcoin).

Designed for a Railway cron slot (supabase-py must be installed there;
it is not installable in the sandboxed local env). Default is DRY-RUN.

Usage:
    python scripts/profile_curated_addresses.py                 # dry-run (list targets)
    python scripts/profile_curated_addresses.py --live          # build + cache profiles
    python scripts/profile_curated_addresses.py --live --only vitalik-buterin,gainzy
    python scripts/profile_curated_addresses.py --live --max-runtime 1400
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("profile_curated")

# The profiler lowercases addresses, so only case-insensitive EVM chains
# are safe to profile through it.
EVM_CHAINS = {"ethereum", "base", "arbitrum", "optimism", "polygon", "bsc", "avalanche"}


def fetch_curated_addresses(client, only: Optional[List[str]]) -> List[Tuple[str, str, str]]:
    """Return (slug, address, chain) tuples for every EVM curated address."""
    query = client.table("curated_entities").select("slug, addresses")
    rows = query.execute().data or []
    out: List[Tuple[str, str, str]] = []
    seen = set()
    for row in rows:
        slug = row.get("slug")
        if only and slug not in only:
            continue
        for a in row.get("addresses") or []:
            addr = str(a.get("address") or "").strip()
            chain = str(a.get("chain") or "").lower().strip()
            if not addr or chain not in EVM_CHAINS:
                continue
            key = addr.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append((slug, addr, chain))
    return out


def run(*, live: bool, only: Optional[List[str]], max_runtime: int) -> Dict[str, int]:
    from supabase import create_client
    from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    targets = fetch_curated_addresses(client, only)
    logger.info("%s — %d EVM curated addresses to profile", "LIVE" if live else "DRY-RUN", len(targets))

    summary = {"profiled": 0, "with_value": 0, "scored": 0, "errors": 0, "skipped_time": 0}

    if not live:
        for slug, addr, chain in targets[:25]:
            logger.info("  would profile [%s] %s (%s)", chain, addr, slug)
        if len(targets) > 25:
            logger.info("  ... and %d more", len(targets) - 25)
        return summary

    from utils.wallet_profiler import WalletProfiler

    profiler = WalletProfiler()
    deadline = time.monotonic() + max_runtime
    for slug, addr, chain in targets:
        if time.monotonic() > deadline:
            summary["skipped_time"] += 1
            continue
        try:
            profile = profiler.get_profile(addr, chain) or {}
            summary["profiled"] += 1
            if float(profile.get("portfolio_value_usd") or 0) > 0:
                summary["with_value"] += 1
            if float(profile.get("smart_money_score") or 0) > 0:
                summary["scored"] += 1
        except Exception as e:
            summary["errors"] += 1
            logger.warning("profile failed [%s] %s (%s): %s", chain, addr, slug, e)

    logger.info(
        "LIVE done — profiled=%d with_value=%d scored=%d errors=%d skipped(time)=%d",
        summary["profiled"], summary["with_value"], summary["scored"],
        summary["errors"], summary["skipped_time"],
    )
    if live and summary["profiled"] and summary["with_value"] == 0:
        logger.warning(
            "0 profiles had a portfolio value — set ZERION_API_KEY for real holdings "
            "(see module docstring)."
        )
    return summary


def main() -> int:
    p = argparse.ArgumentParser(description="Profile curated_entities addresses into wallet_profiles.")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--live", action="store_true", help="Build + cache profiles (default: dry-run).")
    g.add_argument("--dry-run", action="store_true", help="List targets only (default).")
    p.add_argument("--only", type=str, default="", help="Comma-separated slugs to limit to.")
    p.add_argument("--max-runtime", type=int, default=1400, help="Runtime cap in seconds (Railway 30-min slot).")
    args = p.parse_args()

    only = [s.strip() for s in args.only.split(",") if s.strip()] or None
    live = args.live and not args.dry_run
    try:
        run(live=live, only=only, max_runtime=args.max_runtime)
    except Exception as e:
        logger.error("curated profiling failed: %s", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
