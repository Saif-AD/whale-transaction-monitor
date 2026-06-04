#!/usr/bin/env python3
"""Populate the frontend `curated_entities.addresses` JSONB column.

This is the companion to scripts/import_figures_seeds.py, but it targets a
DIFFERENT table. seeds_figures.py fills the backend `addresses` label store;
THIS script fills `curated_entities`, which is what the public figure /
entity detail pages (app/figure/[slug]/page.js) actually read.

Reads CURATED_ENTITY_SEEDS from arkham_backfill.seeds_curated_entities and,
for each slug:

  * loads the existing curated_entities row,
  * validates each seed address against the same per-chain regexes the
    admin API route enforces (so we never write a shape the frontend
    rejects),
  * APPENDS new addresses to the existing array (never clobbers — dedupe
    is by (chain, normalized address), EVM lowercased),
  * writes back the merged array.

Provenance discipline mirrors the admin route: a verified=True row MUST
carry a `source` http(s) URL.

Default is DRY-RUN: prints exactly what WOULD change, touches nothing.
Use --live to write.

Usage:
    python scripts/import_curated_entities.py            # dry-run (default)
    python scripts/import_curated_entities.py --live     # actually write
    python scripts/import_curated_entities.py --only coinbase,binance
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from arkham_backfill.seeds_curated_entities import CURATED_ENTITY_SEEDS  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Validation — mirrors app/api/admin/figures/[slug]/addresses/route.js
# ---------------------------------------------------------------------------
VALID_CHAINS = {
    "ethereum", "polygon", "arbitrum", "optimism", "base",
    "bsc", "avalanche", "solana", "bitcoin",
}
EVM_CHAINS = {"ethereum", "polygon", "arbitrum", "optimism", "base", "bsc", "avalanche"}

_EVM_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
_SOL_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
_BTC_LEGACY_RE = re.compile(r"^[13][a-km-zA-HJ-NP-Z1-9]{25,34}$")
_BTC_BECH32_RE = re.compile(r"^bc1[02-9ac-hj-np-z]{6,87}$")


def validate_address(address: str, chain: str) -> Optional[str]:
    if not isinstance(address, str) or not address.strip():
        return "address is required"
    a = address.strip()
    if chain not in VALID_CHAINS:
        return f"chain must be one of: {', '.join(sorted(VALID_CHAINS))}"
    if chain in EVM_CHAINS:
        if not _EVM_RE.match(a):
            return "invalid EVM address (expected 0x + 40 hex chars)"
    elif chain == "solana":
        if not _SOL_RE.match(a):
            return "invalid Solana address (base58, 32-44 chars)"
    elif chain == "bitcoin":
        if not _BTC_LEGACY_RE.match(a) and not _BTC_BECH32_RE.match(a):
            return "invalid Bitcoin address (legacy or bech32)"
    return None


def normalize_for_compare(address: str, chain: str) -> str:
    return address.lower() if chain in EVM_CHAINS else address


def normalize_for_store(address: str, chain: str) -> str:
    # EVM stored lowercased so the all_whale_transactions join (which
    # lowercases EVM addresses) matches. Solana/BTC are case-sensitive.
    return address.strip().lower() if chain in EVM_CHAINS else address.strip()


def build_row(seed: Dict[str, Any]) -> Dict[str, Any]:
    chain = str(seed["chain"]).lower().strip()
    return {
        "address": normalize_for_store(str(seed["address"]), chain),
        "chain": chain,
        "note": str(seed.get("note", "")).strip()[:280],
        "source": str(seed.get("source", "")).strip()[:500],
        "verified": bool(seed.get("verified", False)),
        "added_by": "import_curated_entities.py",
        "added_at": datetime.now(timezone.utc).isoformat(),
    }


def _validate_seed(slug: str, seed: Dict[str, Any]) -> Optional[str]:
    chain = str(seed.get("chain", "")).lower().strip()
    err = validate_address(str(seed.get("address", "")), chain)
    if err:
        return err
    if seed.get("verified") and not str(seed.get("source", "")).strip():
        return "source URL required when verified=True"
    src = str(seed.get("source", "")).strip()
    if src and not re.match(r"^https?://", src, re.I):
        return "source must be an http(s) URL"
    return None


def merge_for_entity(
    slug: str, existing: List[Dict[str, Any]], seeds: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Return {merged, added, skipped_dupes, invalid} for one entity."""
    existing = existing if isinstance(existing, list) else []
    seen = {
        (str(a.get("chain", "")), normalize_for_compare(str(a.get("address", "")), str(a.get("chain", ""))))
        for a in existing
    }
    merged = list(existing)
    added, skipped, invalid = [], [], []
    for seed in seeds:
        err = _validate_seed(slug, seed)
        if err:
            invalid.append((seed.get("address"), err))
            continue
        chain = str(seed["chain"]).lower().strip()
        key = (chain, normalize_for_compare(str(seed["address"]), chain))
        if key in seen:
            skipped.append(seed["address"])
            continue
        row = build_row(seed)
        merged.append(row)
        seen.add(key)
        added.append(row)
    return {"merged": merged, "added": added, "skipped": skipped, "invalid": invalid}


def run(*, live: bool, only: Optional[set], client=None) -> Dict[str, Any]:
    if client is None:
        from supabase import create_client
        from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
        client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    summary = {
        "entities_seeded": 0, "addresses_added": 0, "dupes_skipped": 0,
        "invalid": 0, "unknown_slugs": [], "live": live,
    }

    for slug, seeds in CURATED_ENTITY_SEEDS.items():
        if only and slug not in only:
            continue

        res = (
            client.table("curated_entities")
            .select("slug, display_name, addresses")
            .eq("slug", slug)
            .maybe_single()
            .execute()
        )
        row = getattr(res, "data", None)
        if not row:
            logger.warning("SKIP unknown slug (not in curated_entities): %s", slug)
            summary["unknown_slugs"].append(slug)
            continue

        existing = row.get("addresses") or []
        result = merge_for_entity(slug, existing, seeds)
        added, skipped, invalid = result["added"], result["skipped"], result["invalid"]

        for addr, err in invalid:
            logger.error("  INVALID %s [%s]: %s", slug, addr, err)
        summary["invalid"] += len(invalid)
        summary["dupes_skipped"] += len(skipped)

        if not added:
            logger.info("· %s (%s): nothing new (%d existing, %d dupes)",
                        slug, row.get("display_name"), len(existing), len(skipped))
            continue

        summary["entities_seeded"] += 1
        summary["addresses_added"] += len(added)
        verb = "WROTE" if live else "WOULD ADD"
        logger.info("%s %d → %s (%s)", verb, len(added), slug, row.get("display_name"))
        for a in added:
            logger.info("    [%s] %s  verified=%s", a["chain"], a["address"], a["verified"])

        if live:
            upd = (
                client.table("curated_entities")
                .update({"addresses": result["merged"]})
                .eq("slug", slug)
                .execute()
            )
            if getattr(upd, "data", None) is None:
                logger.error("  UPDATE returned no data for %s", slug)

    logger.info(
        "%s — entities touched: %d | addresses added: %d | dupes skipped: %d | invalid: %d | unknown slugs: %d",
        "LIVE" if live else "DRY-RUN",
        summary["entities_seeded"], summary["addresses_added"],
        summary["dupes_skipped"], summary["invalid"], len(summary["unknown_slugs"]),
    )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Populate curated_entities.addresses from the curated seed list.",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--live", action="store_true", help="Write to Supabase (default: dry-run).")
    group.add_argument("--dry-run", action="store_true", help="Print changes only (default).")
    parser.add_argument("--only", default="", help="Comma-separated slugs to limit to.")
    args = parser.parse_args()

    live = args.live and not args.dry_run
    only = {s.strip() for s in args.only.split(",") if s.strip()} or None

    logger.info(
        "curated_entities import (%s) — %d slugs, %d seed addresses",
        "LIVE" if live else "DRY-RUN",
        len(CURATED_ENTITY_SEEDS),
        sum(len(v) for v in CURATED_ENTITY_SEEDS.values()),
    )
    summary = run(live=live, only=only)
    return 0 if summary["invalid"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
