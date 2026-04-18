#!/usr/bin/env python3
"""Arkham address backfill via transfer mining.

Iterates the curated entity seed list, fetches transfers from the Arkham
API, extracts addresses attributed to each entity, normalizes them, and
upserts into the Supabase `addresses` table.

Usage:
    python3 arkham_backfill/backfill.py              # dry-run (default)
    python3 arkham_backfill/backfill.py --live        # write to Supabase
    python3 arkham_backfill/backfill.py --live --limit 50  # cap transfers per entity
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from supabase import create_client

from arkham_backfill.arkham_client import ArkhamClient, CreditBudgetExhausted
from arkham_backfill.constants import ARKHAM_CHAIN_MAP, ARKHAM_DEFAULTS, ENTITY_TYPE_OVERRIDES
from arkham_backfill.entities import ENTITY_SEED_LIST, Entity
from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
from shared.address_labels import normalize_address

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Upsert logic with multi-source conflict policy
# ------------------------------------------------------------------

def build_upsert_row(
    addr_info: Dict[str, Any],
    our_blockchain: str,
    entity: Entity,
) -> Dict[str, Any]:
    """Build a row dict for upserting into the `addresses` table.

    Applies ARKHAM_DEFAULTS, then ENTITY_TYPE_OVERRIDES based on
    entity.type, then specific fields from the Arkham transfer data.

    The address is normalized via shared.address_labels.normalize_address()
    to ensure EVM addresses are lowercased, matching the ingest pipeline.
    """
    defaults = dict(ARKHAM_DEFAULTS)
    overrides = ENTITY_TYPE_OVERRIDES.get(entity.type, {})
    defaults.update(overrides)

    normalized_addr = normalize_address(addr_info["address"], our_blockchain)

    arkham_type = addr_info.get("entity_type", "")
    address_type = _map_address_type(arkham_type)

    return {
        "address": normalized_addr,
        "blockchain": our_blockchain,
        "entity_name": addr_info.get("entity_name", ""),
        "label": addr_info.get("label", ""),
        "address_type": address_type,
        "confidence": defaults["confidence"],
        "signal_potential": defaults["signal_potential"],
        "detection_method": defaults["detection_method"],
        "source": defaults["source"],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _map_address_type(arkham_entity_type: str) -> str:
    mapping = {
        "cex": "exchange",
        "dex": "exchange",
        "fund": "fund",
        "individual": "whale",
        "bridge": "bridge",
        "custodian": "custodian",
        "stablecoin_issuer": "issuer",
    }
    return mapping.get(arkham_entity_type, "unknown")


def _normalize_tags(raw) -> list:
    """Coerce analysis_tags from any stored format into a list of strings."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        return [raw]
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return [raw] if raw else []
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
        return [parsed] if parsed else []
    return []


def merge_with_existing(
    new_row: Dict[str, Any],
    existing: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Apply multi-source conflict policy.

    Rules:
      - Arkham always wins on entity_name and label
      - confidence: replace only if new > existing
      - analysis_tags: merge (append 'arkham_api' if not present)
      - All other fields: Arkham wins
    """
    if existing is None:
        return new_row

    merged = dict(new_row)

    existing_confidence = float(existing.get("confidence") or 0)
    if merged["confidence"] <= existing_confidence:
        merged["confidence"] = existing_confidence

    existing_tags = _normalize_tags(existing.get("analysis_tags"))
    if "arkham_api" not in existing_tags:
        existing_tags.append("arkham_api")
    merged["analysis_tags"] = existing_tags

    return merged


# ------------------------------------------------------------------
# Main backfill loop
# ------------------------------------------------------------------

def run_backfill(
    *,
    live: bool = False,
    limit: int = 50,
    entities: Optional[List[Entity]] = None,
    client: Optional[ArkhamClient] = None,
    supabase_client=None,
) -> Dict[str, Any]:
    """Run the Arkham address backfill.

    Returns a summary dict with per-entity and overall counts.
    """
    entity_list = entities or ENTITY_SEED_LIST
    ark = client or ArkhamClient()
    sb = supabase_client or create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    summary: Dict[str, Any] = {
        "entities_processed": 0,
        "total_addresses": 0,
        "total_upserted": 0,
        "total_skipped_chain": 0,
        "total_errors": 0,
        "per_entity": {},
    }

    try:
        for entity in entity_list:
            entity_stats = _process_entity(
                entity, ark=ark, sb=sb, live=live, limit=limit,
            )
            summary["per_entity"][entity.slug] = entity_stats
            summary["entities_processed"] += 1
            summary["total_addresses"] += entity_stats["found"]
            summary["total_upserted"] += entity_stats["upserted"]
            summary["total_skipped_chain"] += entity_stats["skipped_chain"]
            summary["total_errors"] += entity_stats["errors"]

            logger.info(
                "Entity %s: found %d addresses, upserted %d, "
                "skipped %d (unmapped chain), errors %d",
                entity.slug,
                entity_stats["found"],
                entity_stats["upserted"],
                entity_stats["skipped_chain"],
                entity_stats["errors"],
            )
    except CreditBudgetExhausted as e:
        logger.error("ABORTING: %s", e)
        summary["aborted_reason"] = str(e)

    logger.info(
        "Backfill complete: %d entities processed, %d total addresses, "
        "%d upserted, %d errors",
        summary["entities_processed"],
        summary["total_addresses"],
        summary["total_upserted"],
        summary["total_errors"],
    )
    return summary


def _process_entity(
    entity: Entity,
    *,
    ark: ArkhamClient,
    sb,
    live: bool,
    limit: int,
) -> Dict[str, int]:
    stats = {"found": 0, "upserted": 0, "skipped_chain": 0, "errors": 0}

    try:
        transfers = ark.get_entity_transfers(
            entity.slug, limit=limit, min_usd=entity.min_usd,
        )
    except CreditBudgetExhausted:
        raise
    except Exception as e:
        logger.error("Failed to fetch transfers for %s: %s", entity.slug, e)
        stats["errors"] += 1
        return stats

    addresses = ark.extract_addresses(transfers, entity.slug)
    stats["found"] = len(addresses)

    for addr_info in addresses:
        arkham_chain = addr_info["chain"]
        our_chain = ARKHAM_CHAIN_MAP.get(arkham_chain)
        if our_chain is None:
            logger.debug(
                "Skipping unmapped chain %s for %s", arkham_chain, entity.slug,
            )
            stats["skipped_chain"] += 1
            continue

        row = build_upsert_row(addr_info, our_chain, entity)

        if not live:
            logger.debug("DRY-RUN: would upsert %s on %s", row["address"][:16], our_chain)
            stats["upserted"] += 1
            continue

        try:
            existing = _fetch_existing(sb, row["address"], our_chain)
            merged = merge_with_existing(row, existing)
            sb.table("addresses").upsert(
                merged, on_conflict="address,blockchain"
            ).execute()
            stats["upserted"] += 1
        except Exception as e:
            logger.error(
                "Upsert failed for %s/%s: %s",
                row["address"][:16], our_chain, e,
            )
            stats["errors"] += 1

    return stats


def _fetch_existing(sb, address: str, blockchain: str) -> Optional[Dict[str, Any]]:
    """Fetch the current row from addresses table (if any) for conflict merge."""
    try:
        result = (
            sb.table("addresses")
            .select("confidence, analysis_tags")
            .eq("address", address)
            .eq("blockchain", blockchain)
            .limit(1)
            .execute()
        )
        rows = result.data or []
        return rows[0] if rows else None
    except Exception:
        return None


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Arkham address backfill via transfer mining.",
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Write upserts to Supabase (default: dry-run)",
    )
    parser.add_argument(
        "--limit", type=int, default=50,
        help="Max transfers to fetch per entity (default: 50)",
    )
    args = parser.parse_args()

    mode = "LIVE" if args.live else "DRY-RUN"
    logger.info("Arkham backfill starting (%s), %d entities, limit=%d",
                mode, len(ENTITY_SEED_LIST), args.limit)

    summary = run_backfill(live=args.live, limit=args.limit)

    if summary.get("aborted_reason"):
        logger.error("Run aborted: %s", summary["aborted_reason"])
        return 2

    return 0 if summary["total_errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
