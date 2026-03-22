#!/usr/bin/env python3
"""
Address Database Cleaner

Identifies and removes junk/duplicate entries from the Supabase `addresses`
table that were inserted by old flooding scripts (bigquery_multichain_discovery,
whale_discovery_agent, bigquery_crosschain_verified_whales).

Safety:
  - Addresses WITH a real entity_name are NEVER deleted.
  - Addresses with significant balance (>= $1,000 in analysis_tags) are kept.
  - Requires explicit "yes" confirmation before deleting anything.
  - Duplicates are resolved by keeping the row with highest confidence / best data.
"""

import argparse
import json
import logging
import re
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Set, Tuple

from supabase import create_client

from config.api_keys import SUPABASE_SERVICE_ROLE_KEY, SUPABASE_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("cleaner")

# ---------------------------------------------------------------------------
# Junk detection patterns
# ---------------------------------------------------------------------------

JUNK_SOURCE_PATTERNS = [
    r"^bigquery_.*_pattern$",
    r"^bigquery_.*_verification$",
    r"^bigquery_crosschain_verified",
    r"^bigquery_multichain",
    r"^whale_discovery_agent",
]

JUNK_LABELS = {
    "verified whale",
    "smart money",
    "dex router/pool",
    "dex router",
    "bridge whale",
    "stablecoin whale",
    "high-activity exchange wallet",
    "volume whale",
    "cross-chain verified whale",
    "cross-chain whale",
    "multi-chain whale",
    "active whale",
    "whale",
}

MIN_BALANCE_TO_KEEP = 1_000  # USD


def _is_junk_source(source: str) -> bool:
    if not source:
        return False
    for pat in JUNK_SOURCE_PATTERNS:
        if re.match(pat, source, re.IGNORECASE):
            return True
    return False


def _is_junk_label(label: str) -> bool:
    if not label:
        return False
    return label.strip().lower() in JUNK_LABELS


def _get_balance_usd(row: Dict) -> float:
    tags = row.get("analysis_tags")
    if not tags:
        return 0.0
    if isinstance(tags, str):
        try:
            tags = json.loads(tags)
        except (json.JSONDecodeError, TypeError):
            return 0.0
    if isinstance(tags, dict):
        return float(tags.get("balance_usd", 0) or 0)
    return 0.0


def _row_quality_score(row: Dict) -> Tuple:
    """Higher is better. Used to pick the best row among duplicates."""
    has_name = 1 if row.get("entity_name") else 0
    conf = float(row.get("confidence") or 0)
    bal = _get_balance_usd(row)
    verified = 1 if row.get("is_verified") else 0
    return (has_name, conf, bal, verified)


# ---------------------------------------------------------------------------
# Fetch all addresses
# ---------------------------------------------------------------------------

PAGE_SIZE = 1000


def fetch_all_addresses(sb) -> List[Dict]:
    """Paginate through the entire addresses table."""
    all_rows = []
    offset = 0
    while True:
        resp = (
            sb.table("addresses")
            .select("*")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        all_rows.extend(batch)
        offset += len(batch)
        if len(batch) < PAGE_SIZE:
            break
        if offset % 10_000 == 0:
            log.info(f"  Fetched {offset:,} rows...")
    return all_rows


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def analyze(rows: List[Dict]) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Returns (junk_rows, duplicate_rows_to_delete, kept_rows).
    """
    junk: List[Dict] = []
    kept: List[Dict] = []

    for row in rows:
        entity_name = row.get("entity_name")
        if entity_name and entity_name.strip():
            kept.append(row)
            continue

        balance = _get_balance_usd(row)
        if balance >= MIN_BALANCE_TO_KEEP:
            kept.append(row)
            continue

        source = row.get("source") or ""
        label = row.get("label") or ""

        if _is_junk_source(source) or _is_junk_label(label):
            junk.append(row)
        else:
            kept.append(row)

    # Duplicate detection among kept rows
    groups: Dict[str, List[Dict]] = defaultdict(list)
    for row in kept:
        key = (row.get("address", "").lower(), row.get("blockchain", ""))
        groups[f"{key[0]}|{key[1]}"].append(row)

    dup_to_delete: List[Dict] = []
    final_kept: List[Dict] = []

    for key, group in groups.items():
        if len(group) <= 1:
            final_kept.extend(group)
            continue
        group.sort(key=_row_quality_score, reverse=True)
        final_kept.append(group[0])
        dup_to_delete.extend(group[1:])

    return junk, dup_to_delete, final_kept


# ---------------------------------------------------------------------------
# Deletion
# ---------------------------------------------------------------------------

DELETE_BATCH = 50


def delete_rows(sb, rows: List[Dict], label: str) -> int:
    deleted = 0
    ids = [r["id"] for r in rows if r.get("id")]
    for i in range(0, len(ids), DELETE_BATCH):
        batch_ids = ids[i : i + DELETE_BATCH]
        try:
            sb.table("addresses").delete().in_("id", batch_ids).execute()
            deleted += len(batch_ids)
        except Exception as e:
            log.error(f"Delete batch failed: {e}")
            for rid in batch_ids:
                try:
                    sb.table("addresses").delete().eq("id", rid).execute()
                    deleted += 1
                except Exception as e2:
                    log.error(f"  Single delete failed (id={rid}): {e2}")

        if (i + DELETE_BATCH) % 500 == 0:
            log.info(f"  [{label}] Deleted {deleted:,}/{len(ids):,}")

    return deleted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Clean junk/duplicate addresses from Supabase")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    parser.add_argument("--dry-run", action="store_true", help="Analyze only, don't delete")
    args = parser.parse_args()

    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    print("=" * 70)
    print("  ADDRESS DATABASE CLEANER")
    print("=" * 70)
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    log.info("Fetching all addresses from Supabase...")
    rows = fetch_all_addresses(sb)
    log.info(f"Fetched {len(rows):,} total addresses")

    junk, dups, kept = analyze(rows)

    # Summarise
    print()
    print("=" * 70)
    print("  ANALYSIS RESULTS")
    print("=" * 70)
    print(f"  Total addresses:            {len(rows):>8,}")
    print(f"  Junk (to delete):           {len(junk):>8,}")
    print(f"  Duplicates (to delete):     {len(dups):>8,}")
    print(f"  Clean (to keep):            {len(kept):>8,}")
    print()

    # Breakdown by source pattern
    source_counts: Dict[str, int] = defaultdict(int)
    for r in junk:
        source_counts[r.get("source") or "(empty)"] += 1
    if source_counts:
        print("  Junk breakdown by source:")
        for src, cnt in sorted(source_counts.items(), key=lambda x: -x[1])[:15]:
            print(f"    {src:45s} {cnt:>6,}")
        print()

    label_counts: Dict[str, int] = defaultdict(int)
    for r in junk:
        label_counts[r.get("label") or "(empty)"] += 1
    if label_counts:
        print("  Junk breakdown by label:")
        for lbl, cnt in sorted(label_counts.items(), key=lambda x: -x[1])[:15]:
            print(f"    {lbl:45s} {cnt:>6,}")
        print()

    chain_junk: Dict[str, int] = defaultdict(int)
    for r in junk:
        chain_junk[r.get("blockchain") or "unknown"] += 1
    if chain_junk:
        print("  Junk breakdown by chain:")
        for ch, cnt in sorted(chain_junk.items(), key=lambda x: -x[1]):
            print(f"    {ch:20s} {cnt:>8,}")
        print()

    total_to_delete = len(junk) + len(dups)
    if total_to_delete == 0:
        print("  Nothing to clean. Database looks good!")
        return

    if args.dry_run:
        print("  [DRY RUN] No changes made.")
        return

    print(f"  Will delete {total_to_delete:,} rows total.")
    print()

    if not args.yes:
        confirm = input("  Type 'yes' to proceed with deletion: ").strip().lower()
        if confirm != "yes":
            print("  Aborted.")
            return

    print()
    deleted_junk = delete_rows(sb, junk, "junk")
    deleted_dups = delete_rows(sb, dups, "duplicates")

    print()
    print("=" * 70)
    print("  CLEANUP COMPLETE")
    print("=" * 70)
    print(f"  Junk deleted:       {deleted_junk:>8,}")
    print(f"  Duplicates deleted: {deleted_dups:>8,}")
    print(f"  Rows remaining:     {len(rows) - deleted_junk - deleted_dups:>8,}")
    print("=" * 70)


if __name__ == "__main__":
    main()
