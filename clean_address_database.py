#!/usr/bin/env python3
"""
Address Database Cleaner

Scans Supabase addresses table and removes junk:
  1. Empty addresses: no entity_name, no balance, no useful data
  2. Duplicates: same address on same chain (keeps highest confidence)
  3. Dead labels: addresses with auto-generated labels like "Verified Whale"
     but no actual identity or balance data
  4. Low confidence: addresses below threshold with no other redeeming quality
  5. Stale addresses: old entries with no recent activity data

Safe to run multiple times. Shows what it will delete before doing it.
Requires explicit confirmation before any deletion.
"""

import sys
import json
import time
from datetime import datetime
from collections import defaultdict

from supabase import create_client
from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

BATCH_SIZE = 200

# Junk detection patterns
GARBAGE_LABELS = [
    'High-Activity Exchange Wallet',
    'DEX Router/Pool',
    'Verified Whale',
    'Cross-Chain Verified Whale',
    'Bridge Whale',
    'Stablecoin Whale',
    'Smart Money',
    'Volume Whale',
]

# Sources from the old flooding scripts
JUNK_SOURCES = [
    'bigquery_ethereum_cex_pattern',
    'bigquery_ethereum_dex_pattern',
    'bigquery_ethereum_whale_verification',
    'bigquery_polygon_cex_pattern',
    'bigquery_polygon_dex_pattern',
    'bigquery_polygon_whale_verification',
    'bigquery_bsc_cex_pattern',
    'bigquery_bsc_dex_pattern',
    'bigquery_bsc_whale_verification',
    'bigquery_bitcoin_cex_pattern',
    'bigquery_bitcoin_whale_verification',
    'bigquery_crosschain_verified',
]

# Minimum confidence for addresses with no entity_name and no balance
MIN_UNNAMED_CONFIDENCE = 0.70


def get_supabase():
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


def fetch_all_addresses(sb):
    """Fetch all addresses from Supabase in pages."""
    all_rows = []
    page_size = 1000
    offset = 0

    print("  Loading addresses from Supabase...")
    while True:
        result = (sb.table('addresses')
                  .select('id, address, blockchain, address_type, label, '
                          'entity_name, confidence, balance_usd, balance_native, '
                          'source, detection_method')
                  .range(offset, offset + page_size - 1)
                  .execute())
        if not result.data:
            break
        all_rows.extend(result.data)
        offset += page_size
        if len(result.data) < page_size:
            break

    print(f"  Loaded {len(all_rows):,} addresses")
    return all_rows


def find_junk(rows):
    """
    Identify junk addresses. Returns list of (id, reason) tuples.
    An address is junk if ALL of these are true:
    - No entity_name (we don't know who it is)
    - No meaningful balance (balance_usd is null or < $1000)
    - Source is from a known junk source OR label is auto-generated
    """
    junk = []
    reasons = defaultdict(int)

    for row in rows:
        row_id = row['id']
        entity_name = row.get('entity_name')
        balance_usd = float(row.get('balance_usd') or 0)
        label = row.get('label') or ''
        source = row.get('source') or ''
        confidence = float(row.get('confidence') or 0)

        # Skip if it has a real identity
        if entity_name and entity_name.strip():
            continue

        # Skip if it has meaningful balance
        if balance_usd >= 1000:
            continue

        # Check: from a known junk source?
        is_junk_source = source in JUNK_SOURCES

        # Check: auto-generated label?
        is_garbage_label = any(label.startswith(g) for g in GARBAGE_LABELS)

        # Check: low confidence with no identity
        is_low_conf_unnamed = confidence < MIN_UNNAMED_CONFIDENCE

        if is_junk_source:
            junk.append((row_id, f"junk_source:{source}"))
            reasons['junk_source'] += 1
        elif is_garbage_label:
            junk.append((row_id, f"garbage_label:{label[:50]}"))
            reasons['garbage_label'] += 1
        elif is_low_conf_unnamed:
            junk.append((row_id, f"low_conf_unnamed:{confidence:.2f}"))
            reasons['low_conf_unnamed'] += 1

    return junk, dict(reasons)


def find_duplicates(rows):
    """
    Find duplicate addresses (same address+blockchain).
    Keeps the one with highest confidence, marks rest for deletion.
    """
    groups = defaultdict(list)
    for row in rows:
        key = (row['address'].lower(), row['blockchain'])
        groups[key].append(row)

    dupes = []
    for key, group in groups.items():
        if len(group) <= 1:
            continue
        # Sort by: has entity_name first, then confidence desc
        group.sort(key=lambda r: (
            bool(r.get('entity_name')),
            float(r.get('confidence') or 0),
            float(r.get('balance_usd') or 0),
        ), reverse=True)
        # Keep first, mark rest as dupes
        for r in group[1:]:
            dupes.append((r['id'], f"duplicate_of:{group[0]['id']}"))

    return dupes


def delete_batch(sb, ids):
    """Delete addresses by ID in batches."""
    deleted = 0
    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i:i + BATCH_SIZE]
        try:
            sb.table('addresses').delete().in_('id', batch).execute()
            deleted += len(batch)
        except Exception as e:
            print(f"    Delete error: {e}")
    return deleted


def main():
    sb = get_supabase()

    before = sb.table('addresses').select('id', count='exact').execute()
    before_named = (sb.table('addresses').select('id', count='exact')
                    .not_.is_('entity_name', 'null').execute())

    print("=" * 70)
    print("  ADDRESS DATABASE CLEANER")
    print("=" * 70)
    print(f"  Total addresses:  {before.count:,}")
    print(f"  With entity_name: {before_named.count:,}")
    print(f"  Without name:     {before.count - before_named.count:,}")
    print("=" * 70)
    print()

    rows = fetch_all_addresses(sb)
    if not rows:
        print("  No addresses found. Nothing to clean.")
        return

    # Find junk
    print("\n  Scanning for junk addresses...")
    junk, junk_reasons = find_junk(rows)
    print(f"  Found {len(junk):,} junk addresses:")
    for reason, count in sorted(junk_reasons.items(), key=lambda x: -x[1]):
        print(f"    {reason}: {count:,}")

    # Find duplicates
    print("\n  Scanning for duplicates...")
    dupes = find_duplicates(rows)
    print(f"  Found {len(dupes):,} duplicate entries")

    # Combine
    all_to_delete = {}
    for row_id, reason in junk + dupes:
        all_to_delete[row_id] = reason

    total_delete = len(all_to_delete)
    if total_delete == 0:
        print("\n  Database is clean. Nothing to delete.")
        return

    print(f"\n  TOTAL to delete: {total_delete:,} / {len(rows):,} "
          f"({total_delete / len(rows) * 100:.1f}%)")
    print(f"  Will keep: {len(rows) - total_delete:,} addresses")
    print()

    # Show sample of what will be deleted
    print("  Sample deletions (first 10):")
    for i, (row_id, reason) in enumerate(list(all_to_delete.items())[:10]):
        matching = [r for r in rows if r['id'] == row_id]
        if matching:
            r = matching[0]
            print(f"    [{reason}] {r['address'][:16]}... "
                  f"({r['blockchain']}) label='{(r.get('label') or '')[:30]}' "
                  f"balance=${float(r.get('balance_usd') or 0):,.0f}")
    print()

    response = input(f"  Delete {total_delete:,} junk addresses? (yes/no): ")
    if response.lower() != 'yes':
        print("  Aborted.")
        return

    print(f"\n  Deleting {total_delete:,} addresses...")
    ids_to_delete = list(all_to_delete.keys())
    deleted = delete_batch(sb, ids_to_delete)

    after = sb.table('addresses').select('id', count='exact').execute()
    after_named = (sb.table('addresses').select('id', count='exact')
                   .not_.is_('entity_name', 'null').execute())

    print()
    print("=" * 70)
    print("  CLEANUP COMPLETE")
    print("=" * 70)
    print(f"  Deleted:     {deleted:,}")
    print(f"  Before:      {before.count:,} ({before_named.count:,} named)")
    print(f"  After:       {after.count:,} ({after_named.count:,} named)")
    print(f"  Named ratio: {after_named.count / after.count * 100:.1f}% "
          f"(was {before_named.count / before.count * 100:.1f}%)")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted.")
        sys.exit(0)
