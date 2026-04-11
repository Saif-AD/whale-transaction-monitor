#!/usr/bin/env python3
"""Backfill from_label / to_label on per-chain tables from the addresses table.

Reads candidates from all_whale_transactions (last N days) where either label
is missing, resolves labels via lookup_labels(), and PATCHes the underlying
per-chain row.  Resumable via label_backfill_progress.

Default is dry-run.  Use --live to write.  Run before scripts/backfill_reasoning.py.

Prerequisite: create progress table (see deploy/README.md).
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from supabase import create_client

from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
from shared.address_labels import lookup_labels
from utils.supabase_writer import CHAIN_TABLE_MAP

PROGRESS_TABLE = "label_backfill_progress"
PAGE_SIZE = 500


def _needs_labels(row: dict) -> bool:
    fl = row.get("from_label")
    tl = row.get("to_label")
    if fl is None or (isinstance(fl, str) and not fl.strip()):
        return True
    if tl is None or (isinstance(tl, str) and not tl.strip()):
        return True
    return False


def _load_progress_hashes(client) -> set[str]:
    done: set[str] = set()
    offset = 0
    batch = 5000
    while True:
        r = (
            client.table(PROGRESS_TABLE)
            .select("tx_hash")
            .range(offset, offset + batch - 1)
            .execute()
        )
        rows = r.data or []
        for row in rows:
            done.add(row["tx_hash"])
        if len(rows) < batch:
            break
        offset += batch
    return done


def _paginate_candidates(client, since_iso: str):
    """Yield pages of rows from all_whale_transactions in the time window."""
    offset = 0
    while True:
        r = (
            client.table("all_whale_transactions")
            .select(
                "transaction_hash,blockchain,from_address,to_address,"
                "from_label,to_label,timestamp"
            )
            .gte("timestamp", since_iso)
            .order("timestamp", desc=True)
            .order("transaction_hash", desc=True)
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        rows = r.data or []
        if not rows:
            break
        yield rows
        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill address labels on whale tx tables.")
    parser.add_argument("--live", action="store_true", help="Write updates and progress rows (default: dry-run)")
    parser.add_argument("--days", type=int, default=30, help="Look back window (default: 30)")
    parser.add_argument(
        "--rate",
        type=float,
        default=50.0,
        help="Max operations per second against Supabase (default: 50)",
    )
    args = parser.parse_args()
    live = args.live
    min_interval = 1.0 / max(args.rate, 0.1)

    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    since_iso = since.isoformat()

    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    progress_done = _load_progress_hashes(client)

    # Pass 1: count candidates
    total_candidates = 0
    by_chain_count: dict[str, int] = defaultdict(int)
    for page in _paginate_candidates(client, since_iso):
        for row in page:
            txh = row.get("transaction_hash") or ""
            if not txh or txh in progress_done:
                continue
            if not _needs_labels(row):
                continue
            total_candidates += 1
            chain = (row.get("blockchain") or "unknown").lower()
            by_chain_count[chain] += 1

    est_seconds = total_candidates * min_interval
    print(f"Label backfill — {'LIVE' if live else 'DRY-RUN'}")
    print(f"  Window: last {args.days} days (since {since_iso})")
    print(f"  Candidates (missing label, not yet in {PROGRESS_TABLE}): {total_candidates:,}")
    print(f"  Per-chain (candidate counts): {dict(by_chain_count)}")
    print(f"  Rate limit: {args.rate}/sec → est. runtime {est_seconds / 60:.1f} min ({est_seconds:.0f}s)")
    print()

    if total_candidates == 0:
        print("Nothing to do.")
        return 0

    if not live:
        print("Dry-run: no writes or label lookups. Re-run with --live to apply.")
        return 0

    processed = 0
    updated_by_chain: dict[str, int] = defaultdict(int)
    errors = 0
    last_tick = time.monotonic()

    for page in _paginate_candidates(client, since_iso):
        for row in page:
            txh = row.get("transaction_hash") or ""
            if not txh or txh in progress_done:
                continue
            if not _needs_labels(row):
                continue

            blockchain = (row.get("blockchain") or "").lower()
            table = CHAIN_TABLE_MAP.get(blockchain)
            if not table:
                print(f"  SKIP unknown blockchain={blockchain!r} tx={txh[:16]}...")
                errors += 1
                continue

            from_addr = row.get("from_address") or row.get("from") or ""
            to_addr = row.get("to_address") or row.get("to") or ""
            from_label, to_label = lookup_labels(from_addr, to_addr, blockchain)

            try:
                client.table(table).update(
                    {"from_label": from_label, "to_label": to_label}
                ).eq("transaction_hash", txh).execute()
                client.table(PROGRESS_TABLE).upsert(
                    {"tx_hash": txh}, on_conflict="tx_hash"
                ).execute()
            except Exception as e:
                print(f"  ERROR tx={txh[:16]}... {e}")
                errors += 1
                continue

            processed += 1
            updated_by_chain[blockchain] += 1
            progress_done.add(txh)

            elapsed = time.monotonic() - last_tick
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            last_tick = time.monotonic()

    print()
    print(f"Done. Processed: {processed:,}  Errors: {errors}")
    print(f"Per-chain (processed): {dict(updated_by_chain)}")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
