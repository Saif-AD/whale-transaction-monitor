#!/usr/bin/env python3
"""Backfill audience-facing reasoning via Grok on historical whale transactions.

Selects rows from all_whale_transactions in the last N days with
usd_value >= INTERPRETER_LABELED_USD_THRESHOLD (labeled threshold used for
backfill to maximize historical coverage).  Updates the per-chain table only.

Resumable via reasoning_backfill_progress.  Rate-limited to 1 xAI call/sec.

Default is dry-run.  Use --live to write.  Run after scripts/backfill_labels.py.

Prerequisite: XAI_API_KEY, progress table (see deploy/README.md).
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
from shared.config import (
    INTERPRETER_LABELED_USD_THRESHOLD,
    BACKFILL_MAX_COST_USD,
    BACKFILL_COST_PER_CALL_USD,
)
from shared.interpreter import generate_interpretation
from utils.supabase_writer import CHAIN_TABLE_MAP

PROGRESS_TABLE = "reasoning_backfill_progress"
PAGE_SIZE = 300
XAI_INTERVAL_SEC = 1.0


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


def _paginate_window(client, since_iso: str, min_usd: float):
    offset = 0
    while True:
        r = (
            client.table("all_whale_transactions")
            .select(
                "transaction_hash,blockchain,from_address,to_address,from_label,to_label,"
                "token_symbol,usd_value,classification,timestamp"
            )
            .gte("timestamp", since_iso)
            .gte("usd_value", min_usd)
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


def _row_to_tx(row: dict) -> dict:
    return {
        "transaction_hash": row.get("transaction_hash", ""),
        "tx_hash": row.get("transaction_hash", ""),
        "token_symbol": row.get("token_symbol") or "",
        "usd_value": float(row.get("usd_value") or 0),
        "classification": row.get("classification") or "TRANSFER",
        "blockchain": (row.get("blockchain") or "").lower(),
        "from_label": (row.get("from_label") or "").strip(),
        "to_label": (row.get("to_label") or "").strip(),
        "counterparty_type": "",
        "is_cex_transaction": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill Grok reasoning on whale tx tables.")
    parser.add_argument("--live", action="store_true", help="Write updates and progress (default: dry-run)")
    parser.add_argument("--days", type=int, default=30, help="Look back window (default: 30)")
    args = parser.parse_args()
    live = args.live

    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    since_iso = since.isoformat()
    min_usd = float(INTERPRETER_LABELED_USD_THRESHOLD)

    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    progress_done = _load_progress_hashes(client)

    total_candidates = 0
    by_chain_count: dict[str, int] = defaultdict(int)
    for page in _paginate_window(client, since_iso, min_usd):
        for row in page:
            txh = row.get("transaction_hash") or ""
            if not txh or txh in progress_done:
                continue
            total_candidates += 1
            chain = (row.get("blockchain") or "unknown").lower()
            by_chain_count[chain] += 1

    est_cost = total_candidates * BACKFILL_COST_PER_CALL_USD
    est_seconds = total_candidates * XAI_INTERVAL_SEC

    print(f"Reasoning backfill — {'LIVE' if live else 'DRY-RUN'}")
    print(f"  Window: last {args.days} days (since {since_iso})")
    print(f"  usd_value >= {min_usd:,.0f} (INTERPRETER_LABELED_USD_THRESHOLD)")
    print(f"  Candidates (not in {PROGRESS_TABLE}): {total_candidates:,}")
    print(f"  Per-chain (candidate counts): {dict(by_chain_count)}")
    print(f"  Est. cost @ ${BACKFILL_COST_PER_CALL_USD}/call: ${est_cost:,.2f}")
    print(f"  BACKFILL_MAX_COST_USD: ${BACKFILL_MAX_COST_USD:,.2f}")
    print(f"  Est. runtime @ 1 call/sec: {est_seconds / 60:.1f} min ({est_seconds:.0f}s)")
    print()

    if est_cost > BACKFILL_MAX_COST_USD:
        print(
            f"ABORT: estimated cost ${est_cost:,.2f} exceeds BACKFILL_MAX_COST_USD "
            f"(${BACKFILL_MAX_COST_USD:,.2f}). Narrow --days or raise the cap."
        )
        return 2

    if total_candidates == 0:
        print("Nothing to do.")
        return 0

    if not live:
        print("Dry-run: no xAI calls or DB writes. Re-run with --live to apply.")
        return 0

    processed = 0
    updated_by_chain: dict[str, int] = defaultdict(int)
    errors = 0
    last_call = 0.0

    for page in _paginate_window(client, since_iso, min_usd):
        for row in page:
            txh = row.get("transaction_hash") or ""
            if not txh or txh in progress_done:
                continue

            blockchain = (row.get("blockchain") or "").lower()
            table = CHAIN_TABLE_MAP.get(blockchain)
            if not table:
                print(f"  SKIP unknown blockchain={blockchain!r} tx={txh[:16]}...")
                errors += 1
                continue

            tx_payload = _row_to_tx(row)

            now = time.monotonic()
            wait = XAI_INTERVAL_SEC - (now - last_call)
            if wait > 0:
                time.sleep(wait)
            last_call = time.monotonic()
            try:
                reasoning = generate_interpretation(tx_payload)
                client.table(table).update({"reasoning": reasoning}).eq(
                    "transaction_hash", txh
                ).execute()
                client.table(PROGRESS_TABLE).upsert(
                    {"tx_hash": txh}, on_conflict="tx_hash"
                ).execute()
            except Exception as e:
                print(f"  ERROR tx={txh[:16]}... {e}")
                errors += 1
                last_call = time.monotonic()
                continue

            processed += 1
            updated_by_chain[blockchain] += 1
            progress_done.add(txh)

    print()
    print(f"Done. Processed: {processed:,}  Errors: {errors}")
    print(f"Per-chain (processed): {dict(updated_by_chain)}")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
