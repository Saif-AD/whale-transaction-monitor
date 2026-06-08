#!/usr/bin/env python3
"""Arkham wallet-discovery cron — designed for Railway.

Each run:
  1. Mines transfers for a rotating batch of Arkham entities.
  2. Upserts discovered addresses into the `addresses` table.
  3. Bridges new labels into `curated_entities.addresses` so figure/entity
     pages deepen automatically.

Entity batches rotate by UTC hour so we don't burn the full 86-entity seed
list (and Arkham credits) in a single slot. The credit-floor guard in
ArkhamClient still aborts early if balance drops below CREDIT_FLOOR.

Usage:
    python scripts/sync_arkham_cron.py --dry-run
    python scripts/sync_arkham_cron.py --live
    python scripts/sync_arkham_cron.py --live --batch-size 20 --transfer-limit 50
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from arkham_backfill.arkham_client import ArkhamClient  # noqa: E402
from arkham_backfill.backfill import run_backfill  # noqa: E402
from arkham_backfill.entities import ENTITY_SEED_LIST, Entity  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Leave headroom inside a 30-min Railway cron window.
MAX_RUNTIME_SEC = 25 * 60


def pick_entity_batch(
    batch_size: int, *, offset: Optional[int] = None
) -> tuple[List[Entity], int, int]:
    """Return (batch, start_index, total_entities).

    When offset is None, rotate by UTC hour so successive cron runs march
    through the full seed list without needing external state.
    """
    entities = list(ENTITY_SEED_LIST)
    total = len(entities)
    if batch_size >= total:
        return entities, 0, total

    if offset is None:
        hour_slot = int(datetime.now(timezone.utc).timestamp() // 3600)
        start = (hour_slot * batch_size) % total
    else:
        start = offset % total

    batch = [entities[(start + i) % total] for i in range(batch_size)]
    return batch, start, total


def main() -> int:
    p = argparse.ArgumentParser(description="Arkham wallet-discovery cron.")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--live", action="store_true", help="Write to Supabase.")
    g.add_argument("--dry-run", action="store_true", help="No writes (default).")
    p.add_argument(
        "--batch-size", type=int, default=15,
        help="Entities to process this run (default 15; full list rotates hourly).",
    )
    p.add_argument(
        "--transfer-limit", type=int, default=40,
        help="Max transfers to fetch per entity (default 40).",
    )
    p.add_argument(
        "--offset", type=int, default=None,
        help="Force batch start index (default: auto-rotate by UTC hour).",
    )
    p.add_argument(
        "--no-enrich", action="store_true",
        help="Skip curated_entities bridge after backfill.",
    )
    p.add_argument(
        "--max-per-entity", type=int, default=25,
        help="Max new addresses to append per curated entity (default 25).",
    )
    args = p.parse_args()

    api_key = os.getenv("ARKHAM_API_KEY", "").strip()
    if not api_key:
        logger.error(
            "ARKHAM_API_KEY is not set. Add it to the Railway service env vars."
        )
        return 3

    live = args.live and not args.dry_run
    mode = "LIVE" if live else "DRY-RUN"
    batch, start, total = pick_entity_batch(args.batch_size, offset=args.offset)

    slugs = ", ".join(e.slug for e in batch[:5])
    if len(batch) > 5:
        slugs += f", … (+{len(batch) - 5} more)"

    logger.info(
        "Arkham cron starting (%s) — batch=%d offset=%d/%d limit=%d | %s",
        mode, len(batch), start, total, args.transfer_limit, slugs,
    )

    t0 = time.monotonic()
    with ArkhamClient(api_key=api_key) as ark:
        summary = run_backfill(
            live=live,
            limit=args.transfer_limit,
            entities=batch,
            client=ark,
        )
        credits_left = ark.last_credits_remaining

    elapsed = time.monotonic() - t0
    logger.info(
        "Backfill done in %.0fs — entities=%d addresses=%d upserted=%d "
        "skipped_chain=%d errors=%d credits_remaining=%s",
        elapsed,
        summary.get("entities_processed", 0),
        summary.get("total_addresses", 0),
        summary.get("total_upserted", 0),
        summary.get("total_skipped_chain", 0),
        summary.get("total_errors", 0),
        credits_left if credits_left is not None else "unknown",
    )

    if summary.get("aborted_reason"):
        logger.warning("Backfill aborted: %s", summary["aborted_reason"])
        return 2

    # Bridge addresses → curated_entities so figure pages deepen.
    if live and not args.no_enrich and summary.get("total_upserted", 0) > 0:
        remaining = MAX_RUNTIME_SEC - (time.monotonic() - t0)
        if remaining > 120:
            logger.info("Bridging labels into curated_entities (%.0fs left)…", remaining)
            try:
                from scripts.enrich_curated_entities_from_labels import run as enrich_run

                enrich_summary = enrich_run(
                    live=True,
                    only=None,
                    max_per_entity=args.max_per_entity,
                    verify_threshold=0.8,
                    fetch_limit=100,
                    include_prefix=False,
                )
                logger.info(
                    "Enrich done — scanned=%d enriched=%d added=%d",
                    enrich_summary.get("entities_scanned", 0),
                    enrich_summary.get("entities_enriched", 0),
                    enrich_summary.get("addresses_added", 0),
                )
            except Exception as e:
                logger.error("curated_entities enrich failed (non-fatal): %s", e)
        else:
            logger.warning("Skipping enrich — only %.0fs left in runtime budget", remaining)

    if summary.get("total_errors", 0) > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
