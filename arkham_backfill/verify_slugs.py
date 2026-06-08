#!/usr/bin/env python3
"""Validate the Arkham entity seed list against the live API.

Each entity in ENTITY_SEED_LIST is checked with GET /intelligence/entity/{slug}
(1 credit, per Arkham credit pricing). A 4xx response means the slug is not a
real/queryable entity — exactly the class of bad slugs that make the transfer
miner emit 400s every run (e.g. "blackrock-bitcoin-etf", "axelar",
"optimism-foundation"). Validating here fixes the problem at the source.

Usage:
    python -m arkham_backfill.verify_slugs --dry-run     # report only (default)
    python -m arkham_backfill.verify_slugs --report data/arkham_slugs.json

This never edits entities.py automatically (to avoid corrupting the seed file).
It prints the invalid slugs and, with --report, writes a JSON file the operator
can use to prune the list by hand.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

import httpx

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from arkham_backfill.arkham_client import ArkhamClient, CreditBudgetExhausted  # noqa: E402
from arkham_backfill.entities import ENTITY_SEED_LIST  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def validate_slugs(client: ArkhamClient) -> Dict[str, List[str]]:
    """Check every seed slug. Returns {'valid': [...], 'invalid': [...]}."""
    valid: List[str] = []
    invalid: List[str] = []

    for entity in ENTITY_SEED_LIST:
        slug = entity.slug
        try:
            client.get_entity(slug)
            valid.append(slug)
            logger.info("OK    %s", slug)
        except CreditBudgetExhausted:
            logger.error("Credit floor hit — aborting validation early.")
            break
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if 400 <= status < 500:
                invalid.append(slug)
                logger.warning("BAD   %s (HTTP %d — not a queryable entity)", slug, status)
            else:
                logger.error("ERR   %s (HTTP %d)", slug, status)
        except Exception as e:  # network etc. — don't mark invalid on transient errors
            logger.error("ERR   %s (%s)", slug, e)

    return {"valid": valid, "invalid": invalid}


def main() -> int:
    p = argparse.ArgumentParser(description="Validate Arkham entity seed slugs.")
    p.add_argument(
        "--dry-run", action="store_true",
        help="Report only (default behavior; flag kept for symmetry).",
    )
    p.add_argument(
        "--report", type=str, default=None,
        help="Optional path to write a JSON {valid, invalid} report.",
    )
    args = p.parse_args()

    api_key = os.getenv("ARKHAM_API_KEY", "").strip()
    if not api_key:
        logger.error("ARKHAM_API_KEY is not set.")
        return 3

    logger.info("Validating %d seed slugs against /intelligence/entity…", len(ENTITY_SEED_LIST))
    with ArkhamClient(api_key=api_key) as client:
        result = validate_slugs(client)
        credits_left = client.last_credits_remaining

    logger.info(
        "Validation complete — %d valid, %d invalid, credits_remaining=%s",
        len(result["valid"]), len(result["invalid"]),
        credits_left if credits_left is not None else "unknown",
    )
    if result["invalid"]:
        logger.warning("Invalid slugs (prune these from entities.py): %s", ", ".join(result["invalid"]))

    if args.report:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(result, indent=2))
        logger.info("Wrote report to %s", report_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
