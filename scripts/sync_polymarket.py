#!/usr/bin/env python3
"""Sync Polymarket markets + whales into Supabase for the Sonar terminal.

Pulls from the public Polymarket APIs (via polymarket.PolymarketClient) and
upserts into:
  * polymarket_markets         — top markets w/ odds, volume, whale flow
  * polymarket_market_holders  — top holders per market (whale ↔ market)
  * polymarket_whales          — aggregated whale leaderboard

The frontend "Polymarket terminal" reads these tables (same pattern as
all_whale_transactions). Tables: see migrations/polymarket_tables.sql.

Designed for a Railway cron slot (see railway.polymarket.toml). Default is
DRY-RUN; use --live to write.

NOTE: Polymarket is behind Cloudflare and resets very old local TLS stacks
(macOS LibreSSL). This runs fine on Railway / modern Python. If a local run
fails with an SSL/connection reset, that's the environment, not the code.

Usage:
    python scripts/sync_polymarket.py                 # dry-run
    python scripts/sync_polymarket.py --live          # write to Supabase
    python scripts/sync_polymarket.py --live --markets 30 --holders 15
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from polymarket import PolymarketClient  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_payload(
    *, markets_limit: int, holders_per_market: int
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch everything from Polymarket and shape it into table rows."""
    market_rows: List[Dict[str, Any]] = []
    holder_rows: List[Dict[str, Any]] = []
    whale_agg: Dict[str, Dict[str, Any]] = {}

    with PolymarketClient() as pm:
        markets = pm.get_top_markets(limit=markets_limit)
        for m in markets:
            if not m.condition_id:
                continue
            try:
                holders = pm.get_market_holders(m.condition_id, limit=holders_per_market)
            except Exception as e:
                logger.warning("holders fetch failed for %s: %s", m.slug, e)
                holders = []

            whale_flow = sum(h.amount for h in holders)
            market_rows.append({
                "condition_id": m.condition_id,
                "question": m.question,
                "slug": m.slug,
                "category": m.category,
                "outcomes": m.outcomes,
                "outcome_prices": m.outcome_prices,
                "clob_token_ids": m.clob_token_ids,
                "volume_24h": m.volume_24h,
                "liquidity": m.liquidity,
                "whale_flow": whale_flow,
                "whale_count": len(holders),
                "one_day_price_change": m.one_day_price_change,
                "end_date": m.end_date,
                "image": m.image,
                "updated_at": _now(),
            })

            for h in holders:
                if not h.proxy_wallet:
                    continue
                holder_rows.append({
                    "condition_id": m.condition_id,
                    "proxy_wallet": h.proxy_wallet,
                    "name": h.name or h.pseudonym,
                    "amount": h.amount,
                    "outcome_index": h.outcome_index,
                    "updated_at": _now(),
                })
                rec = whale_agg.setdefault(h.proxy_wallet, {
                    "proxy_wallet": h.proxy_wallet,
                    "name": h.name or h.pseudonym,
                    "profile_image": h.profile_image,
                    "total_amount": 0.0,
                    "markets_count": 0,
                    "positions": [],
                    "updated_at": _now(),
                })
                rec["total_amount"] += h.amount
                rec["markets_count"] += 1

    return {
        "markets": market_rows,
        "holders": holder_rows,
        "whales": list(whale_agg.values()),
    }


def run(*, live: bool, markets_limit: int, holders_per_market: int, client=None) -> Dict[str, int]:
    payload = build_payload(markets_limit=markets_limit, holders_per_market=holders_per_market)
    summary = {
        "markets": len(payload["markets"]),
        "holders": len(payload["holders"]),
        "whales": len(payload["whales"]),
    }

    logger.info(
        "%s — markets: %d | market-holders: %d | unique whales: %d",
        "LIVE" if live else "DRY-RUN",
        summary["markets"], summary["holders"], summary["whales"],
    )
    if payload["markets"]:
        top = sorted(payload["markets"], key=lambda r: r["whale_flow"], reverse=True)[:5]
        for m in top:
            logger.info("  $%,.0f whale-flow  %s", m["whale_flow"], m["question"][:60])

    if not live:
        return summary

    if client is None:
        from supabase import create_client
        from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
        client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    if payload["markets"]:
        client.table("polymarket_markets").upsert(
            payload["markets"], on_conflict="condition_id"
        ).execute()
    if payload["whales"]:
        client.table("polymarket_whales").upsert(
            payload["whales"], on_conflict="proxy_wallet"
        ).execute()
    if payload["holders"]:
        client.table("polymarket_market_holders").upsert(
            payload["holders"], on_conflict="condition_id,proxy_wallet,outcome_index"
        ).execute()

    logger.info("LIVE write complete.")
    return summary


def main() -> int:
    p = argparse.ArgumentParser(description="Sync Polymarket markets + whales into Supabase.")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--live", action="store_true", help="Write to Supabase (default: dry-run).")
    g.add_argument("--dry-run", action="store_true", help="Print only (default).")
    p.add_argument("--markets", type=int, default=30, help="Top markets to sync (default 30).")
    p.add_argument("--holders", type=int, default=15, help="Top holders per market (default 15).")
    args = p.parse_args()

    live = args.live and not args.dry_run
    try:
        run(live=live, markets_limit=args.markets, holders_per_market=args.holders)
    except Exception as e:
        logger.error("Polymarket sync failed: %s", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
