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


def _fetch_markets_multi(pm, markets_limit: int) -> List[Any]:
    """Pull markets across several orderings and dedupe by condition_id, so
    coverage isn't limited to a single 'top by 24h volume' slice (a quiet
    market with a huge whale still surfaces via the liquidity ordering)."""
    seen = set()
    out: List[Any] = []
    for order in ("volume24hr", "liquidity", "volume"):
        try:
            for m in pm.get_top_markets(limit=markets_limit, order=order):
                if m.condition_id and m.condition_id not in seen:
                    seen.add(m.condition_id)
                    out.append(m)
        except Exception as e:
            logger.warning("markets fetch failed (order=%s): %s", order, e)
    return out


def _max_price(prices: List[float]) -> float:
    return max(prices) if prices else 0.0


def build_payload(
    *,
    event_pages: int,
    events_per_page: int,
    max_markets: int,
    holders_markets: int,
    holders_per_market: int,
    enrich_whales: int = 0,
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch everything from Polymarket and shape it into table rows.

    Markets come from the /events endpoint (real category tags + pagination).
    We store up to `max_markets` rows, but only fetch holders for the top
    `holders_markets` *contested* markets by volume (skipping near-resolved
    longshots) so the whale data lands where the action actually is.
    """
    market_rows: List[Dict[str, Any]] = []
    holder_rows: List[Dict[str, Any]] = []
    whale_agg: Dict[str, Dict[str, Any]] = {}

    with PolymarketClient() as pm:
        markets = pm.get_markets_via_events(pages=event_pages, per_page=events_per_page)
        if not markets:
            logger.warning("events pull empty — falling back to markets endpoint")
            markets = _fetch_markets_multi(pm, max_markets)

        # Drop near-resolved longshots (e.g. the ~48 "Will <team> win the World
        # Cup" sub-markets sitting at ~0% YES). They have no whale action and
        # would otherwise be stored as orphan rows that show "No holdings found".
        markets = [m for m in markets if _max_price(m.outcome_prices) < 0.97]
        markets.sort(key=lambda m: m.volume_24h, reverse=True)
        markets = markets[:max_markets]

        # Fetch holders for EVERY stored market (top `holders_markets` by volume,
        # which defaults to all of them) so we never store a market without a
        # holder attempt. This is what keeps "No holdings found" rare — the only
        # ones left are markets Polymarket itself reports zero holders for.
        holder_targets = {m.condition_id for m in markets[:holders_markets]}

        for m in markets:
            holders = []
            if m.condition_id in holder_targets:
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
                "tags": m.tags,
                "competitive": m.competitive,
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

        # Depth pass: for the biggest whales, pull their portfolio value +
        # open positions so the leaderboard/drawer shows real holdings & PnL.
        if enrich_whales > 0:
            top = sorted(
                whale_agg.values(), key=lambda r: r["total_amount"], reverse=True
            )[:enrich_whales]
            for rec in top:
                wallet = rec["proxy_wallet"]
                try:
                    rec["total_value_usd"] = pm.get_user_value(wallet)
                except Exception as e:
                    logger.warning("value fetch failed for %s: %s", wallet[:10], e)
                try:
                    rec["positions"] = [
                        p.to_dict() for p in pm.get_user_positions(wallet, limit=25)
                    ]
                except Exception as e:
                    logger.warning("positions fetch failed for %s: %s", wallet[:10], e)

    return {
        "markets": market_rows,
        "holders": holder_rows,
        "whales": list(whale_agg.values()),
    }


def run(
    *, live: bool, event_pages: int, events_per_page: int, max_markets: int,
    holders_markets: int, holders_per_market: int, enrich_whales: int = 0, client=None,
) -> Dict[str, int]:
    # Captured BEFORE the pull so every row we're about to upsert gets a
    # newer updated_at; anything older than this is stale (resolved markets,
    # dropped longshots) and gets pruned after the write.
    prune_cutoff = _now()
    payload = build_payload(
        event_pages=event_pages,
        events_per_page=events_per_page,
        max_markets=max_markets,
        holders_markets=holders_markets,
        holders_per_market=holders_per_market,
        enrich_whales=enrich_whales,
    )
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

    # Prune stale rows the upserts left behind. Upsert never deletes, so
    # resolved markets, dropped longshots, and whales that fell out of the
    # top markets would accumulate forever and show as "—"/spam in the app.
    # Guard: only prune after a healthy pull so a thin/failed run can't wipe
    # the tables.
    if len(payload["markets"]) >= 50:
        for table in ("polymarket_market_holders", "polymarket_whales", "polymarket_markets"):
            try:
                client.table(table).delete().lt("updated_at", prune_cutoff).execute()
            except Exception as e:
                logger.warning("prune of %s failed: %s", table, e)
        logger.info("Pruned rows older than %s", prune_cutoff)
    else:
        logger.warning("Skipping prune — only %d markets this run", len(payload["markets"]))

    logger.info("LIVE write complete.")
    return summary


def main() -> int:
    p = argparse.ArgumentParser(description="Sync Polymarket markets + whales into Supabase.")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--live", action="store_true", help="Write to Supabase (default: dry-run).")
    g.add_argument("--dry-run", action="store_true", help="Print only (default).")
    p.add_argument("--event-pages", type=int, default=4, help="Pages of /events to pull (default 4).")
    p.add_argument("--events-per-page", type=int, default=40, help="Events per page (default 40).")
    p.add_argument("--max-markets", type=int, default=300, help="Max market rows to store (default 300).")
    p.add_argument("--holders-markets", type=int, default=300, help="Fetch holders for the top N stored markets (default 300 == all). Lower it only to rate-limit.")
    p.add_argument("--holders", type=int, default=50, help="Top holders per market (default 50).")
    p.add_argument("--enrich-whales", type=int, default=50, help="Pull value+positions for the top N whales (default 50; 0 disables).")
    args = p.parse_args()

    live = args.live and not args.dry_run
    try:
        run(
            live=live,
            event_pages=args.event_pages,
            events_per_page=args.events_per_page,
            max_markets=args.max_markets,
            holders_markets=args.holders_markets,
            holders_per_market=args.holders,
            enrich_whales=args.enrich_whales,
        )
    except Exception as e:
        logger.error("Polymarket sync failed: %s", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
