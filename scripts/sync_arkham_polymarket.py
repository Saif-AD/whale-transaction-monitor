#!/usr/bin/env python3
"""Arkham-powered Polymarket enrichment cron — designed for Railway (hourly).

Hybrid layer on top of scripts/sync_polymarket.py (public Gamma/Data APIs):
the public cron writes market breadth + odds + volume into polymarket_markets;
this cron uses Arkham's entity-resolved Polymarket endpoints to add the parts
the public API can't:

  1. arkham_entity (real names) on polymarket_whales  — fixes "Whales = —"
  2. polymarket_leaderboard  — PnL-ranked traders (Arkham /polymarket/leaderboard)
  3. polymarket_activity     — live whale trade tape (Arkham /polymarket/activity)

Top-holders are fetched for the top N markets by volume (already stored by the
public cron) to harvest entity-resolved names.

Schema: migrations/polymarket_arkham.sql (apply first).

Usage:
    python scripts/sync_arkham_polymarket.py --dry-run
    python scripts/sync_arkham_polymarket.py --live
    python scripts/sync_arkham_polymarket.py --live --top-markets 100 --activity-min-usd 10000
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from arkham_backfill.arkham_client import ArkhamClient, CreditBudgetExhausted  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pick(d: Dict[str, Any], *keys: str, default=None):
    """Return the first present, non-None value among keys."""
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _entity_name(row: Dict[str, Any]) -> Optional[str]:
    """Extract an Arkham-resolved entity name from a holder/trader row."""
    for key in ("arkhamEntity", "entity"):
        ent = row.get(key)
        if isinstance(ent, dict):
            name = ent.get("name")
            if name:
                return name
    return _pick(row, "entityName", "entity_name")


def _wallet(row: Dict[str, Any]) -> Optional[str]:
    w = _pick(row, "address", "proxyWallet", "proxy_wallet", "wallet", "traderAddress")
    return w.lower() if isinstance(w, str) and w.startswith("0x") else w


def _to_iso(v: Any) -> Optional[str]:
    """Normalize epoch seconds/millis or ISO string into an ISO timestamp."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        ts = float(v)
        if ts > 1e12:  # milliseconds
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except (ValueError, OverflowError, OSError):
            return None
    if isinstance(v, str):
        return v
    return None


# ---------------------------------------------------------------------------
# Build payloads from Arkham
# ---------------------------------------------------------------------------

def build_payload(
    ark: ArkhamClient,
    sb,
    *,
    top_markets: int,
    holders_per_market: int,
    leaderboard_limit: int,
    activity_limit: int,
    activity_min_usd: int,
    period: str,
) -> Dict[str, List[Dict[str, Any]]]:
    resolved_names: Dict[str, str] = {}

    # 1. Top markets (already stored by the public cron) → harvest named holders.
    market_cids: List[str] = []
    try:
        res = (
            sb.table("polymarket_markets")
            .select("condition_id")
            .order("volume_24h", desc=True)
            .limit(top_markets)
            .execute()
        )
        market_cids = [r["condition_id"] for r in (res.data or []) if r.get("condition_id")]
    except Exception as e:
        logger.warning("could not read top markets from Supabase: %s", e)

    for cid in market_cids:
        try:
            holders = ark.get_polymarket_top_holders(cid, limit=holders_per_market)
        except CreditBudgetExhausted:
            raise
        except Exception as e:
            logger.warning("top-holders failed for %s: %s", cid[:12], e)
            continue
        for h in holders:
            w = _wallet(h)
            name = _entity_name(h)
            if w and name:
                resolved_names.setdefault(w, name)

    # 2. Leaderboard (PnL-ranked) → owned table + name harvest.
    leaderboard_rows: List[Dict[str, Any]] = []
    try:
        lb = ark.get_polymarket_leaderboard(period=period, limit=leaderboard_limit)
        for i, row in enumerate(lb):
            w = _wallet(row)
            if not w:
                continue
            name = _entity_name(row)
            if name:
                resolved_names.setdefault(w, name)
            leaderboard_rows.append({
                "proxy_wallet": w,
                "period": period,
                "entity_name": name,
                "name": _pick(row, "name", "pseudonym"),
                "profile_image": _pick(row, "profileImage", "profile_image", default=""),
                "pnl": _to_float(_pick(row, "pnl", "profit", "realizedPnl")),
                "volume": _to_float(_pick(row, "volume", "usdVolume", "vol")),
                "rank": int(_pick(row, "rank", default=i + 1) or i + 1),
                "updated_at": _now(),
            })
    except CreditBudgetExhausted:
        raise
    except Exception as e:
        logger.warning("leaderboard fetch failed: %s", e)

    # 3. Activity tape (whale trades over a USD floor) → owned table + names.
    activity_rows: List[Dict[str, Any]] = []
    try:
        activity = ark.get_polymarket_activity(
            min_usd=activity_min_usd, limit=activity_limit, sort_by="time", sort_order="desc",
        )
        for a in activity:
            w = _wallet(a)
            if not w:
                continue
            name = _entity_name(a)
            if name:
                resolved_names.setdefault(w, name)
            ts = _to_iso(_pick(a, "timestamp", "time", "ts"))
            side = _pick(a, "side", "direction", "action")
            cid = _pick(a, "conditionID", "conditionId", "condition_id")
            if not ts or not cid or not side:
                continue
            activity_rows.append({
                "tx_hash": _pick(a, "transactionHash", "txHash", "tx_hash"),
                "condition_id": cid,
                "proxy_wallet": w,
                "entity_name": name,
                "name": _pick(a, "name", "pseudonym"),
                "side": str(side).lower(),
                "outcome": _pick(a, "outcome"),
                "outcome_index": _pick(a, "outcomeIndex", "outcome_index"),
                "usd_value": _to_float(_pick(a, "usd", "usdValue", "usd_value")),
                "price": _to_float(_pick(a, "price")) or None,
                "size": _to_float(_pick(a, "size", "shares")) or None,
                "ts": ts,
                "updated_at": _now(),
            })
    except CreditBudgetExhausted:
        raise
    except Exception as e:
        logger.warning("activity fetch failed: %s", e)

    return {
        "resolved_names": resolved_names,
        "leaderboard": leaderboard_rows,
        "activity": activity_rows,
        "markets_scanned": market_cids,
    }


# ---------------------------------------------------------------------------
# Write to Supabase
# ---------------------------------------------------------------------------

def _chunked_upsert(client, table: str, rows: list, on_conflict: str, size: int = 500):
    for i in range(0, len(rows), size):
        client.table(table).upsert(rows[i:i + size], on_conflict=on_conflict).execute()


def _dedupe(rows: List[Dict[str, Any]], keys: tuple) -> List[Dict[str, Any]]:
    """Keep the last row per composite key (avoids ON CONFLICT dupes in a batch)."""
    out: Dict[tuple, Dict[str, Any]] = {}
    for r in rows:
        out[tuple(r.get(k) for k in keys)] = r
    return list(out.values())


def write_payload(client, payload: Dict[str, Any]) -> Dict[str, int]:
    resolved = payload["resolved_names"]
    written = {"names_updated": 0, "leaderboard": 0, "activity": 0}

    # Enrich polymarket_whales.arkham_entity, but ONLY for wallets that already
    # exist (upsert with a partial payload updates just arkham_entity on
    # conflict; we filter to existing wallets so we never insert junk rows that
    # would wipe total_amount/markets_count).
    if resolved:
        existing: set = set()
        wallets = list(resolved.keys())
        for i in range(0, len(wallets), 1000):
            chunk = wallets[i:i + 1000]
            try:
                res = (
                    client.table("polymarket_whales")
                    .select("proxy_wallet")
                    .in_("proxy_wallet", chunk)
                    .execute()
                )
                existing.update(r["proxy_wallet"] for r in (res.data or []))
            except Exception as e:
                logger.warning("whale existence check failed: %s", e)
        updates = [
            {"proxy_wallet": w, "arkham_entity": resolved[w]}
            for w in existing
        ]
        if updates:
            _chunked_upsert(client, "polymarket_whales", updates, "proxy_wallet")
            written["names_updated"] = len(updates)

    if payload["leaderboard"]:
        rows = _dedupe(payload["leaderboard"], ("proxy_wallet", "period"))
        _chunked_upsert(client, "polymarket_leaderboard", rows, "proxy_wallet,period")
        written["leaderboard"] = len(rows)

    if payload["activity"]:
        rows = _dedupe(payload["activity"], ("proxy_wallet", "condition_id", "ts", "side"))
        _chunked_upsert(client, "polymarket_activity", rows, "proxy_wallet,condition_id,ts,side")
        written["activity"] = len(rows)

    return written


def run(*, live: bool, top_markets: int, holders_per_market: int, leaderboard_limit: int,
        activity_limit: int, activity_min_usd: int, period: str) -> Dict[str, Any]:
    api_key = os.getenv("ARKHAM_API_KEY", "").strip()
    if not api_key:
        logger.error("ARKHAM_API_KEY is not set.")
        return {"error": "no_api_key"}

    from supabase import create_client
    from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    with ArkhamClient(api_key=api_key) as ark:
        try:
            payload = build_payload(
                ark, sb,
                top_markets=top_markets,
                holders_per_market=holders_per_market,
                leaderboard_limit=leaderboard_limit,
                activity_limit=activity_limit,
                activity_min_usd=activity_min_usd,
                period=period,
            )
        except CreditBudgetExhausted as e:
            logger.error("ABORTING: %s", e)
            return {"aborted_reason": str(e)}
        credits_left = ark.last_credits_remaining

    logger.info(
        "%s — markets_scanned=%d resolved_names=%d leaderboard=%d activity=%d credits_remaining=%s",
        "LIVE" if live else "DRY-RUN",
        len(payload["markets_scanned"]),
        len(payload["resolved_names"]),
        len(payload["leaderboard"]),
        len(payload["activity"]),
        credits_left if credits_left is not None else "unknown",
    )

    if not live:
        sample = list(payload["resolved_names"].items())[:5]
        for w, n in sample:
            logger.info("  would name %s → %s", w[:12], n)
        return {"dry_run": True, **{k: len(v) for k, v in payload.items() if isinstance(v, (list, dict))}}

    written = write_payload(sb, payload)
    logger.info(
        "LIVE write complete — names_updated=%d leaderboard=%d activity=%d",
        written["names_updated"], written["leaderboard"], written["activity"],
    )
    return written


def main() -> int:
    p = argparse.ArgumentParser(description="Arkham Polymarket enrichment cron.")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--live", action="store_true", help="Write to Supabase.")
    g.add_argument("--dry-run", action="store_true", help="No writes (default).")
    p.add_argument("--top-markets", type=int, default=100, help="Top N markets by volume to harvest holders from (default 100).")
    p.add_argument("--holders-per-market", type=int, default=50, help="Holders to pull per market (default 50, max 200).")
    p.add_argument("--leaderboard-limit", type=int, default=200, help="Leaderboard entries (default 200, max 200).")
    p.add_argument("--activity-limit", type=int, default=500, help="Activity rows (default 500, max 500).")
    p.add_argument("--activity-min-usd", type=int, default=10000, help="Min USD notional for the activity tape (default 10000).")
    p.add_argument("--period", type=str, default="1d", choices=["1d", "1w", "1m", "all"], help="Leaderboard period (default 1d).")
    args = p.parse_args()

    live = args.live and not args.dry_run
    try:
        result = run(
            live=live,
            top_markets=args.top_markets,
            holders_per_market=args.holders_per_market,
            leaderboard_limit=args.leaderboard_limit,
            activity_limit=args.activity_limit,
            activity_min_usd=args.activity_min_usd,
            period=args.period,
        )
    except Exception as e:
        logger.error("Arkham Polymarket sync failed: %s", e)
        return 1
    if result.get("error") or result.get("aborted_reason"):
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
