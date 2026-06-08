#!/usr/bin/env python3
"""Arkham-powered Polymarket enrichment cron — designed for Railway (hourly).

Hybrid layer on top of scripts/sync_polymarket.py (public Gamma/Data APIs):
the public cron writes market breadth + odds + volume into polymarket_markets;
this cron uses Arkham's Polymarket endpoints to add the parts the public API
can't:

  1. polymarket_leaderboard  — PnL-ranked traders (Arkham /polymarket/leaderboard)
  2. polymarket_activity     — live whale trade tape (Arkham /polymarket/activity)
  3. arkham_entity (real names) on polymarket_whales — fixes "Whales = —"

NOTE: Arkham's Polymarket endpoints return raw wallet addresses, not entity
names. Names are resolved best-effort via /intelligence/address (1 credit each)
for the highest-signal wallets (top whales + leaderboard + activity); most
proxies map to a generic "Polymarket Deposit Wallet" label and are skipped, so
only genuinely-attributed wallets get a name.

Schema: migrations/polymarket_arkham.sql (apply first).

Usage:
    python scripts/sync_arkham_polymarket.py --dry-run
    python scripts/sync_arkham_polymarket.py --live
    python scripts/sync_arkham_polymarket.py --live --activity-min-usd 10000 --resolve-names 150
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


def _wallet(row: Dict[str, Any]) -> Optional[str]:
    w = _pick(row, "userAddress", "address", "proxyWallet", "proxy_wallet", "wallet", "traderAddress")
    return w.lower() if isinstance(w, str) and w.startswith("0x") else w


# Generic Arkham labels that are not useful as a "whale name" (proxies /
# deposit contracts most Polymarket traders resolve to).
_GENERIC_LABEL_TOKENS = ("polymarket", "deposit", "proxy", "unknown")


def _resolve_entity_name(intel: Dict[str, Any]) -> Optional[str]:
    """Pull a meaningful entity/label name from an /intelligence/address row.

    Prefer a real Arkham entity; fall back to a non-generic label. Skip the
    generic 'Polymarket Deposit Wallet'-style labels almost every proxy maps
    to, since those add no signal.
    """
    ent = intel.get("arkhamEntity")
    if isinstance(ent, dict) and ent.get("name"):
        return ent["name"]
    label = intel.get("arkhamLabel")
    if isinstance(label, dict) and label.get("name"):
        nm = label["name"]
        low = nm.lower()
        if not any(tok in low for tok in _GENERIC_LABEL_TOKENS):
            return nm
    return None


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
    leaderboard_limit: int,
    activity_limit: int,
    activity_min_usd: int,
    period: str,
    resolve_names: int,
) -> Dict[str, List[Dict[str, Any]]]:
    # 1. Leaderboard (PnL-ranked traders) → owned table.
    leaderboard_rows: List[Dict[str, Any]] = []
    try:
        lb = ark.get_polymarket_leaderboard(period=period, limit=leaderboard_limit)
        for i, row in enumerate(lb):
            w = _wallet(row)
            if not w:
                continue
            leaderboard_rows.append({
                "proxy_wallet": w,
                "period": period,
                "entity_name": None,  # filled by name resolution below
                "name": None,
                "profile_image": "",
                "pnl": _to_float(_pick(row, "periodPnl", "pnl", "profit")),
                "volume": _to_float(_pick(row, "volume", "usdVolume", "vol")),
                "rank": int(_pick(row, "rank", default=i + 1) or i + 1),
                "updated_at": _now(),
            })
    except CreditBudgetExhausted:
        raise
    except Exception as e:
        logger.warning("leaderboard fetch failed: %s", e)

    # 2. Activity tape (whale trades over a USD floor) → owned table.
    activity_rows: List[Dict[str, Any]] = []
    try:
        activity = ark.get_polymarket_activity(
            min_usd=activity_min_usd, limit=activity_limit, sort_by="time", sort_order="desc",
        )
        for a in activity:
            w = _wallet(a)
            if not w:
                continue
            ts = _to_iso(_pick(a, "blockTimestamp", "timestamp", "time", "ts"))
            side = _pick(a, "direction", "side", "action")
            cid = _pick(a, "conditionId", "conditionID", "condition_id")
            if not ts or not cid or not side:
                continue
            size = _to_float(_pick(a, "size", "shares"))
            usd = _to_float(_pick(a, "notional", "usd", "usdValue", "usd_value"))
            activity_rows.append({
                "tx_hash": _pick(a, "transactionHash", "txHash", "tx_hash"),
                "condition_id": cid,
                "proxy_wallet": w,
                "entity_name": None,  # filled by name resolution below
                "name": None,
                "side": str(side).lower(),
                "outcome": _pick(a, "outcome"),
                "outcome_index": _pick(a, "outcomeIndex", "outcome_index"),
                "usd_value": usd,
                "price": (usd / size) if size else None,
                "size": size or None,
                "ts": ts,
                "updated_at": _now(),
            })
    except CreditBudgetExhausted:
        raise
    except Exception as e:
        logger.warning("activity fetch failed: %s", e)

    # 3. Best-effort name resolution. Arkham's Polymarket endpoints return raw
    #    wallets; resolve the highest-signal ones (top leaderboard + activity +
    #    existing whales) to a real entity via /intelligence/address. Most
    #    proxies map to a generic "Polymarket Deposit Wallet" label (skipped),
    #    so only genuinely-attributed wallets get a name.
    resolved_names: Dict[str, str] = {}
    if resolve_names > 0:
        candidates: List[str] = []
        seen: set = set()
        # Existing top whales first (fixes "Whales = —" on the leaderboard panel).
        try:
            wres = (
                sb.table("polymarket_whales")
                .select("proxy_wallet")
                .order("total_amount", desc=True)
                .limit(resolve_names)
                .execute()
            )
            for r in (wres.data or []):
                w = (r.get("proxy_wallet") or "").lower()
                if w and w not in seen:
                    seen.add(w)
                    candidates.append(w)
        except Exception as e:
            logger.warning("could not read whales for name resolution: %s", e)
        for row in leaderboard_rows + activity_rows:
            w = row["proxy_wallet"]
            if w and w not in seen:
                seen.add(w)
                candidates.append(w)

        for w in candidates[:resolve_names]:
            try:
                intel = ark.get_address_intelligence(w)
            except CreditBudgetExhausted:
                raise
            except Exception:
                continue
            name = _resolve_entity_name(intel)
            if name:
                resolved_names[w] = name

        # Stamp resolved names onto the owned rows.
        for row in leaderboard_rows + activity_rows:
            nm = resolved_names.get(row["proxy_wallet"])
            if nm:
                row["entity_name"] = nm

    return {
        "resolved_names": resolved_names,
        "leaderboard": leaderboard_rows,
        "activity": activity_rows,
        "markets_scanned": [],
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


def run(*, live: bool, leaderboard_limit: int, activity_limit: int,
        activity_min_usd: int, period: str, resolve_names: int) -> Dict[str, Any]:
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
                leaderboard_limit=leaderboard_limit,
                activity_limit=activity_limit,
                activity_min_usd=activity_min_usd,
                period=period,
                resolve_names=resolve_names,
            )
        except CreditBudgetExhausted as e:
            logger.error("ABORTING: %s", e)
            return {"aborted_reason": str(e)}
        credits_left = ark.last_credits_remaining

    logger.info(
        "%s — resolved_names=%d leaderboard=%d activity=%d credits_remaining=%s",
        "LIVE" if live else "DRY-RUN",
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
    p.add_argument("--leaderboard-limit", type=int, default=200, help="Leaderboard entries (default 200, max 200).")
    p.add_argument("--activity-limit", type=int, default=500, help="Activity rows (default 500, max 500).")
    p.add_argument("--activity-min-usd", type=int, default=10000, help="Min USD notional for the activity tape (default 10000).")
    p.add_argument("--period", type=str, default="1d", choices=["1d", "1w", "1m", "all"], help="Leaderboard period (default 1d).")
    p.add_argument("--resolve-names", type=int, default=150, help="Max wallets to resolve to entity names via /intelligence/address (default 150; 0 disables).")
    args = p.parse_args()

    live = args.live and not args.dry_run
    try:
        result = run(
            live=live,
            leaderboard_limit=args.leaderboard_limit,
            activity_limit=args.activity_limit,
            activity_min_usd=args.activity_min_usd,
            period=args.period,
            resolve_names=args.resolve_names,
        )
    except Exception as e:
        logger.error("Arkham Polymarket sync failed: %s", e)
        return 1
    if result.get("error") or result.get("aborted_reason"):
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
