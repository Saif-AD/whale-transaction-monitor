#!/usr/bin/env python3
"""Whale alert auto-poster — polls all_whale_transactions and posts to Telegram.

Default is dry-run. Use --live to actually send Telegram messages.

Usage:
    python3 whale_poster/poster.py              # single poll, dry-run
    python3 whale_poster/poster.py --live        # single poll, posts to Telegram
    python3 whale_poster/poster.py --loop        # continuous polling (dry-run)
    python3 whale_poster/poster.py --loop --live # continuous polling, live
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from supabase import create_client

from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
from shared.config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHANNEL_ID,
    TELEGRAM_BOT_TOKEN_ADMIN,
    TELEGRAM_ADMIN_CHAT_ID,
    POSTER_USD_THRESHOLD,
    POSTER_POLL_INTERVAL_SECONDS,
    POSTER_DRY_RUN,
    POST_CEX_INTERNAL,
    MIN_SECONDS_BETWEEN_SAME_TOKEN_POSTS,
    POSTER_MIN_TX_TIMESTAMP,
    STABLECOIN_SYMBOLS,
)
from whale_poster import chart_generator
from whale_poster.formatter import format_for_telegram, is_cex_to_cex, is_narrative_reasoning
from whale_poster.dedup import is_posted, mark_posted, is_token_on_cooldown
from whale_poster.telegram import send_message, send_photo, send_admin_alert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

PAGE_SIZE = 200

UNKNOWN_ONLY_BYPASS_USD = 5_000_000
NO_REASONING_BYPASS_USD = 10_000_000


# ------------------------------------------------------------------
# Watermark (poster_state)
# ------------------------------------------------------------------

def _read_watermark(client) -> Optional[str]:
    """Read last_seen_timestamp from poster_state. Returns None if not set."""
    try:
        result = (
            client.table("poster_state")
            .select("value")
            .eq("key", "last_seen_timestamp")
            .limit(1)
            .execute()
        )
        rows = result.data or []
        if rows:
            return rows[0]["value"]
        return None
    except Exception as e:
        logger.error("Failed to read watermark: %s", e)
        return None


def _write_watermark(client, ts_iso: str) -> None:
    """Write last_seen_timestamp to poster_state."""
    try:
        client.table("poster_state").upsert(
            {
                "key": "last_seen_timestamp",
                "value": ts_iso,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            on_conflict="key",
        ).execute()
    except Exception as e:
        logger.error("Failed to write watermark: %s", e)


# ------------------------------------------------------------------
# Query candidates
# ------------------------------------------------------------------

def _fetch_candidates(client, watermark_iso: str) -> List[Dict[str, Any]]:
    """Fetch whale transactions newer than watermark - 2 min overlap."""
    all_rows: List[Dict[str, Any]] = []
    offset = 0

    while True:
        query = (
            client.table("all_whale_transactions")
            .select(
                "transaction_hash,token_symbol,usd_value,blockchain,"
                "from_address,to_address,from_label,to_label,"
                "reasoning,timestamp,is_cex_transaction"
            )
            .gte("timestamp", watermark_iso)
            .gte("usd_value", POSTER_USD_THRESHOLD)
            .order("timestamp", desc=False)
            .order("transaction_hash", desc=False)
            .range(offset, offset + PAGE_SIZE - 1)
        )

        if POSTER_MIN_TX_TIMESTAMP:
            query = query.gte("timestamp", POSTER_MIN_TX_TIMESTAMP)

        result = query.execute()
        rows = result.data or []
        all_rows.extend(rows)

        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return all_rows


# ------------------------------------------------------------------
# Single poll iteration
# ------------------------------------------------------------------

def run_once(
    *,
    live: bool = False,
    client=None,
) -> Dict[str, int]:
    """Execute a single poll iteration.

    Returns stats dict with keys: candidates, skipped_stablecoin,
    skipped_dedup, skipped_cex, skipped_cooldown, posted, errors.
    """
    sb = client or create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    stats = {
        "candidates": 0,
        "skipped_stablecoin": 0,
        "skipped_dedup": 0,
        "skipped_cex": 0,
        "unknown_only": 0,
        "self_transfer": 0,
        "no_reasoning": 0,
        "skipped_cooldown": 0,
        "posted": 0,
        "errors": 0,
        "first_run": False,
    }

    watermark = _read_watermark(sb)

    if watermark is None:
        now_iso = datetime.now(timezone.utc).isoformat()
        logger.info("First run — no watermark found. Setting to NOW() and exiting without posting.")
        _write_watermark(sb, now_iso)
        stats["first_run"] = True
        return stats

    rows = _fetch_candidates(sb, watermark)
    stats["candidates"] = len(rows)

    if not rows:
        logger.info("No new candidates since %s", watermark)
        return stats

    max_ts = watermark

    for tx in rows:
        tx_hash = tx.get("transaction_hash", "")
        token = (tx.get("token_symbol") or "").upper()
        ts = tx.get("timestamp", "")

        if ts > max_ts:
            max_ts = ts

        if token in STABLECOIN_SYMBOLS:
            stats["skipped_stablecoin"] += 1
            continue

        if is_posted(sb, tx_hash):
            stats["skipped_dedup"] += 1
            continue

        from_label = tx.get("from_label", "") or ""
        to_label = tx.get("to_label", "") or ""
        if not POST_CEX_INTERNAL and is_cex_to_cex(from_label, to_label):
            stats["skipped_cex"] += 1
            continue

        # Skip transactions where both parties are unknown (no label on either
        # side), since there's no narrative context to post. Bypass for very
        # large moves that are notable on size alone.
        if not from_label.strip() and not to_label.strip():
            try:
                usd_value = float(tx.get("usd_value") or 0)
            except (TypeError, ValueError):
                usd_value = 0.0
            if usd_value < UNKNOWN_ONLY_BYPASS_USD:
                stats["unknown_only"] += 1
                continue

        # Skip self-transfers where the same labeled entity appears on both
        # sides (e.g. "Top WBTC holder → Top WBTC holder", "Binance Hot Wallet
        # → Binance Hot Wallet"). Internal shuffling carries no narrative.
        from_label_norm = from_label.strip().lower()
        to_label_norm = to_label.strip().lower()
        if from_label_norm and to_label_norm and from_label_norm == to_label_norm:
            stats["self_transfer"] += 1
            continue

        # Skip posts whose reasoning is empty or a placeholder — we don't want
        # to publish without narrative context. Very large moves (>= $10M) are
        # news on size alone and bypass this filter.
        if not is_narrative_reasoning(tx.get("reasoning")):
            try:
                usd_value = float(tx.get("usd_value") or 0)
            except (TypeError, ValueError):
                usd_value = 0.0
            if usd_value < NO_REASONING_BYPASS_USD:
                stats["no_reasoning"] += 1
                continue

        if is_token_on_cooldown(sb, token, MIN_SECONDS_BETWEEN_SAME_TOKEN_POSTS):
            stats["skipped_cooldown"] += 1
            continue

        msg = format_for_telegram(tx, client=sb)

        if live:
            chart_path = None
            try:
                chart_path = chart_generator.generate_whale_chart(tx)
            except Exception as e:
                logger.warning("Chart generation failed: %s", e)

            if chart_path:
                ok = send_photo(
                    chart_path, msg,
                    chat_id=TELEGRAM_CHANNEL_ID, token=TELEGRAM_BOT_TOKEN,
                )
                try:
                    os.remove(chart_path)
                except OSError:
                    pass
            else:
                ok = send_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID, msg)

            if not ok:
                stats["errors"] += 1
                continue
            mark_posted(sb, tx_hash, token, "telegram")
        else:
            logger.info("DRY-RUN post (not written to posted_tx_hashes):\n%s", msg)

        stats["posted"] += 1

    _write_watermark(sb, max_ts)
    logger.info(
        "Poll done: %d candidates, %d posted, %d stablecoin, %d dedup, "
        "%d cex, %d unknown_only, %d self_transfer, %d no_reasoning, "
        "%d cooldown, %d errors",
        stats["candidates"], stats["posted"], stats["skipped_stablecoin"],
        stats["skipped_dedup"], stats["skipped_cex"], stats["unknown_only"],
        stats["self_transfer"], stats["no_reasoning"],
        stats["skipped_cooldown"], stats["errors"],
    )
    return stats


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Whale alert auto-poster — Telegram.",
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Actually send Telegram messages (default: dry-run)",
    )
    parser.add_argument(
        "--loop", action="store_true",
        help="Run continuously, polling every POSTER_POLL_INTERVAL_SECONDS",
    )
    args = parser.parse_args()

    live = args.live and not POSTER_DRY_RUN
    mode = "LIVE" if live else "DRY-RUN"
    logger.info("Whale poster starting (%s)", mode)

    if args.loop:
        _run_loop(live=live)
        return 0
    else:
        stats = run_once(live=live)
        return 0 if stats["errors"] == 0 else 1


def _run_loop(*, live: bool) -> None:
    """Continuous polling loop with crash recovery."""
    while True:
        try:
            run_once(live=live)
        except KeyboardInterrupt:
            logger.info("Shutting down (KeyboardInterrupt)")
            break
        except Exception:
            tb = traceback.format_exc()
            logger.error("Poster crash:\n%s", tb)
            if TELEGRAM_BOT_TOKEN_ADMIN and TELEGRAM_ADMIN_CHAT_ID:
                send_admin_alert(
                    TELEGRAM_BOT_TOKEN_ADMIN,
                    TELEGRAM_ADMIN_CHAT_ID,
                    f"Whale poster crash:\n{tb[:3000]}",
                )

        time.sleep(POSTER_POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    sys.exit(main())
