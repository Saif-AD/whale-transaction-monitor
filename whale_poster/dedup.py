"""Deduplication and per-token cooldown for whale alert posting.

Backed by the Supabase `posted_tx_hashes` table.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def is_posted(client, tx_hash: str) -> bool:
    """Check if a transaction has already been posted."""
    try:
        result = (
            client.table("posted_tx_hashes")
            .select("tx_hash")
            .eq("tx_hash", tx_hash)
            .limit(1)
            .execute()
        )
        return bool(result.data)
    except Exception as e:
        logger.error("Dedup check failed for %s: %s", tx_hash[:16], e)
        return False


def mark_posted(
    client,
    tx_hash: str,
    token_symbol: str = "",
    channel: str = "telegram",
) -> bool:
    """Record a transaction as posted."""
    try:
        client.table("posted_tx_hashes").upsert(
            {
                "tx_hash": tx_hash,
                "token_symbol": token_symbol,
                "channel": channel,
                "posted_at": datetime.now(timezone.utc).isoformat(),
            },
            on_conflict="tx_hash",
        ).execute()
        return True
    except Exception as e:
        logger.error("Mark posted failed for %s: %s", tx_hash[:16], e)
        return False


def is_token_on_cooldown(
    client,
    token_symbol: str,
    cooldown_seconds: int,
) -> bool:
    """Check if a token was posted within the cooldown window.

    Uses ORDER BY posted_at DESC LIMIT 1 (not MAX) per plan spec.
    """
    try:
        result = (
            client.table("posted_tx_hashes")
            .select("posted_at")
            .eq("token_symbol", token_symbol)
            .order("posted_at", desc=True)
            .limit(1)
            .execute()
        )
        rows = result.data or []
        if not rows:
            return False

        last_posted_str = rows[0]["posted_at"]
        last_posted = datetime.fromisoformat(last_posted_str.replace("Z", "+00:00"))
        elapsed = (datetime.now(timezone.utc) - last_posted).total_seconds()
        return elapsed < cooldown_seconds
    except Exception as e:
        logger.error("Cooldown check failed for %s: %s", token_symbol, e)
        return False
