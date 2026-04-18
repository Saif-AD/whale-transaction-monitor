"""Telegram Bot API client for whale alert posting.

Raw httpx.post — no python-telegram-bot dependency.
"""

from __future__ import annotations

import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org"
_SEND_TIMEOUT = 15.0


def send_message(
    bot_token: str,
    chat_id: str,
    text: str,
    *,
    parse_mode: str = "HTML",
    disable_web_page_preview: bool = True,
) -> bool:
    """Post a message to a Telegram chat. Returns True on success."""
    if not bot_token or not chat_id:
        logger.warning("Telegram send skipped: bot_token or chat_id not configured")
        return False

    url = f"{TELEGRAM_API_BASE}/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview,
    }

    try:
        resp = httpx.post(url, json=payload, timeout=_SEND_TIMEOUT)
        if resp.status_code == 200:
            logger.info("Telegram message sent to %s", chat_id)
            return True
        logger.warning(
            "Telegram API error %d: %s", resp.status_code, resp.text[:200],
        )
        return False
    except httpx.HTTPError as e:
        logger.error("Telegram send failed: %s", e)
        return False


def send_admin_alert(
    admin_token: str,
    admin_chat_id: str,
    text: str,
) -> bool:
    """Post an alert to the admin Telegram chat (plain text, no HTML)."""
    return send_message(
        admin_token,
        admin_chat_id,
        text,
        parse_mode="",
        disable_web_page_preview=True,
    )
