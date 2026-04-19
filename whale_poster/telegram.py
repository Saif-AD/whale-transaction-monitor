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
_PHOTO_SEND_TIMEOUT = 15.0

# Telegram caption limit — anything longer is rejected by sendPhoto.
TELEGRAM_CAPTION_MAX = 1024


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


def _truncate_caption(caption: str, limit: int = TELEGRAM_CAPTION_MAX) -> str:
    """Truncate a caption to ``limit`` chars, appending ``...`` when cut."""
    if caption is None:
        return ""
    if len(caption) <= limit:
        return caption
    if limit <= 3:
        return caption[:limit]
    return caption[: limit - 3] + "..."


def send_photo(
    photo_path: str,
    caption: str,
    chat_id: Optional[str] = None,
    token: Optional[str] = None,
) -> bool:
    """Send a photo with caption to a Telegram chat.

    Falls back to ``TELEGRAM_BOT_TOKEN`` / ``TELEGRAM_CHANNEL_ID`` from
    :mod:`shared.config` when ``token`` / ``chat_id`` are not supplied.

    Returns ``True`` on HTTP 200, ``False`` otherwise.
    """
    if token is None or chat_id is None:
        try:
            from shared.config import (
                TELEGRAM_BOT_TOKEN,
                TELEGRAM_CHANNEL_ID,
            )
        except Exception as e:
            logger.warning("send_photo: cannot load telegram config: %s", e)
            return False
        token = token or TELEGRAM_BOT_TOKEN
        chat_id = chat_id or TELEGRAM_CHANNEL_ID

    if not token or not chat_id:
        logger.warning(
            "Telegram send_photo skipped: bot_token or chat_id not configured",
        )
        return False

    caption = _truncate_caption(caption or "")

    url = f"{TELEGRAM_API_BASE}/bot{token}/sendPhoto"
    data = {
        "chat_id": chat_id,
        "caption": caption,
        "parse_mode": "HTML",
    }

    try:
        with open(photo_path, "rb") as fh:
            files = {"photo": (photo_path, fh, "image/png")}
            resp = httpx.post(
                url, data=data, files=files, timeout=_PHOTO_SEND_TIMEOUT,
            )
    except FileNotFoundError:
        logger.error("Telegram send_photo failed: file not found: %s", photo_path)
        return False
    except httpx.HTTPError as e:
        logger.error("Telegram send_photo failed: %s", e)
        return False
    except Exception as e:
        logger.error("Telegram send_photo unexpected error: %s", e)
        return False

    if resp.status_code == 200:
        logger.info("Telegram photo sent to %s", chat_id)
        return True
    logger.warning(
        "Telegram sendPhoto error %d: %s", resp.status_code, resp.text[:200],
    )
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
