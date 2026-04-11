"""Grok-powered whale transaction interpreter.

Generates audience-facing one-line interpretations of whale moves
in ORCA's voice. Used at ingest time to populate the `reasoning`
field on per-chain transaction tables.

Requires:
    pip install openai  (xAI Grok uses the OpenAI-compatible SDK)
    XAI_API_KEY env var set
"""

import logging
import time
from pathlib import Path
from typing import Optional, Tuple

from openai import OpenAI, APITimeoutError, APIConnectionError, APIStatusError

from shared.config import (
    XAI_API_KEY,
    INTERPRETER_MODEL,
    INTERPRETER_TIMEOUT_SECONDS,
)

logger = logging.getLogger(__name__)

_PROMPT_PATH = Path(__file__).resolve().parent.parent / "prompts" / "whale_interpretation.txt"

_system_prompt: Optional[str] = None


def _load_system_prompt() -> str:
    """Load and cache the system prompt from disk. Fails fast on missing file."""
    global _system_prompt
    if _system_prompt is not None:
        return _system_prompt
    if not _PROMPT_PATH.exists():
        raise FileNotFoundError(
            f"Interpreter prompt not found at {_PROMPT_PATH}. "
            "Create prompts/whale_interpretation.txt before enabling the interpreter."
        )
    _system_prompt = _PROMPT_PATH.read_text(encoding="utf-8").strip()
    logger.info("Loaded interpreter system prompt (%d chars)", len(_system_prompt))
    return _system_prompt


def _build_user_message(tx: dict) -> str:
    """Build the per-transaction user message sent to Grok."""
    parts = [
        f"Token: {tx.get('token_symbol', 'UNKNOWN')}",
        f"Action: {tx.get('classification', 'TRANSFER')}",
        f"USD value: ${tx.get('usd_value', 0):,.0f}",
        f"Chain: {tx.get('blockchain', 'unknown')}",
    ]

    from_label = tx.get("from_label", "")
    to_label = tx.get("to_label", "")
    if from_label:
        parts.append(f"From: {from_label}")
    else:
        parts.append("From: unknown wallet")
    if to_label:
        parts.append(f"To: {to_label}")
    else:
        parts.append("To: unknown wallet")

    ctype = tx.get("counterparty_type", "")
    if ctype:
        parts.append(f"Counterparty type: {ctype}")
    if tx.get("is_cex_transaction"):
        parts.append("CEX transaction: yes")

    return "\n".join(parts)


def _get_client() -> OpenAI:
    """Build an xAI-flavored OpenAI client. Raises if key is missing."""
    if not XAI_API_KEY:
        raise RuntimeError(
            "XAI_API_KEY is not set. Either set it or disable the interpreter "
            "with INTERPRETER_ENABLED=false."
        )
    return OpenAI(
        api_key=XAI_API_KEY,
        base_url="https://api.x.ai/v1",
        timeout=INTERPRETER_TIMEOUT_SECONDS,
    )


_TRANSIENT_ERRORS = (APITimeoutError, APIConnectionError)

_MAX_OUTPUT_CHARS = 200


def generate_interpretation(tx: dict) -> str:
    """Generate an audience-facing one-liner for a whale transaction.

    Returns the interpretation string (max 200 chars, no newlines).
    Raises on failure — the caller is responsible for falling back to
    template-based reasoning.

    Retries once on transient errors (network, timeout, 5xx).
    Does NOT retry on 4xx (bad request, auth, quota).
    """
    system_prompt = _load_system_prompt()
    user_msg = _build_user_message(tx)
    client = _get_client()
    tx_hash = tx.get("transaction_hash", tx.get("tx_hash", "unknown"))

    last_exc: Optional[Exception] = None
    for attempt in range(2):
        t0 = time.monotonic()
        try:
            response = client.chat.completions.create(
                model=INTERPRETER_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_msg},
                ],
                max_tokens=80,
                temperature=0.7,
            )
            latency_ms = (time.monotonic() - t0) * 1000
            raw = response.choices[0].message.content.strip()
            # Enforce single-line and char limit
            raw = raw.replace("\n", " ").strip()
            if len(raw) > _MAX_OUTPUT_CHARS:
                raw = raw[:_MAX_OUTPUT_CHARS - 1] + "\u2026"

            logger.info(
                "Interpreter OK | tx=%s model=%s latency=%dms output=%r",
                tx_hash[:16],
                INTERPRETER_MODEL,
                int(latency_ms),
                raw,
            )
            return raw

        except _TRANSIENT_ERRORS as exc:
            latency_ms = (time.monotonic() - t0) * 1000
            last_exc = exc
            if attempt == 0:
                logger.warning(
                    "Interpreter transient error (attempt %d), retrying: %s",
                    attempt + 1,
                    exc,
                )
                continue
            # Second attempt failed — fall through to raise
        except APIStatusError as exc:
            latency_ms = (time.monotonic() - t0) * 1000
            if exc.status_code >= 500 and attempt == 0:
                logger.warning(
                    "Interpreter 5xx (attempt %d), retrying: %s",
                    attempt + 1,
                    exc,
                )
                last_exc = exc
                continue
            # 4xx or second 5xx — raise immediately
            logger.warning(
                "Interpreter failed | tx=%s status=%d latency=%dms error=%s",
                tx_hash[:16],
                exc.status_code,
                int(latency_ms),
                exc,
            )
            raise

    # Both attempts exhausted on transient errors
    logger.warning(
        "Interpreter exhausted retries | tx=%s error=%s",
        tx_hash[:16],
        last_exc,
    )
    raise last_exc  # type: ignore[misc]
