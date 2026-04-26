"""Message formatters for whale alert posting.

format_for_telegram(tx) -> HTML string
format_for_twitter(tx)  -> plain text, max 280 chars

Labels and reasoning are read directly from the all_whale_transactions row.
No Grok call, no label lookup.
"""

from __future__ import annotations

import html
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

_PLACEHOLDER_PREFIXES = (
    "Stage ",
    "Classification:",
    "Score:",
    "N/A",
    "Priority phase:",
    "Phase:",
    "Strategy:",
    "Scoring:",
    "Confidence:",
    # Rule-engine fallback strings — these slipped through when POSTER_USD_THRESHOLD
    # is lowered below INTERPRETER_*_USD_THRESHOLD and Grok never fires.
    # The reasoning field then carries master_classifier_reasoning verbatim.
    "EARLY EXIT",
    "Early Exit",
    "Cost-Optimized",
    "Cost-optimized",
    "Error during analysis",
    "Whale BUY:",
    "Whale SELL:",
    "high-confidence uncontested",
    "High-confidence uncontested",
)

# CEX keywords for case-insensitive substring match on labels
CEX_LABEL_KEYWORDS = frozenset({
    "binance", "coinbase", "kraken", "okx", "bybit", "bitfinex",
    "kucoin", "gate", "htx", "crypto.com", "gemini", "bitstamp",
})

CHAIN_DISPLAY_NAMES = {
    "bitcoin": "Bitcoin",
    "ethereum": "Ethereum",
    "solana": "Solana",
    "polygon": "Polygon",
    "base": "Base",
    "arbitrum": "Arbitrum",
    "xrp": "XRP",
}

EXPLORER_URLS = {
    "bitcoin":  ("https://mempool.space/tx/{tx_hash}",           "mempool.space"),
    "ethereum": ("https://etherscan.io/tx/{tx_hash}",            "Etherscan"),
    "polygon":  ("https://polygonscan.com/tx/{tx_hash}",         "Polygonscan"),
    "arbitrum": ("https://arbiscan.io/tx/{tx_hash}",             "Arbiscan"),
    "base":     ("https://basescan.org/tx/{tx_hash}",            "Basescan"),
    "solana":   ("https://solscan.io/tx/{tx_hash}",              "Solscan"),
    "xrp":      ("https://livenet.xrpl.org/transactions/{tx_hash}", "XRPL Explorer"),
}

SONAR_TX_URL_TEMPLATE = "https://www.sonartracker.io/tx/{tx_hash}"
SONAR_WALLET_URL_TEMPLATE = "https://sonartracker.io/wallet/{addr}"


def _sonar_tx_url(tx_hash: str) -> str:
    """Return the Sonar deep link for a transaction hash, or '' if missing."""
    if not tx_hash:
        return ""
    return SONAR_TX_URL_TEMPLATE.format(tx_hash=tx_hash)


def _sonar_wallet_url(address: str) -> str:
    """Return the Sonar wallet-profile deep link, or '' if missing."""
    if not address:
        return ""
    return SONAR_WALLET_URL_TEMPLATE.format(addr=address)


def _chain_display(blockchain: str) -> str:
    key = (blockchain or "").lower()
    return CHAIN_DISPLAY_NAMES.get(key, (blockchain or "unknown").capitalize())


def _explorer_link(blockchain: str, tx_hash: str) -> tuple[str, str] | None:
    """Return (url, explorer_name) or None if chain is unknown / tx_hash missing."""
    if not tx_hash:
        return None
    key = (blockchain or "").lower()
    entry = EXPLORER_URLS.get(key)
    if not entry:
        return None
    url_template, explorer_name = entry
    return url_template.format(tx_hash=tx_hash), explorer_name


def is_narrative_reasoning(reasoning: str | None) -> bool:
    """Return True if reasoning is real Grok narrative, not a placeholder."""
    if not reasoning or not reasoning.strip():
        return False
    for prefix in _PLACEHOLDER_PREFIXES:
        if reasoning.startswith(prefix):
            return False
    return True


def _short_address(address: str) -> str:
    """Return a shortened form of the address for display."""
    if address and len(address) > 12:
        return f"{address[:6]}...{address[-4:]}"
    return address or "unknown"


# ------------------------------------------------------------------
# Unknown-wallet enrichment (Active Whale / New Wallet / Unlabeled Whale)
# ------------------------------------------------------------------

# Cache: {(address, blockchain): (inserted_monotonic_ts, label_text)}
_ENRICH_CACHE: dict[tuple[str, str], tuple[float, str]] = {}
_ENRICH_CACHE_TTL_SECONDS = 300  # 5 minutes
_ENRICH_CACHE_MAX_SIZE = 500
_ENRICH_HISTORY_DAYS = 7
_ENRICH_ACTIVE_MIN_COUNT = 4  # strictly > 3 prior txs

# "Unlabeled Whale" is the conservative fallback whenever the DB is
# unreachable or the query fails — we still return the short address so
# operators can manually verify on-chain.
_ENRICH_FALLBACK = "Unlabeled Whale"


def _enrich_cache_clear() -> None:
    """Clear the enrichment cache. Primarily for tests."""
    _ENRICH_CACHE.clear()


def _enrich_unknown_label(
    address: str,
    blockchain: str,
    client: Optional[Any] = None,
) -> str:
    """Return a contextual placeholder for an unlabeled whale wallet.

    Returns one of:
      - "Active Whale (0x1234...abcd)"   (>3 prior txs in last 7 days on chain)
      - "New Wallet (0x1234...abcd)"     (no prior history on chain)
      - "Unlabeled Whale (0x1234...abcd)" (1-3 prior txs, or DB error fallback)

    Results are cached in-process for 5 minutes (max 500 entries) so repeated
    lookups for the same address within a polling cycle hit Supabase once.
    """
    short = _short_address(address)
    if not address:
        return short

    chain_key = (blockchain or "").lower()
    cache_key = (address, chain_key)
    now = time.monotonic()

    cached = _ENRICH_CACHE.get(cache_key)
    if cached is not None:
        inserted, cached_label = cached
        if now - inserted < _ENRICH_CACHE_TTL_SECONDS:
            return f"{cached_label} ({short})"
        _ENRICH_CACHE.pop(cache_key, None)

    if client is None:
        return f"{_ENRICH_FALLBACK} ({short})"

    try:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=_ENRICH_HISTORY_DAYS)
        ).isoformat()
        result = (
            client.table("all_whale_transactions")
            .select("transaction_hash")
            .or_(f"from_address.eq.{address},to_address.eq.{address}")
            .eq("blockchain", blockchain)
            .gte("timestamp", cutoff)
            .limit(_ENRICH_ACTIVE_MIN_COUNT + 1)
            .execute()
        )
        rows = getattr(result, "data", None) or []
        count = len(rows)
    except Exception as e:
        logger.debug("Enrichment query failed for %s on %s: %s", short, chain_key, e)
        return f"{_ENRICH_FALLBACK} ({short})"

    if count >= _ENRICH_ACTIVE_MIN_COUNT:
        label = "Active Whale"
    elif count == 0:
        label = "New Wallet"
    else:
        label = "Unlabeled Whale"

    if len(_ENRICH_CACHE) >= _ENRICH_CACHE_MAX_SIZE:
        # Evict oldest entry (simple FIFO-by-insertion-time eviction).
        oldest_key = min(_ENRICH_CACHE, key=lambda k: _ENRICH_CACHE[k][0])
        _ENRICH_CACHE.pop(oldest_key, None)
    _ENRICH_CACHE[cache_key] = (now, label)

    return f"{label} ({short})"


def _label_or_short_address(
    label: str,
    address: str,
    blockchain: Optional[str] = None,
    client: Optional[Any] = None,
) -> str:
    """Return label if present, otherwise an enriched placeholder with the
    shortened address. When ``client`` is None, enrichment is skipped and
    only the shortened address is returned (preserves legacy behavior for
    callers that don't have a Supabase handle)."""
    if label and label.strip():
        return label.strip()
    if not address:
        return "unknown"
    if client is not None and blockchain:
        return _enrich_unknown_label(address, blockchain, client=client)
    return _short_address(address)


# ------------------------------------------------------------------
# Wallet-profile lookup (Sonar tracker deep link)
# ------------------------------------------------------------------

# Cache: {address: (inserted_monotonic_ts, entity_name_or_empty)}
# Empty string is a valid cache value — it means "we asked, no profile
# was found" and we don't want to keep re-querying.
_PROFILE_CACHE: dict[str, tuple[float, str]] = {}
_PROFILE_CACHE_TTL_SECONDS = 300
_PROFILE_CACHE_MAX_SIZE = 500


def _profile_cache_clear() -> None:
    """Clear the wallet-profile cache. Primarily for tests."""
    _PROFILE_CACHE.clear()


def _lookup_profile_entity(
    address: str,
    client: Optional[Any] = None,
) -> str:
    """Return the entity_name for a profiled wallet, or '' if unprofiled.

    Looks up the `wallet_profiles` table with a 5-minute in-process cache
    (max 500 entries). Returns "" for cache-miss-and-no-row, "" for any
    DB error (callers fall back to the existing _enrich_unknown_label
    path), and the trimmed entity_name otherwise.
    """
    if not address or client is None:
        return ""

    cache_key = address
    now = time.monotonic()

    cached = _PROFILE_CACHE.get(cache_key)
    if cached is not None:
        inserted, entity = cached
        if now - inserted < _PROFILE_CACHE_TTL_SECONDS:
            return entity
        _PROFILE_CACHE.pop(cache_key, None)

    entity_name = ""
    try:
        result = (
            client.table("wallet_profiles")
            .select("entity_name")
            .eq("address", address)
            .limit(1)
            .execute()
        )
        rows = getattr(result, "data", None) or []
        if rows:
            entity_name = (rows[0].get("entity_name") or "").strip()
    except Exception as e:
        logger.debug("Wallet-profile lookup failed for %s: %s", address[:10], e)
        return ""

    if len(_PROFILE_CACHE) >= _PROFILE_CACHE_MAX_SIZE:
        oldest_key = min(_PROFILE_CACHE, key=lambda k: _PROFILE_CACHE[k][0])
        _PROFILE_CACHE.pop(oldest_key, None)
    _PROFILE_CACHE[cache_key] = (now, entity_name)

    return entity_name


def _telegram_wallet_display(
    label: str,
    address: str,
    blockchain: Optional[str] = None,
    client: Optional[Any] = None,
) -> str:
    """Return the HTML-escaped Telegram display for a wallet side.

    Resolution order:
      1. Existing label (from_label / to_label on the row) — plain text.
      2. wallet_profiles entity_name lookup — wrap in <a href="sonar/...">
         with entity_name as link text.
      3. Existing _enrich_unknown_label fallback (Active Whale / New Wallet
         / Unlabeled Whale) — wrap in <a href="sonar/..."> too, since the
         user can still click through to a freshly-built profile page.
      4. Plain short address — when no client / blockchain is supplied.

    The returned string is already HTML-escaped where appropriate; the
    caller MUST NOT html.escape() the result again.
    """
    if label and label.strip():
        return html.escape(label.strip())
    if not address:
        return "unknown"

    wallet_url = _sonar_wallet_url(address)

    if client is not None:
        entity = _lookup_profile_entity(address, client=client)
        if entity:
            link_text = html.escape(entity)
            return f'<a href="{html.escape(wallet_url)}">{link_text}</a>'

    if client is not None and blockchain:
        enriched = _enrich_unknown_label(address, blockchain, client=client)
        link_text = html.escape(enriched)
        return f'<a href="{html.escape(wallet_url)}">{link_text}</a>'

    return html.escape(_short_address(address))


def _format_usd(value: float) -> str:
    """Format USD value for display: $4.2M, $850K, $12.5M, etc."""
    if value >= 1_000_000:
        m = value / 1_000_000
        if m >= 10:
            return f"${m:,.0f}M"
        return f"${m:,.1f}M"
    if value >= 1_000:
        k = value / 1_000
        return f"${k:,.0f}K"
    return f"${value:,.0f}"


def _format_token_amount(amount: float | None, symbol: str) -> str:
    """Format token amount with commas."""
    if amount is None or amount <= 0:
        return symbol
    if amount >= 1:
        return f"{amount:,.0f} {symbol}"
    return f"{amount:.4f} {symbol}"


def _is_cex_label(label: str) -> bool:
    """Check if a label matches a known CEX by substring."""
    if not label:
        return False
    lower = label.lower()
    return any(kw in lower for kw in CEX_LABEL_KEYWORDS)


def is_cex_to_cex(from_label: str, to_label: str) -> bool:
    """Return True if both sides are CEX-labeled."""
    return _is_cex_label(from_label) and _is_cex_label(to_label)


def format_for_telegram(tx: dict, client: Optional[Any] = None) -> str:
    """Format a whale transaction for Telegram (HTML mode).

    Reads from_label, to_label, reasoning directly from the row. When a
    Supabase ``client`` is provided, empty labels are replaced with a
    contextual placeholder (Active Whale / New Wallet / Unlabeled Whale).
    """
    symbol = html.escape(tx.get("token_symbol", "???"))
    usd_value = float(tx.get("usd_value", 0))
    usd_str = html.escape(_format_usd(usd_value))
    blockchain_raw = tx.get("blockchain") or "unknown"
    blockchain = html.escape(_chain_display(blockchain_raw))

    from_label = tx.get("from_label", "")
    to_label = tx.get("to_label", "")
    from_addr = tx.get("from_address", "")
    to_addr = tx.get("to_address", "")

    # _telegram_wallet_display returns pre-escaped HTML (linkified when a
    # wallet_profiles row exists or when _enrich_unknown_label fires), so
    # we deliberately do NOT html.escape() the result here.
    from_display = _telegram_wallet_display(from_label, from_addr, blockchain_raw, client)
    to_display = _telegram_wallet_display(to_label, to_addr, blockchain_raw, client)

    lines = [
        f"\U0001F40B <b>{symbol}</b> (~{usd_str})",
        f"From: {from_display}",
        f"To: {to_display}",
        f"Chain: {blockchain}",
    ]

    reasoning = tx.get("reasoning", "")
    if is_narrative_reasoning(reasoning):
        lines.append("")
        lines.append(f"\u2728 {html.escape(reasoning)}")

    tx_hash = tx.get("transaction_hash", "")
    link = _explorer_link(blockchain_raw, tx_hash)
    sonar_url = _sonar_tx_url(tx_hash)

    if link or sonar_url:
        lines.append("")
    if sonar_url:
        lines.append(
            f'<a href="{html.escape(sonar_url)}">View full analysis on Sonar</a>'
        )
    if link:
        url, explorer_name = link
        lines.append(
            f'<a href="{html.escape(url)}">View on {html.escape(explorer_name)}</a>'
        )

    return "\n".join(lines)


def format_for_twitter(tx: dict, client: Optional[Any] = None) -> str:
    """Format a whale transaction for Twitter/X (plain text, max 280 chars).

    Omits reasoning if it would push the message past 280 chars. When a
    Supabase ``client`` is provided, empty labels are replaced with a
    contextual placeholder (Active Whale / New Wallet / Unlabeled Whale).
    """
    symbol = tx.get("token_symbol", "???")
    usd_value = float(tx.get("usd_value", 0))
    usd_str = _format_usd(usd_value)
    blockchain_raw = tx.get("blockchain") or "unknown"
    blockchain = _chain_display(blockchain_raw)

    from_label = tx.get("from_label", "")
    to_label = tx.get("to_label", "")
    from_addr = tx.get("from_address", "")
    to_addr = tx.get("to_address", "")

    from_display = _label_or_short_address(from_label, from_addr, blockchain_raw, client)
    to_display = _label_or_short_address(to_label, to_addr, blockchain_raw, client)

    core_lines = [
        f"\U0001F40B {usd_str} {symbol}",
        f"{from_display} -> {to_display}",
        blockchain,
    ]

    core = "\n".join(core_lines)

    tx_hash = tx.get("transaction_hash", "")
    link = _explorer_link(blockchain_raw, tx_hash)
    explorer_url = link[0] if link else ""
    sonar_url = _sonar_tx_url(tx_hash)

    footer_parts = []
    if sonar_url:
        footer_parts.append(sonar_url)
    if explorer_url:
        footer_parts.append(explorer_url)
    footer = "\n".join(footer_parts)

    sep = "\n\n" if footer else ""

    reasoning = tx.get("reasoning", "")
    if is_narrative_reasoning(reasoning):
        with_reasoning = f"{core}\n\n{reasoning}{sep}{footer}"
        if len(with_reasoning) <= 280:
            return with_reasoning

        # Leave a sensible budget for reasoning — reserve core, footer, and
        # two \n\n separators (4 chars). Drop reasoning entirely if < 20
        # chars would remain for it.
        available = 280 - len(core) - len(footer) - 4
        if available >= 20:
            truncated = reasoning[: available - 1] + "\u2026"
            return f"{core}\n\n{truncated}{sep}{footer}"

    return f"{core}{sep}{footer}"
