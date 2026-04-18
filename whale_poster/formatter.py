"""Message formatters for whale alert posting.

format_for_telegram(tx) -> HTML string
format_for_twitter(tx)  -> plain text, max 280 chars

Labels and reasoning are read directly from the all_whale_transactions row.
No Grok call, no label lookup.
"""

from __future__ import annotations

import html

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


def _label_or_short_address(label: str, address: str) -> str:
    """Return label if present, otherwise a shortened address."""
    if label and label.strip():
        return label.strip()
    if address and len(address) > 12:
        return f"{address[:6]}...{address[-4:]}"
    return address or "unknown"


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


def format_for_telegram(tx: dict) -> str:
    """Format a whale transaction for Telegram (HTML mode).

    Reads from_label, to_label, reasoning directly from the row.
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

    from_display = html.escape(_label_or_short_address(from_label, from_addr))
    to_display = html.escape(_label_or_short_address(to_label, to_addr))

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

    link = _explorer_link(blockchain_raw, tx.get("transaction_hash", ""))
    lines.append("")
    if link:
        url, explorer_name = link
        lines.append(
            f'<a href="{html.escape(url)}">View on {html.escape(explorer_name)}</a>'
        )
    lines.append("sonartracker.io")

    return "\n".join(lines)


def format_for_twitter(tx: dict) -> str:
    """Format a whale transaction for Twitter/X (plain text, max 280 chars).

    Omits reasoning if it would push the message past 280 chars.
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

    from_display = _label_or_short_address(from_label, from_addr)
    to_display = _label_or_short_address(to_label, to_addr)

    core_lines = [
        f"\U0001F40B {usd_str} {symbol}",
        f"{from_display} -> {to_display}",
        blockchain,
    ]

    core = "\n".join(core_lines)

    link = _explorer_link(blockchain_raw, tx.get("transaction_hash", ""))
    explorer_url = link[0] if link else ""

    footer_parts = []
    if explorer_url:
        footer_parts.append(explorer_url)
    footer_parts.append("sonartracker.io")
    footer = "\n".join(footer_parts)

    reasoning = tx.get("reasoning", "")
    if is_narrative_reasoning(reasoning):
        with_reasoning = f"{core}\n\n{reasoning}\n\n{footer}"
        if len(with_reasoning) <= 280:
            return with_reasoning

        available = 280 - len(core) - len(footer) - 4  # 4 = two \n\n pairs
        if available >= 20:
            truncated = reasoning[: available - 1] + "\u2026"
            return f"{core}\n\n{truncated}\n\n{footer}"

    return f"{core}\n\n{footer}"
