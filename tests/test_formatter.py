"""Tests for whale_poster.formatter — Telegram + Twitter formatters."""

from __future__ import annotations

import pytest

from whale_poster.formatter import format_for_telegram, format_for_twitter


def _tx(**overrides) -> dict:
    base = {
        "transaction_hash": "0xabc123",
        "token_symbol": "ETH",
        "usd_value": 4_200_000,
        "blockchain": "ethereum",
        "from_address": "0x1111111111111111111111111111111111111111",
        "to_address": "0x2222222222222222222222222222222222222222",
        "from_label": "Jump Crypto",
        "to_label": "Binance",
        "reasoning": "Smart money rotating into exchange — watch for a sell-off.",
        "timestamp": "2026-04-01T12:00:00+00:00",
    }
    base.update(overrides)
    return base


class TestFormatForTelegram:

    def test_basic_output_contains_key_fields(self):
        msg = format_for_telegram(_tx())
        assert "ETH" in msg
        assert "Jump Crypto" in msg
        assert "Binance" in msg
        assert "Ethereum" in msg
        assert "sonartracker.io/tx/0xabc123" in msg
        assert "View full analysis on Sonar" in msg

    def test_no_plain_sonartracker_footer(self):
        """The plain-text 'sonartracker.io' footer must be gone."""
        msg = format_for_telegram(_tx())
        lines = msg.split("\n")
        assert "sonartracker.io" not in lines
        assert "sonartracker.io/tx/" in msg

    def test_sonar_link_order(self):
        """Sonar deep link must come BEFORE the explorer link."""
        msg = format_for_telegram(_tx())
        explorer_idx = msg.find("View on Etherscan")
        sonar_idx = msg.find("View full analysis on Sonar")
        assert explorer_idx != -1 and sonar_idx != -1
        assert sonar_idx < explorer_idx

    def test_usd_formatted(self):
        msg = format_for_telegram(_tx(usd_value=4_200_000))
        assert "$4.2M" in msg

    def test_reasoning_included_when_narrative(self):
        msg = format_for_telegram(_tx(reasoning="Whale accumulating before earnings report."))
        assert "Whale accumulating" in msg

    def test_reasoning_excluded_when_empty(self):
        msg = format_for_telegram(_tx(reasoning=""))
        assert "\u2728" not in msg

    def test_reasoning_excluded_when_none(self):
        msg = format_for_telegram(_tx(reasoning=None))
        assert "\u2728" not in msg

    def test_shortened_address_fallback(self):
        msg = format_for_telegram(_tx(from_label="", to_label=""))
        assert "0x1111...1111" in msg
        assert "0x2222...2222" in msg

    def test_labels_used_when_present(self):
        msg = format_for_telegram(_tx())
        assert "Jump Crypto" in msg
        assert "0x1111" not in msg

    def test_chain_display_xrp_uppercase(self):
        msg = format_for_telegram(_tx(blockchain="xrp"))
        assert "XRP" in msg
        assert "Xrp" not in msg

    def test_chain_display_bitcoin(self):
        msg = format_for_telegram(_tx(blockchain="bitcoin"))
        assert "Bitcoin" in msg

    def test_chain_display_unknown_chain_capitalized(self):
        msg = format_for_telegram(_tx(blockchain="mysterychain"))
        assert "Mysterychain" in msg

    def test_explorer_link_ethereum(self):
        msg = format_for_telegram(_tx(
            blockchain="ethereum",
            transaction_hash="0xdeadbeef",
        ))
        assert "etherscan.io/tx/0xdeadbeef" in msg
        assert "View on Etherscan" in msg

    def test_explorer_link_xrp(self):
        msg = format_for_telegram(_tx(
            blockchain="xrp",
            transaction_hash="ABC123",
        ))
        assert "livenet.xrpl.org/transactions/ABC123" in msg

    def test_explorer_link_bitcoin(self):
        msg = format_for_telegram(_tx(
            blockchain="bitcoin",
            transaction_hash="btc_tx",
        ))
        assert "mempool.space/tx/btc_tx" in msg

    def test_explorer_link_omitted_for_unknown_chain(self):
        msg = format_for_telegram(_tx(blockchain="unknownchain"))
        assert "View on" not in msg
        # Sonar deep link still present even for unknown chain
        assert "View full analysis on Sonar" in msg
        assert "sonartracker.io/tx/0xabc123" in msg


class TestFormatForTwitter:

    def test_never_exceeds_280(self):
        msg = format_for_twitter(_tx(
            reasoning="A" * 300,
            from_label="Very Long Exchange Name That Goes On",
            to_label="Another Very Long Exchange Name For Testing",
        ))
        assert len(msg) <= 280

    def test_basic_fields(self):
        msg = format_for_twitter(_tx())
        assert "ETH" in msg
        assert "Jump Crypto" in msg
        assert "Binance" in msg
        assert "sonartracker.io/tx/0xabc123" in msg

    def test_no_plain_sonartracker_footer(self):
        """The plain-text 'sonartracker.io' footer must be gone (no tx) — must be URL form."""
        msg = format_for_twitter(_tx())
        lines = msg.split("\n")
        assert "sonartracker.io" not in lines
        assert any("sonartracker.io/tx/" in line for line in lines)

    def test_reasoning_included_when_fits(self):
        msg = format_for_twitter(_tx(reasoning="Short narrative."))
        assert "Short narrative." in msg

    def test_reasoning_truncated_with_ellipsis(self):
        long_reasoning = "A" * 250
        msg = format_for_twitter(_tx(reasoning=long_reasoning))
        assert len(msg) <= 280
        assert "\u2026" in msg

    def test_reasoning_omitted_when_empty(self):
        msg = format_for_twitter(_tx(reasoning=""))
        assert len(msg) <= 280
        assert "sonartracker.io/tx/" in msg

    def test_twitter_includes_explorer_url(self):
        msg = format_for_twitter(_tx(
            blockchain="ethereum",
            transaction_hash="0xdead",
            reasoning="",
        ))
        assert "etherscan.io/tx/0xdead" in msg
        assert len(msg) <= 280

    def test_twitter_chain_display_xrp(self):
        msg = format_for_twitter(_tx(blockchain="xrp"))
        assert "XRP" in msg
