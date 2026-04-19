"""Tests for HTML escaping in Telegram formatter."""

from __future__ import annotations

import pytest

from whale_poster.formatter import format_for_telegram


def _tx(**overrides) -> dict:
    base = {
        "transaction_hash": "0xabc",
        "token_symbol": "ETH",
        "usd_value": 1_000_000,
        "blockchain": "ethereum",
        "from_address": "0x1111111111111111111111111111111111111111",
        "to_address": "0x2222222222222222222222222222222222222222",
        "from_label": "",
        "to_label": "",
        "reasoning": "",
        "timestamp": "2026-04-01T12:00:00+00:00",
    }
    base.update(overrides)
    return base


class TestHtmlEscape:

    def test_escapes_angle_brackets_in_label(self):
        msg = format_for_telegram(_tx(from_label="<SCRIPT>alert(1)</SCRIPT>"))
        assert "<SCRIPT>" not in msg
        assert "&lt;SCRIPT&gt;" in msg

    def test_escapes_ampersand_in_label(self):
        msg = format_for_telegram(_tx(to_label="A&B Exchange"))
        assert "A&amp;B Exchange" in msg

    def test_escapes_in_token_symbol(self):
        msg = format_for_telegram(_tx(token_symbol="<IMG>"))
        assert "&lt;IMG&gt;" in msg

    def test_escapes_in_reasoning(self):
        msg = format_for_telegram(_tx(reasoning="Buy <ETH> & hold for >1yr."))
        assert "&lt;ETH&gt;" in msg
        assert "&amp;" in msg

    def test_clean_strings_pass_through(self):
        msg = format_for_telegram(_tx(from_label="Binance", to_label="Coinbase"))
        assert "Binance" in msg
        assert "Coinbase" in msg

    def test_sonar_href_not_broken_by_reasoning_html(self):
        """Reasoning containing <, >, & must not break the Sonar <a href> tag."""
        tx = _tx(
            blockchain="ethereum",
            transaction_hash="0xdeadbeef",
            reasoning="Buy <ETH> & hold for >1yr period.",
            from_label="Jump Crypto",
            to_label="Binance",
        )
        msg = format_for_telegram(tx)
        assert '<a href="https://www.sonartracker.io/tx/0xdeadbeef">' in msg
        assert "View full analysis on Sonar</a>" in msg
        assert "&lt;ETH&gt;" in msg
        assert "&amp;" in msg

    def test_explorer_href_not_broken_by_reasoning_html(self):
        """Reasoning with HTML chars must not break the explorer <a href> tag."""
        tx = _tx(
            blockchain="ethereum",
            transaction_hash="0xabc",
            reasoning="<SCRIPT>payload</SCRIPT>",
            from_label="Jump",
            to_label="Binance",
        )
        msg = format_for_telegram(tx)
        assert '<a href="https://etherscan.io/tx/0xabc">View on Etherscan</a>' in msg
