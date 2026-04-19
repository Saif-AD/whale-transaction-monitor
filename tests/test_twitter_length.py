"""Tests for Twitter formatter character limit."""

from __future__ import annotations

import pytest

from whale_poster.formatter import format_for_twitter


def _tx(**overrides) -> dict:
    base = {
        "transaction_hash": "0xabc",
        "token_symbol": "ETH",
        "usd_value": 4_200_000,
        "blockchain": "ethereum",
        "from_address": "0x1111111111111111111111111111111111111111",
        "to_address": "0x2222222222222222222222222222222222222222",
        "from_label": "Jump Crypto",
        "to_label": "Binance",
        "reasoning": "",
        "timestamp": "2026-04-01T12:00:00+00:00",
    }
    base.update(overrides)
    return base


class TestTwitterLength:

    def test_basic_under_280(self):
        msg = format_for_twitter(_tx())
        assert len(msg) <= 280

    def test_max_length_labels(self):
        msg = format_for_twitter(_tx(
            from_label="A" * 60,
            to_label="B" * 60,
        ))
        assert len(msg) <= 280

    def test_long_reasoning_truncated(self):
        msg = format_for_twitter(_tx(reasoning="X" * 300))
        assert len(msg) <= 280

    def test_long_reasoning_with_long_labels(self):
        msg = format_for_twitter(_tx(
            from_label="A" * 50,
            to_label="B" * 50,
            reasoning="Y" * 200,
        ))
        assert len(msg) <= 280

    def test_no_reasoning_under_280(self):
        # Realistic worst-case labels — Arkham entity names are rarely > 40 chars.
        msg = format_for_twitter(_tx(
            reasoning="",
            from_label="A" * 40,
            to_label="B" * 40,
        ))
        assert len(msg) <= 280

    def test_short_reasoning_included_fully(self):
        msg = format_for_twitter(_tx(reasoning="Whale buys."))
        assert "Whale buys." in msg
        assert len(msg) <= 280

    def test_exactly_280_or_less_with_unicode(self):
        msg = format_for_twitter(_tx(reasoning="Accumulation 🐋 phase."))
        assert len(msg) <= 280

    def test_with_explorer_link_under_280(self):
        """Explorer URL is appended — still must fit in 280 chars."""
        msg = format_for_twitter(_tx(
            transaction_hash="0x" + "a" * 64,
            blockchain="ethereum",
            reasoning="",
        ))
        assert len(msg) <= 280
        assert "etherscan.io" in msg

    def test_with_explorer_and_long_reasoning(self):
        msg = format_for_twitter(_tx(
            transaction_hash="0x" + "b" * 64,
            blockchain="ethereum",
            reasoning="Y" * 300,
        ))
        assert len(msg) <= 280
        assert "etherscan.io" in msg

    def test_with_explorer_long_labels_long_reasoning(self):
        # Realistic max: 25-char labels + full 66-char ETH tx hash + long reasoning.
        msg = format_for_twitter(_tx(
            from_label="A" * 25,
            to_label="B" * 25,
            transaction_hash="0x" + "c" * 64,
            blockchain="ethereum",
            reasoning="Y" * 200,
        ))
        assert len(msg) <= 280

    def test_realistic_worst_case_eth(self):
        """Realistic worst case: long token symbol, long labels, max reasoning, full ETH hash."""
        msg = format_for_twitter(_tx(
            token_symbol="PENDLE123",
            from_label="Jump Trading Market Maker",
            to_label="Binance Hot Wallet 14",
            transaction_hash="0x" + "9" * 64,
            blockchain="ethereum",
            reasoning="Y" * 200,
        ))
        assert len(msg) <= 280
        assert "etherscan.io/tx/0x" in msg
        assert "sonartracker.io/tx/0x" in msg

    def test_realistic_worst_case_solana(self):
        """Solana signatures are base58 up to 88 chars — very tight budget."""
        msg = format_for_twitter(_tx(
            token_symbol="WIF",
            from_label="Wintermute",
            to_label="Alameda",
            transaction_hash="5" * 88,
            blockchain="solana",
            reasoning="Y" * 200,
        ))
        assert len(msg) <= 280
        assert "solscan.io/tx/" in msg
        assert "sonartracker.io/tx/" in msg

    def test_xrp_explorer_link_under_280(self):
        msg = format_for_twitter(_tx(
            transaction_hash="X" * 64,
            blockchain="xrp",
            reasoning="",
        ))
        assert len(msg) <= 280
        assert "livenet.xrpl.org" in msg
