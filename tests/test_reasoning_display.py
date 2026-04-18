"""Tests for reasoning display heuristic in formatter."""

from __future__ import annotations

import pytest

from whale_poster.formatter import is_narrative_reasoning, format_for_telegram


class TestIsNarrativeReasoning:

    def test_real_grok_narrative(self):
        assert is_narrative_reasoning("Whale accumulating before earnings report.") is True

    def test_short_narrative(self):
        assert is_narrative_reasoning("Big buy signal.") is True

    def test_long_narrative(self):
        text = "A" * 200
        assert is_narrative_reasoning(text) is True

    def test_empty_string(self):
        assert is_narrative_reasoning("") is False

    def test_none(self):
        assert is_narrative_reasoning(None) is False

    def test_whitespace_only(self):
        assert is_narrative_reasoning("   ") is False

    def test_placeholder_stage(self):
        assert is_narrative_reasoning("Stage 1: classification complete") is False

    def test_placeholder_classification(self):
        assert is_narrative_reasoning("Classification: TRANSFER with high confidence") is False

    def test_placeholder_score(self):
        assert is_narrative_reasoning("Score: 0.85 confidence") is False

    def test_placeholder_na(self):
        assert is_narrative_reasoning("N/A") is False

    def test_placeholder_priority_phase(self):
        assert is_narrative_reasoning(
            "Priority phase: cex_classification (USD boost: +0.00)"
        ) is False

    def test_placeholder_phase(self):
        assert is_narrative_reasoning("Phase: stage-2 enrichment") is False

    def test_placeholder_strategy(self):
        assert is_narrative_reasoning("Strategy: accumulate on dips") is False

    def test_placeholder_scoring(self):
        assert is_narrative_reasoning("Scoring: whale=0.8, cex=0.5") is False

    def test_placeholder_confidence(self):
        assert is_narrative_reasoning("Confidence: 0.95") is False

    def test_stage_in_middle_is_ok(self):
        assert is_narrative_reasoning("The whale is at a new stage of accumulation.") is True

    def test_classification_in_middle_is_ok(self):
        assert is_narrative_reasoning("Beyond typical classification, this is a whale move.") is True


class TestReasoningInTelegram:

    def _tx(self, reasoning: str = "") -> dict:
        return {
            "transaction_hash": "0x1",
            "token_symbol": "ETH",
            "usd_value": 5_000_000,
            "blockchain": "ethereum",
            "from_address": "0x1111111111111111111111111111111111111111",
            "to_address": "0x2222222222222222222222222222222222222222",
            "from_label": "Jump Crypto",
            "to_label": "Binance",
            "reasoning": reasoning,
            "timestamp": "2026-04-01T12:00:00+00:00",
        }

    def test_narrative_included(self):
        msg = format_for_telegram(self._tx("Smart money rotating into exchange."))
        assert "\u2728" in msg
        assert "Smart money rotating" in msg

    def test_template_stage_excluded(self):
        msg = format_for_telegram(self._tx("Stage 3: whale classifier detected pattern"))
        assert "\u2728" not in msg

    def test_template_classification_excluded(self):
        msg = format_for_telegram(self._tx("Classification: SELL with 0.9 confidence"))
        assert "\u2728" not in msg

    def test_template_score_excluded(self):
        msg = format_for_telegram(self._tx("Score: 0.95"))
        assert "\u2728" not in msg

    def test_na_excluded(self):
        msg = format_for_telegram(self._tx("N/A"))
        assert "\u2728" not in msg

    def test_empty_excluded(self):
        msg = format_for_telegram(self._tx(""))
        assert "\u2728" not in msg

    def test_none_excluded(self):
        msg = format_for_telegram(self._tx(None))
        assert "\u2728" not in msg
