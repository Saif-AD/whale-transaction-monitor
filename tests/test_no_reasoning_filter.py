"""Tests for the no-reasoning filter.

Posts without real Grok narrative reasoning (empty or placeholder) are
skipped, EXCEPT when usd_value is >= $10M — moves that large are news on
size alone and should be posted regardless of reasoning quality.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def _row(
    *,
    tx_hash: str = "0xabc",
    token: str = "ETH",
    usd_value: float = 1_000_000,
    from_label: str = "Jump Crypto",
    to_label: str = "Binance Hot Wallet",
    reasoning: str = "",
) -> dict:
    return {
        "transaction_hash": tx_hash,
        "token_symbol": token,
        "usd_value": usd_value,
        "blockchain": "ethereum",
        "from_address": "0x1111111111111111111111111111111111111111",
        "to_address": "0x2222222222222222222222222222222222222222",
        "from_label": from_label,
        "to_label": to_label,
        "reasoning": reasoning,
        "timestamp": "2026-04-01T12:00:00+00:00",
        "is_cex_transaction": False,
    }


@patch("whale_poster.poster.mark_posted")
@patch("whale_poster.poster.is_token_on_cooldown", return_value=False)
@patch("whale_poster.poster.is_posted", return_value=False)
@patch("whale_poster.poster._write_watermark")
@patch(
    "whale_poster.poster._read_watermark",
    return_value="2026-04-01T00:00:00+00:00",
)
@patch("whale_poster.poster._fetch_candidates")
@patch("whale_poster.poster.create_client")
class TestNoReasoningFilter:

    def test_empty_reasoning_is_skipped(
        self,
        mock_create, mock_fetch, mock_read, mock_write,
        mock_is_posted, mock_cooldown, mock_mark,
    ):
        """Empty reasoning under $10M must be filtered out."""
        from whale_poster.poster import run_once

        mock_fetch.return_value = [
            _row(usd_value=1_000_000, reasoning=""),
        ]
        stats = run_once(live=False, client=MagicMock())

        assert stats["no_reasoning"] == 1
        assert stats["posted"] == 0

    def test_placeholder_reasoning_is_skipped(
        self,
        mock_create, mock_fetch, mock_read, mock_write,
        mock_is_posted, mock_cooldown, mock_mark,
    ):
        """Placeholder scaffolding like 'Stage 1:' counts as no reasoning."""
        from whale_poster.poster import run_once

        mock_fetch.return_value = [
            _row(
                usd_value=2_500_000,
                reasoning="Stage 1: initial scoring pending Grok narrative.",
            ),
        ]
        stats = run_once(live=False, client=MagicMock())

        assert stats["no_reasoning"] == 1
        assert stats["posted"] == 0

    def test_ten_million_bypasses_filter(
        self,
        mock_create, mock_fetch, mock_read, mock_write,
        mock_is_posted, mock_cooldown, mock_mark,
    ):
        """Moves >= $10M must post even without narrative reasoning."""
        from whale_poster.poster import run_once

        mock_fetch.return_value = [
            _row(usd_value=10_000_000, reasoning=""),
        ]
        stats = run_once(live=False, client=MagicMock())

        assert stats["no_reasoning"] == 0
        assert stats["posted"] == 1
