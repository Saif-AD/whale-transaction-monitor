"""Tests for the self-transfer filter.

Transactions where both sides carry the same label (case-insensitive, stripped)
are internal shuffles with no narrative value — e.g. "Top WBTC holder → Top
WBTC holder" or "Binance Hot Wallet → Binance Hot Wallet". These should be
skipped and counted under stats["self_transfer"].
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def _row(
    *,
    tx_hash: str = "0xabc",
    token: str = "WBTC",
    usd_value: float = 1_000_000,
    from_label: str = "",
    to_label: str = "",
    reasoning: str = "Whale is consolidating WBTC exposure on-chain ahead of expected volatility.",
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
class TestSelfTransferFilter:

    def test_matching_labels_are_skipped(
        self,
        mock_create, mock_fetch, mock_read, mock_write,
        mock_is_posted, mock_cooldown, mock_mark,
    ):
        """Identical from/to labels must be filtered as self-transfer."""
        from whale_poster.poster import run_once

        mock_fetch.return_value = [
            _row(from_label="Top WBTC holder", to_label="Top WBTC holder"),
        ]
        stats = run_once(live=False, client=MagicMock())

        assert stats["self_transfer"] == 1
        assert stats["posted"] == 0

    def test_different_labels_pass(
        self,
        mock_create, mock_fetch, mock_read, mock_write,
        mock_is_posted, mock_cooldown, mock_mark,
    ):
        """Distinct labels on each side must NOT trigger the filter."""
        from whale_poster.poster import run_once

        mock_fetch.return_value = [
            _row(from_label="Jump Crypto", to_label="Binance Hot Wallet"),
        ]
        stats = run_once(live=False, client=MagicMock())

        assert stats["self_transfer"] == 0
        assert stats["posted"] == 1

    def test_case_insensitive_match_is_skipped(
        self,
        mock_create, mock_fetch, mock_read, mock_write,
        mock_is_posted, mock_cooldown, mock_mark,
    ):
        """Label comparison must be case-insensitive and whitespace-tolerant."""
        from whale_poster.poster import run_once

        mock_fetch.return_value = [
            _row(
                from_label="Top WBTC holder",
                to_label="  top wbtc HOLDER  ",
            ),
        ]
        stats = run_once(live=False, client=MagicMock())

        assert stats["self_transfer"] == 1
        assert stats["posted"] == 0

    def test_one_empty_one_labeled_passes(
        self,
        mock_create, mock_fetch, mock_read, mock_write,
        mock_is_posted, mock_cooldown, mock_mark,
    ):
        """If only one side has a label, it is not a self-transfer."""
        from whale_poster.poster import run_once

        mock_fetch.return_value = [
            _row(from_label="Jump Crypto", to_label=""),
        ]
        stats = run_once(live=False, client=MagicMock())

        assert stats["self_transfer"] == 0
        assert stats["posted"] == 1
