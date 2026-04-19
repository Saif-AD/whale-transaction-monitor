"""Tests for the unknown-only filter.

Transactions where both parties are unlabeled (no from_label AND no to_label)
should be skipped, since there's no narrative context to post. Very large
moves (>= $5M) bypass the filter because the size alone is notable.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _row(
    *,
    tx_hash: str = "0xabc",
    token: str = "ETH",
    usd_value: float = 1_000_000,
    from_label: str = "",
    to_label: str = "",
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
        "reasoning": "",
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
class TestUnknownOnlyFilter:

    def test_both_unknown_small_is_skipped(
        self,
        mock_create, mock_fetch, mock_read, mock_write,
        mock_is_posted, mock_cooldown, mock_mark,
    ):
        """Both parties unknown + usd_value < $5M must be filtered out."""
        from whale_poster.poster import run_once

        mock_fetch.return_value = [
            _row(usd_value=1_000_000, from_label="", to_label=""),
        ]
        stats = run_once(live=False, client=MagicMock())

        assert stats["unknown_only"] == 1
        assert stats["posted"] == 0

    def test_both_unknown_large_bypasses_filter(
        self,
        mock_create, mock_fetch, mock_read, mock_write,
        mock_is_posted, mock_cooldown, mock_mark,
    ):
        """Both parties unknown but usd_value >= $5M must still post."""
        from whale_poster.poster import run_once

        mock_fetch.return_value = [
            _row(usd_value=5_000_000, from_label="", to_label=""),
        ]
        stats = run_once(live=False, client=MagicMock())

        assert stats["unknown_only"] == 0
        assert stats["posted"] == 1

    def test_one_labeled_side_passes(
        self,
        mock_create, mock_fetch, mock_read, mock_write,
        mock_is_posted, mock_cooldown, mock_mark,
    ):
        """At least one labeled side means the filter does not apply."""
        from whale_poster.poster import run_once

        mock_fetch.return_value = [
            _row(usd_value=1_000_000, from_label="Jump Crypto", to_label=""),
        ]
        stats = run_once(live=False, client=MagicMock())

        assert stats["unknown_only"] == 0
        assert stats["posted"] == 1

    def test_whitespace_only_labels_treated_as_unknown(
        self,
        mock_create, mock_fetch, mock_read, mock_write,
        mock_is_posted, mock_cooldown, mock_mark,
    ):
        """Whitespace-only labels count as unknown."""
        from whale_poster.poster import run_once

        mock_fetch.return_value = [
            _row(usd_value=1_000_000, from_label="   ", to_label="\t"),
        ]
        stats = run_once(live=False, client=MagicMock())

        assert stats["unknown_only"] == 1
        assert stats["posted"] == 0

    def test_null_labels_treated_as_unknown(
        self,
        mock_create, mock_fetch, mock_read, mock_write,
        mock_is_posted, mock_cooldown, mock_mark,
    ):
        """None/null labels from the DB count as unknown."""
        from whale_poster.poster import run_once

        mock_fetch.return_value = [
            _row(usd_value=1_000_000, from_label=None, to_label=None),
        ]
        stats = run_once(live=False, client=MagicMock())

        assert stats["unknown_only"] == 1
        assert stats["posted"] == 0
