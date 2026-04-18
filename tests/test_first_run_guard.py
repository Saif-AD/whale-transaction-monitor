"""Tests for first-run behavior — poster must post NOTHING on first run."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest

from whale_poster.poster import run_once, _read_watermark, _write_watermark


class TestFirstRunGuard:

    @patch("whale_poster.poster.create_client")
    @patch("whale_poster.poster._write_watermark")
    @patch("whale_poster.poster._read_watermark", return_value=None)
    @patch("whale_poster.poster._fetch_candidates")
    def test_first_run_posts_nothing(
        self, mock_fetch, mock_read, mock_write, mock_create,
    ):
        """On first run (no watermark), poster must post nothing and set watermark to NOW()."""
        mock_sb = MagicMock()
        stats = run_once(live=True, client=mock_sb)

        assert stats["first_run"] is True
        assert stats["posted"] == 0
        mock_fetch.assert_not_called()
        mock_write.assert_called_once()
        written_ts = mock_write.call_args[0][1]
        ts = datetime.fromisoformat(written_ts)
        assert ts.tzinfo is not None
        now = datetime.now(timezone.utc)
        assert abs((now - ts).total_seconds()) < 5

    @patch("whale_poster.poster.create_client")
    @patch("whale_poster.poster._write_watermark")
    @patch("whale_poster.poster._read_watermark", return_value=None)
    def test_first_run_does_not_query_transactions(
        self, mock_read, mock_write, mock_create,
    ):
        """First run must NOT query all_whale_transactions."""
        mock_sb = MagicMock()
        run_once(live=False, client=mock_sb)
        mock_sb.table.assert_not_called()

    @patch("whale_poster.poster.create_client")
    @patch("whale_poster.poster._fetch_candidates", return_value=[])
    @patch("whale_poster.poster._read_watermark", return_value="2026-04-01T00:00:00+00:00")
    def test_second_run_queries_normally(
        self, mock_read, mock_fetch, mock_create,
    ):
        """When watermark exists, poster queries normally."""
        mock_sb = MagicMock()
        stats = run_once(live=False, client=mock_sb)
        assert stats["first_run"] is False
        mock_fetch.assert_called_once()

    @patch("whale_poster.poster.create_client")
    @patch("whale_poster.poster._write_watermark")
    @patch("whale_poster.poster._read_watermark", return_value=None)
    def test_first_run_returns_clean_stats(
        self, mock_read, mock_write, mock_create,
    ):
        """First run returns zero counts for all stat keys."""
        mock_sb = MagicMock()
        stats = run_once(live=False, client=mock_sb)
        assert stats["candidates"] == 0
        assert stats["skipped_stablecoin"] == 0
        assert stats["skipped_dedup"] == 0
        assert stats["skipped_cex"] == 0
        assert stats["skipped_cooldown"] == 0
        assert stats["posted"] == 0
        assert stats["errors"] == 0


class TestDryRunDoesNotWrite:
    """Dry-run must NOT write to posted_tx_hashes — second dry-run should reprocess."""

    def _candidate_row(self, tx_hash="0xdeadbeef", token="ETH"):
        return {
            "transaction_hash": tx_hash,
            "token_symbol": token,
            "usd_value": 5_000_000,
            "blockchain": "ethereum",
            "from_address": "0x1111111111111111111111111111111111111111",
            "to_address": "0x2222222222222222222222222222222222222222",
            "from_label": "Jump Crypto",
            "to_label": "Paradigm",
            "reasoning": "Smart money rotating out of exchange for long-term hold.",
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
    def test_dry_run_skips_mark_posted(
        self,
        mock_create, mock_fetch, mock_read, mock_write,
        mock_is_posted, mock_cooldown, mock_mark,
    ):
        mock_fetch.return_value = [self._candidate_row()]
        mock_sb = MagicMock()
        stats = run_once(live=False, client=mock_sb)
        assert stats["posted"] == 1
        mock_mark.assert_not_called()

    @patch("whale_poster.poster.mark_posted")
    @patch("whale_poster.poster.send_message", return_value=True)
    @patch("whale_poster.poster.is_token_on_cooldown", return_value=False)
    @patch("whale_poster.poster.is_posted", return_value=False)
    @patch("whale_poster.poster._write_watermark")
    @patch(
        "whale_poster.poster._read_watermark",
        return_value="2026-04-01T00:00:00+00:00",
    )
    @patch("whale_poster.poster._fetch_candidates")
    @patch("whale_poster.poster.create_client")
    def test_live_run_calls_mark_posted(
        self,
        mock_create, mock_fetch, mock_read, mock_write,
        mock_is_posted, mock_cooldown, mock_send, mock_mark,
    ):
        mock_fetch.return_value = [self._candidate_row()]
        mock_sb = MagicMock()
        stats = run_once(live=True, client=mock_sb)
        assert stats["posted"] == 1
        mock_mark.assert_called_once()
        mock_send.assert_called_once()

    @patch("whale_poster.poster.mark_posted")
    @patch("whale_poster.poster.send_message", return_value=False)
    @patch("whale_poster.poster.is_token_on_cooldown", return_value=False)
    @patch("whale_poster.poster.is_posted", return_value=False)
    @patch("whale_poster.poster._write_watermark")
    @patch(
        "whale_poster.poster._read_watermark",
        return_value="2026-04-01T00:00:00+00:00",
    )
    @patch("whale_poster.poster._fetch_candidates")
    @patch("whale_poster.poster.create_client")
    def test_live_failed_send_does_not_mark_posted(
        self,
        mock_create, mock_fetch, mock_read, mock_write,
        mock_is_posted, mock_cooldown, mock_send, mock_mark,
    ):
        """If Telegram send fails in live mode, mark_posted must NOT be called."""
        mock_fetch.return_value = [self._candidate_row()]
        mock_sb = MagicMock()
        stats = run_once(live=True, client=mock_sb)
        assert stats["errors"] == 1
        assert stats["posted"] == 0
        mock_mark.assert_not_called()
