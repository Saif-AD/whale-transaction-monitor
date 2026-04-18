"""Tests for whale_poster.dedup — mark/check cycle and cooldown."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

from whale_poster.dedup import is_posted, mark_posted, is_token_on_cooldown


def _mock_client():
    return MagicMock()


class TestIsPosted:

    def test_returns_true_when_found(self):
        client = _mock_client()
        client.table().select().eq().limit().execute.return_value = MagicMock(
            data=[{"tx_hash": "0xabc"}]
        )
        assert is_posted(client, "0xabc") is True

    def test_returns_false_when_not_found(self):
        client = _mock_client()
        client.table().select().eq().limit().execute.return_value = MagicMock(data=[])
        assert is_posted(client, "0xabc") is False

    def test_returns_false_on_exception(self):
        client = _mock_client()
        client.table().select().eq().limit().execute.side_effect = Exception("db down")
        assert is_posted(client, "0xabc") is False


class TestMarkPosted:

    def test_calls_upsert(self):
        client = _mock_client()
        client.table().upsert().execute.return_value = MagicMock(data=[{}])
        result = mark_posted(client, "0xabc", "ETH", "telegram")
        assert result is True
        client.table.assert_called_with("posted_tx_hashes")

    def test_returns_false_on_exception(self):
        client = _mock_client()
        client.table().upsert().execute.side_effect = Exception("db down")
        assert mark_posted(client, "0xabc") is False


class TestIsTokenOnCooldown:

    def test_on_cooldown(self):
        client = _mock_client()
        recent = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
        client.table().select().eq().order().limit().execute.return_value = MagicMock(
            data=[{"posted_at": recent}]
        )
        assert is_token_on_cooldown(client, "ETH", 3600) is True

    def test_not_on_cooldown(self):
        client = _mock_client()
        old = (datetime.now(timezone.utc) - timedelta(seconds=7200)).isoformat()
        client.table().select().eq().order().limit().execute.return_value = MagicMock(
            data=[{"posted_at": old}]
        )
        assert is_token_on_cooldown(client, "ETH", 3600) is False

    def test_no_prior_posts(self):
        client = _mock_client()
        client.table().select().eq().order().limit().execute.return_value = MagicMock(
            data=[]
        )
        assert is_token_on_cooldown(client, "ETH", 3600) is False

    def test_returns_false_on_exception(self):
        client = _mock_client()
        client.table().select().eq().order().limit().execute.side_effect = Exception("err")
        assert is_token_on_cooldown(client, "ETH", 3600) is False
