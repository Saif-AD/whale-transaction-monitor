"""Tests for per-token cooldown logic."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest

from whale_poster.dedup import is_token_on_cooldown


def _mock_client_with_posted_at(seconds_ago: int | None = None) -> MagicMock:
    client = MagicMock()
    if seconds_ago is None:
        client.table().select().eq().order().limit().execute.return_value = MagicMock(data=[])
    else:
        ts = (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).isoformat()
        client.table().select().eq().order().limit().execute.return_value = MagicMock(
            data=[{"posted_at": ts}]
        )
    return client


class TestCooldown:

    def test_blocks_during_cooldown(self):
        client = _mock_client_with_posted_at(seconds_ago=600)
        assert is_token_on_cooldown(client, "ETH", 3600) is True

    def test_allows_after_cooldown(self):
        client = _mock_client_with_posted_at(seconds_ago=4000)
        assert is_token_on_cooldown(client, "ETH", 3600) is False

    def test_allows_first_post(self):
        client = _mock_client_with_posted_at(seconds_ago=None)
        assert is_token_on_cooldown(client, "ETH", 3600) is False

    def test_exact_boundary_not_on_cooldown(self):
        client = _mock_client_with_posted_at(seconds_ago=3601)
        assert is_token_on_cooldown(client, "ETH", 3600) is False

    def test_just_inside_boundary_on_cooldown(self):
        client = _mock_client_with_posted_at(seconds_ago=3599)
        assert is_token_on_cooldown(client, "ETH", 3600) is True

    def test_different_tokens_independent(self):
        client = _mock_client_with_posted_at(seconds_ago=100)
        assert is_token_on_cooldown(client, "ETH", 3600) is True
        client2 = _mock_client_with_posted_at(seconds_ago=None)
        assert is_token_on_cooldown(client2, "BTC", 3600) is False

    def test_uses_order_by_desc_limit_1(self):
        """Verify the query uses ORDER BY posted_at DESC LIMIT 1, not MAX()."""
        client = MagicMock()
        ts = (datetime.now(timezone.utc) - timedelta(seconds=100)).isoformat()
        client.table().select().eq().order().limit().execute.return_value = MagicMock(
            data=[{"posted_at": ts}]
        )
        is_token_on_cooldown(client, "ETH", 3600)
        client.table().select().eq().order.assert_called_with("posted_at", desc=True)
        client.table().select().eq().order().limit.assert_called_with(1)
