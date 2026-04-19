"""Tests for unknown-wallet enrichment in whale_poster.formatter.

When a whale transaction's from_label/to_label is empty, the formatter
should replace the truncated address with a contextual placeholder:

  - "Active Whale (0x1234...abcd)"    — >3 prior txs on the same chain
  - "New Wallet (0x1234...abcd)"      — no prior history on the chain
  - "Unlabeled Whale (0x1234...abcd)" — 1-3 prior txs (or DB fallback)

Lookups are memoized per-process with a 5-minute TTL so repeated calls
within a polling cycle only hit Supabase once per address.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from whale_poster import formatter
from whale_poster.formatter import (
    _enrich_cache_clear,
    _enrich_unknown_label,
    format_for_telegram,
    format_for_twitter,
)


# ------------------------------------------------------------------
# Mock Supabase helper
# ------------------------------------------------------------------

def _make_mock_client(prior_tx_count: int):
    """Build a MagicMock Supabase client whose `.execute()` returns
    ``prior_tx_count`` fake transaction_hash rows.

    The mock also exposes `execute_mock` so tests can assert call counts.
    """
    rows = [{"transaction_hash": f"0xprior{i}"} for i in range(prior_tx_count)]
    result = MagicMock()
    result.data = rows

    chain = MagicMock()
    chain.table.return_value = chain
    chain.select.return_value = chain
    chain.or_.return_value = chain
    chain.eq.return_value = chain
    chain.gte.return_value = chain
    chain.limit.return_value = chain
    chain.execute = MagicMock(return_value=result)

    chain.execute_mock = chain.execute
    return chain


def _make_failing_mock_client(exc: Exception):
    chain = MagicMock()
    chain.table.return_value = chain
    chain.select.return_value = chain
    chain.or_.return_value = chain
    chain.eq.return_value = chain
    chain.gte.return_value = chain
    chain.limit.return_value = chain
    chain.execute = MagicMock(side_effect=exc)
    chain.execute_mock = chain.execute
    return chain


def _tx(**overrides) -> dict:
    base = {
        "transaction_hash": "0xabc123",
        "token_symbol": "ETH",
        "usd_value": 4_200_000,
        "blockchain": "ethereum",
        "from_address": "0x4cff0000000000000000000000000000000042e0",
        "to_address": "0x2222222222222222222222222222222222222222",
        "from_label": "",
        "to_label": "Binance",
        "reasoning": "",
        "timestamp": "2026-04-01T12:00:00+00:00",
    }
    base.update(overrides)
    return base


@pytest.fixture(autouse=True)
def _clear_enrich_cache():
    """Ensure each test gets a fresh cache."""
    _enrich_cache_clear()
    yield
    _enrich_cache_clear()


# ------------------------------------------------------------------
# _enrich_unknown_label — core behavior
# ------------------------------------------------------------------

class TestEnrichUnknownLabel:

    def test_no_history_returns_new_wallet(self):
        client = _make_mock_client(prior_tx_count=0)
        out = _enrich_unknown_label(
            "0x4cff0000000000000000000000000000000042e0",
            "ethereum",
            client=client,
        )
        assert out.startswith("New Wallet (")
        assert out.endswith(")")
        assert "0x4cff...42e0" in out

    def test_five_prior_txs_returns_active_whale(self):
        client = _make_mock_client(prior_tx_count=5)
        out = _enrich_unknown_label(
            "0x4cff0000000000000000000000000000000042e0",
            "ethereum",
            client=client,
        )
        assert out.startswith("Active Whale (")
        assert "0x4cff...42e0" in out

    def test_four_prior_txs_returns_active_whale(self):
        """Rule threshold is strictly > 3, so 4 prior txs must be Active."""
        client = _make_mock_client(prior_tx_count=4)
        out = _enrich_unknown_label(
            "0x4cff0000000000000000000000000000000042e0",
            "ethereum",
            client=client,
        )
        assert out.startswith("Active Whale (")

    def test_one_prior_tx_returns_unlabeled_whale(self):
        client = _make_mock_client(prior_tx_count=1)
        out = _enrich_unknown_label(
            "0x4cff0000000000000000000000000000000042e0",
            "ethereum",
            client=client,
        )
        assert out.startswith("Unlabeled Whale (")
        assert "0x4cff...42e0" in out

    def test_three_prior_txs_returns_unlabeled_whale(self):
        """Boundary: 3 prior txs is NOT enough for Active Whale."""
        client = _make_mock_client(prior_tx_count=3)
        out = _enrich_unknown_label(
            "0x4cff0000000000000000000000000000000042e0",
            "ethereum",
            client=client,
        )
        assert out.startswith("Unlabeled Whale (")

    def test_db_error_falls_back_to_unlabeled(self):
        client = _make_failing_mock_client(RuntimeError("supabase down"))
        out = _enrich_unknown_label(
            "0x4cff0000000000000000000000000000000042e0",
            "ethereum",
            client=client,
        )
        assert out.startswith("Unlabeled Whale (")

    def test_no_client_falls_back_to_unlabeled(self):
        out = _enrich_unknown_label(
            "0x4cff0000000000000000000000000000000042e0",
            "ethereum",
            client=None,
        )
        assert out.startswith("Unlabeled Whale (")
        assert "0x4cff...42e0" in out

    def test_empty_address_returns_unknown(self):
        out = _enrich_unknown_label("", "ethereum", client=_make_mock_client(0))
        assert out == "unknown"

    def test_query_uses_chain_and_address_filters(self):
        client = _make_mock_client(prior_tx_count=5)
        _enrich_unknown_label(
            "0x4cff0000000000000000000000000000000042e0",
            "ethereum",
            client=client,
        )
        client.table.assert_called_with("all_whale_transactions")
        or_args = client.or_.call_args[0][0]
        assert "from_address.eq.0x4cff" in or_args
        assert "to_address.eq.0x4cff" in or_args
        chain_filter = [
            c for c in client.eq.call_args_list
            if c[0] == ("blockchain", "ethereum")
        ]
        assert len(chain_filter) == 1


# ------------------------------------------------------------------
# Cache behavior
# ------------------------------------------------------------------

class TestEnrichCache:

    def test_cache_hit_avoids_second_db_call(self):
        """Two enrichment calls for the same (address, chain) must hit
        Supabase exactly once."""
        client = _make_mock_client(prior_tx_count=5)
        addr = "0x4cff0000000000000000000000000000000042e0"

        out1 = _enrich_unknown_label(addr, "ethereum", client=client)
        out2 = _enrich_unknown_label(addr, "ethereum", client=client)

        assert out1 == out2
        assert client.execute_mock.call_count == 1

    def test_cache_hit_across_two_format_calls(self):
        """Calling format_for_telegram twice for the same empty-label tx
        must only hit the DB once thanks to the cache."""
        client = _make_mock_client(prior_tx_count=5)
        tx = _tx(from_label="", to_label="Binance")

        msg1 = format_for_telegram(tx, client=client)
        msg2 = format_for_telegram(tx, client=client)

        assert "Active Whale" in msg1
        assert msg1 == msg2
        # Only the from-side is unlabeled → exactly 1 DB call on the first
        # pass; the second format must come entirely from cache (0 new calls).
        assert client.execute_mock.call_count == 1

    def test_cache_keyed_by_address_and_chain(self):
        """Same address on a different chain must NOT reuse the cache entry."""
        client = _make_mock_client(prior_tx_count=5)
        addr = "0x4cff0000000000000000000000000000000042e0"

        _enrich_unknown_label(addr, "ethereum", client=client)
        _enrich_unknown_label(addr, "polygon", client=client)

        assert client.execute_mock.call_count == 2

    def test_expired_entry_refetches(self, monkeypatch):
        """Cache entries older than TTL must trigger a new DB call."""
        client = _make_mock_client(prior_tx_count=0)
        addr = "0x4cff0000000000000000000000000000000042e0"

        fake_time = [1000.0]
        monkeypatch.setattr(formatter.time, "monotonic", lambda: fake_time[0])

        _enrich_unknown_label(addr, "ethereum", client=client)
        assert client.execute_mock.call_count == 1

        # Advance past TTL (5 min).
        fake_time[0] += formatter._ENRICH_CACHE_TTL_SECONDS + 1

        _enrich_unknown_label(addr, "ethereum", client=client)
        assert client.execute_mock.call_count == 2


# ------------------------------------------------------------------
# Integration with format_for_telegram / format_for_twitter
# ------------------------------------------------------------------

class TestFormatterIntegration:

    def test_empty_from_label_gets_enriched_in_telegram(self):
        client = _make_mock_client(prior_tx_count=5)
        tx = _tx(from_label="", to_label="Binance")
        msg = format_for_telegram(tx, client=client)
        assert "Active Whale" in msg
        assert "0x4cff...42e0" in msg
        assert "Binance" in msg

    def test_empty_to_label_gets_enriched_in_telegram(self):
        client = _make_mock_client(prior_tx_count=0)
        tx = _tx(
            from_label="Jump Crypto",
            to_label="",
            to_address="0xbeef000000000000000000000000000000000000",
        )
        msg = format_for_telegram(tx, client=client)
        assert "New Wallet" in msg
        assert "0xbeef...0000" in msg

    def test_both_labels_empty_gets_two_enrichments(self):
        client = _make_mock_client(prior_tx_count=1)
        tx = _tx(from_label="", to_label="")
        msg = format_for_telegram(tx, client=client)
        assert msg.count("Unlabeled Whale") == 2

    def test_enriched_in_twitter_under_280(self):
        client = _make_mock_client(prior_tx_count=5)
        tx = _tx(from_label="", to_label="Binance")
        msg = format_for_twitter(tx, client=client)
        assert "Active Whale" in msg
        assert len(msg) <= 280

    def test_labels_present_do_not_trigger_enrichment(self):
        """When both labels are present, the DB must not be queried at all."""
        client = _make_mock_client(prior_tx_count=5)
        tx = _tx(from_label="Jump Crypto", to_label="Binance")
        format_for_telegram(tx, client=client)
        assert client.execute_mock.call_count == 0

    def test_no_client_preserves_legacy_short_address(self):
        """Without a client, no enrichment — behavior matches the original
        truncated-address fallback."""
        tx = _tx(from_label="", to_label="Binance")
        msg = format_for_telegram(tx)
        assert "0x4cff...42e0" in msg
        assert "Active Whale" not in msg
        assert "New Wallet" not in msg


# ------------------------------------------------------------------
# Reasoning interaction
# ------------------------------------------------------------------

class TestEnrichmentWithReasoning:

    def test_non_narrative_reasoning_still_enriches(self):
        """Placeholder / non-narrative reasoning (e.g. 'Stage 1: ...') must
        NOT block address enrichment — they are independent concerns."""
        client = _make_mock_client(prior_tx_count=5)
        tx = _tx(
            from_label="",
            to_label="Binance",
            reasoning="Stage 1: classification complete",
        )
        msg = format_for_telegram(tx, client=client)
        assert "Active Whale" in msg
        assert "0x4cff...42e0" in msg
        # Non-narrative reasoning stays suppressed in the output body.
        assert "Stage 1" not in msg

    def test_empty_reasoning_still_enriches(self):
        client = _make_mock_client(prior_tx_count=0)
        tx = _tx(from_label="", to_label="Binance", reasoning="")
        msg = format_for_telegram(tx, client=client)
        assert "New Wallet" in msg

    def test_narrative_reasoning_coexists_with_enrichment(self):
        client = _make_mock_client(prior_tx_count=5)
        tx = _tx(
            from_label="",
            to_label="Binance",
            reasoning="Whale accumulating ahead of CPI print.",
        )
        msg = format_for_telegram(tx, client=client)
        assert "Active Whale" in msg
        assert "Whale accumulating ahead of CPI print." in msg
