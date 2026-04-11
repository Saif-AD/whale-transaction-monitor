"""Tests for shared.address_labels — label lookup with LRU caching."""

import time
from unittest.mock import patch, MagicMock

import pytest


# ---------------------------------------------------------------------------
# normalize_address
# ---------------------------------------------------------------------------

class TestNormalizeAddress:

    def test_evm_lowercase(self):
        from shared.address_labels import normalize_address
        mixed = "0xAbCdEf1234567890AbCdEf1234567890AbCdEf12"
        assert normalize_address(mixed, "ethereum") == mixed.lower()
        assert normalize_address(mixed, "base") == mixed.lower()
        assert normalize_address(mixed, "arbitrum") == mixed.lower()
        assert normalize_address(mixed, "polygon") == mixed.lower()

    def test_solana_case_preserved(self):
        from shared.address_labels import normalize_address
        addr = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
        assert normalize_address(addr, "solana") == addr

    def test_bitcoin_case_preserved(self):
        from shared.address_labels import normalize_address
        addr = "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
        assert normalize_address(addr, "bitcoin") == addr

    def test_xrp_case_preserved(self):
        from shared.address_labels import normalize_address
        addr = "rN7n3473SaZBCG4dFL83w7p1W9cgZB6PFr"
        assert normalize_address(addr, "xrp") == addr

    def test_empty_address(self):
        from shared.address_labels import normalize_address
        assert normalize_address("", "ethereum") == ""
        assert normalize_address("", "solana") == ""

    def test_whitespace_stripped(self):
        from shared.address_labels import normalize_address
        assert normalize_address("  0xabc  ", "ethereum") == "0xabc"
        assert normalize_address("  SolAddr  ", "solana") == "SolAddr"

    def test_case_insensitive_chain_name(self):
        from shared.address_labels import normalize_address
        addr = "0xAbCd"
        assert normalize_address(addr, "Ethereum") == "0xabcd"
        assert normalize_address(addr, "POLYGON") == "0xabcd"


# ---------------------------------------------------------------------------
# _LabelCache
# ---------------------------------------------------------------------------

class TestLabelCache:

    def test_put_and_get(self):
        from shared.address_labels import _LabelCache
        cache = _LabelCache(max_size=10, ttl=60)
        cache.put(("0xabc", "ethereum"), "Binance")
        assert cache.get(("0xabc", "ethereum")) == "Binance"

    def test_miss_returns_none(self):
        from shared.address_labels import _LabelCache
        cache = _LabelCache(max_size=10, ttl=60)
        assert cache.get(("0xnope", "ethereum")) is None

    def test_ttl_expiry(self):
        from shared.address_labels import _LabelCache
        cache = _LabelCache(max_size=10, ttl=1)
        cache.put(("0xabc", "ethereum"), "Binance")
        assert cache.get(("0xabc", "ethereum")) == "Binance"

        with patch("shared.address_labels.time") as mock_time:
            mock_time.monotonic.return_value = time.monotonic() + 2
            assert cache.get(("0xabc", "ethereum")) is None

    def test_eviction_on_max_size(self):
        from shared.address_labels import _LabelCache
        cache = _LabelCache(max_size=2, ttl=60)
        cache.put(("0x1", "ethereum"), "A")
        cache.put(("0x2", "ethereum"), "B")
        cache.put(("0x3", "ethereum"), "C")
        assert cache.size() == 2
        assert cache.get(("0x1", "ethereum")) is None
        assert cache.get(("0x3", "ethereum")) == "C"

    def test_empty_label_cached(self):
        """Empty string is a valid cache entry (no match in DB)."""
        from shared.address_labels import _LabelCache
        cache = _LabelCache(max_size=10, ttl=60)
        cache.put(("0xabc", "ethereum"), "")
        assert cache.get(("0xabc", "ethereum")) == ""


# ---------------------------------------------------------------------------
# _resolve_label
# ---------------------------------------------------------------------------

class TestResolveLabel:

    def test_label_preferred(self):
        from shared.address_labels import _resolve_label
        row = {"label": "Binance Hot Wallet", "entity_name": "Binance"}
        assert _resolve_label(row) == "Binance Hot Wallet"

    def test_entity_name_fallback(self):
        from shared.address_labels import _resolve_label
        row = {"label": "", "entity_name": "BlackRock"}
        assert _resolve_label(row) == "BlackRock"

    def test_both_empty(self):
        from shared.address_labels import _resolve_label
        row = {"label": "", "entity_name": ""}
        assert _resolve_label(row) == ""

    def test_none_values(self):
        from shared.address_labels import _resolve_label
        row = {"label": None, "entity_name": None}
        assert _resolve_label(row) == ""


# ---------------------------------------------------------------------------
# lookup_labels (integration with mocked Supabase)
# ---------------------------------------------------------------------------

def _mock_supabase_response(rows):
    """Build a mock Supabase .execute() result."""
    resp = MagicMock()
    resp.data = rows
    return resp


def _build_mock_client(rows):
    """Build a mock Supabase client that returns `rows` for any query."""
    client = MagicMock()
    chain = client.table.return_value.select.return_value.in_.return_value.in_.return_value
    chain.execute.return_value = _mock_supabase_response(rows)
    return client


class TestLookupLabels:

    def _reset_cache(self):
        """Reset module-level cache and stats between tests."""
        import shared.address_labels as mod
        mod._cache = mod._LabelCache(max_size=10_000, ttl=3600)
        mod._stats = mod._Stats()

    @patch("shared.address_labels._get_client")
    def test_both_matched(self, mock_get_client):
        self._reset_cache()
        mock_get_client.return_value = _build_mock_client([
            {"address": "0xaaa", "blockchain": "ethereum", "label": "Binance", "entity_name": "Binance"},
            {"address": "0xbbb", "blockchain": "ethereum", "label": "Coinbase", "entity_name": "Coinbase"},
        ])

        from shared.address_labels import lookup_labels
        from_label, to_label = lookup_labels("0xAAA", "0xBBB", "ethereum")
        assert from_label == "Binance"
        assert to_label == "Coinbase"

    @patch("shared.address_labels._get_client")
    def test_one_matched(self, mock_get_client):
        self._reset_cache()
        mock_get_client.return_value = _build_mock_client([
            {"address": "0xaaa", "blockchain": "ethereum", "label": "Binance", "entity_name": "Binance"},
        ])

        from shared.address_labels import lookup_labels
        from_label, to_label = lookup_labels("0xAAA", "0xCCC", "ethereum")
        assert from_label == "Binance"
        assert to_label == ""

    @patch("shared.address_labels._get_client")
    def test_none_matched(self, mock_get_client):
        self._reset_cache()
        mock_get_client.return_value = _build_mock_client([])

        from shared.address_labels import lookup_labels
        from_label, to_label = lookup_labels("0xAAA", "0xBBB", "ethereum")
        assert from_label == ""
        assert to_label == ""

    @patch("shared.address_labels._get_client")
    def test_supabase_raises_returns_empty(self, mock_get_client):
        self._reset_cache()
        client = MagicMock()
        client.table.return_value.select.return_value.in_.return_value.in_.return_value.execute.side_effect = Exception("connection reset")
        mock_get_client.return_value = client

        from shared.address_labels import lookup_labels
        from_label, to_label = lookup_labels("0xAAA", "0xBBB", "ethereum")
        assert from_label == ""
        assert to_label == ""

    @patch("shared.address_labels._get_client")
    def test_cache_hit_avoids_db_call(self, mock_get_client):
        self._reset_cache()
        client = _build_mock_client([
            {"address": "0xaaa", "blockchain": "ethereum", "label": "Binance", "entity_name": "Binance"},
            {"address": "0xbbb", "blockchain": "ethereum", "label": "Coinbase", "entity_name": "Coinbase"},
        ])
        mock_get_client.return_value = client

        from shared.address_labels import lookup_labels

        # First call hits DB
        lookup_labels("0xAAA", "0xBBB", "ethereum")
        first_call_count = client.table.call_count

        # Second call should be fully cached
        from_label, to_label = lookup_labels("0xAAA", "0xBBB", "ethereum")
        assert from_label == "Binance"
        assert to_label == "Coinbase"
        assert client.table.call_count == first_call_count  # No additional DB calls

    @patch("shared.address_labels._get_client")
    def test_ttl_expiry_triggers_requery(self, mock_get_client):
        self._reset_cache()
        import shared.address_labels as mod

        # Use a very short TTL
        mod._cache = mod._LabelCache(max_size=10_000, ttl=0)

        client = _build_mock_client([
            {"address": "0xaaa", "blockchain": "ethereum", "label": "Binance", "entity_name": "Binance"},
        ])
        mock_get_client.return_value = client

        from shared.address_labels import lookup_labels

        # First call
        lookup_labels("0xAAA", "0xBBB", "ethereum")
        first_count = client.table.call_count

        # TTL=0 means immediate expiry, second call should re-query
        lookup_labels("0xAAA", "0xBBB", "ethereum")
        assert client.table.call_count > first_count

    @patch("shared.address_labels._get_client")
    def test_entity_name_fallback(self, mock_get_client):
        self._reset_cache()
        mock_get_client.return_value = _build_mock_client([
            {"address": "0xaaa", "blockchain": "ethereum", "label": "", "entity_name": "Jump Trading"},
        ])

        from shared.address_labels import lookup_labels
        from_label, to_label = lookup_labels("0xAAA", "0xBBB", "ethereum")
        assert from_label == "Jump Trading"
        assert to_label == ""

    @patch("shared.address_labels._get_client")
    def test_solana_case_sensitive_lookup(self, mock_get_client):
        self._reset_cache()
        mock_get_client.return_value = _build_mock_client([
            {"address": "JUP6LkbZbjS1jKK", "blockchain": "solana", "label": "Jupiter", "entity_name": "Jupiter"},
        ])

        from shared.address_labels import lookup_labels
        from_label, _ = lookup_labels("JUP6LkbZbjS1jKK", "SomeOther", "solana")
        assert from_label == "Jupiter"

    @patch("shared.address_labels._get_client")
    def test_same_from_to_single_query(self, mock_get_client):
        """When from == to (self-transfer), only one DB lookup needed."""
        self._reset_cache()
        client = _build_mock_client([
            {"address": "0xaaa", "blockchain": "ethereum", "label": "Binance", "entity_name": "Binance"},
        ])
        mock_get_client.return_value = client

        from shared.address_labels import lookup_labels
        from_label, to_label = lookup_labels("0xAAA", "0xAAA", "ethereum")
        assert from_label == "Binance"
        assert to_label == "Binance"

    @patch("shared.address_labels._get_client")
    def test_supabase_client_none_returns_empty(self, mock_get_client):
        """If Supabase client can't initialize, return empty labels."""
        self._reset_cache()
        mock_get_client.return_value = None

        from shared.address_labels import lookup_labels
        from_label, to_label = lookup_labels("0xAAA", "0xBBB", "ethereum")
        assert from_label == ""
        assert to_label == ""


# ---------------------------------------------------------------------------
# Coverage stats logging
# ---------------------------------------------------------------------------

class TestStatsLogging:

    def test_log_emitted_every_1000(self):
        import shared.address_labels as mod
        stats = mod._Stats()

        with patch.object(mod.logger, "info") as mock_log:
            for _ in range(999):
                stats.record(cache_hit=True)
                stats.snapshot_and_maybe_log()
            assert mock_log.call_count == 0

            stats.record(miss=True)
            stats.snapshot_and_maybe_log()
            assert mock_log.call_count == 1
            call_args = mock_log.call_args[0]
            assert "Label coverage" in call_args[0]
