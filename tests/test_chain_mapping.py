"""Tests for Arkham chain ID mapping."""

from __future__ import annotations

import pytest

from arkham_backfill.constants import ARKHAM_CHAIN_MAP


class TestChainMapping:

    def test_ethereum_maps(self):
        assert ARKHAM_CHAIN_MAP["ethereum"] == "ethereum"

    def test_bitcoin_maps(self):
        assert ARKHAM_CHAIN_MAP["bitcoin"] == "bitcoin"

    def test_solana_maps(self):
        assert ARKHAM_CHAIN_MAP["solana"] == "solana"

    def test_base_maps(self):
        assert ARKHAM_CHAIN_MAP["base"] == "base"

    def test_arbitrum_one_maps_to_arbitrum(self):
        """Arkham uses 'arbitrum_one', our DB uses 'arbitrum'."""
        assert ARKHAM_CHAIN_MAP["arbitrum_one"] == "arbitrum"

    def test_polygon_maps(self):
        assert ARKHAM_CHAIN_MAP["polygon"] == "polygon"

    def test_tron_skipped(self):
        assert ARKHAM_CHAIN_MAP["tron"] is None

    def test_bsc_skipped(self):
        assert ARKHAM_CHAIN_MAP["bsc"] is None

    def test_avalanche_skipped(self):
        assert ARKHAM_CHAIN_MAP["avalanche"] is None

    def test_optimism_skipped(self):
        assert ARKHAM_CHAIN_MAP["optimism"] is None

    def test_dogecoin_skipped(self):
        assert ARKHAM_CHAIN_MAP["dogecoin"] is None

    def test_flare_skipped(self):
        assert ARKHAM_CHAIN_MAP["flare"] is None

    def test_all_mapped_chains_are_strings(self):
        for key, val in ARKHAM_CHAIN_MAP.items():
            assert val is None or isinstance(val, str), f"{key} -> {val!r}"

    def test_no_duplicate_target_chains(self):
        targets = [v for v in ARKHAM_CHAIN_MAP.values() if v is not None]
        assert len(targets) == len(set(targets)), "Duplicate target chain names"

    def test_unknown_chain_not_in_map(self):
        assert "unknown_chain_xyz" not in ARKHAM_CHAIN_MAP
