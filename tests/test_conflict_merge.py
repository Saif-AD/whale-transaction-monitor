"""Tests for multi-source upsert conflict resolution in Arkham backfill."""

from __future__ import annotations

import json

import pytest

from arkham_backfill.backfill import build_upsert_row, merge_with_existing
from arkham_backfill.entities import Entity


def _entity(slug="binance", etype="cex", min_usd=1_000_000) -> Entity:
    return Entity(slug=slug, type=etype, min_usd=min_usd)


def _addr_info(
    address="0xAbCdEf",
    chain="ethereum",
    entity_name="Binance",
    entity_type="cex",
    label="Hot Wallet",
) -> dict:
    return {
        "address": address,
        "chain": chain,
        "entity_name": entity_name,
        "entity_type": entity_type,
        "label": label,
    }


class TestBuildUpsertRow:

    def test_basic_row(self):
        row = build_upsert_row(_addr_info(), "ethereum", _entity())
        assert row["address"] == "0xabcdef"  # normalized / lowercased
        assert row["blockchain"] == "ethereum"
        assert row["entity_name"] == "Binance"
        assert row["label"] == "Hot Wallet"
        assert row["detection_method"] == "arkham_transfer_mining"
        assert row["source"] == "arkham_api"
        assert "updated_at" in row

    def test_evm_address_lowercased(self):
        row = build_upsert_row(
            _addr_info(address="0xABCD1234"),
            "ethereum",
            _entity(),
        )
        assert row["address"] == "0xabcd1234"

    def test_solana_address_not_lowercased(self):
        row = build_upsert_row(
            _addr_info(address="JUP6LkbZ"),
            "solana",
            _entity(slug="jump-trading", etype="fund"),
        )
        assert row["address"] == "JUP6LkbZ"

    def test_entity_type_override_historical(self):
        row = build_upsert_row(
            _addr_info(),
            "ethereum",
            _entity(slug="old-entity", etype="historical"),
        )
        assert row["confidence"] == 0.80
        assert row["signal_potential"] == "LOW"

    def test_entity_type_override_individual(self):
        row = build_upsert_row(
            _addr_info(),
            "ethereum",
            _entity(slug="whale-dude", etype="individual"),
        )
        assert row["signal_potential"] == "MEDIUM"
        assert row["confidence"] == 0.95  # default, not overridden

    def test_address_type_mapped(self):
        row = build_upsert_row(
            _addr_info(entity_type="cex"),
            "ethereum",
            _entity(),
        )
        assert row["address_type"] == "exchange"

    def test_unknown_entity_type(self):
        row = build_upsert_row(
            _addr_info(entity_type="something_new"),
            "ethereum",
            _entity(),
        )
        assert row["address_type"] == "unknown"


class TestMergeWithExisting:

    def test_no_existing_returns_new(self):
        new = {"confidence": 0.95, "entity_name": "Binance"}
        result = merge_with_existing(new, None)
        assert result is new

    def test_arkham_wins_entity_name(self):
        new = {"confidence": 0.95, "entity_name": "Binance", "label": "Hot Wallet"}
        existing = {"confidence": 0.90, "entity_name": "Old Name", "analysis_tags": []}
        result = merge_with_existing(new, existing)
        assert result["entity_name"] == "Binance"
        assert result["label"] == "Hot Wallet"

    def test_confidence_keeps_higher_existing(self):
        new = {"confidence": 0.90, "entity_name": "X"}
        existing = {"confidence": 0.99, "analysis_tags": []}
        result = merge_with_existing(new, existing)
        assert result["confidence"] == 0.99

    def test_confidence_takes_higher_new(self):
        new = {"confidence": 0.99, "entity_name": "X"}
        existing = {"confidence": 0.80, "analysis_tags": []}
        result = merge_with_existing(new, existing)
        assert result["confidence"] == 0.99

    def test_confidence_equal_keeps_existing(self):
        new = {"confidence": 0.95, "entity_name": "X"}
        existing = {"confidence": 0.95, "analysis_tags": []}
        result = merge_with_existing(new, existing)
        assert result["confidence"] == 0.95

    def test_analysis_tags_merged(self):
        new = {"confidence": 0.95, "entity_name": "X"}
        existing = {"confidence": 0.90, "analysis_tags": ["etherscan"]}
        result = merge_with_existing(new, existing)
        assert "etherscan" in result["analysis_tags"]
        assert "arkham_api" in result["analysis_tags"]

    def test_analysis_tags_no_duplicate_arkham(self):
        new = {"confidence": 0.95, "entity_name": "X"}
        existing = {"confidence": 0.90, "analysis_tags": ["arkham_api", "etherscan"]}
        result = merge_with_existing(new, existing)
        assert result["analysis_tags"].count("arkham_api") == 1

    def test_analysis_tags_empty_existing(self):
        new = {"confidence": 0.95, "entity_name": "X"}
        existing = {"confidence": 0.90, "analysis_tags": []}
        result = merge_with_existing(new, existing)
        assert result["analysis_tags"] == ["arkham_api"]

    def test_analysis_tags_none_existing(self):
        new = {"confidence": 0.95, "entity_name": "X"}
        existing = {"confidence": 0.90, "analysis_tags": None}
        result = merge_with_existing(new, existing)
        assert "arkham_api" in result["analysis_tags"]

    def test_analysis_tags_json_string_existing(self):
        new = {"confidence": 0.95, "entity_name": "X"}
        existing = {"confidence": 0.90, "analysis_tags": '["manual"]'}
        result = merge_with_existing(new, existing)
        assert "manual" in result["analysis_tags"]
        assert "arkham_api" in result["analysis_tags"]

    def test_confidence_zero_existing(self):
        new = {"confidence": 0.95, "entity_name": "X"}
        existing = {"confidence": 0, "analysis_tags": []}
        result = merge_with_existing(new, existing)
        assert result["confidence"] == 0.95

    def test_analysis_tags_dict_existing(self):
        """Supabase can store analysis_tags as a dict instead of a list."""
        new = {"confidence": 0.95, "entity_name": "X"}
        existing = {"confidence": 0.90, "analysis_tags": {"source": "etherscan"}}
        result = merge_with_existing(new, existing)
        assert isinstance(result["analysis_tags"], list)
        assert {"source": "etherscan"} in result["analysis_tags"]
        assert "arkham_api" in result["analysis_tags"]

    def test_analysis_tags_json_string_dict(self):
        """analysis_tags stored as a JSON string encoding a dict."""
        new = {"confidence": 0.95, "entity_name": "X"}
        existing = {"confidence": 0.90, "analysis_tags": '{"source": "manual"}'}
        result = merge_with_existing(new, existing)
        assert isinstance(result["analysis_tags"], list)
        assert {"source": "manual"} in result["analysis_tags"]
        assert "arkham_api" in result["analysis_tags"]

    def test_analysis_tags_plain_string_not_json(self):
        """analysis_tags stored as a plain non-JSON string."""
        new = {"confidence": 0.95, "entity_name": "X"}
        existing = {"confidence": 0.90, "analysis_tags": "etherscan"}
        result = merge_with_existing(new, existing)
        assert isinstance(result["analysis_tags"], list)
        assert "etherscan" in result["analysis_tags"]
        assert "arkham_api" in result["analysis_tags"]
