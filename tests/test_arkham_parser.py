"""Tests for Arkham transfer parsing and address extraction."""

from __future__ import annotations

import pytest

from arkham_backfill.arkham_client import ArkhamClient


def _make_transfer(
    from_addr: str = "0xAAA",
    from_chain: str = "ethereum",
    from_entity_id: str = "binance",
    from_entity_name: str = "Binance",
    from_entity_type: str = "cex",
    from_label: str = "Hot Wallet",
    to_addr: str = "0xBBB",
    to_chain: str = "ethereum",
    to_entity_id: str = "",
    to_entity_name: str = "",
    to_entity_type: str = "",
    to_label: str = "",
) -> dict:
    """Build a realistic mocked Arkham transfer object."""
    tx: dict = {"fromAddress": {}, "toAddress": {}}

    tx["fromAddress"] = {
        "address": from_addr,
        "chain": from_chain,
        "arkhamEntity": {"id": from_entity_id, "name": from_entity_name, "type": from_entity_type},
        "arkhamLabel": {"name": from_label},
    }
    tx["toAddress"] = {
        "address": to_addr,
        "chain": to_chain,
    }
    if to_entity_id:
        tx["toAddress"]["arkhamEntity"] = {
            "id": to_entity_id, "name": to_entity_name, "type": to_entity_type,
        }
    if to_label:
        tx["toAddress"]["arkhamLabel"] = {"name": to_label}

    return tx


class TestExtractAddresses:

    def test_extracts_from_side(self):
        client = ArkhamClient(api_key="test")
        transfers = [_make_transfer(from_entity_id="binance")]
        result = client.extract_addresses(transfers, "binance")
        assert len(result) == 1
        assert result[0]["address"] == "0xAAA"
        assert result[0]["chain"] == "ethereum"
        assert result[0]["entity_name"] == "Binance"
        assert result[0]["label"] == "Hot Wallet"

    def test_extracts_to_side(self):
        client = ArkhamClient(api_key="test")
        transfers = [_make_transfer(
            from_entity_id="unknown",
            to_entity_id="binance",
            to_entity_name="Binance",
            to_entity_type="cex",
            to_label="Cold Wallet",
        )]
        result = client.extract_addresses(transfers, "binance")
        assert len(result) == 1
        assert result[0]["address"] == "0xBBB"
        assert result[0]["label"] == "Cold Wallet"

    def test_extracts_both_sides(self):
        client = ArkhamClient(api_key="test")
        transfers = [_make_transfer(
            from_entity_id="binance",
            to_entity_id="binance",
            to_entity_name="Binance",
            to_entity_type="cex",
            to_label="Deposit",
        )]
        result = client.extract_addresses(transfers, "binance")
        assert len(result) == 2
        addrs = {r["address"] for r in result}
        assert addrs == {"0xAAA", "0xBBB"}

    def test_deduplicates_same_address_chain(self):
        client = ArkhamClient(api_key="test")
        transfers = [
            _make_transfer(from_addr="0xSAME", from_entity_id="binance"),
            _make_transfer(from_addr="0xSAME", from_entity_id="binance"),
        ]
        result = client.extract_addresses(transfers, "binance")
        assert len(result) == 1

    def test_same_address_different_chains_kept(self):
        client = ArkhamClient(api_key="test")
        transfers = [
            _make_transfer(from_addr="0xSAME", from_chain="ethereum", from_entity_id="binance"),
            _make_transfer(from_addr="0xSAME", from_chain="base", from_entity_id="binance"),
        ]
        result = client.extract_addresses(transfers, "binance")
        assert len(result) == 2

    def test_ignores_other_entities(self):
        client = ArkhamClient(api_key="test")
        transfers = [_make_transfer(from_entity_id="coinbase")]
        result = client.extract_addresses(transfers, "binance")
        assert len(result) == 0

    def test_empty_transfers(self):
        client = ArkhamClient(api_key="test")
        result = client.extract_addresses([], "binance")
        assert result == []

    def test_none_transfers(self):
        """Arkham sometimes returns null for the transfers field (e.g. htx entity)."""
        client = ArkhamClient(api_key="test")
        result = client.extract_addresses(None, "binance")
        assert result == []

    def test_missing_address_field_skipped(self):
        client = ArkhamClient(api_key="test")
        transfers = [{"fromAddress": {"chain": "ethereum"}, "toAddress": None}]
        result = client.extract_addresses(transfers, "binance")
        assert result == []

    def test_no_arkham_entity_skipped(self):
        client = ArkhamClient(api_key="test")
        transfers = [{
            "fromAddress": {"address": "0xABC", "chain": "ethereum"},
            "toAddress": {},
        }]
        result = client.extract_addresses(transfers, "binance")
        assert result == []

    def test_missing_label_defaults_empty(self):
        client = ArkhamClient(api_key="test")
        tx = _make_transfer(from_entity_id="binance")
        del tx["fromAddress"]["arkhamLabel"]
        result = client.extract_addresses([tx], "binance")
        assert len(result) == 1
        assert result[0]["label"] == ""

    def test_multiple_entities_multiple_transfers(self):
        client = ArkhamClient(api_key="test")
        transfers = [
            _make_transfer(from_addr="0x1", from_entity_id="binance"),
            _make_transfer(from_addr="0x2", from_entity_id="binance", from_chain="solana"),
            _make_transfer(from_addr="0x3", from_entity_id="coinbase"),
        ]
        result = client.extract_addresses(transfers, "binance")
        assert len(result) == 2
        addrs = {r["address"] for r in result}
        assert addrs == {"0x1", "0x2"}
