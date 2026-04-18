"""Tests for CEX-to-CEX filter logic."""

from __future__ import annotations

import pytest

from whale_poster.formatter import is_cex_to_cex, _is_cex_label


class TestIsCexLabel:

    def test_binance(self):
        assert _is_cex_label("Binance") is True

    def test_binance_hot_wallet(self):
        assert _is_cex_label("Binance Hot Wallet") is True

    def test_coinbase_lowercase(self):
        assert _is_cex_label("coinbase prime") is True

    def test_kraken(self):
        assert _is_cex_label("Kraken") is True

    def test_okx(self):
        assert _is_cex_label("OKX Deposit") is True

    def test_bybit(self):
        assert _is_cex_label("Bybit") is True

    def test_bitfinex(self):
        assert _is_cex_label("Bitfinex Cold") is True

    def test_kucoin(self):
        assert _is_cex_label("KuCoin") is True

    def test_gate(self):
        assert _is_cex_label("Gate.io") is True

    def test_htx(self):
        assert _is_cex_label("HTX Exchange") is True

    def test_crypto_com(self):
        assert _is_cex_label("Crypto.com") is True

    def test_gemini(self):
        assert _is_cex_label("Gemini Custody") is True

    def test_bitstamp(self):
        assert _is_cex_label("Bitstamp") is True

    def test_non_cex_fund(self):
        assert _is_cex_label("Jump Crypto") is False

    def test_non_cex_bridge(self):
        assert _is_cex_label("Arbitrum Bridge") is False

    def test_empty_label(self):
        assert _is_cex_label("") is False

    def test_none_label(self):
        assert _is_cex_label(None) is False


class TestIsCexToCex:

    def test_both_cex(self):
        assert is_cex_to_cex("Binance Hot Wallet", "Coinbase Prime") is True

    def test_only_from_cex(self):
        assert is_cex_to_cex("Binance", "Jump Crypto") is False

    def test_only_to_cex(self):
        assert is_cex_to_cex("Jump Crypto", "Binance") is False

    def test_neither_cex(self):
        assert is_cex_to_cex("Jump Crypto", "Wintermute") is False

    def test_both_empty(self):
        assert is_cex_to_cex("", "") is False

    def test_case_insensitive(self):
        assert is_cex_to_cex("BINANCE", "KRAKEN") is True
