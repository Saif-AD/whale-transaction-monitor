"""Tests for utils.dedup interpreter wiring.

Verifies that the unified dedup/enrichment path calls the Grok interpreter
per the same threshold rules used in chains/ethereum.py, so that chains
which only route through dedup (BTC/XRP/Polygon/Base/Arbitrum) get the
same audience-facing reasoning coverage as Ethereum and Solana.
"""

from __future__ import annotations

import sys
import time
import types
from unittest.mock import patch, MagicMock

import pytest

from utils.dedup import TransactionDeduplicator


def _install_fake_interpreter_module(generate_fn):
    """Insert a stub `shared.interpreter` module with the given callable.

    The real module fails to import in CI because `openai` is an optional
    runtime dep. The dedup path re-imports on every call, so swapping
    sys.modules is enough.
    """
    fake_mod = types.ModuleType('shared.interpreter')
    fake_mod.generate_interpretation = generate_fn
    sys.modules['shared.interpreter'] = fake_mod
    return fake_mod


def _base_event(**overrides):
    """Build a plausible unique event the deduplicator will accept."""
    event = {
        'blockchain': 'bitcoin',
        'tx_hash': '0xdeadbeef' + str(time.time_ns()),
        'from': 'bc1qfromaddr',
        'to': 'bc1qtoaddr',
        'amount': 100.0,
        'symbol': 'BTC',
        'usd_value': 10_000_000,
        'classification': 'TRANSFER',
        'confidence': 0.9,
        'whale_score': 0.8,
        'reasoning': 'template reasoning text',
    }
    event.update(overrides)
    return event


class _SyncThread:
    """Stand-in for threading.Thread that runs target synchronously.

    Keeps the test deterministic so we can assert on store_transaction
    without waiting on a real background thread.
    """

    def __init__(self, target=None, args=(), kwargs=None, daemon=None):
        self._target = target
        self._args = args
        self._kwargs = kwargs or {}

    def start(self):
        if self._target is not None:
            self._target(*self._args, **self._kwargs)


class TestDedupInterpreterWiring:
    """Covers the four enrichment paths in utils.dedup.handle_event."""

    def teardown_method(self):
        sys.modules.pop('shared.interpreter', None)

    def test_labeled_above_threshold_calls_interpreter(self):
        dedup = TransactionDeduplicator()
        event = _base_event(usd_value=750_000)

        mock_interp = MagicMock(return_value='Grok audience reasoning')
        mock_store = MagicMock()
        mock_labels = MagicMock(return_value=('Binance Hot Wallet', 'Coinbase'))
        _install_fake_interpreter_module(mock_interp)

        with patch('shared.address_labels.lookup_labels', mock_labels), \
             patch('utils.supabase_writer.store_transaction', mock_store), \
             patch('threading.Thread', _SyncThread):
            assert dedup.handle_event(event) is True

        assert mock_interp.called, 'interpreter must run for labeled tx above labeled threshold'
        call_arg = mock_interp.call_args.args[0]
        assert call_arg['transaction_hash'] == event['tx_hash']
        assert call_arg['token_symbol'] == 'BTC'
        assert call_arg['usd_value'] == 750_000
        assert call_arg['blockchain'] == 'bitcoin'
        assert call_arg['from_label'] == 'Binance Hot Wallet'
        assert call_arg['to_label'] == 'Coinbase'
        assert call_arg['classification'] == 'TRANSFER'

        assert mock_store.called
        stored_classification = mock_store.call_args.args[1]
        assert stored_classification['reasoning'] == 'Grok audience reasoning'

    def test_unlabeled_above_threshold_calls_interpreter(self):
        dedup = TransactionDeduplicator()
        event = _base_event(usd_value=3_000_000)

        mock_interp = MagicMock(return_value='Grok reasoning unlabeled')
        mock_store = MagicMock()
        mock_labels = MagicMock(return_value=(None, None))
        _install_fake_interpreter_module(mock_interp)

        with patch('shared.address_labels.lookup_labels', mock_labels), \
             patch('utils.supabase_writer.store_transaction', mock_store), \
             patch('threading.Thread', _SyncThread):
            assert dedup.handle_event(event) is True

        assert mock_interp.called, 'interpreter must run for unlabeled tx above unlabeled threshold'
        assert mock_store.called
        stored_classification = mock_store.call_args.args[1]
        assert stored_classification['reasoning'] == 'Grok reasoning unlabeled'

    def test_labeled_below_threshold_skips_interpreter(self):
        dedup = TransactionDeduplicator()
        event = _base_event(usd_value=100_000, reasoning='template stays')

        mock_interp = MagicMock(return_value='SHOULD NOT BE USED')
        mock_store = MagicMock()
        mock_labels = MagicMock(return_value=('Binance', 'Kraken'))
        _install_fake_interpreter_module(mock_interp)

        with patch('shared.address_labels.lookup_labels', mock_labels), \
             patch('utils.supabase_writer.store_transaction', mock_store), \
             patch('threading.Thread', _SyncThread):
            assert dedup.handle_event(event) is True

        assert not mock_interp.called, 'interpreter must NOT run below labeled threshold'
        assert mock_store.called
        stored_classification = mock_store.call_args.args[1]
        assert stored_classification['reasoning'] == 'template stays'

    def test_interpreter_exception_falls_back_to_template(self):
        dedup = TransactionDeduplicator()
        event = _base_event(usd_value=5_000_000, reasoning='fallback template')

        mock_interp = MagicMock(side_effect=RuntimeError('grok 500'))
        mock_store = MagicMock()
        mock_labels = MagicMock(return_value=('Binance', None))
        _install_fake_interpreter_module(mock_interp)

        with patch('shared.address_labels.lookup_labels', mock_labels), \
             patch('utils.supabase_writer.store_transaction', mock_store), \
             patch('threading.Thread', _SyncThread):
            result = dedup.handle_event(event)

        assert result is True, 'handle_event must not crash on interpreter failure'
        assert mock_interp.called
        assert mock_store.called, 'store_transaction must still run when interpreter fails'
        stored_classification = mock_store.call_args.args[1]
        assert stored_classification['reasoning'] == 'fallback template'
