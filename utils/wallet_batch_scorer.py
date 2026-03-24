"""Wallet Batch Scorer - Background job that refreshes wallet_profiles in batches.

Runs on startup and every 6 hours. Queries distinct whale addresses from all
chain transaction tables and builds/refreshes their profiles.
"""

import logging
import threading
import time
from typing import Set

logger = logging.getLogger(__name__)

_BATCH_INTERVAL = 6 * 3600  # 6 hours
_BATCH_SIZE = 50


class WalletBatchScorer:
    """Background batch scoring for all known whale addresses."""

    def __init__(self):
        self._timer = None
        self._running = False
        self._lock = threading.Lock()

    def start(self):
        """Start the background scorer (runs immediately, then every 6 hours)."""
        if self._running:
            return
        self._running = True
        thread = threading.Thread(target=self._run_cycle, daemon=True, name='WalletBatchScorer')
        thread.start()
        logger.info("Wallet batch scorer started")

    def stop(self):
        self._running = False
        if self._timer:
            self._timer.cancel()

    def _run_cycle(self):
        """Run one scoring cycle, then schedule next."""
        try:
            self._score_all()
        except Exception as e:
            logger.error(f"Batch scoring cycle failed: {e}")

        if self._running:
            self._timer = threading.Timer(_BATCH_INTERVAL, self._run_cycle)
            self._timer.daemon = True
            self._timer.start()

    def _score_all(self):
        """Query all distinct whale addresses and refresh their profiles."""
        try:
            from supabase import create_client
            from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
            client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        except Exception as e:
            logger.error(f"Cannot init Supabase for batch scoring: {e}")
            return

        chain_tables = [
            'ethereum_transactions', 'bitcoin_transactions',
            'solana_transactions', 'polygon_transactions', 'xrp_transactions',
            'base_transactions', 'arbitrum_transactions',
        ]

        addresses: Set[str] = set()
        for table in chain_tables:
            try:
                rows = (
                    client.table(table)
                    .select('whale_address')
                    .not_.is_('whale_address', 'null')
                    .limit(2000)
                    .execute()
                ).data or []
                for r in rows:
                    addr = r.get('whale_address', '').lower()
                    if addr:
                        addresses.add(addr)
            except Exception as e:
                logger.warning(f"Failed to query {table} for whale addresses: {e}")

        logger.info(f"Batch scoring {len(addresses)} unique whale addresses")

        from utils.wallet_profiler import wallet_profiler

        processed = 0
        for addr in addresses:
            if not self._running:
                break
            try:
                wallet_profiler.get_profile(addr)
                processed += 1
                if processed % _BATCH_SIZE == 0:
                    logger.info(f"Batch scored {processed}/{len(addresses)} wallets")
                    time.sleep(1)  # Rate limit courtesy
            except Exception as e:
                logger.warning(f"Failed to score {addr}: {e}")

        logger.info(f"Batch scoring complete: {processed}/{len(addresses)} wallets")


# Global instance
batch_scorer = WalletBatchScorer()
