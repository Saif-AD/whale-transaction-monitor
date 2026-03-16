import os
import time
from datetime import datetime, timedelta
from google.cloud import bigquery
from config.api_keys import GCP_PROJECT_ID
from config.settings import TEST_MODE
import logging
from typing import Dict, Any, Optional, List, Tuple

logger = logging.getLogger(__name__)

# Minimum ETH value per transaction to count (filters dust/spam)
MIN_TX_ETH = 0.1

# Cache TTL: don't re-query the same address within this window
CACHE_TTL_SECONDS = 3600 * 6  # 6 hours

# Daily query budget (free tier = 1TB/month, ~33GB/day)
# Each address query scans ~2-5GB, so limit to ~10 queries/day to be safe
DAILY_QUERY_LIMIT = 10


class BigQueryAnalyzer:
    """
    On-demand BigQuery lookups for unknown addresses during live classification.

    Only fires when the classifier can't identify an address after all other
    phases (hardcoded lists, Supabase, Zerion, Moralis). Queries BigQuery's
    public Ethereum transaction history to check if the address has whale-like
    patterns (high volume, large single txs, many counterparties).

    Includes:
    - In-memory result cache (avoid repeat queries for same address)
    - Negative cache (remember addresses that aren't whales)
    - Daily query budget (protect free tier quota)
    - Minimum tx value filter (ignore dust transactions in the SQL)
    """
    def __init__(self):
        if TEST_MODE:
            logger.info("TEST_MODE: BigQuery initialization skipped")
            self.client = None
        else:
            self.client = self._initialize_client()

        # Quota management
        self.quota_exhausted = False
        self.quota_exhausted_time = None
        self.quota_reset_check_interval = 3600  # 1 hour

        # Daily query budget
        self.daily_query_count = 0
        self.last_query_date = None

        # Result cache: address -> (result_dict_or_None, timestamp)
        self._cache: Dict[str, Tuple[Optional[Dict], float]] = {}

    def _initialize_client(self) -> Optional[bigquery.Client]:
        try:
            from google.oauth2 import service_account
            from config.api_keys import GOOGLE_APPLICATION_CREDENTIALS

            if not GOOGLE_APPLICATION_CREDENTIALS or not os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
                logger.warning(f"BigQuery credentials file not found: {GOOGLE_APPLICATION_CREDENTIALS}")
                return None

            credentials = service_account.Credentials.from_service_account_file(
                GOOGLE_APPLICATION_CREDENTIALS,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )

            project_id = credentials.project_id or GCP_PROJECT_ID
            client = bigquery.Client(credentials=credentials, project=project_id)

            logger.info(f"BigQuery client initialized for project: {project_id}")

            # Quick connection test
            try:
                job_config = bigquery.QueryJobConfig()
                job_config.job_timeout_ms = 5000
                query_job = client.query("SELECT 1 as test", job_config=job_config)
                list(query_job.result(timeout=5.0))
                logger.info("BigQuery connection verified")
            except Exception as e:
                logger.warning(f"BigQuery connection test failed: {e}")

            return client

        except Exception as e:
            error_msg = str(e)
            if "403" in error_msg or "Access Denied" in error_msg:
                logger.warning("BigQuery 403 - service account needs 'BigQuery Job User' role")
            else:
                logger.warning(f"BigQuery initialization failed: {e}")
            return None

    # --- Cache ---

    def _get_cached(self, address: str) -> Tuple[bool, Optional[Dict]]:
        """Check cache. Returns (hit, result). Result may be None (negative cache)."""
        entry = self._cache.get(address)
        if entry is None:
            return False, None
        result, cached_at = entry
        if time.time() - cached_at > CACHE_TTL_SECONDS:
            del self._cache[address]
            return False, None
        return True, result

    def _set_cached(self, address: str, result: Optional[Dict]):
        self._cache[address] = (result, time.time())

    # --- Quota ---

    def _check_quota_status(self) -> bool:
        if not self.quota_exhausted:
            return True
        if self.quota_exhausted_time:
            elapsed = (datetime.now() - self.quota_exhausted_time).total_seconds()
            if elapsed >= self.quota_reset_check_interval:
                logger.info("BigQuery quota reset check - restoring service")
                self.quota_exhausted = False
                self.quota_exhausted_time = None
                return True
        return False

    def _check_daily_budget(self) -> bool:
        today = datetime.now().date()
        if self.last_query_date != today:
            self.daily_query_count = 0
            self.last_query_date = today
        if self.daily_query_count >= DAILY_QUERY_LIMIT:
            logger.debug(f"BigQuery daily budget exhausted ({DAILY_QUERY_LIMIT} queries)")
            return False
        return True

    def _handle_quota_exhaustion(self, error_msg: str):
        if not self.quota_exhausted:
            logger.warning(f"BigQuery quota exhausted: {error_msg}")
        self.quota_exhausted = True
        self.quota_exhausted_time = datetime.now()

    def _is_quota_error(self, error_msg: str) -> bool:
        indicators = ['quota exceeded', 'quotaexceeded', 'free query bytes',
                       'billing quota', 'exceeds quota', 'limit exceeded']
        return any(i in error_msg.lower() for i in indicators)

    # --- Core analysis ---

    def analyze_address_whale_patterns(self, address: str) -> Optional[Dict[str, Any]]:
        """
        Analyze whale patterns for an address. Returns classification dict or None.

        Guards:
        1. Client must be initialized
        2. Check in-memory cache first (avoid repeat BigQuery calls)
        3. Check daily query budget
        4. Check quota status
        """
        if not self.client:
            return None

        address = address.lower()

        # Check cache (includes negative results)
        hit, cached_result = self._get_cached(address)
        if hit:
            logger.debug(f"BigQuery cache hit for {address}: {'whale' if cached_result else 'not whale'}")
            return cached_result

        # Check daily budget
        if not self._check_daily_budget():
            return None

        # Check quota
        if not self._check_quota_status():
            return None

        try:
            historical_stats = self._query_historical_stats(address)
            if not historical_stats:
                # Negative cache: remember this address is not interesting
                self._set_cached(address, None)
                return None

            total_eth_volume = float(historical_stats.get('total_eth_volume') or 0)
            max_eth_in_tx = float(historical_stats.get('max_eth_in_tx') or 0)
            total_transactions = int(historical_stats.get('total_transactions') or 0)
            active_days = int(historical_stats.get('active_days') or 0)
            unique_counterparties = int(historical_stats.get('unique_counterparties') or 0)

            # Quick filter: if total volume < 10 ETH, not a whale
            if total_eth_volume < 10:
                self._set_cached(address, None)
                return None

            # Whale classification
            whale_tier = "UNKNOWN"
            whale_confidence = 0.0
            whale_signals = []

            if total_eth_volume >= 10000:
                whale_tier = "MEGA_WHALE"
                whale_confidence = 0.95
                whale_signals.append("MEGA_VOLUME_WHALE")
            elif total_eth_volume >= 1000:
                whale_tier = "ULTRA_WHALE"
                whale_confidence = 0.85
                whale_signals.append("ULTRA_VOLUME_WHALE")
            elif total_eth_volume >= 100:
                whale_tier = "WHALE"
                whale_confidence = 0.70
                whale_signals.append("HIGH_VOLUME_WHALE")
            elif total_eth_volume >= 10:
                whale_tier = "MINI_WHALE"
                whale_confidence = 0.50
                whale_signals.append("MODERATE_VOLUME")

            if max_eth_in_tx >= 1000:
                whale_signals.append("MEGA_SINGLE_TX")
                whale_confidence = min(0.95, whale_confidence + 0.15)
            elif max_eth_in_tx >= 100:
                whale_signals.append("LARGE_SINGLE_TX")
                whale_confidence = min(0.90, whale_confidence + 0.10)

            if total_transactions >= 1000:
                whale_signals.append("HIGH_FREQUENCY_TRADER")
                whale_confidence = min(0.90, whale_confidence + 0.05)

            if active_days >= 100:
                whale_signals.append("PERSISTENT_ACTOR")
                whale_confidence = min(0.90, whale_confidence + 0.05)

            if unique_counterparties >= 100:
                whale_signals.append("PROTOCOL_INTERACTOR")
                whale_confidence = min(0.90, whale_confidence + 0.05)

            result = {
                'whale_tier': whale_tier,
                'whale_confidence': whale_confidence,
                'whale_signals': whale_signals,
                'historical_stats': historical_stats,
                'analysis_summary': {
                    'total_volume_eth': total_eth_volume,
                    'max_single_tx_eth': max_eth_in_tx,
                    'activity_score': min(100, (active_days * total_transactions) / 10),
                    'network_reach': unique_counterparties
                },
                'is_whale': whale_confidence >= 0.50,
                'classification_method': 'bigquery_historical_analysis'
            }

            logger.debug(f"BigQuery whale analysis for {address}: {whale_tier} "
                         f"(confidence: {whale_confidence:.2f})")

            # Cache the positive result
            self._set_cached(address, result)
            return result

        except Exception as e:
            logger.warning(f"BigQuery whale pattern analysis failed for {address}: {e}")
            return None

    def _query_historical_stats(self, address: str) -> Optional[Dict[str, Any]]:
        """
        Query BigQuery for address transaction stats.

        Filters out dust transactions (< 0.1 ETH) in the SQL itself to:
        - Reduce data scanned (cheaper)
        - Avoid classifying dust-spammed addresses as "high activity"
        """
        if not self.client:
            return None

        if not self._check_quota_status():
            return None

        query = f"""
            SELECT
                COUNT(*) AS total_transactions,
                COUNT(DISTINCT DATE(block_timestamp)) AS active_days,
                SUM(value / POW(10, 18)) AS total_eth_volume,
                AVG(value / POW(10, 18)) AS avg_eth_per_tx,
                MAX(value / POW(10, 18)) AS max_eth_in_tx,
                COUNT(DISTINCT to_address) AS unique_counterparties
            FROM
                `bigquery-public-data.crypto_ethereum.transactions`
            WHERE
                (from_address = @address OR to_address = @address)
                AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                AND value / POW(10, 18) >= {MIN_TX_ETH}
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("address", "STRING", address),
            ]
        )

        try:
            self.daily_query_count += 1
            logger.debug(f"BigQuery query #{self.daily_query_count}/{DAILY_QUERY_LIMIT} "
                         f"for {address}")

            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()

            for row in results:
                result_dict = {key: value for key, value in row.items()}
                # If zero transactions matched the filter, treat as empty
                if int(result_dict.get('total_transactions') or 0) == 0:
                    return None
                return result_dict
            return None

        except Exception as e:
            error_msg = str(e)
            if self._is_quota_error(error_msg):
                self._handle_quota_exhaustion(error_msg)
                return None
            logger.error(f"BigQuery query failed for {address}: {e}")
            return None

    def get_address_historical_stats(self, address: str) -> Optional[Dict[str, Any]]:
        """Backward-compatible wrapper for _query_historical_stats."""
        return self._query_historical_stats(address.lower())


# Global instance
bigquery_analyzer = BigQueryAnalyzer()
