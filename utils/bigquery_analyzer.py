import os
import time
from datetime import datetime, timedelta, date
from google.cloud import bigquery
from config.api_keys import GCP_PROJECT_ID
from config.settings import TEST_MODE
import logging
from typing import Dict, Any, Optional, List, Tuple

logger = logging.getLogger(__name__)

CACHE_TTL_SECONDS = 6 * 3600  # 6 hours
NEGATIVE_CACHE_TTL_SECONDS = 6 * 3600
DAILY_QUERY_BUDGET = 10
DUST_THRESHOLD_ETH = 0.1
QUICK_REJECT_VOLUME_ETH = 10


class BigQueryAnalyzer:
    """
    Handles historical analysis of Ethereum addresses using Google BigQuery.

    Includes caching, daily query budgets, dust filtering, and quota management.
    """
    def __init__(self):
        if TEST_MODE:
            logger.info("TEST_MODE: BigQuery initialization skipped")
            self.client = None
        else:
            self.client = self._initialize_client()

        self.quota_exhausted = False
        self.quota_exhausted_time = None
        self.quota_reset_check_interval = 3600

        self._cache: Dict[str, Dict[str, Any]] = {}
        self._negative_cache: Dict[str, float] = {}
        self._daily_query_count = 0
        self._query_count_date: Optional[date] = None

    def _initialize_client(self) -> Optional[bigquery.Client]:
        try:
            from google.oauth2 import service_account
            from config.api_keys import GOOGLE_APPLICATION_CREDENTIALS
            import os

            if not GOOGLE_APPLICATION_CREDENTIALS or not os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
                logger.warning(f"BigQuery credentials file not found: {GOOGLE_APPLICATION_CREDENTIALS}")
                logger.info("BigQuery features will be disabled.")
                return None

            credentials = service_account.Credentials.from_service_account_file(
                GOOGLE_APPLICATION_CREDENTIALS,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )

            project_id = credentials.project_id or GCP_PROJECT_ID
            client = bigquery.Client(credentials=credentials, project=project_id)

            logger.info(f"BigQuery client initialized for project: {project_id}")

            try:
                test_query = "SELECT 1 as test_value"
                job_config = bigquery.QueryJobConfig()
                job_config.job_timeout_ms = 5000
                query_job = client.query(test_query, job_config=job_config)
                list(query_job.result(timeout=5.0))
                logger.info("BigQuery client connection verified")
            except Exception as timeout_error:
                logger.warning(f"BigQuery connection test failed: {timeout_error}")

            return client

        except Exception as e:
            error_msg = str(e)
            if "403" in error_msg or "Access Denied" in error_msg:
                logger.warning("BigQuery 403 Access Denied — service account needs 'BigQuery Job User' role")
            else:
                logger.warning(f"BigQuery initialization failed: {e}")
            logger.info("BigQuery features will be disabled.")
            return None

    # --- Cache helpers ---

    def _get_cached(self, address: str) -> Optional[Dict[str, Any]]:
        key = address.lower()
        if key in self._negative_cache:
            ts = self._negative_cache[key]
            if time.time() - ts < NEGATIVE_CACHE_TTL_SECONDS:
                return {"_negative": True}
            del self._negative_cache[key]

        if key in self._cache:
            entry = self._cache[key]
            if time.time() - entry["_cached_at"] < CACHE_TTL_SECONDS:
                return entry
            del self._cache[key]

        return None

    def _put_cache(self, address: str, result: Dict[str, Any]) -> None:
        key = address.lower()
        result["_cached_at"] = time.time()
        self._cache[key] = result

    def _put_negative_cache(self, address: str) -> None:
        self._negative_cache[address.lower()] = time.time()

    # --- Budget helpers ---

    def _budget_available(self) -> bool:
        today = date.today()
        if self._query_count_date != today:
            self._daily_query_count = 0
            self._query_count_date = today
        return self._daily_query_count < DAILY_QUERY_BUDGET

    def _record_query(self) -> None:
        today = date.today()
        if self._query_count_date != today:
            self._daily_query_count = 0
            self._query_count_date = today
        self._daily_query_count += 1
        logger.debug(f"BigQuery daily budget: {self._daily_query_count}/{DAILY_QUERY_BUDGET}")

    # --- Quota management ---

    def _check_quota_status(self) -> bool:
        if not self.quota_exhausted:
            return True
        if self.quota_exhausted_time:
            if (datetime.now() - self.quota_exhausted_time).total_seconds() >= self.quota_reset_check_interval:
                logger.info("BigQuery quota reset check — restoring service")
                self.quota_exhausted = False
                self.quota_exhausted_time = None
                return True
        return False

    def _handle_quota_exhaustion(self, error_msg: str) -> None:
        if not self.quota_exhausted:
            logger.warning("BigQuery quota exhausted — entering fallback mode (retry in 1h)")
        self.quota_exhausted = True
        self.quota_exhausted_time = datetime.now()
        logger.warning(f"BigQuery quota details: {error_msg}")

    def _is_quota_error(self, error_msg: str) -> bool:
        quota_indicators = [
            'quota exceeded', 'quotaExceeded', 'free query bytes scanned',
            'billing quota', 'exceeds quota', 'limit exceeded',
        ]
        error_lower = error_msg.lower()
        return any(ind in error_lower for ind in quota_indicators)

    # --- Public API (signatures kept for classification_final.py) ---

    def analyze_address_whale_patterns(self, address: str) -> Optional[Dict[str, Any]]:
        """
        Analyze whale patterns for an address using BigQuery historical data.

        Returns a dict with whale tier, confidence, signals etc., or None.
        Signature must stay stable — called by classification_final.py Phase 8.
        """
        if not self.client:
            return None

        cached = self._get_cached(address)
        if cached is not None:
            if cached.get("_negative"):
                logger.debug(f"Negative cache hit for {address}")
                return None
            logger.debug(f"Cache hit for {address}")
            return cached

        if not self._check_quota_status():
            logger.debug(f"BigQuery quota exhausted — skipping {address}")
            return None

        if not self._budget_available():
            logger.debug(f"Daily query budget exhausted ({DAILY_QUERY_BUDGET}) — skipping {address}")
            return None

        try:
            address = address.lower()

            historical_stats = self.get_address_historical_stats(address)
            if not historical_stats:
                self._put_negative_cache(address)
                return None

            total_eth_volume = float(historical_stats.get('total_eth_volume') or 0)
            max_eth_in_tx = float(historical_stats.get('max_eth_in_tx') or 0)
            total_transactions = int(historical_stats.get('total_transactions') or 0)
            active_days = int(historical_stats.get('active_days') or 0)
            unique_counterparties = int(historical_stats.get('unique_counterparties') or 0)

            if total_eth_volume < QUICK_REJECT_VOLUME_ETH:
                logger.debug(f"Quick reject {address}: {total_eth_volume:.1f} ETH < {QUICK_REJECT_VOLUME_ETH}")
                self._put_negative_cache(address)
                return None

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

            analysis_result = {
                'whale_tier': whale_tier,
                'whale_confidence': whale_confidence,
                'whale_signals': whale_signals,
                'historical_stats': historical_stats,
                'analysis_summary': {
                    'total_volume_eth': total_eth_volume,
                    'max_single_tx_eth': max_eth_in_tx,
                    'activity_score': min(100, (active_days * total_transactions) / 10),
                    'network_reach': unique_counterparties,
                },
                'is_whale': whale_confidence >= 0.50,
                'classification_method': 'bigquery_historical_analysis',
            }

            self._put_cache(address, analysis_result)
            logger.debug(f"BigQuery whale analysis for {address}: {whale_tier} (confidence: {whale_confidence:.2f})")
            return analysis_result

        except Exception as e:
            logger.warning(f"BigQuery whale pattern analysis failed for {address}: {e}")
            return None

    def get_address_historical_stats(self, address: str) -> Optional[Dict[str, Any]]:
        """
        Query BigQuery for historical transaction stats with dust filtering.
        """
        if not self.client:
            return None

        if not self._check_quota_status():
            return None

        if not self._budget_available():
            logger.debug(f"Daily query budget exhausted — skipping historical stats for {address}")
            return None

        address = address.lower()
        query = """
            SELECT
                COUNT(*) AS total_transactions,
                COUNT(DISTINCT DATE(block_timestamp)) as active_days,
                SUM(value / POW(10, 18)) AS total_eth_volume,
                AVG(value / POW(10, 18)) AS avg_eth_per_tx,
                MAX(value / POW(10, 18)) AS max_eth_in_tx,
                COUNT(DISTINCT to_address) as unique_counterparties
            FROM
                `bigquery-public-data.crypto_ethereum.transactions`
            WHERE
                (from_address = @address OR to_address = @address)
                AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                AND value / POW(10, 18) >= @dust_threshold
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("address", "STRING", address),
                bigquery.ScalarQueryParameter("dust_threshold", "FLOAT64", DUST_THRESHOLD_ETH),
            ]
        )

        try:
            logger.debug(f"Running BigQuery historical analysis for {address}")
            self._record_query()
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()

            for row in results:
                return {key: value for key, value in row.items()}
            return None

        except Exception as e:
            error_msg = str(e)
            if self._is_quota_error(error_msg):
                self._handle_quota_exhaustion(error_msg)
                return None
            logger.error(f"BigQuery query failed for {address}: {e}")
            return None

    def get_whale_addresses_by_volume(self, min_volume_eth: float = 1000) -> Optional[List[Dict[str, Any]]]:
        """
        Find addresses with high transaction volumes (used by discovery scripts).
        """
        if not self.client:
            return None

        query = """
            WITH address_volumes AS (
                SELECT
                    from_address as address,
                    SUM(value / POW(10, 18)) AS total_volume_eth
                FROM
                    `bigquery-public-data.crypto_ethereum.transactions`
                WHERE
                    block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                    AND value / POW(10, 18) >= @dust_threshold
                GROUP BY from_address
                HAVING total_volume_eth >= @min_volume

                UNION ALL

                SELECT
                    to_address as address,
                    SUM(value / POW(10, 18)) AS total_volume_eth
                FROM
                    `bigquery-public-data.crypto_ethereum.transactions`
                WHERE
                    block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                    AND value / POW(10, 18) >= @dust_threshold
                GROUP BY to_address
                HAVING total_volume_eth >= @min_volume
            )
            SELECT
                address,
                SUM(total_volume_eth) as combined_volume
            FROM address_volumes
            GROUP BY address
            ORDER BY combined_volume DESC
            LIMIT 1000
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("min_volume", "FLOAT", min_volume_eth),
                bigquery.ScalarQueryParameter("dust_threshold", "FLOAT64", DUST_THRESHOLD_ETH),
            ]
        )

        try:
            logger.info(f"Running BigQuery whale discovery (min volume: {min_volume_eth} ETH)")
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()

            whale_addresses = []
            for row in results:
                whale_addresses.append({
                    'address': row['address'],
                    'volume_eth': row['combined_volume'],
                })

            logger.info(f"Found {len(whale_addresses)} potential whale addresses")
            return whale_addresses

        except Exception as e:
            logger.error(f"BigQuery whale discovery query failed: {e}")
            return None


bigquery_analyzer = BigQueryAnalyzer()
