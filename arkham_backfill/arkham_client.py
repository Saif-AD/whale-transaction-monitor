"""Sync Arkham Intelligence API client with rate limiting and credit guard.

Rate limits:
  /transfers  — 1 req/sec
  all others  — 20 req/sec

Credit budget guard:
  Every response header is checked for x-intel-datapoints-remaining.
  If remaining < CREDIT_FLOOR (default 500), the client raises
  CreditBudgetExhausted so the caller can abort gracefully.
"""

from __future__ import annotations

import logging
import os
import random
import time
from typing import Any, Dict, List, Optional

import httpx

from arkham_backfill.constants import CREDIT_FLOOR

logger = logging.getLogger(__name__)

ARKHAM_BASE_URL = "https://api.arkhamintelligence.com"


class CreditBudgetExhausted(Exception):
    """Raised when Arkham credit balance drops below the safety floor."""

    def __init__(self, remaining: int):
        self.remaining = remaining
        super().__init__(
            f"Arkham credits below floor: {remaining} remaining (floor={CREDIT_FLOOR})"
        )


class ArkhamRateLimitError(Exception):
    """Raised when the API returns 429 and all retries are exhausted."""


class ArkhamClient:
    """Synchronous Arkham API client."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        credit_floor: int = CREDIT_FLOOR,
        max_retries: int = 3,
    ):
        self._api_key = api_key or os.getenv("ARKHAM_API_KEY", "")
        self._credit_floor = credit_floor
        self._max_retries = max_retries
        self._last_transfer_call: float = 0.0
        self._last_other_call: float = 0.0
        self._http = httpx.Client(
            base_url=ARKHAM_BASE_URL,
            headers={"API-Key": self._api_key},
            timeout=30.0,
        )
        self.last_credits_remaining: Optional[int] = None

    def close(self) -> None:
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _wait_for_rate_limit(self, is_transfer: bool) -> None:
        now = time.monotonic()
        if is_transfer:
            min_interval = 1.0
            elapsed = now - self._last_transfer_call
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            self._last_transfer_call = time.monotonic()
        else:
            min_interval = 0.05  # 20 req/sec
            elapsed = now - self._last_other_call
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            self._last_other_call = time.monotonic()

    # ------------------------------------------------------------------
    # Credit guard
    # ------------------------------------------------------------------

    def _check_credits(self, response: httpx.Response) -> None:
        raw = response.headers.get("x-intel-datapoints-remaining")
        if raw is not None:
            try:
                remaining = int(raw)
            except (ValueError, TypeError):
                return
            self.last_credits_remaining = remaining
            if remaining < self._credit_floor:
                raise CreditBudgetExhausted(remaining)

    # ------------------------------------------------------------------
    # Core request with retry + backoff
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        is_transfer: bool = False,
    ) -> httpx.Response:
        last_exc: Optional[Exception] = None
        for attempt in range(self._max_retries + 1):
            self._wait_for_rate_limit(is_transfer)
            try:
                resp = self._http.request(method, path, params=params)
            except httpx.TransportError as e:
                last_exc = e
                self._backoff(attempt)
                continue

            self._check_credits(resp)

            if resp.status_code == 429:
                logger.warning(
                    "Arkham 429 on %s (attempt %d/%d)",
                    path, attempt + 1, self._max_retries + 1,
                )
                last_exc = ArkhamRateLimitError(f"429 on {path}")
                self._backoff(attempt)
                continue

            resp.raise_for_status()
            return resp

        raise last_exc or ArkhamRateLimitError(f"Exhausted retries for {path}")

    @staticmethod
    def _backoff(attempt: int) -> None:
        base = min(2 ** attempt, 30)
        jitter = random.uniform(0, base * 0.5)
        time.sleep(base + jitter)

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    def get_entity(self, slug: str) -> Dict[str, Any]:
        """Fetch entity metadata."""
        resp = self._request("GET", f"/intelligence/entity/{slug}")
        return resp.json()

    def get_entity_transfers(
        self,
        slug: str,
        *,
        limit: int = 50,
        min_usd: int = 0,
    ) -> List[Dict[str, Any]]:
        """Fetch transfers for an entity (transfer mining).

        Calls GET /transfers?base={slug}&limit={limit}&flow=all[&usdGte={min_usd}].
        Returns the raw list of transfer objects.
        """
        params: Dict[str, Any] = {
            "base": slug,
            "limit": limit,
            "flow": "all",
        }
        if min_usd > 0:
            params["usdGte"] = min_usd

        resp = self._request("GET", "/transfers", params=params, is_transfer=True)
        data = resp.json()
        if isinstance(data, dict):
            return data.get("transfers") or []
        return data or []

    def extract_addresses(
        self, transfers: List[Dict[str, Any]], entity_slug: str
    ) -> List[Dict[str, Any]]:
        """Extract unique addresses attributed to an entity from transfer data.

        Returns a list of dicts with keys:
          address, chain, entity_name, label, entity_type
        Deduplicates on (address, chain).
        """
        seen: set[tuple[str, str]] = set()
        results: list[Dict[str, Any]] = []

        for tx in (transfers or []):
            for side in ("fromAddress", "toAddress"):
                addr_obj = tx.get(side)
                if not addr_obj or not isinstance(addr_obj, dict):
                    continue

                raw_addr = addr_obj.get("address", "")
                chain = addr_obj.get("chain", "")
                if not raw_addr or not chain:
                    continue

                entity = addr_obj.get("arkhamEntity") or {}
                entity_id = entity.get("id", "")
                if entity_id != entity_slug:
                    continue

                dedup_key = (raw_addr, chain)
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)

                ark_label = addr_obj.get("arkhamLabel") or {}
                results.append({
                    "address": raw_addr,
                    "chain": chain,
                    "entity_name": entity.get("name", ""),
                    "entity_type": entity.get("type", ""),
                    "label": ark_label.get("name", ""),
                })

        return results
