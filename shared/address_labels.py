"""Address label lookup with LRU caching.

Resolves from_label / to_label for whale transactions by querying the
Supabase `addresses` table.  Results are cached in-memory with a
configurable TTL so hot wallets don't hammer the database on every tx.

Usage at each chain module's ingest point:

    from shared.address_labels import lookup_labels
    from_label, to_label = lookup_labels(from_addr, to_addr, blockchain)
"""

import logging
import threading
import time
from typing import Tuple, Optional, Dict

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (importable, overridable in tests)
# ---------------------------------------------------------------------------
CACHE_MAX_SIZE: int = 10_000
CACHE_TTL_SECONDS: int = 3600  # 1 hour

# Chains where addresses are case-insensitive (EVM hex).
# All others (Solana base58, Bitcoin bech32/base58check, XRP) are case-sensitive.
_CASE_INSENSITIVE_CHAINS = frozenset({"ethereum", "base", "arbitrum", "polygon"})


# ---------------------------------------------------------------------------
# Address normalization
# ---------------------------------------------------------------------------

def normalize_address(addr: str, blockchain: str) -> str:
    """Normalize an address for consistent DB lookups.

    EVM chains: lowercase (Ethereum, Base, Arbitrum, Polygon).
    Others: left as-is (Solana, Bitcoin, XRP are case-sensitive).

    # TODO(phase-1-arkham): The Arkham backfill MUST call normalize_address()
    # on every address before upserting to the `addresses` table.  If Arkham
    # returns checksummed EVM addresses (0xAbCd...) and we store them without
    # lowercasing, the label join will silently fail.  See shared/address_labels.py.
    """
    if not addr:
        return ""
    if blockchain.lower() in _CASE_INSENSITIVE_CHAINS:
        return addr.lower().strip()
    return addr.strip()


# ---------------------------------------------------------------------------
# Lazy Supabase client (same pattern as supabase_writer.py)
# ---------------------------------------------------------------------------
_supabase_client = None
_client_lock = threading.Lock()


def _get_client():
    """Lazy-init Supabase client (thread-safe)."""
    global _supabase_client
    if _supabase_client is None:
        with _client_lock:
            if _supabase_client is None:
                try:
                    from supabase import create_client
                    from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
                    _supabase_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
                    logger.info("Address-labels Supabase client initialized")
                except Exception as e:
                    logger.error("Failed to init address-labels Supabase client: %s", e)
                    return None
    return _supabase_client


# ---------------------------------------------------------------------------
# LRU cache with TTL
# ---------------------------------------------------------------------------

class _LabelCache:
    """Thread-safe LRU cache with per-entry TTL for address labels.

    Keys:  (normalized_address, blockchain)
    Values: (label_string, insert_timestamp)
    """

    def __init__(self, max_size: int = CACHE_MAX_SIZE, ttl: int = CACHE_TTL_SECONDS):
        self._max_size = max_size
        self._ttl = ttl
        self._store: Dict[Tuple[str, str], Tuple[str, float]] = {}
        self._lock = threading.Lock()

    def get(self, key: Tuple[str, str]) -> Optional[str]:
        """Return cached label or None if miss / expired."""
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            label, ts = entry
            if time.monotonic() - ts > self._ttl:
                del self._store[key]
                return None
            return label

    def put(self, key: Tuple[str, str], label: str) -> None:
        with self._lock:
            if len(self._store) >= self._max_size and key not in self._store:
                oldest_key = next(iter(self._store))
                del self._store[oldest_key]
            self._store[key] = (label, time.monotonic())

    def size(self) -> int:
        with self._lock:
            return len(self._store)


_cache = _LabelCache()


# ---------------------------------------------------------------------------
# Coverage stats (for periodic logging)
# ---------------------------------------------------------------------------

class _Stats:
    def __init__(self):
        self._lock = threading.Lock()
        self.total = 0
        self.cache_hit = 0
        self.db_hit = 0
        self.miss = 0

    def record(self, *, cache_hit: bool = False, db_hit: bool = False, miss: bool = False):
        with self._lock:
            self.total += 1
            if cache_hit:
                self.cache_hit += 1
            elif db_hit:
                self.db_hit += 1
            elif miss:
                self.miss += 1

    def snapshot_and_maybe_log(self) -> None:
        """Log coverage every N lookups (configurable), then reset counters."""
        from shared.config import LABEL_COVERAGE_LOG_INTERVAL
        interval = LABEL_COVERAGE_LOG_INTERVAL
        with self._lock:
            if self.total > 0 and self.total % interval == 0:
                matched = self.cache_hit + self.db_hit
                pct = lambda n: f"{n * 100 / interval:.1f}%"
                logger.info(
                    "Label coverage: %d/%d matched (%s cache hit, %s DB hit, %s miss)",
                    matched,
                    interval,
                    pct(self.cache_hit),
                    pct(self.db_hit),
                    pct(self.miss),
                )
                self.cache_hit = 0
                self.db_hit = 0
                self.miss = 0
                self.total = 0


_stats = _Stats()


# ---------------------------------------------------------------------------
# Core lookup
# ---------------------------------------------------------------------------

def _resolve_label(row: dict) -> str:
    """Pick the best label from an addresses row.

    Prefers `label`; falls back to `entity_name` if label is empty.
    """
    label = (row.get("label") or "").strip()
    if label:
        return label
    return (row.get("entity_name") or "").strip()


def _query_labels(addresses: list[Tuple[str, str]]) -> Dict[Tuple[str, str], str]:
    """Fetch labels for a batch of (normalized_address, blockchain) pairs.

    Uses a single Supabase query with an IN clause to fetch both in one
    round trip.  Returns a dict mapping (addr, chain) -> label_string.
    """
    client = _get_client()
    if client is None:
        return {}

    # Build a unique set of (addr, chain) pairs to query
    unique = list(set(addresses))
    if not unique:
        return {}

    addr_list = [a for a, _ in unique]
    chain_list = list(set(c for _, c in unique))

    try:
        result = (
            client.table("addresses")
            .select("address, blockchain, label, entity_name")
            .in_("address", addr_list)
            .in_("blockchain", chain_list)
            .execute()
        )
    except Exception as e:
        logger.warning("Supabase label query failed: %s", e)
        return {}

    labels: Dict[Tuple[str, str], str] = {}
    for row in result.data or []:
        key = (row["address"], row["blockchain"])
        resolved = _resolve_label(row)
        if resolved:
            labels[key] = resolved
    return labels


def lookup_labels(
    from_address: str,
    to_address: str,
    blockchain: str,
) -> Tuple[str, str]:
    """Look up from_label and to_label for a whale transaction.

    Returns (from_label, to_label).  Empty string when no match — never
    None, never the raw address.

    If Supabase is unreachable, returns ("", "") and logs a warning.
    Never raises — callers must not have their ingest blocked by label
    lookup failures.
    """
    blockchain = blockchain.lower() if blockchain else "unknown"

    from_norm = normalize_address(from_address, blockchain)
    to_norm = normalize_address(to_address, blockchain)

    from_key = (from_norm, blockchain)
    to_key = (to_norm, blockchain)

    # --- Check cache first ---
    from_cached = _cache.get(from_key) if from_norm else None
    to_cached = _cache.get(to_key) if to_norm else None

    from_label = from_cached
    to_label = to_cached

    # Track which addresses still need a DB lookup
    need_query: list[Tuple[str, str]] = []
    if from_norm and from_cached is None:
        need_query.append(from_key)
    if to_norm and to_cached is None and to_key != from_key:
        need_query.append(to_key)

    # --- DB lookup for cache misses ---
    if need_query:
        try:
            db_results = _query_labels(need_query)
        except Exception as e:
            logger.warning("Label lookup failed, proceeding with empty labels: %s", e)
            db_results = {}

        for key in need_query:
            resolved = db_results.get(key, "")
            _cache.put(key, resolved)
            if key == from_key:
                from_label = resolved
            if key == to_key:
                to_label = resolved

    # --- Record stats ---
    for addr_norm, cached_val, final_val, key in [
        (from_norm, from_cached, from_label, from_key),
        (to_norm, to_cached, to_label, to_key),
    ]:
        if not addr_norm:
            continue
        if cached_val is not None:
            _stats.record(cache_hit=True)
        elif final_val:
            _stats.record(db_hit=True)
        else:
            _stats.record(miss=True)

    _stats.snapshot_and_maybe_log()

    return (from_label or "", to_label or "")
