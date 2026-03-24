"""Wallet Profiler - Core profiling, scoring, and tagging for whale wallets.

Aggregates transaction data, enriches with portfolio info, computes smart money
scores, and assigns behavioral tags. Caches results to wallet_profiles table.
"""

import logging
import time
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

# Cache TTL: 1 hour
_PROFILE_TTL_SECONDS = 3600

# Smart money score weights (Phase 3)
_SCORE_WEIGHTS = {
    'profitability': 0.30,
    'timing': 0.20,
    'counterparty_quality': 0.15,
    'volume_consistency': 0.15,
    'token_diversity': 0.10,
    'known_entity': 0.10,
}

# Behavioral tag thresholds
_TAG_THRESHOLDS = {
    'whale_portfolio_usd': 1_000_000,
    'whale_volume_30d_usd': 5_000_000,
    'smart_money_score': 0.70,
    'degen_token_count_30d': 20,
    'accumulator_buy_ratio': 0.80,
    'distributor_sell_ratio': 0.80,
}

# Chain table names (mirrors supabase_writer)
_CHAIN_TABLES = [
    'ethereum_transactions',
    'bitcoin_transactions',
    'solana_transactions',
    'polygon_transactions',
    'xrp_transactions',
    'base_transactions',
    'arbitrum_transactions',
]


class WalletProfiler:
    """Builds and caches comprehensive wallet profiles."""

    def __init__(self):
        self._client = None
        self._client_lock = threading.Lock()
        self._zerion = None
        self._entity_cache = {}

    def _get_client(self):
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    try:
                        from supabase import create_client
                        from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
                        self._client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
                    except Exception as e:
                        logger.error(f"Failed to init Supabase client: {e}")
        return self._client

    def _get_zerion(self):
        if self._zerion is None:
            try:
                from config.api_keys import ZERION_API_KEY
                from utils.zerion_enricher import ZerionEnricher
                self._zerion = ZerionEnricher(ZERION_API_KEY)
            except Exception as e:
                logger.warning(f"Zerion enricher unavailable: {e}")
        return self._zerion

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_profile(self, address: str, chain: str = None) -> Optional[Dict[str, Any]]:
        """Get a wallet profile, returning cached version if fresh enough."""
        address = address.lower()
        client = self._get_client()
        if not client:
            return self._build_profile_fresh(address, chain)

        try:
            row = (
                client.table('wallet_profiles')
                .select('*')
                .eq('address', address)
                .maybe_single()
                .execute()
            ).data
            if row and row.get('updated_at'):
                updated = datetime.fromisoformat(row['updated_at'].replace('Z', '+00:00'))
                age = (datetime.now(timezone.utc) - updated).total_seconds()
                if age < _PROFILE_TTL_SECONDS:
                    return row
        except Exception as e:
            logger.warning(f"Cache lookup failed for {address}: {e}")

        return self._build_profile_fresh(address, chain)

    def get_transaction_history(
        self, address: str, chain: str = None, limit: int = 50, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get paginated transaction history for a wallet from all chain tables."""
        address = address.lower()
        client = self._get_client()
        if not client:
            return []

        all_txs = []
        tables = _CHAIN_TABLES
        if chain:
            table_map = {
                'ethereum': 'ethereum_transactions',
                'bitcoin': 'bitcoin_transactions',
                'solana': 'solana_transactions',
                'polygon': 'polygon_transactions',
                'xrp': 'xrp_transactions',
                'base': 'base_transactions',
                'arbitrum': 'arbitrum_transactions',
            }
            t = table_map.get(chain.lower())
            tables = [t] if t else _CHAIN_TABLES

        for table in tables:
            try:
                rows = (
                    client.table(table)
                    .select('*')
                    .or_(f'from_address.eq.{address},to_address.eq.{address},whale_address.eq.{address}')
                    .order('timestamp', desc=True)
                    .limit(limit + offset)
                    .execute()
                ).data or []
                all_txs.extend(rows)
            except Exception as e:
                logger.warning(f"Failed to query {table} for {address}: {e}")

        all_txs.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        return all_txs[offset:offset + limit]

    def get_counterparties(self, address: str, chain: str = None, limit: int = 20) -> List[Dict[str, Any]]:
        """Get top counterparties for a wallet."""
        address = address.lower()
        txs = self.get_transaction_history(address, chain, limit=500)

        counter = {}
        for tx in txs:
            cp = tx.get('counterparty_address', '')
            if not cp or cp == address:
                fa = tx.get('from_address', '')
                ta = tx.get('to_address', '')
                cp = ta if fa == address else fa
            if not cp or cp == address:
                continue
            if cp not in counter:
                counter[cp] = {'address': cp, 'tx_count': 0, 'total_usd': 0.0, 'label': ''}
            counter[cp]['tx_count'] += 1
            _usd = float(tx.get('usd_value', 0) or 0)
            if _usd <= 10_000_000_000:  # Sanity cap
                counter[cp]['total_usd'] += _usd

        # Enrich with labels
        self._refresh_entity_cache()
        for cp_data in counter.values():
            cp_data['label'] = self._entity_cache.get(cp_data['address'], '')

        ranked = sorted(counter.values(), key=lambda x: x['total_usd'], reverse=True)
        return ranked[:limit]

    def search_wallets(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search wallet profiles by address prefix or entity name."""
        client = self._get_client()
        if not client:
            return []

        results = []
        query_lower = query.lower().strip()

        try:
            # Search by address prefix
            if query_lower.startswith('0x') or len(query_lower) >= 20:
                rows = (
                    client.table('wallet_profiles')
                    .select('address,chain,entity_name,entity_type,tags,smart_money_score,portfolio_value_usd,last_active')
                    .ilike('address', f'{query_lower}%')
                    .limit(limit)
                    .execute()
                ).data or []
                results.extend(rows)

            # Search by entity name
            if not results or not query_lower.startswith('0x'):
                rows = (
                    client.table('wallet_profiles')
                    .select('address,chain,entity_name,entity_type,tags,smart_money_score,portfolio_value_usd,last_active')
                    .ilike('entity_name', f'%{query_lower}%')
                    .limit(limit)
                    .execute()
                ).data or []
                results.extend(rows)

            # Also search the addresses table
            rows = (
                client.table('addresses')
                .select('address,entity_name')
                .ilike('entity_name', f'%{query_lower}%')
                .limit(limit)
                .execute()
            ).data or []
            for r in rows:
                if r.get('address') and not any(x.get('address') == r['address'] for x in results):
                    results.append({
                        'address': r['address'],
                        'entity_name': r.get('entity_name', ''),
                    })
        except Exception as e:
            logger.warning(f"Search failed for '{query}': {e}")

        return results[:limit]

    # ------------------------------------------------------------------
    # Profile building
    # ------------------------------------------------------------------

    def _build_profile_fresh(self, address: str, chain: str = None) -> Dict[str, Any]:
        """Build a fresh profile from transaction history + enrichment."""
        txs = self.get_transaction_history(address, chain, limit=500)
        profile = self._build_profile_from_transactions(address, txs, chain)
        profile = self._enrich_with_portfolio(profile)
        profile['smart_money_score'] = self._compute_smart_money_score(profile)
        profile['tags'] = self._assign_tags(profile)
        profile['updated_at'] = datetime.now(timezone.utc).isoformat()

        # Merge entity labels
        self._refresh_entity_cache()
        if not profile.get('entity_name'):
            profile['entity_name'] = self._entity_cache.get(address, '')

        self._cache_profile(profile)
        return profile

    def _build_profile_from_transactions(
        self, address: str, txs: List[Dict[str, Any]], chain: str = None
    ) -> Dict[str, Any]:
        """Aggregate transaction data into a profile dict."""
        now = datetime.now(timezone.utc)
        thirty_days_ago = now - timedelta(days=30)

        total_volume_all = 0.0
        total_volume_30d = 0.0
        tx_count_all = len(txs)
        tx_count_30d = 0
        buy_count = 0
        sell_count = 0
        token_counts = {}
        counterparty_counts = {}
        first_seen = None
        last_active = None
        pnl_estimated = 0.0

        # Per-transaction sanity cap: skip obviously wrong values that slipped
        # past the storage cap (historical data or race conditions).
        _MAX_TX_USD = 500_000_000  # $500M — no single whale tx exceeds this

        for tx in txs:
            usd = float(tx.get('usd_value', 0) or 0)
            if usd > _MAX_TX_USD:
                usd = 0  # Treat as bad data
            total_volume_all += usd

            ts_str = tx.get('timestamp', '')
            try:
                ts = datetime.fromisoformat(str(ts_str).replace('Z', '+00:00'))
            except Exception:
                ts = now

            if first_seen is None or ts < first_seen:
                first_seen = ts
            if last_active is None or ts > last_active:
                last_active = ts

            if ts >= thirty_days_ago:
                total_volume_30d += usd
                tx_count_30d += 1

            cls = (tx.get('classification', '') or '').upper()
            if cls == 'BUY':
                buy_count += 1
                pnl_estimated -= usd
            elif cls == 'SELL':
                sell_count += 1
                pnl_estimated += usd

            sym = tx.get('token_symbol', '')
            if sym:
                token_counts[sym] = token_counts.get(sym, 0) + 1

            cp = tx.get('counterparty_address', '')
            if cp and cp != address:
                counterparty_counts[cp] = counterparty_counts.get(cp, 0) + 1

        top_tokens = sorted(token_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        top_counterparties = sorted(counterparty_counts.items(), key=lambda x: x[1], reverse=True)[:10]

        detected_chain = chain or ''
        if not detected_chain and txs:
            detected_chain = txs[0].get('blockchain', '')

        return {
            'address': address,
            'chain': detected_chain,
            'entity_name': '',
            'entity_type': '',
            'tags': [],
            'smart_money_score': 0.0,
            'total_volume_usd_30d': round(total_volume_30d, 2),
            'total_volume_usd_all': round(total_volume_all, 2),
            'tx_count_30d': tx_count_30d,
            'tx_count_all': tx_count_all,
            'buy_count': buy_count,
            'sell_count': sell_count,
            'portfolio_value_usd': 0.0,
            'pnl_estimated_usd': round(pnl_estimated, 2),
            'first_seen': first_seen.isoformat() if first_seen else None,
            'last_active': last_active.isoformat() if last_active else None,
            'top_tokens': [{'symbol': s, 'count': c} for s, c in top_tokens],
            'top_counterparties': [{'address': a, 'count': c} for a, c in top_counterparties],
            'updated_at': datetime.now(timezone.utc).isoformat(),
        }

    def _enrich_with_portfolio(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich profile with Zerion portfolio data, with tx-based fallback."""
        zerion = self._get_zerion()
        if zerion:
            try:
                portfolio = zerion.get_wallet_portfolio(profile['address'])
                if portfolio and portfolio.total_value_usd > 0:
                    profile['portfolio_value_usd'] = portfolio.total_value_usd
                    return profile
            except Exception as e:
                logger.warning(f"Portfolio enrichment failed for {profile['address']}: {e}")

        # Fallback: estimate portfolio from net buy volume (buys - sells) in last 30 days.
        # This is a rough lower bound — the wallet likely holds more than just recent buys.
        buy_vol = profile.get('total_volume_usd_30d', 0) * (
            profile.get('buy_count', 0) / max(1, profile.get('buy_count', 0) + profile.get('sell_count', 0))
        )
        sell_vol = profile.get('total_volume_usd_30d', 0) * (
            profile.get('sell_count', 0) / max(1, profile.get('buy_count', 0) + profile.get('sell_count', 0))
        )
        net_accumulation = max(0, buy_vol - sell_vol)
        if net_accumulation > 0:
            profile['portfolio_value_usd'] = round(net_accumulation, 2)

        return profile

    def _compute_smart_money_score(self, profile: Dict[str, Any]) -> float:
        """Compute smart money score (0.0 - 1.0) using weighted signals."""
        score = 0.0

        # 1. Profitability proxy: buy/sell PnL direction
        pnl = profile.get('pnl_estimated_usd', 0)
        vol = profile.get('total_volume_usd_all', 1)
        if vol > 0:
            pnl_ratio = pnl / vol
            profitability = max(0, min(1.0, 0.5 + pnl_ratio))
        else:
            profitability = 0.5
        score += profitability * _SCORE_WEIGHTS['profitability']

        # 2. Timing proxy: has early activity (first_seen long ago)
        first = profile.get('first_seen')
        if first:
            try:
                first_dt = datetime.fromisoformat(str(first).replace('Z', '+00:00'))
                age_days = (datetime.now(timezone.utc) - first_dt).days
                timing = min(1.0, age_days / 365)
            except Exception:
                timing = 0.5
        else:
            timing = 0.0
        score += timing * _SCORE_WEIGHTS['timing']

        # 3. Counterparty quality: number of distinct counterparties
        cp_count = len(profile.get('top_counterparties', []))
        cp_score = min(1.0, cp_count / 10)
        score += cp_score * _SCORE_WEIGHTS['counterparty_quality']

        # 4. Volume consistency: having volume in 30d period
        vol_30d = profile.get('total_volume_usd_30d', 0)
        vol_all = profile.get('total_volume_usd_all', 1)
        if vol_all > 0:
            consistency = min(1.0, (vol_30d / vol_all) * 2)
        else:
            consistency = 0.0
        score += consistency * _SCORE_WEIGHTS['volume_consistency']

        # 5. Token diversity
        token_count = len(profile.get('top_tokens', []))
        diversity = min(1.0, token_count / 10)
        score += diversity * _SCORE_WEIGHTS['token_diversity']

        # 6. Known entity bonus
        entity = profile.get('entity_name', '')
        entity_type = profile.get('entity_type', '').lower()
        if entity and entity_type in ('fund', 'institution', 'exchange', 'market_maker'):
            score += 1.0 * _SCORE_WEIGHTS['known_entity']
        elif entity:
            score += 0.5 * _SCORE_WEIGHTS['known_entity']

        return round(min(1.0, max(0.0, score)), 4)

    def _assign_tags(self, profile: Dict[str, Any]) -> List[str]:
        """Assign behavioral tags based on profile metrics."""
        tags = []
        portfolio = profile.get('portfolio_value_usd', 0)
        vol_30d = profile.get('total_volume_usd_30d', 0)
        score = profile.get('smart_money_score', 0)
        buy = profile.get('buy_count', 0)
        sell = profile.get('sell_count', 0)
        total_trades = buy + sell
        tx_30d = profile.get('tx_count_30d', 0)
        token_count = len(profile.get('top_tokens', []))

        if portfolio >= _TAG_THRESHOLDS['whale_portfolio_usd'] or vol_30d >= _TAG_THRESHOLDS['whale_volume_30d_usd']:
            tags.append('whale')

        if score >= _TAG_THRESHOLDS['smart_money_score']:
            tags.append('smart_money')

        if token_count >= _TAG_THRESHOLDS['degen_token_count_30d'] and tx_30d >= 20:
            tags.append('degen')

        if total_trades > 0:
            buy_ratio = buy / total_trades
            sell_ratio = sell / total_trades

            if buy_ratio >= _TAG_THRESHOLDS['accumulator_buy_ratio']:
                tags.append('accumulator')
            if sell_ratio >= _TAG_THRESHOLDS['distributor_sell_ratio']:
                tags.append('distributor')

            # Market maker: ~50/50 and high volume
            if 0.35 <= buy_ratio <= 0.65 and total_trades >= 20 and vol_30d >= 1_000_000:
                tags.append('market_maker')

        entity_type = profile.get('entity_type', '').lower()
        if entity_type in ('fund', 'institution') or (portfolio >= 10_000_000 and total_trades < 10):
            tags.append('institutional')

        return tags

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _cache_profile(self, profile: Dict[str, Any]):
        """Upsert profile into wallet_profiles table."""
        client = self._get_client()
        if not client:
            return

        row = {
            'address': profile['address'],
            'chain': profile.get('chain', ''),
            'entity_name': profile.get('entity_name', ''),
            'entity_type': profile.get('entity_type', ''),
            'tags': profile.get('tags', []),
            'smart_money_score': profile.get('smart_money_score', 0),
            'total_volume_usd_30d': profile.get('total_volume_usd_30d', 0),
            'total_volume_usd_all': profile.get('total_volume_usd_all', 0),
            'tx_count_30d': profile.get('tx_count_30d', 0),
            'tx_count_all': profile.get('tx_count_all', 0),
            'buy_count': profile.get('buy_count', 0),
            'sell_count': profile.get('sell_count', 0),
            'portfolio_value_usd': profile.get('portfolio_value_usd', 0),
            'pnl_estimated_usd': profile.get('pnl_estimated_usd', 0),
            'first_seen': profile.get('first_seen'),
            'last_active': profile.get('last_active'),
            'top_tokens': profile.get('top_tokens', []),
            'top_counterparties': profile.get('top_counterparties', []),
            'updated_at': profile.get('updated_at', datetime.now(timezone.utc).isoformat()),
        }

        try:
            client.table('wallet_profiles').upsert(row, on_conflict='address').execute()
        except Exception as e:
            logger.warning(f"Failed to cache profile for {profile['address']}: {e}")

    def _refresh_entity_cache(self):
        """Load entity names from Supabase addresses table."""
        if self._entity_cache:
            return
        client = self._get_client()
        if not client:
            return
        try:
            rows = (
                client.table('addresses')
                .select('address,entity_name')
                .not_.is_('entity_name', 'null')
                .limit(5000)
                .execute()
            ).data or []
            self._entity_cache = {r['address'].lower(): r['entity_name'] for r in rows if r.get('entity_name')}
        except Exception:
            pass


# Global instance
wallet_profiler = WalletProfiler()
