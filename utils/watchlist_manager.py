"""Watchlist Manager - CRUD for watchlists and watched addresses.

Wraps the `watchlists` and `watchlist_addresses` Supabase tables and joins
with `wallet_profiles` for enriched watchlist views.
"""

import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


class WatchlistManager:
    """Manage user watchlists and tracked addresses."""

    def __init__(self):
        self._client = None
        self._lock = threading.Lock()

    def _get_client(self):
        if self._client is None:
            with self._lock:
                if self._client is None:
                    try:
                        from supabase import create_client
                        from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
                        self._client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
                    except Exception as e:
                        logger.error(f"Supabase init failed: {e}")
        return self._client

    # ------------------------------------------------------------------
    # Watchlist CRUD
    # ------------------------------------------------------------------

    def list_watchlists(self) -> List[Dict[str, Any]]:
        client = self._get_client()
        if not client:
            return []
        try:
            rows = (
                client.table('watchlists')
                .select('*')
                .order('created_at', desc=True)
                .execute()
            ).data or []
            # Attach address count
            for wl in rows:
                count = (
                    client.table('watchlist_addresses')
                    .select('id', count='exact')
                    .eq('watchlist_id', wl['id'])
                    .execute()
                )
                wl['address_count'] = count.count if count.count is not None else 0
            return rows
        except Exception as e:
            logger.error(f"Failed to list watchlists: {e}")
            return []

    def create_watchlist(self, name: str) -> Optional[Dict[str, Any]]:
        client = self._get_client()
        if not client:
            return None
        try:
            now = datetime.now(timezone.utc).isoformat()
            row = {
                'id': str(uuid.uuid4()),
                'name': name,
                'created_at': now,
                'updated_at': now,
            }
            result = client.table('watchlists').insert(row).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Failed to create watchlist: {e}")
            return None

    def delete_watchlist(self, watchlist_id: str) -> bool:
        client = self._get_client()
        if not client:
            return False
        try:
            client.table('watchlist_addresses').delete().eq('watchlist_id', watchlist_id).execute()
            client.table('watchlists').delete().eq('id', watchlist_id).execute()
            return True
        except Exception as e:
            logger.error(f"Failed to delete watchlist {watchlist_id}: {e}")
            return False

    # ------------------------------------------------------------------
    # Address CRUD
    # ------------------------------------------------------------------

    def list_addresses(self, watchlist_id: str) -> List[Dict[str, Any]]:
        client = self._get_client()
        if not client:
            return []
        try:
            rows = (
                client.table('watchlist_addresses')
                .select('*')
                .eq('watchlist_id', watchlist_id)
                .order('added_at', desc=True)
                .execute()
            ).data or []

            # Enrich with profile data
            for row in rows:
                addr = row.get('address', '').lower()
                if addr:
                    try:
                        profile = (
                            client.table('wallet_profiles')
                            .select('entity_name,tags,smart_money_score,portfolio_value_usd,last_active')
                            .eq('address', addr)
                            .maybe_single()
                            .execute()
                        ).data
                        if profile:
                            row['profile'] = profile
                    except Exception:
                        pass
            return rows
        except Exception as e:
            logger.error(f"Failed to list addresses for watchlist {watchlist_id}: {e}")
            return []

    def add_address(
        self, watchlist_id: str, address: str, chain: str = '',
        custom_label: str = '', notes: str = ''
    ) -> Optional[Dict[str, Any]]:
        client = self._get_client()
        if not client:
            return None
        try:
            row = {
                'id': str(uuid.uuid4()),
                'watchlist_id': watchlist_id,
                'address': address.lower(),
                'chain': chain,
                'custom_label': custom_label,
                'notes': notes,
                'added_at': datetime.now(timezone.utc).isoformat(),
            }
            result = client.table('watchlist_addresses').insert(row).execute()
            # Update watchlist updated_at
            client.table('watchlists').update(
                {'updated_at': datetime.now(timezone.utc).isoformat()}
            ).eq('id', watchlist_id).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Failed to add address: {e}")
            return None

    def remove_address(self, watchlist_id: str, address: str) -> bool:
        client = self._get_client()
        if not client:
            return False
        try:
            client.table('watchlist_addresses').delete().eq(
                'watchlist_id', watchlist_id
            ).eq('address', address.lower()).execute()
            return True
        except Exception as e:
            logger.error(f"Failed to remove address: {e}")
            return False

    # ------------------------------------------------------------------
    # Leaderboard
    # ------------------------------------------------------------------

    def get_leaderboard(
        self, sort_by: str = 'smart_money_score', chain: str = None, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Top wallets ranked by score, volume, or PnL."""
        client = self._get_client()
        if not client:
            return []

        valid_sorts = {
            'smart_money_score', 'total_volume_usd_30d', 'total_volume_usd_all',
            'portfolio_value_usd', 'pnl_estimated_usd', 'tx_count_30d',
        }
        if sort_by not in valid_sorts:
            sort_by = 'smart_money_score'

        try:
            q = (
                client.table('wallet_profiles')
                .select('address,chain,entity_name,tags,smart_money_score,total_volume_usd_30d,portfolio_value_usd,pnl_estimated_usd,tx_count_30d,last_active')
                .order(sort_by, desc=True)
                .limit(limit)
            )
            if chain:
                q = q.eq('chain', chain.lower())
            rows = q.execute().data or []
            return rows
        except Exception as e:
            logger.error(f"Leaderboard query failed: {e}")
            return []


# Global instance
watchlist_manager = WatchlistManager()
