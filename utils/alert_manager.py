"""Alert Manager - Real-time alert evaluation and dispatch.

Loads active alerts from `wallet_alerts` table, evaluates incoming transactions,
and emits Socket.IO `wallet_alert` events. Optional Telegram notifications.
"""

import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

_ALERT_CACHE_TTL = 300  # Refresh every 5 min


class AlertManager:
    """Manage and evaluate wallet alerts in real-time."""

    def __init__(self):
        self._client = None
        self._lock = threading.Lock()
        self._alerts: Dict[str, Dict[str, Any]] = {}  # id -> alert
        self._address_index: Dict[str, List[str]] = {}  # address -> [alert_ids]
        self._cache_ts = 0
        self._socketio = None

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

    def set_socketio(self, sio):
        """Set the Socket.IO instance for emitting alerts."""
        self._socketio = sio

    # ------------------------------------------------------------------
    # Alert cache
    # ------------------------------------------------------------------

    def _refresh_alerts(self):
        """Load active alerts into memory."""
        if time.time() - self._cache_ts < _ALERT_CACHE_TTL:
            return
        client = self._get_client()
        if not client:
            return
        try:
            rows = (
                client.table('wallet_alerts')
                .select('*')
                .eq('is_active', True)
                .execute()
            ).data or []

            alerts = {}
            index = {}
            for r in rows:
                alerts[r['id']] = r
                addr = r.get('address', '').lower()
                if addr not in index:
                    index[addr] = []
                index[addr].append(r['id'])

            self._alerts = alerts
            self._address_index = index
            self._cache_ts = time.time()
            logger.debug(f"Loaded {len(alerts)} active wallet alerts")
        except Exception as e:
            logger.warning(f"Failed to refresh alerts: {e}")

    # ------------------------------------------------------------------
    # Transaction evaluation
    # ------------------------------------------------------------------

    def check_wallet_alerts(self, event: Dict[str, Any]):
        """Check if a transaction triggers any wallet alerts."""
        self._refresh_alerts()
        if not self._alerts:
            return

        from_addr = (event.get('from', event.get('from_address', '')) or '').lower()
        to_addr = (event.get('to', event.get('to_address', '')) or '').lower()
        usd_value = float(event.get('usd_value', 0) or event.get('estimated_usd', 0) or 0)
        classification = (event.get('classification', '') or '').upper()

        for addr in (from_addr, to_addr):
            if addr not in self._address_index:
                continue
            for alert_id in self._address_index[addr]:
                alert = self._alerts.get(alert_id)
                if not alert:
                    continue
                if self._matches_alert(alert, usd_value, classification):
                    self._fire_alert(alert, event)

    def _matches_alert(self, alert: Dict, usd_value: float, classification: str) -> bool:
        """Check if transaction matches alert criteria."""
        min_usd = alert.get('min_usd_value', 0) or 0
        if usd_value < min_usd:
            return False

        alert_type = alert.get('alert_type', 'any_move')
        if alert_type == 'any_move':
            return True
        elif alert_type == 'large_move':
            return usd_value >= max(min_usd, 100_000)
        elif alert_type == 'buy_only':
            return classification == 'BUY'
        elif alert_type == 'sell_only':
            return classification == 'SELL'
        return True

    def _fire_alert(self, alert: Dict, event: Dict):
        """Emit alert via Socket.IO and optionally Telegram."""
        alert_event = {
            'alert_id': alert['id'],
            'address': alert['address'],
            'chain': alert.get('chain', ''),
            'alert_type': alert.get('alert_type', 'any_move'),
            'transaction': {
                'hash': event.get('tx_hash', event.get('hash', '')),
                'from': event.get('from', event.get('from_address', '')),
                'to': event.get('to', event.get('to_address', '')),
                'usd_value': float(event.get('usd_value', 0) or event.get('estimated_usd', 0) or 0),
                'classification': event.get('classification', ''),
                'symbol': event.get('symbol', event.get('token_symbol', '')),
                'blockchain': event.get('blockchain', ''),
            },
            'triggered_at': datetime.now(timezone.utc).isoformat(),
        }

        # Socket.IO emit
        if self._socketio and alert.get('notify_socketio', True):
            try:
                self._socketio.emit('wallet_alert', alert_event)
            except Exception as e:
                logger.warning(f"Failed to emit wallet_alert: {e}")

        # Telegram (optional)
        if alert.get('notify_telegram') and alert.get('telegram_chat_id'):
            self._send_telegram(alert, alert_event)

    def _send_telegram(self, alert: Dict, alert_event: Dict):
        """Send Telegram notification for triggered alert."""
        try:
            import requests
            from config.api_keys import TELEGRAM_BOT_TOKEN
            if not TELEGRAM_BOT_TOKEN:
                return

            tx = alert_event['transaction']
            text = (
                f"🚨 Wallet Alert: {alert['address'][:10]}...\n"
                f"Type: {alert_event['alert_type']}\n"
                f"Token: {tx['symbol']} | ${tx['usd_value']:,.0f}\n"
                f"Classification: {tx['classification']}\n"
                f"Chain: {tx['blockchain']}"
            )

            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={'chat_id': alert['telegram_chat_id'], 'text': text},
                timeout=10,
            )
        except Exception as e:
            logger.warning(f"Telegram alert failed: {e}")

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def list_alerts(self, address: str = None) -> List[Dict[str, Any]]:
        client = self._get_client()
        if not client:
            return []
        try:
            q = client.table('wallet_alerts').select('*').order('created_at', desc=True)
            if address:
                q = q.eq('address', address.lower())
            return q.execute().data or []
        except Exception as e:
            logger.error(f"Failed to list alerts: {e}")
            return []

    def create_alert(
        self, address: str, chain: str = '', alert_type: str = 'any_move',
        min_usd_value: float = 0, notify_socketio: bool = True,
        notify_telegram: bool = False, telegram_chat_id: str = ''
    ) -> Optional[Dict[str, Any]]:
        client = self._get_client()
        if not client:
            return None
        try:
            row = {
                'id': str(uuid.uuid4()),
                'address': address.lower(),
                'chain': chain,
                'alert_type': alert_type,
                'min_usd_value': min_usd_value,
                'is_active': True,
                'notify_socketio': notify_socketio,
                'notify_telegram': notify_telegram,
                'telegram_chat_id': telegram_chat_id,
                'created_at': datetime.now(timezone.utc).isoformat(),
            }
            result = client.table('wallet_alerts').insert(row).execute()
            self._cache_ts = 0  # Force refresh
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Failed to create alert: {e}")
            return None

    def update_alert(self, alert_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        client = self._get_client()
        if not client:
            return None
        allowed = {'alert_type', 'min_usd_value', 'is_active', 'notify_socketio', 'notify_telegram', 'telegram_chat_id'}
        filtered = {k: v for k, v in updates.items() if k in allowed}
        if not filtered:
            return None
        try:
            result = client.table('wallet_alerts').update(filtered).eq('id', alert_id).execute()
            self._cache_ts = 0
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Failed to update alert {alert_id}: {e}")
            return None

    def delete_alert(self, alert_id: str) -> bool:
        client = self._get_client()
        if not client:
            return False
        try:
            client.table('wallet_alerts').delete().eq('id', alert_id).execute()
            self._cache_ts = 0
            return True
        except Exception as e:
            logger.error(f"Failed to delete alert {alert_id}: {e}")
            return False


# Global instance
alert_manager = AlertManager()
