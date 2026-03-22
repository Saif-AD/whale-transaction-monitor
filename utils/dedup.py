# dedup.py

from typing import Dict, Any, Optional, Tuple
from collections import defaultdict
import time

# In dedup.py - update the TransactionDeduplicator class

class TransactionDeduplicator:
    # Stablecoin symbols to exclude — these are high-volume, low-signal transfers
    # that flood the database and drown out actual whale activity.
    EXCLUDED_STABLECOINS = {'USDT', 'USDC', 'DAI', 'BUSD', 'TUSD', 'USDP', 'FDUSD'}

    def __init__(self):
        self.transactions = {}
        self.chain_hashes = defaultdict(set)
        self.address_timestamps = defaultdict(dict)
        self.on_new_transaction = None  # Callback for real-time push
        self._evm_seen_hashes = set()  # First-event-per-tx-hash for EVM multi-leg swaps
        self.stats = {
            'total_received': 0,
            'duplicates_caught': 0,
            'circular_flows_caught': 0,
            'stablecoins_skipped': 0,
            'by_chain': defaultdict(lambda: {'total': 0, 'duplicates': 0, 'circular': 0}),
        }

    # In dedup.py - update the generate_key method
    def generate_key(self, event: Dict[str, Any]) -> Tuple:
        """Generate a more robust unique key for transaction deduplication"""
        chain = event.get('blockchain', '').lower()
        tx_hash = event.get('tx_hash', '')
        
        # For Solana, add more components to make the key unique
        if chain == 'solana':
            from_addr = event.get('from', '')
            to_addr = event.get('to', '')
            amount = str(event.get('amount', '0'))
            return (chain, tx_hash, from_addr, to_addr, amount)
        elif chain == 'bitcoin':
            # Bitcoin has multiple outputs per tx_hash — key by output address + amount
            to_addr = event.get('to', '')
            amount = str(event.get('amount', '0'))
            return (chain, tx_hash, to_addr, amount)
        elif chain in ['ethereum', 'bsc', 'polygon']:
            return (chain, tx_hash, event.get('log_index', 0))
        elif chain == 'xrp':
            return (chain, tx_hash, event.get('sequence', 0))
        else:
            return (chain, tx_hash, event.get('log_index', 0))

    def handle_event(self, event: Dict[str, Any]) -> bool:
        """Process new event with enhanced deduplication"""
        if not event:
            return False

        self.stats['total_received'] += 1
        chain = event.get('blockchain', '').lower()
        self.stats['by_chain'][chain]['total'] += 1

        # Skip stablecoins on most chains (high-volume noise)
        # BUT allow them on Polygon where USDC/USDT are the primary large-value tokens
        symbol = (event.get('symbol') or '').upper()
        chain = event.get('blockchain', '').lower()
        if symbol in self.EXCLUDED_STABLECOINS and chain not in ('polygon', 'solana'):
            self.stats['stablecoins_skipped'] += 1
            return False

        # EVM multi-hop: a single DEX swap produces 3-4 internal ERC-20
        # transfer logs (user→router→pool→user). Only keep the first log
        # per tx_hash to avoid showing the same swap 4 times.
        if chain in ('ethereum', 'polygon', 'bsc'):
            tx_hash = event.get('tx_hash', '')
            if tx_hash:
                if tx_hash in self._evm_seen_hashes:
                    self.stats['duplicates_caught'] += 1
                    self.stats['by_chain'][chain]['duplicates'] += 1
                    return False
                self._evm_seen_hashes.add(tx_hash)

        unique_key = self.generate_key(event)
        
        # Check for direct duplicates
        if unique_key in self.transactions:
            self.stats['duplicates_caught'] += 1
            self.stats['by_chain'][chain]['duplicates'] += 1
            
            # Update classification if needed
            if ('classification' not in self.transactions[unique_key] and 
                'classification' in event):
                self.transactions[unique_key]['classification'] = event['classification']
                
            return False
        
        # Check for circular flows (A->B->A) AND same-destination same-amount
        # within short time windows
        from_addr = event.get('from', '')
        to_addr = event.get('to', '')
        try:
            amount = float(event.get('amount', 0))
        except (ValueError, TypeError):
            amount = 0
        symbol = event.get('symbol', '')
        
        current_time = time.time()
        for key, tx in list(self.transactions.items()):
            tx_time = tx.get('timestamp', 0)
            if current_time - tx_time > 3600:
                continue
                
            if tx.get('blockchain', '').lower() != chain or tx.get('symbol') != symbol:
                continue

            try:
                tx_amount = float(tx.get('amount', 0))
            except (ValueError, TypeError):
                tx_amount = 0

            amount_similar = abs(tx_amount - amount) / max(0.01, amount) < 0.01

            # Circular flow: from/to reversed with similar amount (A->B then B->A)
            if (amount_similar and
                tx.get('from') == to_addr and tx.get('to') == from_addr):
                self.stats['circular_flows_caught'] += 1
                self.stats['by_chain'][chain]['circular'] += 1
                return False

            # Same-destination same-amount: different senders depositing the
            # same amount to the same address within a short window.
            # Use tighter window for XRP/BTC (30s) vs Solana/EVM (300s)
            # because round-number deposits to exchanges are common and legit.
            same_dest_window = 30 if chain in ('xrp', 'bitcoin') else 300
            if (amount_similar and
                to_addr and tx.get('to') == to_addr and
                tx.get('from') != from_addr and
                current_time - tx_time < same_dest_window):
                self.stats['circular_flows_caught'] += 1
                self.stats['by_chain'][chain]['circular'] += 1
                return False

            # Chained transfer: A→B then B→C with same amount (tumbling/wash).
            # The new event's `from` matches a recent event's `to`.
            chain_window = 60 if chain in ('xrp', 'bitcoin') else 300
            if (amount_similar and
                from_addr and tx.get('to') == from_addr and
                current_time - tx_time < chain_window):
                self.stats['circular_flows_caught'] += 1
                self.stats['by_chain'][chain]['circular'] += 1
                return False

            # Same-amount flood: bot farms distributing identical amounts
            # across many unique wallet pairs (e.g. 511.80 SOL x 20).
            # All different addresses, but same chain+symbol+amount.
            # Only for Solana/EVM — XRP and Bitcoin have naturally repetitive
            # round amounts (10K XRP, 1 BTC) that are legitimate distinct txs.
            if (chain not in ('xrp', 'bitcoin') and
                amount_similar and
                current_time - tx_time < 120):
                self.stats['circular_flows_caught'] += 1
                self.stats['by_chain'][chain]['circular'] += 1
                return False

        # Add timestamp if not present
        if 'timestamp' not in event:
            event['timestamp'] = current_time

        self.transactions[unique_key] = event

        # Push to connected clients via SocketIO
        if self.on_new_transaction:
            try:
                self.on_new_transaction(event)
            except Exception:
                pass  # Don't let push errors break the pipeline

        # Persist to Supabase (non-blocking, rate-limited thread pool)
        try:
            from utils.supabase_writer import store_transaction
            import threading
            # Cap concurrent Supabase writes to avoid connection floods
            if threading.active_count() < 20:
                threading.Thread(
                    target=store_transaction,
                    args=(event,),
                    daemon=True
                ).start()
        except Exception:
            pass  # Don't let Supabase errors break the pipeline

        return True

        # In dedup.py - update the get_stats function
    def get_stats(self):
        """Get deduplication statistics with chain breakdown"""
        total_dupes = self.stats['duplicates_caught'] + self.stats['circular_flows_caught'] + self.stats.get('stablecoins_skipped', 0)
        chain_stats = {}
        
        # Calculate per-chain deduplication rates
        for chain, stats in self.stats['by_chain'].items():
            if stats['total'] > 0:
                dedup_rate = ((stats['duplicates'] + stats['circular']) / stats['total']) * 100
                chain_stats[chain] = {
                    'total': stats['total'],
                    'duplicates': stats['duplicates'] + stats['circular'],
                    'rate': dedup_rate
                }
        
        return {
            'total_transactions': len(self.transactions),
            'total_received': self.stats['total_received'],
            'duplicates_caught': self.stats['duplicates_caught'],
            'circular_flows_caught': self.stats['circular_flows_caught'],
            'stablecoins_skipped': self.stats.get('stablecoins_skipped', 0),
            'total_duplicates': total_dupes,
            'by_chain': chain_stats,
            'dedup_ratio': (total_dupes / max(1, self.stats['total_received'])) * 100
        }

# Keep the old name for backward compatibility
EnhancedDeduplication = TransactionDeduplicator  # Add this line

# Global instance
deduplicator = TransactionDeduplicator()

# Export functions
def handle_event(event: Dict[str, Any]) -> bool:
    """Process new event and determine if it's unique"""
    return deduplicator.handle_event(event)

def get_stats() -> Dict[str, Any]:
    import copy
    try:
        return deduplicator.get_stats()
    except Exception as e:
        print(f"Error in get_stats: {e}")
        return {
            'total_transactions': 0,
            'total_received': 0,
            'duplicates_caught': 0,
            'circular_flows_caught': 0,
            'total_duplicates': 0,
            'by_chain': {},
            'dedup_ratio': 0
        }

def get_transactions() -> Dict:
    return deduplicator.transactions

# For backward compatibility
get_dedup_stats = get_stats
deduped_transactions = deduplicator.transactions

# Export these for direct access if needed
__all__ = [
    'handle_event',
    'get_stats',
    'get_transactions',
    'get_dedup_stats',
    'deduped_transactions',
    'TransactionDeduplicator',
    'EnhancedDeduplication',  # Add this line
    'deduplicator'
]

def deduplicate_transactions(transactions):
    """
    If duplicates share the same 'hash', keep whichever has higher confidence.
    """
    unique = {}
    for tx in transactions:
        tx_hash = tx.get("hash")
        if tx_hash is None:
            continue  # Skip if missing a hash

        # Check if we already saw a transaction with this hash
        if tx_hash in unique:
            existing = unique[tx_hash]
            if tx.get("confidence", 0) > existing.get("confidence", 1):
                unique[tx_hash] = tx
        else:
            unique[tx_hash] = tx

    return list(unique.values())