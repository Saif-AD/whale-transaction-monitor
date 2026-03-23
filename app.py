# In app.py

from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO
import time
import threading
import json

# Import your existing monitoring code and data structures
from chains.ethereum import print_new_erc20_transfers
from chains.whale_alert import start_whale_thread
from chains.xrp import start_xrp_thread
from chains.solana import start_solana_thread
from chains.solana_grpc import start_solana_grpc_thread
from chains.polygon import print_new_polygon_transfers
from chains.bitcoin_alchemy import poll_bitcoin_blocks
from chains.solana_api import print_new_solana_transfers
from models.classes import initialize_prices
from utils.dedup import get_stats as get_dedup_stats, deduplicator, deduped_transactions
from config.settings import (
    GLOBAL_USD_THRESHOLD,
    etherscan_buy_counts,
    etherscan_sell_counts,
    whale_buy_counts,
    whale_sell_counts,
    solana_buy_counts,
    solana_sell_counts,
    xrp_buy_counts,
    xrp_sell_counts,
    polygon_buy_counts,
    polygon_sell_counts,
    bitcoin_buy_counts,
    bitcoin_sell_counts,
    solana_api_buy_counts,
    solana_api_sell_counts,
)

# Wallet tracker imports
from utils.wallet_profiler import wallet_profiler
from utils.watchlist_manager import watchlist_manager
from utils.alert_manager import alert_manager
from utils.wallet_batch_scorer import batch_scorer

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="gevent")

# Give alert manager access to socketio for real-time alerts
alert_manager.set_socketio(socketio)

# Home route - render the main template
@app.route('/')
def index():
    return render_template('index.html')

# API route to get transaction data
@app.route('/api/transactions')
def get_transactions():
    # Get query parameters for filtering
    min_value = request.args.get('min_value', type=float, default=GLOBAL_USD_THRESHOLD)
    blockchain = request.args.get('blockchain', default=None)
    symbol = request.args.get('symbol', default=None)
    tx_type = request.args.get('type', default=None)
    limit = request.args.get('limit', type=int, default=50)

    # Get transactions from your deduplicator
    transactions = list(deduped_transactions.values())

    # Filter transactions
    filtered_txs = []
    for tx in transactions:
        # Apply filters
        tx_usd_value = tx.get("usd_value", 0) or tx.get("estimated_usd", 0)

        if tx_usd_value < min_value:
            continue

        if blockchain and tx.get("blockchain", "").lower() != blockchain.lower():
            continue

        if symbol and tx.get("symbol", "").upper() != symbol.upper():
            continue

        if tx_type and tx.get("classification", "").lower() != tx_type.lower():
            continue

        # Add to filtered list
        filtered_txs.append(tx)

        # Respect the limit
        if len(filtered_txs) >= limit:
            break

    # Sort by timestamp (newest first)
    filtered_txs.sort(key=lambda x: x.get("timestamp", 0), reverse=True)

    _refresh_entity_cache()
    for tx in filtered_txs:
        fa = (tx.get('from', tx.get('from_address', '')) or '').lower()
        ta = (tx.get('to', tx.get('to_address', '')) or '').lower()
        if fa in _entity_cache:
            tx['from_entity'] = _entity_cache[fa]
        if ta in _entity_cache:
            tx['to_entity'] = _entity_cache[ta]

    return jsonify(filtered_txs)

# API route to get statistics
@app.route('/api/stats')
def get_stats():
    # Collect token statistics from your global counters
    token_stats = {}

    # Process Ethereum transactions
    for symbol, count in etherscan_buy_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['buys'] += count

    for symbol, count in etherscan_sell_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['sells'] += count

    # Process Whale Alert transactions
    for symbol, count in whale_buy_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['buys'] += count

    for symbol, count in whale_sell_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['sells'] += count

    # Process Solana transactions
    for symbol, count in solana_buy_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['buys'] += count

    for symbol, count in solana_sell_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['sells'] += count

    # Process XRP transactions (now uses defaultdict like other chains)
    for symbol, count in xrp_buy_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['buys'] += count
    for symbol, count in xrp_sell_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['sells'] += count

    # Process Polygon transactions
    for symbol, count in polygon_buy_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['buys'] += count
    for symbol, count in polygon_sell_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['sells'] += count

    # Process Bitcoin transactions
    for symbol, count in bitcoin_buy_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['buys'] += count
    for symbol, count in bitcoin_sell_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['sells'] += count


    # Process Solana API transactions
    for symbol, count in solana_api_buy_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['buys'] += count
    for symbol, count in solana_api_sell_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['sells'] += count

    # Calculate additional statistics
    stats_list = []
    for symbol, stats in token_stats.items():
        total = stats['buys'] + stats['sells']
        if total > 0:
            buy_percentage = (stats['buys'] / total) * 100
        else:
            buy_percentage = 0

        stats_list.append({
            'symbol': symbol,
            'buys': stats['buys'],
            'sells': stats['sells'],
            'total': total,
            'buy_percentage': round(buy_percentage, 1),
            'trend': 'bullish' if buy_percentage > 60 else 'bearish' if buy_percentage < 40 else 'neutral'
        })

    # Sort by total volume
    stats_list.sort(key=lambda x: x['total'], reverse=True)

    # Get deduplication stats
    dedup_stats = get_dedup_stats()

    return jsonify({
        'tokens': stats_list,
        'deduplication': {
            'total_transactions': dedup_stats.get('total_received', 0),
            'unique_transactions': dedup_stats.get('total_transactions', 0),
            'duplicates_caught': dedup_stats.get('duplicates_caught', 0),
            'dedup_ratio': dedup_stats.get('dedup_ratio', 0)
        },
        'monitoring': {
            'active_threads': [t.name for t in threading.enumerate() if t.daemon],
            'min_transaction_value': GLOBAL_USD_THRESHOLD
        }
    })

_entity_cache = {}
_entity_cache_ts = 0
_ENTITY_CACHE_TTL = 300  # refresh every 5 min

def _refresh_entity_cache():
    global _entity_cache, _entity_cache_ts
    if time.time() - _entity_cache_ts < _ENTITY_CACHE_TTL:
        return
    try:
        from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
        from supabase import create_client
        sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        rows = sb.table('addresses').select('address,entity_name').not_.is_('entity_name', 'null').limit(5000).execute().data
        _entity_cache.update({r['address'].lower(): r['entity_name'] for r in rows if r.get('entity_name')})
        _entity_cache_ts = time.time()
    except Exception:
        pass

def push_new_transaction(event):
    """Push a new transaction to all connected clients via SocketIO, enriched with entity names."""
    _refresh_entity_cache()
    from_addr = (event.get('from', event.get('from_address', '')) or '').lower()
    to_addr = (event.get('to', event.get('to_address', '')) or '').lower()
    if from_addr in _entity_cache:
        event['from_entity'] = _entity_cache[from_addr]
    if to_addr in _entity_cache:
        event['to_entity'] = _entity_cache[to_addr]
    socketio.emit('new_transaction', event)

    # Check wallet alerts for this transaction (Phase 4)
    try:
        alert_manager.check_wallet_alerts(event)
    except Exception:
        pass

# -------------------------------------------------------------------------
# Wallet Tracker Routes (Phase 1-4)
# -------------------------------------------------------------------------

@app.route('/tracker')
def tracker():
    """Render the wallet tracker landing page."""
    return render_template('tracker.html')

@app.route('/tracker/<address>')
def wallet_profile_page(address):
    """Render the wallet profile page."""
    chain = request.args.get('chain', '')
    return render_template('wallet_profile.html', address=address, chain=chain)

@app.route('/api/wallet/profile/<address>')
def api_wallet_profile(address):
    """JSON: wallet profile with optional chain filter."""
    chain = request.args.get('chain', None)
    profile = wallet_profiler.get_profile(address, chain)
    if not profile:
        return jsonify({'error': 'Profile not found', 'address': address}), 404
    return jsonify(profile)

@app.route('/api/wallet/transactions/<address>')
def api_wallet_transactions(address):
    """JSON: paginated transaction history for a wallet."""
    chain = request.args.get('chain', None)
    limit = request.args.get('limit', type=int, default=50)
    offset = request.args.get('offset', type=int, default=0)
    txs = wallet_profiler.get_transaction_history(address, chain, limit, offset)
    return jsonify(txs)

@app.route('/api/wallet/counterparties/<address>')
def api_wallet_counterparties(address):
    """JSON: top counterparties for a wallet."""
    chain = request.args.get('chain', None)
    limit = request.args.get('limit', type=int, default=20)
    counterparties = wallet_profiler.get_counterparties(address, chain, limit)
    return jsonify(counterparties)

@app.route('/api/wallet/search')
def api_wallet_search():
    """JSON: search wallets by address or entity name."""
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify([])
    limit = request.args.get('limit', type=int, default=20)
    results = wallet_profiler.search_wallets(q, limit)
    return jsonify(results)

@app.route('/api/wallet/leaderboard')
def api_wallet_leaderboard():
    """JSON: top wallets ranked by score/volume."""
    sort_by = request.args.get('sort_by', 'smart_money_score')
    chain = request.args.get('chain', None)
    limit = request.args.get('limit', type=int, default=50)
    results = watchlist_manager.get_leaderboard(sort_by, chain, limit)
    return jsonify(results)

# -------------------------------------------------------------------------
# Watchlist Routes (Phase 2)
# -------------------------------------------------------------------------

@app.route('/api/watchlist', methods=['GET', 'POST'])
def api_watchlist():
    """List or create watchlists."""
    if request.method == 'POST':
        data = request.get_json(force=True)
        name = data.get('name', '').strip()
        if not name:
            return jsonify({'error': 'Name required'}), 400
        result = watchlist_manager.create_watchlist(name)
        if result:
            return jsonify(result), 201
        return jsonify({'error': 'Failed to create'}), 500
    return jsonify(watchlist_manager.list_watchlists())

@app.route('/api/watchlist/<watchlist_id>', methods=['DELETE'])
def api_watchlist_delete(watchlist_id):
    """Delete a watchlist."""
    if watchlist_manager.delete_watchlist(watchlist_id):
        return jsonify({'ok': True})
    return jsonify({'error': 'Failed to delete'}), 500

@app.route('/api/watchlist/<watchlist_id>/addresses', methods=['GET', 'POST'])
def api_watchlist_addresses(watchlist_id):
    """List or add addresses to a watchlist."""
    if request.method == 'POST':
        data = request.get_json(force=True)
        address = data.get('address', '').strip()
        if not address:
            return jsonify({'error': 'Address required'}), 400
        result = watchlist_manager.add_address(
            watchlist_id, address,
            chain=data.get('chain', ''),
            custom_label=data.get('custom_label', ''),
            notes=data.get('notes', ''),
        )
        if result:
            return jsonify(result), 201
        return jsonify({'error': 'Failed to add'}), 500
    return jsonify(watchlist_manager.list_addresses(watchlist_id))

@app.route('/api/watchlist/<watchlist_id>/addresses/<address>', methods=['DELETE'])
def api_watchlist_address_delete(watchlist_id, address):
    """Remove an address from a watchlist."""
    if watchlist_manager.remove_address(watchlist_id, address):
        return jsonify({'ok': True})
    return jsonify({'error': 'Failed to remove'}), 500

# -------------------------------------------------------------------------
# Alert Routes (Phase 4)
# -------------------------------------------------------------------------

@app.route('/api/alerts', methods=['GET', 'POST'])
def api_alerts():
    """List or create wallet alerts."""
    if request.method == 'POST':
        data = request.get_json(force=True)
        address = data.get('address', '').strip()
        if not address:
            return jsonify({'error': 'Address required'}), 400
        result = alert_manager.create_alert(
            address=address,
            chain=data.get('chain', ''),
            alert_type=data.get('alert_type', 'any_move'),
            min_usd_value=float(data.get('min_usd_value', 0)),
            notify_socketio=data.get('notify_socketio', True),
            notify_telegram=data.get('notify_telegram', False),
            telegram_chat_id=data.get('telegram_chat_id', ''),
        )
        if result:
            return jsonify(result), 201
        return jsonify({'error': 'Failed to create'}), 500
    address = request.args.get('address', None)
    return jsonify(alert_manager.list_alerts(address))

@app.route('/api/alerts/<alert_id>', methods=['PUT', 'DELETE'])
def api_alert_detail(alert_id):
    """Update or delete an alert."""
    if request.method == 'DELETE':
        if alert_manager.delete_alert(alert_id):
            return jsonify({'ok': True})
        return jsonify({'error': 'Failed to delete'}), 500
    data = request.get_json(force=True)
    result = alert_manager.update_alert(alert_id, data)
    if result:
        return jsonify(result)
    return jsonify({'error': 'Failed to update'}), 500

# -------------------------------------------------------------------------
# Monitor startup
# -------------------------------------------------------------------------

def start_monitors():
    """Start all transaction monitoring threads"""
    # Initialize token prices
    initialize_prices()

    # Register real-time push callback
    deduplicator.on_new_transaction = push_new_transaction

    threads = []

    # Start Ethereum monitoring (Etherscan discovery + Alchemy receipts)
    eth_thread = threading.Thread(target=print_new_erc20_transfers, daemon=True, name="Ethereum")
    eth_thread.start()
    threads.append(eth_thread)

    # Start Whale Alert monitoring
    whale_thread = start_whale_thread()
    if whale_thread:
        threads.append(whale_thread)

    # Start XRP monitoring
    xrp_thread = start_xrp_thread()
    if xrp_thread:
        threads.append(xrp_thread)

    # Start Solana gRPC streaming (Yellowstone via Alchemy) — primary
    solana_grpc_thread = start_solana_grpc_thread()
    if solana_grpc_thread:
        threads.append(solana_grpc_thread)

    # Start Solana WebSocket as fallback
    solana_ws_thread = start_solana_thread()
    if solana_ws_thread:
        threads.append(solana_ws_thread)

    # Start Polygon monitoring (Alchemy primary, PolygonScan fallback)
    polygon_thread = threading.Thread(target=print_new_polygon_transfers, daemon=True, name="Polygon")
    polygon_thread.start()
    threads.append(polygon_thread)

    # Start Bitcoin monitoring (Alchemy primary, mempool.space fallback)
    btc_thread = threading.Thread(target=poll_bitcoin_blocks, daemon=True, name="Bitcoin")
    btc_thread.start()
    threads.append(btc_thread)

    # Start Solana API block poller (most reliable Solana source)
    solana_api_thread = threading.Thread(target=print_new_solana_transfers, daemon=True, name="Solana-API")
    solana_api_thread.start()
    threads.append(solana_api_thread)

    # Start background wallet batch scorer (Phase 2)
    batch_scorer.start()

    return threads

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 8080))
    print(f"Starting Flask server on http://0.0.0.0:{port}")

    # Start monitoring threads
    monitor_threads = start_monitors()
    print(f"Started {len([t for t in monitor_threads if t and t.is_alive()])} monitoring threads")

    # Start the Flask app with SocketIO
    socketio.run(app, debug=False, host='0.0.0.0', port=port)
