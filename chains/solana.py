import json
import time
import threading
import traceback
import websocket
from typing import Dict, Optional
from config.api_keys import HELIUS_API_KEY
from config.settings import (
    GLOBAL_USD_THRESHOLD,
    solana_previous_balances,
    solana_buy_counts,
    solana_sell_counts,
    shutdown_flag,
    print_lock
)
from data.tokens import SOL_TOKENS_TO_MONITOR, TOKEN_PRICES
from utils.classification_final import enhanced_solana_classification
from utils.base_helpers import safe_print, log_error
from utils.summary import record_transfer
from utils.summary import has_been_classified, mark_as_classified
from utils.dedup import deduplicator, get_dedup_stats, deduped_transactions, handle_event




total_transfers_fetched = 0
filtered_by_threshold = 0

# Track seen transaction hashes to prevent double-counting DEX swap sides
# Each Solana DEX swap fires two account updates (buyer + seller)
# We only want to record the WHALE side, not the DEX pool/vault side
_seen_solana_tx_hashes = {}  # tx_hash -> (timestamp, classification, is_pool)
_SOLANA_DEDUP_WINDOW = 30  # seconds

# Known DEX pool/vault program addresses — these are passive counterparties, not whales
_SOLANA_POOL_ADDRESSES = {
    'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',   # Jupiter V6
    'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB',   # Jupiter V4
    'jupoNjAxXgZ4rjzxzPMP4oxduvQsQtZzyknqvzYNrNu',   # Jupiter Limit Order
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',  # Raydium AMM V4
    'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK',  # Raydium CLMM
    'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C',  # Raydium CPMM
    'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',   # Orca Whirlpool
    '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',  # Orca V2
    'srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX',   # OpenBook V1
    'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo',   # Meteora DLMM
    'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB',  # Meteora Pools
    'PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY',   # Phoenix DEX
    'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH',   # Drift V2
    'MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD',   # Marinade Staking
}

def _is_pool_address(addr):
    """Check if an address is a known DEX pool/vault (passive counterparty)."""
    return addr in _SOLANA_POOL_ADDRESSES

def _solana_tx_already_seen(tx_hash, classification, owner):
    """
    Check if we've already processed one side of this Solana transaction.
    For DEX swaps, both the whale wallet and the DEX pool fire events.
    We keep the WHALE side (non-pool address) and skip the pool side.
    Returns True if this event should be skipped.
    """
    now = time.time()
    
    # Clean old entries
    expired = [k for k, v in _seen_solana_tx_hashes.items() if now - v[0] > _SOLANA_DEDUP_WINDOW]
    for k in expired:
        del _seen_solana_tx_hashes[k]
    
    this_is_pool = _is_pool_address(owner)
    
    if tx_hash in _seen_solana_tx_hashes:
        prev_time, prev_class, prev_is_pool = _seen_solana_tx_hashes[tx_hash]
        
        if prev_is_pool and not this_is_pool:
            # Previous was the pool side, this is the whale — replace with whale side
            _seen_solana_tx_hashes[tx_hash] = (now, classification, this_is_pool)
            return False
        elif not prev_is_pool and this_is_pool:
            # Previous was the whale, this is the pool — skip pool side
            return True
        else:
            # Both are pool or both are whale — keep first one, skip duplicate
            return True
    
    # First time seeing this tx_hash
    _seen_solana_tx_hashes[tx_hash] = (now, classification, this_is_pool)
    return False

# In solana.py - update the on_solana_message function

# In solana.py - Update the on_solana_message function

def on_solana_message(ws, message):
    try:
        global total_transfers_fetched
        total_transfers_fetched += 1
        
        data = json.loads(message)
        if "params" not in data:
            return

        result = data["params"].get("result", {})
        if "value" not in result:
            return

        value = result["value"]
        if "account" not in value or "data" not in value["account"]:
            return

        parsed_data = value["account"]["data"].get("parsed", {})
        if parsed_data.get("type") != "account":
            return

        info = parsed_data.get("info", {})
        mint = info.get("mint")
        raw_amount = info.get("tokenAmount", {}).get("uiAmount", 0)
        current_amount = float(raw_amount) if raw_amount is not None else 0.0
        owner = info.get("owner")
        context = data["params"].get("result", {}).get("context", {})
        slot = context.get("slot", 0)
        pubkey = value.get("pubkey", "")
        tx_hash = f"sol_ws_{slot}_{pubkey[:16]}"

        if not owner or not mint:
            return
            
        # Get previous state
        prev_owner = None
        prev_amount = 0.0
        
        if mint in solana_previous_balances:
            prev_owner = solana_previous_balances[mint].get("owner")
            
        if owner in solana_previous_balances:
            prev_amount = float(solana_previous_balances.get(owner, {}).get(mint, 0))
            
        amount_change = current_amount - prev_amount
        
        # Skip negligible changes (can be noise)
        if abs(amount_change) < 0.0001:
            return

        # Check monitored tokens
        for symbol, token_info in SOL_TOKENS_TO_MONITOR.items():
            if token_info["mint"] == mint:
                price = TOKEN_PRICES.get(symbol, 0)
                usd_value = abs(amount_change) * price
                from data.tokens import SOL_TOKENS_TO_MONITOR
                token_min = SOL_TOKENS_TO_MONITOR.get(symbol, {}).get("min_threshold", GLOBAL_USD_THRESHOLD)
                min_threshold = max(token_min, GLOBAL_USD_THRESHOLD)

                if usd_value < min_threshold:
                    continue

                # Create standardized event with unique transaction identifier
                # Use a combination of tx_hash, owner, and amount for better uniqueness
                unique_id = f"{tx_hash}_{owner}_{amount_change:.6f}"
                
                event = {
                    "blockchain": "solana",
                    "tx_hash": unique_id,  # Use enhanced ID to avoid duplicates
                    "original_hash": tx_hash,  # Keep original hash for reference
                    "from": prev_owner or "unknown",
                    "to": owner,
                    "amount": abs(amount_change),
                    "symbol": symbol,
                    "usd_value": usd_value,
                    "timestamp": time.time(),
                    "source": "solana"
                }

                # Classify BEFORE dedup so classification is present in stored event
                classification, confidence = enhanced_solana_classification(
                    owner=owner,
                    prev_owner=prev_owner,
                    amount_change=amount_change,
                    tx_hash=tx_hash,
                    token=symbol,
                    source="solana"
                )

                # Add classification to the event before dedup
                event["classification"] = classification

                # Skip duplicate side of DEX swaps (same tx_hash, opposite classification)
                if _solana_tx_already_seen(tx_hash, classification, owner):
                    continue

                # Check if it's a duplicate before processing
                if not handle_event(event):
                    continue
                
                # Only count transactions with sufficient confidence
                if confidence >= 2:  # Increased confidence threshold
                    if classification == "buy":
                        solana_buy_counts[symbol] += 1
                    elif classification == "sell":
                        solana_sell_counts[symbol] += 1

                    # Print transaction details
                    current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    safe_print(f"\n[{symbol} | ${usd_value:,.2f} USD] Solana {classification.upper()}")
                    safe_print(f"  Time: {current_time}")
                    safe_print(f"  TX Hash: {tx_hash[:16]}...")
                    safe_print(f"  Amount: {abs(amount_change):,.2f} {symbol}")
                    safe_print(f"  Classification: {classification} (confidence: {confidence})")

                # Whale tx enrichment order — DO NOT REORDER:
                # 1. classify (sets classification, confidence, whale_score, template reasoning)
                # 2. lookup_labels (sets from_label, to_label from addresses table)
                # 3. generate_interpretation (uses labels to write audience-facing reasoning)
                # 4. store_transaction (writes the enriched row)
                if confidence >= 2:
                    try:
                        from utils.supabase_writer import store_transaction
                        from shared.address_labels import lookup_labels
                        classification_data = {
                            'classification': classification.upper(),
                            'confidence': float(confidence) / 10.0,
                            'whale_score': 0.0,
                            'reasoning': f'Solana WS classification: {classification}',
                        }
                        # Step 2: label lookup
                        from_label, to_label = lookup_labels(
                            event.get('from', ''), event.get('to', ''), 'solana',
                        )
                        classification_data['from_label'] = from_label
                        classification_data['to_label'] = to_label
                        # Step 3: interpreter (audience-facing reasoning)
                        from shared.config import (
                            INTERPRETER_ENABLED, INTERPRETER_LABELED_USD_THRESHOLD,
                            INTERPRETER_UNLABELED_USD_THRESHOLD,
                        )
                        template_reasoning = classification_data['reasoning']
                        usd_val = float(event.get('usd_value', 0) or 0)
                        has_labels = bool(from_label or to_label)
                        interp_threshold = INTERPRETER_LABELED_USD_THRESHOLD if has_labels else INTERPRETER_UNLABELED_USD_THRESHOLD
                        if INTERPRETER_ENABLED and usd_val >= interp_threshold:
                            try:
                                from shared.interpreter import generate_interpretation
                                import logging as _sol_logging
                                tx_for_interp = {
                                    'transaction_hash': event.get('tx_hash', ''),
                                    'token_symbol': event.get('symbol', ''),
                                    'usd_value': usd_val,
                                    'blockchain': 'solana',
                                    **classification_data,
                                }
                                classification_data['reasoning'] = generate_interpretation(tx_for_interp)
                            except Exception as e:
                                _sol_logging.getLogger(__name__).warning("Interpreter failed for %s, falling back: %s", event.get('tx_hash', '?')[:16], e)
                                classification_data['reasoning'] = template_reasoning
                        store_transaction(event, classification_data)
                    except Exception:
                        pass

                # Update balance tracking
                if owner not in solana_previous_balances:
                    solana_previous_balances[owner] = {}
                solana_previous_balances[owner][mint] = current_amount

    except Exception as e:
        error_msg = f"Error processing Solana transfer: {str(e)}"
        safe_print(error_msg)
        log_error(error_msg)
        traceback.print_exc()


def connect_solana_websocket(retry_count=0, max_retries=5):
    from config.api_keys import ALCHEMY_API_KEY
    if not ALCHEMY_API_KEY or len(ALCHEMY_API_KEY) < 10:
        safe_print("Solana WS: ALCHEMY_API_KEY missing or invalid.")
        return None

    ws_url = f"wss://solana-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"

    def on_open(ws):
        safe_print("✅ Solana WebSocket connected – subscribing to SPL token transfers...")
        subscribe_msg = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "programSubscribe",
            "params": [
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed"
                }
            ]
        }
        ws.send(json.dumps(subscribe_msg))

    def on_error(ws, error):
        error_msg = f"Solana WS error: {type(error).__name__}: {str(error)[:200]}"
        safe_print(error_msg)
        log_error(error_msg)

    def on_close(ws, close_status_code, close_msg):
        if not shutdown_flag.is_set():
            nonlocal retry_count
            retry_count += 1
            if retry_count <= max_retries:
                wait_time = min(30, 2 ** retry_count)
                safe_print(f"Solana WS closed (code: {close_status_code}). Reconnecting in {wait_time}s ({retry_count}/{max_retries})...")
                time.sleep(wait_time)
                connect_solana_websocket(retry_count, max_retries)
            else:
                safe_print(f"Solana WS: max retries ({max_retries}) reached. Giving up.")

    ws_app = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_solana_message,
        on_error=on_error,
        on_close=on_close
    )

    ws_thread = threading.Thread(
        target=ws_app.run_forever,
        kwargs={"ping_interval": 60},
        daemon=True
    )
    ws_thread.start()
    return ws_thread


def start_solana_thread():
    """Start Solana WebSocket monitoring thread."""
    try:
        thread = connect_solana_websocket()
        if thread:
            thread.name = "Solana-WS"
            return thread
        else:
            safe_print("⚠️  Solana WebSocket monitor could not be started.")
            return None
    except Exception as e:
        safe_print(f"Error starting Solana thread: {e}")
        traceback.print_exc()
        return None