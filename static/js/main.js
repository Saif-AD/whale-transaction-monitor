// Global variables
let lastTransactionTime = 0;
let knownTokens = new Set();
let socket = null;

// Initialize when the document is ready
document.addEventListener('DOMContentLoaded', function() {
    // Initialize min value from URL or use default
    const urlParams = new URLSearchParams(window.location.search);
    const minValue = urlParams.get('min_value');
    if (minValue) {
        document.getElementById('min-value-input').value = minValue;
    }

    // Set up event listeners
    document.getElementById('set-min-value').addEventListener('click', updateMinValue);
    document.getElementById('blockchain-filter').addEventListener('change', fetchTransactions);
    document.getElementById('token-filter').addEventListener('change', fetchTransactions);
    document.getElementById('type-filter').addEventListener('change', fetchTransactions);
    document.getElementById('limit-filter').addEventListener('change', fetchTransactions);

    // Initial data fetch
    fetchTransactions();
    fetchStats();

    // Connect to Socket.IO for real-time updates
    initSocketIO();

    // Stats still poll (lightweight, no real-time need)
    setInterval(fetchStats, 30000);
});

// Socket.IO real-time connection
function initSocketIO() {
    socket = io();

    socket.on('connect', function() {
        console.log('Real-time connection established');
        showToast('Live updates connected');
    });

    socket.on('disconnect', function() {
        console.log('Real-time connection lost, will auto-reconnect');
    });

    socket.on('new_transaction', function(tx) {
        handleRealtimeTransaction(tx);
    });
}

// Handle a single real-time transaction
function handleRealtimeTransaction(tx) {
    // Check if it passes current filters
    const blockchain = document.getElementById('blockchain-filter').value;
    const token = document.getElementById('token-filter').value;
    const type = document.getElementById('type-filter').value;
    const minValue = document.getElementById('min-value-input').value;

    const txBlockchain = (tx.blockchain || tx.source || '').toLowerCase();
    const txSymbol = (tx.symbol || '').toUpperCase();
    const txType = (tx.classification || '').toLowerCase();
    const txUsdValue = tx.usd_value || tx.estimated_usd || 0;

    if (blockchain && txBlockchain !== blockchain.toLowerCase()) return;
    if (token && txSymbol !== token.toUpperCase()) return;
    if (type && txType !== type.toLowerCase()) return;
    if (minValue && txUsdValue < parseFloat(minValue)) return;

    // Add token to known set
    if (tx.symbol) knownTokens.add(tx.symbol);

    // Prepend to table (newest first)
    const tableBody = document.getElementById('transactions-table');
    const timestamp = tx.timestamp ? new Date(tx.timestamp * 1000) : new Date();
    const hash = tx.tx_hash || '';
    const chain = tx.blockchain || tx.source || 'unknown';

    const newRow = document.createElement('tr');
    newRow.className = 'new-transaction';
    newRow.innerHTML = `
        <td data-label="Blockchain">
            ${getBlockchainIcon(chain)}
            ${capitalize(chain)}
        </td>
        <td data-label="Token">${tx.symbol || ''}</td>
        <td data-label="Amount">${formatNumber(tx.amount || 0)} ${tx.symbol || ''}</td>
        <td data-label="USD Value">$${formatNumber(txUsdValue)}</td>
        <td data-label="Type">
            ${buildBadge(tx, tx.classification || 'transfer')}
        </td>
        <td data-label="Time">
            <span title="${timestamp.toLocaleString()}">${formatTimeAgo(timestamp)}</span>
        </td>
        <td data-label="Transaction">
            <span class="tx-hash" onclick="copyToClipboard('${hash}')" title="Click to copy">
                ${truncateHash(hash)}
            </span>
            ${getBlockExplorerLink(chain, hash)}
        </td>
    `;

    // Insert at top of table
    tableBody.insertBefore(newRow, tableBody.firstChild);

    // Enforce row limit
    const limit = parseInt(document.getElementById('limit-filter').value) || 50;
    while (tableBody.children.length > limit) {
        tableBody.removeChild(tableBody.lastChild);
    }

    // Update count
    const count = document.getElementById('transaction-count');
    count.textContent = tableBody.children.length;

    // Update lastTransactionTime
    if (tx.timestamp > lastTransactionTime) {
        lastTransactionTime = tx.timestamp;
    }
}

// Function to fetch transactions from the API
function fetchTransactions() {
    // Get filter values
    const blockchain = document.getElementById('blockchain-filter').value;
    const token = document.getElementById('token-filter').value;
    const type = document.getElementById('type-filter').value;
    const limit = document.getElementById('limit-filter').value;
    const minValue = document.getElementById('min-value-input').value || '';
    
    // Build query string
    let queryParams = new URLSearchParams();
    if (blockchain) queryParams.append('blockchain', blockchain);
    if (token) queryParams.append('symbol', token);
    if (type) queryParams.append('type', type);
    if (limit) queryParams.append('limit', limit);
    if (minValue) queryParams.append('min_value', minValue);
    
    // Fetch data from API
    fetch(`/api/transactions?${queryParams.toString()}`)
        .then(response => response.json())
        .then(data => {
            updateTransactionsTable(data);
            updateTokenFilterOptions(data);
        })
        .catch(error => {
            console.error('Error fetching transactions:', error);
            document.getElementById('transactions-table').innerHTML = 
                `<tr><td colspan="7" class="text-center text-danger py-4">
                    <i class="fas fa-exclamation-circle me-2"></i>Error loading transactions: ${error.message}
                </td></tr>`;
        });
}

// Function to fetch statistics from the API
function fetchStats() {
    fetch('/api/stats')
        .then(response => response.json())
        .then(data => {
            updateTokenStats(data.tokens);
            updateSystemStatus(data);
        })
        .catch(error => {
            console.error('Error fetching stats:', error);
        });
}

// Function to update minimum transaction value
function updateMinValue() {
    const minValueInput = document.getElementById('min-value-input');
    const newValue = minValueInput.value;
    
    if (newValue && !isNaN(newValue) && parseFloat(newValue) > 0) {
        // Save setting on server
        fetch('/api/settings', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ min_value: parseFloat(newValue) })
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                // Update URL to reflect new min value
                const url = new URL(window.location);
                url.searchParams.set('min_value', newValue);
                window.history.replaceState({}, '', url);
                
                // Refresh data
                fetchTransactions();
                showToast('Minimum value updated successfully');
            } else {
                showToast('Error: ' + data.error, 'error');
            }
        })
        .catch(error => {
            console.error('Error updating min value:', error);
            showToast('Error updating minimum value', 'error');
        });
    } else {
        showToast('Please enter a valid positive number', 'error');
    }
}

// Function to update transactions table
function updateTransactionsTable(transactions) {
    const tableBody = document.getElementById('transactions-table');
    const count = document.getElementById('transaction-count');
    
    // Update count
    count.textContent = transactions.length;
    
    // Check if we have data
    if (transactions.length === 0) {
        tableBody.innerHTML = `
            <tr>
                <td colspan="7" class="text-center py-4">
                    <i class="fas fa-info-circle me-2"></i>No transactions matching your filters
                </td>
            </tr>
        `;
        return;
    }
    
    // Build HTML for transactions
    let html = '';
    
    transactions.forEach(tx => {
        // Extract data from transaction
        const blockchain = tx.blockchain || tx.source || 'unknown';
        const symbol = tx.symbol || '';
        const amount = tx.amount || 0;
        const usdValue = tx.usd_value || tx.estimated_usd || 0;
        const type = tx.classification || 'transfer';
        const timestamp = tx.timestamp ? new Date(tx.timestamp * 1000) : new Date();
        const hash = tx.tx_hash || '';
        
        // Format time string
        const timeStr = formatTimeAgo(timestamp);
        
        // Determine if this is a new transaction
        const isNew = tx.timestamp > lastTransactionTime;
        if (tx.timestamp > lastTransactionTime) {
            lastTransactionTime = tx.timestamp;
        }
        
        // Add token to known tokens set
        if (symbol) {
            knownTokens.add(symbol);
        }
        
        // Create row HTML
        html += `
            <tr class="${isNew ? 'new-transaction' : ''}">
                <td data-label="Blockchain">
                    ${getBlockchainIcon(blockchain)}
                    ${capitalize(blockchain)}
                </td>
                <td data-label="Token">${symbol}</td>
                <td data-label="Amount">${formatNumber(amount)} ${symbol}</td>
                <td data-label="USD Value">$${formatNumber(usdValue)}</td>
                <td data-label="Type">
                    ${buildBadge(tx, type)}
                </td>
                <td data-label="Time">
                    <span title="${timestamp.toLocaleString()}">${timeStr}</span>
                </td>
                <td data-label="Transaction">
                    <span class="tx-hash" onclick="copyToClipboard('${hash}')" title="Click to copy">
                        ${truncateHash(hash)}
                    </span>
                    ${getBlockExplorerLink(blockchain, hash)}
                </td>
            </tr>
        `;
    });
    
    tableBody.innerHTML = html;
}

// Function to update token filter options
function updateTokenFilterOptions(transactions) {
    // Add all tokens from current transactions to the set
    transactions.forEach(tx => {
        if (tx.symbol) {
            knownTokens.add(tx.symbol);
        }
    });
    
    // Get current selection
    const tokenFilter = document.getElementById('token-filter');
    const currentSelection = tokenFilter.value;
    
    // Clear existing options (except the first one)
    while (tokenFilter.options.length > 1) {
        tokenFilter.remove(1);
    }
    
    // Add options for all known tokens
    const sortedTokens = Array.from(knownTokens).sort();
    sortedTokens.forEach(token => {
        const option = document.createElement('option');
        option.value = token;
        option.textContent = token;
        tokenFilter.appendChild(option);
    });
    
    // Restore selection if possible
    if (currentSelection) {
        tokenFilter.value = currentSelection;
    }
}

// Function to update token statistics
function updateTokenStats(tokens) {
    const tableBody = document.getElementById('token-stats');
    
    // Check if we have data
    if (!tokens || tokens.length === 0) {
        tableBody.innerHTML = `
            <tr>
                <td colspan="5" class="text-center py-3">No token statistics available</td>
            </tr>
        `;
        return;
    }
    
    // Build HTML for token stats
    let html = '';
    
    // Show top 10 tokens by volume
    tokens.slice(0, 10).forEach(token => {
        const trendIcon = getTrendIcon(token.trend);
        const trendClass = getTrendClass(token.trend);
        
        html += `
            <tr>
                <td>${token.symbol}</td>
                <td>${token.buys}</td>
                <td>${token.sells}</td>
                <td>${token.buy_percentage}%</td>
                <td class="${trendClass}">${trendIcon}</td>
            </tr>
        `;
    });
    
    tableBody.innerHTML = html;
}

// Function to update system status
function updateSystemStatus(data) {
    // Update monitor badges
    const monitorBadges = document.getElementById('monitor-badges');
    if (data.monitoring && data.monitoring.active_threads) {
        let badgesHtml = '';
        data.monitoring.active_threads.forEach(thread => {
            badgesHtml += `<span class="badge bg-success">${thread}</span>`;
        });
        monitorBadges.innerHTML = badgesHtml || '<span class="badge bg-danger">No active monitors</span>';
    }
    
    // Update deduplication stats
    if (data.deduplication) {
        document.getElementById('total-transactions').textContent = formatNumber(data.deduplication.total_received);
        document.getElementById('unique-transactions').textContent = formatNumber(data.deduplication.unique_transactions);
        document.getElementById('duplicates-caught').textContent = formatNumber(data.deduplication.duplicates_caught);
        document.getElementById('dedup-rate').textContent = `${data.deduplication.dedup_ratio}%`;
    }
}

function buildEntityData(tx) {
    const from = tx.from_entity || '';
    const to = tx.to_entity || '';
    const fromAddr = tx.from || tx.from_address || '';
    const toAddr = tx.to || tx.to_address || '';
    const chain = tx.blockchain || tx.source || '';

    return {
        hasEntity: !!(from || to),
        attrs: [
            `data-from-entity="${escapeAttr(from)}"`,
            `data-to-entity="${escapeAttr(to)}"`,
            `data-from-addr="${escapeAttr(fromAddr)}"`,
            `data-to-addr="${escapeAttr(toAddr)}"`,
            `data-chain="${escapeAttr(chain)}"`,
        ].join(' ')
    };
}

function buildBadge(tx, type) {
    const ed = buildEntityData(tx);
    const color = getTypeColor(type || 'transfer');
    const entityClass = ed.hasEntity ? ' has-entity' : '';
    return `<span class="badge bg-${color} entity-badge${entityClass}" ${ed.attrs}>${capitalize(type || 'transfer')}</span>`;
}

function escapeAttr(str) {
    if (!str) return '';
    return str.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function truncateAddr(addr) {
    if (!addr) return '';
    if (addr.length <= 16) return addr;
    return addr.substring(0, 10) + '...' + addr.substring(addr.length - 6);
}

function getAddrExplorerUrl(chain, addr) {
    if (!addr) return '';
    const c = (chain || '').toLowerCase();
    if (c.includes('ethereum')) return `https://etherscan.io/address/${addr}`;
    if (c.includes('solana')) return `https://solscan.io/account/${addr}`;
    if (c.includes('xrp') || c.includes('ripple')) return `https://xrpscan.com/account/${addr}`;
    if (c.includes('bitcoin')) return `https://blockstream.info/address/${addr}`;
    if (c.includes('polygon') || c.includes('matic')) return `https://polygonscan.com/address/${addr}`;
    if (c.includes('base')) return `https://basescan.org/address/${addr}`;
    if (c.includes('arbitrum')) return `https://arbiscan.io/address/${addr}`;
    return '';
}

// Entity popover system
(function initEntityPopover() {
    const popover = document.createElement('div');
    popover.className = 'entity-popover';
    popover.id = 'entity-popover';
    document.body.appendChild(popover);

    let hideTimeout = null;
    let currentBadge = null;

    function showPopover(badge) {
        clearTimeout(hideTimeout);
        currentBadge = badge;

        const fromEntity = badge.dataset.fromEntity || '';
        const toEntity = badge.dataset.toEntity || '';
        const fromAddr = badge.dataset.fromAddr || '';
        const toAddr = badge.dataset.toAddr || '';
        const chain = badge.dataset.chain || '';
        const classification = badge.textContent.trim();

        let html = '';
        html += `<div class="entity-popover-header">`;
        html += `<span>Wallet Details</span>`;
        html += `<span class="entity-tag ${fromEntity || toEntity ? 'known' : 'unknown'}">${fromEntity || toEntity ? 'Identified' : 'Unknown'}</span>`;
        html += `</div>`;
        html += `<div class="entity-popover-body">`;

        // From section
        if (fromAddr) {
            html += `<div class="entity-section">`;
            html += `<div class="entity-section-label">From</div>`;
            if (fromEntity) {
                html += `<div class="entity-name">${escapeHtml(fromEntity)}</div>`;
            }
            html += `<div class="entity-addr-row">`;
            html += `<span class="addr-text" data-copy="${escapeAttr(fromAddr)}">${truncateAddr(fromAddr)}</span>`;
            html += `<i class="fas fa-copy copy-btn" data-copy="${escapeAttr(fromAddr)}" title="Copy address"></i>`;
            const fromUrl = getAddrExplorerUrl(chain, fromAddr);
            if (fromUrl) {
                html += `<a href="${fromUrl}" target="_blank" class="explorer-btn" title="View on explorer"><i class="fas fa-external-link-alt"></i></a>`;
            }
            html += `</div></div>`;
        }

        // Arrow
        if (fromAddr && toAddr) {
            html += `<div class="entity-arrow"><i class="fas fa-arrow-down"></i></div>`;
        }

        // To section
        if (toAddr) {
            html += `<div class="entity-section">`;
            html += `<div class="entity-section-label">To</div>`;
            if (toEntity) {
                html += `<div class="entity-name">${escapeHtml(toEntity)}</div>`;
            }
            html += `<div class="entity-addr-row">`;
            html += `<span class="addr-text" data-copy="${escapeAttr(toAddr)}">${truncateAddr(toAddr)}</span>`;
            html += `<i class="fas fa-copy copy-btn" data-copy="${escapeAttr(toAddr)}" title="Copy address"></i>`;
            const toUrl = getAddrExplorerUrl(chain, toAddr);
            if (toUrl) {
                html += `<a href="${toUrl}" target="_blank" class="explorer-btn" title="View on explorer"><i class="fas fa-external-link-alt"></i></a>`;
            }
            html += `</div></div>`;
        }

        html += `</div>`;

        // Footer with classification
        html += `<div class="entity-popover-footer">`;
        html += `<span>${capitalize(classification)}</span>`;
        html += `<span>${capitalize(chain)}</span>`;
        html += `</div>`;

        popover.innerHTML = html;

        // Position near the badge
        popover.style.visibility = 'hidden';
        popover.style.display = 'block';
        const rect = badge.getBoundingClientRect();
        const popW = popover.offsetWidth || 340;
        const popH = popover.offsetHeight || 200;
        popover.style.display = '';
        popover.style.visibility = '';

        let left = rect.left + rect.width / 2 - popW / 2;
        let top = rect.bottom + 8;

        if (left < 8) left = 8;
        if (left + popW > window.innerWidth - 8) left = window.innerWidth - popW - 8;
        if (top + popH > window.innerHeight - 8) {
            top = rect.top - popH - 8;
        }

        popover.style.left = left + 'px';
        popover.style.top = top + 'px';
        popover.classList.add('visible');
    }

    function hidePopover() {
        hideTimeout = setTimeout(() => {
            popover.classList.remove('visible');
            currentBadge = null;
        }, 200);
    }

    // Delegated events on document
    document.addEventListener('mouseenter', function(e) {
        const badge = e.target.closest('.entity-badge');
        if (badge) showPopover(badge);
    }, true);

    document.addEventListener('mouseleave', function(e) {
        const badge = e.target.closest('.entity-badge');
        if (badge) hidePopover();
    }, true);

    // Keep popover open when hovering over it
    popover.addEventListener('mouseenter', function() {
        clearTimeout(hideTimeout);
    });
    popover.addEventListener('mouseleave', function() {
        hidePopover();
    });

    // Copy handler inside popover
    popover.addEventListener('click', function(e) {
        const copyEl = e.target.closest('[data-copy]');
        if (copyEl) {
            const text = copyEl.dataset.copy;
            navigator.clipboard.writeText(text).then(() => showToast('Address copied'));
        }
    });
})();

function escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

// Helper function to format number with commas
function formatNumber(num) {
    if (num === undefined || num === null) return '0';
    
    // Convert to number if it's a string
    const value = typeof num === 'string' ? parseFloat(num) : num;
    
    // Check if it's a valid number
    if (isNaN(value)) return '0';
    
    // Format based on size
    if (value >= 1000000) {
        return (value / 1000000).toFixed(2) + 'M';
    } else if (value >= 1000) {
        return (value / 1000).toFixed(2) + 'K';
    } else if (value >= 1) {
        return value.toFixed(2);
    } else {
        // For very small numbers, use scientific notation
        return value.toFixed(6);
    }
}

// Helper function to format time ago
function formatTimeAgo(date) {
    const now = new Date();
    const seconds = Math.floor((now - date) / 1000);
    
    if (seconds < 60) {
        return `${seconds}s ago`;
    } else if (seconds < 3600) {
        return `${Math.floor(seconds / 60)}m ago`;
    } else if (seconds < 86400) {
        return `${Math.floor(seconds / 3600)}h ago`;
    } else {
        return `${Math.floor(seconds / 86400)}d ago`;
    }
}

// Helper function to capitalize first letter
function capitalize(str) {
    if (!str) return '';
    return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
}

// Helper function to truncate transaction hash
function truncateHash(hash) {
    if (!hash) return '';
    if (hash.length <= 16) return hash;
    return hash.substring(0, 8) + '...' + hash.substring(hash.length - 6);
}

// Helper function to get blockchain icon
function getBlockchainIcon(blockchain) {
    const chain = blockchain.toLowerCase();
    let icon = '';
    
    if (chain.includes('ethereum')) {
        icon = '<i class="fab fa-ethereum blockchain-ethereum me-1"></i>';
    } else if (chain.includes('solana')) {
        icon = '<i class="fas fa-sun blockchain-solana me-1"></i>';
    } else if (chain.includes('xrp') || chain.includes('ripple')) {
        icon = '<i class="fas fa-circle blockchain-xrp me-1"></i>';
    } else if (chain.includes('bitcoin')) {
        icon = '<i class="fab fa-bitcoin blockchain-bitcoin me-1"></i>';
    } else if (chain.includes('polygon') || chain.includes('matic')) {
        icon = '<i class="fas fa-diamond me-1" style="color: #8247e5;"></i>';
    } else if (chain.includes('base')) {
        icon = '<i class="fas fa-layer-group me-1" style="color: #0052ff;"></i>';
    } else if (chain.includes('arbitrum')) {
        icon = '<i class="fas fa-infinity me-1" style="color: #28a0f0;"></i>';
    } else {
        icon = '<i class="fas fa-link me-1"></i>';
    }
    
    return icon;
}

// Helper function to get transaction type color
function getTypeColor(type) {
    const lowerType = type.toLowerCase();
    if (lowerType.includes('buy')) return 'success';
    if (lowerType.includes('sell')) return 'danger';
    return 'secondary';
}

// Helper function to get trend icon
function getTrendIcon(trend) {
    if (trend === 'bullish') return '<i class="fas fa-arrow-up"></i> Bullish';
    if (trend === 'bearish') return '<i class="fas fa-arrow-down"></i> Bearish';
    return '<i class="fas fa-minus"></i> Neutral';
}

// Helper function to get trend class
function getTrendClass(trend) {
    if (trend === 'bullish') return 'trend-up';
    if (trend === 'bearish') return 'trend-down';
    return 'trend-neutral';
}

// Helper function to get block explorer link
function getBlockExplorerLink(blockchain, hash) {
    if (!hash) return '';
    
    let url = '';
    const chain = blockchain.toLowerCase();
    
    if (chain.includes('ethereum')) {
        url = `https://etherscan.io/tx/${hash}`;
    } else if (chain.includes('solana')) {
        url = `https://solscan.io/tx/${hash}`;
    } else if (chain.includes('xrp') || chain.includes('ripple')) {
        url = `https://xrpscan.com/tx/${hash}`;
    } else if (chain.includes('bitcoin')) {
        url = `https://blockstream.info/tx/${hash}`;
    } else if (chain.includes('polygon') || chain.includes('matic')) {
        url = `https://polygonscan.com/tx/${hash}`;
    } else if (chain.includes('base')) {
        url = `https://basescan.org/tx/${hash}`;
    } else if (chain.includes('arbitrum')) {
        url = `https://arbiscan.io/tx/${hash}`;
    } else {
        return '';
    }
    
    return `<a href="${url}" target="_blank" class="ms-2"><i class="fas fa-external-link-alt"></i></a>`;
}

// Function to copy text to clipboard
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(() => {
        showToast('Copied to clipboard');
    }).catch(err => {
        console.error('Could not copy text: ', err);
        showToast('Failed to copy', 'error');
    });
}

// Function to show toast notification
function showToast(message, type = 'success') {
    // Create toast container if it doesn't exist
    let toastContainer = document.querySelector('.toast-container');
    if (!toastContainer) {
        toastContainer = document.createElement('div');
        toastContainer.className = 'toast-container position-fixed bottom-0 end-0 p-3';
        document.body.appendChild(toastContainer);
    }
    
    // Create toast
    const toastId = 'toast-' + Date.now();
    const toastHtml = `
        <div id="${toastId}" class="toast" role="alert" aria-live="assertive" aria-atomic="true">
            <div class="toast-header ${type === 'error' ? 'bg-danger text-white' : 'bg-success text-white'}">
                <strong class="me-auto">${type === 'error' ? 'Error' : 'Success'}</strong>
                <button type="button" class="btn-close btn-close-white" data-bs-dismiss="toast" aria-label="Close"></button>
            </div>
            <div class="toast-body">
                ${message}
            </div>
        </div>
    `;
    
    // Add toast to container
    toastContainer.insertAdjacentHTML('beforeend', toastHtml);
    
    // Initialize and show the toast
    const toastElement = document.getElementById(toastId);
    const toast = new bootstrap.Toast(toastElement, { delay: 5000 });
    toast.show();
    
    // Remove toast after it's hidden
    toastElement.addEventListener('hidden.bs.toast', function () {
        toastElement.remove();
    });
}