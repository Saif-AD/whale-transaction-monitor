/**
 * Wallet Tracker - Client-side JS for tracker and wallet profile pages.
 */

// --- Utilities ---
function formatUSD(val) {
    if (val == null) return '$0';
    const n = parseFloat(val);
    if (n >= 1e9) return '$' + (n / 1e9).toFixed(2) + 'B';
    if (n >= 1e6) return '$' + (n / 1e6).toFixed(2) + 'M';
    if (n >= 1e3) return '$' + (n / 1e3).toFixed(1) + 'K';
    return '$' + n.toFixed(0);
}

function truncAddr(addr) {
    if (!addr) return '';
    return addr.length > 12 ? addr.slice(0, 6) + '...' + addr.slice(-4) : addr;
}

function timeAgo(ts) {
    if (!ts) return '';
    const d = new Date(ts);
    const diff = (Date.now() - d.getTime()) / 1000;
    if (diff < 60) return Math.floor(diff) + 's ago';
    if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
    if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
    return Math.floor(diff / 86400) + 'd ago';
}

function classificationBadge(cls) {
    const c = (cls || '').toUpperCase();
    if (c === 'BUY') return '<span class="badge badge-buy">BUY</span>';
    if (c === 'SELL') return '<span class="badge badge-sell">SELL</span>';
    return '<span class="badge badge-transfer">TRANSFER</span>';
}

function chainBadge(chain) {
    const colors = {ethereum:'#627eea', bitcoin:'#f7931a', solana:'#14f195', polygon:'#8247e5', xrp:'#23292f'};
    const c = (chain || '').toLowerCase();
    const color = colors[c] || '#6c757d';
    return `<span class="badge" style="background:${color}">${c || 'unknown'}</span>`;
}

function tagBadges(tags) {
    if (!tags || !tags.length) return '';
    const colors = {
        whale: 'primary', smart_money: 'success', degen: 'danger',
        accumulator: 'info', distributor: 'warning', market_maker: 'secondary',
        institutional: 'dark', early_buyer: 'success'
    };
    return tags.map(t => `<span class="badge bg-${colors[t] || 'secondary'} me-1">${t}</span>`).join('');
}

function getExplorerLink(chain, hash) {
    const explorers = {
        ethereum: 'https://etherscan.io/tx/',
        bitcoin: 'https://mempool.space/tx/',
        solana: 'https://solscan.io/tx/',
        polygon: 'https://polygonscan.com/tx/',
        xrp: 'https://xrpscan.com/tx/',
    };
    const base = explorers[(chain || '').toLowerCase()] || '#';
    return base + (hash || '');
}

function getAddressExplorerLink(chain, addr) {
    const explorers = {
        ethereum: 'https://etherscan.io/address/',
        bitcoin: 'https://mempool.space/address/',
        solana: 'https://solscan.io/account/',
        polygon: 'https://polygonscan.com/address/',
        xrp: 'https://xrpscan.com/account/',
    };
    const base = explorers[(chain || '').toLowerCase()] || '#';
    return base + (addr || '');
}

// --- Socket.IO ---
let socket;
const alertHistory = [];

function initSocket() {
    socket = io();
    socket.on('connect', () => console.log('Tracker socket connected'));
    socket.on('wallet_alert', (data) => {
        alertHistory.unshift(data);
        if (alertHistory.length > 50) alertHistory.pop();
        showAlertToast(data);
        renderAlertHistory();
    });
}

function showAlertToast(data) {
    const tx = data.transaction || {};
    const body = document.getElementById('alert-toast-body');
    const time = document.getElementById('alert-toast-time');
    if (body) {
        body.innerHTML = `<strong>${truncAddr(data.address)}</strong> - ${tx.symbol || ''} ${classificationBadge(tx.classification)} ${formatUSD(tx.usd_value)}`;
    }
    if (time) time.textContent = 'just now';
    const toastEl = document.getElementById('alert-toast');
    if (toastEl) {
        const toast = new bootstrap.Toast(toastEl, {delay: 8000});
        toast.show();
    }
}

function renderAlertHistory() {
    const container = document.getElementById('alert-history');
    if (!container) return;
    if (!alertHistory.length) {
        container.innerHTML = '<div class="text-center py-3 text-muted">No alerts yet</div>';
        return;
    }
    container.innerHTML = alertHistory.slice(0, 20).map(a => {
        const tx = a.transaction || {};
        return `<a href="/tracker/${a.address}" class="list-group-item list-group-item-action">
            <div class="d-flex justify-content-between">
                <small><strong>${truncAddr(a.address)}</strong></small>
                <small class="text-muted">${timeAgo(a.triggered_at)}</small>
            </div>
            <small>${tx.symbol || ''} ${(tx.classification || '').toUpperCase()} ${formatUSD(tx.usd_value)}</small>
        </a>`;
    }).join('');
}

// --- Tracker Landing Page ---
function isTrackerPage() {
    return typeof WALLET_ADDRESS === 'undefined';
}

function initTrackerPage() {
    fetchWatchlists();
    fetchLeaderboard();

    document.getElementById('search-btn')?.addEventListener('click', doSearch);
    document.getElementById('wallet-search')?.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') doSearch();
    });
    document.getElementById('create-watchlist-btn')?.addEventListener('click', createWatchlist);
    document.getElementById('leaderboard-sort')?.addEventListener('change', fetchLeaderboard);
    document.getElementById('leaderboard-chain')?.addEventListener('change', fetchLeaderboard);
}

async function doSearch() {
    const q = document.getElementById('wallet-search')?.value?.trim();
    if (!q) return;

    // If it looks like a full address, go directly to profile
    if (q.length >= 30) {
        const chain = document.getElementById('chain-select')?.value || '';
        window.location.href = `/tracker/${q}` + (chain ? `?chain=${chain}` : '');
        return;
    }

    try {
        const res = await fetch(`/api/wallet/search?q=${encodeURIComponent(q)}`);
        const data = await res.json();
        const container = document.getElementById('search-results');
        const list = document.getElementById('search-results-list');
        if (!data.length) {
            list.innerHTML = '<div class="list-group-item text-muted">No results found</div>';
        } else {
            list.innerHTML = data.map(r => `<a href="/tracker/${r.address}" class="list-group-item list-group-item-action">
                <div class="d-flex justify-content-between">
                    <code>${truncAddr(r.address)}</code>
                    <span>${r.entity_name || ''}</span>
                </div>
                ${r.tags ? tagBadges(r.tags) : ''}
            </a>`).join('');
        }
        container.style.display = 'block';
    } catch (e) {
        console.error('Search failed:', e);
    }
}

async function fetchWatchlists() {
    const container = document.getElementById('watchlists-container');
    if (!container) return;
    try {
        const res = await fetch('/api/watchlist');
        const data = await res.json();
        if (!data.length) {
            container.innerHTML = '<div class="text-center py-3 text-muted">No watchlists yet. Create one!</div>';
            return;
        }
        container.innerHTML = '<div class="list-group list-group-flush">' + data.map(wl => `
            <div class="list-group-item d-flex justify-content-between align-items-center">
                <div>
                    <strong>${wl.name}</strong>
                    <small class="text-muted ms-2">${wl.address_count || 0} addresses</small>
                </div>
                <div>
                    <button class="btn btn-sm btn-outline-danger" onclick="deleteWatchlist('${wl.id}')">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            </div>
        `).join('') + '</div>';
    } catch (e) {
        container.innerHTML = '<div class="text-center py-3 text-danger">Failed to load</div>';
    }
}

async function createWatchlist() {
    const name = document.getElementById('new-watchlist-name')?.value?.trim();
    if (!name) return;
    try {
        await fetch('/api/watchlist', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({name})
        });
        bootstrap.Modal.getInstance(document.getElementById('createWatchlistModal'))?.hide();
        document.getElementById('new-watchlist-name').value = '';
        fetchWatchlists();
    } catch (e) {
        console.error('Create watchlist failed:', e);
    }
}

async function deleteWatchlist(id) {
    if (!confirm('Delete this watchlist?')) return;
    try {
        await fetch(`/api/watchlist/${id}`, {method: 'DELETE'});
        fetchWatchlists();
    } catch (e) {
        console.error('Delete failed:', e);
    }
}

async function fetchLeaderboard() {
    const tbody = document.getElementById('leaderboard-table');
    if (!tbody) return;
    const sort = document.getElementById('leaderboard-sort')?.value || 'smart_money_score';
    const chain = document.getElementById('leaderboard-chain')?.value || '';

    try {
        const params = new URLSearchParams({sort_by: sort, limit: '50'});
        if (chain) params.set('chain', chain);
        const res = await fetch(`/api/wallet/leaderboard?${params}`);
        const data = await res.json();
        if (!data.length) {
            tbody.innerHTML = '<tr><td colspan="7" class="text-center py-4 text-muted">No data yet. Profiles are built as wallets are accessed.</td></tr>';
            return;
        }
        tbody.innerHTML = data.map((w, i) => `<tr>
            <td>${i + 1}</td>
            <td><a href="/tracker/${w.address}">${w.entity_name || truncAddr(w.address)}</a></td>
            <td>${chainBadge(w.chain)}</td>
            <td><strong>${(w.smart_money_score || 0).toFixed(2)}</strong></td>
            <td>${formatUSD(w.total_volume_usd_30d)}</td>
            <td>${tagBadges(w.tags || [])}</td>
            <td>${timeAgo(w.last_active)}</td>
        </tr>`).join('');
    } catch (e) {
        tbody.innerHTML = '<tr><td colspan="7" class="text-center py-4 text-danger">Failed to load</td></tr>';
    }
}

// --- Wallet Profile Page ---
let txOffset = 0;
const TX_PAGE_SIZE = 50;

function initProfilePage() {
    fetchProfile();
    fetchTransactions();
    fetchCounterparties();
    loadWatchlistsForModal();

    document.getElementById('load-more-btn')?.addEventListener('click', () => fetchTransactions(true));
    document.getElementById('add-to-watchlist-btn')?.addEventListener('click', addToWatchlist);
    document.getElementById('set-alert-btn')?.addEventListener('click', setAlert);
    document.getElementById('alert-telegram')?.addEventListener('change', (e) => {
        document.getElementById('telegram-chat-id-group').style.display = e.target.checked ? 'block' : 'none';
    });
}

async function fetchProfile() {
    try {
        const params = WALLET_CHAIN ? `?chain=${WALLET_CHAIN}` : '';
        const res = await fetch(`/api/wallet/profile/${WALLET_ADDRESS}${params}`);
        const p = await res.json();

        document.getElementById('profile-entity-name').textContent = p.entity_name || truncAddr(p.address);
        document.getElementById('profile-chain-badge').textContent = p.chain || 'multi-chain';
        document.getElementById('profile-tags').innerHTML = tagBadges(p.tags || []);

        // Score
        const score = p.smart_money_score || 0;
        document.getElementById('score-value').textContent = score.toFixed(2);
        const bar = document.getElementById('score-bar');
        bar.style.width = (score * 100) + '%';
        bar.className = 'progress-bar ' + (score >= 0.7 ? 'bg-success' : score >= 0.4 ? 'bg-warning' : 'bg-danger');

        // Holdings
        document.getElementById('portfolio-value').textContent = formatUSD(p.portfolio_value_usd);
        const tokensList = document.getElementById('top-tokens-list');
        if (p.top_tokens && p.top_tokens.length) {
            tokensList.innerHTML = p.top_tokens.map(t =>
                `<div class="d-flex justify-content-between"><span>${t.symbol}</span><small class="text-muted">${t.count} txns</small></div>`
            ).join('');
        } else {
            tokensList.innerHTML = '<span class="text-muted">No token data</span>';
        }

        // PnL
        const pnl = p.pnl_estimated_usd || 0;
        const pnlEl = document.getElementById('pnl-value');
        pnlEl.textContent = formatUSD(Math.abs(pnl));
        pnlEl.className = 'mb-0 ' + (pnl >= 0 ? 'text-success' : 'text-danger');
        if (pnl < 0) pnlEl.textContent = '-' + pnlEl.textContent;

        document.getElementById('volume-30d').textContent = formatUSD(p.total_volume_usd_30d);
        document.getElementById('tx-count').textContent = p.tx_count_all || 0;
        document.getElementById('buy-count').textContent = p.buy_count || 0;
        document.getElementById('sell-count').textContent = p.sell_count || 0;
    } catch (e) {
        console.error('Profile fetch failed:', e);
    }
}

async function fetchTransactions(loadMore) {
    if (loadMore) txOffset += TX_PAGE_SIZE;
    else txOffset = 0;

    const tbody = document.getElementById('tx-history-table');
    if (!tbody) return;

    try {
        const params = new URLSearchParams({limit: TX_PAGE_SIZE, offset: txOffset});
        if (WALLET_CHAIN) params.set('chain', WALLET_CHAIN);
        const res = await fetch(`/api/wallet/transactions/${WALLET_ADDRESS}?${params}`);
        const data = await res.json();

        const rows = data.map(tx => {
            const chain = tx.blockchain || '';
            const cp = tx.counterparty_address || (tx.from_address === WALLET_ADDRESS ? tx.to_address : tx.from_address) || '';
            return `<tr>
                <td>${chainBadge(chain)}</td>
                <td>${tx.token_symbol || ''}</td>
                <td>${formatUSD(tx.usd_value)}</td>
                <td>${classificationBadge(tx.classification)}</td>
                <td><a href="/tracker/${cp}">${truncAddr(cp)}</a></td>
                <td>${timeAgo(tx.timestamp)}</td>
                <td><a href="${getExplorerLink(chain, tx.transaction_hash)}" target="_blank" class="tx-hash">${truncAddr(tx.transaction_hash)}</a></td>
            </tr>`;
        }).join('');

        if (loadMore && txOffset > 0) {
            tbody.innerHTML += rows;
        } else {
            tbody.innerHTML = rows || '<tr><td colspan="7" class="text-center py-4 text-muted">No transactions found</td></tr>';
        }

        document.getElementById('tx-total-count').textContent = txOffset + data.length;
        document.getElementById('load-more-btn').style.display = data.length >= TX_PAGE_SIZE ? '' : 'none';
    } catch (e) {
        console.error('Transactions fetch failed:', e);
    }
}

async function fetchCounterparties() {
    const tbody = document.getElementById('counterparties-table');
    if (!tbody) return;
    try {
        const params = WALLET_CHAIN ? `?chain=${WALLET_CHAIN}` : '';
        const res = await fetch(`/api/wallet/counterparties/${WALLET_ADDRESS}${params}`);
        const data = await res.json();
        if (!data.length) {
            tbody.innerHTML = '<tr><td colspan="3" class="text-center py-3 text-muted">No data</td></tr>';
            return;
        }
        tbody.innerHTML = data.map(cp => `<tr>
            <td><a href="/tracker/${cp.address}">${cp.label || truncAddr(cp.address)}</a></td>
            <td>${cp.tx_count}</td>
            <td>${formatUSD(cp.total_usd)}</td>
        </tr>`).join('');
    } catch (e) {
        console.error('Counterparties fetch failed:', e);
    }
}

async function loadWatchlistsForModal() {
    const select = document.getElementById('watchlist-select');
    if (!select) return;
    try {
        const res = await fetch('/api/watchlist');
        const data = await res.json();
        select.innerHTML = data.map(wl =>
            `<option value="${wl.id}">${wl.name}</option>`
        ).join('') || '<option value="">No watchlists - create one first</option>';
    } catch (e) {
        select.innerHTML = '<option value="">Failed to load</option>';
    }
}

async function addToWatchlist() {
    const wlId = document.getElementById('watchlist-select')?.value;
    if (!wlId) return alert('Select a watchlist first');
    try {
        await fetch(`/api/watchlist/${wlId}/addresses`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                address: WALLET_ADDRESS,
                chain: WALLET_CHAIN,
                custom_label: document.getElementById('custom-label')?.value || '',
                notes: document.getElementById('watchlist-notes')?.value || '',
            })
        });
        bootstrap.Modal.getInstance(document.getElementById('addToWatchlistModal'))?.hide();
        alert('Added to watchlist!');
    } catch (e) {
        console.error('Add to watchlist failed:', e);
    }
}

async function setAlert() {
    try {
        await fetch('/api/alerts', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                address: WALLET_ADDRESS,
                chain: WALLET_CHAIN,
                alert_type: document.getElementById('alert-type')?.value || 'any_move',
                min_usd_value: parseFloat(document.getElementById('alert-min-usd')?.value || 0),
                notify_telegram: document.getElementById('alert-telegram')?.checked || false,
                telegram_chat_id: document.getElementById('alert-telegram-chat')?.value || '',
            })
        });
        bootstrap.Modal.getInstance(document.getElementById('setAlertModal'))?.hide();
        alert('Alert set!');
    } catch (e) {
        console.error('Set alert failed:', e);
    }
}

function copyAddress() {
    navigator.clipboard.writeText(WALLET_ADDRESS).then(() => {
        const btn = document.querySelector('[onclick="copyAddress()"]');
        if (btn) {
            btn.innerHTML = '<i class="fas fa-check"></i>';
            setTimeout(() => btn.innerHTML = '<i class="fas fa-copy"></i>', 1500);
        }
    });
}

// --- Init ---
document.addEventListener('DOMContentLoaded', () => {
    initSocket();
    if (isTrackerPage()) {
        initTrackerPage();
    } else {
        initProfilePage();
    }
});
