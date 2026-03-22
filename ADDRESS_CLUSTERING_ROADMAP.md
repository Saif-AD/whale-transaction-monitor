# Address Clustering Roadmap

## Problem

Bitcoin SELL detection is structurally limited because exchanges generate unique per-user deposit addresses that aren't in any public address list. Our static list (~150 known exchange cold/hot wallets) catches withdrawals (BUYs) reliably since exchanges use a handful of known hot wallets for millions of withdrawals, but deposits go to unlabeled, ephemeral addresses.

Professional monitors (Whale Alert, Arkham Intelligence, Chainalysis) solve this with address clustering — algorithmically linking unknown addresses to known exchange entities.

---

## Phase 1: Passive Sweep Learning (Quick Win)

**Goal:** Automatically tag exchange deposit addresses by watching consolidation/sweep transactions.

**How it works:**
- When we process a Bitcoin block and see a transaction where the OUTPUT goes to a known exchange cold/hot wallet (already in `BTC_EXCHANGE_ADDRESSES`), tag ALL input addresses as belonging to that exchange.
- Store learned addresses in Supabase `addresses` table with `source: "sweep_clustering"` and `detection_method: "phase1_passive_sweep"`.
- On startup, load learned addresses into an in-memory set alongside the static `BTC_EXCHANGE_ADDRESSES`.
- Future transactions TO these learned addresses are now classified as SELLs.

**Implementation steps:**
1. Add a `_learn_exchange_addresses(tx_hash, enriched_vin, to_addr, exchange_label)` function in `chains/bitcoin_alchemy.py`.
2. Call it whenever a qualifying output goes to a known exchange address AND the classification is TRANSFER/CONSOLIDATION (indicating an internal sweep).
3. Write learned addresses to Supabase with metadata (exchange name, discovery tx, timestamp, confidence).
4. On monitor startup, query Supabase for all `source: "sweep_clustering"` addresses and merge into the runtime lookup set.
5. Add a TTL or confidence decay — addresses not seen in 90 days get demoted.

**Expected yield:** Thousands of new exchange deposit addresses within days of running. Each exchange consolidation tx typically has 10-100+ inputs, all of which become newly labeled.

**Risk:** None. Additive only — doesn't modify existing classification logic, just expands the address set.

---

## Phase 2: BigQuery Historical Backfill

**Goal:** Use BigQuery public Bitcoin datasets to retroactively trace all transactions into known exchange wallets and label the senders.

**How it works:**
- Query `bigquery-public-data.crypto_bitcoin.transactions` for all transactions where ANY output goes to a known exchange address.
- Extract all input addresses from those transactions and label them as exchange-associated.
- Process months/years of history in batch, building a large initial seed database.

**Implementation steps:**
1. Write a `backfill_exchange_addresses.py` script that:
   - Loads all known exchange addresses from `BTC_EXCHANGE_ADDRESSES`.
   - Queries BigQuery for transactions with outputs to those addresses (batched by date range).
   - Extracts input addresses and upserts to Supabase.
2. Run as a one-time batch job (or periodic weekly refresh).
3. Add deduplication — don't re-process transactions already covered.

**Expected yield:** Tens of thousands to hundreds of thousands of labeled addresses from historical data. This is the single biggest boost to SELL detection accuracy.

**Cost:** BigQuery public datasets are free for the first 1 TB/month of queries. Bitcoin transaction table is ~500 GB, so queries need to be date-partitioned.

---

## Phase 3: Common Input Ownership Heuristic (CIOH)

**Goal:** If two addresses appear as inputs in the same transaction, they are controlled by the same entity. Use this to expand clusters transitively.

**How it works:**
- When processing a Bitcoin transaction, if ANY input address is a known exchange address, tag ALL other input addresses as belonging to the same exchange.
- This works because Bitcoin transactions require private keys for all inputs — if Binance signs one input, they control all inputs in that tx.

**Implementation steps:**
1. In `_process_block`, after resolving input addresses, check if any input is a known exchange address.
2. If yes, tag all OTHER input addresses as the same exchange entity.
3. Store with `detection_method: "phase3_cioh"` and slightly lower confidence (0.7 vs 0.9 for sweep-based).
4. Implement transitive closure — periodically re-process to expand clusters (address A linked to B, B linked to C → A linked to C).

**Expected yield:** Multiplicative expansion of clusters. Each new address discovered can seed further discoveries.

**Risk:** Low but non-zero. CoinJoin and collaborative transactions can create false links. Mitigate by:
- Excluding transactions with 3+ outputs of equal value (CoinJoin signature).
- Requiring at least 2 independent linking transactions before confirming.
- Assigning lower confidence to CIOH-derived labels.

---

## Phase 4: Active Enrichment

**Goal:** On-demand address investigation using external APIs to expand clusters for high-value unknown addresses.

**How it works:**
- When we see a large transaction (>$500K) involving an unknown address, actively query its transaction history.
- Use mempool.space or Blockstream APIs to fetch recent transactions for that address.
- Check if any of those transactions connect to known exchange addresses.

**Implementation steps:**
1. Add an `enrich_address(address)` function that queries mempool.space `/api/address/{addr}/txs`.
2. For each historical transaction, check if counterparties are known exchange addresses.
3. If a connection is found, label the address and store in Supabase.
4. Rate-limit to avoid API abuse (max 10 enrichment lookups per block).
5. Cache results — don't re-enrich addresses already in the database.

**Expected yield:** High-value, targeted expansions. Won't produce volume like Phase 2, but catches the most important addresses (large whale wallets).

**Cost:** mempool.space API is free but rate-limited. Budget ~5-10 calls per block cycle.

---

## Data Schema (Supabase `addresses` table)

Learned addresses should include:
- `address` — the Bitcoin address
- `blockchain` — "bitcoin"
- `label` — e.g., "Binance deposit (learned)"
- `address_type` — "exchange"
- `entity_name` — e.g., "binance", "coinbase"
- `confidence` — 0.5-0.95 depending on detection method
- `source` — "sweep_clustering" | "bigquery_backfill" | "cioh_clustering" | "active_enrichment"
- `detection_method` — "phase1_passive_sweep" | "phase2_bigquery" | "phase3_cioh" | "phase4_active"
- `discovery_tx` — the transaction hash that revealed this address
- `discovered_at` — timestamp
- `last_seen` — last time this address appeared in a monitored transaction

---

## Priority Order

1. **Phase 1** — Implement first. Zero risk, immediate passive learning, biggest ROI per effort.
2. **Phase 2** — Run after Phase 1 is live. One-time batch job seeds the database with historical data.
3. **Phase 3** — Layer on top of Phase 1+2 for multiplicative growth.
4. **Phase 4** — Final polish for high-value edge cases.

## Success Metrics

- **Pre-clustering:** ~0 BTC SELLs detected per hour (only direct-to-cold-wallet deposits)
- **Post Phase 1 (1 week):** ~5-15 BTC SELLs per hour (from learned deposit addresses)
- **Post Phase 2:** ~20-50 BTC SELLs per hour (from historical backfill)
- **Post Phase 3+4:** Approaching parity with BUY detection rates
