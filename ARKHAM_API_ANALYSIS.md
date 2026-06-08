# Arkham Intel API — Analysis & Integration Notes

> Source: [Arkham API docs](https://arkm.com/api/docs) · machine-readable index [llms.txt](https://arkm.com/llms.txt) · OpenAPI [openapi.json](https://arkm.com/openapi.json)
> API version v1.1.0 · Analysis date 2026-06-08

This documents what the Arkham API offers and how it maps onto our whale-page
and Polymarket goals. It is the reference for the planned Arkham-backed
Polymarket sync and the slug-verification cleanup.

---

## 1. Client config: two corrections vs. the docs

- **Base URL.** Docs use `https://api.arkm.com`. Our client
  (`arkham_backfill/arkham_client.py`) hardcodes the legacy
  `https://api.arkhamintelligence.com`. The old domain still resolves (the cron
  works), but the current key targets the `api.arkm.com` surface — switch to
  avoid a future cutover break.
- **Auth header — already correct.** Use `API-Key: <key>`, **not**
  `Authorization`. Wrong header → `401 "error validating API key"`. Our client
  already sends `headers={"API-Key": ...}`.

## 2. The 400 errors (now fixed) — confirmed behavior

- `GET /transfers` documents only `200 / 400 / 500` — **there is no 404**. An
  invalid `base` entity slug returns **400**, exactly what we saw for
  `blackrock-bitcoin-etf`, `axelar`, `optimism-foundation`.
- **Failed requests (4xx/5xx) are not charged.** Those bad slugs were pure log
  noise, never burning credits. Treating 4xx as a non-fatal skip
  (`arkham_backfill/backfill.py`) is the correct handling.

## 3. Billing model: two separate meters (we guard the wrong one)

- **Label lookups** — `X-Intel-Datapoints-Usage / -Limit / -Remaining` headers.
  Counts *unique labeled addresses* per billing period (Trial 10k · Individual
  1M · Org 10M/seat). Repeated lookups of the same address count once.
- **Credit allowance** — monthly credits consumed per endpoint (see §5). This is
  what `/transfers` actually drains.

Our `_check_credits()` floor-guards `x-intel-datapoints-remaining` (the label
meter — the abundant one) and is blind to per-row credit spend. Action items:
- Add `GET /subscription/intel-usage` (per-period total, per-seat limit,
  per-chain breakdown) for true budget visibility.
- Remember `/transfers` cost = `limit × 2 credits`. Current cron
  (`--transfer-limit 40` × 15 entities) ≈ **1,200 credits/run**.

## 4. Rate limits

| Class | Limit | Endpoints |
| --- | --- | --- |
| Standard | 20 req/s | most endpoints (incl. all `/polymarket/*`) |
| Heavy | 1 req/s | `/transfers` |

- Our 1 req/s limiter for `/transfers` is correct.
- On `429`, honor the `Retry-After` response header (our backoff is
  exponential-only — minor improvement to read the header).
- 429 also fires when the **label limit** is hit:
  `"Intelligence label lookup limit reached for this billing period."`

## 5. Native Polymarket suite — replaces our public-API scraping

We currently scrape Polymarket's public Gamma/Data APIs in
`scripts/sync_polymarket.py`, which return **pseudonyms** (→ "Whales = —" /
"No holdings found"). Arkham exposes the same data **entity-resolved with real
names/labels**.

| Current manual step | Arkham native endpoint | Credits |
| --- | --- | --- |
| `get_markets_via_events` / top markets | `GET /polymarket/top-events` (period `1d/1w/1m/all`) | 10/call |
| `get_market_holders` per market | `GET /polymarket/top-holders/{conditionId}` (limit ≤200, filter Yes/No outcome) | 3/call |
| whale leaderboard aggregation | `GET /polymarket/leaderboard` (PnL-ranked, can pin an address's rank) | 10/call |
| *(missing today)* live whale-flow tape | `GET /polymarket/activity` (`minUsd`, `actions=buy/sell`, `minPrice`, time window, sort by `usd`) | 5/call |
| "aggregated top bids" | `GET /polymarket/order-book/{conditionId}` | 2/call |
| whale drawer PnL/positions (`enrich_whales`) | `GET /polymarket/positions/{addr}` + `/polymarket/wallet/{addr}/summary/{pnl,portfolio,stats,biggest-win}` | 1–3/call |

Heart of the "terminal" feel:
- **`/polymarket/activity`** = scrolling live tape of whale buys/sells, filterable
  to `minUsd ≥ $X`. We don't produce this at all today.
- **`/polymarket/leaderboard`** = real PnL-ranked whale board, replacing the
  home-grown `total_amount` sum.

Decisive advantage: Arkham resolves traders to **named entities**, so the
empty-name problem on the frontend largely disappears.

Full Polymarket endpoint list: `top-events`, `top-events/{eventId}/breakdown`,
`events`, `events/{eventId}`, `event-positions/{conditionId}`, `top-holders/{conditionId}`,
`leaderboard`, `activity`, `order-book/{conditionId}`, `positions/{addr}`,
`prices`, `stats`, `pnl/chart`, and `wallet/{addr}/summary/{balance,biggest-win,pnl,portfolio,stats}`,
`wallet/{addr}/prediction-history`.

## 6. Slug verification — fix the 400s at the source

We hand-guess entity slugs (cause of the 400s). Validate before seeding:
- `GET /intelligence/entity/{entity}` — 1 credit, errors on invalid slug.
- `GET /intelligence/search` — 30 credits, resolves free-text name → slug.

`entities.py` already references a `verify_slugs` module that was never built.
Implement it against `/intelligence/entity/{slug}` so the seed list self-prunes
instead of throwing 400s every run.

## 7. Other endpoints for the whale / entity pages

- `GET /balances/entity/{entity}` (5cr) — live treasury balances → real
  "holdings" on entity pages.
- `GET /portfolio/entity/{entity}` + `/portfolio/timeSeries/...` — portfolio
  history charts.
- `GET /counterparties/entity/{entity}` (50cr — pricey) — top counterparties for
  an Arkham-style flow graph.
- WebSocket `/ws/transfers` — live whale-transaction stream (replaces polling)
  for the realtime monitor.
- `GET /intelligence/entity/{entity}/summary`, `/history/entity/{entity}`,
  `/flow/entity/{entity}`, `/volume/entity/{entity}` — entity stat blocks.

## 8. `/transfers` filter cheatsheet (what we use for mining)

- `base` — entity slug or address; supports negation `!wintermute` and arrays.
- `from` / `to` — addresses, entities, `type:cex`, or `deposit:binance`.
- `flow` — `in` / `out` / `self` / `all`.
- `chains` — comma list (`ethereum,bsc,polygon`).
- `usdGte` / `usdLte`, `valueGte` / `valueLte` — value filters.
- `timeLast` (e.g. `24h`) **or** `timeGte`/`timeLte` — not both (else 400).
- `sortKey` (`time`/`value`/`usd`), `sortDir`, `limit` (≤1000), `offset`.

---

## Recommended next steps (priority order)

1. **Build an Arkham-backed Polymarket sync** (`ArkhamPolymarketClient`) reusing
   the rate-limit/credit plumbing, feeding the same `polymarket_*` tables; keep
   the public client as fallback. Gets named whales, PnL leaderboard, and a live
   activity tape. Est. cost ≈ `10 + (markets × 3) + 10` credits/run.
2. **Flip base URL** to `api.arkm.com` and add `/subscription/intel-usage`
   budget logging.
3. **Build `verify_slugs`** self-pruning step against `/intelligence/entity/{slug}`.
