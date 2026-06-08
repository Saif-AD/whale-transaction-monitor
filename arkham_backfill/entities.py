"""Curated entity seed list for Arkham transfer mining.

Each entry defines:
  slug     — Arkham entity slug (used in /transfers?base={slug})
  type     — entity category (cex, fund, custodian, bridge, stablecoin_issuer, etc.)
  min_usd  — minimum USD filter for transfer mining.
             High-volume CEXes use $1M to avoid noise; smaller entities use $0.

Slug verification: not all slugs in this file are guaranteed to resolve on
Arkham. The backfill loop already tolerates per-entity failures (see
arkham_backfill.backfill._process_entity) and logs a WARNING for each
404. If you want to validate the full seed list before a live run, use:

    python -m arkham_backfill.verify_slugs --dry-run

(see deliverable 1 of the Solana expansion handoff). The CREDIT_FLOOR
guard remains the safety net.
"""

from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class Entity:
    slug: str
    type: str
    min_usd: int = 0


# --- CEXes (high volume → $1M filter) ---
_CEXES = [
    Entity("binance", "cex", min_usd=1_000_000),
    Entity("coinbase", "cex", min_usd=1_000_000),
    Entity("kraken", "cex", min_usd=1_000_000),
    Entity("okx", "cex", min_usd=1_000_000),
    Entity("bybit", "cex", min_usd=1_000_000),
    Entity("bitfinex", "cex", min_usd=1_000_000),
    Entity("kucoin", "cex", min_usd=1_000_000),
    Entity("gate-io", "cex", min_usd=1_000_000),
    Entity("htx", "cex", min_usd=1_000_000),
    Entity("crypto-com", "cex", min_usd=1_000_000),
    Entity("gemini", "cex", min_usd=1_000_000),
    Entity("bitstamp", "cex", min_usd=1_000_000),
]

# --- Funds / market makers ---
_FUNDS = [
    Entity("jump-trading", "fund"),
    Entity("wintermute", "fund"),
    Entity("alameda-research", "fund"),
    Entity("three-arrows-capital", "fund"),
    Entity("paradigm-capital", "fund"),
    Entity("a16z", "fund"),
    Entity("blackrock", "fund"),
    Entity("galaxy-digital", "fund"),
    Entity("dragonfly-capital", "fund"),
    Entity("polychain-capital", "fund"),
]

# --- Custodians, bridges, stablecoin issuers ---
_INFRA = [
    Entity("fireblocks", "custodian"),
    Entity("circle", "stablecoin_issuer"),
    Entity("tether", "stablecoin_issuer"),
]


# ---------------------------------------------------------------------------
# Solana-native expansion (Colosseum demo)
# ---------------------------------------------------------------------------
# Slugs sourced from the Arkham public entity directory. Any slug that 404s
# is skipped silently with a WARNING by the backfill loop. If > 30% of new
# slugs 404 in a single run, the operator should investigate before a full
# live backfill — see ACCEPTANCE criteria in handoff doc.
# Pruned 2026-06-08 via `python -m arkham_backfill.verify_slugs`: removed
# jito-foundation, solana-foundation, jupiter-aggregator, helium-foundation,
# magic-eden (HTTP 404 — not queryable entities).
_SOLANA_NATIVE = [
    Entity("marinade-finance", "fund"),
    Entity("drift-protocol", "fund"),
    Entity("kamino-finance", "fund"),
    Entity("marginfi", "fund"),
    Entity("phoenix", "fund"),
    Entity("raydium", "fund"),
    Entity("orca", "fund"),
    Entity("meteora", "fund"),
    Entity("pump-fun", "fund"),
    Entity("tensor", "fund"),
]

# ---------------------------------------------------------------------------
# Additional funds / VCs / institutions we missed in the original seed list
# ---------------------------------------------------------------------------
_MORE_FUNDS = [
    Entity("multicoin-capital", "fund"),
    # "paradigm" pruned (HTTP 404); "paradigm-capital" above is the valid slug.
    Entity("electric-capital", "fund"),
    Entity("pantera-capital", "fund"),
    Entity("delphi-digital", "fund"),
    Entity("ark-invest", "fund"),
    Entity("franklin-templeton", "fund"),
    Entity("securitize", "fund"),
    Entity("ondo-finance", "fund"),
    # NOTE: "blackrock-bitcoin-etf" removed — not queryable on the transfers
    # endpoint (HTTP 400) and redundant with the "blackrock" entity above,
    # which already covers IBIT addresses.
    Entity("fidelity", "fund"),
    Entity("vaneck", "fund"),
]

# ---------------------------------------------------------------------------
# Bridges / interop infra
# ---------------------------------------------------------------------------
_BRIDGES = [
    Entity("wormhole", "bridge"),
    Entity("layerzero", "bridge"),
    Entity("stargate", "bridge"),
    Entity("synapse", "bridge"),
    Entity("hop-protocol", "bridge"),
    Entity("across-protocol", "bridge"),
    # NOTE: "axelar" removed — not queryable on the transfers endpoint (HTTP 400).
]


# ---------------------------------------------------------------------------
# Curated-page backfill: Arkham slugs for the curated_entities rows that are
# still missing addresses. Mining these into the `addresses` table lets
# scripts/enrich_curated_entities_from_labels.py auto-fill the figure/entity
# pages (match is by entity name, not slug). Slugs are best-effort guesses
# at Arkham's directory; any 404 is skipped with a WARNING, and
# `python -m arkham_backfill.verify_slugs --dry-run` prunes them before a
# live run once ARKHAM_API_KEY is set.
# Pruned 2026-06-08 via verify_slugs (HTTP 404): world-liberty-financial,
# union-square-ventures, lightspeed-faction, government-of-el-salvador, us-doj,
# us-government, german-government, wormhole-exploiter, changpeng-zhao,
# charles-hoskinson, anthony-pompliano, sergey-nazarov, rune-christensen,
# lyn-alden. (optimism-foundation/axelar/blackrock-bitcoin-etf removed earlier.)
_CURATED_PAGE_TARGETS = [
    # Protocol treasuries / DAOs
    Entity("balancer", "protocol"),
    Entity("pancakeswap", "protocol"),
    Entity("sushiswap", "protocol"),
    Entity("chainlink", "protocol"),
    # ETF issuers / public-company treasuries
    Entity("21shares", "fund"),
    Entity("bitwise", "fund"),
    Entity("valkyrie", "fund"),
    Entity("tesla", "company"),
    # Historical / illicit (high-signal, Arkham-tracked)
    Entity("lazarus-group", "historical"),
    # Individuals (MEDIUM signal via ENTITY_TYPE_OVERRIDES)
    Entity("gavin-wood", "individual"),
    Entity("raoul-pal", "individual"),
    Entity("logan-paul", "individual"),
]


ENTITY_SEED_LIST: list[Entity] = (
    _CEXES + _FUNDS + _INFRA + _SOLANA_NATIVE + _MORE_FUNDS + _BRIDGES
    + _CURATED_PAGE_TARGETS
)
