"""Curated entity seed list for Arkham transfer mining.

Each entry defines:
  slug     — Arkham entity slug (used in /transfers?base={slug})
  type     — entity category (cex, fund, custodian, bridge, stablecoin_issuer, etc.)
  min_usd  — minimum USD filter for transfer mining.
             High-volume CEXes use $1M to avoid noise; smaller entities use $0.
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

ENTITY_SEED_LIST: list[Entity] = _CEXES + _FUNDS + _INFRA
