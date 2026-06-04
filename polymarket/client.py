"""Polymarket public-API client — markets, whales (holders), positions.

Powers the Sonar "Polymarket whale radar": aggregate top markets, see the
biggest holders (whales) per market, and drill into a whale's open
positions + portfolio value.

All endpoints are PUBLIC and keyless:

  Gamma API      https://gamma-api.polymarket.com
    GET /markets   — markets w/ odds, 24h volume, liquidity, clobTokenIds
    GET /events    — grouped markets (multi-outcome)

  Data API       https://data-api.polymarket.com
    GET /holders?market={conditionId}  — top holders per outcome token
    GET /positions?user={proxyWallet}  — a wallet's open positions
    GET /value?user={proxyWallet}      — a wallet's portfolio value

NOTE ON LOCAL TLS: Polymarket sits behind Cloudflare which resets very old
LibreSSL clients (e.g. macOS system curl). Python httpx with modern certs,
Railway, and Vercel all reach it fine. If a local run fails with an SSL
reset, that is the environment, not the API.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

GAMMA_BASE = "https://gamma-api.polymarket.com"
DATA_BASE = "https://data-api.polymarket.com"

# Browser-ish UA helps avoid Cloudflare bot resets.
_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; SonarTracker/1.0; +https://sonartracker.io)",
}


@dataclass
class Market:
    condition_id: str
    question: str
    slug: str
    outcomes: List[str]
    outcome_prices: List[float]
    clob_token_ids: List[str]
    volume_24h: float
    liquidity: float
    end_date: Optional[str]
    image: Optional[str]
    one_day_price_change: Optional[float]
    category: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    competitive: Optional[float] = None

    @property
    def yes_price(self) -> Optional[float]:
        return self.outcome_prices[0] if self.outcome_prices else None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "condition_id": self.condition_id,
            "question": self.question,
            "slug": self.slug,
            "outcomes": self.outcomes,
            "outcome_prices": self.outcome_prices,
            "clob_token_ids": self.clob_token_ids,
            "volume_24h": self.volume_24h,
            "liquidity": self.liquidity,
            "end_date": self.end_date,
            "image": self.image,
            "one_day_price_change": self.one_day_price_change,
            "category": self.category,
            "tags": self.tags,
            "competitive": self.competitive,
        }


# Polymarket top-level category tags, in display priority. The first match
# wins as a market's `category`; everything else is kept in `tags`.
_CATEGORY_PRIORITY = [
    "Politics", "Sports", "Crypto", "Geopolitics", "Economy", "Business",
    "Tech", "Science", "Culture", "Pop Culture", "World", "Elections",
    "Weather", "Mentions",
]
# Tags that are housekeeping, not real categories.
_GENERIC_TAGS = {
    "hide from new", "all", "recurring", "daily", "weekly", "monthly",
    "hourly", "trending", "new",
}


def _derive_category(tag_labels: List[str]) -> Tuple[Optional[str], List[str]]:
    """Return (primary_category, clean_tags) from a list of tag labels."""
    clean = [t for t in tag_labels if t and t.strip().lower() not in _GENERIC_TAGS]
    for pref in _CATEGORY_PRIORITY:
        for t in clean:
            if t.strip().lower() == pref.lower():
                return pref, clean
    return (clean[0] if clean else None), clean


@dataclass
class Holder:
    proxy_wallet: str
    name: str
    pseudonym: str
    amount: float
    outcome_index: int
    profile_image: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "proxy_wallet": self.proxy_wallet,
            "name": self.name or self.pseudonym,
            "amount": self.amount,
            "outcome_index": self.outcome_index,
            "profile_image": self.profile_image,
        }


@dataclass
class Position:
    market_question: str
    outcome: str
    size: float
    avg_price: float
    cur_price: float
    value_usd: float
    pnl_usd: float = field(default=0.0)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "market_question": self.market_question,
            "outcome": self.outcome,
            "size": self.size,
            "avg_price": self.avg_price,
            "cur_price": self.cur_price,
            "value_usd": self.value_usd,
            "pnl_usd": self.pnl_usd,
        }


def _as_list(maybe_json: Any) -> List[Any]:
    """Gamma returns some array fields as JSON-encoded strings."""
    if isinstance(maybe_json, list):
        return maybe_json
    if isinstance(maybe_json, str) and maybe_json.strip():
        try:
            v = json.loads(maybe_json)
            return v if isinstance(v, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


class PolymarketClient:
    def __init__(self, timeout: float = 20.0):
        self._http = httpx.Client(headers=_HEADERS, timeout=timeout, follow_redirects=True)

    def close(self) -> None:
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------
    # Markets
    # ------------------------------------------------------------------
    def get_top_markets(
        self, *, limit: int = 20, order: str = "volume24hr", active_only: bool = True
    ) -> List[Market]:
        params = {
            "limit": limit,
            "order": order,
            "ascending": "false",
            "closed": "false",
        }
        if active_only:
            params["active"] = "true"
        resp = self._http.get(f"{GAMMA_BASE}/markets", params=params)
        resp.raise_for_status()
        out: List[Market] = []
        for m in resp.json() or []:
            prices = [_to_float(p) for p in _as_list(m.get("outcomePrices"))]
            out.append(Market(
                condition_id=m.get("conditionId", ""),
                question=m.get("question", ""),
                slug=m.get("slug", ""),
                outcomes=[str(o) for o in _as_list(m.get("outcomes"))],
                outcome_prices=prices,
                clob_token_ids=[str(t) for t in _as_list(m.get("clobTokenIds"))],
                volume_24h=_to_float(m.get("volume24hr")),
                liquidity=_to_float(m.get("liquidityNum") or m.get("liquidity")),
                end_date=m.get("endDate"),
                image=m.get("image"),
                one_day_price_change=m.get("oneDayPriceChange"),
            ))
        return out

    # ------------------------------------------------------------------
    # Events-based pull (carries real category tags + paginates)
    # ------------------------------------------------------------------
    def get_events(
        self, *, limit: int = 40, offset: int = 0, order: str = "volume24hr", active_only: bool = True
    ) -> List[Dict[str, Any]]:
        params = {
            "limit": limit,
            "offset": offset,
            "order": order,
            "ascending": "false",
            "closed": "false",
        }
        if active_only:
            params["active"] = "true"
        resp = self._http.get(f"{GAMMA_BASE}/events", params=params)
        resp.raise_for_status()
        return resp.json() or []

    def _market_from_event_row(self, m: Dict[str, Any], category: Optional[str], tags: List[str]) -> Optional[Market]:
        cid = m.get("conditionId", "")
        if not cid or m.get("closed") or m.get("active") is False:
            return None
        prices = [_to_float(p) for p in _as_list(m.get("outcomePrices"))]
        return Market(
            condition_id=cid,
            question=m.get("question", ""),
            slug=m.get("slug", ""),
            outcomes=[str(o) for o in _as_list(m.get("outcomes"))],
            outcome_prices=prices,
            clob_token_ids=[str(t) for t in _as_list(m.get("clobTokenIds"))],
            volume_24h=_to_float(m.get("volume24hr")),
            liquidity=_to_float(m.get("liquidityNum") or m.get("liquidity")),
            end_date=m.get("endDate"),
            image=m.get("image"),
            one_day_price_change=m.get("oneDayPriceChange"),
            category=category,
            tags=tags,
            competitive=_to_float(m.get("competitive")) if m.get("competitive") is not None else None,
        )

    def get_markets_via_events(
        self, *, pages: int = 3, per_page: int = 40, order: str = "volume24hr"
    ) -> List[Market]:
        """Paginate /events and flatten into Market objects enriched with the
        event's category + tags. Dedupes by condition_id across pages."""
        out: List[Market] = []
        seen = set()
        for page in range(pages):
            try:
                events = self.get_events(limit=per_page, offset=page * per_page, order=order)
            except httpx.HTTPError as e:
                logger.warning("events page %d fetch failed: %s", page, e)
                break
            if not events:
                break
            for ev in events:
                labels = [t.get("label", "") for t in (ev.get("tags") or [])]
                category, tags = _derive_category(labels)
                for m in ev.get("markets") or []:
                    mk = self._market_from_event_row(m, category, tags)
                    if mk and mk.condition_id not in seen:
                        seen.add(mk.condition_id)
                        out.append(mk)
        return out

    # ------------------------------------------------------------------
    # Whales / holders
    # ------------------------------------------------------------------
    def get_market_holders(
        self, condition_id: str, *, limit: int = 10
    ) -> List[Holder]:
        """Top holders ("whales") across a market's outcome tokens."""
        resp = self._http.get(
            f"{DATA_BASE}/holders", params={"market": condition_id, "limit": limit}
        )
        resp.raise_for_status()
        holders: List[Holder] = []
        for token_group in resp.json() or []:
            for h in token_group.get("holders", []) or []:
                holders.append(Holder(
                    proxy_wallet=h.get("proxyWallet", ""),
                    name=h.get("name", ""),
                    pseudonym=h.get("pseudonym", ""),
                    amount=_to_float(h.get("amount")),
                    outcome_index=int(h.get("outcomeIndex", 0) or 0),
                    profile_image=h.get("profileImage", "") or "",
                ))
        holders.sort(key=lambda x: x.amount, reverse=True)
        return holders

    def get_top_whales(
        self, *, market_limit: int = 15, holders_per_market: int = 10
    ) -> List[Dict[str, Any]]:
        """Aggregate the biggest holders across the top markets into a
        whale leaderboard keyed by wallet (sum of position sizes seen)."""
        agg: Dict[str, Dict[str, Any]] = {}
        for m in self.get_top_markets(limit=market_limit):
            if not m.condition_id:
                continue
            try:
                holders = self.get_market_holders(m.condition_id, limit=holders_per_market)
            except httpx.HTTPError as e:
                logger.warning("holders fetch failed for %s: %s", m.slug, e)
                continue
            for h in holders:
                rec = agg.setdefault(h.proxy_wallet, {
                    "proxy_wallet": h.proxy_wallet,
                    "name": h.name or h.pseudonym,
                    "profile_image": h.profile_image,
                    "total_amount": 0.0,
                    "markets": 0,
                })
                rec["total_amount"] += h.amount
                rec["markets"] += 1
        whales = sorted(agg.values(), key=lambda r: r["total_amount"], reverse=True)
        return whales

    # ------------------------------------------------------------------
    # Drill into a whale
    # ------------------------------------------------------------------
    def get_user_positions(self, proxy_wallet: str, *, limit: int = 50) -> List[Position]:
        resp = self._http.get(
            f"{DATA_BASE}/positions",
            params={"user": proxy_wallet, "limit": limit, "sortBy": "CURRENT", "sortDirection": "DESC"},
        )
        resp.raise_for_status()
        out: List[Position] = []
        for p in resp.json() or []:
            size = _to_float(p.get("size"))
            avg = _to_float(p.get("avgPrice"))
            cur = _to_float(p.get("curPrice"))
            out.append(Position(
                market_question=p.get("title") or p.get("market") or "",
                outcome=p.get("outcome", ""),
                size=size,
                avg_price=avg,
                cur_price=cur,
                value_usd=_to_float(p.get("currentValue")) or size * cur,
                pnl_usd=_to_float(p.get("cashPnl")),
            ))
        return out

    def get_user_value(self, proxy_wallet: str) -> float:
        resp = self._http.get(f"{DATA_BASE}/value", params={"user": proxy_wallet})
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and data:
            return _to_float(data[0].get("value"))
        if isinstance(data, dict):
            return _to_float(data.get("value"))
        return 0.0


# ---------------------------------------------------------------------------
# CLI smoke test:  python -m polymarket.client
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    with PolymarketClient() as pm:
        print("=== TOP MARKETS (by 24h volume) ===")
        markets = pm.get_top_markets(limit=5)
        for m in markets:
            odds = ", ".join(
                f"{o} {p:.0%}" for o, p in zip(m.outcomes, m.outcome_prices)
            )
            print(f"  ${m.volume_24h:,.0f}  {m.question[:60]:60}  [{odds}]")

        if markets:
            print(f"\n=== TOP WHALES in: {markets[0].question[:50]} ===")
            for h in pm.get_market_holders(markets[0].condition_id, limit=5):
                print(f"  ${h.amount:,.0f}  {h.name or h.pseudonym}  ({h.proxy_wallet[:10]}…)")

        print("\n=== AGGREGATED WHALE LEADERBOARD (across top markets) ===")
        for w in pm.get_top_whales(market_limit=8, holders_per_market=8)[:8]:
            print(f"  ${w['total_amount']:,.0f}  {w['name']}  in {w['markets']} markets  ({w['proxy_wallet'][:10]}…)")
