#!/usr/bin/env python3
"""Bridge the rich `addresses` label store into `curated_entities`.

WHY
---
The backend `addresses` table holds ~166k labeled addresses across
thousands of entities (Arkham backfill + public-label imports). The public
figure/entity pages, however, read `curated_entities.addresses` — a
hand-curated store with only 1-2 addresses per entity. This script makes
the existing curated entities "Arkham-deep" by pulling their top addresses
straight from the label store, with quality + provenance discipline.

It does NOT invent data: every address comes from an existing labeled row
in `addresses`, and each gets an explorer URL as its source citation
(consistent with seeds_figures.py curation rules).

WHAT IT DOES
------------
For each curated_entities row:
  1. derive search terms from display_name (full name + parenthetical
     alias, e.g. "Andreessen Horowitz (a16z)" -> ["Andreessen Horowitz",
     "a16z", "Andreessen Horowitz (a16z)"]),
  2. query `addresses` for rows whose entity_name matches, on
     frontend-supported chains only,
  3. rank by (verified-ish confidence desc, balance_usd desc),
  4. APPEND up to --max-per-entity new addresses (never clobber; dedupe by
     (chain, normalized address)),
  5. write the merged array back.

Provenance: confidence >= --verify-threshold (default 0.8) => verified=True
with an explorer source URL; below that => verified=False (still shown, not
featured).

Default is DRY-RUN. Use --live to write.

Usage:
    python scripts/enrich_curated_entities_from_labels.py
    python scripts/enrich_curated_entities_from_labels.py --only aave,binance
    python scripts/enrich_curated_entities_from_labels.py --max-per-entity 25 --live
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Reuse the validated merge/normalize machinery from the seed importer.
from scripts.import_curated_entities import (  # noqa: E402
    EVM_CHAINS,
    VALID_CHAINS,
    merge_for_entity,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Junk entity_name patterns produced by transfer-mining heuristics — never
# surface these as figure addresses.
_JUNK_PATTERNS = [
    re.compile(r"interactions\)", re.I),
    re.compile(r"outputs\)", re.I),
    re.compile(r"^active (account|user)", re.I),
    re.compile(r"^\d", re.I),                # starts with a digit
    re.compile(r"etherscan label", re.I),
    re.compile(r"whale (sender|receiver)", re.I),
    re.compile(r"^(drop|art|pet|cat in a box)$", re.I),
]

_EXPLORERS = {
    "ethereum": "https://etherscan.io/address/{a}",
    "polygon": "https://polygonscan.com/address/{a}",
    "arbitrum": "https://arbiscan.io/address/{a}",
    "optimism": "https://optimistic.etherscan.io/address/{a}",
    "base": "https://basescan.org/address/{a}",
    "bsc": "https://bscscan.com/address/{a}",
    "avalanche": "https://snowtrace.io/address/{a}",
    "solana": "https://solscan.io/account/{a}",
    "bitcoin": "https://www.blockchain.com/explorer/addresses/btc/{a}",
}


def explorer_url(address: str, chain: str) -> str:
    tmpl = _EXPLORERS.get(chain)
    return tmpl.format(a=address) if tmpl else ""


def is_junk(entity_name: str) -> bool:
    n = (entity_name or "").strip()
    if len(n) < 2:
        return True
    return any(p.search(n) for p in _JUNK_PATTERNS)


def search_terms(display_name: str) -> List[str]:
    """Derive candidate entity_name search terms from a display name."""
    terms = {display_name.strip()}
    # Strip a trailing parenthetical and also keep its contents as an alias.
    m = re.match(r"^(.*?)\s*\(([^)]+)\)\s*$", display_name.strip())
    if m:
        terms.add(m.group(1).strip())
        terms.add(m.group(2).strip())
    # Drop very short / generic single tokens that would over-match.
    return [t for t in terms if len(t) >= 3]


def fetch_label_addresses(
    client, terms: List[str], *, limit: int, include_prefix: bool = False
) -> List[Dict[str, Any]]:
    """Fetch candidate labeled addresses for the given entity search terms."""
    rows: List[Dict[str, Any]] = []
    seen_ids = set()
    # Exact match is index-backed and fast. Prefix ILIKE on a 400k-row
    # table can exceed the statement timeout, so it is opt-in via
    # --include-prefix. Per-query failures are tolerated, never fatal.
    filters = ("eq", "prefix") if include_prefix else ("eq",)
    for term in terms:
        safe = term.replace(",", " ").strip()
        if not safe:
            continue
        for filt in filters:
            try:
                q = client.table("addresses").select(
                    "id,address,blockchain,label,entity_name,address_type,confidence,balance_usd"
                )
                if filt == "eq":
                    q = q.eq("entity_name", safe)
                else:
                    q = q.ilike("entity_name", f"{safe}%")
                res = q.limit(limit).execute()
            except Exception as e:  # APIError (timeout 57014) etc.
                logger.warning("  query failed for term '%s' (%s): %s", safe, filt, e)
                continue
            for r in getattr(res, "data", None) or []:
                rid = r.get("id")
                if rid in seen_ids:
                    continue
                seen_ids.add(rid)
                rows.append(r)
    return rows


def rank_and_build_seeds(
    rows: List[Dict[str, Any]], *, verify_threshold: float
) -> List[Dict[str, Any]]:
    """Filter junk, rank, and convert label rows into seed-address dicts."""
    clean = []
    for r in rows:
        chain = str(r.get("blockchain") or "").lower().strip()
        if chain not in VALID_CHAINS:
            continue
        if is_junk(r.get("entity_name", "")):
            continue
        if not r.get("address"):
            continue
        clean.append(r)

    def sort_key(r):
        conf = r.get("confidence") or 0
        bal = r.get("balance_usd") or 0
        return (float(conf), float(bal))

    clean.sort(key=sort_key, reverse=True)

    seeds = []
    for r in clean:
        chain = str(r["blockchain"]).lower().strip()
        addr = str(r["address"])
        conf = float(r.get("confidence") or 0)
        label = (r.get("label") or r.get("entity_name") or "").strip()
        seeds.append({
            "address": addr,
            "chain": chain,
            "note": (f"{label} — label store (confidence {conf:.2f})").strip()[:280],
            "source": explorer_url(
                addr.lower() if chain in EVM_CHAINS else addr, chain
            ),
            "verified": conf >= verify_threshold,
        })
    return seeds


def run(
    *, live: bool, only: Optional[set], max_per_entity: int,
    verify_threshold: float, fetch_limit: int, include_prefix: bool = False,
    client=None,
) -> Dict[str, Any]:
    if client is None:
        from supabase import create_client
        from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
        client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    # Page through curated_entities.
    entities: List[Dict[str, Any]] = []
    page_size = 1000
    start = 0
    while True:
        res = (
            client.table("curated_entities")
            .select("slug, display_name, addresses")
            .range(start, start + page_size - 1)
            .execute()
        )
        batch = getattr(res, "data", None) or []
        entities.extend(batch)
        if len(batch) < page_size:
            break
        start += page_size

    summary = {
        "entities_scanned": 0, "entities_enriched": 0,
        "addresses_added": 0, "dupes_skipped": 0, "invalid": 0, "live": live,
    }

    for ent in entities:
        slug = ent["slug"]
        if only and slug not in only:
            continue
        summary["entities_scanned"] += 1

        terms = search_terms(ent.get("display_name") or slug)
        if not terms:
            continue
        label_rows = fetch_label_addresses(
            client, terms, limit=fetch_limit, include_prefix=include_prefix
        )
        seeds = rank_and_build_seeds(label_rows, verify_threshold=verify_threshold)
        if not seeds:
            continue
        seeds = seeds[:max_per_entity]

        existing = ent.get("addresses") or []
        result = merge_for_entity(slug, existing, seeds)
        added = result["added"]
        summary["dupes_skipped"] += len(result["skipped"])
        summary["invalid"] += len(result["invalid"])

        if not added:
            continue

        summary["entities_enriched"] += 1
        summary["addresses_added"] += len(added)
        verb = "WROTE" if live else "WOULD ADD"
        logger.info("%s %2d → %s (%s)  [%d existing]",
                    verb, len(added), slug, ent.get("display_name"), len(existing))

        if live:
            client.table("curated_entities").update(
                {"addresses": result["merged"]}
            ).eq("slug", slug).execute()

    logger.info(
        "%s — scanned: %d | enriched: %d | addresses added: %d | dupes: %d | invalid: %d",
        "LIVE" if live else "DRY-RUN",
        summary["entities_scanned"], summary["entities_enriched"],
        summary["addresses_added"], summary["dupes_skipped"], summary["invalid"],
    )
    return summary


def main() -> int:
    p = argparse.ArgumentParser(
        description="Enrich curated_entities.addresses from the addresses label store.",
    )
    g = p.add_mutually_exclusive_group()
    g.add_argument("--live", action="store_true", help="Write to Supabase (default: dry-run).")
    g.add_argument("--dry-run", action="store_true", help="Print changes only (default).")
    p.add_argument("--only", default="", help="Comma-separated slugs to limit to.")
    p.add_argument("--max-per-entity", type=int, default=25, help="Max new addresses per entity (default 25).")
    p.add_argument("--verify-threshold", type=float, default=0.8, help="confidence >= this => verified=True (default 0.8).")
    p.add_argument("--fetch-limit", type=int, default=100, help="Max label rows fetched per search term (default 100).")
    p.add_argument("--include-prefix", action="store_true", help="Also run prefix ILIKE (slower; may hit statement timeouts).")
    args = p.parse_args()

    live = args.live and not args.dry_run
    only = {s.strip() for s in args.only.split(",") if s.strip()} or None

    logger.info("curated_entities enrichment (%s) — max/entity=%d, verify>=%.2f, prefix=%s",
                "LIVE" if live else "DRY-RUN", args.max_per_entity, args.verify_threshold, args.include_prefix)
    summary = run(
        live=live, only=only, max_per_entity=args.max_per_entity,
        verify_threshold=args.verify_threshold, fetch_limit=args.fetch_limit,
        include_prefix=args.include_prefix,
    )
    return 0 if summary["invalid"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
