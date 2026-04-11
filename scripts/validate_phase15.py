#!/usr/bin/env python3
"""Production validation diagnostic for Phase 1.5 / 1.6 deployment.

Queries all_whale_transactions for a recent window and reports:
  - Total row count (overall + per-chain)
  - Label coverage (from_label, to_label non-empty rates)
  - Narrative reasoning rates (length > 80 chars, proxy for Grok output)
  - PASS / FAIL verdict with likely causes

Read-only, safe to run anytime.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from supabase import create_client

from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

NARRATIVE_MIN_LEN = 80
HIGH_VALUE_USD = 500_000
PAGE_SIZE = 1000


def _is_narrative(reasoning: str | None) -> bool:
    if not reasoning or len(reasoning) < NARRATIVE_MIN_LEN:
        return False
    lower = reasoning.lower()
    if "stage" in lower and "classification" in lower:
        return False
    return True


def _paginate(client, since_iso: str):
    offset = 0
    while True:
        r = (
            client.table("all_whale_transactions")
            .select(
                "transaction_hash,blockchain,from_label,to_label,"
                "reasoning,usd_value,timestamp"
            )
            .gte("timestamp", since_iso)
            .order("timestamp", desc=True)
            .order("transaction_hash", desc=True)
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        rows = r.data or []
        if not rows:
            break
        yield rows
        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Phase 1.5/1.6 deployment.")
    parser.add_argument(
        "--hours",
        type=float,
        default=1.0,
        help="Look-back window in hours (default: 1)",
    )
    args = parser.parse_args()

    since = datetime.now(timezone.utc) - timedelta(hours=args.hours)
    since_iso = since.isoformat()

    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    total = 0
    chain_total: dict[str, int] = defaultdict(int)
    from_label_hits = 0
    to_label_hits = 0
    chain_from_label: dict[str, int] = defaultdict(int)
    chain_to_label: dict[str, int] = defaultdict(int)
    narrative_high_value = 0
    total_high_value = 0
    chain_narrative_hv: dict[str, int] = defaultdict(int)
    chain_total_hv: dict[str, int] = defaultdict(int)
    narrative_all = 0
    chain_narrative_all: dict[str, int] = defaultdict(int)

    for page in _paginate(client, since_iso):
        for row in page:
            total += 1
            chain = (row.get("blockchain") or "unknown").lower()
            chain_total[chain] += 1

            fl = row.get("from_label") or ""
            tl = row.get("to_label") or ""
            if fl.strip():
                from_label_hits += 1
                chain_from_label[chain] += 1
            if tl.strip():
                to_label_hits += 1
                chain_to_label[chain] += 1

            reasoning = row.get("reasoning") or ""
            is_narr = _is_narrative(reasoning)
            if is_narr:
                narrative_all += 1
                chain_narrative_all[chain] += 1

            usd = float(row.get("usd_value") or 0)
            if usd >= HIGH_VALUE_USD:
                total_high_value += 1
                chain_total_hv[chain] += 1
                if is_narr:
                    narrative_high_value += 1
                    chain_narrative_hv[chain] += 1

    # --- Report ---
    pct = lambda n, d: f"{n / d * 100:.1f}%" if d else "N/A"
    sep = "-" * 60

    print(f"Phase 1.5/1.6 Validation — window: last {args.hours}h (since {since_iso})")
    print(sep)
    print(f"Total rows: {total:,}")
    for ch in sorted(chain_total):
        print(f"  {ch}: {chain_total[ch]:,}")
    print()

    print(f"Label coverage:")
    print(f"  from_label non-empty: {from_label_hits:,} / {total:,} ({pct(from_label_hits, total)})")
    print(f"  to_label   non-empty: {to_label_hits:,} / {total:,} ({pct(to_label_hits, total)})")
    any_label = from_label_hits + to_label_hits
    label_rate = any_label / (2 * total) if total else 0
    print(f"  Combined label hit rate: {label_rate * 100:.1f}%")

    for ch in sorted(chain_total):
        ct = chain_total[ch]
        fl_ch = chain_from_label.get(ch, 0)
        tl_ch = chain_to_label.get(ch, 0)
        print(f"    {ch}: from={pct(fl_ch, ct)}  to={pct(tl_ch, ct)}")
    print()

    print(f"Narrative reasoning (>{NARRATIVE_MIN_LEN} chars, not template):")
    print(f"  All rows:        {narrative_all:,} / {total:,} ({pct(narrative_all, total)})")
    print(f"  ${HIGH_VALUE_USD / 1e6:.0f}M+ rows:  {narrative_high_value:,} / {total_high_value:,} ({pct(narrative_high_value, total_high_value)})")

    for ch in sorted(chain_total_hv):
        hv = chain_total_hv[ch]
        nv = chain_narrative_hv.get(ch, 0)
        print(f"    {ch} (${HIGH_VALUE_USD / 1e6:.0f}M+): {nv:,} / {hv:,} ({pct(nv, hv)})")
    print()

    # --- Verdict ---
    print(sep)
    failures: list[str] = []

    if total == 0:
        failures.append(
            "No rows in the last hour — ingest may be down or no whale txs occurred"
        )

    has_any_label = from_label_hits > 0 or to_label_hits > 0
    if total > 0 and not has_any_label:
        failures.append(
            "0% label hit rate — label lookup may not be wired, or the addresses table "
            "has no matching rows for recent whale tx counterparties"
        )

    if total_high_value > 0:
        narr_rate = narrative_high_value / total_high_value
        if narr_rate < 0.50:
            failures.append(
                f"Narrative reasoning rate on ${HIGH_VALUE_USD / 1e6:.0f}M+ rows is "
                f"{narr_rate * 100:.1f}% (< 50%) — interpreter may be disabled "
                f"(INTERPRETER_ENABLED=False), XAI_API_KEY may be missing, or the "
                f"USD threshold is too high"
            )

    if failures:
        print("VERDICT: FAIL")
        print()
        print("Likely causes:")
        for i, cause in enumerate(failures, 1):
            print(f"  {i}. {cause}")
    else:
        print("VERDICT: PASS")
        print()
        print("Label coverage > 0% and narrative reasoning >= 50% on high-value rows.")

    print(sep)
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
