# Deployment notes

## Database prerequisites (backfill scripts)

Run the DDL in [migrations/002_backfill_progress.sql](../migrations/002_backfill_progress.sql) in the Supabase SQL editor before using `--live` on the backfill scripts.

## First deployment

1. Deploy with `INTERPRETER_ENABLED=False`
2. Watch logs for 1 hour. Confirm: label coverage log appears, label hit rate is non-zero on at least one chain (Ethereum will hit first since the only labeled addresses currently are EVM)
3. Run `scripts/backfill_labels.py --live` for last 7 days only (`--days 7` flag)
4. Watch label coverage rate increase
5. Set `INTERPRETER_ENABLED=True`, restart ingest
6. Watch logs for 1 hour. Confirm: `reasoning` field on new rows contains narrative Grok output, not template strings. Confirm: ingest latency hasn't degraded.
7. Run `scripts/backfill_labels.py --live` for the full 30 days
8. Run `scripts/backfill_reasoning.py` in dry-run, review cost estimate
9. Run `scripts/backfill_reasoning.py --live` if cost is acceptable

## Backfill scripts (order)

1. **Labels first:** `python scripts/backfill_labels.py` (dry-run by default). Use `--live` to write. Options: `--days N` (default 30), `--rate` (default 50/sec).
2. **Reasoning second:** `python scripts/backfill_reasoning.py` (dry-run by default). Use `--live` to write. Options: `--days N` (default 30). Aborts if estimated cost exceeds `BACKFILL_MAX_COST_USD` (default 50).

Environment variables (see `shared/config.py`): `BACKFILL_MAX_COST_USD`, `BACKFILL_COST_PER_CALL_USD`, `INTERPRETER_LABELED_USD_THRESHOLD`, `XAI_API_KEY`.
