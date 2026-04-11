"""Shared configuration for automation modules.

Loads from environment variables with sane defaults.
On the droplet, secrets live in /etc/sonar/automation.env
and are injected via systemd EnvironmentFile=.
"""

import os

# --- xAI / Grok Interpreter ---
XAI_API_KEY: str = os.getenv("XAI_API_KEY", "")
INTERPRETER_ENABLED: bool = os.getenv("INTERPRETER_ENABLED", "true").lower() in ("true", "1", "yes")
INTERPRETER_TIMEOUT_SECONDS: int = int(os.getenv("INTERPRETER_TIMEOUT_SECONDS", "60"))
INTERPRETER_MODEL: str = os.getenv("INTERPRETER_MODEL", "grok-4.20-beta-0309-reasoning")

# Dual interpreter thresholds: labeled txs get interpreted at a lower
# dollar threshold because the labels make the Grok output much richer.
INTERPRETER_LABELED_USD_THRESHOLD: int = int(os.getenv("INTERPRETER_LABELED_USD_THRESHOLD", "500000"))
INTERPRETER_UNLABELED_USD_THRESHOLD: int = int(os.getenv("INTERPRETER_UNLABELED_USD_THRESHOLD", "2000000"))

# Legacy single threshold — kept as fallback default for both above.
# Prefer the labeled/unlabeled variants in new code.
INTERPRETER_USD_THRESHOLD: int = int(os.getenv("INTERPRETER_USD_THRESHOLD", "500000"))

# --- Address label lookup ---
LABEL_COVERAGE_LOG_INTERVAL: int = int(os.getenv("LABEL_COVERAGE_LOG_INTERVAL", "1000"))

# --- Backfill scripts (reasoning / labels) ---
BACKFILL_MAX_COST_USD: float = float(os.getenv("BACKFILL_MAX_COST_USD", "50"))
BACKFILL_COST_PER_CALL_USD: float = float(os.getenv("BACKFILL_COST_PER_CALL_USD", "0.005"))
