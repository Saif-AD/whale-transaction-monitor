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

# --- Whale poster (Phase 1B) ---
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHANNEL_ID: str = os.getenv("TELEGRAM_CHANNEL_ID", "")
TELEGRAM_BOT_TOKEN_ADMIN: str = os.getenv("TELEGRAM_BOT_TOKEN_ADMIN", "")
TELEGRAM_ADMIN_CHAT_ID: str = os.getenv("TELEGRAM_ADMIN_CHAT_ID", "")

POSTER_USD_THRESHOLD: int = int(os.getenv("POSTER_USD_THRESHOLD", "1000000"))
POSTER_POLL_INTERVAL_SECONDS: int = int(os.getenv("POSTER_POLL_INTERVAL_SECONDS", "60"))
POSTER_DRY_RUN: bool = os.getenv("POSTER_DRY_RUN", "true").lower() in ("true", "1", "yes")
POST_CEX_INTERNAL: bool = os.getenv("POST_CEX_INTERNAL", "false").lower() in ("true", "1", "yes")
MIN_SECONDS_BETWEEN_SAME_TOKEN_POSTS: int = int(os.getenv("MIN_SECONDS_BETWEEN_SAME_TOKEN_POSTS", "3600"))

# Optional hard floor — if set, poster will never post txs older than this.
# ISO 8601 format. Belt-and-suspenders safety for watermark corruption.
POSTER_MIN_TX_TIMESTAMP: str = os.getenv("POSTER_MIN_TX_TIMESTAMP", "")

STABLECOIN_SYMBOLS: frozenset = frozenset({
    "USDT", "USDC", "DAI", "FDUSD", "TUSD", "PYUSD",
    "USDe", "sUSDe", "crvUSD", "USDD", "FRAX",
    "LUSD", "GUSD", "USDP", "BUSD",
})
