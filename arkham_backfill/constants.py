"""Arkham backfill constants — chain mapping, upsert defaults, type overrides."""

# Maps Arkham's chain identifiers (from transfer responses) to our DB blockchain names.
# None = chain not tracked by us, skip silently.
ARKHAM_CHAIN_MAP = {
    "ethereum": "ethereum",
    "bitcoin": "bitcoin",
    "solana": "solana",
    "base": "base",
    "arbitrum_one": "arbitrum",
    "polygon": "polygon",
    "tron": None,
    "bsc": None,
    "avalanche": None,
    "optimism": None,
    "dogecoin": None,
    "flare": None,
}

# Default column values for rows inserted via Arkham transfer mining.
ARKHAM_DEFAULTS = {
    "confidence": 0.95,
    "signal_potential": "HIGH",
    "detection_method": "arkham_transfer_mining",
    "source": "arkham_api",
}

# Per-entity-type overrides applied on top of ARKHAM_DEFAULTS.
ENTITY_TYPE_OVERRIDES = {
    "historical": {"signal_potential": "LOW", "confidence": 0.80},
    "individual": {"signal_potential": "MEDIUM"},
}

# Credit budget guard — abort run if remaining Arkham credits fall below this.
CREDIT_FLOOR = 500
