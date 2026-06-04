"""Seed addresses for the frontend `curated_entities` table (figure pages).

WHY THIS EXISTS
---------------
There are two separate address stores in the stack:

  1. `addresses`         — backend label store (used to enrich
                            all_whale_transactions with from_label/to_label).
                            Populated by scripts/import_figures_seeds.py.
  2. `curated_entities`  — frontend store the public figure/entity DETAIL
                            pages actually read (app/figure/[slug]/page.js).
                            Each row has an `addresses` JSONB column.




Filling (1) does NOT fill (2). As of this writing 105 / 186 curated_entities
rows have an empty `addresses` array, so their public pages render with no
on-chain activity — including featured profiles (Coinbase, Binance, Kraken,
Aave, Uniswap, ...). This module is the curated source that the new
`scripts/import_curated_entities.py` importer reads to populate (2).

ROW SCHEMA (must match app/api/admin/figures/[slug]/addresses/route.js)
-----------------------------------------------------------------------
Each address dict written into curated_entities.addresses is:

    {
      "address":  str,    # case as published (EVM lowercased at import)
      "chain":    str,    # ethereum|polygon|arbitrum|optimism|base|bsc|
                          # avalanche|solana|bitcoin
      "note":     str,    # short human note
      "source":   str,    # REQUIRED public URL when verified=True
      "verified": bool,   # True only with a citation; gates featuring
    }

CURATION RULES (same discipline as seeds_figures.py)
----------------------------------------------------
  * Every verified=True row MUST carry a public `source` URL where the
    address appears next to the entity's name (tweet, article, Etherscan
    public name tag, ENS resolution, project docs, Dune dashboard).
  * No OFAC-sanctioned / Tornado addresses except clearly-labelled
    historical exploit/seizure entities (kept for completeness, flagged
    in `note`).
  * If a citation cannot be found, OMIT the row. A short verified list
    beats a long list with fakes.

The starter set below is lifted from the already-verified, already-cited
entries in arkham_backfill/seeds_figures.py and mapped onto their
curated_entities slugs. Everything here is import-ready and safe to ship.
Expand it with the deep-research output (see WHALE_PAGE_BRIEF / research
prompts) — add new `slug: [rows]` entries and re-run the importer.
"""

from __future__ import annotations

from typing import Dict, List, TypedDict


class AddressRow(TypedDict):
    address: str
    chain: str
    note: str
    source: str
    verified: bool

# ---------------------------------------------------------------------------
# slug -> list of address rows.
#
# Slugs MUST match an existing curated_entities.slug (the importer skips
# unknown slugs and reports them). The starter set reuses citations already
# vetted in seeds_figures.py.
# ---------------------------------------------------------------------------
CURATED_ENTITY_SEEDS: Dict[str, List[AddressRow]] = {
    # ---- People (ENS-resolved / self-disclosed) --------------------------
    "vitalik-buterin": [
        {
            "address": "0xd8da6bf26964af9d7eed9e03e53415d37aa96045",
            "chain": "ethereum",
            "note": "vitalik.eth — primary publicly-disclosed wallet (ENS forward-resolution).",
            "source": "https://app.ens.domains/vitalik.eth",
            "verified": True,
        },
    ],
    "cozomo-de-medici": [
        {
            "address": "0x3d3e52831b31f3c35170a4a094685748b2f8ea39",
            "chain": "ethereum",
            "note": "cozomo.eth — Snoop Dogg's NFT wallet, self-disclosed Sept 2021.",
            "source": "https://twitter.com/SnoopDogg/status/1437431576962744322",
            "verified": True,
        },
    ],
    "steve-aoki": [
        {
            "address": "0xa6d3a33a1c66083859765b9d6e407d095a908193",
            "chain": "ethereum",
            "note": "aoki.eth — publicly-disclosed wallet (ENS forward-resolution).",
            "source": "https://app.ens.domains/aoki.eth",
            "verified": True,
        },
    ],
    "pranksy": [
        {
            "address": "0xd387a6e4e84a6c86bd90c158c6028a58cc8ac459",
            "chain": "ethereum",
            "note": "pranksy.eth — long-time NFT collector (ENS forward-resolution).",
            "source": "https://app.ens.domains/pranksy.eth",
            "verified": True,
        },
    ],
    "hayden-adams": [
        {
            "address": "0x50ec05ade8280758e2077fcbc08d878d4aef79c3",
            "chain": "ethereum",
            "note": "hayden.eth — Uniswap founder (ENS forward-resolution).",
            "source": "https://app.ens.domains/hayden.eth",
            "verified": True,
        },
    ],
    "andre-cronje": [
        {
            "address": "0x2d407ddb06311396fe14d4b49da5f0471447d45c",
            "chain": "ethereum",
            "note": "andrecronje.eth — Yearn / Sonic founder (ENS forward-resolution).",
            "source": "https://app.ens.domains/andrecronje.eth",
            "verified": True,
        },
    ],
    "stani-kulechov": [
        {
            "address": "0x2e21f5d32841cf8c7da805185a041400bf15f21a",
            "chain": "ethereum",
            "note": "Etherscan public-name-tag 'Stani Kulechov' — Aave founder.",
            "source": "https://etherscan.io/address/0x2e21f5d32841cf8c7da805185a041400bf15f21a",
            "verified": True,
        },
    ],
    "mark-cuban": [
        {
            "address": "0x293ed38530005620e4b28600f196a97e1125daac",
            "chain": "ethereum",
            "note": "Etherscan public-name-tag 'Mark Cuban 2' — named in DLNews Sept 2023 hack coverage.",
            "source": "https://www.dlnews.com/articles/people-culture/mark-cuban-loses-870k-to-a-crypto-scam/",
            "verified": True,
        },
    ],
    "justin-sun": [
        {
            "address": "0x3ddfa8ec3052539b6c9549f12cea2c295cff5296",
            "chain": "ethereum",
            "note": "Etherscan public-name-tag 'Justin Sun'; Coingape coverage cites the address.",
            "source": "https://coingape.com/ethereum-whale-justin-sun-transfers-43-million-worth-eth-to-poloniex/",
            "verified": True,
        },
    ],
    "ansem": [
        {
            "address": "AVAZvHLR2PcWpDf8BXY4rVxNHYRBytycHkcB5z5QNXYm",
            "chain": "solana",
            "note": "Ansem-attributed Solana wallet — Dune 'Solana KOL Wallets' (4838225).",
            "source": "https://dune.com/queries/4838225",
            "verified": True,
        },
    ],

    # ---- Defunct / historical (kept, clearly flagged) --------------------
    "alameda-research": [
        {
            "address": "0xc098b2a3aa256d2140208c3de6543aaef5cd3a94",
            "chain": "ethereum",
            "note": "historical, defunct entity — Etherscan public-name-tag 'FTX 2' (Alameda cluster).",
            "source": "https://etherscan.io/address/0xc098b2a3aa256d2140208c3de6543aaef5cd3a94",
            "verified": True,
        },
    ],

    # ---- Market makers / funds -------------------------------------------
    "jump-trading": [
        {
            "address": "0xf584f8728b874a6a5c7a8d4d387c9aae9172d621",
            "chain": "ethereum",
            "note": "Etherscan public-name-tag 'Jump Trading'; Decrypt Wormhole-bailout coverage.",
            "source": "https://decrypt.co/92709/jump-crypto-wormhole-defi",
            "verified": True,
        },
    ],
    "wintermute": [
        {
            "address": "0x0000006daea1723962647b7e189d311d757fb793",
            "chain": "ethereum",
            "note": "Wintermute hot wallet — self-disclosed Sept 2022 hack tweet; Etherscan 'Wintermute 1'.",
            "source": "https://twitter.com/wintermute_t/status/1571504612785426432",
            "verified": True,
        },
    ],

    # ---- Exchanges / custodians (canonical hot wallets) ------------------
    "coinbase": [
        {
            "address": "0x71660c4005ba85c37ccec55d0c4493e66fe775d3",
            "chain": "ethereum",
            "note": "Etherscan public-name-tag 'Coinbase 1' — canonical hot wallet.",
            "source": "https://etherscan.io/address/0x71660c4005ba85c37ccec55d0c4493e66fe775d3",
            "verified": True,
        },
        {
            "address": "0xcd531ae9efcce479654c4926dec5f6209531ca7b",
            "chain": "ethereum",
            "note": "Etherscan public-name-tag 'Coinbase Prime 1' — institutional custody.",
            "source": "https://etherscan.io/address/0xcd531ae9efcce479654c4926dec5f6209531ca7b",
            "verified": True,
        },
        {
            "address": "0x7830c87c02e56aff27fa8ab1241711331fa86f43",
            "chain": "polygon",
            "note": "PolygonScan public-name-tag 'Coinbase: Deposit' — exchange deposit wallet.",
            "source": "https://polygonscan.com/address/0x7830c87c02e56aff27fa8ab1241711331fa86f43",
            "verified": True,
        },
    ],
    "kraken": [
        {
            "address": "0xe9f7ecae3a53d2a67105292894676b00d1fab785",
            "chain": "ethereum",
            "note": "Etherscan public-name-tag 'Kraken: Hot Wallet'.",
            "source": "https://etherscan.io/address/0xe9f7ecae3a53d2a67105292894676b00d1fab785",
            "verified": True,
        },
        {
            "address": "0x22af984f13dfb5c80145e3f9ee1050ae5a5fb651",
            "chain": "ethereum",
            "note": "Etherscan public-name-tag 'Kraken: Cold Wallet'.",
            "source": "https://etherscan.io/address/0x22af984f13dfb5c80145e3f9ee1050ae5a5fb651",
            "verified": True,
        },
        {
            "address": "0xc06f25517e906b7f9b4dec3c7889503bb00b3370",
            "chain": "ethereum",
            "note": "Etherscan public-name-tag 'Kraken: Cold Wallet 2'.",
            "source": "https://etherscan.io/address/0xc06f25517e906b7f9b4dec3c7889503bb00b3370",
            "verified": True,
        },
    ],
    "binance": [
        {
            "address": "0x28c6c06298d514db089934071355e5743bf21d60",
            "chain": "ethereum",
            "note": "Etherscan public-name-tag 'Binance 14'; cited in Lookonchain flow analyses.",
            "source": "https://twitter.com/lookonchain/status/1726675272340054488",
            "verified": True,
        },
        {
            "address": "0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be",
            "chain": "ethereum",
            "note": "Explorer public-name-tag 'Binance' — well-known large Binance exchange wallet.",
            "source": "https://etherscan.io/address/0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be",
            "verified": True,
        },
    ],

    # ---- Protocols / stablecoin issuers (canonical contracts/treasuries) -
    "aave": [
        {
            "address": "0x89c51828427f70d77875c6747759fb17ba10ceb0",
            "chain": "ethereum",
            "note": "Aave Grants DAO (AGD) multisig handling DAO treasury flows (Aave governance posts).",
            "source": "https://etherscan.io/address/0x89c51828427f70d77875c6747759fb17ba10ceb0",
            "verified": True,
        },
    ],
    "uniswap": [
        {
            "address": "0x1a9c8182c09f50c8318d769245bea52c32be35bc",
            "chain": "ethereum",
            "note": "Uniswap Governance Treasury (Uniswap Foundation TREASURY_ADDRESSES.md, Ethereum row).",
            "source": "https://etherscan.io/address/0x1a9c8182c09f50c8318d769245bea52c32be35bc",
            "verified": True,
        },
    ],
    "lido": [
        {
            "address": "0x3e40d73eb977dc6a537af587d48316fee66e9c8c",
            "chain": "ethereum",
            "note": "Etherscan public-name-tag 'Lido: Agent' — DAO agent / proxy admin (Lido docs).",
            "source": "https://etherscan.io/address/0x3e40d73eb977dc6a537af587d48316fee66e9c8c",
            "verified": True,
        },
        {
            "address": "0xd65fa54f8df43064dfd8ddf223a446fc638800a9",
            "chain": "polygon",
            "note": "Lido on Polygon DAO/treasury admin multisig (Lido Polygon deployment metadata).",
            "source": "https://polygonscan.com/address/0xd65fa54f8df43064dfd8ddf223a446fc638800a9",
            "verified": True,
        },
    ],
    "circle": [
        {
            "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "chain": "ethereum",
            "note": "Canonical USDC ERC-20 contract on Ethereum (Circle developer docs). Token contract, not a treasury wallet.",
            "source": "https://developers.circle.com/stablecoins/usdc-contract-addresses",
            "verified": True,
        },
    ],
    "tether": [
        {
            "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "chain": "ethereum",
            "note": "Canonical USDT ERC-20 contract on Ethereum. Token contract, not a treasury wallet.",
            "source": "https://etherscan.io/address/0xdac17f958d2ee523a2206206994597c13d831ec7",
            "verified": True,
        },
    ],
    "bybit": [
        {
            "address": "0x1db92e2eebc8e0c075a02bea49a2935bcd2dfcf4",
            "chain": "ethereum",
            "note": "Etherscan public-name-tag 'Bybit: Cold Wallet 1' (referenced in Feb 2025 cold-wallet exploit reports).",
            "source": "https://etherscan.io/address/0x1db92e2eebc8e0c075a02bea49a2935bcd2dfcf4",
            "verified": True,
        },
    ],

    # ---- Additional figure wallets from research ------------------------
    "cobie": [
        {
            "address": "0x4cbe68d825d21cb4978f56815613eed06cf30152",
            "chain": "ethereum",
            "note": "Cobie's address per pcaversaccio/escrow-contract README (contract 'governed by Cobie').",
            "source": "https://github.com/pcaversaccio/escrow-contract",
            "verified": True,
        },
    ],
    "arthur-hayes": [
        {
            "address": "0xa86e3d1c80a750a310b484fb9bdc470753a7506f",
            "chain": "ethereum",
            "note": "Community-attributed: Etherscan 'Arthur Hayes 2' (cites a tweet). Medium confidence — not self-disclosed.",
            "source": "https://etherscan.io/address/0xa86e3d1c80a750a310b484fb9bdc470753a7506f",
            "verified": False,
        },
    ],
    # ---- Deep-research batch 2 (pseudonymous / individual) ---------------
    "gainzy": [
        {
            "address": "0x333345727be2ec482baf99a25cb1765cb7b78de6",
            "chain": "ethereum",
            "note": "gainzy222.eth — ENS forward-resolution; OpenSea 'gainzy' profile + matching @gainzy222 X bio. Self-persona, ENS-backed.",
            "source": "https://etherscan.io/address/0x333345727be2ec482baf99a25cb1765cb7b78de6",
            "verified": True,
        },
        {
            "address": "0x333345727be2ec482baf99a25cb1765cb7b78de6",
            "chain": "base",
            "note": "Same EVM EOA resolving via gainzy222.eth on Base.",
            "source": "https://basescan.org/address/0x333345727be2ec482baf99a25cb1765cb7b78de6",
            "verified": True,
        },
        {
            "address": "5UqXrqhGHnu6CkUArmqvT8rQsAtLo7c8V9TJU2ECCnrj",
            "chain": "solana",
            "note": "Community OSINT (@Gr1zzlyTrades thread): RLB buy wallet matched to Gainzy's posted trades. Not self-disclosed — low confidence.",
            "source": "https://tradersunion.com/persons/gainzy/",
            "verified": False,
        },
        {
            "address": "FwawkT3Zuzh94pBXR2aNognqPxUM3qgj4hHQ8394d9iH",
            "chain": "solana",
            "note": "Community OSINT: traced from 5Uqx via shared Rollbit deposit address. Low confidence — investigator attribution.",
            "source": "https://tradersunion.com/persons/gainzy/",
            "verified": False,
        },
    ],
    # CryptoPunk Whale normalized to Punk6529 (most-cited self-branded
    # CryptoPunk whale with a public ENS + museum).
    "cryptopunk-whale": [
        {
            "address": "0xfd22004806a6846ea67ad883356be810f0428793",
            "chain": "ethereum",
            "note": "Punk6529 — genesis.punk6529.eth ('6529 Transactional Wallet'); sends CryptoPunks to 6529museum.eth.",
            "source": "https://etherscan.io/address/0xfd22004806a6846ea67ad883356be810f0428793",
            "verified": True,
        },
    ],
    "tyler-hobbs": [
        {
            "address": "0x33c9371d25ce44a408f8a6473fbad86bf81e1a17",
            "chain": "ethereum",
            "note": "1.tylerxhobbs.eth — documented in qql-art/qql-headless repo as Tyler's address; first-party + ENS.",
            "source": "https://etherscan.io/address/0x33c9371d25ce44a408f8a6473fbad86bf81e1a17",
            "verified": True,
        },
        {
            "address": "0x60ac3d2f372a0a3bd28493b73034370338e193b2",
            "chain": "ethereum",
            "note": "1.collection.tylerxhobbs.eth — collection wallet under the same ENS root (QQL/Fidenza).",
            "source": "https://etherscan.io/address/0x60ac3d2f372a0a3bd28493b73034370338e193b2",
            "verified": True,
        },
    ],
}


def total_rows() -> int:
    return sum(len(v) for v in CURATED_ENTITY_SEEDS.values())
