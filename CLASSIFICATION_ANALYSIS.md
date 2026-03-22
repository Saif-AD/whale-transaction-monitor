# Transaction Classification Analysis

## Overview

Sonar classifies whale transactions across 5 blockchains using chain-specific logic tailored to each chain's architecture. The goal: determine whether a large transaction represents a **BUY** (accumulation), **SELL** (distribution to exchange), or **TRANSFER** (neutral movement).

```
                    ┌─────────────────────────────────┐
                    │     Incoming Transaction         │
                    │   (passes threshold + dedup)     │
                    └──────────────┬──────────────────┘
                                   │
              ┌────────────┬───────┴───────┬────────────┬──────────┐
              ▼            ▼               ▼            ▼          ▼
         ┌─────────┐ ┌──────────┐   ┌──────────┐ ┌─────────┐ ┌───────┐
         │ Bitcoin  │ │ Ethereum │   │  Solana   │ │   XRP   │ │Polygon│
         │  UTXO    │ │  EVM     │   │  Account  │ │  XRPL   │ │  EVM  │
         └────┬────┘ └────┬─────┘   └────┬─────┘ └────┬────┘ └───┬───┘
              │           │              │             │          │
              ▼           ▼              ▼             ▼          ▼
         Local UTXO   2-Stage       Program ID     Dest Tag   2-Stage
         Classifier   Whale Intel   + CEX/DEX      + CEX      Whale Intel
                      Engine        Matching       Matching    Engine
```

---

## Bitcoin (UTXO Model)

Bitcoin uses a **UTXO classifier** that understands inputs/outputs. Each output is classified independently.

```
Transaction arrives (≥$50K output)
         │
         ▼
┌─────────────────────────────────────┐
│  Resolve input addresses            │
│  (mempool.space API for prevout)    │
└──────────────┬──────────────────────┘
               │
         ┌─────▼─────┐
         │ For each   │
         │  output:   │
         └─────┬─────┘
               │
    ┌──────────▼──────────┐     YES
    │ to_addr in inputs?  ├──────────► SKIP (change output, not printed)
    │ (change output)     │
    └──────────┬──────────┘
               │ NO
    ┌──────────▼──────────┐     YES
    │ Coinbase tx?        ├──────────► MINING_REWARD
    │ (no inputs)         │
    └──────────┬──────────┘
               │ NO
    ┌──────────▼──────────┐     YES    ┌──────────────────────┐
    │ Any input from      ├───────────►│ to_addr is exchange? │
    │ known exchange?     │            └──────────┬───────────┘
    └──────────┬──────────┘                YES    │    NO
               │ NO                        │      │
               │                    TRANSFER ◄────┘    ► BUY
               │                  (internal)       (withdrawal)
    ┌──────────▼──────────┐     YES
    │ to_addr is a known  ├──────────► SELL (deposit to exchange)
    │ exchange?           │
    └──────────┬──────────┘
               │ NO
    ┌──────────▼──────────┐     YES
    │ ≥5 inputs,          ├──────────► CONSOLIDATION
    │ ≤2 outputs?         │
    └──────────┬──────────┘
               │ NO
    ┌──────────▼──────────┐     YES
    │ ≤2 inputs,          ├──────────► DISTRIBUTION
    │ ≥5 outputs?         │
    └──────────┬──────────┘
               │ NO
    ┌──────────▼──────────┐     YES
    │ 3+ inputs, 3+ outs  ├──────────► MIXING (CoinJoin)
    │ with equal values?  │
    └──────────┬──────────┘
               │ NO
    ┌──────────▼──────────┐     YES
    │ 2 outputs, other    ├──────────► BUY (this is the non-change output)
    │ output is change?   │
    └──────────┬──────────┘
               │ NO
    ┌──────────▼──────────┐     YES
    │ Round amount         ├──────────► BUY (OTC accumulation)
    │ ≥5 BTC?             │
    └──────────┬──────────┘
               │ NO
               ▼
           TRANSFER (default)
```

### Bitcoin Classification Summary

| Classification | Signal | Confidence | Sentiment |
|---|---|---|---|
| **BUY** | Exchange withdrawal / change heuristic / round amount | High (exchange) or Medium (heuristic) | Bullish |
| **SELL** | Deposit TO known exchange address | High | Bearish |
| **TRANSFER** | Wallet-to-wallet, no exchange involvement | Low | Neutral |
| **CONSOLIDATION** | Many inputs → few outputs | Medium | Neutral |
| **DISTRIBUTION** | Few inputs → many outputs | Medium | Neutral |
| **MIXING** | CoinJoin pattern (equal outputs) | Medium | Neutral |
| **MINING_REWARD** | Coinbase transaction | High | Bullish |

### Key Limitation
SELL detection requires the destination to be a **known** exchange address. We have ~150 exchange addresses, but exchanges generate unique per-user deposit addresses not in any public list. BUYs are more reliably detected because withdrawals come from a handful of known hot wallets.

---

## Ethereum & Polygon (EVM Chains)

EVM chains use a **2-stage Whale Intelligence Engine** with an EVM fast-path shortcut.

```
ERC-20 Transfer event arrives (≥$10K)
         │
         ▼
┌─────────────────────────────────────┐
│  FAST PATH (pre-check)              │
│  Checks from_addr and to_addr       │
│  against known address databases    │
└──────────────┬──────────────────────┘
               │
    ┌──────────▼──────────┐     YES
    │ Null address         ├──────────► MINT (from 0x000...0)
    │ (0x000...0)?         │            BURN (to 0x000...0)
    └──────────┬──────────┘
               │ NO
    ┌──────────▼──────────┐     YES
    │ To/From DEX router?  ├──────────► BUY (from DEX) or SELL (to DEX)
    │ (Uniswap, 1inch,    │
    │  CoW, Curve, etc.)  │
    └──────────┬──────────┘
               │ NO
    ┌──────────▼──────────┐     YES
    │ To/From DeFi         ├──────────► DEPOSIT or WITHDRAWAL
    │ protocol?            │
    │ (Aave, Compound...)  │
    └──────────┬──────────┘
               │ NO (fast path inconclusive)
               ▼
┌─────────────────────────────────────┐
│  STAGE 1: Rule-Based Analysis       │
│                                     │
│  • Check from/to against Supabase   │
│    address database                 │
│  • CEX classification (exchange     │
│    deposit/withdrawal detection)    │
│  • Wallet behavior analysis         │
│  • Pattern matching                 │
│                                     │
│  Produces: classification +         │
│            confidence score         │
└──────────────┬──────────────────────┘
               │
    ┌──────────▼──────────────────┐
    │ Confidence ≥ 0.85 and       │  YES
    │ no conflicting signals?     ├──────────► EARLY EXIT (return result)
    └──────────┬──────────────────┘
               │ NO
               ▼
┌─────────────────────────────────────┐
│  STAGE 2: API Enrichment            │
│                                     │
│  • Query Etherscan/PolygonScan      │
│    for address labels               │
│  • Transaction history analysis     │
│  • Contract verification            │
│                                     │
│  If still low confidence:           │
│  • TIER 3: BigQuery historical      │
│    analysis (most expensive)        │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  MASTER DECISION                    │
│                                     │
│  Weighs all signals:                │
│  • cex_classification (highest)     │
│  • wallet_behavior                  │
│  • historical_patterns              │
│  • api_enrichment                   │
│                                     │
│  Returns: BUY / SELL / TRANSFER     │
│           + confidence + reasoning  │
└─────────────────────────────────────┘
```

### EVM DEX Routers Recognized
| Protocol | Address | Detection |
|---|---|---|
| Uniswap V2 | `0x7a250d...` | DEX swap |
| Uniswap V3 | `0xE592427...` | DEX swap |
| Uniswap Universal | `0x3fC91A...` | DEX swap |
| Uniswap Permit2 | `0x00000000000444...` | DEX swap |
| 1inch V5/V6 | `0x1111111...` | DEX swap |
| SushiSwap | `0xd9e1cE17...` | DEX swap |
| CoW Protocol | `0x9008d19...` | DEX swap |
| Curve Router | `0x99a5848...` | DEX swap |
| Balancer V2 | `0xBA12222...` | DEX swap |
| 0x Exchange | `0xDef1C0d...` | DEX swap |
| PancakeSwap V3 | `0x13f4EA8...` | DEX swap |

---

## Solana

Solana uses **CEX/DEX address matching** combined with **program ID detection** (from gRPC instruction data).

```
SPL Token transfer arrives (≥$10K)
         │
         ▼
┌─────────────────────────────────────┐
│  gRPC: Collect all balance changes  │
│  per (tx_signature, symbol)         │
│                                     │
│  Pick ONE event per tx:             │
│  → Non-pool/non-exchange address    │
│  → Largest USD value if tie         │
└──────────────┬──────────────────────┘
               │
    ┌──────────▼──────────┐     YES
    │ Owner is known CEX?  ├──────────► If receiving: BUY (withdrawal)
    │ (Binance, Coinbase,  │            If sending: SELL (deposit)
    │  Kraken, OKX, etc.)  │
    └──────────┬──────────┘
               │ NO
    ┌──────────▼──────────┐     YES
    │ Owner is known DEX?  ├──────────► If receiving: SELL (swap out)
    │ (Jupiter, Raydium,   │            If sending: BUY (swap in)
    │  Orca, Phoenix)      │
    └──────────┬──────────┘
               │ NO
    ┌──────────▼──────────────────┐
    │ Program IDs in transaction? │
    │ (gRPC only)                 │
    └──────────┬──────────────────┘
               │
    ┌──────────▼──────────┐     YES
    │ DEX program?         ├──────────► BUY or SELL (based on amount direction)
    │ (Jupiter, Raydium    │
    │  AMM/CLMM, Orca     │
    │  Whirlpool, Lifinity)│
    └──────────┬──────────┘
               │ NO
    ┌──────────▼──────────┐     YES
    │ Staking program?     ├──────────► STAKE or UNSTAKE
    │ (Marinade, Jito,     │
    │  Sanctum, SPL Stake) │
    └──────────┬──────────┘
               │ NO
    ┌──────────▼──────────┐     YES
    │ Lending program?     ├──────────► LEND or REDEEM
    │ (Solend, Marginfi,   │
    │  Kamino, Drift)      │
    └──────────┬──────────┘
               │ NO
               ▼
    Amount change > 0 → BUY (default: receiving tokens)
    Amount change < 0 → SELL (default: sending tokens)
    Fallback         → TRANSFER
```

### Solana Data Sources (3 parallel streams)

| Source | Method | Strength |
|---|---|---|
| **gRPC (Yellowstone)** | Alchemy Geyser stream | Fastest, has program IDs |
| **WebSocket** | Alchemy token account subscribe | Backup, real-time |
| **API Poller** | Alchemy getSignaturesForAddress | Catches missed txs |

---

## XRP (XRP Ledger)

XRP classification uses **exchange address matching** and **DestinationTag** analysis.

```
XRP Payment arrives (≥$10K)
         │
         ▼
    ┌──────────▼──────────┐     YES
    │ Ripple Treasury      ├──────────► SKIP (internal Ripple ops)
    │ address? (>$50M)     │
    └──────────┬──────────┘
               │ NO
    ┌──────────▼──────────┐     YES
    │ From known exchange? ├──────────► BUY (exchange withdrawal)
    └──────────┬──────────┘
               │ NO
    ┌──────────▼──────────┐     YES
    │ To known exchange?   ├──────────► SELL (exchange deposit)
    └──────────┬──────────┘
               │ NO
    ┌──────────▼──────────┐     YES
    │ Has DestinationTag?  ├──────────► SELL (deposit to exchange —
    │ + to non-exchange    │            dest tags identify accounts)
    └──────────┬──────────┘
               │ NO
    ┌──────────▼──────────┐     YES
    │ Large round amount?  ├──────────► OTC_TRANSFER
    │ (≥10K XRP, ≥$500K   │            (counts as BUY)
    │  whole/1000 incr.)   │
    └──────────┬──────────┘
               │ NO
               ▼
           TRANSFER (default)
```

### XRP-Specific Signals

| Signal | Meaning | Why |
|---|---|---|
| **DestinationTag present** | Likely exchange deposit | Exchanges require dest tags to route to user accounts |
| **No DestinationTag** | Wallet-to-wallet | Direct transfers don't need routing tags |
| **Round amount ≥$500K** | OTC deal | Institutional trades use clean round numbers |
| **Ripple Treasury** | Filtered out | Ripple moves billions internally; not market signal |

---

## Deduplication (All Chains)

Before any transaction reaches the frontend, it passes through the dedup engine.
Windows are **chain-specific** — XRP and Bitcoin have naturally repetitive round
amounts (10K XRP, 1 BTC), so they use tighter windows to avoid over-suppression.

```
Event arrives
    │
    ▼
┌─ Stablecoin filter ──────► SKIP (USDT, USDC, DAI, BUSD, TUSD, FDUSD)
│
├─ EVM tx_hash dedup ──────► SKIP if same tx_hash seen (multi-leg swaps)
│                             (Ethereum, Polygon, BSC only)
│
├─ Exact hash dedup ───────► SKIP if same (chain, tx_hash, log_index/sequence)
│
├─ Circular flow ──────────► SKIP if A→B then B→A same amount
│                             (all chains, no time limit)
│
├─ Same-dest same-amount ──► SKIP if same to_addr + similar amount
│                             XRP/BTC: 30-second window
│                             Solana/EVM: 300-second window
│
├─ Chained transfer ───────► SKIP if A→B then B→C same amount (tumbling)
│                             XRP/BTC: 60-second window
│                             Solana/EVM: 300-second window
│
├─ Same-amount flood ──────► SKIP if same chain+symbol+amount (±1%)
│                             Solana/EVM only (120-second window)
│                             XRP/BTC EXEMPT (round amounts are legit)
│
└─ ✅ PASS ────────────────► Push to frontend + store in Supabase
```

---

## Frontend Display

| Classification | Badge Color | Counted In Stats | Sentiment Signal |
|---|---|---|---|
| BUY | 🟢 Green | Yes (buy counter) | Bullish |
| SELL | 🔴 Red | Yes (sell counter) | Bearish |
| TRANSFER | ⚪ Grey | No | Neutral |
| CONSOLIDATION | ⚪ Grey | No | Neutral |
| DISTRIBUTION | ⚪ Grey | No | Neutral |
| MIXING | ⚪ Grey | No | Neutral |
| MINING_REWARD | 🟢 Green | Yes (buy counter) | Bullish |
| OTC_TRANSFER | 🟢 Green | Yes (buy counter) | Bullish |
| STAKE/UNSTAKE | ⚪ Grey | No | Neutral |
| MINT/BURN | ⚪ Grey | No | Neutral |
