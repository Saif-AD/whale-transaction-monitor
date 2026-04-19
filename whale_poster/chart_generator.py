"""Render a 24h candlestick chart for a whale transaction, Sonar-branded.

The only public entry point is ``generate_whale_chart(tx)`` which returns a
filesystem path to a PNG or ``None`` on any failure. The function is
deliberately forgiving: we never raise from here — the poster's fallback is a
plain text-only Telegram message.

CoinGecko OHLC is used as the candle source (24 hour window). Raw CoinGecko
data is resampled to 30-minute bars for a consistent look across tokens.
Tokens without a CoinGecko mapping fall back to text-only by returning None.

Visual style is tuned to match the sonartracker.io dashboard:
  * Deep navy background (#020617)
  * Teal up-candles (#14b8a6) and rose down-candles (#f43f5e)
  * Subtle slate grid (#1e293b), slate axis labels (#64748b)
  * Multi-run title with Sonar teal accent on the ticker
  * Translucent fill + hard border annotation callouts
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

MIN_OHLC_POINTS = 10
RESAMPLE_RULE = "30min"

_COINGECKO_OHLC_URL = "https://api.coingecko.com/api/v3/coins/{id}/ohlc"
_FETCH_TIMEOUT = 5.0

# ---------------------------------------------------------------------------
# Sonar brand palette
# ---------------------------------------------------------------------------

_BG_COLOR = "#020617"          # deep navy, matches sonartracker.io
_UP_COLOR = "#14b8a6"          # teal (Sonar brand)
_DOWN_COLOR = "#f43f5e"        # rose
_NEUTRAL_COLOR = "#64748b"     # slate
_GRID_COLOR = "#1e293b"        # subtle slate
_AXIS_COLOR = "#64748b"        # slate labels
_TITLE_COLOR = "#f1f5f9"       # near-white
_TITLE_MUTED = "#64748b"       # muted secondary
_BRAND_TEAL = "#14b8a6"        # brand accent
_WATERMARK_MUTED = "#64748b"

# Translucent callout fills (RGBA in 0..1 space)
_SELL_FILL = (0xF4 / 255, 0x3F / 255, 0x5E / 255, 0.15)
_BUY_FILL = (0x14 / 255, 0xB8 / 255, 0xA6 / 255, 0.15)
_NEUTRAL_FILL = (0x64 / 255, 0x74 / 255, 0x8B / 255, 0.15)


COINGECKO_IDS = {
    "BTC": "bitcoin",
    "WBTC": "wrapped-bitcoin",
    "ETH": "ethereum",
    "WETH": "weth",
    "SOL": "solana",
    "XRP": "ripple",
    "LINK": "chainlink",
    "AAVE": "aave",
    "UNI": "uniswap",
    "ONDO": "ondo-finance",
    "PEPE": "pepe",
    "WLD": "worldcoin-wld",
    "VIRTUAL": "virtual-protocol",
    "COMP": "compound-governance-token",
    "SAND": "the-sandbox",
    "TRUMP": "official-trump",
    "ARB": "arbitrum",
    "OP": "optimism",
    "MATIC": "matic-network",
    "AVAX": "avalanche-2",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compact_usd(value: float) -> str:
    """Compact money format: $3.45K, $65.0K, $650K, $1.20M, $4.50B.

    Uses two decimal places for values in ``[$1K, $10K)`` so y-axis ticks
    for tokens like ETH (~$3500) don't all collapse to the same label.
    """
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "$0"
    sign = "-" if v < 0 else ""
    v = abs(v)
    if v >= 1_000_000_000:
        return f"{sign}${v / 1_000_000_000:.2f}B"
    if v >= 1_000_000:
        return f"{sign}${v / 1_000_000:.2f}M"
    if v >= 100_000:
        return f"{sign}${v / 1_000:.0f}K"
    if v >= 10_000:
        return f"{sign}${v / 1_000:.1f}K"
    if v >= 1_000:
        return f"{sign}${v / 1_000:.2f}K"
    if v >= 1:
        return f"{sign}${v:.2f}"
    if v > 0:
        return f"{sign}${v:.4f}"
    return "$0"


def _classification_color(classification: Optional[str]) -> str:
    """Map a classification string to the accent color."""
    if not classification:
        return _NEUTRAL_COLOR
    c = classification.strip().upper()
    if c == "BUY":
        return _UP_COLOR
    if c == "SELL":
        return _DOWN_COLOR
    return _NEUTRAL_COLOR


def _classification_fill(classification: Optional[str]) -> Tuple[float, float, float, float]:
    """Translucent RGBA fill color matching the classification accent."""
    if not classification:
        return _NEUTRAL_FILL
    c = classification.strip().upper()
    if c == "BUY":
        return _BUY_FILL
    if c == "SELL":
        return _SELL_FILL
    return _NEUTRAL_FILL


def _preferred_font_family() -> str:
    """Pick a modern sans-serif if one is installed, else DejaVu Sans."""
    try:
        import matplotlib.font_manager as fm
        available = {f.name for f in fm.fontManager.ttflist}
    except Exception:
        return "DejaVu Sans"
    for candidate in (
        "SF Pro Display", "SF Pro Text", "Inter",
        "Helvetica Neue", "Arial",
    ):
        if candidate in available:
            return candidate
    return "DejaVu Sans"


def _fetch_ohlc(coingecko_id: str) -> Optional[List[List[float]]]:
    """Fetch 24h OHLC bars from CoinGecko. Returns None on any error."""
    url = _COINGECKO_OHLC_URL.format(id=coingecko_id)
    try:
        resp = httpx.get(
            url,
            params={"vs_currency": "usd", "days": 1},
            timeout=_FETCH_TIMEOUT,
        )
    except httpx.HTTPError as e:
        logger.warning("CoinGecko OHLC request failed for %s: %s", coingecko_id, e)
        return None
    except Exception as e:
        logger.warning("CoinGecko OHLC unexpected error for %s: %s", coingecko_id, e)
        return None

    if resp.status_code != 200:
        logger.warning(
            "CoinGecko OHLC %s returned HTTP %d", coingecko_id, resp.status_code,
        )
        return None

    try:
        data = resp.json()
    except ValueError:
        logger.warning("CoinGecko OHLC %s returned non-JSON body", coingecko_id)
        return None

    if not isinstance(data, list) or not data:
        return None
    return data


# ---------------------------------------------------------------------------
# Title + watermark composition
# ---------------------------------------------------------------------------

def _draw_sonar_title(fig, token: str, font_family: str) -> None:
    """Draw a three-run title in the top-left of the figure:

        [TOKEN]       bold, teal (brand)
        [ • 24h]      regular, slate (muted)
        [ • WHALE ALERT]  semibold, near-white, letter-spaced
    """
    from matplotlib.offsetbox import (
        AnchoredOffsetbox, HPacker, TextArea,
    )

    parts = [
        TextArea(
            token,
            textprops=dict(
                color=_BRAND_TEAL, weight=700, size=14, family=font_family,
            ),
        ),
        TextArea(
            "  \u2022  24h",
            textprops=dict(
                color=_TITLE_MUTED, weight=400, size=12, family=font_family,
            ),
        ),
        TextArea(
            "  \u2022  W H A L E   A L E R T",
            textprops=dict(
                color=_TITLE_COLOR, weight=600, size=11, family=font_family,
            ),
        ),
    ]
    box = HPacker(children=parts, align="baseline", pad=0, sep=0)
    anchored = AnchoredOffsetbox(
        loc="upper left",
        child=box,
        pad=0.0,
        borderpad=0.0,
        frameon=False,
        bbox_to_anchor=(0.012, 0.965),
        bbox_transform=fig.transFigure,
    )
    fig.add_artist(anchored)


def _draw_sonar_watermark(fig, font_family: str) -> None:
    """Draw the Sonar watermark in the bottom-right of the figure."""
    from matplotlib.offsetbox import (
        AnchoredOffsetbox, HPacker, TextArea,
    )
    parts = [
        TextArea(
            "SONAR",
            textprops=dict(
                color=_BRAND_TEAL, weight=700, size=8, family=font_family,
            ),
        ),
        TextArea(
            "  \u00B7  sonartracker.io",
            textprops=dict(
                color=_WATERMARK_MUTED, weight=400, size=8, family=font_family,
            ),
        ),
    ]
    box = HPacker(children=parts, align="baseline", pad=0, sep=0)
    anchored = AnchoredOffsetbox(
        loc="lower right",
        child=box,
        pad=0.0,
        borderpad=0.0,
        frameon=False,
        bbox_to_anchor=(0.988, 0.015),
        bbox_transform=fig.transFigure,
    )
    fig.add_artist(anchored)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def generate_whale_chart(tx: dict) -> Optional[str]:
    """Render a 24h candle chart annotated with a whale transaction.

    Returns the absolute path to a PNG file on success, or ``None`` if
    rendering is not possible for any reason. Never raises.

    Callers are responsible for removing the returned file after use.
    """
    try:
        token = (tx.get("token_symbol") or "").upper()
        coingecko_id = COINGECKO_IDS.get(token)
        if not coingecko_id:
            return None

        ohlc = _fetch_ohlc(coingecko_id)
        if not ohlc or len(ohlc) < MIN_OHLC_POINTS:
            return None

        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.patheffects as pe
        import matplotlib.pyplot as plt
        import mplfinance as mpf
        import pandas as pd
        from matplotlib.ticker import FuncFormatter

        font_family = _preferred_font_family()

        df = pd.DataFrame(
            ohlc, columns=["timestamp_ms", "Open", "High", "Low", "Close"],
        )
        df["Date"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
        df = df.set_index("Date").drop(columns=["timestamp_ms"]).sort_index()

        df = (
            df.resample(RESAMPLE_RULE)
            .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"})
            .dropna()
        )
        if len(df) < MIN_OHLC_POINTS:
            return None

        classification = (tx.get("classification") or "").strip().upper()
        vline_color = _classification_color(classification)
        vline_fill = _classification_fill(classification)

        tx_ts_raw = tx.get("timestamp")
        tx_ts = None
        if tx_ts_raw:
            try:
                tx_ts = pd.to_datetime(tx_ts_raw, utc=True)
            except Exception:
                tx_ts = None

        market_colors = mpf.make_marketcolors(
            up=_UP_COLOR,
            down=_DOWN_COLOR,
            wick={"up": _UP_COLOR, "down": _DOWN_COLOR},
            edge={"up": _UP_COLOR, "down": _DOWN_COLOR},
        )
        style = mpf.make_mpf_style(
            base_mpf_style="nightclouds",
            marketcolors=market_colors,
            facecolor=_BG_COLOR,
            edgecolor=_GRID_COLOR,
            gridcolor=_GRID_COLOR,
            gridstyle=":",
            figcolor=_BG_COLOR,
            rc={
                "font.family": font_family,
                "axes.labelcolor": _AXIS_COLOR,
                "xtick.color": _AXIS_COLOR,
                "ytick.color": _AXIS_COLOR,
                "axes.edgecolor": _GRID_COLOR,
                "text.color": _AXIS_COLOR,
            },
        )

        fig, axes = mpf.plot(
            df,
            type="candle",
            style=style,
            figsize=(10, 5),
            returnfig=True,
            axisoff=False,
            xrotation=0,
            datetime_format="%H:%M",
            ylabel="",
            tight_layout=False,
            show_nontrading=False,
            update_width_config=dict(candle_linewidth=0.9, candle_width=0.65),
        )
        ax = axes[0]

        # Reserve vertical headroom so the title + callout sit in their own
        # band above the plot. Also pull the plot in from the left to give
        # the title a clean gutter against the y-axis tick labels.
        fig.subplots_adjust(
            top=0.85, bottom=0.12, left=0.08, right=0.985,
        )

        # ---- Spines: hide top/right/left entirely, keep a hair-thin bottom.
        for side in ("top", "right", "left"):
            ax.spines[side].set_visible(False)
        ax.spines["bottom"].set_color(_GRID_COLOR)
        ax.spines["bottom"].set_linewidth(0.5)

        # Tick marks should disappear visually; the grid carries the rhythm.
        ax.tick_params(
            axis="both", colors=_AXIS_COLOR, which="both",
            length=0, labelsize=9,
        )

        ax.yaxis.set_major_formatter(
            FuncFormatter(lambda v, _pos: _compact_usd(v)),
        )

        # ---- Sonar-brand title (drawn on the figure, not the axes).
        _draw_sonar_title(fig, token, font_family)

        # ---- Vertical "whale" line + annotation callout.
        x_pos: Optional[int] = None
        if tx_ts is not None:
            try:
                idx = df.index.get_indexer([tx_ts], method="nearest")[0]
                if 0 <= idx < len(df):
                    x_pos = int(idx)
            except Exception:
                x_pos = None

        if x_pos is not None:
            vline = ax.axvline(
                x=x_pos,
                color=vline_color,
                linestyle="--",
                linewidth=1.5,
                alpha=0.8,
                zorder=5,
            )
            # Soft glow: wider, translucent stroke behind the crisp line.
            try:
                vline.set_path_effects([
                    pe.Stroke(linewidth=5, foreground=vline_color, alpha=0.22),
                    pe.Normal(),
                ])
            except Exception:
                pass

            usd_value = float(tx.get("usd_value", 0) or 0)
            label = f"WHALE  {_compact_usd(usd_value)}"
            if classification:
                label += f"  {classification}"

            y_high = float(df["High"].max())
            y_low = float(df["Low"].min())
            y_range = max(y_high - y_low, 1e-9)
            y_anchor = y_high - y_range * 0.01
            y_text = y_high + y_range * 0.14

            # Keep the callout inside the plot horizontally so it doesn't
            # get clipped when tx_ts falls near the left/right edge.
            n = len(df)
            text_align = "center"
            text_x = x_pos
            if x_pos < n * 0.1:
                text_align = "left"
                text_x = x_pos + max(1, int(n * 0.02))
            elif x_pos > n * 0.9:
                text_align = "right"
                text_x = x_pos - max(1, int(n * 0.02))

            ax.annotate(
                label,
                xy=(x_pos, y_anchor),
                xytext=(text_x, y_text),
                ha=text_align,
                va="bottom",
                fontsize=10,
                fontweight=600,
                color=_TITLE_COLOR,
                family=font_family,
                bbox=dict(
                    boxstyle="round,pad=0.5",
                    facecolor=vline_fill,
                    edgecolor=vline_color,
                    linewidth=1.5,
                ),
                arrowprops=dict(
                    arrowstyle="->",
                    color=vline_color,
                    lw=1.8,
                    alpha=0.9,
                ),
                zorder=6,
            )

        # ---- Sonar watermark (bottom-right of figure).
        _draw_sonar_watermark(fig, font_family)

        fd, path = tempfile.mkstemp(suffix=".png", prefix="sonar_chart_")
        os.close(fd)
        fig.savefig(
            path,
            dpi=100,
            bbox_inches="tight",
            facecolor=_BG_COLOR,
        )
        plt.close(fig)
        return path
    except Exception as e:
        logger.warning("Chart generation failed: %s", e)
        return None
