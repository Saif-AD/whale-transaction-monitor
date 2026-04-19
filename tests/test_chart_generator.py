"""Tests for whale_poster.chart_generator.

Network access is always mocked. We only exercise the rendering code path
with stub OHLC data.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import httpx
import pytest

from whale_poster import chart_generator
from whale_poster.chart_generator import (
    COINGECKO_IDS,
    _classification_color,
    _compact_usd,
    generate_whale_chart,
)


def _stub_ohlc_response(n_bars: int = 48) -> MagicMock:
    """Build a fake httpx.Response with ``n_bars`` 30-min OHLC bars
    centered on 2026-04-01T12:00:00Z (the tx timestamp used in tests).
    """
    import datetime as _dt

    tx_time = _dt.datetime(2026, 4, 1, 12, 0, 0, tzinfo=_dt.timezone.utc)
    start = tx_time - _dt.timedelta(minutes=30 * (n_bars // 2))
    base_ts_ms = int(start.timestamp() * 1000)

    bars = []
    price = 3500.0
    for i in range(n_bars):
        ts = base_ts_ms + i * 1_800_000
        o = price
        # Alternate up/down with a small drift so the price series is
        # non-degenerate and spread over a real range.
        direction = 1 if i % 2 == 0 else -1
        c = price + direction * 7.5 + (i - n_bars / 2) * 0.4
        h = max(o, c) + 4
        l = min(o, c) - 4
        bars.append([ts, o, h, l, c])
        price = c
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 200
    resp.json.return_value = bars
    return resp


def _base_tx(**overrides) -> dict:
    tx = {
        "token_symbol": "ETH",
        "usd_value": 4_200_000,
        "timestamp": "2026-04-01T12:00:00+00:00",
        "classification": "BUY",
        "transaction_hash": "0xabc",
        "blockchain": "ethereum",
    }
    tx.update(overrides)
    return tx


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

class TestCompactUsd:

    def test_thousands(self):
        assert _compact_usd(3_400) == "$3.40K"

    def test_thousands_distinct_precision(self):
        """ETH-range values must render with enough precision to be distinct."""
        assert _compact_usd(3_450) != _compact_usd(3_500)

    def test_tens_of_thousands(self):
        assert _compact_usd(65_000) == "$65.0K"

    def test_hundreds_of_thousands(self):
        assert _compact_usd(650_000) == "$650K"

    def test_millions(self):
        assert _compact_usd(4_200_000) == "$4.20M"

    def test_billions(self):
        assert _compact_usd(3_200_000_000) == "$3.20B"

    def test_sub_thousand(self):
        assert _compact_usd(42.50) == "$42.50"

    def test_zero(self):
        assert _compact_usd(0) == "$0"


class TestClassificationColor:
    """Color mapping uses the Sonar brand palette (teal / rose / slate)."""

    def test_buy_is_brand_teal(self):
        assert _classification_color("BUY") == "#14b8a6"

    def test_sell_is_brand_rose(self):
        assert _classification_color("SELL") == "#f43f5e"

    def test_lowercase_buy(self):
        assert _classification_color("buy") == "#14b8a6"

    def test_empty_is_neutral_slate(self):
        assert _classification_color("") == "#64748b"

    def test_other_is_neutral_slate(self):
        assert _classification_color("TRANSFER") == "#64748b"


# ---------------------------------------------------------------------------
# generate_whale_chart — happy path + failure modes
# ---------------------------------------------------------------------------

class TestGenerateWhaleChart:

    def test_unknown_token_returns_none(self):
        """Tokens not in the CoinGecko mapping must fall back to text-only."""
        assert "FOO" not in COINGECKO_IDS
        assert generate_whale_chart(_base_tx(token_symbol="FOO")) is None

    def test_missing_token_returns_none(self):
        assert generate_whale_chart(_base_tx(token_symbol="")) is None

    def test_coingecko_404_returns_none(self):
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 404
        resp.text = "not found"
        with patch.object(chart_generator.httpx, "get", return_value=resp):
            assert generate_whale_chart(_base_tx()) is None

    def test_coingecko_timeout_returns_none(self):
        def raise_timeout(*args, **kwargs):
            raise httpx.ConnectTimeout("slow")

        with patch.object(chart_generator.httpx, "get", side_effect=raise_timeout):
            assert generate_whale_chart(_base_tx()) is None

    def test_generic_network_error_returns_none(self):
        with patch.object(
            chart_generator.httpx,
            "get",
            side_effect=httpx.ConnectError("no dns"),
        ):
            assert generate_whale_chart(_base_tx()) is None

    def test_empty_ohlc_returns_none(self):
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.json.return_value = []
        with patch.object(chart_generator.httpx, "get", return_value=resp):
            assert generate_whale_chart(_base_tx()) is None

    def test_too_few_data_points_returns_none(self):
        """Fewer than 10 OHLC points is not enough for a meaningful chart."""
        resp = _stub_ohlc_response(n_bars=5)
        with patch.object(chart_generator.httpx, "get", return_value=resp):
            assert generate_whale_chart(_base_tx()) is None

    def test_returns_valid_png(self, tmp_path):
        """Happy path: writes a real PNG we can detect by the magic signature."""
        with patch.object(
            chart_generator.httpx, "get", return_value=_stub_ohlc_response(),
        ):
            path = generate_whale_chart(_base_tx())

        assert path is not None
        try:
            assert os.path.exists(path)
            with open(path, "rb") as fh:
                header = fh.read(8)
            assert header[:4] == b"\x89PNG"
            assert os.path.getsize(path) > 1_000
        finally:
            if path and os.path.exists(path):
                os.remove(path)

    def test_calls_coingecko_with_expected_params(self):
        """Verify the outgoing CoinGecko call shape."""
        captured = {}

        def fake_get(url, params=None, timeout=None, **kwargs):
            captured["url"] = url
            captured["params"] = params
            captured["timeout"] = timeout
            return _stub_ohlc_response()

        with patch.object(chart_generator.httpx, "get", side_effect=fake_get):
            path = generate_whale_chart(_base_tx(token_symbol="BTC"))
            if path:
                os.remove(path)

        assert "coins/bitcoin/ohlc" in captured["url"]
        assert captured["params"]["vs_currency"] == "usd"
        assert captured["params"]["days"] == 1
        assert captured["timeout"] == 5.0


# ---------------------------------------------------------------------------
# Vertical line color behavior
#
# We intercept ``Axes.axvline`` so we can observe the color argument
# without needing to parse the rendered PNG.
# ---------------------------------------------------------------------------

class TestVerticalLineColor:

    def _capture_vline_colors(self, classification: str) -> list:
        """Run generate_whale_chart and return the list of axvline colors."""
        from matplotlib.axes import Axes

        captured: list = []
        original = Axes.axvline

        def spy(self, *args, **kwargs):
            if "color" in kwargs:
                captured.append(kwargs["color"])
            return original(self, *args, **kwargs)

        with patch.object(
            chart_generator.httpx, "get", return_value=_stub_ohlc_response(),
        ):
            with patch.object(Axes, "axvline", spy):
                path = generate_whale_chart(
                    _base_tx(classification=classification),
                )
        if path and os.path.exists(path):
            os.remove(path)
        return captured

    def test_buy_uses_brand_teal_vline(self):
        colors = self._capture_vline_colors("BUY")
        assert "#14b8a6" in colors

    def test_sell_uses_brand_rose_vline(self):
        colors = self._capture_vline_colors("SELL")
        assert "#f43f5e" in colors

    def test_unknown_classification_uses_neutral_vline(self):
        colors = self._capture_vline_colors("TRANSFER")
        assert "#64748b" in colors


# ---------------------------------------------------------------------------
# Never-raise contract
# ---------------------------------------------------------------------------

class TestNeverRaises:

    def test_render_exception_returns_none(self):
        """Even if matplotlib blows up, the caller gets None, not an exception."""
        with patch.object(
            chart_generator.httpx, "get", return_value=_stub_ohlc_response(),
        ), patch("mplfinance.plot", side_effect=RuntimeError("boom")):
            result = generate_whale_chart(_base_tx())
        assert result is None

    def test_bad_json_returns_none(self):
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.json.side_effect = ValueError("not json")
        with patch.object(chart_generator.httpx, "get", return_value=resp):
            assert generate_whale_chart(_base_tx()) is None


# ---------------------------------------------------------------------------
# Visual sanity: candles must spread across the plot, not collapse to a line.
# ---------------------------------------------------------------------------

class TestChartIsNotFlat:
    """Regression guard for the "all candles squashed into one vertical line"
    failure mode. The test inspects the rendered PNG: the candle area must
    contain colored (red/green) pixels spanning many distinct columns, and
    the center of the chart must have meaningful pixel variance.
    """

    def test_candle_region_has_spread_and_variance(self):
        try:
            import numpy as np
            from PIL import Image
        except ImportError:
            pytest.skip("numpy / Pillow not available")

        with patch.object(
            chart_generator.httpx, "get", return_value=_stub_ohlc_response(),
        ):
            path = generate_whale_chart(_base_tx(classification="SELL"))

        assert path is not None, "chart generation returned None"
        try:
            img = np.asarray(Image.open(path).convert("RGB"))
            h, w, _ = img.shape

            # Crop the interior plot area (roughly, inside the axes).
            top = int(h * 0.15)
            bottom = int(h * 0.85)
            left = int(w * 0.10)
            right = int(w * 0.90)
            center = img[top:bottom, left:right]

            # Colorful pixels = any pixel where R/G/B channels diverge
            # significantly. This picks up red/green candles but ignores
            # the near-grayscale background + grid.
            hi = center.max(axis=2).astype(int)
            lo = center.min(axis=2).astype(int)
            colorful = (hi - lo) > 40

            cols_with_candles = int(colorful.any(axis=0).sum())
            total_cols = center.shape[1]

            assert cols_with_candles >= 20, (
                f"Only {cols_with_candles}/{total_cols} columns contain "
                f"candle pixels — candles are compressed to a narrow strip."
            )
            assert cols_with_candles / total_cols >= 0.25, (
                f"Candle pixels span only {cols_with_candles / total_cols:.0%} "
                f"of the plot width (< 25%) — chart looks collapsed."
            )

            # Central variance must be non-trivial: a flat line would make
            # nearly every row identical.
            assert center.std() > 10, (
                f"Chart center variance={center.std():.1f} is suspiciously low."
            )
        finally:
            if path and os.path.exists(path):
                os.remove(path)

    def test_whale_marker_label_is_text_not_emoji(self):
        """The annotation must spell 'WHALE' — DejaVu Sans can't render the
        whale emoji, which was previously emitting a UserWarning and a tofu
        box glyph."""
        import matplotlib
        matplotlib.use("Agg")
        import warnings

        with patch.object(
            chart_generator.httpx, "get", return_value=_stub_ohlc_response(),
        ), warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            path = generate_whale_chart(_base_tx())

        try:
            whale_glyph_warnings = [
                w for w in caught
                if "WHALE" in str(w.message) or "128011" in str(w.message)
            ]
            assert not whale_glyph_warnings, (
                f"Chart still tries to render the whale emoji: "
                f"{[str(w.message) for w in whale_glyph_warnings]}"
            )
        finally:
            if path and os.path.exists(path):
                os.remove(path)
