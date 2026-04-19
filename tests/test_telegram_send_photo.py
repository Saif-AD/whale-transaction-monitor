"""Tests for whale_poster.telegram.send_photo."""

from __future__ import annotations

import io
import logging
from unittest.mock import MagicMock, patch

import httpx
import pytest

from whale_poster import telegram as tg_mod
from whale_poster.telegram import (
    TELEGRAM_CAPTION_MAX,
    _truncate_caption,
    send_photo,
)


# ---------------------------------------------------------------------------
# _truncate_caption helper
# ---------------------------------------------------------------------------

class TestTruncateCaption:

    def test_under_limit_unchanged(self):
        assert _truncate_caption("hello") == "hello"

    def test_exact_limit_unchanged(self):
        caption = "x" * TELEGRAM_CAPTION_MAX
        assert _truncate_caption(caption) == caption

    def test_over_limit_truncates_with_ellipsis(self):
        caption = "y" * (TELEGRAM_CAPTION_MAX + 500)
        out = _truncate_caption(caption)
        assert len(out) == TELEGRAM_CAPTION_MAX
        assert out.endswith("...")
        assert out[: TELEGRAM_CAPTION_MAX - 3] == "y" * (TELEGRAM_CAPTION_MAX - 3)

    def test_empty_string(self):
        assert _truncate_caption("") == ""

    def test_none_returns_empty(self):
        assert _truncate_caption(None) == ""


# ---------------------------------------------------------------------------
# send_photo
# ---------------------------------------------------------------------------

@pytest.fixture()
def fake_png(tmp_path):
    p = tmp_path / "chart.png"
    p.write_bytes(b"\x89PNG\r\n\x1a\nfake-image-body")
    return str(p)


def _ok_response() -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 200
    resp.text = '{"ok": true}'
    return resp


class TestSendPhotoSuccess:

    def test_posts_to_send_photo_endpoint(self, fake_png):
        captured = {}

        def fake_post(url, data=None, files=None, timeout=None, **kwargs):
            captured["url"] = url
            captured["data"] = data
            captured["files"] = files
            captured["timeout"] = timeout
            return _ok_response()

        with patch.object(tg_mod.httpx, "post", side_effect=fake_post):
            ok = send_photo(fake_png, "hello", chat_id="-100", token="T123")

        assert ok is True
        assert captured["url"] == "https://api.telegram.org/botT123/sendPhoto"
        assert captured["data"]["chat_id"] == "-100"
        assert captured["data"]["caption"] == "hello"
        assert captured["data"]["parse_mode"] == "HTML"
        # multipart file field present
        assert "photo" in captured["files"]
        assert captured["timeout"] == 15.0

    def test_multipart_file_body_is_read(self, fake_png):
        """Whatever we pass to httpx as the photo field must be a real file handle."""
        captured = {}

        def fake_post(url, data=None, files=None, timeout=None, **kwargs):
            photo = files["photo"]
            fh = photo[1] if isinstance(photo, tuple) else photo
            captured["body"] = fh.read()
            return _ok_response()

        with patch.object(tg_mod.httpx, "post", side_effect=fake_post):
            ok = send_photo(fake_png, "cap", chat_id="-1", token="tok")

        assert ok is True
        assert captured["body"].startswith(b"\x89PNG")


class TestSendPhotoCaptionTruncation:

    def test_caption_truncated_to_1024(self, fake_png):
        long_caption = "a" * 1500
        captured = {}

        def fake_post(url, data=None, files=None, timeout=None, **kwargs):
            captured["caption"] = data["caption"]
            return _ok_response()

        with patch.object(tg_mod.httpx, "post", side_effect=fake_post):
            ok = send_photo(fake_png, long_caption, chat_id="-1", token="tok")

        assert ok is True
        assert len(captured["caption"]) == 1024
        assert captured["caption"].endswith("...")

    def test_short_caption_unchanged(self, fake_png):
        captured = {}

        def fake_post(url, data=None, files=None, timeout=None, **kwargs):
            captured["caption"] = data["caption"]
            return _ok_response()

        with patch.object(tg_mod.httpx, "post", side_effect=fake_post):
            send_photo(fake_png, "compact", chat_id="-1", token="tok")

        assert captured["caption"] == "compact"


class TestSendPhotoFailure:

    def test_401_returns_false_and_logs_warning(self, fake_png, caplog):
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 401
        resp.text = '{"ok":false,"error_code":401,"description":"Unauthorized"}'

        with patch.object(tg_mod.httpx, "post", return_value=resp):
            with caplog.at_level(logging.WARNING, logger="whale_poster.telegram"):
                ok = send_photo(fake_png, "cap", chat_id="-1", token="bad")

        assert ok is False
        assert any("401" in rec.message for rec in caplog.records)

    def test_500_returns_false(self, fake_png):
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 500
        resp.text = "internal error"
        with patch.object(tg_mod.httpx, "post", return_value=resp):
            assert send_photo(fake_png, "cap", chat_id="-1", token="tok") is False

    def test_network_error_returns_false(self, fake_png):
        with patch.object(
            tg_mod.httpx,
            "post",
            side_effect=httpx.ConnectError("boom"),
        ):
            assert send_photo(fake_png, "cap", chat_id="-1", token="tok") is False

    def test_missing_photo_file_returns_false(self):
        with patch.object(tg_mod.httpx, "post") as post:
            ok = send_photo(
                "/tmp/definitely-does-not-exist-sonar-chart.png",
                "cap", chat_id="-1", token="tok",
            )
        assert ok is False
        post.assert_not_called()

    def test_missing_token_and_chat_id_returns_false(self, fake_png, monkeypatch):
        """Explicit empty creds short-circuit without making an HTTP call."""
        monkeypatch.setattr("shared.config.TELEGRAM_BOT_TOKEN", "", raising=False)
        monkeypatch.setattr("shared.config.TELEGRAM_CHANNEL_ID", "", raising=False)
        with patch.object(tg_mod.httpx, "post") as post:
            ok = send_photo(fake_png, "cap")
        assert ok is False
        post.assert_not_called()
