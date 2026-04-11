"""Tests for shared.interpreter — Grok-powered whale tx interpretation."""

import types
from unittest.mock import patch, MagicMock

import pytest

from shared.config import (
    INTERPRETER_USD_THRESHOLD,
    INTERPRETER_LABELED_USD_THRESHOLD,
    INTERPRETER_UNLABELED_USD_THRESHOLD,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_tx(**overrides):
    """Build a minimal whale tx dict for testing."""
    base = {
        "transaction_hash": "0xabc123def456",
        "token_symbol": "ETH",
        "usd_value": 2_000_000,
        "classification": "BUY",
        "blockchain": "ethereum",
        "from_label": "Binance",
        "to_label": "Unknown Whale",
        "counterparty_type": "CEX",
        "is_cex_transaction": True,
    }
    base.update(overrides)
    return base


def _mock_response(text: str):
    """Build a fake OpenAI ChatCompletion response."""
    choice = MagicMock()
    choice.message.content = text
    resp = MagicMock()
    resp.choices = [choice]
    return resp


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestGenerateInterpretation:
    """Core interpreter function tests."""

    @patch("shared.interpreter._get_client")
    @patch("shared.interpreter._load_system_prompt", return_value="You are ORCA.")
    def test_success_returns_string(self, _mock_prompt, mock_client_fn):
        from shared.interpreter import generate_interpretation

        client = MagicMock()
        client.chat.completions.create.return_value = _mock_response(
            "Jump rotating into cold storage — long-term conviction play."
        )
        mock_client_fn.return_value = client

        result = generate_interpretation(_make_tx())
        assert isinstance(result, str)
        assert "Jump rotating" in result
        assert "\n" not in result
        client.chat.completions.create.assert_called_once()

    @patch("shared.interpreter._get_client")
    @patch("shared.interpreter._load_system_prompt", return_value="You are ORCA.")
    def test_timeout_raises_after_retry(self, _mock_prompt, mock_client_fn):
        from openai import APITimeoutError
        from shared.interpreter import generate_interpretation

        client = MagicMock()
        client.chat.completions.create.side_effect = APITimeoutError(request=MagicMock())
        mock_client_fn.return_value = client

        with pytest.raises(APITimeoutError):
            generate_interpretation(_make_tx())

        assert client.chat.completions.create.call_count == 2

    @patch("shared.interpreter._get_client")
    @patch("shared.interpreter._load_system_prompt", return_value="You are ORCA.")
    def test_4xx_does_not_retry(self, _mock_prompt, mock_client_fn):
        from openai import APIStatusError
        from shared.interpreter import generate_interpretation

        client = MagicMock()
        err_response = MagicMock()
        err_response.status_code = 401
        err_response.headers = {}
        client.chat.completions.create.side_effect = APIStatusError(
            message="Unauthorized",
            response=err_response,
            body=None,
        )
        mock_client_fn.return_value = client

        with pytest.raises(APIStatusError):
            generate_interpretation(_make_tx())

        assert client.chat.completions.create.call_count == 1

    @patch("shared.interpreter._get_client")
    @patch("shared.interpreter._load_system_prompt", return_value="You are ORCA.")
    def test_5xx_retries_once(self, _mock_prompt, mock_client_fn):
        from openai import APIStatusError
        from shared.interpreter import generate_interpretation

        client = MagicMock()
        err_response = MagicMock()
        err_response.status_code = 503
        err_response.headers = {}

        client.chat.completions.create.side_effect = [
            APIStatusError(message="Service Unavailable", response=err_response, body=None),
            _mock_response("Whale accumulating — smart money signal."),
        ]
        mock_client_fn.return_value = client

        result = generate_interpretation(_make_tx())
        assert "smart money" in result
        assert client.chat.completions.create.call_count == 2

    @patch("shared.interpreter._get_client")
    @patch("shared.interpreter._load_system_prompt", return_value="You are ORCA.")
    def test_long_output_truncated(self, _mock_prompt, mock_client_fn):
        from shared.interpreter import generate_interpretation, _MAX_OUTPUT_CHARS

        client = MagicMock()
        long_text = "A" * 300
        client.chat.completions.create.return_value = _mock_response(long_text)
        mock_client_fn.return_value = client

        result = generate_interpretation(_make_tx())
        assert len(result) <= _MAX_OUTPUT_CHARS
        assert result.endswith("\u2026")

    @patch("shared.interpreter._get_client")
    @patch("shared.interpreter._load_system_prompt", return_value="You are ORCA.")
    def test_newlines_stripped(self, _mock_prompt, mock_client_fn):
        from shared.interpreter import generate_interpretation

        client = MagicMock()
        client.chat.completions.create.return_value = _mock_response(
            "Line one.\nLine two.\nLine three."
        )
        mock_client_fn.return_value = client

        result = generate_interpretation(_make_tx())
        assert "\n" not in result
        assert "Line one. Line two. Line three." == result

    @patch("shared.interpreter._get_client")
    @patch("shared.interpreter._load_system_prompt", return_value="You are ORCA.")
    def test_empty_labels_still_calls_grok(self, _mock_prompt, mock_client_fn):
        from shared.interpreter import generate_interpretation

        client = MagicMock()
        client.chat.completions.create.return_value = _mock_response(
            "Unknown whale moving size — watch this one."
        )
        mock_client_fn.return_value = client

        tx = _make_tx(from_label="", to_label="")
        result = generate_interpretation(tx)
        assert isinstance(result, str)
        client.chat.completions.create.assert_called_once()

        user_msg = client.chat.completions.create.call_args[1]["messages"][1]["content"]
        assert "unknown wallet" in user_msg.lower()


class TestPromptLoading:
    """Tests for system prompt file loading."""

    @patch("shared.interpreter._PROMPT_PATH")
    def test_missing_prompt_file_raises(self, mock_path):
        import shared.interpreter as mod

        mod._system_prompt = None
        mock_path.exists.return_value = False

        with pytest.raises(FileNotFoundError, match="whale_interpretation.txt"):
            mod._load_system_prompt()

    def test_real_prompt_file_loads(self):
        """Smoke test: the actual prompt file in the repo loads correctly."""
        import shared.interpreter as mod

        mod._system_prompt = None
        prompt = mod._load_system_prompt()
        assert len(prompt) > 50
        assert "ORCA" in prompt


class TestBuildUserMessage:
    """Tests for the user message assembly."""

    def test_message_contains_tx_fields(self):
        from shared.interpreter import _build_user_message

        tx = _make_tx()
        msg = _build_user_message(tx)
        assert "ETH" in msg
        assert "BUY" in msg
        assert "Binance" in msg
        assert "Unknown Whale" in msg
        assert "CEX transaction: yes" in msg

    def test_message_with_no_labels(self):
        from shared.interpreter import _build_user_message

        tx = _make_tx(from_label="", to_label="")
        msg = _build_user_message(tx)
        assert "unknown wallet" in msg.lower()
        assert "Binance" not in msg


class TestConfigIntegration:
    """Tests for config-driven gating (threshold check lives in callers,
    but we verify the config values load correctly)."""

    def test_threshold_default(self):
        assert INTERPRETER_USD_THRESHOLD == 500_000

    def test_dual_threshold_defaults(self):
        assert INTERPRETER_LABELED_USD_THRESHOLD == 500_000
        assert INTERPRETER_UNLABELED_USD_THRESHOLD == 2_000_000

    def test_below_threshold_caller_skips(self):
        """Callers should gate on usd_value >= threshold.
        This test documents the expected pattern."""
        tx = _make_tx(usd_value=100_000)
        assert tx["usd_value"] < INTERPRETER_USD_THRESHOLD

    def test_labeled_tx_uses_lower_threshold(self):
        """When either label is non-empty, use the labeled threshold."""
        tx = _make_tx(usd_value=600_000, from_label="Binance", to_label="")
        has_labels = bool(tx.get("from_label") or tx.get("to_label"))
        threshold = INTERPRETER_LABELED_USD_THRESHOLD if has_labels else INTERPRETER_UNLABELED_USD_THRESHOLD
        assert threshold == 500_000
        assert tx["usd_value"] >= threshold

    def test_unlabeled_tx_uses_higher_threshold(self):
        """When both labels are empty, use the unlabeled threshold."""
        tx = _make_tx(usd_value=600_000, from_label="", to_label="")
        has_labels = bool(tx.get("from_label") or tx.get("to_label"))
        threshold = INTERPRETER_LABELED_USD_THRESHOLD if has_labels else INTERPRETER_UNLABELED_USD_THRESHOLD
        assert threshold == 2_000_000
        assert tx["usd_value"] < threshold

    def test_unlabeled_tx_above_threshold_passes(self):
        """Unlabeled tx above $2M should be interpreted."""
        tx = _make_tx(usd_value=3_000_000, from_label="", to_label="")
        has_labels = bool(tx.get("from_label") or tx.get("to_label"))
        threshold = INTERPRETER_LABELED_USD_THRESHOLD if has_labels else INTERPRETER_UNLABELED_USD_THRESHOLD
        assert threshold == 2_000_000
        assert tx["usd_value"] >= threshold


class TestClientConstruction:
    """Tests for the OpenAI client builder."""

    @patch("shared.interpreter.XAI_API_KEY", "")
    def test_missing_api_key_raises(self):
        from shared.interpreter import _get_client

        with pytest.raises(RuntimeError, match="XAI_API_KEY"):
            _get_client()

    @patch("shared.interpreter.XAI_API_KEY", "test-key-123")
    def test_valid_key_creates_client(self):
        from shared.interpreter import _get_client

        client = _get_client()
        assert client is not None
