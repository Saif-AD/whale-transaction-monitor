"""Tests for Arkham credit budget guard."""

from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest
import httpx

from arkham_backfill.arkham_client import (
    ArkhamClient,
    CreditBudgetExhausted,
    ARKHAM_BASE_URL,
)
from arkham_backfill.constants import CREDIT_FLOOR


def _mock_response(
    status_code: int = 200,
    json_data: dict | None = None,
    credits_remaining: int | None = None,
) -> httpx.Response:
    headers = {}
    if credits_remaining is not None:
        headers["x-intel-datapoints-remaining"] = str(credits_remaining)

    resp = httpx.Response(
        status_code=status_code,
        json=json_data or {},
        headers=headers,
        request=httpx.Request("GET", f"{ARKHAM_BASE_URL}/test"),
    )
    return resp


class TestCreditGuard:

    def test_raises_when_below_floor(self):
        client = ArkhamClient(api_key="test", credit_floor=500)
        resp = _mock_response(credits_remaining=499)
        with pytest.raises(CreditBudgetExhausted) as exc_info:
            client._check_credits(resp)
        assert exc_info.value.remaining == 499

    def test_passes_when_at_floor(self):
        client = ArkhamClient(api_key="test", credit_floor=500)
        resp = _mock_response(credits_remaining=500)
        client._check_credits(resp)  # should not raise

    def test_passes_when_above_floor(self):
        client = ArkhamClient(api_key="test", credit_floor=500)
        resp = _mock_response(credits_remaining=9999)
        client._check_credits(resp)  # should not raise

    def test_stores_last_credits(self):
        client = ArkhamClient(api_key="test", credit_floor=100)
        resp = _mock_response(credits_remaining=750)
        client._check_credits(resp)
        assert client.last_credits_remaining == 750

    def test_no_header_does_not_raise(self):
        client = ArkhamClient(api_key="test", credit_floor=500)
        resp = _mock_response(credits_remaining=None)
        client._check_credits(resp)  # should not raise
        assert client.last_credits_remaining is None

    def test_non_numeric_header_ignored(self):
        client = ArkhamClient(api_key="test", credit_floor=500)
        resp = httpx.Response(
            status_code=200,
            json={},
            headers={"x-intel-datapoints-remaining": "not-a-number"},
            request=httpx.Request("GET", f"{ARKHAM_BASE_URL}/test"),
        )
        client._check_credits(resp)  # should not raise

    def test_credit_floor_default_is_500(self):
        assert CREDIT_FLOOR == 500

    @patch.object(ArkhamClient, "_wait_for_rate_limit")
    def test_credit_guard_fires_during_request(self, mock_wait):
        """The credit check fires on a normal successful request."""
        client = ArkhamClient(api_key="test", credit_floor=500)

        low_credit_resp = _mock_response(
            status_code=200,
            json_data={"name": "test"},
            credits_remaining=100,
        )
        client._http = MagicMock()
        client._http.request.return_value = low_credit_resp

        with pytest.raises(CreditBudgetExhausted):
            client.get_entity("test-slug")

    @patch.object(ArkhamClient, "_wait_for_rate_limit")
    def test_transfers_call_checks_credits(self, mock_wait):
        """Transfer mining calls also trigger the credit guard."""
        client = ArkhamClient(api_key="test", credit_floor=500)

        low_credit_resp = _mock_response(
            status_code=200,
            json_data={"transfers": []},
            credits_remaining=200,
        )
        client._http = MagicMock()
        client._http.request.return_value = low_credit_resp

        with pytest.raises(CreditBudgetExhausted):
            client.get_entity_transfers("test-slug")
