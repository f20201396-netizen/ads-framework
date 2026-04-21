"""
Integration tests for MetaClient extractors.

Uses respx to mock httpx calls (cassette-style). Each test loads a JSON fixture
from tests/cassettes/ and asserts the parsed output shape is correct.
"""

import json
import pytest
import pytest_asyncio
import httpx
import respx

from services.shared.meta_client import MetaClient
from services.shared.rate_limiter import RateLimiter

BASE_URL = "https://graph.facebook.com/v21.0"
FAKE_TOKEN = "FAKE_TOKEN"
FAKE_ACCOUNT = "act_123456789"
FAKE_BUSINESS = "111111111111111"


class _NoopRateLimiter:
    """RateLimiter stub — does nothing during tests."""
    async def record(self, headers, endpoint=""):
        pass


def _make_client(http_client: httpx.AsyncClient) -> MetaClient:
    return MetaClient(
        access_token=FAKE_TOKEN,
        http_client=http_client,
        rate_limiter=_NoopRateLimiter(),
    )


def _cassette(name: str) -> dict:
    from tests.conftest import CASSETTES_DIR
    return json.loads((CASSETTES_DIR / f"{name}.json").read_text())


# ---------------------------------------------------------------------------
# Campaigns extractor
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_get_campaigns_returns_data():
    payload = _cassette("campaigns")
    respx.get(f"{BASE_URL}/{FAKE_ACCOUNT}/campaigns").mock(
        return_value=httpx.Response(200, json=payload)
    )

    async with httpx.AsyncClient() as http:
        client = _make_client(http)
        pages = []
        async for page in client.get_campaigns(FAKE_ACCOUNT):
            pages.extend(page)

    assert len(pages) == 1
    assert pages[0]["id"] == "23851234567890"
    assert pages[0]["status"] == "ACTIVE"
    assert pages[0]["objective"] == "OUTCOME_TRAFFIC"


# ---------------------------------------------------------------------------
# Ad-level insights extractor
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_get_insights_ad_level():
    payload = _cassette("insights_ad")
    time_range = {"since": "2024-06-01", "until": "2024-06-01"}

    respx.get(f"{BASE_URL}/{FAKE_ACCOUNT}/insights").mock(
        return_value=httpx.Response(200, json=payload)
    )

    async with httpx.AsyncClient() as http:
        client = _make_client(http)
        rows = []
        async for batch in client.get_insights(
            object_id=FAKE_ACCOUNT,
            level="ad",
            time_range=time_range,
            fields="ad_id,impressions,reach,spend,clicks,ctr,actions,purchase_roas",
            action_attribution_windows=["7d_click"],
        ):
            rows.extend(batch)

    assert len(rows) == 1
    row = rows[0]
    assert row["ad_id"] == "23851234567891"
    assert float(row["impressions"]) == 10000
    assert float(row["spend"]) == 47.23
    assert isinstance(row["actions"], list)
    assert row["actions"][0]["action_type"] == "link_click"


# ---------------------------------------------------------------------------
# Breakdown extractor (age_gender)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_get_insights_breakdown_age_gender():
    payload = _cassette("breakdown_age_gender")
    time_range = {"since": "2024-06-01", "until": "2024-06-01"}

    respx.get(f"{BASE_URL}/{FAKE_ACCOUNT}/insights").mock(
        return_value=httpx.Response(200, json=payload)
    )

    async with httpx.AsyncClient() as http:
        client = _make_client(http)
        rows = []
        async for batch in client.get_insights(
            object_id=FAKE_ACCOUNT,
            level="ad",
            time_range=time_range,
            fields="ad_id,impressions,reach,spend,clicks",
            breakdowns=["age", "gender"],
        ):
            rows.extend(batch)

    assert len(rows) == 2
    genders = {r["gender"] for r in rows}
    assert genders == {"female", "male"}
    total_spend = sum(float(r["spend"]) for r in rows)
    assert abs(total_spend - 47.23) < 0.01


# ---------------------------------------------------------------------------
# Retry behaviour — 500 then 200
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_client_retries_on_500():
    payload = _cassette("campaigns")
    call_count = 0

    def _side_effect(request):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(500, json={"error": {"message": "server error", "code": 1}})
        return httpx.Response(200, json=payload)

    respx.get(f"{BASE_URL}/{FAKE_ACCOUNT}/campaigns").mock(side_effect=_side_effect)

    async with httpx.AsyncClient() as http:
        client = _make_client(http)
        pages = []
        async for page in client.get_campaigns(FAKE_ACCOUNT):
            pages.extend(page)

    assert call_count == 2
    assert len(pages) == 1
