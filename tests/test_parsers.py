"""
Unit tests for worker parsers — no DB, no network.
"""

import pytest
from services.worker.parsers import (
    parse_campaign,
    parse_insight_ad,
    parse_insight_breakdown,
    _i,
    _f,
    _b,
)


# ---------------------------------------------------------------------------
# Type coercion helpers
# ---------------------------------------------------------------------------

def test_int_coercion():
    assert _i("42") == 42
    assert _i(99) == 99
    assert _i(None) is None
    assert _i("bad") is None


def test_float_coercion():
    assert _f("3.14") == pytest.approx(3.14)
    assert _f(0) == pytest.approx(0.0)
    assert _f(None) is None
    assert _f("NaN") is None


def test_bool_coercion():
    assert _b(True) is True
    assert _b(False) is False
    assert _b(None) is None


# ---------------------------------------------------------------------------
# Campaign parser
# ---------------------------------------------------------------------------

def test_parse_campaign_minimal():
    raw = {
        "id": "23851234567890",
        "name": "Q1 Brand Awareness",
        "status": "ACTIVE",
        "effective_status": "ACTIVE",
        "objective": "OUTCOME_AWARENESS",
    }
    result = parse_campaign(raw, account_id="act_123")
    assert result["id"] == "23851234567890"
    assert result["account_id"] == "act_123"
    assert result["name"] == "Q1 Brand Awareness"
    assert result["status"] == "ACTIVE"
    assert result["objective"] == "OUTCOME_AWARENESS"
    assert result["daily_budget"] is None
    assert result["raw"] == raw


def test_parse_campaign_with_budget():
    raw = {
        "id": "999",
        "daily_budget": "10000",
        "lifetime_budget": "0",
        "budget_remaining": "7500",
    }
    result = parse_campaign(raw, account_id="act_1")
    assert result["daily_budget"] == "10000"
    assert result["budget_remaining"] == "7500"


# ---------------------------------------------------------------------------
# Ad-level insight parser
# ---------------------------------------------------------------------------

def test_parse_insight_ad_scalars():
    raw = {
        "ad_id": "23851234567891",
        "date_start": "2024-06-01",
        "impressions": "10000",
        "reach": "8500",
        "spend": "47.23",
        "cpm": "4.723",
        "cpc": "0.89",
        "ctr": "0.053",
        "clicks": "531",
    }
    result = parse_insight_ad(raw, attribution_window="7d_click")
    assert result["ad_id"] == "23851234567891"
    assert result["date"] == "2024-06-01"
    assert result["attribution_window"] == "7d_click"
    assert result["impressions"] == 10000
    assert result["spend"] == pytest.approx(47.23)
    assert result["ctr"] == pytest.approx(0.053)


def test_parse_insight_ad_jsonb_passthrough():
    raw = {
        "ad_id": "123",
        "date_start": "2024-06-01",
        "actions": [{"action_type": "link_click", "value": "42"}],
        "purchase_roas": [{"action_type": "omni_purchase", "value": "3.21"}],
    }
    result = parse_insight_ad(raw, attribution_window="7d_click")
    assert result["actions"] == raw["actions"]
    assert result["purchase_roas"] == raw["purchase_roas"]


def test_parse_insight_ad_missing_fields_are_none():
    raw = {"ad_id": "123", "date_start": "2024-06-01"}
    result = parse_insight_ad(raw, attribution_window="1d_click")
    assert result["impressions"] is None
    assert result["spend"] is None
    assert result["actions"] is None


# ---------------------------------------------------------------------------
# Breakdown parser
# ---------------------------------------------------------------------------

def test_parse_insight_breakdown_age_gender():
    raw = {
        "ad_id": "23851234567891",
        "date_start": "2024-06-01",
        "age": "25-34",
        "gender": "female",
        "impressions": "4100",
        "spend": "18.50",
    }
    result = parse_insight_breakdown(raw, breakdown_type="age,gender", attribution_window="7d_click")
    assert result["ad_id"] == "23851234567891"
    assert result["breakdown_type"] == "age,gender"
    assert result["breakdown_key"] == {"age": "25-34", "gender": "female"}
    assert len(result["breakdown_key_hash"]) == 32   # md5 hex digest
    assert result["impressions"] == 4100
    assert result["spend"] == pytest.approx(18.50)


def test_parse_insight_breakdown_country():
    raw = {
        "ad_id": "111",
        "date_start": "2024-06-01",
        "country": "IN",
        "impressions": "500",
    }
    result = parse_insight_breakdown(raw, breakdown_type="country", attribution_window="7d_click")
    assert result["breakdown_key"] == {"country": "IN"}
    assert result["breakdown_type"] == "country"


def test_parse_insight_breakdown_hash_is_stable():
    raw = {
        "ad_id": "111",
        "date_start": "2024-06-01",
        "age": "18-24",
        "gender": "male",
    }
    r1 = parse_insight_breakdown(raw, breakdown_type="age,gender", attribution_window="7d_click")
    r2 = parse_insight_breakdown(raw, breakdown_type="age,gender", attribution_window="7d_click")
    assert r1["breakdown_key_hash"] == r2["breakdown_key_hash"]
