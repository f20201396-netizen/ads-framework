"""
Google Ads API client — thin wrapper around the google-ads Python library.

Uses OAuth2 refresh token (no service account needed for Ads API).
All monetary values from Google Ads are in micros (1/1,000,000 of currency unit).
"""

import logging
from typing import Generator

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

from services.shared.config import settings

log = logging.getLogger(__name__)

_MICROS = 1_000_000


def _build_client() -> GoogleAdsClient:
    config = {
        "developer_token":  settings.google_ads_developer_token,
        "client_id":        settings.google_ads_client_id,
        "client_secret":    settings.google_ads_client_secret,
        "refresh_token":    settings.google_ads_refresh_token,
        "login_customer_id": settings.google_ads_login_customer_id,
        "use_proto_plus":   True,
    }
    return GoogleAdsClient.load_from_dict(config)


def get_client() -> GoogleAdsClient:
    return _build_client()


def run_query(gaql: str, customer_id: str | None = None) -> list:
    """Run a GAQL query and return all rows as a list."""
    client = _build_client()
    cid = (customer_id or settings.google_ads_customer_id_clean).replace("-", "")
    service = client.get_service("GoogleAdsService")
    try:
        response = service.search(customer_id=cid, query=gaql)
        return list(response)
    except GoogleAdsException as exc:
        for error in exc.failure.errors:
            log.error("Google Ads API error: %s", error.message)
        raise


def micros_to_units(micros: int | None) -> float | None:
    """Convert micros to currency units (INR)."""
    if micros is None:
        return None
    return micros / _MICROS
