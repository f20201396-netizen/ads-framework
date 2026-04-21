"""
Shared pytest fixtures.

VCR cassettes are stored under tests/cassettes/. When a cassette does not exist
httpx will make a real request and record it. To re-record, delete the cassette.
"""

import json
import pathlib
import pytest
import pytest_asyncio
import httpx
import respx

CASSETTES_DIR = pathlib.Path(__file__).parent / "cassettes"
BASE_URL = "https://graph.facebook.com/v21.0"
FAKE_TOKEN = "FAKE_TOKEN"
FAKE_ACCOUNT_ID = "act_123456789"
FAKE_BUSINESS_ID = "111111111111111"


def load_cassette(name: str) -> dict:
    """Load a JSON cassette by name (without extension)."""
    path = CASSETTES_DIR / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(
            f"Cassette {path} not found. "
            "Run tests with a real token once to record, or create the file manually."
        )
    return json.loads(path.read_text())


@pytest.fixture
def fake_token() -> str:
    return FAKE_TOKEN


@pytest.fixture
def fake_account_id() -> str:
    return FAKE_ACCOUNT_ID


@pytest.fixture
def fake_business_id() -> str:
    return FAKE_BUSINESS_ID
