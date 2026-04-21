"""FastAPI shared dependencies."""

import base64
import logging
from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from services.shared.config import settings
from services.shared.db import AsyncSessionLocal

log = logging.getLogger(__name__)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session


def require_admin(x_admin_key: Annotated[str, Header()] = "") -> None:
    if not settings.admin_api_key:
        raise HTTPException(status_code=503, detail="Admin key not configured")
    if x_admin_key != settings.admin_api_key:
        raise HTTPException(status_code=401, detail="Invalid admin key")


# ---------------------------------------------------------------------------
# Cursor-based pagination (base64-encoded offset)
# ---------------------------------------------------------------------------

def encode_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(str(offset).encode()).decode()


def decode_cursor(cursor: str | None) -> int:
    if not cursor:
        return 0
    try:
        return max(0, int(base64.urlsafe_b64decode(cursor).decode()))
    except Exception:
        return 0
