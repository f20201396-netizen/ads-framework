"""
Shared upsert helpers and sync_run lifecycle management.

All jobs call upsert_dims() / upsert_facts() — never raw session.add()
so every write is idempotent and can be re-run safely.
"""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from services.shared.db import AsyncSessionLocal
from services.shared.meta_client import MetaAPIError
from services.shared.models import SyncRun

log = logging.getLogger(__name__)

UTC = timezone.utc
_CHUNK = 500  # rows per INSERT for narrow tables
_PG_MAX_PARAMS = 32000  # asyncpg hard limit is 32767; stay safely below


def _chunk_size(row: dict) -> int:
    """Compute max rows per INSERT so we never exceed asyncpg's 32767-param limit."""
    ncols = len(row)
    return max(1, _PG_MAX_PARAMS // ncols)


# ---------------------------------------------------------------------------
# Run tracking
# ---------------------------------------------------------------------------

@asynccontextmanager
async def track_run(
    entity_type: str,
    account_id: str | None = None,
) -> AsyncGenerator[SyncRun, None]:
    """
    Context manager that writes a sync_runs row on entry and updates it
    on exit.  Yields the SyncRun so the job can increment rows_upserted
    and request_count inline.

    Usage::

        async with track_run("campaigns", account_id) as run:
            async for item in client.list_campaigns(account_id):
                ...
                run.rows_upserted += 1
    """
    async with AsyncSessionLocal() as session:
        run = SyncRun(
            entity_type=entity_type,
            account_id=account_id,
            status="running",
            started_at=datetime.now(UTC),
            rows_upserted=0,
            request_count=0,
        )
        session.add(run)
        await session.commit()
        await session.refresh(run)

        try:
            yield run
            run.status = "success"
        except Exception as exc:
            run.status = "failed"
            run.error = _serialize_exc(exc)
            log.exception("sync_run %d (%s) failed", run.id, entity_type)
            raise
        finally:
            run.finished_at = datetime.now(UTC)
            # re-merge because the yielded object may span session boundaries
            merged = await session.merge(run)
            await session.commit()
            log.info(
                "sync_run id=%d entity=%s status=%s rows=%d",
                merged.id,
                entity_type,
                merged.status,
                merged.rows_upserted,
            )


def _serialize_exc(exc: Exception) -> dict:
    if isinstance(exc, MetaAPIError):
        return {"type": "MetaAPIError", "status_code": exc.status_code, **exc.error}
    return {"type": type(exc).__name__, "message": str(exc)}


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------

async def upsert_dims(
    session: AsyncSession,
    model,
    rows: list[dict],
    pk: list[str] | None = None,
) -> int:
    """
    Idempotent upsert for dimension tables (businesses, campaigns, ads…).
    Conflict target defaults to ["id"].
    Updates every column except PK and created_at; always refreshes
    updated_at and last_synced_at.
    """
    if not rows:
        return 0
    pk = pk or ["id"]
    total = 0
    for chunk in _chunks(rows, _chunk_size(rows[0])):
        stmt = pg_insert(model).values(chunk)
        skip = set(pk) | {"created_at"}
        update = {k: stmt.excluded[k] for k in chunk[0] if k not in skip}
        update["updated_at"] = func.now()
        update["last_synced_at"] = func.now()
        result = await session.execute(
            stmt.on_conflict_do_update(index_elements=pk, set_=update)
        )
        total += result.rowcount
    await session.commit()
    return total


async def upsert_facts(
    session: AsyncSession,
    model,
    rows: list[dict],
    pk: list[str],
) -> int:
    """
    Idempotent upsert for partitioned fact tables.
    pk must list the composite primary-key columns exactly as in the DDL.
    Updates every non-PK column; always refreshes synced_at.
    """
    if not rows:
        return 0
    pk_set = set(pk)
    # Deduplicate within the batch by PK (last row wins) to avoid
    # "ON CONFLICT DO UPDATE cannot affect row a second time"
    seen: dict[tuple, dict] = {}
    for row in rows:
        key = tuple(row.get(k) for k in pk)
        seen[key] = row
    rows = list(seen.values())
    total = 0
    for chunk in _chunks(rows, _chunk_size(rows[0])):
        stmt = pg_insert(model).values(chunk)
        update = {k: stmt.excluded[k] for k in chunk[0] if k not in pk_set}
        update["synced_at"] = func.now()
        result = await session.execute(
            stmt.on_conflict_do_update(index_elements=pk, set_=update)
        )
        total += result.rowcount
    await session.commit()
    return total


async def upsert_pixel_stats(
    session: AsyncSession,
    rows: list[dict],
) -> int:
    """Upsert for pixel_event_stats_daily, which uses a named constraint."""
    from services.shared.models import PixelEventStatsDaily

    if not rows:
        return 0
    total = 0
    for chunk in _chunks(rows, _CHUNK):
        stmt = pg_insert(PixelEventStatsDaily).values(chunk)
        update = {k: stmt.excluded[k] for k in chunk[0] if k not in {"pixel_id", "date", "event_name"}}
        update["synced_at"] = func.now()
        result = await session.execute(
            stmt.on_conflict_do_update(
                constraint="uq_pixel_event_stats_daily",
                set_=update,
            )
        )
        total += result.rowcount
    await session.commit()
    return total


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _chunks(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
