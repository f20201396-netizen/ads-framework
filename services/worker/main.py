"""
Worker entrypoint — APScheduler with AsyncIOScheduler.

Job schedule (all times UTC):
  sync_account_structure      every 30 min
  sync_insights_daily         every 1 h
  sync_insights_higher_levels every 1 h
  sync_insights_breakdowns    every 6 h
  sync_audiences_pixels_catalogs  daily 21:30 UTC (03:00 IST)
  sync_pixel_stats            daily 21:30 UTC (03:00 IST)
"""

import asyncio
import json
import logging
import logging.config

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from services.worker.jobs.sync_attribution import (
    refresh_conversion_mv,
    sync_attribution_conversions,
    sync_attribution_signups,
)
from services.worker.jobs.sync_aux import sync_audiences_pixels_catalogs, sync_pixel_stats
from services.worker.jobs.sync_breakdowns import sync_insights_breakdowns
from services.worker.jobs.sync_higher_levels import sync_insights_higher_levels
from services.worker.jobs.sync_insights import sync_insights_daily
from services.worker.jobs.sync_structure import sync_account_structure


# ---------------------------------------------------------------------------
# Structured JSON logging
# ---------------------------------------------------------------------------

class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def _configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers = [handler]

    # Silence noisy third-party loggers
    for lib in ("httpx", "httpcore", "apscheduler.executors"):
        logging.getLogger(lib).setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    _configure_logging()
    log = logging.getLogger(__name__)
    log.info("Worker starting")

    scheduler = AsyncIOScheduler(timezone="UTC")

    scheduler.add_job(
        sync_account_structure,
        IntervalTrigger(minutes=30),
        id="sync_structure",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        sync_insights_daily,
        IntervalTrigger(hours=1),
        id="sync_insights_daily",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        sync_insights_higher_levels,
        IntervalTrigger(hours=1),
        id="sync_higher_levels",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        sync_insights_breakdowns,
        IntervalTrigger(hours=6),
        id="sync_breakdowns",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        sync_audiences_pixels_catalogs,
        CronTrigger(hour=21, minute=30, timezone="UTC"),
        id="sync_aux",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        sync_pixel_stats,
        CronTrigger(hour=21, minute=30, timezone="UTC"),
        id="sync_pixel_stats",
        max_instances=1,
        coalesce=True,
    )

    # Attribution (Phase 6) — every 15 min; MV refresh hourly
    scheduler.add_job(
        sync_attribution_signups,
        IntervalTrigger(minutes=15),
        id="sync_attribution_signups",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        sync_attribution_conversions,
        IntervalTrigger(minutes=15),
        id="sync_attribution_conversions",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        refresh_conversion_mv,
        IntervalTrigger(hours=1),
        id="refresh_conversion_mv",
        max_instances=1,
        coalesce=True,
    )

    scheduler.start()
    log.info(
        "Scheduler started with %d jobs: %s",
        len(scheduler.get_jobs()),
        [j.id for j in scheduler.get_jobs()],
    )

    try:
        await asyncio.Event().wait()  # block forever
    except (KeyboardInterrupt, SystemExit):
        log.info("Shutdown signal received")
    finally:
        scheduler.shutdown(wait=False)
        log.info("Worker stopped")


if __name__ == "__main__":
    asyncio.run(main())
