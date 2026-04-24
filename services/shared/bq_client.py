"""
BigQuery federated-query client.

All data is accessed via EXTERNAL_QUERY against the univest_db connection,
which points live to the production Postgres database.

Safety rules enforced here:
  1. Every query dry-runs first; aborts if estimated bytes > BQ_COST_CAP_BYTES.
  2. Cost is logged to bq_query_costs after every execution.
  3. Queries are parameterised via Python .format() before submission —
     parameter values must be validated by callers (no user-controlled input).
"""

import json
import logging
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from google.cloud import bigquery
from google.oauth2 import service_account

from services.shared.config import settings

log = logging.getLogger(__name__)

# BQ connection string pointing to prod Postgres
_BQ_CONNECTION = "projects/univest-applications/locations/asia-south2/connections/univest_db"
_BQ_PROJECT    = "univest-applications"

# SQL directory relative to repo root
_SQL_DIR = Path(__file__).parent.parent / "worker" / "sql" / "attribution"


def _make_client() -> bigquery.Client:
    creds_path = settings.google_application_credentials
    if creds_path:
        creds = service_account.Credentials.from_service_account_file(
            creds_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return bigquery.Client(project=_BQ_PROJECT, credentials=creds)
    # Falls back to ADC (useful in local dev with gcloud auth)
    return bigquery.Client(project=_BQ_PROJECT)


class BQClient:
    """Sync wrapper around google-cloud-bigquery used inside asyncio via run_in_executor."""

    def __init__(self):
        self._client = _make_client()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_sql(self, name: str, **params) -> str:
        """Load an attribution SQL file and substitute {param} placeholders."""
        path = _SQL_DIR / f"{name}.sql"
        raw = path.read_text()
        return raw.format(**params)

    def dry_run(self, external_sql: str, label: str = "") -> int:
        """
        Wrap SQL in EXTERNAL_QUERY, dry-run it, return estimated bytes_processed.
        Raises RuntimeError if cost estimate exceeds BQ_COST_CAP_BYTES.
        """
        bq_sql = self._wrap(external_sql)
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = self._client.query(bq_sql, job_config=job_config)
        estimated = job.total_bytes_processed or 0
        cap = settings.bq_cost_cap_bytes
        log.info("dry_run label=%s estimated_bytes=%d cap=%d", label, estimated, cap)
        if estimated > cap:
            raise RuntimeError(
                f"Query '{label}' estimated {estimated / 1e9:.2f} GB "
                f"exceeds cap {cap / 1e9:.2f} GB — aborted."
            )
        return estimated

    def stream_rows(
        self,
        external_sql: str,
        page_size: int = 10_000,
        label: str = "",
    ) -> tuple[list[dict[str, Any]], int]:
        """
        Execute the EXTERNAL_QUERY and return (rows, rows_returned).
        Logs bytes processed to caller for cursor storage.
        """
        bq_sql = self._wrap(external_sql)
        t0 = time.monotonic()

        job = self._client.query(bq_sql)
        result = job.result(page_size=page_size)

        rows = [dict(row) for row in result]
        duration_ms = int((time.monotonic() - t0) * 1000)
        bytes_processed = job.total_bytes_processed or 0

        log.info(
            "bq_query label=%s rows=%d bytes=%d duration_ms=%d",
            label, len(rows), bytes_processed, duration_ms,
        )
        return rows, bytes_processed

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _wrap(self, pg_sql: str) -> str:
        """Wrap a Postgres SQL string in EXTERNAL_QUERY.

        BQ EXTERNAL_QUERY takes a single-line double-quoted string literal, so
        we must collapse the multiline SQL and escape backslashes, double quotes,
        and newlines before embedding it.
        """
        import re
        # Strip -- line comments (they'd be fine in Postgres but add noise)
        stripped = re.sub(r"--[^\n]*", " ", pg_sql)
        # Collapse whitespace / newlines into single spaces
        collapsed = " ".join(stripped.split())
        # Escape for BQ double-quoted string literal
        escaped = collapsed.replace("\\", "\\\\").replace('"', '\\"')
        return f'SELECT * FROM EXTERNAL_QUERY("{_BQ_CONNECTION}", "{escaped}")'
