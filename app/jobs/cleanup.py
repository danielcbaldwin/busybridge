"""Retention cleanup job — ledger-backed.

Retention policy (unchanged at the API contract level):

* Single-occurrence ledger events past ``event_retention_days``
  since their end time → deleted.
* Cancelled (soft-deleted) recurring series past
  ``recurring_soft_delete_days`` since cancellation → deleted.
* Old ``sync_log`` rows past ``audit_log_retention_days`` → deleted.
* Disconnected client_calendars past
  ``disconnected_calendar_retention_days`` → deleted.
* Settled outbox rows (``done`` / ``superseded``) older than 7 days
  are pruned so the table doesn't grow without bound.

The legacy busy_blocks/event_mappings tables are no longer
written to under the ledger architecture; if they exist (from a
pre-cutover deployment) we also prune them here so the DB stays
clean during the parallel-run period.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from app.config import get_settings
from app.database import get_database

logger = logging.getLogger(__name__)


async def run_retention_cleanup() -> dict:
    """Run one retention-cleanup pass and return per-bucket counts."""
    settings = get_settings()
    db = await get_database()
    now = datetime.utcnow()

    summary = {
        "expired_ledger_events": 0,
        "deleted_recurring_series": 0,
        "old_sync_logs": 0,
        "disconnected_calendars": 0,
        "settled_outbox_rows": 0,
        # Legacy buckets (kept in the response shape for back-compat;
        # always zero post-cutover):
        "expired_event_mappings": 0,
        "old_busy_blocks": 0,
    }

    # 1. Single (non-recurring) ledger events past retention.
    event_cutoff = (
        now - timedelta(days=settings.event_retention_days)
    ).isoformat()
    cursor = await db.execute(
        """DELETE FROM ledger_events
            WHERE is_recurring = 0
              AND end_at IS NOT NULL
              AND end_at < ?
              AND status IN ('cancelled', 'active')
            RETURNING id""",
        (event_cutoff,),
    )
    summary["expired_ledger_events"] = len(await cursor.fetchall())

    # 2. Cancelled recurring series past retention.
    recurring_cutoff = (
        now - timedelta(days=settings.recurring_soft_delete_days)
    ).isoformat()
    cursor = await db.execute(
        """DELETE FROM ledger_events
            WHERE is_recurring = 1
              AND status = 'cancelled'
              AND cancelled_at IS NOT NULL
              AND cancelled_at < ?
            RETURNING id""",
        (recurring_cutoff,),
    )
    summary["deleted_recurring_series"] = len(await cursor.fetchall())

    # 3. Old sync logs.
    log_cutoff = (
        now - timedelta(days=settings.audit_log_retention_days)
    ).isoformat()
    cursor = await db.execute(
        "DELETE FROM sync_log WHERE created_at < ? RETURNING id",
        (log_cutoff,),
    )
    summary["old_sync_logs"] = len(await cursor.fetchall())

    # 4. Disconnected calendars past retention.
    calendar_cutoff = (
        now - timedelta(days=settings.disconnected_calendar_retention_days)
    ).isoformat()
    cursor = await db.execute(
        """DELETE FROM client_calendars
            WHERE is_active = FALSE
              AND disconnected_at IS NOT NULL
              AND disconnected_at < ?
            RETURNING id""",
        (calendar_cutoff,),
    )
    summary["disconnected_calendars"] = len(await cursor.fetchall())

    # 5. Settled outbox rows.
    outbox_cutoff = (now - timedelta(days=7)).isoformat()
    cursor = await db.execute(
        """DELETE FROM outbox_operations
            WHERE status IN ('done', 'superseded', 'permanent_failure')
              AND completed_at IS NOT NULL
              AND completed_at < ?
            RETURNING id""",
        (outbox_cutoff,),
    )
    summary["settled_outbox_rows"] = len(await cursor.fetchall())

    # 6. Legacy tables — best-effort prune for the parallel-run window.
    try:
        cursor = await db.execute(
            """DELETE FROM event_mappings
                WHERE is_recurring = FALSE
                  AND event_end IS NOT NULL
                  AND event_end < ?
                RETURNING id""",
            (event_cutoff,),
        )
        summary["expired_event_mappings"] = len(await cursor.fetchall())
    except Exception:
        pass
    try:
        cursor = await db.execute(
            """DELETE FROM busy_blocks
                WHERE event_mapping_id NOT IN (SELECT id FROM event_mappings)
                RETURNING id""",
        )
        summary["old_busy_blocks"] = len(await cursor.fetchall())
    except Exception:
        pass

    await db.commit()
    logger.info(f"Retention cleanup completed: {summary}")
    await db.execute(
        """INSERT INTO sync_log (action, status, details)
           VALUES ('retention_cleanup', 'success', ?)""",
        (json.dumps(summary),),
    )
    await db.commit()
    return summary


async def vacuum_database() -> None:
    """Run VACUUM on the database to reclaim space."""
    db = await get_database()
    logger.info("Running database VACUUM")
    await db.execute("VACUUM")
    logger.info("Database VACUUM completed")
