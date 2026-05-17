"""Retention cleanup job — ledger-backed.

Retention policy (unchanged at the API contract level):

* Single-occurrence ledger events past ``event_retention_days``
  since their end time → CANCELLED if still active (so the outbox
  drains their Google copies), then hard-deleted once every
  projection has drained.
* Cancelled (soft-deleted) recurring series past
  ``recurring_soft_delete_days`` since cancellation → hard-deleted
  once every projection has drained.
* Old ``sync_log`` rows past ``audit_log_retention_days`` → deleted.
* Disconnected client_calendars past
  ``disconnected_calendar_retention_days`` → deleted.
* Settled outbox rows (``done`` / ``superseded``) older than 7 days
  are pruned so the table doesn't grow without bound.

A ledger_event is never hard-deleted while a projection of it is
still ``present`` on Google — that would orphan the Google copy
(REWRITE_PLAN.md §18, enforced by a DB trigger).  Expired active
events are cancelled and re-planned here; the next reconcile drains
the deletes, and a subsequent retention pass removes the row.
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
        "expired_events_cancelled": 0,
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

    # 1. Single (non-recurring) events past retention.
    #
    #    Active expired events are CANCELLED and re-planned — the
    #    planner drives every projection to 'absent' and the next
    #    reconcile's diff drains the Google deletes.  Only rows whose
    #    projections have all drained ('present' nowhere) are then
    #    hard-deleted: deleting a row with a live projection would
    #    orphan its Google copy (REWRITE_PLAN.md §18).
    from app.ledger.planner import plan_for_ledger_event

    nowiso = now.isoformat()
    event_cutoff = (
        now - timedelta(days=settings.event_retention_days)
    ).isoformat()

    cancelled_rows = await (await db.execute(
        """UPDATE ledger_events
              SET status = 'cancelled',
                  cancelled_at = COALESCE(cancelled_at, ?),
                  version = version + 1,
                  updated_at = ?
            WHERE is_recurring = 0
              AND status = 'active'
              AND end_at IS NOT NULL
              AND end_at < ?
            RETURNING id""",
        (nowiso, nowiso, event_cutoff),
    )).fetchall()
    summary["expired_events_cancelled"] = len(cancelled_rows)
    # Re-plan each freshly-cancelled row so its projections flip to
    # 'absent' and become diverged; the next reconcile drains them.
    for row in cancelled_rows:
        await plan_for_ledger_event(db, ledger_event_id=int(row["id"]))

    cursor = await db.execute(
        """DELETE FROM ledger_events
            WHERE is_recurring = 0
              AND status = 'cancelled'
              AND end_at IS NOT NULL
              AND end_at < ?
              AND NOT EXISTS (
                  SELECT 1 FROM ledger_projections p
                   WHERE p.ledger_event_id = ledger_events.id
                     AND p.current_state = 'present')
            RETURNING id""",
        (event_cutoff,),
    )
    summary["expired_ledger_events"] = len(await cursor.fetchall())

    # 2. Cancelled recurring series past retention — only once every
    #    projection has drained (no Google copy left to orphan).
    recurring_cutoff = (
        now - timedelta(days=settings.recurring_soft_delete_days)
    ).isoformat()
    cursor = await db.execute(
        """DELETE FROM ledger_events
            WHERE is_recurring = 1
              AND status = 'cancelled'
              AND cancelled_at IS NOT NULL
              AND cancelled_at < ?
              AND NOT EXISTS (
                  SELECT 1 FROM ledger_projections p
                   WHERE p.ledger_event_id = ledger_events.id
                     AND p.current_state = 'present')
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
    # Only purge a disconnected calendar once nothing in the ledger
    # still needs its google_calendar_id mapping: a projection that is
    # still present on Google, permanently failed, or diverged still
    # owes a delete that the outbox routes via this calendar.  Purging
    # early would strand that delete (and may trip an FK).
    cursor = await db.execute(
        """DELETE FROM client_calendars
            WHERE is_active = FALSE
              AND disconnected_at IS NOT NULL
              AND disconnected_at < ?
              AND NOT EXISTS (
                  SELECT 1 FROM ledger_projections p
                   WHERE p.target_calendar_id = client_calendars.id
                     AND (p.current_state = 'present'
                          OR p.permanently_failed = 1
                          OR p.applied_ledger_version IS NULL
                          OR p.applied_ledger_version != p.desired_ledger_version
                          OR p.applied_payload_hash != p.desired_payload_hash)
              )
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

    # Legacy event_mappings / busy_blocks tables were dropped at the
    # Stage-5 cutover; nothing to prune here.  The summary keys stay
    # at zero for backwards-compatibility with admin UI consumers.

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
    """Run VACUUM on the database to reclaim space.

    VACUUM rewrites the whole file under an exclusive lock that can
    outlast ``busy_timeout`` on a large database.  Maintenance mode is
    held for its duration so the reconciler, webhook, and drain paths
    freeze rather than collide with it, and any reconcile pass already
    in flight is drained out first.
    """
    from app.maintenance import (
        enter_maintenance,
        exit_maintenance,
        wait_for_reconcile_quiescence,
    )

    db = await get_database()
    logger.info("Running database VACUUM")
    enter_maintenance()
    try:
        await wait_for_reconcile_quiescence()
        await db.execute("VACUUM")
    finally:
        exit_maintenance()
    logger.info("Database VACUUM completed")
