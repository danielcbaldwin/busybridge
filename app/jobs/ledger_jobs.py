"""Background jobs that drive the ledger pipeline.

These are scheduled by ``app.jobs.scheduler.setup_scheduler``.
They run alongside the legacy sync jobs — both systems write to
their own state (event_mappings/busy_blocks vs.
ledger_events/ledger_projections/outbox_operations) until the
Stage-5 cutover swap.

* :func:`ledger_drain_due` (every 30s) — pull every user with a
  due ``reconcile_requests`` row and run their reconciliation.
* :func:`ledger_enqueue_periodic` (every 5min) — upsert a
  ``reconcile_requests`` row for every active user so the drain
  picks them up even without an inbound webhook.
"""

from __future__ import annotations

import logging

from app.database import get_database, get_setting
from app.ledger.runtime import drain_all_due_users
from app.ledger.triggers import enqueue_periodic

logger = logging.getLogger(__name__)


async def ledger_drain_due() -> None:
    """Run a single drain tick: find every user with a due
    reconcile request and reconcile them.

    Errors per-user are logged and swallowed; one bad user must
    not block the whole tick.
    """
    try:
        out = await drain_all_due_users()
    except Exception as e:
        logger.exception("ledger_drain_due crashed: %s", e)
        return
    if out:
        succeeded = sum(1 for v in out.values() if not isinstance(v, dict) or "error" not in v)
        logger.info(
            "ledger drain processed %d users (%d succeeded, %d failed)",
            len(out), succeeded, len(out) - succeeded,
        )


async def ledger_enqueue_periodic() -> None:
    """Upsert a periodic ``reconcile_requests`` row for every
    active user.  ``enqueue_periodic`` preserves the earliest
    schedule so the drain doesn't get postponed.
    """
    global_pause = await get_setting("sync_paused")
    if global_pause and global_pause.get("value_plain") == "true":
        logger.debug("ledger_enqueue_periodic skipped: global sync pause on")
        return
    db = await get_database()
    rows = await (await db.execute(
        """SELECT id FROM users
            WHERE COALESCE(sync_paused, 0) = 0""",
    )).fetchall()
    for row in rows:
        try:
            await enqueue_periodic(db, user_id=int(row["id"]))
        except Exception as e:
            logger.warning(
                "ledger_enqueue_periodic failed for user %s: %s",
                row["id"], e,
            )
    logger.debug("ledger_enqueue_periodic upserted %d users", len(rows))
