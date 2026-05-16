"""Periodic sync job — ledger-backed.

The legacy ``run_periodic_sync`` body has been replaced by a thin
shim that delegates to ``app.jobs.ledger_jobs``: the ledger drain
job handles the actual work, while this module keeps the legacy
entry-points (``run_periodic_sync``, ``run_consistency_check_job``,
``run_orphan_scan_job``, ``refresh_expiring_tokens``) so the
scheduler's ``IntervalTrigger`` rows keep firing without
modification.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from app.database import get_database, get_setting, set_setting

logger = logging.getLogger(__name__)

# Auto-pause when every active calendar has this many consecutive failures.
_CIRCUIT_BREAKER_THRESHOLD = 3


async def run_periodic_sync() -> None:
    """Periodic sync: enqueue every active user + drain the queue.

    The drain itself is implemented by
    :func:`app.jobs.ledger_jobs.ledger_drain_due`; here we ensure
    every active user has a due ``reconcile_requests`` row.
    """
    paused = await get_setting("sync_paused")
    if paused and paused.get("value_plain") == "true":
        logger.debug("Sync is paused, skipping periodic sync")
        return

    if not await acquire_job_lock("periodic_sync"):
        logger.debug("Periodic sync already running, skipping")
        return
    try:
        from app.jobs.ledger_jobs import ledger_drain_due, ledger_enqueue_periodic
        await ledger_enqueue_periodic()
        await ledger_drain_due()
        await _check_circuit_breaker()
        await _alert_failing_calendars()
    finally:
        await release_job_lock("periodic_sync")


async def _alert_failing_calendars() -> None:
    """Email an alert for any calendar stuck at 5+ consecutive sync
    failures (REWRITE_PLAN.md §12).

    Distinct from the circuit breaker, which fires only when EVERY
    calendar is failing.  ``queue_alert`` dedups per alert-type per
    user within an hour, so a persistently-failing calendar alerts
    at most hourly.  Calendars are grouped per user into one alert.
    """
    db = await get_database()
    rows = await (await db.execute(
        """SELECT cc.user_id,
                  cc.display_name,
                  cc.google_calendar_id,
                  css.consecutive_failures,
                  css.last_error
             FROM client_calendars cc
             JOIN calendar_sync_state css
               ON cc.id = css.client_calendar_id
            WHERE cc.is_active = TRUE
              AND COALESCE(css.consecutive_failures, 0) >= 5
            ORDER BY cc.user_id""",
    )).fetchall()
    if not rows:
        return

    from app.alerts.email import queue_alert

    by_user: dict[int, list] = {}
    for row in rows:
        by_user.setdefault(int(row["user_id"]), []).append(row)

    for user_id, cals in by_user.items():
        lines = [
            f"- {c['display_name'] or c['google_calendar_id']}: "
            f"{c['consecutive_failures']} consecutive failures "
            f"(last error: {c['last_error'] or 'unknown'})"
            for c in cals
        ]
        await queue_alert(
            alert_type="calendar_sync_failing",
            user_id=user_id,
            details=(
                "One or more calendars have failed to sync 5+ times "
                "in a row:\n" + "\n".join(lines)
            ),
        )


async def _check_circuit_breaker() -> None:
    """Auto-pause sync if every active calendar is consistently failing."""
    db = await get_database()
    cursor = await db.execute(
        "SELECT DISTINCT user_id FROM client_calendars WHERE is_active = TRUE",
    )
    user_ids = [row["user_id"] for row in await cursor.fetchall()]

    for user_id in user_ids:
        cursor = await db.execute(
            """SELECT COUNT(*) as total,
                      SUM(CASE WHEN COALESCE(css.consecutive_failures, 0) >= ?
                               THEN 1 ELSE 0 END) as failing
               FROM client_calendars cc
               LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
               WHERE cc.user_id = ? AND cc.is_active = TRUE""",
            (_CIRCUIT_BREAKER_THRESHOLD, user_id),
        )
        row = await cursor.fetchone()
        total = row["total"] or 0
        failing = row["failing"] or 0
        if total == 0 or failing < total:
            continue
        logger.error(
            "Circuit breaker: ALL %d calendars for user %d have %d+ consecutive failures. "
            "Auto-pausing sync.",
            total, user_id, _CIRCUIT_BREAKER_THRESHOLD,
        )
        await set_setting("sync_paused", "true")
        from app.alerts.email import queue_alert
        await queue_alert(
            alert_type="circuit_breaker",
            user_id=user_id,
            details=(
                f"Sync has been automatically paused because all {total} calendars "
                f"have failed {_CIRCUIT_BREAKER_THRESHOLD}+ times consecutively. "
                f"Check your connected accounts and resume sync from the settings page."
            ),
        )
        return


async def run_consistency_check_job() -> None:
    """Consistency-check job under the ledger architecture is a no-op:
    consistency is structurally enforced by the planner + outbox.
    Kept for backwards-compat with the scheduler config."""
    paused = await get_setting("sync_paused")
    if paused and paused.get("value_plain") == "true":
        return
    logger.debug(
        "consistency_check job: no-op (ledger architecture handles "
        "consistency structurally)",
    )


async def run_orphan_scan_job() -> None:
    """Periodic orphan scan via the ledger discovery pass."""
    paused = await get_setting("sync_paused")
    if paused and paused.get("value_plain") == "true":
        return
    if not await acquire_job_lock("orphan_scan"):
        logger.debug("Orphan scan already running, skipping")
        return
    try:
        from app.ledger.runtime import reconcile_user_by_id
        db = await get_database()
        rows = await (await db.execute(
            "SELECT id FROM users WHERE COALESCE(sync_paused, 0) = 0",
        )).fetchall()
        for r in rows:
            try:
                await reconcile_user_by_id(
                    int(r["id"]),
                    include_main=False,
                    run_discovery=True,
                )
            except Exception:
                logger.exception("orphan scan failed for user %s", r["id"])
    finally:
        await release_job_lock("orphan_scan")


async def refresh_expiring_tokens() -> None:
    """Proactively refresh tokens that will expire within an hour."""
    db = await get_database()
    threshold = (datetime.utcnow() + timedelta(hours=1)).isoformat()
    cursor = await db.execute(
        """SELECT ot.*, u.email as user_email
           FROM oauth_tokens ot
           JOIN users u ON ot.user_id = u.id
           WHERE ot.token_expiry IS NOT NULL AND ot.token_expiry < ?""",
        (threshold,),
    )
    expiring = await cursor.fetchall()
    if not expiring:
        return
    logger.info(f"Refreshing {len(expiring)} expiring tokens")
    from app.auth.google import get_valid_access_token
    for token in expiring:
        try:
            await get_valid_access_token(token["user_id"], token["google_account_email"])
            logger.debug(f"Refreshed token for {token['google_account_email']}")
        except Exception as e:
            logger.error(f"Failed to refresh token for {token['google_account_email']}: {e}")
            if "invalid_grant" in str(e).lower():
                from app.alerts.email import queue_alert
                await queue_alert(
                    alert_type="token_revoked",
                    user_id=token["user_id"],
                    details=(
                        f"Token for {token['google_account_email']} has been revoked. "
                        f"User needs to re-authenticate."
                    ),
                )


# ---------------------------------------------------------------------------
# Job-lock primitives (used by scheduler tests)
# ---------------------------------------------------------------------------
async def acquire_job_lock(job_name: str, timeout_minutes: int = 30) -> bool:
    """Acquire a lock for a job.  Returns True if acquired, False if held."""
    db = await get_database()
    now = datetime.utcnow()
    cutoff = (now - timedelta(minutes=timeout_minutes)).isoformat()
    await db.execute(
        "DELETE FROM job_locks WHERE job_name = ? AND locked_at < ?",
        (job_name, cutoff),
    )
    await db.commit()
    try:
        await db.execute(
            "INSERT INTO job_locks (job_name, locked_at, locked_by) VALUES (?, ?, ?)",
            (job_name, now.isoformat(), "worker"),
        )
        await db.commit()
        return True
    except Exception:
        return False


async def release_job_lock(job_name: str) -> None:
    db = await get_database()
    await db.execute("DELETE FROM job_locks WHERE job_name = ?", (job_name,))
    await db.commit()
