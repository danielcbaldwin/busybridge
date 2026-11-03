"""Periodic sync job — ledger-backed.

The legacy ``run_periodic_sync`` body has been replaced by a
rollback-compatible shim that delegates to ``app.jobs.ledger_jobs``.
In normal ledger mode, ``app.jobs.scheduler`` schedules the ledger
enqueue/drain jobs directly and schedules ``run_sync_health_checks``
for circuit-breaker and failure-alert checks.
"""

from __future__ import annotations

import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

from app.database import get_database, get_setting

logger = logging.getLogger(__name__)

# Auto-pause when every active calendar has this many consecutive failures.
_CIRCUIT_BREAKER_THRESHOLD = 3

# Marks a ``users.sync_paused`` set by the breaker (as opposed to the
# user or an admin) — only these pauses self-heal.
CIRCUIT_BREAKER_PAUSE_REASON = "circuit_breaker"

# A breaker pause is lifted for a probe sync after this delay, doubling
# per consecutive trip up to the cap.  A probe that fails re-trips the
# breaker on the next health check; one that succeeds resets the
# failure counters and sync carries on.
_BREAKER_PROBE_BASE = timedelta(minutes=15)
_BREAKER_PROBE_MAX = timedelta(hours=6)


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

    lock = await acquire_job_lock("periodic_sync")
    if not lock:
        logger.debug("Periodic sync already running, skipping")
        return
    try:
        from app.jobs.ledger_jobs import ledger_drain_due, ledger_enqueue_periodic
        await ledger_enqueue_periodic()
        await ledger_drain_due()
        await _run_sync_health_checks_unlocked()
    finally:
        await release_job_lock("periodic_sync", lock)


async def run_sync_health_checks() -> None:
    """Run periodic sync health checks without enqueueing reconcile work."""
    paused = await get_setting("sync_paused")
    if paused and paused.get("value_plain") == "true":
        logger.debug("Sync is paused, skipping sync health checks")
        return

    lock = await acquire_job_lock("sync_health_checks")
    if not lock:
        logger.debug("Sync health checks already running, skipping")
        return
    try:
        await _run_sync_health_checks_unlocked()
    finally:
        await release_job_lock("sync_health_checks", lock)


async def _run_sync_health_checks_unlocked() -> None:
    await _probe_circuit_breaker_pauses()
    await _check_circuit_breaker()
    await _alert_failing_calendars()


async def _alert_failing_calendars() -> None:
    """Email an alert for any calendar stuck at 5+ consecutive sync
    failures.

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


def _breaker_probe_delay(trips: int) -> timedelta:
    """Backoff before the breaker lets a paused user try again."""
    delay = _BREAKER_PROBE_BASE * (2 ** max(trips - 1, 0))
    return min(delay, _BREAKER_PROBE_MAX)


def _parse_ts(value) -> Optional[datetime]:
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


async def _probe_circuit_breaker_pauses(now: Optional[datetime] = None) -> None:
    """Self-heal breaker pauses.

    A breaker pause used to be permanent until someone noticed and
    clicked resume — a five-minute DNS outage once froze sync for
    days.  Once the backoff for a user's trip count has elapsed the
    user is resumed and a reconcile is queued immediately.  If every
    calendar still fails, the next health check re-trips the breaker
    (with a longer backoff); if anything succeeds, sync just continues.

    Also forgets the trip count for users whose sync has recovered,
    and logs a WARNING for every user still held by the breaker so the
    state is visible in the log rather than only at DEBUG.
    """
    now = now or datetime.now(timezone.utc)
    db = await get_database()

    await db.execute(
        """UPDATE users SET breaker_trips = 0
            WHERE breaker_trips > 0
              AND COALESCE(sync_paused, 0) = 0
              AND EXISTS (
                  SELECT 1 FROM client_calendars cc
                    JOIN calendar_sync_state css
                      ON css.client_calendar_id = cc.id
                   WHERE cc.user_id = users.id AND cc.is_active = TRUE
                     AND COALESCE(css.consecutive_failures, 0) = 0
              )""",
    )
    await db.commit()

    rows = await (await db.execute(
        """SELECT id, sync_paused_at, breaker_trips FROM users
            WHERE COALESCE(sync_paused, 0) = 1 AND sync_pause_reason = ?""",
        (CIRCUIT_BREAKER_PAUSE_REASON,),
    )).fetchall()
    for row in rows:
        user_id = int(row["id"])
        trips = int(row["breaker_trips"] or 0)
        paused_at = _parse_ts(row["sync_paused_at"]) or now
        probe_at = paused_at + _breaker_probe_delay(trips)
        if now < probe_at:
            logger.warning(
                "Circuit breaker: sync for user %d paused since %s "
                "(trip %d); next automatic retry at %s",
                user_id, paused_at.isoformat(), trips, probe_at.isoformat(),
            )
            continue
        logger.warning(
            "Circuit breaker: retrying sync for user %d after %s pause "
            "(trip %d)",
            user_id, now - paused_at, trips,
        )
        from app.ledger.admin_ops import resume_sync
        await resume_sync(db, user_id=user_id, now=now, reset_trips=False)
        await db.execute(
            """INSERT INTO sync_log (user_id, action, status, details)
               VALUES (?, 'sync_resume_user', 'success', ?)""",
            (user_id, f"Circuit breaker automatic retry (trip {trips})"),
        )
        await db.commit()


async def _check_circuit_breaker(now: Optional[datetime] = None) -> None:
    """Auto-pause an individual user's sync when every one of their
    active sync sources is consistently failing.

    The pause is PER-USER (``users.sync_paused``) — one user's dead
    calendars must never stop sync for every other user.  Each
    affected user is paused independently; users already paused are
    skipped so the breaker does not re-alert them every tick.

    "Every sync source" spans both Google calendars
    (``client_calendars`` — client + personal) and webcal
    subscriptions (``webcal_subscriptions``).  Webcal feeds carry
    their own ``consecutive_failures`` counter; counting only the
    Google calendars would let a user whose every webcal feed is dead
    (or who has only webcal feeds) slip past the breaker entirely.

    A Google calendar only counts as failing when its latest failure
    was not transient (see ``is_transient_ingest_error``) and was
    recorded after the user's last resume — otherwise the stale
    counters re-trip the breaker before the resumed sync has had a
    chance to run.

    The pause is tagged ``sync_pause_reason = 'circuit_breaker'`` so
    :func:`_probe_circuit_breaker_pauses` can lift it later.
    """
    now = now or datetime.now(timezone.utc)
    db = await get_database()
    cursor = await db.execute(
        """SELECT DISTINCT u.id AS user_id, u.sync_resumed_at,
                  u.breaker_trips
             FROM users u
            WHERE COALESCE(u.sync_paused, 0) = 0
              AND (
                  EXISTS (SELECT 1 FROM client_calendars cc
                           WHERE cc.user_id = u.id AND cc.is_active = TRUE)
               OR EXISTS (SELECT 1 FROM webcal_subscriptions ws
                           WHERE ws.user_id = u.id AND ws.is_active = TRUE)
              )""",
    )
    users = await cursor.fetchall()

    for user in users:
        user_id = int(user["user_id"])
        cal_row = await (await db.execute(
            """SELECT COUNT(*) as total,
                      SUM(CASE WHEN COALESCE(css.consecutive_failures, 0) >= ?
                                AND COALESCE(css.last_error_transient, 0) = 0
                                AND (? IS NULL
                                     OR css.last_incremental_sync > ?)
                               THEN 1 ELSE 0 END) as failing
               FROM client_calendars cc
               LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
               WHERE cc.user_id = ? AND cc.is_active = TRUE""",
            (
                _CIRCUIT_BREAKER_THRESHOLD,
                user["sync_resumed_at"], user["sync_resumed_at"],
                user_id,
            ),
        )).fetchone()
        webcal_row = await (await db.execute(
            """SELECT COUNT(*) as total,
                      SUM(CASE WHEN COALESCE(consecutive_failures, 0) >= ?
                               THEN 1 ELSE 0 END) as failing
               FROM webcal_subscriptions
               WHERE user_id = ? AND is_active = TRUE""",
            (_CIRCUIT_BREAKER_THRESHOLD, user_id),
        )).fetchone()
        total = (cal_row["total"] or 0) + (webcal_row["total"] or 0)
        failing = (cal_row["failing"] or 0) + (webcal_row["failing"] or 0)
        if total == 0 or failing < total:
            continue
        trips = int(user["breaker_trips"] or 0) + 1
        logger.error(
            "Circuit breaker: all %d calendars for user %d have %d+ "
            "consecutive failures — pausing this user's sync (trip %d, "
            "automatic retry in %s).",
            total, user_id, _CIRCUIT_BREAKER_THRESHOLD, trips,
            _breaker_probe_delay(trips),
        )
        await db.execute(
            """UPDATE users
                  SET sync_paused = 1,
                      sync_pause_reason = ?,
                      sync_paused_at = ?,
                      breaker_trips = ?
                WHERE id = ?""",
            (CIRCUIT_BREAKER_PAUSE_REASON, now.isoformat(), trips, user_id),
        )
        await db.execute(
            """INSERT INTO sync_log (user_id, action, status, details)
               VALUES (?, 'sync_pause_user', 'success', ?)""",
            (user_id, f"Circuit breaker tripped (trip {trips})"),
        )
        await db.commit()
        if trips > 1:
            # Already alerted on the first trip; a failed automatic
            # retry is not news.
            continue
        from app.alerts.email import queue_alert
        await queue_alert(
            alert_type="circuit_breaker",
            user_id=user_id,
            details=(
                f"Sync has been automatically paused for your account "
                f"because all {total} of your connected calendars and "
                f"feeds have failed {_CIRCUIT_BREAKER_THRESHOLD}+ times "
                f"in a row. BusyBridge will retry automatically; check "
                f"your connected accounts, or resume sync from the "
                f"settings page."
            ),
        )
        # Do NOT return — pause every affected user, not just the first.


async def run_consistency_check_job() -> None:
    """Content audit: re-verify that ingested source content still
    matches Google, and correct drift incremental sync can't see.

    The planner + outbox structurally enforce consistency for whatever
    has been *ingested* — but if Google folds an edit into a revision
    without advancing the sync cursor (the create-then-rename race),
    incremental sync never re-delivers it and BB latches a stale value.
    This pass re-lists each client source calendar over a forward window
    and re-ingests any drifted event.  It never touches sync tokens and
    never infers deletions from the windowed list (see
    ``reconciler.audit_user``)."""
    paused = await get_setting("sync_paused")
    if paused and paused.get("value_plain") == "true":
        return
    lock = await acquire_job_lock("content_audit")
    if not lock:
        logger.debug("Content audit already running, skipping")
        return
    try:
        from app.ledger.runtime import audit_user_by_id
        db = await get_database()
        rows = await (await db.execute(
            "SELECT id FROM users WHERE COALESCE(sync_paused, 0) = 0",
        )).fetchall()
        for r in rows:
            try:
                out = await audit_user_by_id(int(r["id"]))
                if out.get("reingested"):
                    logger.info(
                        "content audit user=%s corrected %s drifted event(s)",
                        r["id"], out["reingested"],
                    )
            except Exception:
                logger.exception("content audit failed for user %s", r["id"])
    finally:
        await release_job_lock("content_audit", lock)


async def run_orphan_scan_job() -> None:
    """Periodic orphan scan via the ledger discovery pass."""
    paused = await get_setting("sync_paused")
    if paused and paused.get("value_plain") == "true":
        return
    lock = await acquire_job_lock("orphan_scan")
    if not lock:
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
        await release_job_lock("orphan_scan", lock)


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
async def acquire_job_lock(
    job_name: str, timeout_minutes: int = 30,
) -> Optional[str]:
    """Acquire a lock for a job.

    Returns a unique owner token on success, or ``None`` if the lock
    is already held.  The claim is an ``INSERT ... ON CONFLICT DO
    NOTHING`` checked via ``rowcount`` — so a lock that is genuinely
    held reports via rowcount 0, while a transient error (e.g. a
    locked DB) raises rather than being silently mistaken for "held"
    and skipping the job.

    The returned token must be passed to :func:`release_job_lock` so a
    job only ever releases its OWN lock — never a successor's that
    took over after this lock expired.
    """
    db = await get_database()
    now = datetime.utcnow()
    cutoff = (now - timedelta(minutes=timeout_minutes)).isoformat()
    owner = secrets.token_hex(16)
    await db.execute(
        "DELETE FROM job_locks WHERE job_name = ? AND locked_at < ?",
        (job_name, cutoff),
    )
    cursor = await db.execute(
        """INSERT INTO job_locks (job_name, locked_at, locked_by)
           VALUES (?, ?, ?)
           ON CONFLICT(job_name) DO NOTHING""",
        (job_name, now.isoformat(), owner),
    )
    await db.commit()
    return owner if (cursor.rowcount or 0) > 0 else None


async def release_job_lock(job_name: str, owner: Optional[str] = None) -> None:
    """Release a job lock.

    When ``owner`` is given, only a lock still held by that owner is
    deleted — so a job whose lock already expired and was taken over
    by a successor does not delete the successor's lock.
    """
    db = await get_database()
    if owner is None:
        await db.execute(
            "DELETE FROM job_locks WHERE job_name = ?", (job_name,),
        )
    else:
        await db.execute(
            "DELETE FROM job_locks WHERE job_name = ? AND locked_by = ?",
            (job_name, owner),
        )
    await db.commit()
