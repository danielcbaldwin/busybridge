"""Admin API endpoints."""

import json
import logging
import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse
from pydantic import BaseModel

from app.auth.session import require_admin, User
from app.database import get_database, get_setting, set_setting
from app.config import get_settings
from app.encryption import encrypt_value

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["admin"])


class SystemHealth(BaseModel):
    """System health overview."""
    total_users: int
    active_users_24h: int
    total_calendars: int
    active_calendars: int
    total_events_synced: int
    total_busy_blocks: int
    sync_errors_24h: int
    webhooks_active: int
    webhooks_expiring_soon: int
    sync_paused: bool
    database_size_mb: float


class UserSummary(BaseModel):
    """User summary for admin view."""
    id: int
    email: str
    display_name: Optional[str] = None
    is_admin: bool
    calendars_connected: int
    last_login: Optional[str] = None
    created_at: str


class UserDetail(BaseModel):
    """Detailed user information."""
    id: int
    email: str
    display_name: Optional[str] = None
    main_calendar_id: Optional[str] = None
    is_admin: bool
    created_at: str
    last_login_at: Optional[str] = None
    calendars: list
    recent_sync_logs: list


class SettingsResponse(BaseModel):
    """System settings response."""
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None
    smtp_username: Optional[str] = None
    smtp_from_address: Optional[str] = None
    alert_emails: Optional[str] = None
    alerts_enabled: bool = False
    sync_paused: bool = False


class UpdateSettingsRequest(BaseModel):
    """Request to update settings."""
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None
    smtp_username: Optional[str] = None
    smtp_password: Optional[str] = None
    smtp_from_address: Optional[str] = None
    alert_emails: Optional[str] = None
    alerts_enabled: Optional[bool] = None


class FactoryResetRequest(BaseModel):
    """Factory reset request."""
    confirmation: str  # Must be "RESET"


@router.get("/health", response_model=SystemHealth)
async def get_system_health(admin: User = Depends(require_admin)):
    """Get detailed system health."""
    db = await get_database()
    settings = get_settings()

    # Total users
    cursor = await db.execute("SELECT COUNT(*) FROM users")
    total_users = (await cursor.fetchone())[0]

    # Active users in last 24h
    cursor = await db.execute(
        """SELECT COUNT(*) FROM users
           WHERE datetime(last_login_at) > datetime('now', '-1 day')"""
    )
    active_users = (await cursor.fetchone())[0]

    # Total and active calendars
    cursor = await db.execute("SELECT COUNT(*) FROM client_calendars")
    total_calendars = (await cursor.fetchone())[0]

    cursor = await db.execute(
        "SELECT COUNT(*) FROM client_calendars WHERE is_active = TRUE"
    )
    active_calendars = (await cursor.fetchone())[0]

    # Events and busy blocks: now sourced from the ledger.
    cursor = await db.execute(
        """SELECT COUNT(*) FROM ledger_events
            WHERE status = 'active' AND user_intentionally_deleted = 0"""
    )
    total_events = (await cursor.fetchone())[0]

    cursor = await db.execute(
        """SELECT COUNT(*) FROM ledger_projections
            WHERE current_state = 'present'
              AND desired_state IN ('present_busy', 'present_personal_busy')"""
    )
    total_busy_blocks = (await cursor.fetchone())[0]

    # Sync errors in last 24h
    cursor = await db.execute(
        """SELECT COUNT(*) FROM sync_log
           WHERE status = 'failure' AND datetime(created_at) > datetime('now', '-1 day')"""
    )
    sync_errors = (await cursor.fetchone())[0]

    # Webhooks
    cursor = await db.execute(
        "SELECT COUNT(*) FROM webhook_channels WHERE datetime(expiration) > datetime('now')"
    )
    active_webhooks = (await cursor.fetchone())[0]

    cursor = await db.execute(
        """SELECT COUNT(*) FROM webhook_channels
           WHERE datetime(expiration) > datetime('now')
           AND datetime(expiration) < datetime('now', '+1 day')"""
    )
    expiring_webhooks = (await cursor.fetchone())[0]

    # Sync paused
    paused = await get_setting("sync_paused")
    sync_paused = bool(paused and paused.get("value_plain") == "true")

    # Database size
    db_size = 0
    if os.path.exists(settings.database_path):
        db_size = os.path.getsize(settings.database_path) / (1024 * 1024)

    return SystemHealth(
        total_users=total_users,
        active_users_24h=active_users,
        total_calendars=total_calendars,
        active_calendars=active_calendars,
        total_events_synced=total_events,
        total_busy_blocks=total_busy_blocks,
        sync_errors_24h=sync_errors,
        webhooks_active=active_webhooks,
        webhooks_expiring_soon=expiring_webhooks,
        sync_paused=sync_paused,
        database_size_mb=round(db_size, 2),
    )


@router.get("/users", response_model=list[UserSummary])
async def list_users(
    admin: User = Depends(require_admin),
    search: Optional[str] = None,
):
    """List all users."""
    db = await get_database()

    query = """
        SELECT u.*, COUNT(cc.id) as calendar_count
        FROM users u
        LEFT JOIN client_calendars cc ON u.id = cc.user_id AND cc.is_active = TRUE
    """
    params = []

    if search:
        query += " WHERE u.email LIKE ? OR u.display_name LIKE ?"
        params.extend([f"%{search}%", f"%{search}%"])

    query += " GROUP BY u.id ORDER BY u.created_at DESC"

    cursor = await db.execute(query, params)
    rows = await cursor.fetchall()

    return [
        UserSummary(
            id=row["id"],
            email=row["email"],
            display_name=row["display_name"],
            is_admin=bool(row["is_admin"]),
            calendars_connected=row["calendar_count"],
            last_login=row["last_login_at"],
            created_at=row["created_at"],
        )
        for row in rows
    ]


@router.get("/users/{user_id}", response_model=UserDetail)
async def get_user_detail(
    user_id: int,
    admin: User = Depends(require_admin),
):
    """Get detailed user information."""
    db = await get_database()

    # Get user
    cursor = await db.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = await cursor.fetchone()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # Get calendars
    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email, css.consecutive_failures,
                  css.last_incremental_sync
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
           WHERE cc.user_id = ?
           ORDER BY cc.created_at DESC""",
        (user_id,)
    )
    calendars = [dict(row) for row in await cursor.fetchall()]

    # Get recent sync logs
    cursor = await db.execute(
        """SELECT * FROM sync_log
           WHERE user_id = ?
           ORDER BY created_at DESC LIMIT 20""",
        (user_id,)
    )
    logs = [dict(row) for row in await cursor.fetchall()]

    return UserDetail(
        id=user["id"],
        email=user["email"],
        display_name=user["display_name"],
        main_calendar_id=user["main_calendar_id"],
        is_admin=bool(user["is_admin"]),
        created_at=user["created_at"],
        last_login_at=user["last_login_at"],
        calendars=calendars,
        recent_sync_logs=logs,
    )


@router.post("/users/{user_id}/sync")
async def trigger_user_sync(
    user_id: int,
    admin: User = Depends(require_admin),
):
    """Trigger sync for a user's calendars."""
    db = await get_database()

    cursor = await db.execute("SELECT id FROM users WHERE id = ?", (user_id,))
    if not await cursor.fetchone():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    from app.ledger.triggers import enqueue_manual
    await enqueue_manual(db, user_id=user_id, source_hint="all")

    return {"status": "ok", "message": "Sync triggered"}


@router.post("/users/{user_id}/force-reauth")
async def force_user_reauth(
    user_id: int,
    admin: User = Depends(require_admin),
):
    """Force user to re-authenticate by invalidating their tokens."""
    db = await get_database()

    cursor = await db.execute("SELECT id FROM users WHERE id = ?", (user_id,))
    if not await cursor.fetchone():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # Stop the user's push channels on Google BEFORE wiping local state,
    # so Google stops POSTing to them.  Best-effort: in a forced reauth
    # the token is often already bad, in which case the channels simply
    # expire on their TTL — but a voluntary reauth (valid token) is torn
    # down cleanly.  Runs outside the transaction below because the helper
    # commits internally.
    from app.api.webhooks import stop_channels_for_user
    try:
        await stop_channels_for_user(db, user_id=user_id)
    except Exception:
        logger.exception(
            "force_user_reauth: channel teardown failed for user %s", user_id,
        )

    # Wipe the user's sync state in one transaction so a crash mid-way
    # cannot leave some tables cleared and others intact.
    await db.execute("BEGIN IMMEDIATE")
    try:
        # Remove dependent client-calendar records first to satisfy
        # foreign keys.  Filter webhook_channels by user_id so
        # MAIN-calendar rows (client_calendar_id IS NULL) go too.
        await db.execute(
            "DELETE FROM webhook_channels WHERE user_id = ?",
            (user_id,)
        )
        await db.execute(
            """DELETE FROM calendar_sync_state
               WHERE client_calendar_id IN (
                   SELECT id FROM client_calendars WHERE user_id = ?
               )""",
            (user_id,)
        )
        # Delete the ledger projections (and, by cascade, the outbox)
        # explicitly BEFORE the ledger rows: the orphan-guard trigger on
        # ledger_events refuses a delete while a projection is still
        # 'present'.  This is a deliberate full sync-state wipe — the
        # user re-onboards from scratch — so removing the projections
        # first is the intended path here.
        await db.execute(
            """DELETE FROM ledger_projections
               WHERE ledger_event_id IN (
                   SELECT id FROM ledger_events WHERE user_id = ?
               )""",
            (user_id,),
        )
        await db.execute(
            """DELETE FROM ledger_events WHERE user_id = ?""", (user_id,),
        )
        await db.execute(
            """DELETE FROM reconcile_requests WHERE user_id = ?""", (user_id,),
        )
        await db.execute(
            """DELETE FROM sync_log
               WHERE calendar_id IN (
                   SELECT id FROM client_calendars WHERE user_id = ?
               )""",
            (user_id,)
        )
        await db.execute("DELETE FROM client_calendars WHERE user_id = ?", (user_id,))
        await db.execute("DELETE FROM oauth_tokens WHERE user_id = ?", (user_id,))
        # Bump the session-token version so the user's existing app
        # session cookies are invalidated too — not just Google tokens.
        await db.execute(
            "UPDATE users SET session_token_version = session_token_version + 1 "
            "WHERE id = ?",
            (user_id,),
        )
        await db.execute("COMMIT")
    except BaseException:
        await db.execute("ROLLBACK")
        raise

    # Log action
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'force_reauth', 'success', 'Admin forced re-authentication')""",
        (user_id,)
    )
    await db.commit()

    return {"status": "ok", "message": "User tokens invalidated"}


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: int,
    force: bool = False,
    admin: User = Depends(require_admin),
):
    """Delete a user and all their data.

    The user's BusyBridge-managed Google events are drained first.  If
    that drain cannot complete (e.g. a revoked token) the deletion is
    refused with HTTP 409 — unless ``force=true`` is passed, which
    deletes the user anyway and accepts that those events are left
    orphaned on Google.
    """
    if user_id == admin.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete yourself"
        )

    db = await get_database()

    cursor = await db.execute("SELECT id FROM users WHERE id = ?", (user_id,))
    if not await cursor.fetchone():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # Cleanup user's calendars first
    cursor = await db.execute(
        "SELECT id FROM client_calendars WHERE user_id = ?", (user_id,)
    )
    calendars = await cursor.fetchall()

    from app.ledger.admin_ops import disconnect_calendar
    for cal in calendars:
        try:
            await disconnect_calendar(
                db, user_id=user_id, client_calendar_id=int(cal["id"]),
            )
        except Exception as e:
            logger.warning(f"Error cleaning up calendar {cal['id']}: {e}")

    # Drain the staged Google deletes BEFORE wiping the ledger.
    # disconnect_calendar only sets projections to absent; the diff +
    # outbox drain that actually removes the events from Google runs
    # inside a reconcile pass.  Without this, DELETE FROM users
    # cascades the ledger/outbox away and the BusyBridge-managed
    # events are orphaned on Google with nothing left to track them.
    try:
        from app.ledger.runtime import reconcile_user_by_id
        await reconcile_user_by_id(user_id)
    except Exception as e:
        logger.warning(
            "could not drain Google deletes before deleting user %s: %s",
            user_id, e,
        )

    # Verify the managed events were actually removed.  A projection
    # still 'present' means the delete did not reach Google (typically
    # a revoked token) — refuse the deletion unless the admin
    # explicitly forces it and accepts the orphans.
    remaining = await (await db.execute(
        """SELECT COUNT(*) AS n FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ? AND p.current_state = 'present'""",
        (user_id,),
    )).fetchone()
    orphan_count = int(remaining["n"] or 0)
    if orphan_count and not force:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"{orphan_count} BusyBridge-managed event(s) could not be "
                f"removed from Google (likely a revoked token). Resolve the "
                f"account and retry, or pass force=true to delete the user "
                f"anyway and leave those events orphaned on Google."
            ),
        )
    if orphan_count:
        logger.warning(
            "force-deleting user %s leaves %d orphaned Google event(s)",
            user_id, orphan_count,
        )

    # Stop any push channels still live on Google — the main calendar's,
    # plus any the per-calendar disconnect loop above didn't cover —
    # before the cascade removes their local rows.  Otherwise Google keeps
    # POSTing to them for the channel TTL (the "Unknown webhook channel"
    # storm).  Best-effort.
    from app.api.webhooks import stop_channels_for_user
    try:
        await stop_channels_for_user(db, user_id=user_id)
    except Exception:
        logger.exception(
            "delete_user: channel teardown failed for user %s", user_id,
        )

    # Delete user (cascades to related records)
    await db.execute("DELETE FROM users WHERE id = ?", (user_id,))
    await db.commit()

    return {"status": "ok", "message": "User deleted"}


@router.put("/users/{user_id}/admin")
async def set_user_admin(
    user_id: int,
    is_admin: bool,
    admin: User = Depends(require_admin),
):
    """Set admin status for a user."""
    db = await get_database()

    cursor = await db.execute(
        "SELECT id, is_admin FROM users WHERE id = ?", (user_id,)
    )
    target = await cursor.fetchone()
    if not target:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # Refuse to demote the last remaining admin — that would lock
    # everyone out of the admin surface with no way back in.
    if not is_admin and target["is_admin"]:
        admin_count = (await (await db.execute(
            "SELECT COUNT(*) FROM users WHERE is_admin = TRUE",
        )).fetchone())[0]
        if admin_count <= 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot remove admin rights from the last administrator.",
            )

    await db.execute(
        "UPDATE users SET is_admin = ? WHERE id = ?",
        (is_admin, user_id)
    )
    await db.commit()

    return {"status": "ok", "is_admin": is_admin}


@router.post("/users/{user_id}/calendars/{calendar_id}/disconnect")
async def admin_disconnect_calendar(
    user_id: int,
    calendar_id: int,
    admin: User = Depends(require_admin),
):
    """Disconnect a calendar for a user."""
    db = await get_database()

    cursor = await db.execute(
        """SELECT * FROM client_calendars
           WHERE id = ? AND user_id = ? AND is_active = TRUE""",
        (calendar_id, user_id)
    )
    calendar = await cursor.fetchone()

    if not calendar:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Calendar not found"
        )

    from app.ledger.admin_ops import disconnect_calendar
    await disconnect_calendar(
        db, user_id=user_id, client_calendar_id=calendar_id,
    )
    return {"status": "ok", "message": "Calendar disconnected"}


@router.get("/logs")
async def get_system_logs(
    admin: User = Depends(require_admin),
    page: int = 1,
    page_size: int = 100,
    user_id: Optional[int] = None,
    status_filter: Optional[str] = None,
    action_filter: Optional[str] = None,
):
    """Get system-wide sync logs."""
    # Clamp pagination so a hostile query can't request a giant page
    # or drive a negative OFFSET.
    page = max(1, page)
    page_size = min(max(1, page_size), 500)
    db = await get_database()

    query = """
        SELECT sl.*, u.email as user_email, cc.display_name as calendar_name
        FROM sync_log sl
        LEFT JOIN users u ON sl.user_id = u.id
        LEFT JOIN client_calendars cc ON sl.calendar_id = cc.id
        WHERE 1=1
    """
    params = []

    if user_id:
        query += " AND sl.user_id = ?"
        params.append(user_id)

    if status_filter:
        query += " AND sl.status = ?"
        params.append(status_filter)

    if action_filter:
        query += " AND sl.action = ?"
        params.append(action_filter)

    # Get total
    count_query = query.replace(
        "SELECT sl.*, u.email as user_email, cc.display_name as calendar_name",
        "SELECT COUNT(*)"
    )
    cursor = await db.execute(count_query, params)
    total = (await cursor.fetchone())[0]

    # Get paginated results
    query += " ORDER BY sl.created_at DESC LIMIT ? OFFSET ?"
    params.extend([page_size, (page - 1) * page_size])

    cursor = await db.execute(query, params)
    rows = await cursor.fetchall()

    return {
        "entries": [dict(row) for row in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.post("/sync/pause")
async def pause_sync(admin: User = Depends(require_admin)):
    """Pause all sync operations."""
    await set_setting("sync_paused", "true")
    return {"status": "ok", "sync_paused": True}


@router.post("/sync/resume")
async def resume_sync(admin: User = Depends(require_admin)):
    """Resume sync operations."""
    await set_setting("sync_paused", "false")
    return {"status": "ok", "sync_paused": False}


@router.post("/cleanup")
async def trigger_cleanup(admin: User = Depends(require_admin)):
    """Trigger manual cleanup of old records."""
    from app.jobs.cleanup import run_retention_cleanup
    from app.utils.tasks import create_background_task
    create_background_task(run_retention_cleanup(), "admin_trigger_cleanup")
    return {"status": "ok", "message": "Cleanup triggered"}


@router.post("/consistency/check")
async def trigger_consistency_check(
    dry_run: bool = False,
    user_id: Optional[int] = None,
    admin: User = Depends(require_admin),
):
    """Run (or preview) the consistency check.

    Under the ledger architecture (REWRITE_PLAN.md §3) consistency
    is structurally enforced by the planner + outbox: divergences
    between desired and applied projection state ARE the
    inconsistencies, and they're reconciled automatically every
    drain tick.  This endpoint therefore reports — but does not
    "fix" — the current divergence count.

    For active repair, use POST ``/admin/ledger/users/{id}/sync-now``
    (enqueue a reconcile) or
    ``/admin/ledger/users/{id}/full-resync`` (clear sync tokens).
    """
    from app.ledger.facade import outbox_summary

    db = await get_database()
    if user_id is not None:
        cursor = await db.execute("SELECT id FROM users WHERE id = ?", (user_id,))
        if not await cursor.fetchone():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="User not found",
            )
        target_ids = [user_id]
    else:
        rows = await (await db.execute("SELECT id FROM users")).fetchall()
        target_ids = [int(r["id"]) for r in rows]

    summary = {
        "users_checked": len(target_ids),
        "pending_outbox": 0,
        "in_flight_outbox": 0,
        "permanently_failed": 0,
        "diverged_projections": 0,
    }
    for uid in target_ids:
        ob = await outbox_summary(db, user_id=uid)
        summary["pending_outbox"] += ob.get("pending", 0)
        summary["in_flight_outbox"] += ob.get("in_flight", 0)
        summary["permanently_failed"] += ob.get("permanent_failure", 0)
        row = await (await db.execute(
            """SELECT COUNT(*) AS n FROM ledger_projections p
                 JOIN ledger_events e ON e.id = p.ledger_event_id
                WHERE e.user_id = ?
                  AND (p.applied_ledger_version IS NULL
                       OR p.applied_ledger_version != p.desired_ledger_version
                       OR p.applied_payload_hash IS NULL
                       OR p.applied_payload_hash != p.desired_payload_hash)""",
            (uid,),
        )).fetchone()
        summary["diverged_projections"] += int(row["n"] or 0)
    return {"dry_run": dry_run, "summary": summary}


@router.post("/consistency/cleanup-duplicates")
async def trigger_duplicate_cleanup(
    dry_run: bool = True,
    admin: User = Depends(require_admin),
):
    """Duplicates from recurring rescheduling are structurally
    prevented under the ledger architecture (the deterministic
    Google ID + 409-as-success path in app/ledger/outbox.py).
    Kept as a no-op endpoint for backwards compatibility."""
    return {
        "dry_run": dry_run,
        "summary": {
            "duplicates_removed": 0,
            "note": "deterministic IDs make duplicates structurally impossible",
        },
    }


@router.get("/settings", response_model=SettingsResponse)
async def get_admin_settings(admin: User = Depends(require_admin)):
    """Get system settings."""
    result = SettingsResponse()

    for key in ["smtp_host", "smtp_port", "smtp_username", "smtp_from_address", "alert_emails"]:
        setting = await get_setting(key)
        if setting:
            value = setting.get("value_plain")
            if key == "smtp_port" and value:
                value = int(value)
            setattr(result, key, value)

    alerts_enabled = await get_setting("alerts_enabled")
    result.alerts_enabled = bool(alerts_enabled and alerts_enabled.get("value_plain") == "true")

    sync_paused = await get_setting("sync_paused")
    result.sync_paused = bool(sync_paused and sync_paused.get("value_plain") == "true")

    return result


@router.put("/settings")
async def update_admin_settings(
    request: UpdateSettingsRequest,
    admin: User = Depends(require_admin),
):
    """Update system settings."""
    if request.smtp_host is not None:
        await set_setting("smtp_host", request.smtp_host)

    if request.smtp_port is not None:
        await set_setting("smtp_port", str(request.smtp_port))

    if request.smtp_username is not None:
        await set_setting("smtp_username", request.smtp_username)

    if request.smtp_password is not None:
        await set_setting("smtp_password", request.smtp_password, is_sensitive=True, encrypt_func=encrypt_value)

    if request.smtp_from_address is not None:
        await set_setting("smtp_from_address", request.smtp_from_address)

    if request.alert_emails is not None:
        await set_setting("alert_emails", request.alert_emails)

    if request.alerts_enabled is not None:
        await set_setting("alerts_enabled", "true" if request.alerts_enabled else "false")

    return {"status": "ok"}


@router.post("/settings/test-email")
async def send_test_email(admin: User = Depends(require_admin)):
    """Send a test email to verify SMTP configuration."""
    from app.alerts.email import send_email

    try:
        await send_email(
            to_email=admin.email,
            subject="Calendar Sync - Test Email",
            body="This is a test email from Calendar Sync Engine.\n\nIf you received this, your email configuration is working correctly.",
        )
        return {"status": "ok", "message": f"Test email sent to {admin.email}"}
    except Exception as e:
        logger.exception("Test email send failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send test email. Check the server logs for details.",
        )


@router.post("/factory-reset")
async def factory_reset(
    request: FactoryResetRequest,
    admin: User = Depends(require_admin),
):
    """Factory reset - delete all data and return to OOBE."""
    if request.confirmation != "RESET":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Confirmation must be 'RESET'"
        )

    db = await get_database()
    settings = get_settings()

    # Delete all data in FK-safe order.
    # SQLite doesn't support parameterized table names, so this is intentionally
    # a hardcoded ordered list.
    SAFE_TABLES_IN_DELETE_ORDER = [
        "outbox_operations",
        "ledger_projections",
        "ledger_events",
        "reconcile_requests",
        "webhook_channels",
        "calendar_sync_state",
        "main_calendar_sync_state",
        "sync_log",
        "alert_queue",
        "oauth_states",
        "integrity_status",
        "client_calendars",
        "webcal_subscriptions",
        "oauth_tokens",
        "users",
        "settings",
        "organization",
        "job_locks",
    ]

    # Wipe every table in one transaction so a mid-reset failure rolls
    # back cleanly instead of leaving a half-erased database.
    import sqlite3
    await db.execute("BEGIN IMMEDIATE")
    try:
        for table in SAFE_TABLES_IN_DELETE_ORDER:
            try:
                await db.execute(f"DELETE FROM {table}")
            except sqlite3.OperationalError as e:
                # A table a past migration already removed is fine to
                # skip; any other operational error is a real failure.
                if "no such table" not in str(e).lower():
                    raise
        await db.execute("COMMIT")
    except BaseException:
        await db.execute("ROLLBACK")
        logger.exception("factory reset failed — rolled back, no data deleted")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Factory reset failed; no data was deleted. "
                   "Check the server logs.",
        )

    # Delete encryption key file
    if os.path.exists(settings.encryption_key_file):
        os.remove(settings.encryption_key_file)

    # Invalidate every existing session cookie.  After a reset the
    # first new admin is created with the same reusable user_id (1)
    # and session_token_version (0), so an old signed cookie would
    # otherwise authenticate as that brand-new admin.  Delete the
    # persisted session secret and clear its in-process cache so the
    # next session secret is freshly random and old cookies fail
    # signature verification.
    import app.config as _config
    _config._session_secret_cache = None
    if get_settings().session_secret_key:
        logger.warning(
            "factory reset: SESSION_SECRET_KEY is set from the "
            "environment — rotate it before re-running setup, or old "
            "session cookies will remain valid."
        )
    else:
        secret_dir = os.path.dirname(settings.encryption_key_file) or "."
        try:
            os.remove(os.path.join(secret_dir, "session_secret"))
        except FileNotFoundError:
            pass

    return {"status": "ok", "message": "Factory reset complete. Please restart the application."}


@router.get("/export")
async def export_database(admin: User = Depends(require_admin)):
    """Download a consistent backup of the database.

    Goes through the backup pipeline's sqlite3-backup ZIP rather than
    serving the raw DB file: the database runs in WAL mode, so a raw
    file copy can omit changes still sitting in an uncheckpointed
    write-ahead log.
    """
    from app.sync.backup import _backup_filepath, create_backup

    meta = await create_backup()
    zip_path = _backup_filepath(meta["backup_id"])
    if not os.path.exists(zip_path):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Backup could not be created",
        )
    return FileResponse(
        path=zip_path,
        filename=f"{meta['backup_id']}.zip",
        media_type="application/zip",
    )


# NOTE: service-account endpoints were removed per REWRITE_PLAN.md §1
# and §9.  The 🔒 emoji + uniform revert-on-drift mechanism in the
# ledger pipeline (app/ledger/payload.py + app/ledger/outbox.py)
# replaces SA mode entirely.
