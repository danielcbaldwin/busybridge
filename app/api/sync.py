"""Sync status and control API endpoints (ledger-backed).

Every endpoint here used to drive ``app/sync/engine.py``; under
the ledger architecture (REWRITE_PLAN.md) they drive
``app.ledger.triggers`` / ``app.ledger.admin_ops`` / the facade
instead.  Behaviour is preserved at the API contract; internals
are the new pipeline.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.auth.session import User, get_current_user, require_admin
from app.database import get_database, get_setting, set_setting
from app.ledger import admin_ops, facade
from app.ledger.triggers import enqueue_manual

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sync", tags=["sync"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------
class SyncStatusResponse(BaseModel):
    calendars_connected: int
    calendars_healthy: int
    calendars_warning: int
    calendars_error: int
    last_sync: Optional[str] = None
    events_synced: int
    busy_blocks_created: int
    sync_paused: bool = False
    integrity_status: Optional[str] = None
    integrity_last_check: Optional[str] = None
    integrity_unresolved: int = 0


class SyncLogEntry(BaseModel):
    id: int
    calendar_id: Optional[int] = None
    calendar_name: Optional[str] = None
    action: str
    status: str
    details: Optional[str] = None
    created_at: str


class SyncLogResponse(BaseModel):
    entries: list[SyncLogEntry]
    total: int
    page: int
    page_size: int


# ---------------------------------------------------------------------------
# Status + activity
# ---------------------------------------------------------------------------
@router.get("/status", response_model=SyncStatusResponse)
async def get_sync_status(user: User = Depends(get_current_user)):
    """Overall sync status for the current user.  Counts come from
    the ledger facade; calendar health comes from the per-calendar
    consecutive_failures counter the ingest path maintains."""
    db = await get_database()

    cursor = await db.execute(
        """SELECT cc.id, css.consecutive_failures,
                  css.last_incremental_sync, css.last_full_sync
           FROM client_calendars cc
           LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
           WHERE cc.user_id = ? AND cc.is_active = TRUE""",
        (user.id,),
    )
    calendars = await cursor.fetchall()

    total = len(calendars)
    healthy = warning = error = 0
    last_sync: Optional[str] = None
    for cal in calendars:
        f = cal["consecutive_failures"] or 0
        if f >= 5:
            error += 1
        elif f >= 1:
            warning += 1
        else:
            healthy += 1
        ts = cal["last_incremental_sync"] or cal["last_full_sync"]
        if ts and (last_sync is None or ts > last_sync):
            last_sync = ts

    events_synced = await facade.count_active_ledger_events(db, user_id=user.id)
    busy_per_cal = await facade.count_busy_blocks_per_calendar(db, user_id=user.id)
    busy_blocks = sum(busy_per_cal.values())

    paused_setting = await get_setting("sync_paused")
    global_paused = bool(paused_setting and paused_setting.get("value_plain") == "true")
    user_row = await (await db.execute(
        "SELECT sync_paused FROM users WHERE id = ?", (user.id,),
    )).fetchone()
    user_paused = bool(user_row and user_row["sync_paused"])
    sync_paused = global_paused or user_paused

    # Integrity status is computed live from ledger state.  The
    # legacy ``integrity_status`` table is never written under the
    # ledger architecture (its consistency-check job is a no-op), so
    # reading it would leave the panel permanently blank.
    integrity = await facade.integrity_status_for_user(db, user_id=user.id)
    integrity_status_val: Optional[str] = integrity["status"]
    integrity_last_check: Optional[str] = None  # continuously checked
    integrity_unresolved = integrity["unresolved_issues"]

    return SyncStatusResponse(
        calendars_connected=total,
        calendars_healthy=healthy,
        calendars_warning=warning,
        calendars_error=error,
        last_sync=last_sync,
        events_synced=events_synced,
        busy_blocks_created=busy_blocks,
        sync_paused=sync_paused,
        integrity_status=integrity_status_val,
        integrity_last_check=integrity_last_check,
        integrity_unresolved=integrity_unresolved,
    )


@router.get("/log", response_model=SyncLogResponse)
async def get_sync_log(
    user: User = Depends(get_current_user),
    page: int = 1,
    page_size: int = 50,
    calendar_id: Optional[int] = None,
    status_filter: Optional[str] = None,
):
    """Paginated sync activity log.  The ``sync_log`` table is still
    populated (now by the ledger reconciler + admin ops) — see
    REWRITE_PLAN.md §12."""
    db = await get_database()
    query = """
        SELECT sl.*, cc.display_name as calendar_name
          FROM sync_log sl
          LEFT JOIN client_calendars cc ON sl.calendar_id = cc.id
         WHERE sl.user_id = ?
    """
    params: list = [user.id]
    if calendar_id:
        query += " AND sl.calendar_id = ?"
        params.append(calendar_id)
    if status_filter:
        query += " AND sl.status = ?"
        params.append(status_filter)

    count_q = query.replace(
        "SELECT sl.*, cc.display_name as calendar_name", "SELECT COUNT(*)",
    )
    total = (await (await db.execute(count_q, params)).fetchone())[0]

    query += " ORDER BY sl.created_at DESC LIMIT ? OFFSET ?"
    params.extend([page_size, (page - 1) * page_size])
    rows = await (await db.execute(query, params)).fetchall()
    return SyncLogResponse(
        entries=[
            SyncLogEntry(
                id=r["id"],
                calendar_id=r["calendar_id"],
                calendar_name=r["calendar_name"],
                action=r["action"],
                status=r["status"],
                details=r["details"],
                created_at=r["created_at"],
            )
            for r in rows
        ],
        total=total,
        page=page,
        page_size=page_size,
    )


# ---------------------------------------------------------------------------
# Triggers
# ---------------------------------------------------------------------------
@router.post("/full")
async def trigger_full_resync(user: User = Depends(get_current_user)):
    """Wipe sync tokens and enqueue a manual reconcile."""
    db = await get_database()
    await admin_ops.full_resync(db, user_id=user.id)
    await enqueue_manual(db, user_id=user.id, source_hint="all")
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'full_resync', 'success', 'User triggered full re-sync')""",
        (user.id,),
    )
    await db.commit()
    return {"status": "ok", "message": "Full re-sync triggered"}


@router.post("/pause")
async def pause_sync(user: User = Depends(require_admin)):
    """Pause sync globally (admin only)."""
    await set_setting("sync_paused", "true")
    db = await get_database()
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'sync_pause', 'success', 'Admin paused sync')""",
        (user.id,),
    )
    await db.commit()
    return {"status": "ok", "sync_paused": True}


@router.post("/resume")
async def resume_sync(user: User = Depends(require_admin)):
    """Resume sync globally (admin only)."""
    await set_setting("sync_paused", "false")
    db = await get_database()
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'sync_resume', 'success', 'Admin resumed sync')""",
        (user.id,),
    )
    await db.commit()
    return {"status": "ok", "sync_paused": False}


@router.post("/my/pause")
async def pause_my_sync(user: User = Depends(get_current_user)):
    """Pause sync for the current user only."""
    db = await get_database()
    await db.execute(
        "UPDATE users SET sync_paused = TRUE WHERE id = ?", (user.id,),
    )
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'sync_pause_user', 'success', 'User paused their own sync')""",
        (user.id,),
    )
    await db.commit()
    return {"status": "ok", "sync_paused": True}


@router.post("/my/resume")
async def resume_my_sync(user: User = Depends(get_current_user)):
    """Resume sync for the current user only."""
    db = await get_database()
    await admin_ops.resume_sync(db, user_id=user.id)
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'sync_resume_user', 'success', 'User resumed their own sync')""",
        (user.id,),
    )
    await db.commit()
    return {"status": "ok", "sync_paused": False}


# ---------------------------------------------------------------------------
# Cleanup / orphan-scan / integrity (ledger-backed reframing)
# ---------------------------------------------------------------------------
@router.get("/cleanup-progress")
async def get_cleanup_progress(user: User = Depends(get_current_user)):
    """Surface outbox-drain progress as cleanup-progress.

    The old "two-pass cleanup" (DB-driven + prefix sweep) is
    replaced by the planner setting projections to absent and the
    outbox draining the deletes.  Progress IS the outbox state.
    """
    db = await get_database()
    ob = await facade.outbox_summary(db, user_id=user.id)
    pending = int(ob.get("pending", 0))
    in_flight = int(ob.get("in_flight", 0))
    done = int(ob.get("done", 0))
    failed = int(ob.get("permanent_failure", 0))
    if pending == 0 and in_flight == 0:
        return {
            "status": "idle",
            "done": done,
            "permanently_failed": failed,
        }
    return {
        "status": "running",
        "pending": pending,
        "in_flight": in_flight,
        "done": done,
        "permanently_failed": failed,
    }


@router.get("/activity")
async def get_activity(user: User = Depends(get_current_user)):
    """Recent activity for the dashboard's live feed.

    Returns a JSON *array* — the feed iterates the response directly
    and reads ``time`` / ``level`` / ``action`` / ``calendar`` /
    ``detail`` off each item.  It previously returned a
    ``{"recent_outbox": [...]}`` object, so the feed's ``.slice`` threw
    and the panel silently never rendered."""
    db = await get_database()
    rows = await (await db.execute(
        """SELECT operation, status, completed_at, next_attempt_at,
                  created_at, last_error, target_google_calendar_id
             FROM outbox_operations
            WHERE user_id = ?
            ORDER BY id DESC LIMIT 20""",
        (user.id,),
    )).fetchall()
    items = []
    for r in rows:
        st = r["status"]
        if st == "permanent_failure":
            level, action = "error", "sync_failed"
            detail = (r["last_error"] or f"{r['operation']} failed")[:200]
        elif st == "done":
            level, action = "info", "sync_complete"
            detail = f"{r['operation']} applied"
        elif st == "superseded":
            level, action = "warning", "superseded"
            detail = f"{r['operation']} superseded by a newer change"
        else:  # pending / in_flight
            level, action = "warning", "sync_pending"
            detail = f"{r['operation']} {st}"
        items.append({
            "time": r["completed_at"] or r["next_attempt_at"] or r["created_at"],
            "level": level,
            "action": action,
            "calendar": r["target_google_calendar_id"],
            "detail": detail,
        })
    return items


@router.get("/integrity")
async def get_integrity_status(user: User = Depends(get_current_user)):
    """Integrity status surfaced via the facade.

    Under the ledger model, "integrity issues" = diverged
    projections + permanently-failed projections.  We compose a
    legacy-shaped response so existing dashboards keep rendering."""
    db = await get_database()
    integrity = await facade.integrity_status_for_user(db, user_id=user.id)
    return {
        "status": integrity["status"],
        "last_check_at": None,  # the ledger is continuously checked
        "issues_found": integrity["issues_found"],
        "issues_auto_fixed": 0,
        "unresolved_issues": integrity["unresolved_issues"],
        "consecutive_check_failures": 0,
        "details": {
            "diverged_projections": integrity["diverged"],
            "permanently_failed_projections": integrity["permanent_failures"],
        },
    }


@router.get("/check-connections")
async def check_connections(user: User = Depends(get_current_user)):
    """Verify every OAuth token can be refreshed."""
    from app.auth.google import get_valid_access_token

    db = await get_database()
    rows = await (await db.execute(
        """SELECT id, google_account_email, account_type, token_expiry
             FROM oauth_tokens WHERE user_id = ?""",
        (user.id,),
    )).fetchall()
    accounts: list[dict] = []
    all_ok = True
    for tok in rows:
        email = tok["google_account_email"]
        account_type = tok["account_type"]
        try:
            await get_valid_access_token(user.id, email)
            accounts.append({
                "email": email,
                "account_type": account_type,
                "status": "ok",
            })
        except Exception as e:
            all_ok = False
            msg = str(e).lower()
            if "invalid_grant" in msg or "no token found" in msg:
                fix = "reconnect"
                message = (
                    "Token has been revoked. Please reconnect this account."
                    if "invalid_grant" in msg
                    else "No token on file. Please reconnect this account."
                )
            else:
                fix = "retry"
                message = f"Token refresh failed: {type(e).__name__}"
            accounts.append({
                "email": email,
                "account_type": account_type,
                "status": "error",
                "message": message,
                "fix": fix,
            })
    return {"status": "ok" if all_ok else "error", "accounts": accounts}


@router.post("/scan-orphans")
async def trigger_orphan_scan(
    user: User = Depends(get_current_user),
    dry_run: bool = False,
):
    """Discovery / orphan scan via the ledger reconciler with
    run_discovery=True.  The dry_run param is preserved for API
    compatibility but is currently a no-op — the discovery pass
    is non-destructive on its own (it just enqueues deletes that
    the outbox drains; aborting between is possible only at the
    drain layer)."""
    from app.ledger.runtime import reconcile_user_by_id
    out = await reconcile_user_by_id(
        user.id,
        include_main=False,  # run_discovery walks main itself
        run_discovery=True,
        drain=not dry_run,
    )
    db = await get_database()
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'orphan_scan', 'success', ?)""",
        (user.id, json.dumps(out.get("discovery", {}))),
    )
    await db.commit()
    return {"status": "ok", "dry_run": dry_run, "result": out.get("discovery", {})}


@router.post("/cleanup-managed")
async def cleanup_managed_events(user: User = Depends(get_current_user)):
    """Cleanup-and-resync: cleanup every connected calendar then
    let the next reconcile rebuild from source."""
    db = await get_database()
    rows = await (await db.execute(
        """SELECT id FROM client_calendars
            WHERE user_id = ? AND is_active = 1""",
        (user.id,),
    )).fetchall()
    for r in rows:
        await admin_ops.cleanup_one_calendar(
            db, user_id=user.id, client_calendar_id=int(r["id"]),
        )
    await enqueue_manual(db, user_id=user.id, source_hint="all")
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'managed_cleanup', 'success', 'cleanup_and_resync enqueued')""",
        (user.id,),
    )
    await db.commit()
    return {"status": "started", "message": "Cleanup enqueued; events will re-sync automatically."}


@router.post("/cleanup-and-pause")
async def cleanup_and_pause(user: User = Depends(get_current_user)):
    """Global cleanup + pause for the current user."""
    db = await get_database()
    await admin_ops.cleanup_and_pause(db, user_id=user.id)
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'cleanup_and_pause', 'success', 'cleanup_and_pause set')""",
        (user.id,),
    )
    await db.commit()
    return {"status": "started", "message": "Cleanup enqueued; sync is paused."}
