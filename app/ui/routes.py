"""UI page routes."""

import logging
import re
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.auth.session import get_current_user, get_current_user_optional, User
from app.config import get_settings, get_test_mode_home_allowlist
from app.database import get_database, get_setting, is_oobe_completed

logger = logging.getLogger(__name__)
router = APIRouter(tags=["ui"])

templates = Jinja2Templates(directory="app/ui/templates")

# A Google calendar's backgroundColor is rendered into a CSS style
# attribute; only a plain #rgb / #rrggbb(aa) hex value is allowed
# through, everything else falls back to a safe default.
_HEX_COLOR_RE = re.compile(r"^#[0-9A-Fa-f]{3,8}$")


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Root route - redirect to app or setup."""
    if not await is_oobe_completed():
        return RedirectResponse(url="/setup", status_code=status.HTTP_302_FOUND)

    return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)


@router.get("/app", response_class=HTMLResponse)
async def dashboard(request: Request, error: Optional[str] = None):
    """Main dashboard page."""
    if not await is_oobe_completed():
        return RedirectResponse(url="/setup", status_code=status.HTTP_302_FOUND)

    user = await get_current_user_optional(request)
    if not user:
        return RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)

    db = await get_database()

    # Per-calendar event + busy-block counts are sourced from the
    # ledger (REWRITE_PLAN.md §4) — the source-of-truth post-rewrite.
    from app.ledger.facade import (
        count_active_events_per_source_calendar,
        count_active_events_per_webcal_subscription,
        count_active_ledger_events,
        count_busy_blocks_per_calendar,
    )
    events_per_calendar = await count_active_events_per_source_calendar(
        db, user_id=user.id,
    )
    busy_per_calendar = await count_busy_blocks_per_calendar(db, user_id=user.id)
    events_per_webcal = await count_active_events_per_webcal_subscription(
        db, user_id=user.id,
    )

    # Get connected client calendars (rows; counts merged in below).
    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email, css.last_incremental_sync,
                  css.last_full_sync, css.consecutive_failures, css.last_error
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
           WHERE cc.user_id = ? AND cc.is_active = TRUE AND cc.calendar_type = 'client'
           ORDER BY cc.created_at DESC""",
        (user.id,)
    )
    calendar_rows = await cursor.fetchall()
    calendars = [
        _merge_counts(row, events_per_calendar, busy_per_calendar)
        for row in calendar_rows
    ]

    # Get connected personal calendars
    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email, css.last_incremental_sync,
                  css.last_full_sync, css.consecutive_failures, css.last_error
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
           WHERE cc.user_id = ? AND cc.is_active = TRUE AND cc.calendar_type = 'personal'
           ORDER BY cc.created_at DESC""",
        (user.id,)
    )
    personal_rows = await cursor.fetchall()
    personal_calendars = [
        _merge_counts(row, events_per_calendar, busy_per_calendar)
        for row in personal_rows
    ]

    # Sync-failure status — straight from calendar_sync_state.
    cursor = await db.execute(
        """SELECT COUNT(*) as total,
                  SUM(CASE WHEN css.consecutive_failures >= 5 THEN 1 ELSE 0 END) as errors,
                  SUM(CASE WHEN css.consecutive_failures BETWEEN 1 AND 4 THEN 1 ELSE 0 END) as warnings
           FROM client_calendars cc
           LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
           WHERE cc.user_id = ? AND cc.is_active = TRUE""",
        (user.id,)
    )
    status_row = await cursor.fetchone()

    # Total event count via the ledger.
    event_count = await count_active_ledger_events(db, user_id=user.id)
    managed_event_prefix = (get_settings().managed_event_prefix or "").strip()

    paused_setting = await get_setting("sync_paused")
    global_paused = bool(paused_setting and paused_setting.get("value_plain") == "true")
    cursor = await db.execute(
        "SELECT sync_paused FROM users WHERE id = ?", (user.id,)
    )
    user_pause_row = await cursor.fetchone()
    user_paused = bool(user_pause_row and user_pause_row["sync_paused"])
    sync_paused = global_paused or user_paused

    # Integrity check status — computed live from ledger state.  The
    # legacy integrity_status table is never written post-cutover, so
    # reading it left this panel permanently blank.
    from app.ledger import facade as _facade
    _integrity = await _facade.integrity_status_for_user(db, user_id=user.id)
    integrity = {
        "last_check_at": None,  # the ledger is continuously checked
        "unresolved_issues": _integrity["unresolved_issues"],
        "issues_auto_fixed": 0,
        "consecutive_check_failures": 0,
        "status": _integrity["status"],
    }

    # Get webcal subscriptions with placement-target metadata so the
    # dashboard can render the "Placement target disconnected" badge
    # (and so the edit form can pre-select the current target).  LEFT
    # JOIN client_calendars on the placement_client_calendar_id; rows
    # whose target is gone or inactive resolve to NULL on the joined
    # columns and fall into the 'disconnected' status bucket.
    cursor = await db.execute(
        """SELECT ws.*,
                  cc.display_name AS placement_client_display_name,
                  cc.is_active    AS placement_client_is_active
             FROM webcal_subscriptions ws
        LEFT JOIN client_calendars cc
               ON cc.id = ws.placement_client_calendar_id
            WHERE ws.user_id = ? AND ws.is_active = TRUE
         ORDER BY ws.created_at DESC""",
        (user.id,)
    )
    webcal_rows = await cursor.fetchall()
    webcal_subscriptions = [
        _merge_webcal_count(row, events_per_webcal) for row in webcal_rows
    ]

    # Active client calendars for the placement dropdown.  The list
    # excludes disconnected calendars (per webcal.md §User Flow:
    # "lists only currently active client calendars").  The template
    # disables the Client radio when this list is empty.
    cursor = await db.execute(
        """SELECT id, display_name
             FROM client_calendars
            WHERE user_id = ? AND is_active = 1
         ORDER BY display_name""",
        (user.id,)
    )
    active_client_calendars = [
        {"id": row["id"], "display_name": row["display_name"] or "(unnamed)"}
        for row in await cursor.fetchall()
    ]

    return templates.TemplateResponse(request, "dashboard.html", context={
        "user": user,
        "calendars": calendars,
        "personal_calendars": personal_calendars,
        "webcal_subscriptions": webcal_subscriptions,
        "active_client_calendars": active_client_calendars,
        "status": status_row,
        "event_count": event_count,
        "managed_event_prefix": managed_event_prefix,
        "sync_paused": sync_paused,
        "global_paused": global_paused,
        "user_paused": user_paused,
        "integrity": integrity,
        "error": error,
    })


@router.get("/app/login", response_class=HTMLResponse)
async def login_page(
    request: Request,
    error: Optional[str] = None,
    domain: Optional[str] = None,
):
    """Login page."""
    if not await is_oobe_completed():
        return RedirectResponse(url="/setup", status_code=status.HTTP_302_FOUND)

    user = await get_current_user_optional(request)
    if user:
        return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)

    settings = get_settings()
    required_domain = None
    allowed_home_emails = sorted(get_test_mode_home_allowlist()) if settings.test_mode else []

    if not settings.test_mode:
        # Get organization domain for display
        db = await get_database()
        cursor = await db.execute("SELECT google_workspace_domain FROM organization LIMIT 1")
        org = await cursor.fetchone()
        required_domain = domain or (org["google_workspace_domain"] if org else None)

    return templates.TemplateResponse(request, "login.html", context={
        "error": error,
        "required_domain": required_domain,
        "test_mode": settings.test_mode,
        "allowed_home_emails": allowed_home_emails,
    })


@router.get("/app/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    """User settings page."""
    user = await get_current_user_optional(request)
    if not user:
        return RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)

    db = await get_database()

    # Get user's calendars from Google
    calendars = []
    try:
        from app.auth.google import fetch_calendar_list

        calendars = await fetch_calendar_list(user.id, user.email)
    except Exception as e:
        logger.error(f"Failed to get calendars: {e}")

    return templates.TemplateResponse(request, "settings.html", context={
        "user": user,
        "calendars": calendars,
    })


@router.get("/app/settings/sync", response_class=HTMLResponse)
async def sync_control_page(request: Request):
    """Sync control page — cleanup, pause, and re-sync operations."""
    user = await get_current_user_optional(request)
    if not user:
        return RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)

    db = await get_database()
    paused_setting = await get_setting("sync_paused")
    global_paused = bool(paused_setting and paused_setting.get("value_plain") == "true")
    cursor = await db.execute(
        "SELECT sync_paused FROM users WHERE id = ?", (user.id,)
    )
    user_pause_row = await cursor.fetchone()
    user_paused = bool(user_pause_row and user_pause_row["sync_paused"])
    sync_paused = global_paused or user_paused
    managed_event_prefix = (get_settings().managed_event_prefix or "").strip()

    return templates.TemplateResponse(request, "sync_control.html", context={
        "user": user,
        "sync_paused": sync_paused,
        "global_paused": global_paused,
        "user_paused": user_paused,
        "managed_event_prefix": managed_event_prefix,
    })


@router.get("/app/settings/exports", response_class=HTMLResponse)
async def exports_page(request: Request):
    """ICS calendar export downloads."""
    user = await get_current_user_optional(request)
    if not user:
        return RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)

    from app.sync.ics_export import list_ics_backups
    backups = list_ics_backups()

    return templates.TemplateResponse(request, "exports.html", context={
        "user": user,
        "backups": backups,
    })


@router.get("/app/logs", response_class=HTMLResponse)
async def logs_page(
    request: Request,
    page: int = 1,
    calendar_id: Optional[int] = None,
    status_filter: Optional[str] = None,
):
    """Sync logs page."""
    user = await get_current_user_optional(request)
    if not user:
        return RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)

    db = await get_database()
    page_size = 50

    # Build query
    query = """
        SELECT sl.*, cc.display_name as calendar_name
        FROM sync_log sl
        LEFT JOIN client_calendars cc ON sl.calendar_id = cc.id
        WHERE sl.user_id = ?
    """
    params = [user.id]

    if calendar_id:
        query += " AND sl.calendar_id = ?"
        params.append(calendar_id)

    if status_filter:
        query += " AND sl.status = ?"
        params.append(status_filter)

    # Get total
    count_query = query.replace("SELECT sl.*, cc.display_name as calendar_name", "SELECT COUNT(*)")
    cursor = await db.execute(count_query, params)
    total = (await cursor.fetchone())[0]

    # Get paginated results
    query += " ORDER BY sl.created_at DESC LIMIT ? OFFSET ?"
    params.extend([page_size, (page - 1) * page_size])

    cursor = await db.execute(query, params)
    logs = await cursor.fetchall()

    # Get user's calendars for filter dropdown
    cursor = await db.execute(
        "SELECT id, display_name FROM client_calendars WHERE user_id = ?",
        (user.id,)
    )
    calendars = await cursor.fetchall()

    total_pages = (total + page_size - 1) // page_size

    return templates.TemplateResponse(request, "logs.html", context={
        "user": user,
        "logs": logs,
        "calendars": calendars,
        "page": page,
        "total_pages": total_pages,
        "total": total,
        "calendar_id": calendar_id,
        "status_filter": status_filter,
    })


@router.get("/app/calendars/select", response_class=HTMLResponse)
async def select_calendar_page(
    request: Request,
    token_id: int,
    email: str,
    calendar_type: str = "client",
):
    """Calendar selection page after OAuth."""
    user = await get_current_user_optional(request)
    if not user:
        return RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)

    db = await get_database()

    # Verify token belongs to user
    cursor = await db.execute(
        "SELECT * FROM oauth_tokens WHERE id = ? AND user_id = ?",
        (token_id, user.id)
    )
    token = await cursor.fetchone()

    if not token:
        return RedirectResponse(url="/app?error=invalid_token", status_code=status.HTTP_302_FOUND)

    # Get calendars from the account
    calendars = []
    try:
        from app.auth.google import fetch_calendar_list

        for cal in await fetch_calendar_list(user.id, email):
            # backgroundColor is interpolated into a CSS style
            # attribute in the template — validate it is a plain hex
            # colour so a hostile value cannot break out of the rule.
            bg = cal.get("backgroundColor")
            if not (isinstance(bg, str) and _HEX_COLOR_RE.match(bg)):
                cal["backgroundColor"] = "#4285f4"
            if calendar_type == "personal":
                # Personal calendars: show all readable calendars
                if cal.get("accessRole") in ["owner", "writer", "reader", "freeBusyReader"]:
                    calendars.append(cal)
            else:
                # Client calendars: need write access
                if cal.get("accessRole") in ["owner", "writer"]:
                    calendars.append(cal)

    except Exception as e:
        logger.error(f"Failed to get calendars: {e}")
        return RedirectResponse(url="/app?error=calendar_fetch_failed", status_code=status.HTTP_302_FOUND)

    # Find already-connected calendar IDs for this user
    cursor = await db.execute(
        """SELECT google_calendar_id FROM client_calendars
           WHERE user_id = ? AND is_active = TRUE""",
        (user.id,)
    )
    connected_ids = {row["google_calendar_id"] for row in await cursor.fetchall()}

    return templates.TemplateResponse(request, "select_calendar.html", context={
        "user": user,
        "token_id": token_id,
        "email": email,
        "calendars": calendars,
        "calendar_type": calendar_type,
        "connected_ids": connected_ids,
    })


# Admin routes
@router.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    """Admin dashboard."""
    user = await get_current_user_optional(request)
    if not user:
        return RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)

    if not user.is_admin:
        return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)

    db = await get_database()

    # Get system stats
    cursor = await db.execute("SELECT COUNT(*) FROM users")
    total_users = (await cursor.fetchone())[0]

    cursor = await db.execute("SELECT COUNT(*) FROM client_calendars WHERE is_active = TRUE")
    active_calendars = (await cursor.fetchone())[0]

    cursor = await db.execute(
        """SELECT COUNT(*) FROM ledger_events
            WHERE status = 'active' AND user_intentionally_deleted = 0"""
    )
    total_events = (await cursor.fetchone())[0]

    cursor = await db.execute(
        """SELECT COUNT(*) FROM sync_log
           WHERE status = 'failure' AND datetime(created_at) > datetime('now', '-1 day')"""
    )
    errors_24h = (await cursor.fetchone())[0]

    # Recent alerts
    cursor = await db.execute(
        """SELECT * FROM alert_queue
           ORDER BY created_at DESC LIMIT 10"""
    )
    recent_alerts = await cursor.fetchall()

    return templates.TemplateResponse(request, "admin/dashboard.html", context={
        "user": user,
        "total_users": total_users,
        "active_calendars": active_calendars,
        "total_events": total_events,
        "errors_24h": errors_24h,
        "recent_alerts": recent_alerts,
    })


@router.get("/admin/users", response_class=HTMLResponse)
async def admin_users(request: Request, search: Optional[str] = None):
    """Admin user management page."""
    user = await get_current_user_optional(request)
    if not user or not user.is_admin:
        return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)

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
    users = await cursor.fetchall()

    return templates.TemplateResponse(request, "admin/users.html", context={
        "user": user,
        "users": users,
        "search": search,
    })


@router.get("/admin/users/{user_id}", response_class=HTMLResponse)
async def admin_user_detail(request: Request, user_id: int):
    """Admin user detail page."""
    user = await get_current_user_optional(request)
    if not user or not user.is_admin:
        return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)

    db = await get_database()

    # Get target user
    cursor = await db.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    target_user = await cursor.fetchone()

    if not target_user:
        return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)

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
    calendars = await cursor.fetchall()

    # Get recent logs
    cursor = await db.execute(
        """SELECT * FROM sync_log
           WHERE user_id = ?
           ORDER BY created_at DESC LIMIT 20""",
        (user_id,)
    )
    logs = await cursor.fetchall()

    return templates.TemplateResponse(request, "admin/user_detail.html", context={
        "user": user,
        "target_user": target_user,
        "calendars": calendars,
        "logs": logs,
    })


@router.get("/admin/logs", response_class=HTMLResponse)
async def admin_logs(
    request: Request,
    page: int = 1,
    user_id: Optional[int] = None,
    status_filter: Optional[str] = None,
):
    """Admin system logs page."""
    user = await get_current_user_optional(request)
    if not user or not user.is_admin:
        return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)

    db = await get_database()
    page_size = 100

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

    # Get total
    count_query = query.replace(
        "SELECT sl.*, u.email as user_email, cc.display_name as calendar_name",
        "SELECT COUNT(*)"
    )
    cursor = await db.execute(count_query, params)
    total = (await cursor.fetchone())[0]

    query += " ORDER BY sl.created_at DESC LIMIT ? OFFSET ?"
    params.extend([page_size, (page - 1) * page_size])

    cursor = await db.execute(query, params)
    logs = await cursor.fetchall()

    total_pages = (total + page_size - 1) // page_size

    return templates.TemplateResponse(request, "admin/logs.html", context={
        "user": user,
        "logs": logs,
        "page": page,
        "total_pages": total_pages,
        "total": total,
        "user_id": user_id,
        "status_filter": status_filter,
    })


@router.get("/admin/settings", response_class=HTMLResponse)
async def admin_settings(request: Request):
    """Admin settings page."""
    user = await get_current_user_optional(request)
    if not user or not user.is_admin:
        return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)

    from app.database import get_setting

    settings = {}
    for key in ["smtp_host", "smtp_port", "smtp_username", "smtp_from_address", "alert_emails", "alerts_enabled", "sync_paused"]:
        setting = await get_setting(key)
        if setting:
            settings[key] = setting.get("value_plain", "")

    # Service-account mode was removed at the Stage-5 cutover; the
    # template no longer renders any sa_* fields, so we don't pass
    # them in.
    db = await get_database()
    cursor = await db.execute(
        "SELECT id, email, display_name, main_calendar_id FROM users ORDER BY id"
    )
    all_users = [dict(row) for row in await cursor.fetchall()]

    return templates.TemplateResponse(request, "admin/settings.html", context={
        "user": user,
        "settings": settings,
        "all_users": all_users,
    })


# ---------------------------------------------------------------------------
# Helpers: merge facade counts into row dicts so templates that read
# ``row['event_count']`` / ``row['busy_block_count']`` keep working.
# ---------------------------------------------------------------------------
def _merge_counts(row, events_by_cal: dict[int, int], busy_by_cal: dict[int, int]) -> dict:
    """Convert an aiosqlite.Row into a dict and inject ledger
    counts under ``event_count`` / ``busy_block_count`` keys."""
    d = {k: row[k] for k in row.keys()}
    cid = int(d.get("id") or 0)
    d["event_count"] = events_by_cal.get(cid, 0)
    d["busy_block_count"] = busy_by_cal.get(cid, 0)
    return d


def _merge_webcal_count(row, events_by_sub: dict[int, int]) -> dict:
    d = {k: row[k] for k in row.keys()}
    sid = int(d.get("id") or 0)
    d["event_count"] = events_by_sub.get(sid, 0)
    # placement_target_status drives the dashboard badge.  Mirrors the
    # API list response (see app/api/webcal.py:_derive_placement_status)
    # so the UI and API agree on which subscriptions are "stale".
    kind = d.get("placement_kind") or "main"
    target_id = d.get("placement_client_calendar_id")
    target_active = d.get("placement_client_is_active")
    if kind != "client":
        d["placement_target_status"] = "not_applicable"
    elif target_id is None or not target_active:
        d["placement_target_status"] = "disconnected"
    else:
        d["placement_target_status"] = "active"
    # Display name with cache fallback so the badge can name the lost
    # target even after its row goes inactive (or, on hard delete,
    # disappears entirely).
    name = d.get("placement_client_display_name")
    if kind == "client" and not name:
        name = d.get("placement_client_display_name_cache")
    d["placement_client_display_name"] = name
    return d
