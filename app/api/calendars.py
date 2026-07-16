"""Calendar management API endpoints."""

import json
import logging
import sqlite3
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.auth.session import get_current_user, User
from app.database import get_database

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/client-calendars", tags=["calendars"])


class ClientCalendarResponse(BaseModel):
    """Client calendar response model."""
    id: int
    google_calendar_id: str
    display_name: Optional[str] = None
    google_account_email: str
    is_active: bool = True
    last_sync: Optional[str] = None
    sync_status: str = "unknown"
    consecutive_failures: int = 0


class UpdateColorRequest(BaseModel):
    """Request to change a client calendar's color."""
    color_id: str


class ConnectCalendarRequest(BaseModel):
    """Request to connect a client calendar."""
    token_id: int
    calendar_id: str
    display_name: Optional[str] = None


class CalendarStatusResponse(BaseModel):
    """Detailed calendar status response."""
    id: int
    google_calendar_id: str
    display_name: Optional[str] = None
    is_active: bool = True
    sync_token: Optional[str] = None
    last_full_sync: Optional[str] = None
    last_incremental_sync: Optional[str] = None
    consecutive_failures: int = 0
    last_error: Optional[str] = None
    event_count: int = 0
    busy_block_count: int = 0


@router.get("", response_model=list[ClientCalendarResponse])
async def list_client_calendars(user: User = Depends(get_current_user)):
    """List connected client calendars for current user."""
    db = await get_database()

    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email, css.last_incremental_sync,
                  css.last_full_sync, css.consecutive_failures
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
           WHERE cc.user_id = ? AND cc.is_active = TRUE
             AND cc.calendar_type = 'client'
           ORDER BY cc.created_at DESC""",
        (user.id,)
    )

    rows = await cursor.fetchall()
    calendars = []

    for row in rows:
        last_sync = row["last_incremental_sync"] or row["last_full_sync"]

        # consecutive_failures is NULL when the LEFT JOIN finds no
        # calendar_sync_state row yet (calendar never synced) — coerce
        # before comparing or the endpoint 500s.
        failures = row["consecutive_failures"] or 0

        # Determine sync status
        status = "ok"
        if failures >= 5:
            status = "error"
        elif failures >= 1:
            status = "warning"
        elif not last_sync:
            status = "pending"

        calendars.append(ClientCalendarResponse(
            id=row["id"],
            google_calendar_id=row["google_calendar_id"],
            display_name=row["display_name"],
            google_account_email=row["google_account_email"],
            is_active=bool(row["is_active"]),
            last_sync=last_sync,
            sync_status=status,
            consecutive_failures=row["consecutive_failures"] or 0,
        ))

    return calendars


@router.post("", response_model=ClientCalendarResponse)
async def connect_client_calendar(
    request: ConnectCalendarRequest,
    user: User = Depends(get_current_user)
):
    """Connect a new client calendar."""
    db = await get_database()

    # Verify token belongs to user
    cursor = await db.execute(
        """SELECT * FROM oauth_tokens
           WHERE id = ? AND user_id = ? AND account_type = 'client'""",
        (request.token_id, user.id)
    )
    token = await cursor.fetchone()

    if not token:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Token not found"
        )

    # The main calendar must not also be connected as a client
    # calendar.  Routing is keyed on google_calendar_id, so an overlap
    # would send main-calendar API calls through the client account's
    # token (or vice versa).
    main_row = await (await db.execute(
        "SELECT main_calendar_id FROM users WHERE id = ?", (user.id,),
    )).fetchone()
    if main_row and main_row["main_calendar_id"] == request.calendar_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "This calendar is your main calendar and cannot also be "
                "connected as a client calendar."
            ),
        )

    # Verify calendar exists and is accessible
    try:
        from app.auth.google import fetch_calendar

        cal_info = await fetch_calendar(
            user.id, token["google_account_email"], request.calendar_id,
        )
        display_name = request.display_name or cal_info.get(
            "summary", request.calendar_id,
        )

    except Exception as e:
        logger.error(f"Failed to verify calendar: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot access the selected calendar. Verify the account still has access to it.",
        )

    # Check if already connected
    cursor = await db.execute(
        """SELECT id FROM client_calendars
           WHERE user_id = ? AND google_calendar_id = ? AND is_active = TRUE""",
        (user.id, request.calendar_id)
    )
    existing = await cursor.fetchone()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Calendar already connected"
        )

    # Auto-assign a color for this calendar (Google Calendar event colorIds 1-11)
    cursor = await db.execute(
        """SELECT color_id FROM client_calendars
           WHERE user_id = ? AND is_active = TRUE AND color_id IS NOT NULL""",
        (user.id,)
    )
    used_colors = {row["color_id"] for row in await cursor.fetchall()}
    # Pick the first unused color, cycling through 1-11
    all_colors = [str(i) for i in range(1, 12)]
    color_id = next((c for c in all_colors if c not in used_colors), all_colors[0])

    # Create the calendar connection.  A concurrent connect of the same
    # calendar can interleave between the duplicate check above and
    # this INSERT — the partial UNIQUE index on active
    # (user_id, google_calendar_id) is the real guard.
    try:
        cursor = await db.execute(
            """INSERT INTO client_calendars
               (user_id, oauth_token_id, google_calendar_id, display_name, color_id)
               VALUES (?, ?, ?, ?, ?)
               RETURNING id""",
            (user.id, request.token_id, request.calendar_id, display_name, color_id)
        )
    except sqlite3.IntegrityError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Calendar already connected"
        )
    row = await cursor.fetchone()
    calendar_id = row["id"]

    # Create sync state entry
    await db.execute(
        """INSERT INTO calendar_sync_state (client_calendar_id)
           VALUES (?)""",
        (calendar_id,)
    )
    await db.commit()

    # Log the connection
    await db.execute(
        """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
           VALUES (?, ?, 'connect', 'success', ?)""",
        (user.id, calendar_id, json.dumps({"calendar_id": request.calendar_id}))
    )
    await db.commit()

    # Trigger initial sync via the ledger queue.
    from app.ledger.triggers import enqueue_manual
    await enqueue_manual(
        db, user_id=user.id, source_hint=f"client:{calendar_id}",
    )

    # Replan every existing active event against the CURRENT
    # active-client-calendars set so the just-connected calendar
    # receives busy-block projections from events already ingested.
    # Without this, only NEW events after the connect would target
    # this calendar; previously-ingested events keep their old
    # projection set and cast nothing here.
    from app.ledger import admin_ops as _admin
    try:
        queued = await _admin.replan_all_active_events(db, user_id=user.id)
        logger.info(
            "replanned %d active events for user %s after connecting client_calendar_id=%s",
            queued, user.id, calendar_id,
        )
    except Exception as e:
        # Non-fatal: initial sync will still ingest new events; the
        # user can always click Full re-sync to force a replan later.
        logger.warning(
            "replan_all_active_events failed for user %s: %s (connect still succeeded)",
            user.id, e,
        )

    # Register a Google push channel for the new calendar so it gets
    # real-time webhook sync without waiting for a server restart.
    from app.jobs.webhook_renewal import schedule_webhook_registration
    schedule_webhook_registration(user.id)

    return ClientCalendarResponse(
        id=calendar_id,
        google_calendar_id=request.calendar_id,
        display_name=display_name,
        google_account_email=token["google_account_email"],
        is_active=True,
        sync_status="pending",
    )


@router.delete("/{calendar_id}")
async def disconnect_client_calendar(
    calendar_id: int,
    user: User = Depends(get_current_user)
):
    """Disconnect a client calendar."""
    db = await get_database()

    # Verify calendar belongs to user
    cursor = await db.execute(
        """SELECT * FROM client_calendars
           WHERE id = ? AND user_id = ? AND is_active = TRUE""",
        (calendar_id, user.id)
    )
    calendar = await cursor.fetchone()

    if not calendar:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Calendar not found"
        )

    # Ledger disconnect: cleanup + deactivate + drain.
    from app.ledger.admin_ops import disconnect_calendar
    await disconnect_calendar(
        db, user_id=user.id, client_calendar_id=calendar_id,
    )

    # Log the disconnection
    await db.execute(
        """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
           VALUES (?, ?, 'disconnect', 'success', NULL)""",
        (user.id, calendar_id)
    )
    await db.commit()

    return {"status": "ok", "message": "Calendar disconnected"}


@router.post("/{calendar_id}/sync")
async def trigger_calendar_sync(
    calendar_id: int,
    user: User = Depends(get_current_user)
):
    """Trigger manual sync for a calendar with settling delay."""
    db = await get_database()

    # Verify calendar belongs to user
    cursor = await db.execute(
        """SELECT * FROM client_calendars
           WHERE id = ? AND user_id = ? AND is_active = TRUE""",
        (calendar_id, user.id)
    )
    calendar = await cursor.fetchone()

    if not calendar:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Calendar not found"
        )

    # Settling delay matches the ledger trigger so Google's
    # cross-session eventual consistency has time to propagate.
    from app.ledger.triggers import MANUAL_SETTLING_DELAY, enqueue_manual
    await enqueue_manual(
        db, user_id=user.id, source_hint=f"client:{calendar_id}",
    )
    return {
        "status": "ok",
        "message": "Sync triggered",
        "settle_seconds": int(MANUAL_SETTLING_DELAY.total_seconds()),
    }


@router.get("/{calendar_id}/sync-progress")
async def get_calendar_sync_progress(
    calendar_id: int,
    user: User = Depends(get_current_user)
):
    """Poll manual sync progress for a calendar."""
    db = await get_database()

    cursor = await db.execute(
        """SELECT id FROM client_calendars
           WHERE id = ? AND user_id = ?""",
        (calendar_id, user.id)
    )
    if not await cursor.fetchone():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Calendar not found"
        )

    # Translate ledger/outbox state into the status vocabulary the
    # dashboard's progress widget understands: settling → syncing →
    # complete (or error).  The widget never handled the old
    # idle/running shape, so the bar sat on "Starting..." forever.
    from datetime import datetime, timedelta, timezone
    from app.ledger.triggers import MANUAL_SETTLING_DELAY

    now = datetime.now(timezone.utc)
    recent_cutoff = (now - timedelta(minutes=2)).isoformat()

    async def _count(extra_sql: str, params: tuple) -> int:
        # Count outbox work for projections of events SOURCED from this
        # calendar — its main-calendar copies and the busy blocks it
        # casts onto other calendars.  Scoping by the event's source
        # calendar (not "any main-kind projection") keeps one
        # calendar's progress widget from reflecting unrelated
        # main-copy work driven by the user's other calendars.
        row = await (await db.execute(
            f"""SELECT COUNT(*) AS n FROM outbox_operations o
                  JOIN ledger_projections p ON p.id = o.projection_id
                  JOIN ledger_events e ON e.id = p.ledger_event_id
                 WHERE o.user_id = ?
                   AND e.source_calendar_id = ?
                   AND e.source_type IN ('client', 'personal')
                   {extra_sql}""",
            (user.id, calendar_id, *params),
        )).fetchone()
        return int(row["n"] or 0)

    failed = await _count(
        "AND o.status = 'permanent_failure' AND o.completed_at >= ?",
        (recent_cutoff,),
    )
    if failed:
        return {
            "status": "error",
            "message": f"{failed} change(s) could not be applied",
        }

    req = await (await db.execute(
        """SELECT scheduled_for, enqueued_at, in_flight, last_run_at
             FROM reconcile_requests WHERE user_id = ?""",
        (user.id,),
    )).fetchone()
    in_flight = bool(req and req["in_flight"])
    # The reconcile for the current request has run once its claim
    # timestamp catches up to when the request was enqueued.
    reconcile_ran = bool(
        req and req["last_run_at"] and req["enqueued_at"]
        and req["last_run_at"] >= req["enqueued_at"]
    )

    # Settling window: a manual sync waits for Google's eventual
    # consistency before the reconcile fires.
    if req and req["scheduled_for"] and not in_flight and not reconcile_ran:
        try:
            sched = datetime.fromisoformat(req["scheduled_for"])
            if sched.tzinfo is None:
                sched = sched.replace(tzinfo=timezone.utc)
        except ValueError:
            sched = None
        if sched is not None and sched > now:
            total = max(1, int(MANUAL_SETTLING_DELAY.total_seconds()))
            remaining = min(total, max(1, round((sched - now).total_seconds())))
            return {"status": "settling", "total": total, "remaining": remaining}

    pending = await _count("AND o.status IN ('pending', 'in_flight')", ())
    if pending or in_flight:
        return {
            "status": "syncing",
            "step": (
                f"Applying {pending} change(s)…" if pending
                else "Checking calendars…"
            ),
        }
    if req and not reconcile_ran:
        # Settling done, but the drain tick has not fired yet.
        return {"status": "syncing", "step": "Waiting to sync…"}

    done = await _count(
        "AND o.status = 'done' AND o.completed_at >= ?", (recent_cutoff,),
    )
    return {"status": "complete", "events_processed": done}


@router.post("/{calendar_id}/resync")
async def trigger_calendar_resync(
    calendar_id: int,
    user: User = Depends(get_current_user)
):
    """Clear sync token and trigger full re-sync for a single calendar."""
    db = await get_database()

    cursor = await db.execute(
        """SELECT * FROM client_calendars
           WHERE id = ? AND user_id = ? AND is_active = TRUE""",
        (calendar_id, user.id)
    )
    calendar = await cursor.fetchone()

    if not calendar:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Calendar not found"
        )

    await db.execute(
        """UPDATE calendar_sync_state SET sync_token = NULL
           WHERE client_calendar_id = ?""",
        (calendar_id,)
    )
    await db.commit()

    from app.ledger.triggers import enqueue_manual
    await enqueue_manual(
        db, user_id=user.id, source_hint=f"client:{calendar_id}",
    )
    return {"status": "ok", "message": "Full resync triggered"}


@router.post("/{calendar_id}/cleanup-resync")
async def trigger_calendar_cleanup_resync(
    calendar_id: int,
    user: User = Depends(get_current_user)
):
    """Clean up all synced events for a calendar and trigger a fresh re-sync."""
    db = await get_database()

    cursor = await db.execute(
        """SELECT * FROM client_calendars
           WHERE id = ? AND user_id = ? AND is_active = TRUE""",
        (calendar_id, user.id)
    )
    calendar = await cursor.fetchone()

    if not calendar:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Calendar not found"
        )

    from app.ledger.admin_ops import cleanup_one_calendar
    from app.ledger.triggers import enqueue_manual
    await cleanup_one_calendar(
        db, user_id=user.id, client_calendar_id=calendar_id,
    )
    await enqueue_manual(
        db, user_id=user.id, source_hint=f"client:{calendar_id}",
    )
    return {"status": "ok", "message": "Cleanup & re-sync started"}


@router.get("/{calendar_id}/status", response_model=CalendarStatusResponse)
async def get_calendar_status(
    calendar_id: int,
    user: User = Depends(get_current_user)
):
    """Get detailed sync status for a calendar."""
    db = await get_database()

    # Get calendar with sync state
    cursor = await db.execute(
        """SELECT cc.*, css.sync_token, css.last_full_sync,
                  css.last_incremental_sync, css.consecutive_failures, css.last_error
           FROM client_calendars cc
           LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
           WHERE cc.id = ? AND cc.user_id = ?""",
        (calendar_id, user.id)
    )
    calendar = await cursor.fetchone()

    if not calendar:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Calendar not found"
        )

    # Count events and busy blocks via the ledger.
    from app.ledger.facade import (
        count_active_events_per_source_calendar,
        count_busy_blocks_per_calendar,
    )
    src_counts = await count_active_events_per_source_calendar(
        db, user_id=user.id,
    )
    busy_counts = await count_busy_blocks_per_calendar(db, user_id=user.id)
    event_count = int(src_counts.get(calendar_id, 0))
    busy_block_count = int(busy_counts.get(calendar_id, 0))

    return CalendarStatusResponse(
        id=calendar["id"],
        google_calendar_id=calendar["google_calendar_id"],
        display_name=calendar["display_name"],
        is_active=bool(calendar["is_active"]),
        sync_token=calendar["sync_token"][:20] + "..." if calendar["sync_token"] else None,
        last_full_sync=calendar["last_full_sync"],
        last_incremental_sync=calendar["last_incremental_sync"],
        consecutive_failures=calendar["consecutive_failures"] or 0,
        last_error=calendar["last_error"],
        event_count=event_count,
        busy_block_count=busy_block_count,
    )


@router.patch("/{calendar_id}/color")
async def update_calendar_color(
    calendar_id: int,
    request: UpdateColorRequest,
    user: User = Depends(get_current_user),
):
    """Change a client calendar's color and recolor all its events on main."""
    valid_colors = {str(i) for i in range(1, 12)}
    if request.color_id not in valid_colors:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="color_id must be '1' through '11'",
        )

    db = await get_database()

    cursor = await db.execute(
        """SELECT * FROM client_calendars
           WHERE id = ? AND user_id = ? AND is_active = TRUE""",
        (calendar_id, user.id),
    )
    calendar = await cursor.fetchone()
    if not calendar:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Calendar not found",
        )

    old_color = calendar["color_id"]
    if old_color == request.color_id:
        return {"status": "ok", "message": "Color unchanged"}

    # Recolor every ledger row sourced from this calendar; the
    # next reconcile re-renders projection payloads with the new
    # colorId.
    from app.ledger.admin_ops import recolor_client_calendar
    from app.ledger.triggers import enqueue_manual
    await recolor_client_calendar(
        db, client_calendar_id=calendar_id, new_color_id=request.color_id,
    )
    await enqueue_manual(db, user_id=user.id, source_hint="all")
    return {"status": "ok", "message": "Color updated, recoloring events in background"}
