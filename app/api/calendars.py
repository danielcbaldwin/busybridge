"""Calendar management API endpoints."""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.auth.session import get_current_user, User
from app.auth.google import get_valid_access_token
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
           ORDER BY cc.created_at DESC""",
        (user.id,)
    )

    rows = await cursor.fetchall()
    calendars = []

    for row in rows:
        last_sync = row["last_incremental_sync"] or row["last_full_sync"]

        # Determine sync status
        status = "ok"
        if row["consecutive_failures"] >= 5:
            status = "error"
        elif row["consecutive_failures"] >= 1:
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

    # Verify calendar exists and is accessible
    try:
        from googleapiclient.discovery import build
        from google.oauth2.credentials import Credentials

        access_token = await get_valid_access_token(user.id, token["google_account_email"])
        credentials = Credentials(token=access_token)
        service = build("calendar", "v3", credentials=credentials)

        cal_info = service.calendars().get(calendarId=request.calendar_id).execute()
        display_name = request.display_name or cal_info.get("summary", request.calendar_id)

    except Exception as e:
        logger.error(f"Failed to verify calendar: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot access calendar: {str(e)}"
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

    # Create the calendar connection
    cursor = await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, color_id)
           VALUES (?, ?, ?, ?, ?)
           RETURNING id""",
        (user.id, request.token_id, request.calendar_id, display_name, color_id)
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
        (user.id, calendar_id, f'{{"calendar_id": "{request.calendar_id}"}}')
    )
    await db.commit()

    # Trigger initial sync via the ledger queue.
    from app.ledger.triggers import enqueue_manual
    await enqueue_manual(
        db, user_id=user.id, source_hint=f"client:{calendar_id}",
    )

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

    # Settling delay matches the legacy trigger so Google's
    # cross-session eventual consistency has time to propagate.
    _MANUAL_SYNC_SETTLE = 25
    from app.ledger.triggers import enqueue_manual
    await enqueue_manual(
        db, user_id=user.id, source_hint=f"client:{calendar_id}",
    )
    return {"status": "ok", "message": "Sync triggered", "settle_seconds": _MANUAL_SYNC_SETTLE}


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

    # Surface the outbox queue depth for this calendar's projections
    # as "progress".  Empty queue = idle = done.
    row = await (await db.execute(
        """SELECT COUNT(*) AS pending FROM outbox_operations o
             JOIN ledger_projections p ON p.id = o.projection_id
            WHERE o.user_id = ? AND o.status IN ('pending', 'in_flight')
              AND (p.target_calendar_id = ? OR p.target_kind = 'main')""",
        (user.id, calendar_id),
    )).fetchone()
    pending = int(row["pending"] or 0)
    if pending == 0:
        return {"status": "idle"}
    return {"status": "running", "pending": pending}


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
