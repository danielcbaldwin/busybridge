"""Personal calendar management API endpoints."""

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
router = APIRouter(prefix="/personal-calendars", tags=["personal-calendars"])


class PersonalCalendarResponse(BaseModel):
    id: int
    google_calendar_id: str
    display_name: Optional[str] = None
    google_account_email: str
    is_active: bool = True
    last_sync: Optional[str] = None
    sync_status: str = "unknown"
    consecutive_failures: int = 0


class ConnectPersonalCalendarRequest(BaseModel):
    token_id: int
    calendars: list[dict]  # [{calendar_id, display_name}]


@router.get("", response_model=list[PersonalCalendarResponse])
async def list_personal_calendars(user: User = Depends(get_current_user)):
    """List connected personal calendars for current user."""
    db = await get_database()

    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email, css.last_incremental_sync,
                  css.last_full_sync, css.consecutive_failures
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
           WHERE cc.user_id = ? AND cc.is_active = TRUE AND cc.calendar_type = 'personal'
           ORDER BY cc.created_at DESC""",
        (user.id,)
    )
    rows = await cursor.fetchall()
    calendars = []

    for row in rows:
        last_sync = row["last_incremental_sync"] or row["last_full_sync"]
        sync_status = "ok"
        if row["consecutive_failures"] and row["consecutive_failures"] >= 5:
            sync_status = "error"
        elif row["consecutive_failures"] and row["consecutive_failures"] >= 1:
            sync_status = "warning"
        elif not last_sync:
            sync_status = "pending"

        calendars.append(PersonalCalendarResponse(
            id=row["id"],
            google_calendar_id=row["google_calendar_id"],
            display_name=row["display_name"],
            google_account_email=row["google_account_email"],
            is_active=bool(row["is_active"]),
            last_sync=last_sync,
            sync_status=sync_status,
            consecutive_failures=row["consecutive_failures"] or 0,
        ))

    return calendars


@router.post("", response_model=list[PersonalCalendarResponse])
async def connect_personal_calendars(
    request: ConnectPersonalCalendarRequest,
    user: User = Depends(get_current_user)
):
    """Connect one or more personal calendars."""
    db = await get_database()

    # Verify token belongs to user and is a personal token
    cursor = await db.execute(
        """SELECT * FROM oauth_tokens
           WHERE id = ? AND user_id = ? AND account_type = 'personal'""",
        (request.token_id, user.id)
    )
    token = await cursor.fetchone()

    if not token:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Token not found"
        )

    if not request.calendars:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No calendars selected"
        )

    # Each calendar is verified with a Google API round-trip; cap the
    # batch so one request cannot tie a worker up indefinitely.
    if len(request.calendars) > 50:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Too many calendars in one request (max 50).",
        )

    # The main calendar must not also be connected as a personal
    # calendar — routing is keyed on google_calendar_id, so an overlap
    # would route main-calendar API calls through the wrong account.
    main_row = await (await db.execute(
        "SELECT main_calendar_id FROM users WHERE id = ?", (user.id,),
    )).fetchone()
    main_calendar_id = main_row["main_calendar_id"] if main_row else None
    if main_calendar_id and any(
        c.get("calendar_id") == main_calendar_id for c in request.calendars
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Your main calendar cannot also be connected as a "
                "personal calendar."
            ),
        )

    from app.auth.google import fetch_calendar

    results = []
    for cal_info in request.calendars:
        calendar_id = cal_info.get("calendar_id")
        display_name = cal_info.get("display_name", calendar_id)

        if not calendar_id:
            continue

        # Check if already connected
        cursor = await db.execute(
            """SELECT id FROM client_calendars
               WHERE user_id = ? AND google_calendar_id = ? AND is_active = TRUE""",
            (user.id, calendar_id)
        )
        if await cursor.fetchone():
            continue

        # Verify the calendar is reachable with this token before
        # storing it — an unreachable id would just fail every sync.
        try:
            await fetch_calendar(
                user.id, token["google_account_email"], calendar_id,
            )
        except Exception as e:
            logger.warning(
                "skipping inaccessible personal calendar %s: %s",
                calendar_id, e,
            )
            continue

        # Create the personal calendar connection (no color needed).
        # The already-connected check above runs BEFORE the slow Google
        # verify call, so a concurrent connect of the same calendar can
        # slip past it — the partial UNIQUE index on active
        # (user_id, google_calendar_id) is the real guard.
        try:
            cursor = await db.execute(
                """INSERT INTO client_calendars
                   (user_id, oauth_token_id, google_calendar_id, display_name, calendar_type)
                   VALUES (?, ?, ?, ?, 'personal')
                   RETURNING id""",
                (user.id, request.token_id, calendar_id, display_name)
            )
        except sqlite3.IntegrityError:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Calendar already connected: {calendar_id}"
            )
        row = await cursor.fetchone()
        new_id = row["id"]

        # Create sync state
        await db.execute(
            "INSERT INTO calendar_sync_state (client_calendar_id) VALUES (?)",
            (new_id,)
        )
        await db.commit()

        # Log
        await db.execute(
            """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
               VALUES (?, ?, 'connect_personal', 'success', ?)""",
            (user.id, new_id, json.dumps({"calendar_id": calendar_id}))
        )
        await db.commit()

        # Trigger initial sync via the ledger queue.
        from app.ledger.triggers import enqueue_manual
        await enqueue_manual(
            db, user_id=user.id, source_hint=f"personal:{new_id}",
        )

        results.append(PersonalCalendarResponse(
            id=new_id,
            google_calendar_id=calendar_id,
            display_name=display_name,
            google_account_email=token["google_account_email"],
            is_active=True,
            sync_status="pending",
        ))

    if results:
        # Register Google push channels for the new calendars so they
        # get real-time webhook sync without waiting for a restart.
        from app.jobs.webhook_renewal import schedule_webhook_registration
        schedule_webhook_registration(user.id)

    return results


@router.delete("/{calendar_id}")
async def disconnect_personal_calendar(
    calendar_id: int,
    user: User = Depends(get_current_user)
):
    """Disconnect a personal calendar."""
    db = await get_database()

    cursor = await db.execute(
        """SELECT * FROM client_calendars
           WHERE id = ? AND user_id = ? AND is_active = TRUE AND calendar_type = 'personal'""",
        (calendar_id, user.id)
    )
    calendar = await cursor.fetchone()

    if not calendar:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Personal calendar not found"
        )

    from app.ledger.admin_ops import disconnect_calendar
    await disconnect_calendar(
        db, user_id=user.id, client_calendar_id=calendar_id,
    )

    await db.execute(
        """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
           VALUES (?, ?, 'disconnect_personal', 'success', NULL)""",
        (user.id, calendar_id)
    )
    await db.commit()

    return {"status": "ok", "message": "Personal calendar disconnected"}


@router.post("/{calendar_id}/sync")
async def trigger_personal_calendar_sync(
    calendar_id: int,
    user: User = Depends(get_current_user)
):
    """Trigger manual sync for a personal calendar."""
    db = await get_database()

    cursor = await db.execute(
        """SELECT * FROM client_calendars
           WHERE id = ? AND user_id = ? AND is_active = TRUE AND calendar_type = 'personal'""",
        (calendar_id, user.id)
    )
    calendar = await cursor.fetchone()

    if not calendar:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Personal calendar not found"
        )

    from app.ledger.triggers import enqueue_manual
    await enqueue_manual(
        db, user_id=user.id, source_hint=f"personal:{calendar_id}",
    )

    return {"status": "ok", "message": "Sync triggered"}
